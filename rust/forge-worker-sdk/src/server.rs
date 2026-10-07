//! Async IPC server — accepts connections and drives the per-connection loop.
//!
//! The entry point is [`run_worker`], which product binaries call from `main`:
//!
//! ```rust,ignore
//! forge_worker_sdk::server::run_worker(
//!     &args.socket,
//!     ArchivistHandler::new(&args.source_id),
//!     forge_worker_sdk::framing::Encoding::from_str(&args.encoding).unwrap(),
//! ).await?;
//! ```
//!
//! ## Connection model
//! One connection = one Go supervisor client.  Multiple concurrent connections
//! are supported (each gets its own event channel), though in practice only one
//! supervisor connects at a time.  Jobs outlive the connection that started them:
//! if the client disconnects mid-scan the runner task continues and the job stays
//! in the registry until GC.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{error, info};

use crate::dispatcher::{err_response, ok_response};
use crate::dispatcher::{BaseDispatcher, WorkerHandler};
use crate::framing::{Encoding, Frame, FrameCodec};

// ─── PUBLIC ENTRY POINT ──────────────────────────────────────────────────────

/// Start the IPC server.  Blocks until the process exits (or an unrecoverable
/// listener error occurs).
///
/// Spawns a background GC task that evicts old completed-job records every 60 s.
pub async fn run_worker<H: WorkerHandler>(
    socket_path: &str,
    handler: H,
    encoding: Encoding,
) -> anyhow::Result<()> {
    let dispatcher = Arc::new(BaseDispatcher::new(handler, encoding));

    // Background GC: keep the registry from growing unbounded
    let gc_reg = dispatcher.registry.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            gc_reg.gc(50);
        }
    });

    serve(socket_path, dispatcher, encoding).await
}

// ─── PLATFORM DISPATCH ───────────────────────────────────────────────────────

async fn serve<H: WorkerHandler>(
    socket_path: &str,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding: Encoding,
) -> anyhow::Result<()> {
    #[cfg(unix)]
    return serve_unix(socket_path, dispatcher, encoding).await;

    #[cfg(windows)]
    return serve_windows(socket_path, dispatcher, encoding).await;
}

// ─── UNIX SOCKET LISTENER ────────────────────────────────────────────────────

#[cfg(unix)]
async fn serve_unix<H: WorkerHandler>(
    socket_path: &str,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding: Encoding,
) -> anyhow::Result<()> {
    use interprocess::local_socket::tokio::prelude::*;
    use interprocess::local_socket::{GenericFilePath, ListenerOptions, ToFsName};

    let _ = std::fs::remove_file(socket_path);
    let name = socket_path.to_fs_name::<GenericFilePath>()?;
    let listener = ListenerOptions::new().name(name).create_tokio()?;
    info!(path = socket_path, "forge worker listening on Unix socket");

    loop {
        match listener.accept().await {
            Ok(conn) => {
                let d = dispatcher.clone();
                tokio::spawn(async move {
                    info!("client connected");
                    let (rx, tx) = conn.split();
                    handle_connection(rx, tx, d, encoding).await;
                    info!("client disconnected");
                });
            }
            Err(e) => error!("accept error: {e}"),
        }
    }
}

// ─── WINDOWS NAMED PIPE LISTENER ─────────────────────────────────────────────

#[cfg(windows)]
async fn serve_windows<H: WorkerHandler>(
    pipe_name: &str,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding: Encoding,
) -> anyhow::Result<()> {
    use interprocess::os::windows::named_pipe::pipe_mode;
    use interprocess::os::windows::named_pipe::{PipeListenerOptions, PipeMode};

    let listener = PipeListenerOptions::new()
        .path(pipe_name)
        .mode(PipeMode::Bytes)
        .create_tokio_duplex::<pipe_mode::Bytes>()?;
    info!(
        pipe = pipe_name,
        "forge worker listening on Windows named pipe"
    );

    loop {
        match listener.accept().await {
            Ok(conn) => {
                let d = dispatcher.clone();
                tokio::spawn(async move {
                    let (rx, tx) = tokio::io::split(conn);
                    handle_connection(rx, tx, d, encoding).await;
                });
            }
            Err(e) => error!("accept error: {e}"),
        }
    }
}

// ─── PER-CONNECTION HANDLER ──────────────────────────────────────────────────
//
// Each connection gets:
//   - an outbound mpsc channel  →  write loop → socket
//   - an event mpsc channel     →  forwarded into the outbound channel as Event frames
//   - a Framed read stream      →  decodes Request frames, spawns dispatch tasks

async fn handle_connection<R, W, H>(
    reader: R,
    writer: W,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding: Encoding,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    H: WorkerHandler,
{
    let (delivery, mut events) = crate::delivery::Delivery::new(encoding);
    let (responses, mut response_rx) =
        mpsc::channel::<(Frame, tokio::sync::OwnedSemaphorePermit)>(32);
    let response_bytes = Arc::new(tokio::sync::Semaphore::new(1024 * 1024));
    let requests = Arc::new(tokio::sync::Semaphore::new(16));
    let writer_delivery = delivery.clone();
    let write_handle = tokio::spawn(async move {
        let mut sink = Framed::new(writer, FrameCodec::new(encoding));
        loop {
            let next = tokio::select! {biased;
                Some((frame,permit))=response_rx.recv()=>Some((frame,Some(permit))),
                Some(frame)=events.recv()=>Some((frame,None)),
                _=tokio::time::sleep(std::time::Duration::from_millis(50))=>{if writer_delivery.is_closed(){break;}continue;},
                else=>None,
            };
            let Some((frame, _permit)) = next else { break };
            let frame = match frame {
                Frame::Event(e) => Frame::Event(writer_delivery.latest_progress(e)),
                other => other,
            };
            let progress = if let Frame::Event(e) = &frame {
                Some(e.clone())
            } else {
                None
            };
            if !matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), sink.send(frame)).await,
                Ok(Ok(()))
            ) {
                break;
            }
            if let Some(event) = progress {
                writer_delivery.progress_written(&event);
            }
        }
        writer_delivery.close();
    });
    let mut stream = Framed::new(reader, FrameCodec::new(encoding));
    loop {
        let result = tokio::select! {frame=stream.next()=>frame,_=tokio::time::sleep(std::time::Duration::from_millis(50))=>{if delivery.is_closed(){break;}continue;}};
        let Some(Ok(Frame::Request(req))) = result else {
            break;
        };
        let id = req.id.clone();
        let params = req.params.clone().unwrap_or_default();
        let direct = match req.method.as_str() {
            "configure_event_delivery" => Some(
                match delivery.configure(
                    params
                        .get("delivery_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                ) {
                    Ok(()) => ok_response(&id, serde_json::json!({"configured":true})),
                    Err(e) => err_response(&id, "DELIVERY_CONFIGURATION", &e.to_string()),
                },
            ),
            "ack_events" => Some(
                match delivery.ack(
                    params
                        .get("delivery_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                    params.get("job_id").and_then(|v| v.as_str()).unwrap_or(""),
                    params
                        .get("through_sequence")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(u64::MAX),
                ) {
                    Ok(()) => ok_response(&id, serde_json::json!({"acknowledged":true})),
                    Err(e) => err_response(&id, "INVALID_ACK", &e.to_string()),
                },
            ),
            "delivery_stats" => Some(ok_response(&id, delivery.stats())),
            _ => None,
        };
        // Never wait for a product request slot on the reader: acknowledgments must remain readable.
        let permit = if direct.is_none() {
            match requests.clone().try_acquire_owned() {
                Ok(p) => Some(p),
                Err(_) => {
                    delivery.close();
                    break;
                }
            }
        } else {
            None
        };
        let dispatcher = dispatcher.clone();
        let sender = delivery.sender();
        let response_tx = responses.clone();
        let budget = response_bytes.clone();
        let d = delivery.clone();
        tokio::spawn(async move {
            let _request_permit = permit;
            let response = match direct {
                Some(r) => r,
                None => dispatcher.dispatch(req, sender).await,
            };
            let size = match encoding {
                Encoding::Json => serde_json::to_vec(&response)
                    .map(|v| v.len())
                    .unwrap_or(usize::MAX),
                Encoding::Msgpack => rmp_serde::to_vec_named(&response)
                    .map(|v| v.len())
                    .unwrap_or(usize::MAX),
            };
            if size > 1024 * 1024 {
                d.close();
                return;
            }
            let Ok(credit) = budget.try_acquire_many_owned(size as u32) else {
                d.close();
                return;
            };
            if response_tx
                .try_send((Frame::Response(response), credit))
                .is_err()
            {
                d.close();
            }
        });
    }
    delivery.close();
    write_handle.abort();
}

#[cfg(test)]
mod bounded_writer_tests {
    use super::*;
    use crate::job_registry::{EventSender, JobRegistry};
    use crate::protocol::{WireRequest, WireResponse};
    use serde_json::Value;

    struct LargeResponse;
    impl WorkerHandler for LargeResponse {
        fn handle_method(
            &self,
            id: &str,
            _: &str,
            _: Option<Value>,
            _: EventSender,
            _: Arc<JobRegistry>,
        ) -> WireResponse {
            ok_response(id, serde_json::json!({"data": "x".repeat(32 * 1024)}))
        }
        fn worker_version(&self) -> &str {
            "test"
        }
        fn features(&self) -> Vec<String> {
            vec![]
        }
    }

    #[tokio::test]
    async fn stalled_reader_closes_within_write_deadline() {
        for encoding in [Encoding::Json, Encoding::Msgpack] {
            // A 64-byte duplex pipe forces the response write to block without
            // allocating large buffers or exhausting machine memory.
            let (client, server) = tokio::io::duplex(64);
            let (reader, writer) = tokio::io::split(server);
            let dispatcher = Arc::new(BaseDispatcher::new(LargeResponse, encoding));
            let task = tokio::spawn(handle_connection(reader, writer, dispatcher, encoding));
            let mut peer = Framed::new(client, FrameCodec::new(encoding));
            peer.send(Frame::Request(WireRequest {
                id: "blocked".into(),
                method: "large".into(),
                params: None,
            }))
            .await
            .unwrap();
            let started = std::time::Instant::now();
            tokio::time::timeout(std::time::Duration::from_secs(7), task)
                .await
                .expect("stalled connection did not close")
                .unwrap();
            assert!(started.elapsed() >= std::time::Duration::from_secs(4));
        }
    }
}
