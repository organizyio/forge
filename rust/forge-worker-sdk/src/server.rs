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
use tracing::{error, info, warn};

use crate::dispatcher::{BaseDispatcher, WorkerHandler};
use crate::framing::{Encoding, Frame, FrameCodec};
use crate::protocol::WireEvent;

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
    dispatcher:  Arc<BaseDispatcher<H>>,
    encoding:    Encoding,
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
    dispatcher:  Arc<BaseDispatcher<H>>,
    encoding:    Encoding,
) -> anyhow::Result<()> {
    use interprocess::local_socket::tokio::prelude::*;
    use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToFsName};

    let _ = std::fs::remove_file(socket_path);
    let name     = socket_path.to_fs_name::<GenericNamespaced>()?;
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
    pipe_name:  &str,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding:   Encoding,
) -> anyhow::Result<()> {
    use interprocess::os::windows::named_pipe::pipe_mode;
    use interprocess::os::windows::named_pipe::{PipeListenerOptions, PipeMode};

    let listener = PipeListenerOptions::new()
        .path(pipe_name)
        .mode(PipeMode::Bytes)
        .create_tokio_duplex::<pipe_mode::Bytes>()?;
    info!(pipe = pipe_name, "forge worker listening on Windows named pipe");

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
    reader:     R,
    writer:     W,
    dispatcher: Arc<BaseDispatcher<H>>,
    encoding:   Encoding,
)
where
    R: AsyncRead  + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    H: WorkerHandler,
{
    // outbound: coalesces responses + events into a single write stream
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Frame>();
    // event: product tasks push WireEvents here; we wrap and forward to out_tx
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<WireEvent>();

    // Event fan-out task
    let out_tx_evt = out_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            if out_tx_evt.send(Frame::Event(ev)).is_err() {
                break;
            }
        }
    });

    // Write loop
    let mut sink = Framed::new(writer, FrameCodec::new(encoding));
    let write_handle = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if let Err(e) = sink.send(frame).await {
                warn!("write error: {e}");
                break;
            }
        }
    });

    // Read loop — one dispatch task per request so slow handlers don't block reads
    let mut stream = Framed::new(reader, FrameCodec::new(encoding));
    while let Some(result) = stream.next().await {
        match result {
            Ok(Frame::Request(req)) => {
                let d       = dispatcher.clone();
                let evt_tx  = event_tx.clone();
                let resp_tx = out_tx.clone();
                tokio::spawn(async move {
                    let resp = d.dispatch(req, evt_tx).await;
                    let _ = resp_tx.send(Frame::Response(resp));
                });
            }
            Ok(_) => warn!("unexpected frame kind received from client — ignoring"),
            Err(e) => {
                error!("frame decode error: {e}");
                break;
            }
        }
    }

    write_handle.abort();
}
