//! Minimal Forge worker: handles built-in control RPCs via `BaseDispatcher`;
//! any other method returns `UNKNOWN_METHOD`.
//!
//! Run (Unix):
//! ```text
//! cargo run -p minimal-worker -- --socket /tmp/forge-minimal.sock --encoding json
//! ```

use std::sync::Arc;

use clap::Parser;
use forge_worker_sdk::{
    dispatcher::{unknown_method, WorkerHandler},
    framing::Encoding,
    job_registry::{EventSender, JobRegistry},
    protocol::WireResponse,
    run_worker,
};
use serde_json::Value;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "minimal-worker", about = "Minimal Forge IPC worker example")]
struct Args {
    #[arg(long, default_value = "/tmp/forge-minimal.sock")]
    socket: String,

    /// Accepted for compatibility with Go WorkerProcess; ignored by this example.
    #[arg(long, default_value = "")]
    source_id: String,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "msgpack")]
    encoding: String,
}

struct MinimalHandler;

impl WorkerHandler for MinimalHandler {
    fn handle_method(
        &self,
        req_id: &str,
        method: &str,
        params: Option<Value>,
        event_tx: EventSender,
        _registry: Arc<JobRegistry>,
    ) -> WireResponse {
        if method == "example_events" {
            let count = params
                .and_then(|p| p.get("count").and_then(|v| v.as_u64()))
                .unwrap_or(300)
                .min(1000);
            tokio::task::spawn_blocking(move || {
                for index in 0..count {
                    if event_tx
                        .blocking_reliable(forge_worker_sdk::protocol::WireEvent {
                            event_type: "record".into(),
                            job_id: "example".into(),
                            payload: Some(serde_json::json!({"index":index})),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            });
            return forge_worker_sdk::ok_response(req_id, serde_json::json!({"started":true}));
        }
        unknown_method(req_id, method)
    }

    fn worker_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn features(&self) -> Vec<String> {
        vec!["example.minimal".into()]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
        )
        .init();

    let encoding: Encoding = args.encoding.parse().unwrap();
    run_worker(&args.socket, MinimalHandler, encoding).await
}
