//! Organizyio **Forge** — Rust-side building blocks for local worker processes.
//!
//! This crate provides everything a product worker binary needs to participate
//! in the Forge IPC protocol with a Go supervisor.  It intentionally contains
//! no Archivist / UFIS scan types — those live in `organizy-worker`.
//!
//! ## Quick-start
//!
//! ```rust,ignore
//! use forge_worker_sdk::{dispatcher::{WorkerHandler, ok_response, unknown_method}, framing::Encoding,
//!                 job_registry::{EventSender, JobRegistry}, protocol::WireResponse, server};
//! use std::sync::Arc;
//!
//! struct MyHandler;
//! impl WorkerHandler for MyHandler {
//!     fn worker_version(&self) -> &str { env!("CARGO_PKG_VERSION") }
//!     fn features(&self) -> Vec<String> { vec![] }
//!     fn handle_method(&self, req_id: &str, method: &str, params: Option<serde_json::Value>,
//!                      _event_tx: EventSender, _registry: Arc<JobRegistry>) -> WireResponse {
//!         unknown_method(req_id, method)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     server::run_worker("/tmp/my-worker.sock", MyHandler, Encoding::Msgpack).await
//! }
//! ```
//!
//! ## Modules
//!
//! - [`framing`]      — `FrameCodec` (Tokio `Decoder`/`Encoder`) + `Frame` enum.
//! - [`protocol`]     — `WireRequest` / `WireResponse` / `WireEvent` + control types.
//! - [`job_registry`] — Thread-safe job tracking with cancel tokens and event routing.
//! - [`dispatcher`]   — `WorkerHandler` trait + `BaseDispatcher` (control-plane handling).
//! - [`server`]       — `run_worker` entry point; Unix socket + Windows named pipe.
//! - [`prelude`]      — `run_worker`, `WorkerHandler`, `Encoding`, `ErrorPayload` for quick imports.

pub mod dispatcher;
pub mod framing;
pub mod job_registry;
pub mod prelude;
pub mod protocol;
pub mod server;

// Flat re-exports for the most commonly needed items
pub use framing::{Encoding, Frame, FrameCodec, KIND_EVENT, KIND_REQUEST, KIND_RESPONSE, MAX_FRAME_PAYLOAD};
pub use job_registry::{cancel_pair, CancelSignal, CancelToken, EventSender, JobRegistry, JobState, JobStatus};
pub use dispatcher::{err_response, ok_response, unknown_method, WorkerHandler};
pub use server::run_worker;
