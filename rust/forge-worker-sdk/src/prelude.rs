//! Narrow re-exports for small worker binaries: `use forge_worker_sdk::prelude::*`.
//!
//! For full control (e.g. [`crate::job_registry::JobRegistry`], response helpers),
//! import from the crate root or submodules.

pub use crate::dispatcher::WorkerHandler;
pub use crate::framing::Encoding;
pub use crate::protocol::ErrorPayload;
pub use crate::run_worker;
