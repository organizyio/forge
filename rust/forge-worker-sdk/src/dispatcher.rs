//! Product-extension point and control-plane dispatcher.
//!
//! ## How it works
//!
//! 1. The product worker implements [`WorkerHandler`] — only the product-specific
//!    RPC methods (`start_scan`, etc.) need to be handled there.
//! 2. [`BaseDispatcher`] wraps the handler and takes care of all Forge control-plane
//!    methods automatically: `ping`, `health`, `capabilities`, `shutdown`,
//!    `cancel_job`, `job_status`.
//! 3. The server calls `BaseDispatcher::dispatch` for every inbound request.
//!
//! ## Implementing a handler
//!
//! ```rust,ignore
//! struct MyHandler { source_id: String }
//!
//! impl WorkerHandler for MyHandler {
//!     fn worker_version(&self) -> &str { env!("CARGO_PKG_VERSION") }
//!     fn features(&self) -> Vec<String> { vec!["my.feature".into()] }
//!
//!     fn handle_method(&self, req_id: &str, method: &str, params: Option<Value>,
//!                      event_tx: EventSender, registry: Arc<JobRegistry>) -> WireResponse {
//!         match method {
//!             "start_scan" => { /* spawn task, register job … */ todo!() }
//!             _ => forge_worker_sdk::dispatcher::unknown_method(req_id, method),
//!         }
//!     }
//! }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
#[cfg(not(test))]
use std::time::Duration;

use serde_json::Value;
use tracing::info;

use crate::framing::Encoding;
use crate::job_registry::{EventSender, JobRegistry};
use crate::protocol::{ErrorPayload, WireRequest, WireResponse};

const DEFAULT_SHUTDOWN_DELAY_MS: u64 = 100;
const MAX_SHUTDOWN_DELAY_MS: u64 = 5 * 60 * 1000;

// ─── EXTENSION TRAIT ─────────────────────────────────────────────────────────

/// Implemented by product workers to handle product-specific RPC methods.
///
/// All Forge control-plane methods (`ping`, `health`, `capabilities`, `shutdown`,
/// `cancel_job`, `job_status`) are handled automatically by [`BaseDispatcher`].
pub trait WorkerHandler: Send + Sync + 'static {
    /// Dispatch a product-specific RPC method.
    ///
    /// - `req_id`   — request ID to echo in the response.
    /// - `method`   — RPC method name (e.g. `"start_scan"`).
    /// - `params`   — raw JSON params from the Go supervisor, or `None`.
    /// - `event_tx` — channel to stream [`crate::protocol::WireEvent`]s back to Go.
    /// - `registry` — shared job registry; use to register, track, and cancel jobs.
    fn handle_method(
        &self,
        req_id:   &str,
        method:   &str,
        params:   Option<Value>,
        event_tx: EventSender,
        registry: Arc<JobRegistry>,
    ) -> WireResponse;

    /// Semantic version string for this worker binary (reported in `health` / `capabilities`).
    fn worker_version(&self) -> &str;

    /// Feature flags advertised via the `capabilities` response.
    fn features(&self) -> Vec<String>;

    /// Maximum concurrent jobs this worker accepts.  Default: 1.
    fn max_concurrent_jobs(&self) -> u32 { 1 }
}

// ─── BASE DISPATCHER ─────────────────────────────────────────────────────────

/// Wraps a [`WorkerHandler`] with automatic control-plane handling.
pub struct BaseDispatcher<H: WorkerHandler> {
    pub handler:   H,
    pub registry:  Arc<JobRegistry>,
    negotiated_encoding: Encoding,
    start_time:    Instant,
    accepting:     AtomicBool,
}

impl<H: WorkerHandler> BaseDispatcher<H> {
    pub fn new(handler: H, negotiated_encoding: Encoding) -> Self {
        Self {
            handler,
            registry:   Arc::new(JobRegistry::new()),
            negotiated_encoding,
            start_time: Instant::now(),
            accepting:  AtomicBool::new(true),
        }
    }

    /// Dispatch a single request.  Called by the server for every inbound frame.
    pub async fn dispatch(&self, req: WireRequest, event_tx: EventSender) -> WireResponse {
        let id     = req.id.clone();
        let method = req.method.as_str();
        let params = req.params;

        match method {
            "ping" => ok_response(&id, serde_json::json!({"pong": true})),

            "health" => {
                let active = self.registry.active_count();
                let status = if active > 0 { "busy" } else { "ok" };
                ok_response(&id, serde_json::json!({
                    "status":      status,
                    "active_jobs": active,
                    "uptime_secs": self.start_time.elapsed().as_secs(),
                    "pid":         std::process::id(),
                    "version":     self.handler.worker_version(),
                }))
            }

            "capabilities" => ok_response(&id, serde_json::json!({
                "version":              self.handler.worker_version(),
                "protocol_version":     1,
                "features":             self.handler.features(),
                "max_concurrent_jobs":  self.handler.max_concurrent_jobs(),
                "encoding":             self.negotiated_encoding.wire_name(),
            })),

            "shutdown" => {
                self.accepting.store(false, Ordering::SeqCst);
                let delay_ms = shutdown_delay_ms(&params);
                info!(delay_ms, "shutdown requested");
                // In tests, skip `process::exit` so fixture/contract tests can assert the response.
                #[cfg(not(test))]
                {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        std::process::exit(0);
                    });
                }
                ok_response(&id, serde_json::json!({"bye": true}))
            }

            "cancel_job" => {
                let job_id = str_param(&params, "job_id").unwrap_or_default();
                if self.registry.cancel(job_id) {
                    ok_response(&id, serde_json::json!({"cancelled": true, "job_id": job_id}))
                } else {
                    err_response(&id, "JOB_NOT_FOUND", &format!("job {job_id} not found"))
                }
            }

            "job_status" => {
                let job_id = str_param(&params, "job_id").unwrap_or_default();
                match self.registry.status(job_id) {
                    Some(s) => ok_response(&id, serde_json::to_value(s).unwrap_or_default()),
                    None    => err_response(&id, "JOB_NOT_FOUND", &format!("job {job_id} not found")),
                }
            }

            // Everything else is product-specific.
            _ => self.handler.handle_method(&id, method, params, event_tx, self.registry.clone()),
        }
    }
}

// ─── RESPONSE HELPERS ────────────────────────────────────────────────────────

/// Convenience: build a successful `WireResponse` with a JSON payload.
pub fn ok_response(req_id: &str, payload: Value) -> WireResponse {
    WireResponse {
        id:      req_id.to_owned(),
        ok:      true,
        error:   None,
        payload: Some(payload),
    }
}

/// Convenience: build an error `WireResponse`.
pub fn err_response(req_id: &str, code: &str, message: &str) -> WireResponse {
    WireResponse {
        id:      req_id.to_owned(),
        ok:      false,
        error:   Some(ErrorPayload { code: code.to_owned(), message: message.to_owned(), detail: String::new() }),
        payload: None,
    }
}

/// Convenience: return an `UNKNOWN_METHOD` error response.
pub fn unknown_method(req_id: &str, method: &str) -> WireResponse {
    err_response(req_id, "UNKNOWN_METHOD", &format!("unknown method: {method}"))
}

// ─── INTERNAL HELPERS ────────────────────────────────────────────────────────

fn str_param<'a>(params: &'a Option<Value>, key: &str) -> Option<&'a str> {
    params.as_ref()?.get(key)?.as_str()
}

fn shutdown_delay_ms(params: &Option<Value>) -> u64 {
    let Some(v) = params.as_ref().and_then(|p| p.get("delay_ms")) else {
        return DEFAULT_SHUTDOWN_DELAY_MS;
    };
    if let Some(u) = v.as_u64() {
        return u.min(MAX_SHUTDOWN_DELAY_MS);
    }
    if let Some(f) = v.as_f64() {
        if f.is_finite() && f >= 0.0 {
            return f.min(MAX_SHUTDOWN_DELAY_MS as f64) as u64;
        }
    }
    DEFAULT_SHUTDOWN_DELAY_MS
}
