//! Shared control-plane request params and response shapes (no UFIS / scan job types).
//!
//! Product workers extend this layer with domain methods (e.g. `start_scan`) and
//! deserialize `WireRequest.params` into their own structs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Request params (maps sent as Go `params`) ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelJobParams {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusParams {
    pub job_id: String,
}

// ─── Response payload fragments (decode from `WireResponse.payload`) ─────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub active_jobs: i32,
    pub uptime_secs: i64,
    pub pid: i32,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesResponse {
    pub version: String,
    pub protocol_version: i32,
    pub features: Vec<String>,
    pub max_concurrent_jobs: i32,
    pub encoding: String,
}

/// Use when the worker only needs to echo structured JSON inside `payload`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongPayload {
    #[serde(default)]
    pub pong: bool,
}

/// Decode `WireResponse.payload` for `ping` (Go expects a JSON object inside payload).
pub fn decode_pong_payload(v: &Value) -> Option<()> {
    let _: PongPayload = serde_json::from_value(v.clone()).ok()?;
    Some(())
}
