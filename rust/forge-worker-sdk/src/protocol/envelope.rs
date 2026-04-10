//! Wire envelopes aligned with the Go supervisor (`go/transport`).
//!
//! **Events** must serialize as three fields: `type`, `job_id`, and nested `payload`
//! (not a flattened body), so `transport.Conn` can fill `Event.RawBody` correctly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Go: `wireRequest` — method string + optional params map/struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRequest {
    pub id: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// Go: `wireResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireResponse {
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<super::ErrorPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// Go: `wireEvent` — inner job data lives under `payload`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}
