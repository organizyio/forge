//! Wire protocol types shared with the Go Forge supervisor.

mod control;
mod envelope;

pub use control::{
    decode_pong_payload, CancelJobParams, CapabilitiesResponse, HealthResponse, JobStatusParams,
    PongPayload,
};
pub use envelope::{WireEvent, WireRequest, WireResponse};

use serde::{Deserialize, Serialize};

/// Go: `ErrorPayload` in `go/transport`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}
