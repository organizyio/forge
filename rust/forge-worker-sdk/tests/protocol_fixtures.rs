//! Contract tests: JSON under `forge/protocol/fixtures/` must match
//! [`forge_worker_sdk::dispatcher::BaseDispatcher::dispatch`] for control-plane RPCs.
//!
//! Naming convention:
//! - `{method}-request.json` + `{method}-response.json` for success paths.
//! - `{method}-error-<case>.json` for error responses; the request is the same
//!   `{method}-request.json` unless a dedicated error request file exists.

use std::path::PathBuf;

use forge_worker_sdk::dispatcher::{BaseDispatcher, WorkerHandler};
use forge_worker_sdk::job_registry::{Job, JobRegistry};
use forge_worker_sdk::protocol::{WireRequest, WireResponse};
use forge_worker_sdk::{cancel_pair, Encoding, EventSender};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Matches `forge/protocol/fixtures` (`hash.xxh64`, `0.1.0`, msgpack); see repository spec.
struct FixtureHandler;

impl WorkerHandler for FixtureHandler {
    fn handle_method(
        &self,
        req_id: &str,
        method: &str,
        _params: Option<Value>,
        _event_tx: EventSender,
        _registry: std::sync::Arc<JobRegistry>,
    ) -> WireResponse {
        forge_worker_sdk::unknown_method(req_id, method)
    }

    fn worker_version(&self) -> &str {
        "0.1.0"
    }

    fn features(&self) -> Vec<String> {
        vec!["hash.xxh64".into()]
    }
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../protocol/fixtures")
}

fn read_fixture(name: &str) -> String {
    let p = fixtures_dir().join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn parse_request(json: &str) -> WireRequest {
    serde_json::from_str(json).expect("WireRequest")
}

/// Drop volatile `health` payload keys before comparing JSON (see spec / fixtures).
fn normalize_health_response(mut v: Value) -> Value {
    if let Some(obj) = v.get_mut("payload").and_then(|p| p.as_object_mut()) {
        obj.remove("uptime_secs");
        obj.remove("pid");
    }
    v
}

fn response_to_value(r: &WireResponse) -> Value {
    serde_json::to_value(r).expect("serialize WireResponse")
}

async fn dispatch(d: &BaseDispatcher<FixtureHandler>, req: WireRequest) -> WireResponse {
    let (tx, _rx) = mpsc::unbounded_channel();
    d.dispatch(req, tx).await
}

#[tokio::test]
async fn fixture_ping() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("ping-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("ping-response.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_health() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("health-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("health-response.json")).unwrap();
    assert_eq!(
        normalize_health_response(response_to_value(&actual)),
        normalize_health_response(expected)
    );
}

#[tokio::test]
async fn fixture_capabilities_msgpack() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("capabilities-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("capabilities-response.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_shutdown() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("shutdown-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("shutdown-response.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_cancel_job_ok() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let (event_tx, _rx) = mpsc::unbounded_channel();
    let (cancel_tx, _sig) = cancel_pair();
    let job = Job::new("job-123".into(), event_tx, cancel_tx);
    d.registry.register(job).expect("register");
    let req = parse_request(&read_fixture("cancel_job-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("cancel_job-response.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_cancel_job_not_found() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("cancel_job-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value =
        serde_json::from_str(&read_fixture("cancel_job-error-not-found.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_job_status_ok() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let (event_tx, _rx) = mpsc::unbounded_channel();
    let (cancel_tx, _sig) = cancel_pair();
    let job = Job::new("job-123".into(), event_tx, cancel_tx);
    d.registry.register(job).expect("register");
    d.registry.set_running("job-123");
    d.registry.update_progress(
        "job-123",
        json!({
            "phase": "walk",
            "items_done": 5000
        }),
    );
    let req = parse_request(&read_fixture("job_status-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value = serde_json::from_str(&read_fixture("job_status-response.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}

#[tokio::test]
async fn fixture_job_status_not_found() {
    let d = BaseDispatcher::new(FixtureHandler, Encoding::Msgpack);
    let req = parse_request(&read_fixture("job_status-request.json"));
    let actual = dispatch(&d, req).await;
    let expected: Value =
        serde_json::from_str(&read_fixture("job_status-error-not-found.json")).unwrap();
    assert_eq!(response_to_value(&actual), expected);
}
