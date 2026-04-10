//! Control-plane RPC handling via [`forge_worker_sdk::dispatcher::BaseDispatcher`].
//! Never dispatch `shutdown` here — it triggers `process::exit` in the SDK.

use std::sync::Arc;

use forge_worker_sdk::dispatcher::{BaseDispatcher, WorkerHandler};
use forge_worker_sdk::Encoding;
use forge_worker_sdk::job_registry::{EventSender, JobRegistry};
use forge_worker_sdk::protocol::{WireRequest, WireResponse};
use forge_worker_sdk::unknown_method;
use serde_json::{json, Value};
use tokio::sync::mpsc;

struct StubHandler;

impl WorkerHandler for StubHandler {
    fn handle_method(
        &self,
        req_id: &str,
        method: &str,
        _params: Option<Value>,
        _event_tx: EventSender,
        _registry: Arc<JobRegistry>,
    ) -> WireResponse {
        unknown_method(req_id, method)
    }

    fn worker_version(&self) -> &str {
        "9.9.9-test"
    }

    fn features(&self) -> Vec<String> {
        vec!["test.stub".into()]
    }
}

fn req(id: &str, method: &str, params: Option<Value>) -> WireRequest {
    WireRequest {
        id: id.to_owned(),
        method: method.to_owned(),
        params,
    }
}

#[tokio::test]
async fn ping_returns_pong() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d.dispatch(req("1", "ping", None), tx).await;
    assert!(r.ok);
    let p = r.payload.expect("payload");
    assert_eq!(p.get("pong"), Some(&json!(true)));
}

#[tokio::test]
async fn health_reports_version_and_counts() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d.dispatch(req("h", "health", None), tx).await;
    assert!(r.ok);
    let p = r.payload.expect("payload");
    assert_eq!(p.get("status"), Some(&json!("ok")));
    assert_eq!(p.get("active_jobs"), Some(&json!(0)));
    assert_eq!(p.get("version"), Some(&json!("9.9.9-test")));
    assert!(p.get("uptime_secs").is_some());
    assert!(p.get("pid").is_some());
}

#[tokio::test]
async fn capabilities_reflect_handler() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d.dispatch(req("c", "capabilities", None), tx).await;
    assert!(r.ok);
    let p = r.payload.expect("payload");
    assert_eq!(p.get("protocol_version"), Some(&json!(1)));
    assert_eq!(p.get("version"), Some(&json!("9.9.9-test")));
    assert_eq!(
        p.get("features"),
        Some(&json!(["test.stub"]))
    );
    assert_eq!(p.get("max_concurrent_jobs"), Some(&json!(1)));
    assert_eq!(p.get("encoding"), Some(&json!("msgpack")));
}

#[tokio::test]
async fn capabilities_encoding_matches_negotiated_json() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Json);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d.dispatch(req("c", "capabilities", None), tx).await;
    assert!(r.ok);
    let p = r.payload.expect("payload");
    assert_eq!(p.get("encoding"), Some(&json!("json")));
}

#[tokio::test]
async fn cancel_job_missing_is_job_not_found() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d
        .dispatch(
            req(
                "x",
                "cancel_job",
                Some(json!({ "job_id": "nope" })),
            ),
            tx,
        )
        .await;
    assert!(!r.ok);
    let e = r.error.expect("error");
    assert_eq!(e.code, "JOB_NOT_FOUND");
}

#[tokio::test]
async fn job_status_missing_is_job_not_found() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d
        .dispatch(
            req(
                "y",
                "job_status",
                Some(json!({ "job_id": "missing" })),
            ),
            tx,
        )
        .await;
    assert!(!r.ok);
    let e = r.error.expect("error");
    assert_eq!(e.code, "JOB_NOT_FOUND");
}

#[tokio::test]
async fn product_method_unknown_via_stub() {
    let d = BaseDispatcher::new(StubHandler, Encoding::Msgpack);
    let (tx, _rx) = mpsc::unbounded_channel();
    let r = d.dispatch(req("z", "start_scan", None), tx).await;
    assert!(!r.ok);
    let e = r.error.expect("error");
    assert_eq!(e.code, "UNKNOWN_METHOD");
    assert!(e.message.contains("start_scan"));
}
