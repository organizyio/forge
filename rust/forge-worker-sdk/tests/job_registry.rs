//! [`JobRegistry`] public API: register, status, cancel, active count, duplicates.

use forge_worker_sdk::cancel_pair;
use forge_worker_sdk::job_registry::{Job, JobRegistry, JobState};
use forge_worker_sdk::protocol::WireEvent;
use tokio::sync::mpsc;

#[test]
fn register_and_active_count() {
    let reg = JobRegistry::new();
    let (ev_tx, _ev_rx) = mpsc::unbounded_channel();
    let (cancel_tx, _cancel_rx) = cancel_pair();
    let job = Job::new("job-a".into(), ev_tx, cancel_tx);
    reg.register(job).expect("register");
    assert_eq!(reg.active_count(), 1);
    let st = reg.status("job-a").expect("status");
    assert_eq!(st.job_id, "job-a");
    assert_eq!(st.state, "pending");
}

#[test]
fn duplicate_register_errors() {
    let reg = JobRegistry::new();
    let (ev_tx, _ev_rx) = mpsc::unbounded_channel();
    let (c1, _r1) = cancel_pair();
    let (c2, _r2) = cancel_pair();
    reg.register(Job::new("dup".into(), ev_tx.clone(), c1)).unwrap();
    let err = reg.register(Job::new("dup".into(), ev_tx, c2)).unwrap_err();
    assert!(err.contains("already registered"));
}

#[test]
fn cancel_marks_cancelled_and_drops_active() {
    let reg = JobRegistry::new();
    let (ev_tx, _ev_rx) = mpsc::unbounded_channel();
    let (cancel_tx, _cancel_rx) = cancel_pair();
    reg.register(Job::new("j".into(), ev_tx, cancel_tx)).unwrap();
    assert!(reg.cancel("j"));
    assert_eq!(reg.active_count(), 0);
    let st = reg.status("j").expect("still in map");
    assert_eq!(st.state, JobState::Cancelled.to_string());
}

#[test]
fn emit_delivers_when_connected() {
    let reg = JobRegistry::new();
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let (cancel_tx, _cancel_rx) = cancel_pair();
    reg.register(Job::new("e".into(), ev_tx, cancel_tx)).unwrap();
    let ev = WireEvent {
        event_type: "progress".into(),
        job_id: "e".into(),
        payload: Some(serde_json::json!({ "n": 1 })),
    };
    reg.emit("e", ev.clone());
    assert_eq!(ev_rx.try_recv().unwrap(), ev);
}
