use forge_worker_sdk::cancel_pair;
use forge_worker_sdk::job_registry::{Job, JobRegistry, Transition};
use tokio::sync::mpsc;
fn registry() -> JobRegistry {
    let r = JobRegistry::new();
    let (tx, _rx) = mpsc::unbounded_channel();
    let (c, _cr) = cancel_pair();
    r.register(Job::new("j".into(), tx, c)).unwrap();
    assert_eq!(r.try_running("j"), Transition::Accepted);
    r
}
#[test]
fn cancellation_is_requested_until_runner_confirms() {
    let r = registry();
    r.cancel("j");
    assert_eq!(r.status("j").unwrap().state, "running");
    assert!(r.status("j").unwrap().cancel_requested);
    assert_eq!(r.complete("j", serde_json::json!({})), Transition::Conflict);
    assert_eq!(r.confirm_cancelled("j"), Transition::Accepted);
    r.set_running("j");
    r.set_failed("j", "late".into());
    r.set_completed("j", serde_json::json!({}));
    assert_eq!(r.status("j").unwrap().state, "cancelled");
    assert_eq!(r.total_completed(), 1);
}
#[test]
fn terminal_accounting_is_idempotent() {
    let r = registry();
    assert_eq!(
        r.complete("missing", serde_json::json!({})),
        Transition::Missing
    );
    assert_eq!(r.total_completed(), 0);
    assert_eq!(r.complete("j", serde_json::json!({})), Transition::Accepted);
    assert_eq!(
        r.complete("j", serde_json::json!({})),
        Transition::Duplicate
    );
    assert_eq!(r.fail("j", "late".into()), Transition::Conflict);
    assert_eq!(r.total_completed(), 1);
}
#[test]
fn pending_cancel_prevents_start() {
    let r = JobRegistry::new();
    let (tx, _rx) = mpsc::unbounded_channel();
    let (c, _cr) = cancel_pair();
    r.register(Job::new("j".into(), tx, c)).unwrap();
    r.cancel("j");
    r.cancel("j");
    assert_eq!(r.try_running("j"), Transition::Conflict);
    assert_eq!(r.total_completed(), 1);
}
#[test]
fn concurrent_cancel_and_complete_has_one_valid_outcome() {
    for _ in 0..100 {
        let r = registry();
        let other = r.clone();
        let t = std::thread::spawn(move || other.cancel("j"));
        r.complete("j", serde_json::json!({}));
        t.join().unwrap();
        let s = r.status("j").unwrap();
        if s.cancel_requested {
            r.confirm_cancelled("j");
        }
        assert_eq!(r.total_completed(), 1);
        assert!(matches!(
            r.status("j").unwrap().state.as_str(),
            "completed" | "cancelled"
        ));
    }
}
