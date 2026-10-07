use forge_worker_sdk::delivery::{Delivery, DeliveryError, MAX_EVENTS};
use forge_worker_sdk::framing::{Encoding, Frame};
use forge_worker_sdk::protocol::WireEvent;
fn event() -> WireEvent {
    WireEvent {
        event_type: "record".into(),
        job_id: "j".into(),
        payload: Some(serde_json::json!({"path":"file"})),
    }
}
#[test]
fn acknowledgment_releases_bounded_credit() {
    for encoding in [Encoding::Json, Encoding::Msgpack] {
        let (d, mut rx) = Delivery::new(encoding);
        d.configure("id").unwrap();
        let sender = d.sender();
        for n in 1..=MAX_EVENTS {
            sender.blocking_reliable(event()).unwrap();
            match rx.try_recv().unwrap() {
                Frame::ReliableEvent(e) => assert_eq!(e.sequence, n as u64),
                _ => panic!("unsequenced"),
            }
        }
        assert_eq!(d.stats()["queued_events"], MAX_EVENTS);
        assert!(d.ack("other", "j", 1).is_err());
        assert!(d.ack("id", "j", MAX_EVENTS as u64 + 1).is_err());
        d.ack("id", "j", MAX_EVENTS as u64).unwrap();
        assert_eq!(d.stats()["queued_bytes"], 0);
        sender.blocking_reliable(event()).unwrap();
    }
}
#[test]
fn disconnect_and_cancel_wake_blocked_producers() {
    let (d, _rx) = Delivery::new(Encoding::Json);
    d.configure("id").unwrap();
    for _ in 0..MAX_EVENTS {
        d.sender().blocking_reliable(event()).unwrap();
    }
    let sender = d.sender();
    let t = std::thread::spawn(move || sender.blocking_reliable(event()));
    d.cancel("j");
    assert!(matches!(t.join().unwrap(), Err(DeliveryError::Interrupted)));
    let sender = d.sender();
    d.close();
    assert!(sender.blocking_reliable(event()).is_err());
}
#[test]
fn oversized_and_unnegotiated_events_fail_closed() {
    let (d, _rx) = Delivery::new(Encoding::Json);
    assert!(matches!(
        d.sender().blocking_reliable(event()),
        Err(DeliveryError::NotNegotiated)
    ));
    d.configure("id").unwrap();
    let mut e = event();
    e.payload = Some(serde_json::json!({"data":"x".repeat(1024*1024)}));
    assert!(matches!(
        d.sender().blocking_reliable(e),
        Err(DeliveryError::Oversized)
    ));
    assert!(d.is_closed());
}

#[test]
fn progress_is_coalesced_and_terminal_counts_records() {
    let (d, mut rx) = Delivery::new(Encoding::Json);
    d.configure("id").unwrap();
    for n in 0..1000 {
        let mut e = event();
        e.event_type = "job_progress".into();
        e.payload = Some(serde_json::json!({"seen":n}));
        d.sender().send(e).unwrap();
    }
    assert_eq!(d.stats()["queued_events"], 1);
    assert_eq!(d.stats()["progress_dropped"], 999);
    let Frame::Event(token) = rx.try_recv().unwrap() else {
        panic!("progress frame")
    };
    let latest = d.latest_progress(token);
    assert_eq!(latest.payload.as_ref().unwrap()["seen"], 999);
    d.progress_written(&latest);
    assert_eq!(d.stats()["queued_bytes"], 0);
    d.sender().blocking_reliable(event()).unwrap();
    let mut terminal = event();
    terminal.event_type = "job_completed".into();
    d.sender().blocking_reliable(terminal).unwrap();
    rx.try_recv().unwrap();
    let Frame::ReliableEvent(last) = rx.try_recv().unwrap() else {
        panic!("terminal frame")
    };
    assert_eq!(last.event.payload.as_ref().unwrap()["record_count"], 1);
    assert_eq!(last.event.payload.as_ref().unwrap()["final_sequence"], 2);
    assert!(d.sender().blocking_reliable(event()).is_err());
}
