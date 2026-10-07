# Reliable events v1 (opt in)

Workers advertise `reliable_events_v1`. The consumer calls
`configure_event_delivery` with a unique `delivery_id` before starting jobs.
Events retain frame kind 3 and their existing fields, adding `delivery_id` and
`sequence`. Sequence starts at 1 independently for each job. Completion, failure
and cancellation payloads carry `final_sequence` and `record_count` (the number
of authoritative `record` events). Consumers must validate these before success.

Call `ack_events` with delivery ID, job ID and `through_sequence` only after
committing all events through that sequence. Invalid, future or mismatched
acknowledgments return `INVALID_ACK`. Acknowledgment releases in-memory credit;
it provides no crash-surviving replay. A disconnected generation is interrupted
and a retry must create a new job.

Production connections bound queued/unacknowledged events at 256 items and
16 MiB serialized bytes, individual events at 1 MiB, responses at 32 items and
1 MiB, and dispatched product requests at 16. Reliable admission waits at most
30 seconds; socket writes wait at most 5 seconds. Control responses take priority
between frames. Admission failure closes the stream. At most 4096 job sequence
tombstones are retained per connection; reaching that limit requires a new
connection instead of forgetting replay evidence. `delivery_stats` reports
outstanding events/bytes, best-effort drops and delivery failures.

Rust `EventSender::blocking_reliable` is for blocking scanner threads;
`EventSender::reliable` is the asynchronous interface. Both return errors. Legacy
`send` and registry `emit` remain best effort and may drop under pressure.
The raw unbounded sender conversion exists for compatibility with embedding
adapters; it does not provide reliable delivery.

Go `Conn.ConfigureReliable` returns a bounded subscription. Consume in sequence,
commit storage, then call `Commit`. It rejects out-of-order acknowledgments.
`Close` terminates the connection. Legacy callbacks and channel-bus subscriptions
remain lossy; use their drop counters and cancellable subscriptions.

This protocol remains opt in. Organizy must complete durable SQLite receipt,
projection and terminal validation before enabling authoritative scanning.
