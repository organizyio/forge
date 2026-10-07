//! Bounded, connection-owned event delivery. Acknowledgments release credit only;
//! reconnect requires a new scan generation, not replay from this in-memory queue.
use crate::framing::{Encoding, Frame};
use crate::protocol::WireEvent;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub const MAX_EVENTS: usize = 256;
pub const MAX_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_EVENT_BYTES: usize = 1024 * 1024;
const ADMISSION: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error("reliable delivery not negotiated")]
    NotNegotiated,
    #[error("event exceeds delivery size budget")]
    Oversized,
    #[error("event delivery interrupted")]
    Interrupted,
    #[error("invalid delivery acknowledgment")]
    InvalidAck,
}
#[derive(Default)]
struct JobCredit {
    sequence: u64,
    records: u64,
    terminal: bool,
    acknowledged: u64,
    pending: BTreeMap<u64, usize>,
}
#[derive(Default)]
struct State {
    id: Option<String>,
    closed: bool,
    count: usize,
    bytes: usize,
    jobs: HashMap<String, JobCredit>,
    progress: HashMap<String, (WireEvent, usize)>,
    cancelled: HashSet<String>,
    dropped: u64,
    failures: u64,
}
pub struct Delivery {
    tx: mpsc::Sender<Frame>,
    state: Mutex<State>,
    changed: Condvar,
    encoding: Encoding,
    emit: Mutex<()>,
}

/// Sender API replacing raw unbounded channels. The conversion exists for legacy
/// test/embedding adapters; production connections always use bounded delivery.
#[derive(Clone)]
pub enum EventSender {
    Legacy(mpsc::UnboundedSender<WireEvent>),
    Bounded(Arc<Delivery>),
}
impl From<mpsc::UnboundedSender<WireEvent>> for EventSender {
    fn from(tx: mpsc::UnboundedSender<WireEvent>) -> Self {
        Self::Legacy(tx)
    }
}
impl EventSender {
    pub fn send(&self, event: WireEvent) -> Result<(), DeliveryError> {
        match self {
            Self::Legacy(tx) => tx.send(event).map_err(|_| DeliveryError::Interrupted),
            Self::Bounded(d) => d.send(event, false),
        }
    }
    pub fn blocking_reliable(&self, event: WireEvent) -> Result<(), DeliveryError> {
        match self {
            Self::Bounded(d) => d.send(event, true),
            _ => Err(DeliveryError::NotNegotiated),
        }
    }
    pub async fn reliable(&self, event: WireEvent) -> Result<(), DeliveryError> {
        let sender = self.clone();
        tokio::task::spawn_blocking(move || sender.blocking_reliable(event))
            .await
            .map_err(|_| DeliveryError::Interrupted)?
    }
}
impl Delivery {
    pub fn new(encoding: Encoding) -> (Arc<Self>, mpsc::Receiver<Frame>) {
        let (tx, rx) = mpsc::channel(MAX_EVENTS);
        (
            Arc::new(Self {
                tx,
                state: Mutex::new(State::default()),
                changed: Condvar::new(),
                encoding,
                emit: Mutex::new(()),
            }),
            rx,
        )
    }
    pub fn sender(self: &Arc<Self>) -> EventSender {
        EventSender::Bounded(self.clone())
    }
    pub fn configure(&self, id: &str) -> Result<(), DeliveryError> {
        let mut s = self.state.lock().unwrap();
        if s.closed || s.id.is_some() || id.is_empty() || id.len() > 128 {
            return Err(DeliveryError::NotNegotiated);
        }
        s.id = Some(id.into());
        Ok(())
    }
    pub fn close(&self) {
        let mut s = self.state.lock().unwrap();
        s.closed = true;
        self.changed.notify_all();
    }
    pub fn cancel(&self, job: &str) {
        self.state.lock().unwrap().cancelled.insert(job.into());
        self.changed.notify_all();
    }
    pub fn stats(&self) -> serde_json::Value {
        let s = self.state.lock().unwrap();
        serde_json::json!({"queued_events":s.count,"queued_bytes":s.bytes,"progress_dropped":s.dropped,"delivery_failures":s.failures})
    }
    fn size(&self, value: &impl serde::Serialize) -> Result<usize, DeliveryError> {
        match self.encoding {
            Encoding::Json => serde_json::to_vec(value)
                .map(|v| v.len())
                .map_err(|_| DeliveryError::Oversized),
            Encoding::Msgpack => rmp_serde::to_vec_named(value)
                .map(|v| v.len())
                .map_err(|_| DeliveryError::Oversized),
        }
    }
    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
    fn send(&self, mut event: WireEvent, reliable: bool) -> Result<(), DeliveryError> {
        let _ordered = if reliable {
            Some(self.emit.lock().unwrap())
        } else {
            None
        };
        let deadline = Instant::now() + ADMISSION;
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(DeliveryError::Interrupted);
        }
        let job = event.job_id.clone();
        let terminal = matches!(
            event.event_type.as_str(),
            "job_completed" | "job_failed" | "job_cancelled"
        );
        if !reliable && event.event_type == "job_progress" {
            let size = self.size(&event)?;
            if size > MAX_EVENT_BYTES {
                return Err(DeliveryError::Oversized);
            }
            if let Some((_, previous)) = s.progress.get(&job) {
                let previous = *previous;
                if s.bytes - previous + size <= MAX_BYTES {
                    s.bytes = s.bytes - previous + size;
                    s.progress.insert(job, (event, size));
                }
                s.dropped += 1;
                return Ok(());
            }
        }
        let (mut frame, size, sequence) = if reliable {
            let id = s.id.clone().ok_or(DeliveryError::NotNegotiated)?;
            // Bound retained sequence tombstones too. A fresh connection is required
            // after this many jobs; silently forgetting a sequence would permit replay.
            if !s.jobs.contains_key(&job) && s.jobs.len() >= 4096 {
                s.closed = true;
                s.failures += 1;
                self.changed.notify_all();
                return Err(DeliveryError::Interrupted);
            }
            if s.jobs.get(&job).is_some_and(|j| j.terminal) {
                return Err(DeliveryError::Interrupted);
            }
            let sequence = s.jobs.get(&job).map_or(1, |j| j.sequence + 1);
            if terminal {
                let records = s.jobs.get(&job).map_or(0, |j| j.records);
                let payload = event.payload.get_or_insert_with(|| serde_json::json!({}));
                let object = payload.as_object_mut().ok_or(DeliveryError::Interrupted)?;
                object.insert("final_sequence".into(), sequence.into());
                object.insert("record_count".into(), records.into());
            }
            let wrapped = crate::protocol::SequencedEvent {
                event: event.clone(),
                delivery_id: id,
                sequence,
            };
            let size = self.size(&wrapped)?;
            (Frame::ReliableEvent(wrapped), size, sequence)
        } else {
            let size = self.size(&event)?;
            (Frame::Event(event.clone()), size, 0)
        };
        if size > MAX_EVENT_BYTES {
            if reliable {
                s.closed = true;
                s.failures += 1;
                self.changed.notify_all();
            }
            return Err(DeliveryError::Oversized);
        }
        loop {
            if s.closed || (reliable && !terminal && s.cancelled.contains(&job)) {
                return Err(DeliveryError::Interrupted);
            }
            if s.count < MAX_EVENTS && s.bytes + size <= MAX_BYTES && self.tx.capacity() > 0 {
                break;
            }
            if !reliable {
                s.dropped += 1;
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                s.closed = true;
                s.failures += 1;
                self.changed.notify_all();
                return Err(DeliveryError::Interrupted);
            }
            s = self
                .changed
                .wait_timeout(s, remaining.min(Duration::from_millis(50)))
                .unwrap()
                .0;
        }
        if !reliable && event.event_type == "job_progress" {
            s.progress.insert(job.clone(), (event.clone(), size));
            let mut token = event.clone();
            token.payload = None;
            frame = Frame::Event(token);
        }
        if self.tx.try_send(frame).is_err() {
            s.closed = true;
            s.failures += 1;
            self.changed.notify_all();
            return Err(DeliveryError::Interrupted);
        }
        s.count += 1;
        s.bytes += size;
        if reliable {
            let j = s.jobs.entry(job).or_default();
            j.sequence = sequence;
            j.terminal = terminal;
            if event.event_type == "record" {
                j.records += 1;
            }
            j.pending.insert(sequence, size);
        }
        Ok(())
    }
    /// Resolve a queued progress token to its newest bounded value.
    pub fn latest_progress(&self, event: WireEvent) -> WireEvent {
        if event.event_type != "job_progress" {
            return event;
        }
        self.state
            .lock()
            .unwrap()
            .progress
            .remove(&event.job_id)
            .map(|(latest, _)| latest)
            .unwrap_or(event)
    }
    pub fn progress_written(&self, event: &WireEvent) {
        if let Ok(size) = self.size(event) {
            let mut s = self.state.lock().unwrap();
            s.count = s.count.saturating_sub(1);
            s.bytes = s.bytes.saturating_sub(size);
            self.changed.notify_all();
        }
    }
    pub fn ack(&self, id: &str, job: &str, through: u64) -> Result<(), DeliveryError> {
        let mut s = self.state.lock().unwrap();
        if s.closed || s.id.as_deref() != Some(id) {
            return Err(DeliveryError::InvalidAck);
        }
        let j = s.jobs.get_mut(job).ok_or(DeliveryError::InvalidAck)?;
        if through > j.sequence || through < j.acknowledged {
            return Err(DeliveryError::InvalidAck);
        }
        let keys: Vec<u64> = j.pending.range(..=through).map(|(k, _)| *k).collect();
        let count = keys.len();
        let bytes: usize = keys
            .into_iter()
            .map(|k| j.pending.remove(&k).unwrap())
            .sum();
        j.acknowledged = through;
        s.count -= count;
        s.bytes -= bytes;
        self.changed.notify_all();
        Ok(())
    }
}
