//! Job tracking, cancel tokens, and per-connection event routing.
//!
//! The registry is product-generic: it tracks job lifecycle state and cancel
//! signals but does not care about what events look like — those are typed
//! [`WireEvent`] values that the product worker constructs and sends.
//!
//! Thread-safe via `Arc<Mutex<...>>`; the lock is held only for in-memory map
//! mutations (never across I/O), so contention is negligible.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::Value;
use tokio::sync::oneshot;
use tracing::info;

use crate::protocol::WireEvent;

// ─── CHANNEL TYPES ───────────────────────────────────────────────────────────

/// Sender half for forwarding [`WireEvent`]s to the connected Go client.
pub use crate::delivery::EventSender;
/// Cancel token: dropping *or* sending `()` signals the runner to stop.
pub type CancelToken = oneshot::Sender<()>;
/// Paired receiver end of a cancel token.
pub type CancelSignal = oneshot::Receiver<()>;

pub fn cancel_pair() -> (CancelToken, CancelSignal) {
    oneshot::channel()
}

// ─── JOB STATE ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JobState::Pending => "pending",
            JobState::Running => "running",
            JobState::Completed => "completed",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        };
        write!(f, "{}", s)
    }
}

// ─── JOB STATUS SNAPSHOT ─────────────────────────────────────────────────────

/// Returned by [`JobRegistry::status`] for `job_status` RPC queries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobStatus {
    pub job_id: String,
    pub state: String,
    #[serde(default)]
    pub cancel_requested: bool,
    /// Opaque product progress (e.g. scan counters); omitted when empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

// ─── JOB ENTRY ───────────────────────────────────────────────────────────────

pub struct Job {
    pub job_id: String,
    pub state: JobState,
    pub cancel_requested: bool,
    pub started_at: Instant,
    /// Latest product-specific progress snapshot for `job_status`.
    pub progress: Option<Value>,
    pub error_msg: Option<String>,
    /// Consumed once on cancel.
    cancel: Option<CancelToken>,
    /// Channel to the connected client's write loop.  `None` after disconnect.
    event_tx: Option<EventSender>,
}

impl Job {
    pub fn new(job_id: String, event_tx: impl Into<EventSender>, cancel: CancelToken) -> Self {
        Self {
            job_id,
            state: JobState::Pending,
            cancel_requested: false,
            started_at: Instant::now(),
            progress: None,
            error_msg: None,
            cancel: Some(cancel),
            event_tx: Some(event_tx.into()),
        }
    }

    /// Signal cancellation.  Idempotent.
    pub fn cancel(&mut self) {
        if self.state != JobState::Pending && self.state != JobState::Running {
            return;
        }
        self.cancel_requested = true;
        if let Some(tok) = self.cancel.take() {
            let _ = tok.send(());
        }
        if self.state == JobState::Pending {
            self.state = JobState::Cancelled;
        }
    }

    /// Emit an event towards the connected Go client.  Silent no-op if disconnected.
    pub fn emit(&self, ev: WireEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(ev);
        }
    }

    /// Mark client as disconnected; job continues running but stops emitting events.
    pub fn detach_client(&mut self) {
        self.event_tx = None;
    }

    pub fn duration_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }
}

/// Result of a guarded job lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Accepted,
    Duplicate,
    Conflict,
    Missing,
}

// ─── REGISTRY ────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    jobs: HashMap<String, Job>,
    total_completed: u64,
}

impl JobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new job.  Returns `Err` if a job with the same id already exists.
    pub fn register(&self, job: Job) -> Result<(), String> {
        let mut g = self.inner.lock().unwrap();
        if g.jobs.contains_key(&job.job_id) {
            return Err(format!("job {} already registered", job.job_id));
        }
        info!(job_id = %job.job_id, "job registered");
        g.jobs.insert(job.job_id.clone(), job);
        Ok(())
    }

    /// Attempts pending -> running without reviving cancelled work.
    pub fn try_running(&self, job_id: &str) -> Transition {
        let mut g = self.inner.lock().unwrap();
        let Some(j) = g.jobs.get_mut(job_id) else {
            return Transition::Missing;
        };
        if j.state == JobState::Running && !j.cancel_requested {
            return Transition::Duplicate;
        }
        if j.state != JobState::Pending || j.cancel_requested {
            return Transition::Conflict;
        }
        j.state = JobState::Running;
        Transition::Accepted
    }

    pub fn set_running(&self, job_id: &str) {
        self.try_running(job_id);
    }

    pub fn complete(&self, job_id: &str, progress: Value) -> Transition {
        self.finish(job_id, JobState::Completed, Some(progress), None)
    }
    pub fn fail(&self, job_id: &str, err: String) -> Transition {
        self.finish(job_id, JobState::Failed, None, Some(err))
    }
    pub fn confirm_cancelled(&self, job_id: &str) -> Transition {
        self.finish(job_id, JobState::Cancelled, None, None)
    }
    pub fn set_completed(&self, job_id: &str, progress: Value) {
        self.complete(job_id, progress);
    }
    pub fn set_failed(&self, job_id: &str, err: String) {
        self.fail(job_id, err);
    }

    fn finish(
        &self,
        job_id: &str,
        state: JobState,
        progress: Option<Value>,
        error: Option<String>,
    ) -> Transition {
        let mut g = self.inner.lock().unwrap();
        let Some(j) = g.jobs.get_mut(job_id) else {
            return Transition::Missing;
        };
        if j.state == state {
            return Transition::Duplicate;
        }
        if !matches!(j.state, JobState::Pending | JobState::Running)
            || (j.cancel_requested && state == JobState::Completed)
        {
            return Transition::Conflict;
        }
        j.state = state;
        if progress.is_some() {
            j.progress = progress;
        }
        j.error_msg = error;
        g.total_completed += 1;
        Transition::Accepted
    }

    /// Cancel a running job.  Returns `true` if the job was found.
    pub fn cancel(&self, job_id: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        if let Some(j) = g.jobs.get_mut(job_id) {
            let was_pending = j.state == JobState::Pending;
            j.cancel();
            if let Some(EventSender::Bounded(d)) = &j.event_tx {
                d.cancel(job_id);
            }
            if was_pending && j.state == JobState::Cancelled {
                g.total_completed += 1;
            }
            true
        } else {
            false
        }
    }

    pub fn update_progress(&self, job_id: &str, progress: Value) {
        self.mutate(job_id, |j| {
            if matches!(j.state, JobState::Pending | JobState::Running) {
                j.progress = Some(progress);
            }
        });
    }

    /// Forward an event to the connected client for `job_id`.
    pub fn emit(&self, job_id: &str, ev: WireEvent) {
        let g = self.inner.lock().unwrap();
        if let Some(j) = g.jobs.get(job_id) {
            j.emit(ev);
        }
    }

    /// Emits authoritative data without retaining the registry lock while waiting.
    pub fn emit_reliable(
        &self,
        job_id: &str,
        event: WireEvent,
    ) -> Result<(), crate::delivery::DeliveryError> {
        let sender = self
            .inner
            .lock()
            .unwrap()
            .jobs
            .get(job_id)
            .and_then(|j| j.event_tx.clone())
            .ok_or(crate::delivery::DeliveryError::Interrupted)?;
        sender.blocking_reliable(event)
    }

    pub fn detach_client(&self, job_id: &str) {
        self.mutate(job_id, |j| j.detach_client());
    }

    /// Status snapshot used by the `job_status` RPC handler.
    pub fn status(&self, job_id: &str) -> Option<JobStatus> {
        let g = self.inner.lock().unwrap();
        g.jobs.get(job_id).map(|j| JobStatus {
            job_id: j.job_id.clone(),
            state: j.state.to_string(),
            cancel_requested: j.cancel_requested,
            progress: j.progress.clone(),
            error: j.error_msg.clone().unwrap_or_default(),
        })
    }

    /// Number of jobs currently in `Pending` or `Running` state.
    pub fn active_count(&self) -> u32 {
        let g = self.inner.lock().unwrap();
        g.jobs
            .values()
            .filter(|j| j.state == JobState::Running || j.state == JobState::Pending)
            .count() as u32
    }

    /// Number of unique registered jobs reaching a terminal outcome, including cancellation.
    pub fn total_completed(&self) -> u64 {
        self.inner.lock().unwrap().total_completed
    }

    /// Evict completed / failed / cancelled jobs, keeping at most `keep_n` most recent.
    pub fn gc(&self, keep_n: usize) {
        let mut g = self.inner.lock().unwrap();
        let mut done: Vec<(String, Instant)> = g
            .jobs
            .iter()
            .filter(|(_, j)| j.state != JobState::Running && j.state != JobState::Pending)
            .map(|(k, j)| (k.clone(), j.started_at))
            .collect();
        if done.len() > keep_n {
            done.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
            for (id, _) in &done[keep_n..] {
                g.jobs.remove(id);
            }
        }
    }

    fn mutate(&self, job_id: &str, f: impl FnOnce(&mut Job)) {
        let mut g = self.inner.lock().unwrap();
        if let Some(j) = g.jobs.get_mut(job_id) {
            f(j);
        }
    }
}
