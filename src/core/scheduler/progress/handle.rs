//! The cloneable, read-only progress handle.

use super::snapshot::{OutputProgress, Progress};
use super::state::ProgressState;
use super::tracker::ProgressTracker;
use crate::core::scheduler::ffmpeg_scheduler::{STATUS_ABORT, STATUS_END, STATUS_PAUSE};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A cheap, cloneable, thread-safe, **read-only** view of a scheduled FFmpeg
/// job's progress, obtained from `FfmpegScheduler::progress_handle` in the
/// `Running` or `Paused` state.
///
/// The handle holds only reference-counted `std` synchronization state — no
/// FFmpeg pointers, no callbacks — so it is `Send + Sync`, can be cloned
/// freely across threads, and **remains safe to use after the scheduler has
/// been consumed** (by `wait()`/`stop()`/`abort()`) **or dropped**: from
/// then on [`snapshot`](Self::snapshot) keeps returning the job's final
/// frozen values with [`ProgressState::Ended`].
///
/// Observing progress is pull-based: call `snapshot()` whenever a fresh
/// value is wanted. Each call is a handful of atomic loads; no locks are
/// taken and no FFmpeg call is made.
#[derive(Clone)]
pub struct ProgressHandle {
    /// The scheduler's status word (`STATUS_*` in `ffmpeg_scheduler`).
    status: Arc<AtomicUsize>,
    /// The scheduler's seqlock-style pause parity: odd means a
    /// pause()/resume() transition is in flight or the job is paused.
    pause_epoch: Arc<AtomicUsize>,
    tracker: Arc<ProgressTracker>,
}

impl ProgressHandle {
    pub(crate) fn new(
        status: Arc<AtomicUsize>,
        pause_epoch: Arc<AtomicUsize>,
        tracker: Arc<ProgressTracker>,
    ) -> Self {
        Self {
            status,
            pause_epoch,
            tracker,
        }
    }

    /// Takes a point-in-time snapshot of the job's progress.
    pub fn snapshot(&self) -> Progress {
        // Sample the state FIRST: reading it after the values would let a
        // completion sealed in between produce a snapshot claiming `Ended`
        // while its values predate the freeze. Sampled this way, a snapshot
        // that says `Ended` was taken entirely after the latch sealed, so
        // its values are the frozen finals.
        let state = self.state();
        let elapsed = self.tracker.elapsed();
        let outputs = self
            .tracker
            .outputs()
            .iter()
            .enumerate()
            .map(|(index, telemetry)| OutputProgress::collect(index, telemetry, elapsed))
            .collect();
        Progress::new(state, elapsed, outputs)
    }

    /// Whether every tracked worker of the job has torn down — the same
    /// edge `wait()`/`stop()` unblock on, and the point at which
    /// [`snapshot`](Self::snapshot) values (elapsed included) freeze.
    ///
    /// Note this is stricter than `FfmpegScheduler::is_ended`, which reports
    /// the terminal status *signal* (a stopping job answers `true` there
    /// while workers may still be flushing).
    pub fn is_ended(&self) -> bool {
        self.tracker.is_completed()
    }

    /// Derives the job-level state. Precedence: the sealed completion latch
    /// wins (workers all gone — `Ended`), then a published terminal signal
    /// (`Finishing`: teardown in flight), then pause, then the
    /// producers-drained edge separating `Running` from `Finishing`.
    fn state(&self) -> ProgressState {
        if self.tracker.is_completed() {
            return ProgressState::Ended;
        }
        match self.status.load(Ordering::Acquire) {
            STATUS_PAUSE => ProgressState::Paused,
            STATUS_END | STATUS_ABORT => ProgressState::Finishing,
            _ => {
                // STATUS_RUN (STATUS_INIT is unreachable through a handle:
                // they are only obtainable after start() published RUN).
                // An odd pause epoch marks the pause()/resume() transition
                // windows, where "paused" is the conservative answer (see
                // the epoch invariants on FfmpegScheduler::pause).
                if self.pause_epoch.load(Ordering::Acquire) % 2 == 1 {
                    ProgressState::Paused
                } else if self.tracker.inputs_drained() {
                    ProgressState::Finishing
                } else {
                    ProgressState::Running
                }
            }
        }
    }
}

impl std::fmt::Debug for ProgressHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressHandle")
            .field("state", &self.state())
            .field("is_ended", &self.is_ended())
            .finish_non_exhaustive()
    }
}
