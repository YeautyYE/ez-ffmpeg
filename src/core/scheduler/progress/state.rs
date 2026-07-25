//! Job-level lifecycle phase reported by a progress snapshot.

/// Coarse lifecycle phase of a scheduled FFmpeg job at the moment a
/// [`Progress`](super::Progress) snapshot was taken.
///
/// The variants are ordered by the normal lifecycle
/// (`Running` → `Finishing` → `Ended`, with `Paused` reachable from
/// `Running`), but a snapshot only reports the current phase — it does not
/// promise that every phase is observable (a short job can go straight from
/// `Running` to `Ended` between two snapshots).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProgressState {
    /// Workers are processing and at least one input is still producing.
    Running,
    /// The job is paused (see `FfmpegScheduler::pause`). Wall-clock time
    /// keeps accruing in [`Progress::elapsed`](super::Progress::elapsed)
    /// while paused.
    Paused,
    /// The pipeline is winding down: either every input producer (demuxer /
    /// frame source) has finished producing and the decoders, filters,
    /// encoders and muxers are flushing their tails, or a stop/abort signal
    /// has been published and workers are tearing down. Outputs may still be
    /// finalizing (trailer writes included).
    Finishing,
    /// Every tracked worker has torn down. All snapshot values — including
    /// [`Progress::elapsed`](super::Progress::elapsed) — are frozen from
    /// this point on, whether the job succeeded, failed, or was aborted
    /// (this state does not distinguish those outcomes; `wait()`/`stop()`
    /// report them).
    Ended,
}
