//! Crate-internal shared state behind [`ProgressHandle`](super::ProgressHandle).
//!
//! Everything in this file is plain `std` synchronization state (atomics and
//! `OnceLock` timestamps) written by scheduler workers and read by any number
//! of progress handles. No FFmpeg pointer ever enters this module: the mux
//! worker converts packet timestamps to microseconds and byte positions to
//! plain integers *before* they land here, so a handle that outlives the
//! scheduler (and the FFmpeg contexts it owned) can never dangle.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Watermark sentinel: no packet has ever been committed for this stream.
const STREAM_NOT_STARTED: i64 = i64::MIN;
/// `video_stream` encoding: the mux worker has not resolved the streams yet.
const VIDEO_UNKNOWN: usize = usize::MAX;
/// `video_stream` encoding: this output has no video stream.
const VIDEO_NONE: usize = usize::MAX - 1;

/// Per-output telemetry shared between one mux worker (the only writer) and
/// every progress handle (readers).
///
/// The write side is deliberately tiny — a `fetch_max`, a counter bump and a
/// byte-position store per successfully committed packet — so it adds no
/// measurable work to the mux hot path and needs no lock.
pub(crate) struct OutputTelemetry {
    /// Post-fixup write watermark per output stream, in microseconds:
    /// `fetch_max` of the committed packet's presentation timestamp
    /// (falling back to its DTS when the muxer left PTS unset), updated only
    /// AFTER `av_interleaved_write_frame` returned success. Monotonic
    /// non-decreasing per stream by construction.
    stream_watermark_us: Box<[AtomicI64]>,
    /// Streams retired by the mux worker's single completion authority
    /// (`finish_output_stream`) or by the worker's terminal path. A retired
    /// stream leaves the "active" set consulted by [`Self::out_time_us`],
    /// so a short stream that finished early cannot pin the output's
    /// progress at its final timestamp forever.
    stream_finished: Box<[AtomicBool]>,
    /// Selected video stream index (`VIDEO_UNKNOWN` until the mux worker
    /// resolves stream types, `VIDEO_NONE` when the output carries no
    /// video). The lowest-index video stream is selected, mirroring which
    /// stream the CLI's `frame=` counter follows.
    video_stream: AtomicUsize,
    /// Packets of the selected video stream successfully committed to the
    /// muxer.
    video_packets: AtomicU64,
    /// Latest muxer byte position; `-1` while unknown. Never written for
    /// `AVFMT_NOFILE` outputs (HLS, null, ...), whose sizes the muxer
    /// manages out of band — those report `None`, not a made-up 0.
    total_size: AtomicI64,
    /// Set once a `ProgressHandle` for this job has been handed out. Gates
    /// the per-packet byte-position probe in the mux hot path: a job that
    /// never calls `progress_handle()` pays nothing for `total_size`
    /// sampling. Purely an optimization hint (`Relaxed`) — the watermark and
    /// packet counters are always maintained, and a first packet that races
    /// the flag's publication merely misses one size sample, self-healing on
    /// the next.
    observed: AtomicBool,
    /// Test-only: number of per-packet logical-position probes taken on the
    /// mux write path (bumped only when the `is_observed` gate lets a probe
    /// through). Lets a test prove the gate end-to-end — observed jobs probe,
    /// unobserved jobs do not — without a process-global counter that
    /// concurrent tests would perturb. Compiled out of non-test builds.
    #[cfg(test)]
    size_probes: AtomicU64,
}

impl OutputTelemetry {
    pub(crate) fn new(stream_count: usize) -> Self {
        Self {
            stream_watermark_us: (0..stream_count)
                .map(|_| AtomicI64::new(STREAM_NOT_STARTED))
                .collect(),
            stream_finished: (0..stream_count).map(|_| AtomicBool::new(false)).collect(),
            video_stream: AtomicUsize::new(VIDEO_UNKNOWN),
            video_packets: AtomicU64::new(0),
            total_size: AtomicI64::new(-1),
            observed: AtomicBool::new(false),
            #[cfg(test)]
            size_probes: AtomicU64::new(0),
        }
    }

    /// Marks this output as observed by at least one progress handle, arming
    /// the per-packet byte-position probe. Idempotent.
    pub(crate) fn mark_observed(&self) {
        self.observed.store(true, Ordering::Relaxed);
    }

    /// Whether a progress handle has been handed out — the mux hot path
    /// consults this before the per-packet `total_size` probe so an
    /// unobserved job skips it entirely.
    pub(crate) fn is_observed(&self) -> bool {
        self.observed.load(Ordering::Relaxed)
    }

    /// Test-only: records that the mux hot path took one per-packet
    /// logical-position probe (called only inside the `is_observed` gate).
    #[cfg(test)]
    pub(crate) fn note_perpacket_size_probe(&self) {
        self.size_probes.fetch_add(1, Ordering::Relaxed);
    }

    /// Test-only: how many per-packet logical-position probes this output
    /// took — 0 for an unobserved job, positive once observed.
    #[cfg(test)]
    pub(crate) fn perpacket_size_probes(&self) -> u64 {
        self.size_probes.load(Ordering::Relaxed)
    }

    /// Publishes the selected video stream (or the absence of one). Called
    /// once by the mux worker prologue; packet-sink outputs never call it,
    /// so their `video_packets()` stays `None` — their packets bypass the
    /// muxer write path entirely.
    pub(crate) fn set_video_stream(&self, index: Option<usize>) {
        self.video_stream
            .store(index.unwrap_or(VIDEO_NONE), Ordering::Release);
    }

    /// Records one packet successfully committed to the muxer:
    /// raises the stream's post-fixup watermark to `ts_us` (when the packet
    /// carried a usable timestamp) and counts it when it belongs to the
    /// selected video stream. Out-of-range indices are ignored defensively —
    /// the bounds gate also keeps an invalid index from ever aliasing the
    /// `video_stream` sentinels.
    pub(crate) fn record_written(&self, stream_index: usize, ts_us: Option<i64>) {
        if stream_index >= self.stream_watermark_us.len() {
            return;
        }
        if let Some(ts_us) = ts_us {
            self.stream_watermark_us[stream_index].fetch_max(ts_us, Ordering::AcqRel);
        }
        if stream_index == self.video_stream.load(Ordering::Acquire) {
            self.video_packets.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Retires one stream from the active set (it will never receive another
    /// packet). Idempotent.
    pub(crate) fn mark_stream_finished(&self, stream_index: usize) {
        if let Some(finished) = self.stream_finished.get(stream_index) {
            finished.store(true, Ordering::Release);
        }
    }

    /// Retires every stream: called on the mux worker's terminal path, where
    /// no stream of this output can receive another packet.
    pub(crate) fn mark_all_streams_finished(&self) {
        for finished in &self.stream_finished {
            finished.store(true, Ordering::Release);
        }
    }

    /// Publishes the muxer's current byte position; negative values (an
    /// unavailable position) are ignored so an established size is never
    /// regressed to "unknown".
    pub(crate) fn set_total_size(&self, bytes: i64) {
        if bytes >= 0 {
            self.total_size.store(bytes, Ordering::Release);
        }
    }

    /// The output's progress position in microseconds: the minimum committed
    /// post-fixup watermark across the *active* (not yet finished) streams.
    ///
    /// Strict like the fftools progress line: while any active stream has
    /// not committed its first packet the position is unknowable and this
    /// returns `None` rather than a guess. Once every stream is finished the
    /// value freezes at the maximum committed watermark (the true high-water
    /// mark of the output). The sequence of `Some` values returned over time
    /// is monotonic non-decreasing: per-stream watermarks only rise, the
    /// active set only shrinks (removing a stream can only raise a minimum),
    /// and the final freeze is a maximum over the same watermarks.
    pub(crate) fn out_time_us(&self) -> Option<i64> {
        let mut active_min: Option<i64> = None;
        let mut started_max: Option<i64> = None;
        let mut all_finished = true;
        for (watermark, finished) in self.stream_watermark_us.iter().zip(&self.stream_finished) {
            // Acquire the finished flag BEFORE this stream's watermark — the
            // ONE load order this function's correctness depends on.
            // `finish_output_stream` flushes the stream's final watermark and
            // THEN marks it finished with Release (mux_task.rs). Reading
            // `finished == true` with Acquire first establishes the
            // synchronizes-with edge, so the watermark load below is
            // guaranteed to observe that published final value in the SAME
            // all-finished read. Reversed (watermark then finished), an
            // already-completed watermark load need not see the final store:
            // a terminal read could then take a stale watermark — even
            // STREAM_NOT_STARTED for a just-finished stream, collapsing the
            // result to `None` — and jump back up on the next read, breaking
            // the documented monotonic-non-decreasing contract. Do NOT swap
            // these two loads. (The load-order property is a memory-ordering
            // invariant; its semantic consequence — a finished stream's
            // watermark must count toward the terminal max — is pinned by
            // `all_finished_publication_is_stable_and_never_collapses`, and
            // exhaustive interleaving verification is a Loom backlog item.)
            let is_finished = finished.load(Ordering::Acquire);
            let ts = watermark.load(Ordering::Acquire);
            if ts != STREAM_NOT_STARTED {
                started_max = Some(started_max.map_or(ts, |max: i64| max.max(ts)));
            }
            if is_finished {
                continue;
            }
            all_finished = false;
            if ts == STREAM_NOT_STARTED {
                // An active stream has not started: the min is unknowable.
                return None;
            }
            active_min = Some(active_min.map_or(ts, |min: i64| min.min(ts)));
        }
        if all_finished {
            started_max
        } else {
            active_min
        }
    }

    /// Packets of the selected video stream committed to the muxer; `None`
    /// while the selection is unresolved or when the output has no video
    /// stream.
    pub(crate) fn video_packets(&self) -> Option<u64> {
        match self.video_stream.load(Ordering::Acquire) {
            VIDEO_UNKNOWN | VIDEO_NONE => None,
            _ => Some(self.video_packets.load(Ordering::Acquire)),
        }
    }

    /// Bytes the muxer has written so far; `None` while unknown and for
    /// `AVFMT_NOFILE` outputs.
    pub(crate) fn total_size(&self) -> Option<u64> {
        let bytes = self.total_size.load(Ordering::Acquire);
        (bytes >= 0).then_some(bytes as u64)
    }
}

/// Scheduler-owned root of the progress state. One per scheduler, created in
/// `FfmpegScheduler::new` and shared (via `Arc`) with every
/// [`ProgressHandle`](super::ProgressHandle), each mux worker (its
/// [`OutputTelemetry`] entry) and the `ThreadSynchronizer` (which seals the
/// completion latch when the last tracked worker releases its slot).
pub(crate) struct ProgressTracker {
    /// Wall-clock start of the job, set once at the top of `start()`.
    started_at: OnceLock<Instant>,
    /// Completion latch: sealed exactly when the live worker count reaches
    /// zero (see `ThreadSynchronizer::thread_done_with_settled`), freezing
    /// [`Self::elapsed`] and flipping the reported state to `Ended`.
    completed_at: OnceLock<Instant>,
    /// Per-output telemetry, indexed by output declaration order.
    outputs: Box<[Arc<OutputTelemetry>]>,
    /// Demuxer "task exited" flags (shared with the balancing scheduler's
    /// demux nodes): a set flag means that input produces nothing further.
    demux_exited: Box<[Arc<AtomicBool>]>,
    /// Frame-source (VideoWriter) worker exit flags, one per source; set by
    /// the worker on every exit path.
    frame_source_exited: Box<[Arc<AtomicBool>]>,
}

impl ProgressTracker {
    pub(crate) fn new(
        outputs: Vec<Arc<OutputTelemetry>>,
        demux_exited: Vec<Arc<AtomicBool>>,
        frame_source_count: usize,
    ) -> Self {
        Self {
            started_at: OnceLock::new(),
            completed_at: OnceLock::new(),
            outputs: outputs.into_boxed_slice(),
            demux_exited: demux_exited.into_boxed_slice(),
            frame_source_exited: (0..frame_source_count)
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect(),
        }
    }

    /// Stamps the job's wall-clock start; later calls are ignored.
    pub(crate) fn mark_started(&self) {
        let _ = self.started_at.set(Instant::now());
    }

    /// Arms every output's per-packet byte-position probe. Called by
    /// `progress_handle()` when a handle is first handed out, so a job that
    /// never observes progress leaves the mux hot path free of the probe.
    pub(crate) fn mark_observed(&self) {
        for output in &self.outputs {
            output.mark_observed();
        }
    }

    /// Seals the completion latch; later calls are ignored. Called by the
    /// thread synchronizer when the live worker count reaches zero — the
    /// same edge `wait()`/`stop()` unblock on.
    pub(crate) fn seal_completed(&self) {
        let _ = self.completed_at.set(Instant::now());
    }

    /// Whether every tracked worker has torn down.
    pub(crate) fn is_completed(&self) -> bool {
        self.completed_at.get().is_some()
    }

    /// Wall-clock time since `start()`, frozen at the completion latch.
    /// Includes time spent paused. Zero before `start()` stamped the clock
    /// (unreachable through the public API: handles only exist after
    /// `start()` returned).
    pub(crate) fn elapsed(&self) -> Duration {
        let Some(&started_at) = self.started_at.get() else {
            return Duration::ZERO;
        };
        match self.completed_at.get() {
            Some(&completed_at) => completed_at.saturating_duration_since(started_at),
            None => started_at.elapsed(),
        }
    }

    pub(crate) fn outputs(&self) -> &[Arc<OutputTelemetry>] {
        &self.outputs
    }

    /// The telemetry entry a mux worker updates for output `index`.
    pub(crate) fn output_telemetry(&self, index: usize) -> Arc<OutputTelemetry> {
        self.outputs[index].clone()
    }

    /// The exit flag `start()` hands to frame source `index`.
    pub(crate) fn frame_source_exit_flag(&self, index: usize) -> Arc<AtomicBool> {
        self.frame_source_exited[index].clone()
    }

    /// Whether every input producer (demuxer and frame source) has finished
    /// producing — the job is draining/flushing what is already in flight.
    /// `false` for a job with no tracked producers (defensive; `build()`
    /// rejects input-less jobs).
    pub(crate) fn inputs_drained(&self) -> bool {
        let producers = self.demux_exited.iter().chain(&self.frame_source_exited);
        let mut any = false;
        for exited in producers {
            any = true;
            if !exited.load(Ordering::Acquire) {
                return false;
            }
        }
        any
    }
}
