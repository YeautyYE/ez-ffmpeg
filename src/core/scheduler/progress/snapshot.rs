//! Owned progress snapshots: [`Progress`] and its per-output entries.

use super::state::ProgressState;
use super::tracker::OutputTelemetry;
use std::time::Duration;

/// A point-in-time, owned snapshot of a scheduled FFmpeg job's progress,
/// obtained from [`ProgressHandle::snapshot`](super::ProgressHandle::snapshot).
///
/// A snapshot is plain data: it stays valid (and unchanged) no matter what
/// the job does afterwards. Take a new snapshot to observe newer values.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Progress {
    state: ProgressState,
    elapsed: Duration,
    outputs: Vec<OutputProgress>,
}

impl Progress {
    pub(crate) fn new(state: ProgressState, elapsed: Duration, outputs: Vec<OutputProgress>) -> Self {
        Self {
            state,
            elapsed,
            outputs,
        }
    }

    /// The job's lifecycle phase at the moment of the snapshot.
    pub fn state(&self) -> ProgressState {
        self.state
    }

    /// Wall-clock time since `start()`, frozen once the job reaches
    /// [`ProgressState::Ended`]. This is real elapsed time and **includes
    /// time spent paused** — divide-by-elapsed rates in a long-paused job
    /// sag accordingly.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Per-output progress, indexed by output declaration order (the order
    /// outputs were added to the builder). Always one entry per output —
    /// multi-output jobs such as an HLS ladder report each rung separately.
    pub fn outputs(&self) -> &[OutputProgress] {
        &self.outputs
    }
}

/// Progress of one output of the job.
///
/// Apart from [`output_index`](Self::output_index), which is always present,
/// every telemetry metric is an `Option`: `None` means "not knowable right
/// now" (or not applicable to this output), never a fabricated zero. In
/// particular:
///
/// - a packet-sink output delivers packets through callbacks instead of a
///   muxer, so all of its optional metrics stay `None`;
/// - an `AVFMT_NOFILE` muxer (HLS, `null`, ...) manages its own I/O, so
///   [`total_size`](Self::total_size) and
///   [`bitrate_kbps`](Self::bitrate_kbps) stay `None`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OutputProgress {
    output_index: usize,
    video_packets: Option<u64>,
    fps: Option<f64>,
    out_time_us: Option<i64>,
    total_size: Option<u64>,
    bitrate_kbps: Option<f64>,
    speed: Option<f64>,
}

impl OutputProgress {
    /// Reads one output's shared telemetry into an owned snapshot entry,
    /// deriving the rate fields from `elapsed`.
    pub(crate) fn collect(
        output_index: usize,
        telemetry: &OutputTelemetry,
        elapsed: Duration,
    ) -> Self {
        let video_packets = telemetry.video_packets();
        let out_time_us = telemetry.out_time_us();
        let total_size = telemetry.total_size();
        let elapsed_secs = elapsed.as_secs_f64();
        let fps = match video_packets {
            Some(packets) if elapsed_secs > 0.0 => Some(packets as f64 / elapsed_secs),
            _ => None,
        };
        // Bytes * 8 / milliseconds is numerically kbit/s — the same formula
        // the CLI progress line uses.
        let bitrate_kbps = match (total_size, out_time_us) {
            (Some(bytes), Some(t_us)) if t_us > 0 => {
                Some(bytes as f64 * 8.0 * 1000.0 / t_us as f64)
            }
            _ => None,
        };
        let speed = match out_time_us {
            Some(t_us) if t_us >= 0 && elapsed_secs > 0.0 => {
                Some(t_us as f64 / 1_000_000.0 / elapsed_secs)
            }
            _ => None,
        };
        Self {
            output_index,
            video_packets,
            fps,
            out_time_us,
            total_size,
            bitrate_kbps,
            speed,
        }
    }

    /// Position of this output in the job's output declaration order.
    pub fn output_index(&self) -> usize {
        self.output_index
    }

    /// Packets of the selected video stream (the lowest-index video stream
    /// of this output) successfully committed to the muxer.
    ///
    /// This is the typed counterpart of the CLI's `frame=` counter, but it
    /// deliberately is not called "frames": bitstream filters between the
    /// encoder and the muxer can change the packet count, so what is
    /// counted here is committed *packets*. `None` when the output has no
    /// video stream, before the muxer has resolved its streams, and for
    /// packet-sink outputs.
    pub fn video_packets(&self) -> Option<u64> {
        self.video_packets
    }

    /// Video packets per second of wall-clock time
    /// ([`video_packets`](Self::video_packets) / elapsed). `None` whenever
    /// `video_packets` is `None` or no time has elapsed.
    pub fn fps(&self) -> Option<f64> {
        self.fps
    }

    /// The output's progress position in microseconds: the minimum
    /// post-fixup timestamp watermark across this output's *active* streams,
    /// where a watermark advances only when a packet was **successfully
    /// written** to the muxer.
    ///
    /// Strict like the fftools progress line: while any active stream has
    /// not committed its first packet this is `None` rather than a guess.
    /// Streams that finished early leave the active set (a sparse or short
    /// stream cannot pin the value forever), and once every stream finished
    /// the value freezes at the output's high-water mark. Across snapshots
    /// the sequence of `Some` values is **monotonic non-decreasing**.
    pub fn out_time_us(&self) -> Option<i64> {
        self.out_time_us
    }

    /// Bytes written to the output so far (header included), as reported by
    /// the muxer's I/O context after the most recent successful write.
    /// `None` for `AVFMT_NOFILE` muxers (e.g. HLS — its segment sizes are
    /// not observable here), for packet-sink outputs, and before the first
    /// write.
    pub fn total_size(&self) -> Option<u64> {
        self.total_size
    }

    /// Average bitrate so far in kbit/s
    /// ([`total_size`](Self::total_size) * 8 / [`out_time_us`](Self::out_time_us)).
    /// `None` whenever either input is unavailable or the position is not
    /// yet positive.
    pub fn bitrate_kbps(&self) -> Option<f64> {
        self.bitrate_kbps
    }

    /// Processing speed relative to media time: media seconds produced per
    /// wall-clock second (the CLI's `speed=`). `None` whenever
    /// [`out_time_us`](Self::out_time_us) is unavailable or negative, or no
    /// time has elapsed.
    pub fn speed(&self) -> Option<f64> {
        self.speed
    }

    /// Convenience: this output's position as a percentage of a
    /// caller-supplied total duration in microseconds (for example from
    /// `container_info`), clamped to `[0, 100]`.
    ///
    /// The library does not derive the percentage itself: only the caller
    /// knows the intended total (trims, `-t` limits and live inputs make
    /// any guess wrong). Returns `None` when the position is unknown or
    /// `total_us` is not positive.
    pub fn percent_of(&self, total_us: i64) -> Option<f64> {
        if total_us <= 0 {
            return None;
        }
        let out_time_us = self.out_time_us?;
        Some((out_time_us as f64 / total_us as f64 * 100.0).clamp(0.0, 100.0))
    }
}
