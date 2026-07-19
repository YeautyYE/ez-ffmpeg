//! Input-side exact-N sampler for [`Sampling::UniformN`](super::options::Sampling::UniformN).
//!
//! Placed on the input frame pipeline (pre-filtergraph, S2): frames that lose
//! the grid race are dropped before `scale`. The post-grid tail (~`1/(2n)` of
//! the span) still passes through — and is scaled — because dropping it would
//! starve the encoder-side termination path (see the passthrough branch
//! below). It never physically re-emits tail duplicates (which the EOS drain
//! caps at 1024/filter): instead it stamps a *count* into one held frame's
//! metadata (`EZ_FFMPEG_EMIT_COUNT`) and the output sink clones the packed
//! bytes that many times — O(1) frames regardless of the count.
//!
//! Grid (C1, corrected): anchor at the first delivered frame AT/AFTER the trim
//! boundary, `p0`; targets `t_i = p0 + (i + 0.5) * span / n` for `i in 0..n`;
//! a target resolves to the frame "displayed at" it (the last real frame with
//! `pts <= t_i`). The span (µs) is resolved at open time and published through
//! a shared `OnceLock`.
//!
//! Trim boundary: with `start_time_us`, the container seek lands on a keyframe
//! at or BEFORE the request (up to a GOP early, minus an extra B-frame rewind),
//! the timeline is re-zeroed at the request, and the filtergraph's trim drops
//! every `pts < 0` frame — AFTER this pipeline. Anchoring on such lead-in
//! frames would shift the whole grid early and let trim destroy stamped
//! targets, so the sampler skips frames below the boundary outright.

use super::error::FrameExportError;
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_dict_get, av_dict_set, av_rescale_q, AVFrame, AVMediaType, AVMediaType::AVMEDIA_TYPE_VIDEO,
    AVRational, AV_NOPTS_VALUE,
};
use std::ffi::CString;
use std::sync::{Arc, OnceLock};

/// Metadata key carrying the number of times the output sink should emit a
/// held frame. Interpreted ONLY for `UniformN` (the sink ignores it otherwise).
pub(crate) const EMIT_COUNT_KEY: &str = "EZ_FFMPEG_EMIT_COUNT";

/// The uniform grid span in microseconds, resolved at open time (from
/// `duration_hint_us` > selected-stream duration > container duration, or the
/// trim window) and read by the sampler at its first frame.
pub(crate) type UniformSpan = Arc<OnceLock<i64>>;

const US_PER_SEC: AVRational = AVRational {
    num: 1,
    den: 1_000_000,
};

/// Input-side sampling clock: a frame's µs PTS with a NOPTS fallback
/// (`prev + delta`). Shared by the UniformN sampler and the EveryNth/EverySec
/// input selector so the NOPTS policy cannot drift between them. Used ONLY for
/// grid/selection decisions — the exported `pts_us` still comes from the real
/// frame downstream.
pub(crate) struct SamplingClock {
    prev_pts_us: i64,
    prev_delta_us: i64,
}

impl SamplingClock {
    pub(crate) fn new() -> Self {
        Self {
            prev_pts_us: 0,
            prev_delta_us: 0,
        }
    }

    /// `track_delta` is false until the caller has anchored its grid: the very
    /// first real timestamp has no meaningful inter-frame delta.
    ///
    /// Reads `pts` first: decoder processing normalizes it (including the
    /// forced-framerate rewrite, which re-stamps `pts` AND `time_base` onto
    /// the forced CFR grid while `best_effort_timestamp` keeps the container's
    /// original ticks — pairing those with the rewritten time base mis-scales
    /// every decision). `best_effort_timestamp` is only the fallback for
    /// frames whose canonical `pts` is unset.
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame`.
    pub(crate) unsafe fn pts_us(&mut self, p: *const AVFrame, track_delta: bool) -> i64 {
        let tb = (*p).time_base;
        let raw = (*p).pts;
        let raw = if raw == AV_NOPTS_VALUE {
            (*p).best_effort_timestamp
        } else {
            raw
        };
        if tb.den != 0 && raw != AV_NOPTS_VALUE {
            let pts = av_rescale_q(raw, tb, US_PER_SEC);
            if track_delta {
                // saturating: extreme timestamp discontinuities must not overflow.
                self.prev_delta_us = pts.saturating_sub(self.prev_pts_us).max(0);
            }
            self.prev_pts_us = pts;
            pts
        } else {
            // No usable timestamp: extrapolate from the last real frame.
            let est = self.prev_pts_us.saturating_add(self.prev_delta_us.max(1));
            self.prev_pts_us = est;
            est
        }
    }
}

/// Input-side exact-N frame sampler.
pub(crate) struct ExportSampler {
    n: u32,
    span_cell: UniformSpan,
    /// PTS of the first delivered frame (µs) — the grid anchor `p0`.
    anchor_us: Option<i64>,
    /// Resolved grid span (µs); read from `span_cell` at the first frame.
    span_us: i64,
    /// Number of targets resolved so far (0..=n).
    cursor: i128,
    /// The frame currently held back, awaiting its displayed-target count.
    held: Option<Frame>,
    /// Frames with sampling PTS below this are lead-in the in-graph trim will
    /// drop (`Some(0)` when `start_time_us` re-zeroes the timeline); skipped.
    trim_boundary_us: Option<i64>,
    clock: SamplingClock,
    /// Debug bookkeeping: total copies the sink is asked to emit.
    emitted_targets: i128,
}

impl ExportSampler {
    pub(crate) fn new(n: u32, span_cell: UniformSpan, trim_boundary_us: Option<i64>) -> Self {
        Self {
            n,
            span_cell,
            anchor_us: None,
            span_us: 0,
            cursor: 0,
            held: None,
            trim_boundary_us,
            clock: SamplingClock::new(),
            emitted_targets: 0,
        }
    }

    /// A frame's sampling PTS in µs (see [`SamplingClock`]).
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame`.
    unsafe fn sampling_pts_us(&mut self, p: *const AVFrame) -> i64 {
        self.clock.pts_us(p, self.anchor_us.is_some())
    }

    /// Count of targets strictly below `pts`, i.e. `|{ i in 0..n : t_i < pts }|`,
    /// computed in i128 with no per-target scan (n may be huge).
    fn targets_below(&self, pts_us: i64) -> i128 {
        let p0 = self.anchor_us.unwrap_or(0) as i128;
        let span = self.span_us.max(1) as i128;
        let n = self.n as i128;
        // t_i < pts  <=>  (2i + 1) * span < 2n * (pts - p0)
        // count = ceil( (2n*(pts-p0) - span) / (2*span) ), clamped to [0, n]
        let num = 2 * n * (pts_us as i128 - p0) - span;
        let den = 2 * span;
        let ceil_div = (num + den - 1).div_euclid(den);
        ceil_div.clamp(0, n)
    }

    /// Emits `held` (if any) with a dup-count of `count`, consuming the marker.
    fn emit_held(&mut self, count: i128) -> Result<Option<Frame>, FrameFilterError> {
        match self.held.take() {
            Some(mut held) => {
                stamp_emit_count(&mut held, count as u64)?;
                self.emitted_targets += count;
                Ok(Some(held))
            }
            None => Ok(None),
        }
    }
}

impl FrameFilter for ExportSampler {
    fn media_type(&self) -> AVMediaType {
        AVMEDIA_TYPE_VIDEO
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // End-of-stream flush cue: release the held frame with all remaining
        // targets, or report an empty stream.
        if frame_is_eof_marker(&frame) {
            let remaining = self.n as i128 - self.cursor;
            if remaining <= 0 {
                return Ok(Some(frame)); // nothing left; pass the marker through
            }
            self.cursor = self.n as i128;
            if self.held.is_some() {
                // Consume the marker; the pipeline propagates EOF via its
                // post-flush dry-chain traversal.
                return self.emit_held(remaining);
            }
            // A target remains but no frame was ever delivered.
            return Err(Box::new(FrameExportError::EmptyVideoStream { n: self.n }));
        }

        // SAFETY: null-checked before any deref.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(Some(frame));
        }

        // Grid complete: switch to passthrough. Dropping the tail instead would
        // starve the encoder, and input recording-time is enforced there for
        // decoded streams (the demux cut at demux_task.rs only flags streamcopy
        // destinations) — an unbounded source would then never terminate. The
        // sink ignores these unstamped frames (its n-budget clamp reads 0).
        if self.cursor >= self.n as i128 {
            return Ok(Some(frame));
        }

        let f_pts = unsafe { self.sampling_pts_us(p) };

        // Lead-in below the trim boundary: the in-graph trim discards these
        // frames AFTER this pipeline, so anchoring or stamping on them would
        // aim the grid at frames that never survive. Consume them here; the
        // grid anchors at the first frame the trim will keep.
        if self.trim_boundary_us.is_some_and(|b| f_pts < b) {
            return Ok(None);
        }

        // First surviving frame anchors the grid and reads the resolved span.
        if self.anchor_us.is_none() {
            let span = *self.span_cell.get().ok_or_else(|| -> FrameFilterError {
                "frame export: UniformN span was not resolved before the run started".into()
            })?;
            self.anchor_us = Some(f_pts);
            self.span_us = span.max(1);
            self.held = Some(frame);
            return Ok(None);
        }

        let new_count = self.targets_below(f_pts);
        let k = new_count - self.cursor;
        if k > 0 {
            // Targets in [held.pts, f.pts) are displayed on the held frame.
            self.cursor = new_count;
            let out = self.emit_held(k)?;
            self.held = Some(frame);
            Ok(out)
        } else {
            // The held frame won no targets; drop it and hold this one.
            self.held = Some(frame);
            Ok(None)
        }
    }
}

/// Stamps the emit-count on a frame's metadata, purging every pre-existing
/// same-named entry first. The purge matters: a hostile or MULTIKEY-built
/// source dict can hold several entries, and a plain `av_dict_set` replace
/// appends the new value at the END of the dict — a leftover entry would then
/// win the sink's first-match read.
fn stamp_emit_count(frame: &mut Frame, count: u64) -> Result<(), FrameFilterError> {
    let key = CString::new(EMIT_COUNT_KEY).expect("literal has no NUL");
    let val = CString::new(count.to_string()).expect("digits have no NUL");
    // SAFETY: as_mut_ptr yields a valid owned frame; each av_dict_set(NULL)
    // removes one matching entry, so the loop strictly shrinks the dict.
    let ret = unsafe {
        let p = frame.as_mut_ptr();
        while !av_dict_get((*p).metadata, key.as_ptr(), std::ptr::null(), 0).is_null() {
            if av_dict_set(&mut (*p).metadata, key.as_ptr(), std::ptr::null(), 0) < 0 {
                break;
            }
        }
        av_dict_set(&mut (*p).metadata, key.as_ptr(), val.as_ptr(), 0)
    };
    if ret < 0 {
        return Err("frame export: failed to stamp EMIT_COUNT metadata".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sampler(n: u32, p0: i64, span: i64) -> ExportSampler {
        let mut s = ExportSampler::new(n, Arc::new(OnceLock::new()), None);
        s.anchor_us = Some(p0);
        s.span_us = span;
        s
    }

    #[test]
    fn targets_below_boundaries() {
        // n=16, p0=0, span=1.6e6 => t_i = (i + 0.5) * 100_000.
        let s = sampler(16, 0, 1_600_000);
        assert_eq!(s.targets_below(0), 0);
        assert_eq!(s.targets_below(50_000), 0, "t_0 is not strictly below");
        assert_eq!(s.targets_below(50_001), 1);
        assert_eq!(s.targets_below(150_001), 2);
        assert_eq!(s.targets_below(i64::MAX), 16, "clamped to n");
    }

    #[test]
    fn targets_below_offset_grid() {
        // n=4, p0=1000, span=4000 => t_i = 1500, 2500, 3500, 4500.
        let s = sampler(4, 1000, 4000);
        assert_eq!(s.targets_below(1500), 0);
        assert_eq!(s.targets_below(1501), 1);
        assert_eq!(s.targets_below(4500), 3);
        assert_eq!(s.targets_below(4501), 4);
    }

    #[test]
    fn targets_below_is_monotonic() {
        let s = sampler(64, 500, 2_000_000);
        let mut prev = 0;
        for pts in (0..2_600_000).step_by(9_137) {
            let c = s.targets_below(pts);
            assert!(c >= prev && c <= 64, "non-decreasing, bounded by n");
            prev = c;
        }
        assert_eq!(s.targets_below(i64::MAX), 64);
    }
}
