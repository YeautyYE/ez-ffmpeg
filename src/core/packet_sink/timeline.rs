//! Shared-origin timeline state for the strict tier.
//!
//! Anchoring is an explicit state transition ([`Timeline`]), not a scatter of
//! correlated `Option`s: either no packet has been delivered yet
//! (`Unanchored`) or the origin is committed with one offset per stream
//! (`Anchored`). The transition is all-or-nothing — every stream's offset is
//! rescaled first and only then committed, so a rescale overflow can never
//! leave a half-anchored job behind.

use crate::error::PacketSinkError;
use ffmpeg_sys_next::{av_rescale_q_rnd, AVRational, AVRounding};

/// Job-wide shared origin.
pub(crate) enum Timeline {
    /// No packet delivered yet.
    Unanchored,
    /// Origin committed: one shift per stream, in that stream's time base.
    Anchored { offsets: Vec<i64> },
}

impl Timeline {
    pub(crate) fn new() -> Self {
        Timeline::Unanchored
    }

    /// Anchors on the first delivered packet's `(dts0, tb0)` if not anchored
    /// yet, deriving every stream's offset via
    /// `av_rescale_q_rnd(dts0, tb0, tb_i, AV_ROUND_NEAR_INF)`. Idempotent
    /// after the first call. A rescale overflow reports the offending stream
    /// and leaves the timeline unanchored.
    pub(crate) fn ensure_anchored(
        &mut self,
        dts0: i64,
        tb0: AVRational,
        stream_time_bases: &[AVRational],
    ) -> Result<(), PacketSinkError> {
        if matches!(self, Timeline::Anchored { .. }) {
            return Ok(());
        }
        let mut offsets = Vec::with_capacity(stream_time_bases.len());
        for (stream_index, tb) in stream_time_bases.iter().enumerate() {
            // SAFETY: pure integer arithmetic over validated rationals (every
            // stream time base was checked positive at collection).
            let offset =
                unsafe { av_rescale_q_rnd(dts0, tb0, *tb, AVRounding::AV_ROUND_NEAR_INF) };
            if offset == i64::MIN {
                return Err(PacketSinkError::TimestampOverflow { stream_index });
            }
            offsets.push(offset);
        }
        *self = Timeline::Anchored { offsets };
        Ok(())
    }

    /// This stream's committed shift. Panics if unanchored — callers anchor
    /// first (`ensure_anchored` precedes every use on the packet path).
    pub(crate) fn offset(&self, stream_index: usize) -> i64 {
        match self {
            Timeline::Anchored { offsets } => offsets[stream_index],
            Timeline::Unanchored => unreachable!("timeline queried before anchoring"),
        }
    }
}

/// Per-stream S7 timestamp validation on the shifted timeline.
pub(crate) struct StreamTimeline {
    /// Last delivered dts, for strict monotonicity.
    last_dts: Option<i64>,
    /// Delivered pts values still above the dts watermark. Since `pts >= dts`
    /// holds and dts strictly increases, values at or below the watermark can
    /// never recur — the window stays bounded by the encoder reorder depth.
    pending_pts: Vec<i64>,
}

impl StreamTimeline {
    pub(crate) fn new() -> Self {
        Self {
            last_dts: None,
            pending_pts: Vec::new(),
        }
    }

    /// Validates one shifted `(pts, dts)` pair and records it: `pts >= dts`,
    /// strictly increasing dts, and no duplicate pts. Duplicate membership is
    /// checked BEFORE pruning — a recorded pts equal to the NEW dts is
    /// exactly the boundary a duplicate can still collide with (e.g.
    /// (pts 3, dts 0) followed by (pts 3, dts 3)); pruning first forgot it.
    pub(crate) fn observe(
        &mut self,
        stream_index: usize,
        pts: i64,
        dts: i64,
    ) -> Result<(), PacketSinkError> {
        if pts < dts {
            return Err(PacketSinkError::PtsBeforeDts {
                stream_index,
                pts,
                dts,
            });
        }
        if let Some(prev) = self.last_dts {
            if dts <= prev {
                return Err(PacketSinkError::NonMonotonicDts {
                    stream_index,
                    prev,
                    current: dts,
                });
            }
        }
        if self.pending_pts.contains(&pts) {
            return Err(PacketSinkError::DuplicatePts { stream_index, pts });
        }
        self.pending_pts.retain(|&p| p > dts);
        self.pending_pts.push(pts);
        self.last_dts = Some(dts);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_is_all_or_nothing_and_idempotent() {
        let tb25 = AVRational { num: 1, den: 25 };
        let tb_audio = AVRational { num: 1, den: 44100 };
        let mut tl = Timeline::new();
        tl.ensure_anchored(5, tb25, &[tb25, tb_audio]).unwrap();
        assert_eq!(tl.offset(0), 5);
        assert_eq!(tl.offset(1), 8820);
        // Second anchor attempt is a no-op (offsets unchanged).
        tl.ensure_anchored(100, tb25, &[tb25, tb_audio]).unwrap();
        assert_eq!(tl.offset(0), 5);
    }

    #[test]
    fn observe_rejects_the_boundary_duplicate() {
        let mut st = StreamTimeline::new();
        st.observe(0, 3, 0).unwrap();
        assert!(matches!(
            st.observe(0, 3, 3),
            Err(PacketSinkError::DuplicatePts { pts: 3, .. })
        ));
    }
}
