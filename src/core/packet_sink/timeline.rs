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
    /// No packet delivered yet. Carries the offset storage, sized for the
    /// job's stream count at collection, so the anchor transition on the
    /// first delivered packet allocates nothing.
    Unanchored { storage: Vec<i64> },
    /// Origin committed: one shift per stream, in that stream's time base.
    Anchored { offsets: Vec<i64> },
}

impl Timeline {
    pub(crate) fn new(stream_count: usize) -> Self {
        Timeline::Unanchored {
            storage: Vec::with_capacity(stream_count),
        }
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
        let storage = match self {
            Timeline::Anchored { .. } => return Ok(()),
            Timeline::Unanchored { storage } => storage,
        };
        // Empty on entry: preallocated empty at collection, and the error
        // path below clears whatever it partially filled.
        debug_assert!(storage.is_empty());
        for (stream_index, tb) in stream_time_bases.iter().enumerate() {
            // SAFETY: pure integer arithmetic over validated rationals (every
            // stream time base was checked positive at collection).
            let offset =
                unsafe { av_rescale_q_rnd(dts0, tb0, *tb, AVRounding::AV_ROUND_NEAR_INF) };
            if offset == i64::MIN {
                storage.clear();
                return Err(PacketSinkError::TimestampOverflow { stream_index });
            }
            storage.push(offset);
        }
        let offsets = std::mem::take(storage);
        *self = Timeline::Anchored { offsets };
        Ok(())
    }

    /// This stream's committed shift. Panics if unanchored — callers anchor
    /// first (`ensure_anchored` precedes every use on the packet path).
    pub(crate) fn offset(&self, stream_index: usize) -> i64 {
        match self {
            Timeline::Anchored { offsets } => offsets[stream_index],
            Timeline::Unanchored { .. } => unreachable!("timeline queried before anchoring"),
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

/// Preallocation hint for the pending-pts window. H.264 caps the decoded
/// picture buffer at 16 frames, so a compliant encoder's reorder window
/// never exceeds 16 entries (audio never reorders: the window stays at 1).
/// A deeper window merely grows the `Vec` past the hint.
const PENDING_PTS_CAPACITY: usize = 16;

impl StreamTimeline {
    pub(crate) fn new() -> Self {
        Self {
            last_dts: None,
            pending_pts: Vec::with_capacity(PENDING_PTS_CAPACITY),
        }
    }

    /// Validates one shifted `(pts, dts)` pair and records it: `pts >= dts`,
    /// strictly increasing dts, and no duplicate pts. The duplicate probe
    /// covers every recorded value, including those pruned in the same pass —
    /// a recorded pts equal to the NEW dts is exactly the boundary a
    /// duplicate can still collide with (e.g. (pts 3, dts 0) followed by
    /// (pts 3, dts 3)); pruning without probing would forget it.
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
        // One pass serves both the duplicate probe and the prune: `retain`
        // visits every recorded value exactly once, so the probe still sees
        // the full pre-prune set. On the duplicate error the prune is
        // already committed while `last_dts` and the push are not; that
        // mixed state is never repaired because a delivery error stops the
        // job's packet flow — nothing observes this timeline again.
        let mut duplicate = false;
        self.pending_pts.retain(|&p| {
            duplicate |= p == pts;
            p > dts
        });
        if duplicate {
            return Err(PacketSinkError::DuplicatePts { stream_index, pts });
        }
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
        let mut tl = Timeline::new(2);
        tl.ensure_anchored(5, tb25, &[tb25, tb_audio]).unwrap();
        assert_eq!(tl.offset(0), 5);
        assert_eq!(tl.offset(1), 8820);
        // Second anchor attempt is a no-op (offsets unchanged).
        tl.ensure_anchored(100, tb25, &[tb25, tb_audio]).unwrap();
        assert_eq!(tl.offset(0), 5);
    }

    #[test]
    fn anchor_overflow_leaves_the_timeline_unanchored_and_reusable() {
        let tb1 = AVRational { num: 1, den: 1 };
        let tb90k = AVRational { num: 1, den: 90000 };
        let mut tl = Timeline::new(2);
        // The second stream's rescale overflows: the transition must commit
        // nothing (all-or-nothing) and report the offending stream.
        assert!(matches!(
            tl.ensure_anchored(i64::MAX / 2, tb1, &[tb1, tb90k]),
            Err(PacketSinkError::TimestampOverflow { stream_index: 1 })
        ));
        assert!(matches!(tl, Timeline::Unanchored { .. }));
        // A later anchor starts from scratch: nothing partially filled ahead
        // of the overflow leaks into the committed offsets.
        tl.ensure_anchored(5, tb1, &[tb1, tb90k]).unwrap();
        assert_eq!(tl.offset(0), 5);
        assert_eq!(tl.offset(1), 450_000);
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
