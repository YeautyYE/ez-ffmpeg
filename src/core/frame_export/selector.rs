//! Input-side frame selector for [`Sampling::EveryNth`] / [`Sampling::EverySec`].
//!
//! Placed on the input frame pipeline (pre-filtergraph), like the UniformN
//! sampler: unselected frames are consumed here and never pay the graph's
//! `scale`/`format` conversion. Selecting downstream of the graph instead
//! meant `EverySec(1.0)` on a 30 fps input ran 30 full-cost conversions per
//! second of content to keep one — swscale at source resolution costs the same
//! order as the decode itself.
//!
//! Selection reads the decoder-era timestamp (via [`SamplingClock`]), so on
//! VFR containers the decision uses the container's real timing rather than
//! the vsync stage's frame-rate grid. Both selection rules are the exact
//! arithmetic the output sink used before this move:
//! - `EveryNth(n)`: a countdown selecting frame 0, n, 2n, ...
//! - `EverySec(s)`: the first delivered frame anchors the grid; a frame is
//!   selected when it reaches the next grid point, which then advances in O(1)
//!   to the smallest grid point strictly past the frame.
//!
//! Trim boundary: as with the UniformN sampler, frames below the boundary are
//! lead-in that the in-graph trim discards after this pipeline; selecting (or
//! grid-anchoring on) them would waste selections on frames that never
//! survive, so they are consumed outright.

use super::options::Sampling;
use super::sampler::SamplingClock;
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{AVMediaType, AVMediaType::AVMEDIA_TYPE_VIDEO};

/// The selection rule and its state.
enum SelectorMode {
    /// Countdown to the next selection (0 means take this frame).
    Nth { n: u64, countdown: u64 },
    /// `EverySec` grid. `next_target_us` is `i128` so a boundary past
    /// `i64::MAX` moves beyond every possible PTS (correctly ending selection)
    /// instead of saturating and re-selecting.
    Sec {
        step_us: i64,
        next_target_us: Option<i128>,
    },
}

/// Input-side selector for the sparse sampling modes.
pub(crate) struct InputSelector {
    mode: SelectorMode,
    /// Frames with sampling PTS below this are lead-in the in-graph trim will
    /// drop (`Some(0)` when `start_time_us` re-zeroes the timeline); skipped.
    trim_boundary_us: Option<i64>,
    clock: SamplingClock,
    anchored: bool,
}

impl InputSelector {
    /// Returns a selector for `sampling`, or `None` for modes that do not
    /// select on the input side (`All`, `KeyframesOnly`, `UniformN`).
    pub(crate) fn for_sampling(
        sampling: &Sampling,
        trim_boundary_us: Option<i64>,
    ) -> Option<Self> {
        let mode = match sampling {
            // n >= 1 is validated at build time; guard anyway.
            Sampling::EveryNth(n) => SelectorMode::Nth {
                n: (*n).max(1),
                countdown: 0,
            },
            Sampling::EverySec(s) => SelectorMode::Sec {
                step_us: ((s * 1_000_000.0) as i64).max(1),
                next_target_us: None,
            },
            _ => return None,
        };
        Some(Self {
            mode,
            trim_boundary_us,
            clock: SamplingClock::new(),
            anchored: false,
        })
    }

    /// Advances the selection state for a frame at `pts_us` and decides.
    fn select(&mut self, pts_us: i64) -> bool {
        match &mut self.mode {
            SelectorMode::Nth { n, countdown } => {
                if *countdown == 0 {
                    *countdown = *n - 1;
                    true
                } else {
                    *countdown -= 1;
                    false
                }
            }
            SelectorMode::Sec {
                step_us,
                next_target_us,
            } => {
                let pts = pts_us as i128;
                let step = *step_us as i128;
                match *next_target_us {
                    // First delivered frame anchors the grid (C1).
                    None => {
                        *next_target_us = Some(pts + step);
                        true
                    }
                    Some(target) => {
                        if pts >= target {
                            // Advance in O(1) to the smallest grid point strictly
                            // past this frame. All-i128, unclamped: a huge gap can
                            // neither hang nor overflow, and a boundary beyond
                            // i64::MAX simply exceeds every future PTS. Each source
                            // frame is selected at most once even across many steps.
                            let advance = ((pts - target) / step + 1) * step;
                            *next_target_us = Some(target + advance);
                            true
                        } else {
                            false
                        }
                    }
                }
            }
        }
    }
}

impl FrameFilter for InputSelector {
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
        // End-of-stream markers pass through untouched (the sink handles them).
        if frame_is_eof_marker(&frame) {
            return Ok(Some(frame));
        }
        // SAFETY: null-checked before any deref.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(Some(frame));
        }

        let pts = unsafe { self.clock.pts_us(p, self.anchored) };
        if self.trim_boundary_us.is_some_and(|b| pts < b) {
            return Ok(None);
        }
        self.anchored = true;

        if self.select(pts) {
            Ok(Some(frame))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sec(step_us: i64) -> InputSelector {
        InputSelector::for_sampling(&Sampling::EverySec(step_us as f64 / 1_000_000.0), None)
            .expect("EverySec selects input-side")
    }

    #[test]
    fn nth_counts_down_from_the_first_frame() {
        let mut s = InputSelector::for_sampling(&Sampling::EveryNth(3), None).unwrap();
        let picks: Vec<bool> = (0..7).map(|i| s.select(i * 100)).collect();
        assert_eq!(picks, [true, false, false, true, false, false, true]);
    }

    #[test]
    fn sec_anchors_then_advances_in_o1() {
        let mut s = sec(1_000_000);
        assert!(s.select(0), "first frame anchors and is selected");
        assert!(!s.select(400_000));
        assert!(!s.select(999_999));
        assert!(s.select(1_000_000), "boundary at the exact tick is selected");
        // A huge gap selects once and lands the grid past the frame.
        assert!(s.select(10_000_000));
        assert!(!s.select(10_500_000));
        assert!(s.select(11_000_000));
    }

    #[test]
    fn all_keyframes_uniform_have_no_input_selector() {
        for sampling in [
            Sampling::All,
            Sampling::KeyframesOnly,
            Sampling::UniformN(4),
        ] {
            assert!(InputSelector::for_sampling(&sampling, None).is_none());
        }
    }
}
