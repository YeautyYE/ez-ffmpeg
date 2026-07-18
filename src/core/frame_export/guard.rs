//! Input-side per-frame color guard: runtime HDR rejection plus the
//! [`ColorPolicy::TaggedOrResolutionGuess`] stamp.
//!
//! Placed on the input frame pipeline (pre-filtergraph) for every run, so it
//! sees frames BEFORE any YUV→RGB conversion, in every sampling mode:
//!
//! - **HDR guard.** The resolver's open-time check reads stream parameters
//!   only; a mid-stream splice to HDR (e.g. MPEG-TS ad insertion) never shows
//!   up there. This filter re-checks every decoded frame's color properties
//!   and fails the run with the typed
//!   [`FrameExportError::HdrRequiresToneMapping`] instead of silently
//!   producing wrong colors.
//! - **Resolution-guess stamp.** Under `TaggedOrResolutionGuess`, frames with
//!   an UNSPECIFIED colorspace get one guessed from their height (BT.709 for
//!   `height >= 720`, BT.601 otherwise), and an UNSPECIFIED range is pinned to
//!   limited. Real tags are never overridden, and the guess never freezes: the
//!   stamp is per frame, and a stamped-value change rides the filtergraph's
//!   existing input-reinit path (which re-checks `colorspace`/`color_range`
//!   against the previous frame), so `scale`'s `in_color_matrix=auto` always
//!   sees the current value.
//!
//! The stream binding follows the input pipeline's rules: an explicit
//! `video_stream_index` pins the stream; otherwise the pipeline binds to the
//! first video stream, which is the exported stream except on exotic
//! multi-video inputs (see the resolver for how that mismatch is handled).

use super::error::FrameExportError;
use super::options::ColorPolicy;
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    AVColorPrimaries, AVColorRange, AVColorSpace, AVColorTransferCharacteristic, AVMediaType,
    AVMediaType::AVMEDIA_TYPE_VIDEO,
};

/// Whether a color-property triplet marks HDR content (BT.2020 matrix, PQ or
/// HLG transfer, or BT.2020 primaries). Shared by the resolver's open-time
/// stream-parameter check and this filter's per-frame check, so the two can
/// never drift apart.
pub(crate) fn is_hdr(
    colorspace: AVColorSpace,
    trc: AVColorTransferCharacteristic,
    primaries: AVColorPrimaries,
) -> bool {
    colorspace == AVColorSpace::AVCOL_SPC_BT2020_NCL
        || colorspace == AVColorSpace::AVCOL_SPC_BT2020_CL
        || trc == AVColorTransferCharacteristic::AVCOL_TRC_SMPTE2084
        || trc == AVColorTransferCharacteristic::AVCOL_TRC_ARIB_STD_B67
        || primaries == AVColorPrimaries::AVCOL_PRI_BT2020
}

/// The classic resolution heuristic for untagged content: HD-sized frames
/// (`height >= 720`) are assumed BT.709, everything smaller BT.601
/// (SMPTE 170M — swscale selects the same coefficients for all 601-family
/// tags).
fn guessed_colorspace(height: i32) -> AVColorSpace {
    if height >= 720 {
        AVColorSpace::AVCOL_SPC_BT709
    } else {
        AVColorSpace::AVCOL_SPC_SMPTE170M
    }
}

/// Per-frame HDR guard + `TaggedOrResolutionGuess` stamp (input side).
pub(crate) struct ColorGuard {
    policy: ColorPolicy,
}

impl ColorGuard {
    pub(crate) fn new(policy: ColorPolicy) -> Self {
        Self { policy }
    }
}

impl FrameFilter for ColorGuard {
    fn media_type(&self) -> AVMediaType {
        AVMEDIA_TYPE_VIDEO
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        mut frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // Flush cues carry no pixels to guard; pass them through untouched so
        // downstream filters (the UniformN sampler) still see their cue.
        if frame_is_eof_marker(&frame) {
            return Ok(Some(frame));
        }
        // SAFETY: null-checked before any deref.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(Some(frame));
        }

        // Authoritative HDR check, BEFORE any stamping: the open-time probe
        // only sees declared stream parameters, so a mid-stream splice to HDR
        // must be caught here — on the pre-conversion frame — or the output
        // would be silently tone-distorted.
        let (colorspace, trc, primaries) =
            unsafe { ((*p).colorspace, (*p).color_trc, (*p).color_primaries) };
        if is_hdr(colorspace, trc, primaries) {
            return Err(Box::new(FrameExportError::HdrRequiresToneMapping));
        }

        if matches!(self.policy, ColorPolicy::TaggedOrResolutionGuess) {
            // Fill ONLY unspecified properties; real tags always win. These are
            // per-reference struct fields (not shared data buffers), so writing
            // them affects exactly the reference this pipeline owns.
            // SAFETY: `p` was null-checked above; as_mut_ptr on the owned frame
            // yields the same valid AVFrame.
            unsafe {
                let p = frame.as_mut_ptr();
                if (*p).colorspace == AVColorSpace::AVCOL_SPC_UNSPECIFIED {
                    (*p).colorspace = guessed_colorspace((*p).height);
                }
                if (*p).color_range == AVColorRange::AVCOL_RANGE_UNSPECIFIED {
                    (*p).color_range = AVColorRange::AVCOL_RANGE_MPEG;
                }
            }
        }

        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::frame_export::options::{YuvMatrix, YuvRange};
    use ffmpeg_sys_next::{av_frame_alloc, av_frame_get_buffer, AVPixelFormat};
    use AVColorPrimaries::{AVCOL_PRI_BT2020, AVCOL_PRI_BT709, AVCOL_PRI_UNSPECIFIED};
    use AVColorRange::{AVCOL_RANGE_JPEG, AVCOL_RANGE_MPEG, AVCOL_RANGE_UNSPECIFIED};
    use AVColorSpace::{
        AVCOL_SPC_BT2020_CL, AVCOL_SPC_BT2020_NCL, AVCOL_SPC_BT709, AVCOL_SPC_SMPTE170M,
        AVCOL_SPC_UNSPECIFIED,
    };
    use AVColorTransferCharacteristic::{
        AVCOL_TRC_ARIB_STD_B67, AVCOL_TRC_BT709, AVCOL_TRC_SMPTE2084, AVCOL_TRC_UNSPECIFIED,
    };

    /// A real decodable-shaped frame (allocated buffers, so it is NOT an EOF
    /// marker) with the given size and color properties.
    fn test_frame(width: i32, height: i32, colorspace: AVColorSpace, range: AVColorRange) -> Frame {
        // SAFETY: fresh alloc, sized, buffered; wrapped so Drop frees it.
        unsafe {
            let p = av_frame_alloc();
            assert!(!p.is_null());
            (*p).width = width;
            (*p).height = height;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P as i32;
            assert!(av_frame_get_buffer(p, 0) >= 0, "test frame buffer");
            (*p).colorspace = colorspace;
            (*p).color_range = range;
            Frame::wrap(p)
        }
    }

    /// Runs one frame through a guard, returning the observed
    /// (colorspace, range) of the output frame.
    fn stamp(
        policy: ColorPolicy,
        frame: Frame,
    ) -> Result<(AVColorSpace, AVColorRange), FrameFilterError> {
        let mut guard = ColorGuard::new(policy);
        let mut attrs = std::collections::HashMap::new();
        let mut ctx = FrameFilterContext::new("test", &mut attrs);
        let out = guard
            .filter_frame(frame, &mut ctx)?
            .expect("guard forwards real frames");
        // SAFETY: the guard returned the (non-null) frame it was given.
        unsafe {
            let p = out.as_ptr();
            Ok(((*p).colorspace, (*p).color_range))
        }
    }

    #[test]
    fn stamp_fills_unspecified_colorspace_and_range() {
        // The load-bearing writes, asserted directly: dropping either the
        // colorspace or the range stamp turns this red.
        let (cs, range) = stamp(
            ColorPolicy::TaggedOrResolutionGuess,
            test_frame(64, 64, AVCOL_SPC_UNSPECIFIED, AVCOL_RANGE_UNSPECIFIED),
        )
        .expect("sdr frame passes");
        assert_eq!(cs, AVCOL_SPC_SMPTE170M, "SD height guesses 601");
        assert_eq!(range, AVCOL_RANGE_MPEG, "unspecified range pins limited");

        let (cs, _) = stamp(
            ColorPolicy::TaggedOrResolutionGuess,
            test_frame(1280, 720, AVCOL_SPC_UNSPECIFIED, AVCOL_RANGE_UNSPECIFIED),
        )
        .expect("sdr frame passes");
        assert_eq!(cs, AVCOL_SPC_BT709, "HD height guesses 709");
    }

    #[test]
    fn stamp_never_overrides_real_tags() {
        // A tagged colorspace survives even when the height disagrees with it,
        // and a REAL full-range tag is never clobbered by the limited pin.
        let (cs, range) = stamp(
            ColorPolicy::TaggedOrResolutionGuess,
            test_frame(1920, 1080, AVCOL_SPC_SMPTE170M, AVCOL_RANGE_JPEG),
        )
        .expect("sdr frame passes");
        assert_eq!(cs, AVCOL_SPC_SMPTE170M, "tagged matrix wins over the guess");
        assert_eq!(range, AVCOL_RANGE_JPEG, "full-range tag must survive");

        // Mixed: untagged matrix + tagged range — only the matrix is filled.
        let (cs, range) = stamp(
            ColorPolicy::TaggedOrResolutionGuess,
            test_frame(64, 64, AVCOL_SPC_UNSPECIFIED, AVCOL_RANGE_JPEG),
        )
        .expect("sdr frame passes");
        assert_eq!(cs, AVCOL_SPC_SMPTE170M);
        assert_eq!(range, AVCOL_RANGE_JPEG);
    }

    #[test]
    fn tagged_and_force_policies_stamp_nothing() {
        for policy in [
            ColorPolicy::Tagged,
            ColorPolicy::Force {
                matrix: YuvMatrix::Bt709,
                range: YuvRange::Full,
            },
        ] {
            let (cs, range) = stamp(
                policy,
                test_frame(64, 64, AVCOL_SPC_UNSPECIFIED, AVCOL_RANGE_UNSPECIFIED),
            )
            .expect("sdr frame passes");
            assert_eq!(cs, AVCOL_SPC_UNSPECIFIED, "{policy:?} must not stamp");
            assert_eq!(range, AVCOL_RANGE_UNSPECIFIED, "{policy:?} must not stamp");
        }
    }

    #[test]
    fn hdr_frame_is_typed_error_in_every_policy() {
        for policy in [
            ColorPolicy::Tagged,
            ColorPolicy::TaggedOrResolutionGuess,
            ColorPolicy::Force {
                matrix: YuvMatrix::Bt601,
                range: YuvRange::Limited,
            },
        ] {
            let err = stamp(
                policy,
                test_frame(64, 64, AVCOL_SPC_BT2020_NCL, AVCOL_RANGE_MPEG),
            )
            .expect_err("BT.2020 frame must be rejected");
            let typed = err.downcast_ref::<FrameExportError>();
            assert!(
                matches!(typed, Some(FrameExportError::HdrRequiresToneMapping)),
                "{policy:?}: expected typed HDR error, got {err:?}"
            );
        }
    }

    #[test]
    fn hdr_predicate_matches_bt2020_pq_hlg() {
        // Each marker alone is enough.
        assert!(is_hdr(
            AVCOL_SPC_BT2020_NCL,
            AVCOL_TRC_UNSPECIFIED,
            AVCOL_PRI_UNSPECIFIED
        ));
        assert!(is_hdr(
            AVCOL_SPC_BT2020_CL,
            AVCOL_TRC_UNSPECIFIED,
            AVCOL_PRI_UNSPECIFIED
        ));
        assert!(is_hdr(
            AVCOL_SPC_UNSPECIFIED,
            AVCOL_TRC_SMPTE2084,
            AVCOL_PRI_UNSPECIFIED
        ));
        assert!(is_hdr(
            AVCOL_SPC_UNSPECIFIED,
            AVCOL_TRC_ARIB_STD_B67,
            AVCOL_PRI_UNSPECIFIED
        ));
        assert!(is_hdr(
            AVCOL_SPC_UNSPECIFIED,
            AVCOL_TRC_UNSPECIFIED,
            AVCOL_PRI_BT2020
        ));
    }

    #[test]
    fn hdr_predicate_passes_sdr() {
        assert!(!is_hdr(AVCOL_SPC_BT709, AVCOL_TRC_BT709, AVCOL_PRI_BT709));
        assert!(!is_hdr(
            AVCOL_SPC_SMPTE170M,
            AVCOL_TRC_UNSPECIFIED,
            AVCOL_PRI_UNSPECIFIED
        ));
        assert!(!is_hdr(
            AVCOL_SPC_UNSPECIFIED,
            AVCOL_TRC_UNSPECIFIED,
            AVCOL_PRI_UNSPECIFIED
        ));
    }

    #[test]
    fn resolution_guess_splits_at_720() {
        assert_eq!(guessed_colorspace(480), AVCOL_SPC_SMPTE170M);
        assert_eq!(guessed_colorspace(576), AVCOL_SPC_SMPTE170M);
        assert_eq!(guessed_colorspace(719), AVCOL_SPC_SMPTE170M);
        assert_eq!(guessed_colorspace(720), AVCOL_SPC_BT709);
        assert_eq!(guessed_colorspace(1080), AVCOL_SPC_BT709);
        assert_eq!(guessed_colorspace(2160), AVCOL_SPC_BT709);
    }
}
