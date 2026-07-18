//! Crate-private stream/HDR resolver and filter-graph assembly.
//!
//! Runs as a deferred closure against the already-opened demuxer (S8): it
//! selects the video stream with `av_find_best_stream` semantics (or validates
//! an explicit index), fast-fails HDR inputs from codec parameters, and builds
//! the `scale`/`format` graph description referencing the *resolved absolute*
//! stream index — never the `[0:v]` first-match linklabel.

use super::error::FrameExportError;
use super::options::{ColorPolicy, PixelLayout};
use super::sampler::UniformSpan;
use ffmpeg_sys_next::{
    av_find_best_stream, av_rescale_q, AVColorPrimaries, AVColorSpace,
    AVColorTransferCharacteristic, AVFormatContext, AVMediaType::AVMEDIA_TYPE_VIDEO,
    AV_NOPTS_VALUE, AV_TIME_BASE_Q,
};
use std::ptr::null_mut;

/// Configuration the resolver needs, captured from the builder.
pub(crate) struct ResolvePlan {
    pub(crate) stream_index: Option<usize>,
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) pixel: PixelLayout,
    pub(crate) color: ColorPolicy,
    /// Present for `UniformN`: resolve the grid span and publish it.
    pub(crate) uniform: Option<UniformResolve>,
}

/// What the resolver needs to compute and publish the UniformN grid span.
pub(crate) struct UniformResolve {
    pub(crate) span_cell: UniformSpan,
    pub(crate) duration_hint_us: Option<i64>,
    pub(crate) duration_us: Option<i64>,
    pub(crate) start_time_us: Option<i64>,
}

/// Resolves the stream against `fmt_ctx`, fast-fails HDR, and returns the
/// assembled `filter_complex` string.
///
/// # Safety
/// `fmt_ctx` must be a valid, opened `AVFormatContext` pointer for the duration
/// of this call.
pub(crate) unsafe fn resolve_and_build_desc(
    fmt_ctx: *mut AVFormatContext,
    plan: &ResolvePlan,
) -> crate::error::Result<String> {
    let stream_index = resolve_stream_index(fmt_ctx, plan.stream_index)?;
    if plan.uniform.is_some() {
        // The input-side sampler binds by MEDIA TYPE when no explicit index is
        // given, while this graph uses the best stream. With more than one video
        // stream the two can disagree — the sampler would sample one stream while
        // the graph exports another, breaking the exact-N contract. Reject the
        // ambiguity instead of silently mis-sampling (checked before the HDR
        // fast-fail so the actionable error wins on ambiguous HDR inputs).
        if plan.stream_index.is_none() && count_video_streams(fmt_ctx) > 1 {
            return Err(FrameExportError::InvalidOption(
                "UniformN on an input with multiple video streams requires video_stream_index()"
                    .to_string(),
            )
            .into());
        }
    }
    // Unconditional for every sampling mode (the ambiguity check above only
    // runs first so its actionable error wins on ambiguous HDR inputs).
    hdr_fast_fail(fmt_ctx, stream_index)?;
    if let Some(u) = &plan.uniform {
        let span = resolve_span(fmt_ctx, stream_index, u)?;
        // Filled before start(); the input-side sampler reads it at frame 0.
        let _ = u.span_cell.set(span);
    }
    Ok(build_filter_desc(stream_index, plan))
}

/// Number of video streams (any disposition, including attached pictures — the
/// input-side pipeline's media-type binding does not distinguish them either).
///
/// # Safety
/// `fmt_ctx` must be a valid, opened `AVFormatContext` pointer.
unsafe fn count_video_streams(fmt_ctx: *mut AVFormatContext) -> usize {
    let nb = (*fmt_ctx).nb_streams as usize;
    (0..nb)
        .filter(|&i| {
            let stream = *(*fmt_ctx).streams.add(i);
            let par = (*stream).codecpar;
            !par.is_null() && (*par).codec_type == AVMEDIA_TYPE_VIDEO
        })
        .count()
}

/// Resolves the UniformN grid span in microseconds:
/// `duration_us` (trim window) > `duration_hint_us` > selected-stream duration
/// > container duration, else [`FrameExportError::UnknownDuration`].
///
/// # Safety
/// `fmt_ctx` must be valid and `stream_index` in range.
unsafe fn resolve_span(
    fmt_ctx: *mut AVFormatContext,
    stream_index: usize,
    u: &UniformResolve,
) -> crate::error::Result<i64> {
    if let Some(d) = u.duration_us {
        if d > 0 {
            return Ok(d);
        }
    }
    if let Some(h) = u.duration_hint_us {
        if h > 0 {
            return Ok(h);
        }
    }
    // Probed fallbacks describe the WHOLE input, but the grid anchors at the
    // seek point — subtract the requested start so a start-only UniformN covers
    // the remaining content instead of aiming targets beyond EOF (which would
    // over-pad the tail with duplicates of the last frame).
    let start = u.start_time_us.unwrap_or(0).max(0);
    let mut probed: Option<i64> = None;
    let stream = *(*fmt_ctx).streams.add(stream_index);
    let sdur = (*stream).duration;
    if sdur != AV_NOPTS_VALUE && sdur > 0 {
        let us = av_rescale_q(sdur, (*stream).time_base, AV_TIME_BASE_Q);
        if us > 0 {
            probed = Some(us);
        }
    }
    if probed.is_none() {
        let cdur = (*fmt_ctx).duration;
        if cdur != AV_NOPTS_VALUE && cdur > 0 {
            // AVFormatContext.duration is already in AV_TIME_BASE (µs) units.
            probed = Some(cdur);
        }
    }
    match probed {
        Some(dur) => {
            let span = dur.saturating_sub(start);
            if span > 0 {
                Ok(span)
            } else {
                Err(FrameExportError::InvalidOption(format!(
                    "start_time_us ({start}) is at or beyond the input duration ({dur})"
                ))
                .into())
            }
        }
        None => Err(FrameExportError::UnknownDuration.into()),
    }
}

/// Selects the video stream: an explicit index is range/type-validated; the
/// default uses `av_find_best_stream` (which skips attached-pic and honors the
/// default disposition).
///
/// # Safety
/// `fmt_ctx` must be a valid, opened `AVFormatContext` pointer.
unsafe fn resolve_stream_index(
    fmt_ctx: *mut AVFormatContext,
    explicit: Option<usize>,
) -> crate::error::Result<usize> {
    let nb_streams = (*fmt_ctx).nb_streams as usize;
    if let Some(index) = explicit {
        if index >= nb_streams {
            return Err(FrameExportError::StreamIndexOutOfBounds {
                index,
                count: nb_streams,
            }
            .into());
        }
        let stream = *(*fmt_ctx).streams.add(index);
        let codecpar = (*stream).codecpar;
        if codecpar.is_null() || (*codecpar).codec_type != AVMEDIA_TYPE_VIDEO {
            return Err(FrameExportError::NotAVideoStream { index }.into());
        }
        Ok(index)
    } else {
        let ret = av_find_best_stream(fmt_ctx, AVMEDIA_TYPE_VIDEO, -1, -1, null_mut(), 0);
        if ret < 0 {
            return Err(FrameExportError::NoVideoStream.into());
        }
        Ok(ret as usize)
    }
}

/// Rejects HDR inputs (BT.2020 / PQ / HLG) from the OPEN-TIME stream parameters
/// only. There is no per-frame runtime check yet, so a mid-stream splice to HDR
/// (e.g. MPEG-TS ad insertion) is not detected — that guard lands with the
/// per-frame color-stamp work.
///
/// # Safety
/// `fmt_ctx` must be valid and `stream_index` in range.
unsafe fn hdr_fast_fail(
    fmt_ctx: *mut AVFormatContext,
    stream_index: usize,
) -> crate::error::Result<()> {
    let stream = *(*fmt_ctx).streams.add(stream_index);
    let codecpar = (*stream).codecpar;
    if codecpar.is_null() {
        return Ok(());
    }
    let cs = (*codecpar).color_space;
    let trc = (*codecpar).color_trc;
    let pri = (*codecpar).color_primaries;
    let hdr = cs == AVColorSpace::AVCOL_SPC_BT2020_NCL
        || cs == AVColorSpace::AVCOL_SPC_BT2020_CL
        || trc == AVColorTransferCharacteristic::AVCOL_TRC_SMPTE2084
        || trc == AVColorTransferCharacteristic::AVCOL_TRC_ARIB_STD_B67
        || pri == AVColorPrimaries::AVCOL_PRI_BT2020;
    if hdr {
        return Err(FrameExportError::HdrRequiresToneMapping.into());
    }
    Ok(())
}

/// Assembles the `filter_complex` string for the resolved stream.
fn build_filter_desc(stream_index: usize, plan: &ResolvePlan) -> String {
    let size = match (plan.width, plan.height) {
        (Some(w), Some(h)) => format!("{w}:{h}"),
        (Some(w), None) => format!("{w}:-2"),
        (None, Some(h)) => format!("-2:{h}"),
        (None, None) => "iw:ih".to_string(),
    };
    let (matrix, range) = match plan.color {
        ColorPolicy::Tagged => ("auto".to_string(), "auto".to_string()),
        ColorPolicy::Force { matrix, range } => (
            matrix.in_color_matrix().to_string(),
            range.in_range().to_string(),
        ),
    };
    // `[0:{idx}]` = input 0, ABSOLUTE stream index. `[{idx}:v]` would be wrong:
    // there the leading number is the INPUT-file index, so a resolved video at
    // stream 1 of a single input would reference a nonexistent input 1.
    format!(
        "[0:{stream_index}]scale={size}:flags=bicubic+accurate_rnd+full_chroma_int:\
         in_color_matrix={matrix}:in_range={range}:out_range=full,format={pix}[export]",
        pix = plan.pixel.ffmpeg_format_name()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::frame_export::options::{YuvMatrix, YuvRange};

    fn plan(
        width: Option<u32>,
        height: Option<u32>,
        pixel: PixelLayout,
        color: ColorPolicy,
    ) -> ResolvePlan {
        ResolvePlan {
            stream_index: None,
            width,
            height,
            pixel,
            color,
            uniform: None,
        }
    }

    #[test]
    fn tagged_rgb_no_resize() {
        let d = build_filter_desc(
            0,
            &plan(None, None, PixelLayout::Rgb24, ColorPolicy::Tagged),
        );
        assert_eq!(
            d,
            "[0:0]scale=iw:ih:flags=bicubic+accurate_rnd+full_chroma_int:\
             in_color_matrix=auto:in_range=auto:out_range=full,format=rgb24[export]"
        );
    }

    #[test]
    fn force_709_full_resized_width_only() {
        let color = ColorPolicy::Force {
            matrix: YuvMatrix::Bt709,
            range: YuvRange::Full,
        };
        let d = build_filter_desc(3, &plan(Some(336), None, PixelLayout::Rgb24, color));
        assert!(d.starts_with("[0:3]scale=336:-2:"), "{d}");
        assert!(
            d.contains("in_color_matrix=bt709:in_range=pc:out_range=full"),
            "{d}"
        );
        assert!(d.ends_with("format=rgb24[export]"), "{d}");
    }

    #[test]
    fn gray_and_rgba_pixel_names() {
        let g = build_filter_desc(
            0,
            &plan(None, None, PixelLayout::Gray8, ColorPolicy::Tagged),
        );
        assert!(g.ends_with("format=gray[export]"), "{g}");
        let a = build_filter_desc(
            0,
            &plan(None, None, PixelLayout::Rgba32, ColorPolicy::Tagged),
        );
        assert!(a.ends_with("format=rgba[export]"), "{a}");
    }

    #[test]
    fn height_only_resize() {
        let d = build_filter_desc(
            1,
            &plan(None, Some(224), PixelLayout::Rgb24, ColorPolicy::Tagged),
        );
        assert!(d.starts_with("[0:1]scale=-2:224:"), "{d}");
    }
}
