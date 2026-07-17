//! Crate-private stream/HDR resolver and filter-graph assembly.
//!
//! Runs as a deferred closure against the already-opened demuxer (S8): it
//! selects the video stream with `av_find_best_stream` semantics (or validates
//! an explicit index), fast-fails HDR inputs from codec parameters, and builds
//! the `scale`/`format` graph description referencing the *resolved absolute*
//! stream index — never the `[0:v]` first-match linklabel.

use super::error::FrameExportError;
use super::options::{ColorPolicy, PixelLayout};
use ffmpeg_sys_next::{
    av_find_best_stream, AVColorPrimaries, AVColorSpace, AVColorTransferCharacteristic,
    AVFormatContext, AVMediaType::AVMEDIA_TYPE_VIDEO,
};
use std::ptr::null_mut;

/// Configuration the resolver needs, captured from the builder.
pub(crate) struct ResolvePlan {
    pub(crate) stream_index: Option<usize>,
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) pixel: PixelLayout,
    pub(crate) color: ColorPolicy,
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
    hdr_fast_fail(fmt_ctx, stream_index)?;
    Ok(build_filter_desc(stream_index, plan))
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

/// Rejects HDR inputs (BT.2020 / PQ / HLG) at build time. The per-frame runtime
/// check remains authoritative for mid-stream splices.
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
