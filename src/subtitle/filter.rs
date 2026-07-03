//! The subtitle burn-in frame filter.

use super::backend::SubtitleRenderer;
use super::blend::{self, ColorMatrix, ColorRange, OverlayImage, PlaneView, SampleFormat};
use super::layout::{self, ColorModel, ComponentPlacement, FormatSpec};
use super::options::SubtitleFilterBuilder;
use crate::core::filter::frame_filter::FrameFilter;
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::av_err2str;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_frame_make_writable, av_get_pix_fmt_name, av_rescale_q, AVColorRange, AVColorSpace, AVFrame,
    AVMediaType, AVPixelFormat, AVRational, AV_NOPTS_VALUE,
};
use std::ffi::CStr;

/// Burns ASS/SRT subtitles onto video frames inside a frame pipeline.
///
/// Build one with [`SubtitleFilter::builder`], then attach it to an output
/// (recommended: subtitles are rendered against the final, post-filter-graph
/// geometry) via a frame pipeline:
///
/// ```rust,ignore
/// use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
/// use ez_ffmpeg::subtitle::SubtitleFilter;
/// use ez_ffmpeg::{AVMediaType, FfmpegContext, Output};
///
/// let filter = SubtitleFilter::builder()
///     .ass_content(ass_script)
///     .build()?;
/// let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
/// FfmpegContext::builder()
///     .input("input.mp4")
///     .output(Output::from("output.mp4")
///         .add_frame_pipeline(pipeline.filter("subtitles", Box::new(filter))))
///     .build()?.start()?.wait()?;
/// ```
///
/// Timing comes from each frame's `pts` × `time_base` (the scheduler keeps
/// both valid on either pipeline side), so subtitle timestamps line up with
/// the post-filter-graph timeline exactly like FFmpeg's own `subtitles`
/// filter.
pub struct SubtitleFilter {
    /// The rendering backend (the pure-Rust renderer; only ever reached
    /// through the [`SubtitleRenderer`] trait).
    renderer: Box<dyn SubtitleRenderer>,
    /// Authoring resolution for aspect compensation (FFmpeg `original_size`).
    original_size: Option<(u32, u32)>,
    /// Frame geometry the renderer is currently configured for.
    configured_dims: Option<(i32, i32)>,
    /// Reusable per-node mask-sum buffer for the two-phase pooled blend
    /// (grows to the largest node once, then no per-frame allocation).
    pool_scratch: Vec<u16>,
    warned_missing_time: bool,
    warned_colorspace: bool,
    /// Matrix/range locked on the first timed frame (vf_subtitles wires
    /// ff_draw once in config_input; per-frame drift is ignored). `None`
    /// range = unspecified, resolved by the format default at blend time.
    locked_color: Option<(ColorMatrix, Option<ColorRange>)>,
}

impl std::fmt::Debug for SubtitleFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubtitleFilter")
            .field("original_size", &self.original_size)
            .finish_non_exhaustive()
    }
}

impl SubtitleFilter {
    /// Starts configuring a subtitle filter.
    pub fn builder() -> SubtitleFilterBuilder {
        SubtitleFilterBuilder::default()
    }

    /// Test access to the rendering backend.
    #[cfg(test)]
    pub(crate) fn renderer_mut(&mut self) -> &mut dyn SubtitleRenderer {
        self.renderer.as_mut()
    }

    pub(crate) fn new(
        renderer: Box<dyn SubtitleRenderer>,
        original_size: Option<(u32, u32)>,
    ) -> Self {
        Self {
            renderer,
            original_size,
            configured_dims: None,
            pool_scratch: Vec::new(),
            warned_missing_time: false,
            warned_colorspace: false,
            locked_color: None,
        }
    }

    /// FFmpeg vf_subtitles `config_input` semantics (n7.1.3): frame size is
    /// the real geometry; with `original_size` the storage size is the
    /// authoring resolution plus a pixel-aspect compensation.
    fn configure_renderer(&mut self, width: i32, height: i32) {
        if self.configured_dims == Some((width, height)) {
            return;
        }
        self.configured_dims = Some((width, height));
        self.renderer.set_frame_size(width, height);
        match self.original_size {
            Some((original_w, original_h)) if original_w > 0 && original_h > 0 => {
                let par = (f64::from(width) / f64::from(height))
                    / (f64::from(original_w) / f64::from(original_h));
                self.renderer.set_pixel_aspect(par);
                self.renderer
                    .set_storage_size(original_w as i32, original_h as i32);
            }
            _ => self.renderer.set_storage_size(width, height),
        }
    }

    /// Maps colorspace/range to the subtitle color conversion with
    /// FFmpeg's semantics: the matrix set covers `av_csp_luma_coeffs`
    /// entries; UNSPECIFIED falls back to BT.601; the range stays
    /// undecided here (`None`) so the per-format default can resolve it
    /// like `ff_draw_init2` (yuvj/RGB default to full, else limited).
    /// Like vf_subtitles' config_input, the choice is LOCKED on first use —
    /// later frames reuse it even if their metadata drifts.
    fn frame_color(
        &mut self,
        colorspace: AVColorSpace,
        color_range: AVColorRange,
    ) -> (ColorMatrix, Option<ColorRange>) {
        if let Some(locked) = self.locked_color {
            return locked;
        }
        let matrix = match colorspace {
            AVColorSpace::AVCOL_SPC_BT709 => ColorMatrix::Bt709,
            AVColorSpace::AVCOL_SPC_SMPTE170M | AVColorSpace::AVCOL_SPC_BT470BG => {
                ColorMatrix::Bt601
            }
            AVColorSpace::AVCOL_SPC_BT2020_NCL | AVColorSpace::AVCOL_SPC_BT2020_CL => {
                ColorMatrix::Bt2020
            }
            AVColorSpace::AVCOL_SPC_FCC => ColorMatrix::Fcc,
            AVColorSpace::AVCOL_SPC_SMPTE240M => ColorMatrix::Smpte240m,
            AVColorSpace::AVCOL_SPC_YCGCO => ColorMatrix::YCoCg,
            AVColorSpace::AVCOL_SPC_UNSPECIFIED => ColorMatrix::Bt601,
            other => {
                if !self.warned_colorspace {
                    self.warned_colorspace = true;
                    log::warn!(
                        "subtitle filter: colorspace {other:?} not supported for subtitle \
                         color conversion, falling back to BT.601 (logged once; FFmpeg's \
                         subtitles filter leaves its draw context uninitialized here)"
                    );
                }
                ColorMatrix::Bt601
            }
        };
        let range = match color_range {
            AVColorRange::AVCOL_RANGE_JPEG => Some(ColorRange::Full),
            AVColorRange::AVCOL_RANGE_MPEG => Some(ColorRange::Limited),
            _ => None,
        };
        self.locked_color = Some((matrix, range));
        (matrix, range)
    }

    /// Composites rendered overlays onto a writable software frame laid out
    /// as described by `spec`.
    ///
    /// # Safety
    /// `frame` must be a writable software frame whose pixel format matches
    /// `spec`, with positive linesizes.
    unsafe fn blend_images(
        frame: *mut AVFrame,
        images: &[OverlayImage<'_>],
        spec: &FormatSpec,
        matrix: ColorMatrix,
        range: Option<ColorRange>,
        scratch: &mut Vec<u16>,
    ) {
        // ff_draw_init2: an EXPLICIT range is honored; only an unspecified
        // one falls back to the format default (yuvj/RGB full, else
        // limited).
        let range = range.unwrap_or(if spec.force_full_range {
            ColorRange::Full
        } else {
            ColorRange::Limited
        });
        let width = (*frame).width as usize;
        let height = (*frame).height as usize;

        /// Builds the writable view of one component. Returns `None` when
        /// the plane is unusable (defensive; the scheduler never feeds such
        /// frames).
        ///
        /// # Safety
        /// `frame` must outlive the returned view, and no two views over
        /// the same plane may be alive at once (the callers below build and
        /// consume views strictly one at a time).
        unsafe fn component_view<'a>(
            frame: *mut AVFrame,
            placement: &ComponentPlacement,
            width: usize,
            height: usize,
        ) -> Option<PlaneView<'a>> {
            let plane_w = (width + (1usize << placement.hsub) - 1) >> placement.hsub;
            let plane_h = (height + (1usize << placement.vsub) - 1) >> placement.vsub;
            let linesize = (*frame).linesize[placement.plane];
            let data = (*frame).data[placement.plane];
            if linesize <= 0 || data.is_null() || plane_w == 0 || plane_h == 0 {
                return None;
            }
            let linesize = linesize as usize;
            let plane_bytes = linesize * (plane_h - 1) + plane_w * placement.pixel_step;
            Some(PlaneView {
                data: std::slice::from_raw_parts_mut(
                    data.add(placement.offset),
                    plane_bytes - placement.offset,
                ),
                linesize,
                pixel_step: placement.pixel_step,
            })
        }

        // Every layout-table format has at most two subsampled components.
        // When there are exactly two (4:2:0/4:2:2 planar, NV12/NV21, P010),
        // each node's mask is pooled ONCE into `scratch` and applied to both
        // components, instead of re-pooling per component.
        let mut pooled = [usize::MAX; 2];
        let mut pooled_count = 0usize;
        for (index, (_, placement)) in spec.comps.iter().enumerate() {
            if placement.hsub != 0 || placement.vsub != 0 {
                if pooled_count < 2 {
                    pooled[pooled_count] = index;
                }
                pooled_count += 1;
            }
        }
        // Two-phase wins for 8-bit samples (dense 1080p chroma pair: 411us
        // vs 496us fused); for 16-bit the fused per-component path measured
        // faster (472us vs 537us), so it keeps the old route.
        let share_pooling = pooled_count == 2 && spec.sample == SampleFormat::U8 && {
            let a = &spec.comps[pooled[0]].1;
            let b = &spec.comps[pooled[1]].1;
            a.hsub == 1 && b.hsub == 1 && a.vsub == b.vsub && a.vsub <= 1
        };

        for overlay in images {
            let alpha = spec.sample.alpha_fixed(overlay.opacity());
            if alpha == 0 {
                continue;
            }
            let source_values = match spec.model {
                ColorModel::Yuv => {
                    blend::yuv_components(overlay.rgb(), matrix, range, spec.scale_bits)
                }
                ColorModel::Rgb => blend::rgb_components(overlay.rgb(), range, spec.scale_bits),
            };

            if share_pooling {
                let vsub = spec.comps[pooled[0]].1.vsub;
                if let Some(rect) = blend::pool_sums_h2(width, height, overlay, vsub, scratch) {
                    for &index in &pooled {
                        let (source, placement) = &spec.comps[index];
                        let Some(mut plane) = component_view(frame, placement, width, height)
                        else {
                            continue;
                        };
                        blend::blend_pooled_from_sums(
                            &mut plane,
                            scratch,
                            rect,
                            source_values[*source],
                            alpha,
                            spec.sample,
                        );
                    }
                }
            }
            for (index, (source, placement)) in spec.comps.iter().enumerate() {
                if share_pooling && (index == pooled[0] || index == pooled[1]) {
                    continue;
                }
                let Some(mut plane) = component_view(frame, placement, width, height) else {
                    continue;
                };
                blend::blend_component(
                    &mut plane,
                    width,
                    height,
                    overlay,
                    source_values[*source],
                    alpha,
                    placement.hsub,
                    placement.vsub,
                    spec.sample,
                );
            }
        }
    }
}

impl FrameFilter for SubtitleFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        mut frame: Frame,
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, String> {
        // Props-only frames (buf[0] == null EOF markers) and pixel-less frames
        // pass through untouched — and this filter never returns Ok(None),
        // which would starve downstream consumers.
        // SAFETY: pointer null-checked before any field read.
        unsafe {
            if frame.as_ptr().is_null() || frame.is_empty() {
                return Ok(Some(frame));
            }
        }
        // SAFETY: non-null software frame owned by us for the duration.
        let (width, height, format, pts, time_base, is_hw, colorspace, color_range) = unsafe {
            let p = frame.as_ptr();
            (
                (*p).width,
                (*p).height,
                (*p).format,
                (*p).pts,
                (*p).time_base,
                !(*p).hw_frames_ctx.is_null(),
                (*p).colorspace,
                (*p).color_range,
            )
        };
        if width <= 0 || height <= 0 {
            return Ok(Some(frame));
        }
        if is_hw {
            return Err(
                "SubtitleFilter requires software frames; do not set hwaccel_output_format to a \
                 hardware format (frames must be downloaded before this filter)"
                    .to_string(),
            );
        }
        // SAFETY: the value was written into the AVFrame by FFmpeg, so it is
        // a valid AVPixelFormat discriminant (or -1 = NONE).
        let format_enum = unsafe { std::mem::transmute::<i32, AVPixelFormat>(format) };
        let Some(spec) = layout::format_spec(format_enum) else {
            return Err(format!(
                "SubtitleFilter: unsupported pixel format {}; convert first, e.g. \
                 .filter_desc(\"format=yuv420p\") or Output::set_pix_fmt(\"yuv420p\"). \
                 Supported: {}",
                pix_fmt_name(format),
                layout::SUPPORTED_LIST
            ));
        };

        if pts == AV_NOPTS_VALUE || time_base.num <= 0 || time_base.den <= 0 {
            if !self.warned_missing_time {
                self.warned_missing_time = true;
                log::warn!(
                    "subtitle filter: frame without pts/time_base cannot be timed, \
                     passing through (logged once)"
                );
            }
            return Ok(Some(frame));
        }
        // SAFETY: pure arithmetic FFI.
        let now_ms = unsafe { av_rescale_q(pts, time_base, AVRational { num: 1, den: 1000 }) };

        self.configure_renderer(width, height);
        // Computed before rendering: `images` below borrows the renderer
        // field, so no whole-`self` calls may sit between render and blend.
        let (matrix, range) = self.frame_color(colorspace, color_range);

        let images = self.renderer.render_frame(now_ms);
        if images.is_empty() {
            // Nothing visible at this timestamp: zero-cost passthrough (no
            // make_writable copy on subtitle-free frames).
            return Ok(Some(frame));
        }
        // A non-empty list can still consist solely of clipped-away or fully
        // transparent nodes; those must not pay the make_writable either
        // (on fanned-out frames it copies the whole frame).
        if !any_visible_image(&images, width, height, spec.sample) {
            return Ok(Some(frame));
        }

        // Frames are fanned out with av_frame_ref; unshare before writing.
        // SAFETY: valid owned frame.
        let ret = unsafe { av_frame_make_writable(frame.as_mut_ptr()) };
        if ret < 0 {
            return Err(format!(
                "av_frame_make_writable failed: {}",
                av_err2str(ret)
            ));
        }

        // Negative linesizes (bottom-up layouts, e.g. produced by vflip) can
        // survive make_writable when no copy was needed. FFmpeg blends those
        // natively; this filter does not yet — fail deterministically instead
        // of silently skipping planes.
        // SAFETY: valid owned frame; only reads plane linesizes.
        let negative_linesize = unsafe {
            let p = frame.as_ptr();
            spec.comps
                .iter()
                .any(|(_, placement)| (*p).linesize[placement.plane] < 0)
        };
        if negative_linesize {
            return Err(
                "SubtitleFilter: negative linesize (bottom-up/vertically flipped frames) is \
                 not supported; apply vflip after this filter or convert the frame first"
                    .to_string(),
            );
        }

        // SAFETY: frame verified software with a supported layout and made
        // writable; `images` comes from the render call above.
        unsafe {
            Self::blend_images(
                frame.as_mut_ptr(),
                &images,
                spec,
                matrix,
                range,
                &mut self.pool_scratch,
            )
        };

        Ok(Some(frame))
    }

    fn uninit(&mut self, _ctx: &FrameFilterContext) {
        // Idempotent; Drop covers the paths where uninit never runs.
        self.renderer.teardown();
    }
}

/// True when at least one overlay would actually touch the frame:
/// non-transparent and intersecting the frame rectangle.
fn any_visible_image(
    images: &[OverlayImage<'_>],
    width: i32,
    height: i32,
    sample: SampleFormat,
) -> bool {
    images.iter().any(|image| {
        if sample.alpha_fixed(image.opacity()) == 0 {
            return false;
        }
        let (x0, y0) = (i64::from(image.dst_x), i64::from(image.dst_y));
        x0 < i64::from(width)
            && y0 < i64::from(height)
            && x0 + image.w as i64 > 0
            && y0 + image.h as i64 > 0
    })
}

fn pix_fmt_name(format: i32) -> String {
    // SAFETY: `format` originates from an AVFrame written by FFmpeg, so it is
    // a valid AVPixelFormat discriminant (or -1 = NONE).
    unsafe {
        let name = av_get_pix_fmt_name(std::mem::transmute::<i32, AVPixelFormat>(format));
        if name.is_null() {
            format!("#{format}")
        } else {
            CStr::from_ptr(name).to_string_lossy().into_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtitle::test_util::{self, diff_stats, temp_path, transcode_test_mp4};
    use crate::subtitle::FontProvider;

    const W: usize = 320;
    const H: usize = 240;
    const FRAME_BYTES: usize = W * H + 2 * ((W / 2) * (H / 2));

    /// Full-opacity white drawing covering the top-left quarter of the
    /// 640x360 playfield (=> top-left quarter of the frame), visible for the
    /// given window.
    fn quarter_box_script(start: &str, end: &str) -> String {
        test_util::minimal_ass(&format!(
            "Dialogue: 0,{start},{end},Default,,0,0,0,,{{\\an7\\pos(0,0)\\p1}}m 0 0 l 320 0 320 180 0 180{{\\p0}}\n"
        ))
    }

    fn build_filter(script: String, font: &str) -> SubtitleFilter {
        SubtitleFilter::builder()
            .ass_content(script)
            .default_font_file(font)
            .font_provider(FontProvider::None)
            .build()
            .expect("build subtitle filter")
    }

    /// End-to-end through the real scheduler: rawvideo output is compared
    /// byte-by-byte against a baseline run without the filter.
    #[test]
    #[allow(clippy::manual_is_multiple_of)] // is_multiple_of needs Rust 1.87
    fn burns_subtitles_into_the_pipeline_output() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };

        let baseline_path = temp_path("baseline.yuv");
        let burned_path = temp_path("burned.yuv");
        let unburned_path = temp_path("unburned.yuv");

        transcode_test_mp4(&baseline_path, None, "yuv420p", None);
        transcode_test_mp4(
            &burned_path,
            Some(build_filter(
                quarter_box_script("0:00:00.00", "0:00:10.00"),
                font,
            )),
            "yuv420p",
            None,
        );
        // Event entirely outside the 5s clip: must be byte-identical to the
        // baseline (proves the no-image fast path + untouched passthrough).
        transcode_test_mp4(
            &unburned_path,
            Some(build_filter(
                quarter_box_script("0:00:20.00", "0:00:25.00"),
                font,
            )),
            "yuv420p",
            None,
        );

        let baseline = std::fs::read(&baseline_path).expect("read baseline");
        let burned = std::fs::read(&burned_path).expect("read burned");
        let unburned = std::fs::read(&unburned_path).expect("read unburned");
        let _ = std::fs::remove_file(&baseline_path);
        let _ = std::fs::remove_file(&burned_path);
        let _ = std::fs::remove_file(&unburned_path);

        assert!(!baseline.is_empty() && baseline.len() % FRAME_BYTES == 0);
        assert_eq!(baseline.len(), burned.len(), "same frame count");
        assert_eq!(
            baseline, unburned,
            "out-of-window subtitles must not alter any byte"
        );
        assert_ne!(baseline, burned, "in-window subtitles must alter the video");

        // First frame, luma plane: inside the drawn quarter (white fill) vs
        // far outside it.
        let inside = 60 * W + 80; // (80, 60) — inside 160x120 top-left quarter
        let outside = 230 * W + 300; // (300, 230) — far bottom-right
        assert_ne!(
            baseline[inside], burned[inside],
            "pixel inside the subtitle box should change"
        );
        assert_eq!(
            baseline[outside], burned[outside],
            "pixel far outside the subtitle box must not change"
        );
        // White in limited range lands near 235.
        assert!(
            burned[inside] >= 230,
            "expected near-white luma inside the box, got {}",
            burned[inside]
        );
    }

    /// The blend engine reaches every supported layout family: semi-planar
    /// chroma (nv12), no-subsampling planar (yuv444p), and packed RGB.
    #[test]
    fn burns_into_nv12_yuv444p_and_rgb24() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let script = || quarter_box_script("0:00:00.00", "0:00:10.00");
        let inside_luma = 60 * W + 80;
        let outside_luma = 230 * W + 300;

        for (pix_fmt, white_check) in [("nv12", None), ("yuv444p", None), ("rgb24", Some(250u8))] {
            let baseline_path = temp_path(&format!("base_{pix_fmt}"));
            let burned_path = temp_path(&format!("burn_{pix_fmt}"));
            transcode_test_mp4(&baseline_path, None, pix_fmt, None);
            transcode_test_mp4(
                &burned_path,
                Some(build_filter(script(), font)),
                pix_fmt,
                None,
            );
            let baseline = std::fs::read(&baseline_path).expect("read baseline");
            let burned = std::fs::read(&burned_path).expect("read burned");
            let _ = std::fs::remove_file(&baseline_path);
            let _ = std::fs::remove_file(&burned_path);

            assert_eq!(baseline.len(), burned.len(), "{pix_fmt}: frame count");
            assert_ne!(baseline, burned, "{pix_fmt}: subtitles must alter video");
            match white_check {
                None => {
                    // Planar/semi-planar: luma plane comes first in both.
                    assert!(
                        burned[inside_luma] >= 230,
                        "{pix_fmt}: near-white luma expected, got {}",
                        burned[inside_luma]
                    );
                    assert_eq!(
                        baseline[outside_luma], burned[outside_luma],
                        "{pix_fmt}: outside pixel must not change"
                    );
                }
                Some(threshold) => {
                    // Packed RGB: 3 bytes per pixel.
                    let inside = inside_luma * 3;
                    let outside = outside_luma * 3;
                    for c in 0..3 {
                        assert!(
                            burned[inside + c] >= threshold,
                            "{pix_fmt}: white component {c} expected, got {}",
                            burned[inside + c]
                        );
                        assert_eq!(
                            baseline[outside + c],
                            burned[outside + c],
                            "{pix_fmt}: outside component {c} must not change"
                        );
                    }
                }
            }
        }
    }

    fn u16le_at(buf: &[u8], sample_index: usize) -> u16 {
        u16::from_le_bytes([buf[sample_index * 2], buf[sample_index * 2 + 1]])
    }

    /// High-depth formats end-to-end: 10-bit planar (values 0..1023) and
    /// P010 (10 bits MSB-aligned in u16 containers).
    #[test]
    fn burns_into_10bit_planar_and_p010() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let script = || quarter_box_script("0:00:00.00", "0:00:10.00");
        let inside_luma = 60 * W + 80; // sample index within the luma plane
        let outside_luma = 230 * W + 300;

        // White floors: ff_draw-scaled white is 943 (10-bit) / 60395 (P010),
        // and the FFmpeg 16-bit blend keeps a 257/65537 residual of the dark
        // background underneath.
        for (pix_fmt, white_floor) in [("yuv420p10le", 900u16), ("p010le", 57000u16)] {
            let baseline_path = temp_path(&format!("base_{pix_fmt}"));
            let burned_path = temp_path(&format!("burn_{pix_fmt}"));
            transcode_test_mp4(&baseline_path, None, pix_fmt, None);
            transcode_test_mp4(
                &burned_path,
                Some(build_filter(script(), font)),
                pix_fmt,
                None,
            );
            let baseline = std::fs::read(&baseline_path).expect("read baseline");
            let burned = std::fs::read(&burned_path).expect("read burned");
            let _ = std::fs::remove_file(&baseline_path);
            let _ = std::fs::remove_file(&burned_path);

            assert_eq!(baseline.len(), burned.len(), "{pix_fmt}: frame count");
            assert_ne!(baseline, burned, "{pix_fmt}: subtitles must alter video");
            let inside = u16le_at(&burned, inside_luma);
            assert!(
                inside >= white_floor,
                "{pix_fmt}: near-white luma expected inside the box, got {inside}"
            );
            assert_eq!(
                u16le_at(&baseline, outside_luma),
                u16le_at(&burned, outside_luma),
                "{pix_fmt}: outside sample must not change"
            );
        }
    }

    fn ffmpeg_cli_has_libass() -> bool {
        std::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-buildconf"])
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).contains("--enable-libass"))
            .unwrap_or(false)
    }

    /// Parity against FFmpeg's own subtitles filter, run with an identical
    /// font setup. Ignored by default: requires an ffmpeg CLI built with
    /// --enable-libass (run with `cargo test --features subtitle -- --ignored`).
    #[test]
    #[ignore = "requires ffmpeg CLI built with --enable-libass"]
    fn parity_with_ffmpeg_cli() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        if !ffmpeg_cli_has_libass() {
            eprintln!("skipping: no ffmpeg CLI with --enable-libass on PATH");
            return;
        }

        // Shared, single-font directory so both sides resolve the same face.
        let fonts_dir = temp_path("parity_fonts");
        std::fs::create_dir_all(&fonts_dir).expect("create fonts dir");
        let font_file = fonts_dir.join("DejaVuSans.ttf");
        std::fs::copy(font, &font_file).expect("copy test font");

        // Red text + red drawing: red is matrix-dependent (white is not), so
        // the BT.709 variant actually validates the color matrix selection.
        const FORCE_STYLE: &str = "PrimaryColour=&H0000FF&";
        let fonts_str = fonts_dir.to_str().expect("utf8 path");

        let run_pair = |script: &str,
                        name: &str,
                        setparams: Option<&str>,
                        pix_fmt: &str|
         -> (Vec<u8>, Vec<u8>) {
            let label = format!("{name}_{}", setparams.map_or("default", |_| "bt709"));
            let script_path = temp_path(&format!("parity_{label}.ass"));
            std::fs::write(&script_path, script).expect("write script");
            let script_str = script_path.to_str().expect("utf8 path");
            // Keep lavfi escaping trivial by construction.
            assert!(
                !script_str.contains([':', '\'']) && !fonts_str.contains([':', '\'']),
                "temp paths must not need lavfi escaping"
            );
            let reference_path = temp_path(&format!("parity_ref_{label}.yuv"));
            let ours_path = temp_path(&format!("parity_ours_{label}.yuv"));

            let mut chain = String::new();
            if let Some(setparams) = setparams {
                chain.push_str(setparams);
                chain.push(',');
            }
            chain.push_str(&format!(
                "format={pix_fmt},subtitles=filename='{script_str}':fontsdir='{fonts_str}':force_style='{FORCE_STYLE}'"
            ));
            let status = std::process::Command::new("ffmpeg")
                .args(["-y", "-v", "error", "-i", "test.mp4", "-an", "-vf"])
                .arg(&chain)
                .args(["-f", "rawvideo"])
                .arg(&reference_path)
                .status()
                .expect("run ffmpeg CLI");
            assert!(status.success(), "ffmpeg CLI failed for {label}");

            let filter = SubtitleFilter::builder()
                .ass_content(script)
                .fonts_dir(&fonts_dir)
                .default_font_file(&font_file)
                .font_provider(FontProvider::None)
                .force_style(FORCE_STYLE)
                .build()
                .expect("build subtitle filter");
            transcode_test_mp4(&ours_path, Some(filter), pix_fmt, setparams);

            let reference = std::fs::read(&reference_path).expect("read reference");
            let ours = std::fs::read(&ours_path).expect("read ours");
            // Keep intermediates for offline diffing when requested.
            if std::env::var_os("EZ_PARITY_KEEP").is_none() {
                let _ = std::fs::remove_file(&script_path);
                let _ = std::fs::remove_file(&reference_path);
                let _ = std::fs::remove_file(&ours_path);
            } else {
                eprintln!("kept: {reference_path:?} vs {ours_path:?}");
            }
            assert_eq!(reference.len(), ours.len(), "{label}: frame count");
            (reference, ours)
        };

        // Phase 1 — vector drawing only. Interiors must be byte-exact
        // (proves blend math, color conversion for both matrices, and
        // geometry); shape-EDGE pixels may differ by a few percent of
        // coverage because zeno and libass rasterize anti-aliased edges
        // with different algorithms (measured: <=11/255 on ~0.3% of
        // pixels, all on the outline perimeter).
        let drawing = test_util::minimal_ass(test_util::DRAWING_EVENT);
        let mut ours_by_matrix = Vec::new();
        for setparams in [None, Some("setparams=colorspace=bt709")] {
            let (reference, ours) = run_pair(&drawing, "draw", setparams, "yuv420p");
            let (max, mean) = diff_stats(&reference, &ours);
            let big_diffs = reference
                .iter()
                .zip(&ours)
                .filter(|(a, b)| a.abs_diff(**b) > 8)
                .count();
            let big_fraction = big_diffs as f64 / reference.len() as f64;
            assert!(
                max <= 16 && mean < 0.05 && big_fraction < 0.0005,
                "drawing parity diverged (setparams {setparams:?}: max {max}, mean {mean:.4}, \
                 >8-diff fraction {big_fraction:.6}) — interiors must stay byte-exact, edges \
                 within cross-rasterizer AA noise"
            );
            ours_by_matrix.push(ours);
        }
        // Red is matrix-dependent (white is not): both matrices must differ,
        // guarding against both sides being wrong identically.
        assert_ne!(
            ours_by_matrix[0], ours_by_matrix[1],
            "BT.601 and BT.709 renders should differ for a red subtitle"
        );

        // Same drawing at 10 bit — compared in 10-bit code units (byte-wise
        // diffs would misreport u16 carries). Edge-AA differences scale by
        // 4x in 10-bit units, so the bounds scale accordingly.
        let (reference, ours) = run_pair(&drawing, "draw10", None, "yuv420p10le");
        let (max, mean) = test_util::diff_stats_u16le(&reference, &ours);
        assert!(
            max <= 64 && mean < 0.2,
            "10-bit drawing parity diverged (max {max}, mean {mean:.4})"
        );

        // Phase 2 — text glyphs. Glyph GEOMETRY must match (advances,
        // positions, line placement — a scaling or shaping bug shifts whole
        // glyph runs and explodes these bounds); pixel-level anti-aliasing
        // legitimately differs because zeno and libass rasterize and stroke
        // outlines with different algorithms. Measured divergence with
        // correct geometry: mean 0.59, >8-diff fraction 0.018 (edge pixels
        // only); with the pre-fix aspect-scaling bug: mean 2.56, fraction
        // 0.044 — the bounds separate the two regimes cleanly.
        let text = test_util::minimal_ass(&format!(
            "{}{}",
            test_util::HELLO_EVENT,
            test_util::DRAWING_EVENT
        ));
        let (reference, ours) = run_pair(&text, "text", None, "yuv420p");
        let (_, mean) = diff_stats(&reference, &ours);
        let outliers = reference
            .iter()
            .zip(&ours)
            .filter(|(a, b)| a.abs_diff(**b) > 8)
            .count();
        let outlier_fraction = outliers as f64 / reference.len() as f64;
        assert!(
            mean < 1.0 && outlier_fraction < 0.03,
            "text parity diverged (mean {mean:.4}, >8-diff fraction {outlier_fraction:.5})"
        );

        let _ = std::fs::remove_file(&font_file);
        let _ = std::fs::remove_dir(&fonts_dir);
    }
}
