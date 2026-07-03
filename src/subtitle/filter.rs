//! The subtitle burn-in frame filter.

use super::backend::SubtitleRenderer;
use super::blend::{self, ColorMatrix, ColorRange, OverlayImage, PlaneView, SampleFormat};
use super::layout::{self, ColorModel, FormatSpec};
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
    /// Reusable blend buffers (grow once, then no per-frame allocation).
    blend_scratch: BlendScratch,
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
            blend_scratch: BlendScratch::default(),
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
    /// as described by `spec`. With `parallel` (callers gate it through
    /// [`should_parallelize`]) the components split into two plane-disjoint
    /// groups blended on two threads; the split silently stays serial when
    /// the format only touches one plane (packed RGB, gray).
    ///
    /// # Safety
    /// `frame` must be a writable software frame whose pixel format matches
    /// `spec`, with positive linesizes, exclusively owned by the caller for
    /// the duration of the call (`filter_frame` guarantees this after
    /// `av_frame_make_writable`).
    unsafe fn blend_images(
        frame: *mut AVFrame,
        images: &[OverlayImage<'_>],
        spec: &FormatSpec,
        matrix: ColorMatrix,
        range: Option<ColorRange>,
        scratch: &mut BlendScratch,
        parallel: bool,
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

        // Per-overlay alpha + converted color, computed once and shared by
        // both workers.
        scratch.preps.clear();
        scratch.preps.reserve(images.len());
        for overlay in images {
            let alpha = spec.sample.alpha_fixed(overlay.opacity());
            if alpha == 0 {
                scratch.preps.push(OverlayPrep { alpha, src: [0; 3] });
                continue;
            }
            let src = match spec.model {
                ColorModel::Yuv => {
                    blend::yuv_components(overlay.rgb(), matrix, range, spec.scale_bits)
                }
                ColorModel::Rgb => blend::rgb_components(overlay.rgb(), range, spec.scale_bits),
            };
            scratch.preps.push(OverlayPrep { alpha, src });
        }

        // Raw component geometry, captured once on this thread. Unusable
        // planes are skipped (defensive; the scheduler never feeds such
        // frames) exactly like the old per-view checks.
        let mut tasks: Vec<CompTask> = Vec::with_capacity(spec.comps.len());
        for (source, placement) in spec.comps {
            let plane_w = (width + (1usize << placement.hsub) - 1) >> placement.hsub;
            let plane_h = (height + (1usize << placement.vsub) - 1) >> placement.vsub;
            let linesize = (*frame).linesize[placement.plane];
            let data = (*frame).data[placement.plane];
            if linesize <= 0 || data.is_null() || plane_w == 0 || plane_h == 0 {
                continue;
            }
            let linesize = linesize as usize;
            let plane_bytes = linesize * (plane_h - 1) + plane_w * placement.pixel_step;
            tasks.push(CompTask {
                plane: placement.plane,
                data: data.add(placement.offset),
                len: plane_bytes - placement.offset,
                linesize,
                pixel_step: placement.pixel_step,
                source: *source,
                hsub: placement.hsub,
                vsub: placement.vsub,
            });
        }

        let dims = (width, height);
        let sample = spec.sample;
        let preps = &scratch.preps;
        if parallel {
            if let Some((group_a, group_b)) = split_tasks(&tasks) {
                let (pool_a, pool_b) = (&mut scratch.pool_a, &mut scratch.pool_b);
                std::thread::scope(|s| {
                    // Group A (the first plane — luma) on the spawned
                    // thread, the remaining planes inline: one spawn per
                    // frame. `split_tasks` verified the groups' byte
                    // ranges are disjoint, so the two workers never touch
                    // the same memory; per-plane compositing order is
                    // preserved because every plane lives entirely inside
                    // one group and each worker walks overlays in order.
                    s.spawn(move || blend_task_group(group_a, images, preps, sample, dims, pool_a));
                    blend_task_group(group_b, images, preps, sample, dims, pool_b);
                });
                return;
            }
        }
        blend_task_group(&tasks, images, preps, sample, dims, &mut scratch.pool_a);
    }
}

/// Reusable buffers for [`SubtitleFilter::blend_images`] (grow once, then
/// no per-frame allocation).
#[derive(Default)]
struct BlendScratch {
    /// Per-overlay alpha + converted color, shared by both blend workers.
    preps: Vec<OverlayPrep>,
    /// Mask-sum buffer for the serial path / parallel group A.
    pool_a: Vec<u16>,
    /// Mask-sum buffer for parallel group B (each worker pools into its
    /// own buffer).
    pool_b: Vec<u16>,
}

/// Precomputed per-overlay blend parameters (`alpha_fixed` + the converted
/// component triple).
struct OverlayPrep {
    alpha: u32,
    src: [u32; 3],
}

/// One component's blend work over one frame, described by raw plane
/// geometry so component groups can cross the `thread::scope` boundary.
#[derive(Clone, Copy)]
struct CompTask {
    /// AVFrame plane index (drives the group split only).
    plane: usize,
    /// First byte of this component (plane base + component offset).
    data: *mut u8,
    /// Addressable bytes from `data` (same bound the serial views used:
    /// `linesize * (plane_h - 1) + plane_w * pixel_step - offset`).
    len: usize,
    linesize: usize,
    pixel_step: usize,
    /// Index into the converted color triple.
    source: usize,
    hsub: u32,
    vsub: u32,
}

/// SAFETY: a `CompTask` only describes a byte range inside an AVFrame that
/// `filter_frame` exclusively owns after `av_frame_make_writable`; the
/// bytes are touched exclusively through [`CompTask::view`] under its
/// contract (one live view at a time per worker, and byte-disjoint task
/// groups across workers — enforced by [`split_tasks`] before any spawn).
unsafe impl Send for CompTask {}
/// SAFETY: shared references expose only the plain-data fields; all writes
/// go through [`CompTask::view`] under the same contract as `Send` above.
unsafe impl Sync for CompTask {}

impl CompTask {
    /// Materializes the writable component view.
    ///
    /// # Safety
    /// No other view over an overlapping byte range may be alive anywhere:
    /// within a worker, views are created and dropped strictly one at a
    /// time (NV12/P010 interleaved components overlap in memory); across
    /// workers, [`split_tasks`] verified the groups byte-disjoint. The
    /// backing frame outlives the view (it is owned by the running
    /// `filter_frame` call).
    unsafe fn view(&self) -> PlaneView<'_> {
        PlaneView {
            data: std::slice::from_raw_parts_mut(self.data, self.len),
            linesize: self.linesize,
            pixel_step: self.pixel_step,
        }
    }
}

/// Splits tasks into (components on the first plane, the rest) for the
/// two-thread blend. `None` when the split is impossible: fewer than two
/// planes touched (packed RGB, gray) or any byte overlap between the groups
/// (never true for the layout table, but checked so the unsafe plane split
/// can never alias).
fn split_tasks(tasks: &[CompTask]) -> Option<(&[CompTask], &[CompTask])> {
    let first_plane = tasks.first()?.plane;
    let split = tasks.iter().position(|task| task.plane != first_plane)?;
    let (a, b) = tasks.split_at(split);
    if b.iter().any(|task| task.plane == first_plane) {
        return None;
    }
    let disjoint = a.iter().all(|ta| {
        b.iter().all(|tb| {
            let (a0, a1) = (ta.data as usize, ta.data as usize + ta.len);
            let (b0, b1) = (tb.data as usize, tb.data as usize + tb.len);
            a1 <= b0 || b1 <= a0
        })
    });
    disjoint.then_some((a, b))
}

/// Blends every overlay onto the components in `tasks`, in overlay order.
/// This is the whole per-frame blend when called with all components
/// (serial path) and one worker's half in the parallel split; compositing
/// order is a per-plane contract and every plane lives entirely inside one
/// group, so both call shapes produce identical bytes.
fn blend_task_group(
    tasks: &[CompTask],
    images: &[OverlayImage<'_>],
    preps: &[OverlayPrep],
    sample: SampleFormat,
    (width, height): (usize, usize),
    pool: &mut Vec<u16>,
) {
    // When this group carries exactly two h2-subsampled components
    // (4:2:0/4:2:2 planar, NV12/NV21, P010 chroma) and the two-phase route
    // is preferred for the sample width, each node's mask is pooled ONCE
    // and applied to both components instead of re-pooling per component.
    let mut pooled = [usize::MAX; 2];
    let mut pooled_count = 0usize;
    for (index, task) in tasks.iter().enumerate() {
        if task.hsub != 0 || task.vsub != 0 {
            if pooled_count < 2 {
                pooled[pooled_count] = index;
            }
            pooled_count += 1;
        }
    }
    let share_pooling = pooled_count == 2 && blend::two_phase_pooled_preferred(sample) && {
        let (a, b) = (&tasks[pooled[0]], &tasks[pooled[1]]);
        a.hsub == 1 && b.hsub == 1 && a.vsub == b.vsub && a.vsub <= 1
    };

    for (overlay, prep) in images.iter().zip(preps) {
        if prep.alpha == 0 {
            continue;
        }
        if share_pooling {
            if let Some(rect) =
                blend::pool_sums_h2(width, height, overlay, tasks[pooled[0]].vsub, pool)
            {
                for &index in &pooled {
                    let task = &tasks[index];
                    // SAFETY: views live one at a time inside this worker;
                    // other workers' tasks are byte-disjoint (split_tasks).
                    let mut plane = unsafe { task.view() };
                    blend::blend_pooled_from_sums(
                        &mut plane,
                        pool,
                        rect,
                        prep.src[task.source],
                        prep.alpha,
                        sample,
                    );
                }
            }
        }
        for (index, task) in tasks.iter().enumerate() {
            if share_pooling && (index == pooled[0] || index == pooled[1]) {
                continue;
            }
            // SAFETY: views live one at a time inside this worker; other
            // workers' tasks are byte-disjoint (split_tasks).
            let mut plane = unsafe { task.view() };
            blend::blend_component(
                &mut plane,
                width,
                height,
                overlay,
                prep.src[task.source],
                prep.alpha,
                task.hsub,
                task.vsub,
                sample,
            );
        }
    }
}

/// Minimum total clipped mask pixels before [`SubtitleFilter::blend_images`]
/// splits plane work across two threads. Below this the spawn/join overhead
/// (tens of microseconds) rivals the blend itself: the bench's sparse
/// one-line 1080p dialogue measures ~42k mask px and blends in well under
/// 50us with the AVX2 kernels, while the dense multi-line scene measures
/// ~640k px and ~220us — 256k separates the two regimes with margin on
/// both sides (`bench_kernels.rs` scenario stats).
const PARALLEL_MASK_PX_THRESHOLD: usize = 256 * 1024;

/// The parallel-blend gate, kept pure for unit testing.
fn should_parallelize(clipped_mask_px: usize) -> bool {
    clipped_mask_px > PARALLEL_MASK_PX_THRESHOLD
}

/// Total overlay mask pixels that actually intersect the frame.
fn clipped_mask_px(images: &[OverlayImage<'_>], width: usize, height: usize) -> usize {
    images
        .iter()
        .map(|image| blend::clipped_area(width, height, image))
        .sum()
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

        let parallel =
            should_parallelize(clipped_mask_px(&images, width as usize, height as usize));
        // SAFETY: frame verified software with a supported layout and made
        // writable (exclusively owned for the call); `images` comes from
        // the render call above.
        unsafe {
            Self::blend_images(
                frame.as_mut_ptr(),
                &images,
                spec,
                matrix,
                range,
                &mut self.blend_scratch,
                parallel,
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
    use ffmpeg_sys_next::{av_frame_alloc, av_frame_free, av_frame_get_buffer};

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

    /// Deterministic LCG byte stream for synthetic planes and masks.
    fn lcg_bytes(len: usize, seed: &mut u64) -> Vec<u8> {
        (0..len)
            .map(|_| {
                *seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (*seed >> 33) as u8
            })
            .collect()
    }

    /// Rows of one AVFrame plane, derived from the format spec (0 when the
    /// plane is unused by the format).
    fn plane_rows(spec: &FormatSpec, plane: usize, height: usize) -> usize {
        spec.comps
            .iter()
            .filter(|(_, p)| p.plane == plane)
            .map(|(_, p)| (height + (1usize << p.vsub) - 1) >> p.vsub)
            .max()
            .unwrap_or(0)
    }

    /// Allocates a `w x h` frame of `format` and fills every used plane
    /// (including alignment padding) with a deterministic pattern.
    ///
    /// # Safety
    /// Caller frees the frame with `av_frame_free`.
    unsafe fn filled_frame(
        w: i32,
        h: i32,
        format: AVPixelFormat,
        spec: &FormatSpec,
        mut seed: u64,
    ) -> *mut AVFrame {
        let frame = av_frame_alloc();
        assert!(!frame.is_null());
        (*frame).width = w;
        (*frame).height = h;
        (*frame).format = format as i32;
        assert!(av_frame_get_buffer(frame, 0) >= 0, "av_frame_get_buffer");
        for plane in 0..4usize {
            let rows = plane_rows(spec, plane, h as usize);
            let data = (*frame).data[plane];
            if rows == 0 || data.is_null() {
                continue;
            }
            let len = (*frame).linesize[plane] as usize * rows;
            let bytes = lcg_bytes(len, &mut seed);
            std::slice::from_raw_parts_mut(data, len).copy_from_slice(&bytes);
        }
        frame
    }

    /// Copies out every used plane (full linesize x rows, padding included).
    ///
    /// # Safety
    /// `frame` must be a valid frame produced by [`filled_frame`].
    unsafe fn plane_bytes(frame: *mut AVFrame, spec: &FormatSpec, height: usize) -> Vec<Vec<u8>> {
        (0..4usize)
            .map(|plane| {
                let rows = plane_rows(spec, plane, height);
                let data = (*frame).data[plane];
                if rows == 0 || data.is_null() {
                    return Vec::new();
                }
                let len = (*frame).linesize[plane] as usize * rows;
                std::slice::from_raw_parts(data, len).to_vec()
            })
            .collect()
    }

    /// The parallel plane split must produce byte-identical frames to the
    /// serial path on every layout family: planar 4:2:0, semi-planar
    /// (NV12), no-subsampling planar, high-depth planar + P010, and the
    /// single-plane formats where the split falls back to serial.
    #[test]
    fn parallel_blend_matches_serial_bitexact() {
        use ffmpeg_sys_next::AVPixelFormat::*;
        let (w, h) = (318i32, 178i32);
        let mut seed = 0x00C0_FFEE_0DDB_A11Du64;

        // Three overlays: dense structured (overhanging top-left),
        // translucent red, and a solid overhanging the bottom — different
        // colors and opacities so every source component and the
        // compositing order matter. None touches the exact right plane
        // edge: interleaved components (NV12 V, P010 V, RGB G/B) have a
        // pre-existing 1-byte view overrun there (predates this test and
        // is identical on both paths compared here).
        let mask_a = {
            let mut mask = lcg_bytes(220 * 130, &mut seed);
            for (i, byte) in mask.iter_mut().enumerate() {
                if (i / 40) % 3 == 0 {
                    *byte = 0;
                }
            }
            mask
        };
        let mask_b = lcg_bytes(97 * 53, &mut seed);
        let mask_c = vec![255u8; 64 * 33];
        let images = [
            OverlayImage {
                w: 220,
                h: 130,
                stride: 220,
                bitmap: &mask_a,
                color: 0xFFFFFF00,
                dst_x: -8,
                dst_y: -6,
            },
            OverlayImage {
                w: 97,
                h: 53,
                stride: 97,
                bitmap: &mask_b,
                color: 0xFF000040,
                dst_x: 40,
                dst_y: 30,
            },
            OverlayImage {
                w: 64,
                h: 33,
                stride: 64,
                bitmap: &mask_c,
                color: 0x00FF0080,
                dst_x: 240,
                dst_y: 160,
            },
        ];

        for format in [
            AV_PIX_FMT_YUV420P,
            AV_PIX_FMT_NV12,
            AV_PIX_FMT_YUV444P,
            AV_PIX_FMT_YUV420P10LE,
            AV_PIX_FMT_P010LE,
            AV_PIX_FMT_RGB24,
            AV_PIX_FMT_GRAY8,
        ] {
            let spec = layout::format_spec(format).expect("supported format");
            let fill_seed = 0x5EED_0000 ^ format as u64;
            // SAFETY: frames allocated and freed here; blend_images gets a
            // writable, exclusively-owned software frame matching `spec`.
            unsafe {
                let serial = filled_frame(w, h, format, spec, fill_seed);
                let parallel = filled_frame(w, h, format, spec, fill_seed);
                let original = plane_bytes(serial, spec, h as usize);

                let mut scratch_serial = BlendScratch::default();
                let mut scratch_parallel = BlendScratch::default();
                SubtitleFilter::blend_images(
                    serial,
                    &images,
                    spec,
                    ColorMatrix::Bt601,
                    Some(ColorRange::Limited),
                    &mut scratch_serial,
                    false,
                );
                SubtitleFilter::blend_images(
                    parallel,
                    &images,
                    spec,
                    ColorMatrix::Bt601,
                    Some(ColorRange::Limited),
                    &mut scratch_parallel,
                    true,
                );

                let serial_planes = plane_bytes(serial, spec, h as usize);
                let parallel_planes = plane_bytes(parallel, spec, h as usize);
                assert_ne!(
                    original, serial_planes,
                    "{format:?}: blend must alter the frame"
                );
                assert_eq!(
                    serial_planes, parallel_planes,
                    "{format:?}: parallel blend diverged from serial"
                );

                let mut serial = serial;
                let mut parallel = parallel;
                av_frame_free(&mut serial);
                av_frame_free(&mut parallel);
            }
        }
    }

    /// End-to-end `blend_images` timing, serial vs plane-parallel, on the
    /// captured dense/sparse scenes (real rendered masks). Deliberately NOT
    /// named `bench_blend...` so it never times concurrently with the
    /// kernel tables; run it on its own with:
    ///
    /// ```text
    /// cargo test --release --features subtitle bench_plane_parallel -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "manual micro-benchmark; run in release with --nocapture"]
    fn bench_plane_parallel_blend_images() {
        use crate::subtitle::bench_kernels::{capture, dense_events, measure, sparse_events};
        use ffmpeg_sys_next::AVPixelFormat::*;

        let Some(dense) = capture(dense_events()) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let sparse = capture(sparse_events()).expect("font present per the check above");
        for (name, owned) in [("dense", dense), ("sparse", sparse)] {
            let images: Vec<OverlayImage<'_>> = owned.iter().map(|o| o.as_view()).collect();
            let px = clipped_mask_px(&images, 1920, 1080);
            println!(
                "blend_images {name}: mask_px={px} gate_parallel={}",
                should_parallelize(px)
            );
            for (format, label) in [
                (AV_PIX_FMT_YUV420P, "yuv420p"),
                (AV_PIX_FMT_YUV420P10LE, "yuv420p10le"),
            ] {
                let spec = layout::format_spec(format).expect("supported format");
                // SAFETY: frame allocated and freed here; blend_images gets
                // a writable, exclusively-owned software frame matching
                // `spec`. Repeated composites only change dst values, not
                // the amount of work (the kernels are branchless in dst).
                unsafe {
                    let frame = filled_frame(1920, 1080, format, spec, 0xBEEF);
                    let mut scratch = BlendScratch::default();
                    let serial = measure(|| {
                        SubtitleFilter::blend_images(
                            frame,
                            &images,
                            spec,
                            ColorMatrix::Bt601,
                            Some(ColorRange::Limited),
                            &mut scratch,
                            false,
                        );
                    });
                    let parallel = measure(|| {
                        SubtitleFilter::blend_images(
                            frame,
                            &images,
                            spec,
                            ColorMatrix::Bt601,
                            Some(ColorRange::Limited),
                            &mut scratch,
                            true,
                        );
                    });
                    println!(
                        "  {label:<12} serial {serial:>10.0} ns/frame   parallel \
                         {parallel:>10.0} ns/frame   {:>5.2}x",
                        serial / parallel
                    );
                    let mut frame = frame;
                    av_frame_free(&mut frame);
                }
            }
        }
    }

    /// The gate is pure and exclusive at the threshold.
    #[test]
    fn parallel_gate_threshold_is_exclusive() {
        assert!(!should_parallelize(0));
        assert!(!should_parallelize(PARALLEL_MASK_PX_THRESHOLD));
        assert!(should_parallelize(PARALLEL_MASK_PX_THRESHOLD + 1));
    }

    /// Work estimation counts only mask area intersecting the frame.
    #[test]
    fn clipped_mask_px_counts_only_intersecting_area() {
        let bitmap = vec![255u8; 100 * 50];
        let image = |dst_x: i32, dst_y: i32| OverlayImage {
            w: 100,
            h: 50,
            stride: 100,
            bitmap: &bitmap,
            color: 0xFFFFFF00,
            dst_x,
            dst_y,
        };
        let on_frame = image(10, 10);
        assert_eq!(
            clipped_mask_px(std::slice::from_ref(&on_frame), 640, 360),
            5000
        );
        let half_off = image(-50, 0);
        assert_eq!(
            clipped_mask_px(std::slice::from_ref(&half_off), 640, 360),
            2500
        );
        let fully_off = image(640, 0);
        assert_eq!(
            clipped_mask_px(std::slice::from_ref(&fully_off), 640, 360),
            0
        );
        let both = [image(10, 10), image(-50, 0)];
        assert_eq!(clipped_mask_px(&both, 640, 360), 7500);
    }

    /// Split rules: multi-plane formats split after the first plane's
    /// components; single-plane formats and (defensively) overlapping
    /// ranges refuse to split.
    #[test]
    fn split_tasks_by_plane_and_overlap() {
        let mut buf_a = vec![0u8; 64];
        let mut buf_b = vec![0u8; 64];
        let task = |plane: usize, data: *mut u8, len: usize| CompTask {
            plane,
            data,
            len,
            linesize: 8,
            pixel_step: 1,
            source: 0,
            hsub: 0,
            vsub: 0,
        };
        let a_ptr = buf_a.as_mut_ptr();
        let b_ptr = buf_b.as_mut_ptr();

        // Planar layout: luma group + two chroma planes.
        // SAFETY: pointer arithmetic stays inside the owned buffers.
        let chroma2 = unsafe { b_ptr.add(32) };
        let tasks = [task(0, a_ptr, 64), task(1, b_ptr, 32), task(2, chroma2, 32)];
        let (group_a, group_b) = split_tasks(&tasks).expect("planar split");
        assert_eq!((group_a.len(), group_b.len()), (1, 2));

        // Packed RGB: everything on plane 0 -> no split.
        // SAFETY: as above.
        let (rgb1, rgb2) = unsafe { (a_ptr.add(1), a_ptr.add(2)) };
        let tasks = [task(0, a_ptr, 62), task(0, rgb1, 62), task(0, rgb2, 62)];
        assert!(split_tasks(&tasks).is_none());

        // Single component (gray): no split.
        assert!(split_tasks(&[task(0, a_ptr, 64)]).is_none());
        assert!(split_tasks(&[]).is_none());

        // Defensive: distinct plane indices but overlapping bytes.
        // SAFETY: as above.
        let overlap = unsafe { a_ptr.add(32) };
        let tasks = [task(0, a_ptr, 64), task(1, overlap, 32)];
        assert!(split_tasks(&tasks).is_none());

        // Defensive: the first plane reappears after the split point.
        let tasks = [task(0, a_ptr, 32), task(1, b_ptr, 32), task(0, overlap, 32)];
        assert!(split_tasks(&tasks).is_none());
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
