//! The [`FrameExtractor`] builder and its `frames()` / `collect_frames()` runs.

use super::error::FrameExportError;
use super::frame::VideoFrame;
use super::guard::ColorGuard;
use super::iter::FrameIter;
use super::options::{ColorPolicy, ConversionPrecision, PixelLayout, Sampling};
use super::resolve::{resolve_and_build_desc, ResolvePlan, UniformResolve};
use super::sampler::{ExportSampler, UniformSpan};
use super::selector::InputSelector;
use super::sink::ExportSink;
use crate::core::context::demuxer::Demuxer;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::filter_complex::FilterComplex;
use crate::core::context::input::Input;
use crate::core::context::output::{Output, VSyncMethod};
use crate::core::filter::frame_pipeline_builder::FramePipelineBuilder;
use crate::core::scheduler::ffmpeg_scheduler::FfmpegScheduler;
use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;

/// Extracts packed RGB/gray frames from a video input, one pass, for AI/CV.
///
/// Build one with [`FrameExtractor::new`], chain options, then call
/// [`frames`](FrameExtractor::frames) for a streaming iterator or
/// [`collect_frames`](FrameExtractor::collect_frames) for a `Vec`.
pub struct FrameExtractor {
    input: Input,
    sampling: Sampling,
    video_stream_index: Option<usize>,
    width: Option<u32>,
    height: Option<u32>,
    pixel: PixelLayout,
    color: ColorPolicy,
    precision: ConversionPrecision,
    max_frames: Option<u64>,
    start_time_us: Option<i64>,
    duration_us: Option<i64>,
    duration_hint_us: Option<i64>,
    channel_capacity: usize,
}

impl FrameExtractor {
    /// Creates an extractor over `input` (a path, URL, or anything convertible
    /// into an [`Input`]). Defaults: all frames, source size, `Rgb24`,
    /// [`ColorPolicy::Tagged`], channel capacity 1.
    pub fn new(input: impl Into<Input>) -> Self {
        Self {
            input: input.into(),
            sampling: Sampling::All,
            video_stream_index: None,
            width: None,
            height: None,
            pixel: PixelLayout::Rgb24,
            color: ColorPolicy::Tagged,
            precision: ConversionPrecision::Standard,
            max_frames: None,
            start_time_us: None,
            duration_us: None,
            duration_hint_us: None,
            channel_capacity: 1,
        }
    }

    /// Sets the sampling strategy (default [`Sampling::All`]).
    pub fn sampling(mut self, sampling: Sampling) -> Self {
        self.sampling = sampling;
        self
    }

    /// Selects an explicit video stream by absolute index (default: best video
    /// stream). Also pins the input-side per-frame HDR guard / color stamp to
    /// that stream — without it they bind to the first video stream, which
    /// coincides with the exported one except on exotic multi-video layouts.
    pub fn video_stream_index(mut self, index: usize) -> Self {
        self.video_stream_index = Some(index);
        self
    }

    /// Sets the output width in pixels; height (if unset) is derived keeping the
    /// aspect ratio (`scale=W:-2`).
    pub fn width(mut self, width: u32) -> Self {
        self.width = Some(width);
        self
    }

    /// Sets the output height in pixels; width (if unset) is derived keeping the
    /// aspect ratio (`scale=-2:H`).
    pub fn height(mut self, height: u32) -> Self {
        self.height = Some(height);
        self
    }

    /// Sets the packed pixel layout (default [`PixelLayout::Rgb24`]).
    pub fn pixel(mut self, layout: PixelLayout) -> Self {
        self.pixel = layout;
        self
    }

    /// Sets the color-interpretation policy (default [`ColorPolicy::Tagged`]).
    pub fn color(mut self, policy: ColorPolicy) -> Self {
        self.color = policy;
        self
    }

    /// Sets the swscale precision tier for the conversion stage — the single
    /// swscale pass that runs any resize plus the pixel-format conversion
    /// (default [`ConversionPrecision::Standard`], which matches the FFmpeg
    /// CLI's default scaler flags). [`ConversionPrecision::High`] opts into
    /// accurate rounding + full chroma interpolation at a several-fold
    /// conversion cost — see the enum docs for the tradeoff.
    pub fn conversion_precision(mut self, precision: ConversionPrecision) -> Self {
        self.precision = precision;
        self
    }

    /// Caps the number of exported frames. The sink owns this exact count.
    pub fn max_frames(mut self, max: u64) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Seeks to this start time (microseconds) before extracting.
    pub fn start_time_us(mut self, us: i64) -> Self {
        self.start_time_us = Some(us);
        self
    }

    /// Limits extraction to this many microseconds of content past the start.
    pub fn duration_us(mut self, us: i64) -> Self {
        self.duration_us = Some(us);
        self
    }

    /// Supplies a duration (microseconds) for `UniformN` when the input is not
    /// probeable (live/piped). Ignored by other sampling modes. Takes precedence
    /// over the container/stream duration, but not over an explicit
    /// [`duration_us`](FrameExtractor::duration_us) trim window.
    pub fn duration_hint_us(mut self, us: i64) -> Self {
        self.duration_hint_us = Some(us);
        self
    }

    /// Sets the prefetch channel capacity (default 1, minimum 1). Each slot
    /// costs one packed frame of memory.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Starts the run and returns a streaming iterator over the exported frames.
    ///
    /// Option and stream/HDR resolution errors surface here, before any frame is
    /// produced; runtime failures surface as the iterator's terminal `Err`.
    pub fn frames(self) -> crate::error::Result<FrameIter> {
        self.validate()?;

        let capacity = self.channel_capacity.max(1);
        let (tx, rx) = crossbeam_channel::bounded(capacity);

        // Output-side sink on the single graph output stream ([export] => 0).
        let sink = ExportSink::new(tx, self.sampling, self.pixel, self.max_frames);
        let sink_pipeline = FramePipelineBuilder::new(AVMEDIA_TYPE_VIDEO)
            .filter("frame_export_sink", Box::new(sink))
            .set_stream_index(0)
            .build();

        // UniformN selects frames pre-filtergraph on the input side; the grid
        // span is published through this cell at open time (§4.4) and read by
        // the sampler at its first frame.
        let uniform_n = match self.sampling {
            Sampling::UniformN(n) => Some(n),
            _ => None,
        };
        let span_cell: UniformSpan = std::sync::Arc::new(std::sync::OnceLock::new());

        // Input decoder fast path + time window. The extractor's own options
        // take precedence, but a start/duration preconfigured directly on the
        // Input must feed the SAME machinery (trim boundary, span sizing) —
        // otherwise it would reintroduce the GOP lead-in mis-anchoring the
        // boundary exists to prevent.
        let mut input = self.input;
        let effective_start = self.start_time_us.or(input.start_time_us);
        let effective_duration = self.duration_us.or(input.recording_time_us);
        if matches!(self.sampling, Sampling::KeyframesOnly) {
            input = input.set_video_codec_opt("skip_frame", "nokey");
        }
        if let Some(start) = self.start_time_us {
            input = input.set_start_time_us(start);
        }
        if let Some(dur) = self.duration_us {
            input = input.set_recording_time_us(dur);
        } else if uniform_n.is_some() && input.recording_time_us.is_none() {
            if let Some(hint) = self.duration_hint_us {
                // The hint declares the sampled span; also use it as the demux
                // stop boundary so an unbounded (live/piped) input terminates
                // once the grid is covered instead of decoding forever. A
                // recording time already set on the Input wins over the hint.
                input = input.set_recording_time_us(hint);
            }
        }
        // Input-side pipeline, every run: the color guard re-checks each
        // decoded (pre-conversion) frame for HDR and applies the
        // TaggedOrResolutionGuess stamp; UniformN chains its sampler after the
        // guard so selection only ever sees guarded, stamped frames.
        let mut builder = FramePipelineBuilder::new(AVMEDIA_TYPE_VIDEO).filter(
            "frame_export_color_guard",
            Box::new(ColorGuard::new(self.color)),
        );
        // With a start time (from the extractor or preconfigured on the
        // Input) the demux timeline is re-zeroed at the request and the
        // in-graph trim drops pts < 0 — but the container seek lands on a
        // keyframe at or BEFORE the request, so on GOP video this pipeline
        // sees negative-pts lead-in first. The boundary makes the input-side
        // sampler/selector skip that lead-in instead of anchoring grids on
        // (or spending selections on) frames the trim will destroy.
        let trim_boundary = effective_start.map(|_| 0i64);
        if let Some(n) = uniform_n {
            let sampler = ExportSampler::new(n, span_cell.clone(), trim_boundary);
            builder = builder.filter("frame_export_sampler", Box::new(sampler));
        } else if let Some(selector) = InputSelector::for_sampling(&self.sampling, trim_boundary) {
            // EveryNth/EverySec drop unselected frames before the filtergraph,
            // so they never pay the scale/format conversion.
            builder = builder.filter("frame_export_selector", Box::new(selector));
        }
        // Bind to the explicit stream when given; otherwise the video stream
        // by media type (the resolver's best-stream selection coincides for
        // single-video-stream inputs, the common case — the resolver rejects
        // or warns on the mismatching layouts).
        if let Some(idx) = self.video_stream_index {
            builder = builder.set_stream_index(idx);
        }
        input = input.add_frame_pipeline(builder.build());

        // Null output: vsync passthrough (S4) preserves 1:1 frames and PTS.
        let mut output = Output::from("-")
            .set_format("null")
            .set_vsync_method(VSyncMethod::VsyncPassthrough)
            .add_stream_map("[export]")
            .add_frame_pipeline(sink_pipeline);
        // UniformN emits <= n unique frames and must reach EOF for its flush, so
        // it gets no encoder terminator; the sink's u64 cap stays authoritative.
        if uniform_n.is_none() {
            if let Some(max) = self.max_frames {
                // Upstream terminator only; the sink owns the exact public cap
                // (S5). If the cap exceeds i64, skip the terminator (the sink's
                // u64 cap stays authoritative) rather than wrapping negative.
                if let Ok(max_i64) = i64::try_from(max) {
                    output = output.set_max_video_frames(Some(max_i64));
                }
            }
        }

        // Deferred resolver: runs against the opened demuxer (S8/S9).
        let resolve_plan = ResolvePlan {
            stream_index: self.video_stream_index,
            width: self.width,
            height: self.height,
            pixel: self.pixel,
            color: self.color,
            precision: self.precision,
            input_side_sampling: matches!(
                self.sampling,
                Sampling::UniformN(_) | Sampling::EveryNth(_) | Sampling::EverySec(_)
            ),
            uniform: uniform_n.map(|_| UniformResolve {
                span_cell: span_cell.clone(),
                duration_hint_us: self.duration_hint_us,
                duration_us: effective_duration,
                start_time_us: effective_start,
            }),
        };
        let resolver = move |demuxs: &[Demuxer]| -> crate::error::Result<FilterComplex> {
            let demux = demuxs.first().ok_or(FrameExportError::NoVideoStream)?;
            // SAFETY: the demuxer is opened for the lifetime of this call.
            let desc = unsafe { resolve_and_build_desc(demux.in_fmt_ctx_ptr(), &resolve_plan)? };
            Ok(desc.into())
        };

        let context = FfmpegContext::builder()
            .input(input)
            .output(output)
            .add_deferred_filter_desc(Box::new(resolver))
            .build()?;
        let scheduler = FfmpegScheduler::new(context).start()?;
        Ok(FrameIter::new(rx, scheduler))
    }

    /// Runs to completion and collects every exported frame into a `Vec`.
    ///
    /// On a terminal error the partial frames are dropped and the error is
    /// returned; use [`frames`](FrameExtractor::frames) to keep partial output.
    pub fn collect_frames(self) -> crate::error::Result<Vec<VideoFrame>> {
        self.frames()?.collect()
    }

    fn validate(&self) -> crate::error::Result<()> {
        if self.channel_capacity == 0 {
            return Err(invalid("channel_capacity must be >= 1"));
        }
        match self.sampling {
            Sampling::EveryNth(0) => {
                return Err(invalid("EveryNth requires n >= 1"));
            }
            Sampling::EverySec(s) if !s.is_finite() || s <= 0.0 => {
                return Err(invalid(
                    "EverySec requires a finite, positive seconds value",
                ));
            }
            Sampling::UniformN(0) => {
                return Err(invalid("UniformN requires n >= 1"));
            }
            _ => {}
        }
        if self.width == Some(0) {
            return Err(invalid("width must be > 0"));
        }
        if self.height == Some(0) {
            return Err(invalid("height must be > 0"));
        }
        if self.max_frames == Some(0) {
            return Err(invalid("max_frames must be > 0"));
        }
        // Effective value: a recording time preconfigured on the Input feeds
        // the same trim machinery, where 0 means "no limit" to FFmpeg — the
        // opposite of the documented meaning.
        if let Some(d) = self.duration_us.or(self.input.recording_time_us) {
            if d <= 0 {
                return Err(invalid(
                    "duration_us (or the Input's recording time) must be > 0",
                ));
            }
        }
        // The hint is only consumed by UniformN; other modes ignore it as
        // documented, so it is validated only where it has meaning.
        if matches!(self.sampling, Sampling::UniformN(_)) {
            if let Some(h) = self.duration_hint_us {
                if h <= 0 {
                    return Err(invalid("duration_hint_us must be > 0"));
                }
            }
        }
        Ok(())
    }
}

fn invalid(msg: &str) -> crate::error::Error {
    FrameExportError::InvalidOption(msg.to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_every_nth_is_rejected() {
        let r = FrameExtractor::new("x.mp4")
            .sampling(Sampling::EveryNth(0))
            .validate();
        assert!(matches!(
            r,
            Err(crate::error::Error::FrameExport(
                FrameExportError::InvalidOption(_)
            ))
        ));
    }

    #[test]
    fn non_finite_every_sec_is_rejected() {
        assert!(FrameExtractor::new("x.mp4")
            .sampling(Sampling::EverySec(f64::NAN))
            .validate()
            .is_err());
        assert!(FrameExtractor::new("x.mp4")
            .sampling(Sampling::EverySec(-1.0))
            .validate()
            .is_err());
        assert!(FrameExtractor::new("x.mp4")
            .sampling(Sampling::EverySec(0.0))
            .validate()
            .is_err());
    }

    #[test]
    fn zero_dimensions_and_capacity_rejected() {
        assert!(FrameExtractor::new("x.mp4").width(0).validate().is_err());
        assert!(FrameExtractor::new("x.mp4").height(0).validate().is_err());
        assert!(FrameExtractor::new("x.mp4")
            .channel_capacity(0)
            .validate()
            .is_err());
        assert!(FrameExtractor::new("x.mp4")
            .max_frames(0)
            .validate()
            .is_err());
    }

    #[test]
    fn defaults_validate() {
        assert!(FrameExtractor::new("x.mp4").validate().is_ok());
        assert!(FrameExtractor::new("x.mp4")
            .sampling(Sampling::EverySec(1.5))
            .width(224)
            .height(224)
            .max_frames(16)
            .validate()
            .is_ok());
    }
}
