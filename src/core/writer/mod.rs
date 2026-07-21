//! Push raw video frames from Rust code into a full FFmpeg pipeline.
//!
//! [`VideoWriter`](crate::VideoWriter) is a narrow, ergonomic facade over the
//! crate's existing [`Output`](crate::Output) surface: you build frames in
//! memory (any packed or planar `AVPixelFormat` — `rgba`, `rgb24`, `gray8`,
//! `yuv420p`, `nv12`, …, delivered as tightly packed plane bytes) and push
//! them in, and they flow through the ordinary filter → encode → mux
//! pipeline. Because the destination is a plain [`Output`](crate::Output),
//! the writer accepts any video encoder and container the linked FFmpeg build
//! supports for that pairing, plus filter chains (one video input, one video
//! output, with a directed input-to-output path), GPU frame pipelines, and
//! RTMP targets. Out of scope: stream maps are rejected, and video
//! stream-copy does not apply (frames are raw, so they are always encoded).
//!
//! **Experimental:** this API is new in 0.14 and its surface may still be
//! refined in minor releases while it settles.
//!
//! ```no_run
//! use ez_ffmpeg::{Output, VideoWriter};
//!
//! # fn render(_i: usize) -> Vec<u8> { vec![0u8; 1920 * 1080 * 4] }
//! // Pick an encoder explicitly: with a bare "out.mp4" the linked FFmpeg
//! // build chooses the container default (H.264 when libx264 is compiled
//! // in, otherwise mpeg4 at a low default bitrate).
//! let out = Output::from("out.mp4").set_video_codec("mpeg4").set_video_qscale(5);
//! let mut writer = VideoWriter::builder(1920, 1080).fps(30, 1).open(out)?;
//! for i in 0..300 {
//!     writer.write_owned(render(i))?; // one RGBA frame, tightly packed
//! }
//! writer.finish()?; // drains the encoder, finalizes the container
//! # Ok::<(), ez_ffmpeg::error::Error>(())
//! ```
//!
//! # Scope (v1)
//!
//! - **Constant frame rate, video only.** Every pushed frame advances exactly
//!   `den/num` seconds; there is no per-frame PTS and no audio. `write` takes
//!   `&mut self` so the total frame order is fixed at compile time.
//! - **Tight packing.** A frame is exactly
//!   [`frame_size`](crate::VideoWriter::frame_size) bytes:
//!   `av_image_get_buffer_size(pix_fmt, w, h, 1)`, planes concatenated in
//!   descriptor order with no row padding.
//!
//! # How it works
//!
//! There is no demuxer and no decoder: the builder assembles a pipeline whose
//! single filtergraph (`buffersrc → [filter_desc | null] → buffersink`) is fed
//! directly by a frame-source worker. Pushed frames cross an in-process
//! bounded channel to that worker, which copies them plane-by-plane into
//! pooled `AVFrame`s and hands them to the filtergraph — exactly where a
//! decoder would. End of stream is an explicit in-band marker the worker
//! enqueues when ingress closes, so frames still buffered inside filters
//! (e.g. `reverse`) are flushed with correct tail timing. Teardown order is
//! owned by [`VideoWriter`](crate::VideoWriter):
//!
//! - [`finish`](crate::VideoWriter::finish) closes ingress (the worker emits
//!   the EOF marker), then calls `wait()` — the authoritative way to retrieve
//!   the pipeline's first error.
//! - [`Drop`] does the same and logs any error; it may block until the encoder
//!   drains, so prefer `finish()` when you need the result.
//! - [`abort`](crate::VideoWriter::abort) discards the export (closes ingress,
//!   then `abort()`s the scheduler); the partial output is not guaranteed
//!   playable.
//!
//! # Memory
//!
//! Only the **ingress** queue is bounded, by frame count: `queue_capacity`
//! frames (an exact `frame_size` copy per slot with
//! [`write`](crate::VideoWriter::write); the caller's `Vec` as provided with
//! [`write_owned`](crate::VideoWriter::write_owned)), plus a bounded handful
//! of in-flight frames in the internal channels.
//! Filters that buffer (`reverse`, `tpad`, …), the codec's lookahead, and
//! output I/O can each hold further data with no global limit — exactly as
//! they do in any other job of this crate.
//!
//! # FFmpeg versions
//!
//! Works on every FFmpeg release this crate supports (7.1–8.x); it opens no
//! input format context, so no version-specific probing behavior applies.

use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{SendTimeoutError, Sender};
use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE;
use ffmpeg_sys_next::{
    av_get_pix_fmt, av_image_get_buffer_size, av_pix_fmt_desc_get, AVPixelFormat,
    AV_PIX_FMT_FLAG_HWACCEL,
};

use crate::core::context::ffmpeg_context::build_writer_context;
use crate::core::context::frame_source::FrameSourceParams;
use crate::core::context::output::Output;
use crate::core::scheduler::ffmpeg_scheduler::{is_stopping, FfmpegScheduler, Running};
use crate::error::OpenOutputError;

/// Ingress memory budget used to derive the default queue depth.
const QUEUE_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// How long `write` parks on a full queue before re-checking the pipeline
/// status. Backpressure, not a busy loop: the crossbeam fast path returns
/// immediately when there is room.
const SEND_POLL: Duration = Duration::from_millis(100);

/// Errors raised while validating a [`VideoWriterBuilder`] or opening its
/// pipeline. Reachable through [`crate::error::Error::Writer`]; it is exported
/// from [`crate::error`] rather than the crate root to keep the root surface to
/// the three settled writer types.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    /// Width or height is zero or exceeds `i32::MAX`.
    #[error("invalid dimensions {width}x{height}")]
    InvalidDimensions { width: u32, height: u32 },

    /// The pixel-format name is not known to `av_get_pix_fmt`.
    #[error("unknown pixel format '{0}'")]
    UnknownPixelFormat(String),

    /// A hardware pixel format (e.g. `cuda`, `vaapi`) cannot be filled from a
    /// CPU byte buffer.
    #[error("hardware pixel format '{0}' cannot be pushed from CPU memory")]
    HardwarePixelFormat(String),

    /// `fps(num, den)` had a non-positive component.
    #[error("invalid fps {num}/{den}: both must be positive")]
    InvalidFps { num: i32, den: i32 },

    /// `queue_capacity(0)` was requested.
    #[error("queue_capacity must be >= 1")]
    ZeroQueueCapacity,

    /// The built pipeline consumes no video from the pushed frames (a
    /// `disable_video()` output). Without this check `write` would block
    /// forever against a live-but-unconsumed receiver.
    #[error("output consumes no video stream from the pushed frames")]
    NoVideoDestination,

    /// The `filter_desc` graph does not consume exactly one video input and
    /// produce exactly one video output. A second input pad would have no
    /// producer (the pipeline would buffer forever waiting for it) and a
    /// second output pad would have no destination.
    #[error(
        "filter_desc must have exactly one video input pad and one video \
         output pad; found {input_pads} input pad(s) ({video_input_pads} \
         video) and {output_pads} output pad(s) ({video_output_pads} video)"
    )]
    FilterShape {
        input_pads: usize,
        video_input_pads: usize,
        output_pads: usize,
        video_output_pads: usize,
    },

    /// Both [`VideoWriterBuilder::filter_desc`] and the opened `Output`'s
    /// `set_video_filter` were configured. The writer runs exactly one
    /// filter chain between the pushed frames and the encoder, and guessing
    /// which of the two the caller meant would silently ignore the other —
    /// configure the chain in exactly one place.
    #[error(
        "both VideoWriterBuilder::filter_desc and Output::set_video_filter are set; \
         configure the writer's filter chain in exactly one place"
    )]
    ConflictingFilterDescriptions,

    /// The `filter_desc` parses into more than one disconnected filter
    /// component (e.g. `"nullsink;color=..."`). The pushed frames would feed
    /// one part while an unrelated part feeds (or starves) the encoder — an
    /// unbounded side source could even keep the job from ever finishing.
    #[error("filter_desc must be a single connected graph; found {components} disconnected parts")]
    DisconnectedFilterGraph { components: usize },

    /// The `filter_desc` is connected, but no directed path leads from its
    /// input pad to its output pad (e.g.
    /// `"color,split[out][aux];[aux][in]overlay,nullsink"`): the pushed
    /// frames drain into a sink while an unrelated branch feeds the encoder,
    /// so they could never influence the encoded output — and an unbounded
    /// side source would keep the job from ever finishing.
    ///
    /// This check is STRUCTURAL: it follows the links between filters, and
    /// inside each filter every input pad is assumed to influence every
    /// output pad. Filter routing is not pruned by the applied options,
    /// because libavfilter routing is not static — a selector's `map`
    /// (`streamselect`) can be rewritten mid-stream by `sendcmd` or the
    /// send-command API, and ffmpeg itself accepts and runs descriptions
    /// whose current selection drops the pushed stream. A description that
    /// discards the pushed frames at runtime (an unselected `streamselect`
    /// input, a multi-stream `concat` steering them into a sink leg, ...)
    /// therefore passes this gate and runs as declared, exactly like the
    /// CLI; what cannot pass is a graph where no wiring could ever carry the
    /// pushed frames toward the encoder.
    #[error(
        "filter_desc has no directed path from its input pad to its output \
         pad; the pushed frames could not influence the encoded output"
    )]
    UnreachableFilterOutput,

    /// The [`Output`] carries stream maps (`add_stream_map` /
    /// `add_stream_map_with_copy`). The writer's single video stream is the
    /// only stream there is, so maps have nothing to select; rejecting them
    /// beats silently ignoring them.
    #[error("stream maps are not supported by VideoWriter; the pushed frames are the only stream")]
    StreamMapsUnsupported,
}

/// Errors returned by [`VideoWriter::write`] / [`VideoWriter::write_owned`].
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum PushError {
    /// The frame was not exactly [`VideoWriter::frame_size`] bytes.
    #[error("frame has {got} bytes, expected exactly {expected} (tightly packed)")]
    InvalidSize { expected: usize, got: usize },

    /// The pipeline stopped accepting frames (a worker failed, an [`Output`]
    /// limit such as `set_max_video_frames` completed the job early, or
    /// teardown began). Call [`VideoWriter::finish`] for the authoritative
    /// result: the underlying error if one occurred, or `Ok` when the
    /// pipeline closed after completing normally.
    #[error("pipeline closed; call finish() to retrieve the pipeline result")]
    PipelineClosed,
}

/// Pushes raw video frames from Rust code into a full FFmpeg pipeline. See the
/// [module documentation](self) for the frame contract and teardown semantics.
///
/// **Experimental:** new in 0.14; the surface may still be refined.
pub struct VideoWriter {
    /// `None` once ingress is closed by `finish`/`abort`/`Drop`.
    sender: Option<Sender<Vec<u8>>>,
    /// `None` once consumed by `finish`/`abort`, so `Drop` is a no-op afterward.
    scheduler: Option<FfmpegScheduler<Running>>,
    frame_size: usize,
    /// The scheduler status atomic, polled by `write` to fail fast when the
    /// pipeline is stopping rather than blocking on a full queue forever.
    status: Arc<AtomicUsize>,
}

// Send: a writer can move to a dedicated producer thread. Not Sync: `write`
// takes `&mut self`, which already forbids concurrent producers.
impl VideoWriter {
    /// Starts a builder. Width and height are positional so they cannot be
    /// forgotten.
    pub fn builder(width: u32, height: u32) -> VideoWriterBuilder {
        VideoWriterBuilder::new(width, height)
    }

    /// Exact number of bytes one frame must contain:
    /// `av_image_get_buffer_size(pix_fmt, width, height, 1)` (tight packing).
    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    /// Pushes one frame, copying the borrowed slice into an owned buffer.
    /// Blocks while the internal queue is full (backpressure).
    pub fn write(&mut self, frame: &[u8]) -> Result<(), PushError> {
        if frame.len() != self.frame_size {
            return Err(PushError::InvalidSize {
                expected: self.frame_size,
                got: frame.len(),
            });
        }
        self.enqueue(frame.to_vec())
    }

    /// Pushes one owned frame; the `Vec` is moved into the pipeline, saving
    /// the borrow-copy that [`write`](Self::write) performs. The pipeline
    /// still copies the bytes once, plane-by-plane, into an aligned `AVFrame`
    /// on the worker thread. `frame.len()` must equal
    /// [`frame_size`](Self::frame_size); the `Vec` is queued as-is, so any
    /// spare `capacity` beyond its length stays allocated while it waits.
    pub fn write_owned(&mut self, frame: Vec<u8>) -> Result<(), PushError> {
        if frame.len() != self.frame_size {
            return Err(PushError::InvalidSize {
                expected: self.frame_size,
                got: frame.len(),
            });
        }
        self.enqueue(frame)
    }

    fn enqueue(&mut self, frame: Vec<u8>) -> Result<(), PushError> {
        // Check the terminal state before sending: probing it only after a send
        // timeout could still admit one frame in the window between the
        // terminal publish and the queue draining.
        if is_stopping(self.status.load(Ordering::Acquire)) {
            return Err(PushError::PipelineClosed);
        }
        let sender = match &self.sender {
            Some(sender) => sender,
            None => return Err(PushError::PipelineClosed),
        };
        let mut msg = frame;
        loop {
            match sender.send_timeout(msg, SEND_POLL) {
                Ok(()) => return Ok(()),
                Err(SendTimeoutError::Timeout(returned)) => {
                    // Full queue = backpressure. Re-check status so a pipeline
                    // that dies while we are parked wakes us instead of hanging.
                    if is_stopping(self.status.load(Ordering::Acquire)) {
                        return Err(PushError::PipelineClosed);
                    }
                    msg = returned;
                }
                Err(SendTimeoutError::Disconnected(_)) => return Err(PushError::PipelineClosed),
            }
        }
    }

    /// Closes ingress (the frame source emits its end-of-stream marker),
    /// drains the encoder, finalizes the container, and returns the first
    /// authoritative pipeline error. This is the result path — a `write` that
    /// returned [`PushError::PipelineClosed`] resolves here, either to the
    /// real cause or to `Ok` when the pipeline closed after completing
    /// normally (e.g. an [`Output`] frame limit was reached).
    pub fn finish(mut self) -> crate::error::Result<()> {
        self.sender = None; // drop the sender → the frame source observes EOF
        match self.scheduler.take() {
            Some(scheduler) => scheduler.wait(),
            None => Ok(()),
        }
    }

    /// Discards the export: drops ingress, then hard-aborts the scheduler. For
    /// "user cancelled" flows where the output is not wanted.
    ///
    /// Caveat: `abort()` blocks until every worker releases its slot (no fixed
    /// time bound) and may skip the encoder flush / muxer trailer, so the
    /// partial file is not guaranteed playable. Use [`finish`](Self::finish) to
    /// finalize.
    pub fn abort(mut self) {
        self.sender = None;
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.abort();
        }
    }
}

impl Drop for VideoWriter {
    fn drop(&mut self) {
        // Close ingress first, then wait: the dropped sender is what lets the
        // frame source emit EOF and exit, so the worker join can complete.
        self.sender = None;
        if let Some(scheduler) = self.scheduler.take() {
            if let Err(e) = scheduler.wait() {
                log::error!("VideoWriter dropped without finish(); pipeline error: {e}");
            }
        }
    }
}

/// Builder for a [`VideoWriter`]. Required parameters (width, height) are
/// positional; everything else has a default.
pub struct VideoWriterBuilder {
    width: u32,
    height: u32,
    pixel_format: String,
    fps_num: i32,
    fps_den: i32,
    queue_capacity: Option<usize>,
    filter_desc: Option<String>,
}

impl VideoWriterBuilder {
    fn new(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            pixel_format: "rgba".to_string(),
            fps_num: 30,
            fps_den: 1,
            queue_capacity: None,
            filter_desc: None,
        }
    }

    /// Any non-hardware `AVPixelFormat` name (`av_get_pix_fmt`). Default:
    /// `"rgba"`. Frames are tightly packed, planes concatenated in descriptor
    /// order (e.g. `yuv420p` = Y then U then V).
    pub fn pixel_format(mut self, fmt: impl Into<String>) -> Self {
        self.pixel_format = fmt.into();
        self
    }

    /// Frame rate as `num/den`. Default `(30, 1)`; NTSC rates like
    /// `fps(30000, 1001)` are supported. Every written frame advances exactly
    /// `den/num` seconds.
    pub fn fps(mut self, num: i32, den: i32) -> Self {
        self.fps_num = num;
        self.fps_den = den;
        self
    }

    /// Queue depth in frames. Default: `max(1, min(4, 64 MiB / frame_size))`, so
    /// large frames do not silently reserve a lot of memory. The queue is
    /// count-bounded: with [`write`](VideoWriter::write) each slot holds an
    /// exact `frame_size` copy (`capacity × frame_size` bytes total); with
    /// [`write_owned`](VideoWriter::write_owned) each slot holds the caller's
    /// `Vec` as provided, including any spare capacity it carries.
    pub fn queue_capacity(mut self, frames: usize) -> Self {
        self.queue_capacity = Some(frames);
        self
    }

    /// Optional FFmpeg filter chain between the pushed frames and the encoder,
    /// e.g. `"hue=s=0"` or `"pad=ceil(iw/2)*2:ceil(ih/2)*2"` (an odd-size
    /// remedy). Must be a single connected graph
    /// ([`WriterError::DisconnectedFilterGraph`] otherwise) consuming exactly
    /// one video input and producing exactly one video output
    /// ([`WriterError::FilterShape`] otherwise), with the output downstream
    /// of the input ([`WriterError::UnreachableFilterOutput`] otherwise).
    ///
    /// Generator filters embedded in the graph (`color`, `testsrc`, …) are
    /// allowed as long as the pushed frames still reach the output — e.g.
    /// compositing over a generated background with
    /// `"color=...[bg];[in][bg]overlay=shortest=1"`. The same rules as the
    /// FFmpeg CLI apply: a graph built to outlive the pushed stream (an
    /// `overlay` without `shortest=1`, a `concat` onto an unbounded
    /// generator) keeps running after [`finish`](VideoWriter::finish) closes
    /// the input, until the generator ends or the job is aborted — that is
    /// the semantics asked for, not a writer malfunction.
    ///
    /// The validation is structural (see
    /// [`WriterError::UnreachableFilterOutput`]): the pushed frames must be
    /// wired into the flow that feeds the output, but a filter that may
    /// discard them at runtime is accepted — a `streamselect` whose current
    /// `map` selects another input still passes, since that map is
    /// commandable mid-stream (`sendcmd`), and a multi-stream `concat`
    /// steering the pushed stream into a sink leg runs exactly as declared,
    /// like both would in the CLI.
    ///
    /// The opened `Output`'s `set_video_filter` supplies the same chain from
    /// the output side and is honored when this builder-level description is
    /// absent; setting BOTH fails with
    /// [`WriterError::ConflictingFilterDescriptions`].
    pub fn filter_desc(mut self, desc: impl Into<String>) -> Self {
        self.filter_desc = Some(desc.into());
        self
    }

    /// Builds the pipeline and starts it, returning without waiting for the
    /// first frame. `output` carries the [`Output`] capabilities that make
    /// sense for a single pushed video stream: codec, codec options, format,
    /// format options (`movflags`…), write/seek callbacks, frame pipelines
    /// (wgpu…), bitrate, qscale, frame limits, and `set_video_filter` (used
    /// when no builder-level [`filter_desc`](Self::filter_desc) is set;
    /// both at once is
    /// [`WriterError::ConflictingFilterDescriptions`]). Stream maps are the
    /// exception and are rejected ([`WriterError::StreamMapsUnsupported`]) — there is
    /// only one stream to map. A format that cannot actually carry the video
    /// stream is not second-guessed at build time: it surfaces as a pipeline
    /// error from [`finish`](VideoWriter::finish), like any other muxer
    /// failure.
    pub fn open(self, output: impl Into<Output>) -> crate::error::Result<VideoWriter> {
        if self.width == 0
            || self.height == 0
            || self.width > i32::MAX as u32
            || self.height > i32::MAX as u32
        {
            return Err(WriterError::InvalidDimensions {
                width: self.width,
                height: self.height,
            }
            .into());
        }
        if self.fps_num <= 0 || self.fps_den <= 0 {
            return Err(WriterError::InvalidFps {
                num: self.fps_num,
                den: self.fps_den,
            }
            .into());
        }
        let (pix_fmt, frame_size) =
            resolve_source_format(&self.pixel_format, self.width, self.height)?;
        let queue_capacity = match self.queue_capacity {
            Some(0) => return Err(WriterError::ZeroQueueCapacity.into()),
            Some(n) => n,
            None => adaptive_capacity(frame_size),
        };

        let params = FrameSourceParams {
            width: self.width as i32,
            height: self.height as i32,
            pix_fmt,
            fps_num: self.fps_num,
            fps_den: self.fps_den,
        };

        let (ctx, sender) = match build_writer_context(
            params,
            queue_capacity,
            self.filter_desc.as_deref(),
            output.into(),
        ) {
            Ok(built) => built,
            // disable_video()/streamless output: the build detects that the
            // pushed frames have no destination. Translate to the
            // writer-facing cause.
            Err(crate::error::Error::OpenOutput(OpenOutputError::NotContainStream)) => {
                return Err(WriterError::NoVideoDestination.into());
            }
            Err(e) => return Err(e),
        };

        // Clone the status before the context moves into the scheduler; new()
        // clones the same Arc internally, so no accessor is needed.
        let status = ctx.scheduler_status.clone();
        let scheduler = FfmpegScheduler::new(ctx);
        let scheduler = scheduler.start()?;

        Ok(VideoWriter {
            sender: Some(sender),
            scheduler: Some(scheduler),
            frame_size,
            status,
        })
    }
}

/// Default queue depth: cap the ingress buffer at `QUEUE_BUDGET_BYTES` while
/// keeping at least one and at most four frames. `frame_size >= 1` is
/// guaranteed by the caller, so the division never traps.
fn adaptive_capacity(frame_size: usize) -> usize {
    (QUEUE_BUDGET_BYTES / frame_size).clamp(1, 4)
}

/// Resolves and validates the pixel format plus the tightly-packed frame size
/// for it at `width`x`height`. Rejects unknown and hardware formats.
/// `width`/`height` are already known positive and `<= i32::MAX`.
fn resolve_source_format(
    pixel_format: &str,
    width: u32,
    height: u32,
) -> Result<(AVPixelFormat, usize), WriterError> {
    let cstr = CString::new(pixel_format)
        .map_err(|_| WriterError::UnknownPixelFormat(pixel_format.to_string()))?;
    // SAFETY: `cstr` is a valid NUL-terminated string for the duration of the
    // call; av_get_pix_fmt only reads it.
    let pix_fmt = unsafe { av_get_pix_fmt(cstr.as_ptr()) };
    if pix_fmt == AV_PIX_FMT_NONE {
        return Err(WriterError::UnknownPixelFormat(pixel_format.to_string()));
    }
    // SAFETY: pix_fmt is a valid, non-NONE format; av_pix_fmt_desc_get returns a
    // static descriptor or null.
    let desc = unsafe { av_pix_fmt_desc_get(pix_fmt) };
    if !desc.is_null() && unsafe { (*desc).flags } & (AV_PIX_FMT_FLAG_HWACCEL as u64) != 0 {
        return Err(WriterError::HardwarePixelFormat(pixel_format.to_string()));
    }
    // SAFETY: pix_fmt is valid; dimensions are within i32 range.
    let size = unsafe { av_image_get_buffer_size(pix_fmt, width as i32, height as i32, 1) };
    if size <= 0 {
        return Err(WriterError::UnknownPixelFormat(pixel_format.to_string()));
    }
    Ok((pix_fmt, size as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_size_of(fmt: &str, w: u32, h: u32) -> Result<usize, WriterError> {
        resolve_source_format(fmt, w, h).map(|(_, size)| size)
    }

    #[test]
    fn frame_size_matches_ffmpeg_for_common_formats() {
        // rgba/rgb24/gray8: exact, no padding.
        assert_eq!(frame_size_of("rgba", 64, 48).unwrap(), 64 * 48 * 4);
        assert_eq!(frame_size_of("rgb24", 64, 48).unwrap(), 64 * 48 * 3);
        assert_eq!(frame_size_of("gray8", 64, 48).unwrap(), 64 * 48);
        // yuv420p: Y + U/4 + V/4 = w*h*3/2 for even dimensions.
        assert_eq!(frame_size_of("yuv420p", 64, 48).unwrap(), 64 * 48 * 3 / 2);
        // nv12: same total as yuv420p.
        assert_eq!(frame_size_of("nv12", 64, 48).unwrap(), 64 * 48 * 3 / 2);
    }

    #[test]
    fn yuv420p_odd_height_rounds_chroma_up() {
        // Odd height: chroma planes use ceil(h/2). 64x49: Y=64*49, each chroma
        // plane = 32*25, total = 64*49 + 2*(32*25).
        let expected = 64 * 49 + 2 * (32 * 25);
        assert_eq!(frame_size_of("yuv420p", 64, 49).unwrap(), expected);
    }

    #[test]
    fn unknown_pixel_format_is_rejected() {
        assert!(matches!(
            frame_size_of("definitely_not_a_format", 16, 16),
            Err(WriterError::UnknownPixelFormat(_))
        ));
    }

    #[test]
    fn hardware_pixel_format_is_rejected() {
        // cuda is a hwaccel format when the build knows it; if av_get_pix_fmt
        // does not recognize it, UnknownPixelFormat is the honest answer. Either
        // way it must not be accepted as a CPU push format.
        assert!(matches!(
            frame_size_of("cuda", 16, 16),
            Err(WriterError::HardwarePixelFormat(_)) | Err(WriterError::UnknownPixelFormat(_))
        ));
    }

    #[test]
    fn adaptive_capacity_scales_with_frame_size() {
        // Small frame: capped at 4.
        assert_eq!(adaptive_capacity(1), 4);
        assert_eq!(adaptive_capacity(1024), 4);
        // 1080p rgba (~7.9 MiB): 64 MiB / 7.9 MiB = 8 → clamped to 4.
        assert_eq!(adaptive_capacity(1920 * 1080 * 4), 4);
        // 4K rgba (~31.6 MiB): floor(64 / 31.6) = 2.
        assert_eq!(adaptive_capacity(3840 * 2160 * 4), 2);
        // Frame larger than the whole budget: at least 1.
        assert_eq!(adaptive_capacity(QUEUE_BUDGET_BYTES * 2), 1);
    }

    #[test]
    fn invalid_dimensions_and_fps_are_rejected_at_open() {
        use crate::Output;
        let out = || Output::from("/dev/null").set_video_codec("mpeg4");
        assert!(matches!(
            VideoWriter::builder(0, 48).open(out()),
            Err(crate::error::Error::Writer(
                WriterError::InvalidDimensions { .. }
            ))
        ));
        assert!(matches!(
            VideoWriter::builder(64, 48).fps(0, 1).open(out()),
            Err(crate::error::Error::Writer(WriterError::InvalidFps { .. }))
        ));
        assert!(matches!(
            VideoWriter::builder(64, 48).queue_capacity(0).open(out()),
            Err(crate::error::Error::Writer(WriterError::ZeroQueueCapacity))
        ));
        assert!(matches!(
            VideoWriter::builder(64, 48)
                .pixel_format("nope")
                .open(out()),
            Err(crate::error::Error::Writer(
                WriterError::UnknownPixelFormat(_)
            ))
        ));
    }

    /// Compile-time proof that `?` composes across `write` and `finish` in a
    /// function returning `crate::error::Result` (the `From<PushError>` /
    /// `From<WriterError>` conversions exist).
    #[allow(dead_code)]
    fn error_composition(writer: &mut VideoWriter, frame: &[u8]) -> crate::error::Result<()> {
        writer.write(frame)?;
        Ok(())
    }

    #[test]
    fn push_error_display_reports_sizes() {
        let e = PushError::InvalidSize {
            expected: 12288,
            got: 100,
        };
        assert!(e.to_string().contains("12288"));
        assert!(e.to_string().contains("100"));
    }

    /// Teardown liveness with the producer still alive: the frame-source
    /// worker sits in no wake set, so scheduler teardown must reach it purely
    /// through its 100 ms status polls. Drop the running scheduler (or
    /// abort()) while the ingress sender is still held and no EOF was ever
    /// signalled — the RunningGuard's join must complete anyway. A regression
    /// here (e.g. an untimed recv) hangs, so the join runs under a watchdog.
    #[test]
    fn scheduler_teardown_completes_with_live_ingress_sender() {
        use crate::core::context::ffmpeg_context::build_writer_context;
        use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_RGBA;

        for (label, abort) in [("drop", false), ("abort", true)] {
            let out = std::env::temp_dir().join(format!(
                "ez_ffmpeg_writer_sender_held_{label}_{}.mp4",
                std::process::id()
            ));
            let params = FrameSourceParams {
                width: 64,
                height: 48,
                pix_fmt: AV_PIX_FMT_RGBA,
                fps_num: 30,
                fps_den: 1,
            };
            let (ctx, sender) = build_writer_context(
                params,
                2,
                None,
                Output::from(out.to_str().unwrap()).set_video_codec("mpeg4"),
            )
            .expect("writer context");
            let scheduler = FfmpegScheduler::new(ctx).start().expect("start");
            // Put the worker mid-stream so it holds a pooled frame path, not
            // just an idle recv.
            sender
                .send(vec![0u8; 64 * 48 * 4])
                .expect("worker must be consuming");

            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                if abort {
                    scheduler.abort();
                } else {
                    drop(scheduler);
                }
                let _ = tx.send(());
            });
            // Hang-detection watchdog, generous for loaded machines: the
            // property is that teardown completes at all, not how fast.
            rx.recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|_| panic!("{label}: teardown hung with a live ingress sender"));
            drop(sender);
        }
    }
}
