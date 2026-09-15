//! Frame-push inputs: feed raw video frames from Rust code into a full
//! [`FfmpegContext`](crate::FfmpegContext) job, with the caller supplying
//! each frame's presentation timestamp.
//!
//! This is the context-level sibling of [`VideoWriter`](crate::VideoWriter).
//! The writer is a self-contained facade — one CFR video stream, one output,
//! no other inputs — while a [`FramePushSource`](crate::FramePushSource) is an ordinary context
//! *input*: it occupies an input position like any file or callback input,
//! participates in `[N:v]`-style filter references by that position, and can
//! be combined with demuxer inputs (e.g. pushed video plus the audio streams
//! of a source file) in one job.
//!
//! The timing contract is **variable frame rate by passthrough**: every
//! pushed frame carries its own `pts` (and optionally a duration) in the
//! source's declared time base, and those values land verbatim on the
//! `AVFrame`s entering the filter graph — no constant-frame-rate grid is
//! fabricated in between. A constant-rate stream is just the special case of
//! pushing equidistant timestamps.
//!
//! **Experimental:** this API is new in 0.19 and its surface may still be
//! refined in minor releases while it settles.
//!
//! ```no_run
//! use ez_ffmpeg::{FfmpegContext, FramePushSource, Output};
//!
//! # fn frames() -> Vec<(Vec<u8>, i64)> { Vec::new() }
//! // Timestamps in microseconds (the default time base).
//! let (source, mut handle) = FramePushSource::builder(1920, 1080)
//!     .pixel_format("rgba")
//!     .build()?;
//!
//! let scheduler = FfmpegContext::builder()
//!     .input(source) // input 0: the pushed frames
//!     .filter_desc("[0:v]null")
//!     .output(Output::from("out.mp4").set_video_codec("libx264"))
//!     .build()?
//!     .start()?;
//!
//! for (pixels, pts_us) in frames() {
//!     handle.push_owned(pixels, pts_us, None)?;
//! }
//! handle.finish(); // end of stream; the job drains and finalizes
//! scheduler.wait()?;
//! # Ok::<(), ez_ffmpeg::error::Error>(())
//! ```
//!
//! # Contract
//!
//! - **Video only, one filter pad.** A push source must be consumed by
//!   exactly one video input pad of a filter graph — reference it by its
//!   input position (`[N:v]` or `[N]`), or leave the pad unlabeled and let
//!   position-ordered auto-binding pick it up. A source no graph consumes
//!   fails the build ([`FramePushError::UnboundSource`](crate::FramePushError::UnboundSource)): its frames would
//!   have nowhere to go and `push` would block forever.
//! - **Strictly increasing `pts`.** Each accepted frame's `pts` must exceed
//!   the previous one's ([`PushError::NonMonotonicPts`](crate::PushError::NonMonotonicPts) otherwise, and the
//!   frame is not queued). There is no silent reordering, dropping, or
//!   re-gridding; what you push is what the encoder sees.
//! - **Timestamp passthrough.** `pts`/`duration` are forwarded in the
//!   declared [`time_base`](crate::FramePushSourceBuilder::time_base) without
//!   rescaling or normalization: the first frame's `pts` is NOT shifted to
//!   zero (push zero-based timestamps, or configure the output's own
//!   normalization, if that is what the container should carry). A `None`
//!   duration is stored as 0 — FFmpeg's "unknown", derived downstream from
//!   timestamp deltas; the last frame of a stream has no successor, so give
//!   it an explicit duration when the tail matters.
//! - **Tight packing.** A frame is exactly [`frame_size`](crate::FramePushHandle::frame_size)
//!   bytes: `av_image_get_buffer_size(pix_fmt, w, h, 1)`, planes concatenated
//!   in descriptor order with no row padding — the same layout
//!   [`VideoWriter`](crate::VideoWriter) takes.
//! - **No input options.** A push source has no demuxer and no decoder, so
//!   every [`Input`](crate::Input) option (seek offsets, hardware
//!   acceleration, frame pipelines, ...) is rejected at build time
//!   ([`FramePushError::UnsupportedInputOption`](crate::FramePushError::UnsupportedInputOption)) rather than silently
//!   ignored.
//!
//! # End of stream and teardown
//!
//! Dropping the handle — or calling [`finish`](crate::FramePushHandle::finish),
//! which is the same thing spelled as intent — closes ingress; the worker
//! forwards buffered frames, emits the end-of-stream marker, and the job
//! completes through the normal drain path. **The handle owns only the
//! stream, not the job**: dropping it mid-stream truncates the input at
//! whatever was pushed (the job still finalizes normally — including when a
//! producer thread panics and unwinds the handle), and aborting the job is
//! the scheduler's affordance, not the handle's. `push` applies backpressure
//! when the bounded queue is full and fails fast with
//! [`PushError::PipelineClosed`](crate::PushError::PipelineClosed) once the job stops accepting frames.
//!
//! The fail-fast probe is armed when the context is **built**. Before a job
//! exists — the source not yet built into a context, or a built context not
//! yet `start()`ed — nothing drains the queue and nothing publishes a
//! terminal status, so once the queue fills, `push` parks until the source
//! (or the built context) is dropped, which closes ingress and fails the
//! parked push with `PipelineClosed`. Start the job before running the
//! producer flat out.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, SendTimeoutError};
use ffmpeg_sys_next::AVRational;

use crate::core::context::frame_source::{FrameSourceParams, PushedFrame};
use crate::core::scheduler::ffmpeg_scheduler::is_stopping;
use crate::core::writer::{
    adaptive_capacity, resolve_source_format_raw, OwnedPushError, PushError, SourceFormatIssue,
};

/// How long a full-queue push parks before re-checking the pipeline status.
/// Same backpressure poll as [`VideoWriter`](crate::VideoWriter)'s writes.
const SEND_POLL: Duration = Duration::from_millis(100);

/// Build-time validation errors for [`FramePushSource`] — both the builder's
/// own parameters and the context build steps that wire the source into a
/// job. Push-time errors are [`PushError`] / [`OwnedPushError`], shared with
/// [`VideoWriter`](crate::VideoWriter).
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum FramePushError {
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

    /// `time_base(num, den)` had a non-positive component.
    #[error("invalid time base {num}/{den}: both must be positive")]
    InvalidTimeBase { num: i32, den: i32 },

    /// `queue_capacity(0)` was requested.
    #[error("queue_capacity must be >= 1")]
    ZeroQueueCapacity,

    /// An [`Input`](crate::Input) option was set on a frame-push input. A
    /// push source has no demuxer, no decoder, and no byte stream, so no
    /// input option can take effect; rejecting is better than silently
    /// ignoring the configuration.
    #[error(
        "Input option {option} is not supported by a frame-push input: \
         pushed frames have no demuxer or decoder for it to act on"
    )]
    UnsupportedInputOption {
        /// The offending setter as spelled on [`Input`](crate::Input)
        /// (e.g. `"set_start_time_us"`).
        option: &'static str,
    },

    /// No filter graph consumes the frame-push input at this position.
    /// Pushed frames reach outputs only through a filter graph pad; without
    /// one, `push` would block forever against a queue nothing drains. Add a
    /// `filter_desc` referencing the input (even a plain `"[N:v]null"`).
    #[error(
        "frame-push input {input_index} is not consumed by any filter graph; \
         add a filter_desc referencing it (e.g. \"[{input_index}:v]null\")"
    )]
    UnboundSource { input_index: usize },

    /// More than one filter pad tried to consume the same frame-push input.
    /// A push source feeds exactly one pad; route it through `split` inside
    /// one graph when several branches need the frames.
    #[error(
        "frame-push input {input_index} is referenced by more than one filter \
         pad; feed one pad and use split inside the graph instead"
    )]
    SourceBoundTwice { input_index: usize },

    /// A filter pad referenced the frame-push input but does not match its
    /// single video stream — a non-video specifier (e.g. `[N:a]`), a stream
    /// index beyond the only stream (e.g. `[N:v:1]`), or a non-video
    /// consuming pad.
    #[error(
        "frame-push input {input_index} carries exactly one video stream; the \
         referencing pad or specifier '{spec}' does not match it"
    )]
    NotVideo {
        input_index: usize,
        /// The stream-specifier portion of the reference (empty when the pad
        /// type itself mismatched).
        spec: String,
    },

    /// An output stream map (`add_stream_map*`) referenced the frame-push
    /// input. Pushed frames carry no packet stream to map; route them
    /// through a filter graph instead.
    #[error(
        "stream map references frame-push input {input_index}, which has no \
         mappable packet stream; consume it with a filter graph instead"
    )]
    StreamMapReferencesFramePush { input_index: usize },
}

/// Builder for a [`FramePushSource`]. Required parameters (width, height) are
/// positional; everything else has a default.
pub struct FramePushSourceBuilder {
    width: u32,
    height: u32,
    pixel_format: String,
    tb_num: i32,
    tb_den: i32,
    queue_capacity: Option<usize>,
}

impl FramePushSourceBuilder {
    fn new(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            pixel_format: "rgba".to_string(),
            tb_num: 1,
            tb_den: 1_000_000,
            queue_capacity: None,
        }
    }

    /// Any non-hardware `AVPixelFormat` name (`av_get_pix_fmt`). Default:
    /// `"rgba"`. Frames are tightly packed, planes concatenated in descriptor
    /// order (e.g. `yuv420p` = Y then U then V).
    pub fn pixel_format(mut self, fmt: impl Into<String>) -> Self {
        self.pixel_format = fmt.into();
        self
    }

    /// The unit of every pushed `pts`/`duration`, as the rational tick
    /// `num/den` seconds. Default `(1, 1_000_000)` — microseconds. A source
    /// whose native timestamps are container ticks can pass that rational
    /// directly (e.g. `(1, 90000)`) and push the values unconverted.
    pub fn time_base(mut self, num: i32, den: i32) -> Self {
        self.tb_num = num;
        self.tb_den = den;
        self
    }

    /// Queue depth in frames. Default: `max(1, min(4, 64 MiB / frame_size))`,
    /// so large frames do not silently reserve a lot of memory. The queue is
    /// count-bounded: each slot holds one pushed frame as provided.
    pub fn queue_capacity(mut self, frames: usize) -> Self {
        self.queue_capacity = Some(frames);
        self
    }

    /// Validates the parameters and creates the source/handle pair: the
    /// [`FramePushSource`] goes into an
    /// [`FfmpegContext`](crate::FfmpegContext) as an input, the
    /// [`FramePushHandle`] stays with the producing code. No FFmpeg objects
    /// are created here; the pipeline behind the source comes from the
    /// context build.
    pub fn build(self) -> crate::error::Result<(FramePushSource, FramePushHandle)> {
        if self.width == 0
            || self.height == 0
            || self.width > i32::MAX as u32
            || self.height > i32::MAX as u32
        {
            return Err(FramePushError::InvalidDimensions {
                width: self.width,
                height: self.height,
            }
            .into());
        }
        if self.tb_num <= 0 || self.tb_den <= 0 {
            return Err(FramePushError::InvalidTimeBase {
                num: self.tb_num,
                den: self.tb_den,
            }
            .into());
        }
        let (pix_fmt, frame_size) =
            resolve_source_format_raw(&self.pixel_format, self.width, self.height).map_err(
                |issue| match issue {
                    SourceFormatIssue::Unknown => {
                        FramePushError::UnknownPixelFormat(self.pixel_format.clone())
                    }
                    SourceFormatIssue::Hardware => {
                        FramePushError::HardwarePixelFormat(self.pixel_format.clone())
                    }
                },
            )?;
        let queue_capacity = match self.queue_capacity {
            Some(0) => return Err(FramePushError::ZeroQueueCapacity.into()),
            Some(n) => n,
            None => adaptive_capacity(frame_size),
        };

        let (sender, receiver) = crossbeam_channel::bounded(queue_capacity);
        let status_slot = Arc::new(OnceLock::new());
        let time_base = AVRational {
            num: self.tb_num,
            den: self.tb_den,
        };
        let source = FramePushSource {
            ingress: receiver,
            params: FrameSourceParams {
                width: self.width as i32,
                height: self.height as i32,
                pix_fmt,
                time_base,
                // VFR: the graph must not advertise a constant rate the
                // pushed timestamps do not follow.
                framerate: None,
            },
            status_slot: status_slot.clone(),
        };
        let handle = FramePushHandle {
            sender: Some(sender),
            frame_size,
            time_base: (self.tb_num, self.tb_den),
            last_pts: None,
            status_slot,
        };
        Ok((source, handle))
    }
}

/// The context-side half of a frame-push input: passes to
/// `FfmpegContextBuilder::input` (via
/// `Into<Input>`) and takes an ordinary input position. See the
/// [module documentation](self) for the contract.
///
/// **Experimental:** new in 0.19; the surface may still be refined.
pub struct FramePushSource {
    pub(crate) ingress: Receiver<PushedFrame>,
    pub(crate) params: FrameSourceParams,
    /// Filled with the scheduler status atomic when the context is built, so
    /// the handle can fail fast once the job stops. Before that, pushes rely
    /// on queue backpressure alone.
    pub(crate) status_slot: Arc<OnceLock<Arc<AtomicUsize>>>,
}

impl FramePushSource {
    /// Starts a builder. Width and height are positional so they cannot be
    /// forgotten.
    pub fn builder(width: u32, height: u32) -> FramePushSourceBuilder {
        FramePushSourceBuilder::new(width, height)
    }
}

/// The producer-side half of a frame-push input. `Send` (move it to a
/// producer thread); pushes take `&mut self`, so one producer at a time.
///
/// **Experimental:** new in 0.19; the surface may still be refined.
pub struct FramePushHandle {
    /// `None` once [`finish`](Self::finish) consumed the sender.
    sender: Option<Sender<PushedFrame>>,
    frame_size: usize,
    time_base: (i32, i32),
    /// Timestamp of the last frame actually queued; the strict-monotonicity
    /// gate compares against it, and it advances only on success so a
    /// rejected frame leaves no gap.
    last_pts: Option<i64>,
    status_slot: Arc<OnceLock<Arc<AtomicUsize>>>,
}

impl FramePushHandle {
    /// Exact number of bytes one frame must contain:
    /// `av_image_get_buffer_size(pix_fmt, width, height, 1)` (tight packing).
    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    /// The `(num, den)` tick unit every pushed `pts`/`duration` is in.
    pub fn time_base(&self) -> (i32, i32) {
        self.time_base
    }

    /// Pushes one frame, copying the borrowed slice into an owned buffer.
    /// `pts` is in [`time_base`](Self::time_base) ticks and must be strictly
    /// greater than the previous accepted frame's; `duration` of `None`
    /// stores FFmpeg's "unknown" (0), derived downstream from timestamp
    /// deltas. Blocks while the internal queue is full (backpressure); see
    /// the [module docs](self) for when a blocked push fails fast.
    pub fn push(&mut self, frame: &[u8], pts: i64, duration: Option<i64>) -> Result<(), PushError> {
        if frame.len() != self.frame_size {
            return Err(PushError::InvalidSize {
                expected: self.frame_size,
                got: frame.len(),
            });
        }
        // The rejected buffer is this handle's own copy, not the caller's, so
        // dropping it on an error path loses nothing the caller could reuse.
        self.push_owned(frame.to_vec(), pts, duration)
            .map_err(PushError::from)
    }

    /// Pushes one owned frame; the `Vec` is moved into the pipeline, saving
    /// the borrow-copy that [`push`](Self::push) performs. Every error path
    /// hands the frame back inside [`OwnedPushError`] (the `SendError`
    /// convention), exactly like
    /// [`VideoWriter::write_owned`](crate::VideoWriter::write_owned).
    pub fn push_owned(
        &mut self,
        frame: Vec<u8>,
        pts: i64,
        duration: Option<i64>,
    ) -> Result<(), OwnedPushError> {
        if frame.len() != self.frame_size {
            let error = PushError::InvalidSize {
                expected: self.frame_size,
                got: frame.len(),
            };
            return Err(OwnedPushError::new(frame, error));
        }
        if let Some(d) = duration {
            if d < 0 {
                return Err(OwnedPushError::new(frame, PushError::InvalidDuration { got: d }));
            }
        }
        if let Some(prev) = self.last_pts {
            if pts <= prev {
                return Err(OwnedPushError::new(
                    frame,
                    PushError::NonMonotonicPts { prev, got: pts },
                ));
            }
        }
        // Fail fast on a stopping pipeline BEFORE queueing: probing only
        // after a send timeout could still admit one frame in the window
        // between the terminal publish and the queue draining. Before the
        // context is built the slot is empty and backpressure is the only
        // gate — nothing drains a never-started job, which is the documented
        // blocking behavior.
        if self
            .status_slot
            .get()
            .is_some_and(|status| is_stopping(status.load(Ordering::Acquire)))
        {
            return Err(OwnedPushError::new(frame, PushError::PipelineClosed));
        }
        let sender = match &self.sender {
            Some(sender) => sender,
            None => return Err(OwnedPushError::new(frame, PushError::PipelineClosed)),
        };
        let mut msg = PushedFrame {
            data: frame,
            pts,
            duration: duration.unwrap_or(0),
        };
        loop {
            match sender.send_timeout(msg, SEND_POLL) {
                Ok(()) => {
                    self.last_pts = Some(pts);
                    return Ok(());
                }
                Err(SendTimeoutError::Timeout(returned)) => {
                    // Full queue = backpressure. Re-check status so a pipeline
                    // that dies while we are parked wakes us instead of
                    // hanging.
                    if self
                        .status_slot
                        .get()
                        .is_some_and(|status| is_stopping(status.load(Ordering::Acquire)))
                    {
                        return Err(OwnedPushError::new(
                            returned.data,
                            PushError::PipelineClosed,
                        ));
                    }
                    msg = returned;
                }
                Err(SendTimeoutError::Disconnected(returned)) => {
                    return Err(OwnedPushError::new(
                        returned.data,
                        PushError::PipelineClosed,
                    ));
                }
            }
        }
    }

    /// Signals end of stream: the worker forwards everything still queued,
    /// emits the end-of-stream marker, and the job drains normally. Dropping
    /// the handle does exactly the same; this spelling makes the intent
    /// readable. The job's completion is observed on its scheduler
    /// ([`wait`](crate::FfmpegScheduler::wait)), not here.
    pub fn finish(mut self) {
        self.sender = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_validates_parameters() {
        assert!(matches!(
            FramePushSource::builder(0, 48).build(),
            Err(crate::error::Error::FramePush(
                FramePushError::InvalidDimensions { .. }
            ))
        ));
        assert!(matches!(
            FramePushSource::builder(64, 48).time_base(0, 1000).build(),
            Err(crate::error::Error::FramePush(
                FramePushError::InvalidTimeBase { .. }
            ))
        ));
        assert!(matches!(
            FramePushSource::builder(64, 48).pixel_format("nope").build(),
            Err(crate::error::Error::FramePush(
                FramePushError::UnknownPixelFormat(_)
            ))
        ));
        assert!(matches!(
            FramePushSource::builder(64, 48).queue_capacity(0).build(),
            Err(crate::error::Error::FramePush(
                FramePushError::ZeroQueueCapacity
            ))
        ));
    }

    #[test]
    fn push_rejects_wrong_size_and_returns_buffer() {
        let (_source, mut handle) = FramePushSource::builder(4, 2).build().expect("build");
        assert_eq!(handle.frame_size(), 4 * 2 * 4);
        let rejected = handle
            .push_owned(vec![0u8; 3], 0, None)
            .expect_err("wrong size must be rejected");
        assert!(matches!(
            rejected.error(),
            PushError::InvalidSize { expected, got: 3 } if *expected == 4 * 2 * 4
        ));
        assert_eq!(rejected.into_frame().len(), 3);
    }

    #[test]
    fn push_enforces_strictly_increasing_pts() {
        let (source, mut handle) = FramePushSource::builder(4, 2).build().expect("build");
        let size = handle.frame_size();
        handle.push(&vec![0u8; size], 10, Some(5)).expect("first");
        // Equal pts is rejected and does NOT advance the gate...
        let equal = handle.push(&vec![0u8; size], 10, None).unwrap_err();
        assert!(matches!(
            equal,
            PushError::NonMonotonicPts { prev: 10, got: 10 }
        ));
        // ...and a regression is rejected against the ACCEPTED predecessor.
        let backwards = handle.push(&vec![0u8; size], 3, None).unwrap_err();
        assert!(matches!(
            backwards,
            PushError::NonMonotonicPts { prev: 10, got: 3 }
        ));
        handle.push(&vec![0u8; size], 11, None).expect("recovery");

        // The queued frames carry the pushed values verbatim (None -> 0).
        let first = source.ingress.try_recv().expect("frame 1 queued");
        assert_eq!((first.pts, first.duration), (10, 5));
        let second = source.ingress.try_recv().expect("frame 2 queued");
        assert_eq!((second.pts, second.duration), (11, 0));
        assert!(source.ingress.try_recv().is_err(), "rejected frames must not queue");
    }

    #[test]
    fn push_rejects_negative_duration() {
        let (_source, mut handle) = FramePushSource::builder(4, 2).build().expect("build");
        let size = handle.frame_size();
        assert!(matches!(
            handle.push(&vec![0u8; size], 0, Some(-1)).unwrap_err(),
            PushError::InvalidDuration { got: -1 }
        ));
    }

    #[test]
    fn finish_and_source_drop_fail_pushes_cleanly() {
        let (source, mut handle) = FramePushSource::builder(4, 2).build().expect("build");
        let size = handle.frame_size();
        drop(source); // receiver gone (job never built / already torn down)
        let rejected = handle.push_owned(vec![7u8; size], 0, None).unwrap_err();
        assert!(matches!(rejected.error(), PushError::PipelineClosed));
        assert_eq!(rejected.into_frame(), vec![7u8; size]);
    }

    #[test]
    fn stopping_status_fails_fast() {
        use crate::core::scheduler::ffmpeg_scheduler::STATUS_END;
        let (source, mut handle) = FramePushSource::builder(4, 2).build().expect("build");
        let size = handle.frame_size();
        source
            .status_slot
            .set(Arc::new(AtomicUsize::new(STATUS_END)))
            .ok();
        assert!(matches!(
            handle.push(&vec![0u8; size], 0, None).unwrap_err(),
            PushError::PipelineClosed
        ));
    }
}
