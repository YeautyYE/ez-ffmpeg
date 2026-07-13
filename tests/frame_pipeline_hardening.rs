//! Frame-pipeline hardening: the user-facing `FrameFilter` boundary.
//!
//! - The scheduler's internal end-of-stream sentinel is a `Frame` wrapping a
//!   NULL `AVFrame` (`dec_task` dec_done, `filter_task` close_output). The
//!   pipeline must forward it around user filters, never through them:
//!   ffmpeg_next's safe accessors (`pts()`, `width()`, `is_key()`, ...)
//!   dereference the pointer unconditionally, so a filter inspecting the
//!   frame would hit a null dereference at every stream end. The short
//!   circuit must not disturb end-of-stream draining either: the sentinel
//!   still has to reach the encoder so it flushes and the muxer finalizes.
//! - The pipeline's request_frame drain loop must re-check the scheduler
//!   status and the surviving senders on every iteration. A saturating
//!   generator filter (request_frame always yields a frame) used to keep
//!   that inner loop spinning after stop(): the downstream encoder exits,
//!   every send fails, the sender list empties — and the loop kept producing
//!   frames into the void at 100% CPU, hanging stop()/wait() forever.

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{AVMediaType, FfmpegContext, Frame, Input, Output};
use ffmpeg_sys_next::{
    av_buffer_create, av_frame_copy_props, av_frame_new_side_data_from_buf, av_frame_ref, av_free,
    av_malloc, AVFrameSideDataType, AVPixelFormat, AVRational,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_frame_pipeline_hardening_tests", name)
}

/// Offline 30fps blue-video fixture, `frames` frames long.
fn mpegts_fixture(name: &str, frames: i64) -> String {
    let path = tmp_path(name);
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_format("mpegts")
                    .set_video_codec("mpeg2video")
                    .set_max_video_frames(frames),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fixture",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");
    path
}

fn probe_video_frames(path: &str) -> i64 {
    match find_video_stream_info(path)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => nb_frames,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

/// Polls `cond` until it holds or `secs` elapse (then panics naming `what`).
fn wait_until(secs: u64, what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(
            Instant::now() < deadline,
            "timed out after {secs}s waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Passthrough tap that records what `filter_frame` is handed. Only reads the
/// raw pointer VALUE — it must stay observation-only so that, should the null
/// EOF sentinel ever be routed through user filters again, the regression
/// shows up as a failed assertion instead of a segfault.
struct RecordingFilter {
    frames_seen: Arc<AtomicUsize>,
    saw_null: Arc<AtomicBool>,
}

impl FrameFilter for RecordingFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if unsafe { frame.as_ptr().is_null() } {
            self.saw_null.store(true, Ordering::SeqCst);
            // Never dereference; hand it back untouched.
            return Ok(Some(frame));
        }
        self.frames_seen.fetch_add(1, Ordering::SeqCst);
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

fn recording_pipeline(
    frames_seen: &Arc<AtomicUsize>,
    saw_null: &Arc<AtomicBool>,
) -> FramePipelineBuilder {
    FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "recorder",
        Box::new(RecordingFilter {
            frames_seen: frames_seen.clone(),
            saw_null: saw_null.clone(),
        }),
    )
}

/// Passthrough that, once `armed`, turns into a saturating generator: every
/// `request_frame` poll yields a fresh reference-counted clone of the last
/// real frame with a strictly increasing pts, so the pipeline's drain loop
/// never sees `None` again.
struct SaturatingGenerator {
    armed: Arc<AtomicBool>,
    generated: Arc<AtomicUsize>,
    frames_seen: Arc<AtomicUsize>,
    stash: Option<Frame>,
    next_pts: i64,
    time_base: AVRational,
    duration: i64,
}

impl FrameFilter for SaturatingGenerator {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            if frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() {
                // EOF sentinel or props-only marker: nothing to stash.
                return Ok(Some(frame));
            }
        }

        // Keep a reference-counted copy of the newest real frame as the
        // template for generated output.
        let mut copy = unsafe { Frame::empty() };
        if unsafe { copy.as_ptr().is_null() } {
            return Err("av_frame_alloc failed".into());
        }
        let ret = unsafe { av_frame_ref(copy.as_mut_ptr(), frame.as_ptr()) };
        if ret < 0 {
            return Err(format!("av_frame_ref failed: {ret}").into());
        }
        unsafe {
            self.next_pts = (*frame.as_ptr()).pts + 1;
            self.time_base = (*frame.as_ptr()).time_base;
            self.duration = (*frame.as_ptr()).duration;
        }
        self.stash = Some(copy);
        self.frames_seen.fetch_add(1, Ordering::SeqCst);
        Ok(Some(frame))
    }

    fn request_frame(
        &mut self,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if !self.armed.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let Some(stash) = self.stash.as_ref() else {
            return Ok(None);
        };

        let mut clone = unsafe { Frame::empty() };
        if unsafe { clone.as_ptr().is_null() } {
            return Err("av_frame_alloc failed".into());
        }
        let ret = unsafe { av_frame_ref(clone.as_mut_ptr(), stash.as_ptr()) };
        if ret < 0 {
            return Err(format!("av_frame_ref failed: {ret}").into());
        }
        unsafe {
            (*clone.as_mut_ptr()).pts = self.next_pts;
            (*clone.as_mut_ptr()).time_base = self.time_base;
            (*clone.as_mut_ptr()).duration = self.duration;
        }
        self.next_pts += 1;
        self.generated.fetch_add(1, Ordering::SeqCst);
        Ok(Some(clone))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::MayProduce
    }
}

/// The null EOF sentinel must be short-circuited around user filters on both
/// the decoder-side and the encoder-side pipeline: `filter_frame` must only
/// ever be handed frames whose underlying `AVFrame` pointer is valid.
#[test]
fn eof_sentinel_never_reaches_user_frame_filters() {
    let fixture = mpegts_fixture("sentinel_in.ts", 30);
    let out = tmp_path("sentinel_out.mp4");

    let input_seen = Arc::new(AtomicUsize::new(0));
    let input_null = Arc::new(AtomicBool::new(false));
    let output_seen = Arc::new(AtomicUsize::new(0));
    let output_null = Arc::new(AtomicBool::new(false));

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from(fixture.as_str())
                    .add_frame_pipeline(recording_pipeline(&input_seen, &input_null)),
            )
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(recording_pipeline(&output_seen, &output_null)),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "sentinel transcode",
    );
    assert!(
        result.is_ok(),
        "transcode with recorders failed: {result:?}"
    );

    assert!(
        input_seen.load(Ordering::SeqCst) > 0,
        "decoder-side filter processed no frames"
    );
    assert!(
        output_seen.load(Ordering::SeqCst) > 0,
        "encoder-side filter processed no frames"
    );
    assert!(
        !input_null.load(Ordering::SeqCst),
        "decoder-side filter was handed a Frame wrapping a NULL AVFrame \
         (EOF sentinel leaked into user filter_frame)"
    );
    assert!(
        !output_null.load(Ordering::SeqCst),
        "encoder-side filter was handed a Frame wrapping a NULL AVFrame \
         (EOF sentinel leaked into user filter_frame)"
    );
}

/// Forwarding the sentinel around the filters must keep end-of-stream
/// draining intact: with a passthrough recorder attached on both sides, the
/// job must still finish and emit exactly as many frames as the same
/// transcode without any frame pipeline.
#[test]
fn recording_filter_keeps_output_frame_count() {
    let fixture = mpegts_fixture("drain_in.ts", 30);

    let baseline_out = tmp_path("drain_baseline.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline transcode produced no frames");

    let filtered_out = tmp_path("drain_filtered.mp4");
    let input_seen = Arc::new(AtomicUsize::new(0));
    let input_null = Arc::new(AtomicBool::new(false));
    let output_seen = Arc::new(AtomicUsize::new(0));
    let output_null = Arc::new(AtomicBool::new(false));

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from(fixture.as_str())
                    .add_frame_pipeline(recording_pipeline(&input_seen, &input_null)),
            )
            .output(
                Output::from(filtered_out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(recording_pipeline(&output_seen, &output_null)),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "filtered transcode",
    );
    assert!(result.is_ok(), "filtered transcode failed: {result:?}");

    let out_size = std::fs::metadata(&filtered_out)
        .expect("filtered output missing")
        .len();
    assert!(out_size > 0, "filtered output is empty");

    let filtered_frames = probe_video_frames(&filtered_out);
    assert_eq!(
        filtered_frames, baseline_frames,
        "a passthrough frame pipeline changed the output frame count \
         (EOF drain broken: expected {baseline_frames}, got {filtered_frames})"
    );
    assert!(
        input_seen.load(Ordering::SeqCst) > 0 && output_seen.load(Ordering::SeqCst) > 0,
        "recording filters were not exercised"
    );
}

/// Emulates the crate's async-filter contract (the GPU pipeline): frames are
/// held back in an internal queue and only released when a props-only flush
/// cue (valid AVFrame, no data buffers) arrives via `filter_frame`; the
/// backlog then drains through `request_frame`. Holds up to `depth` frames.
struct AsyncBufferingFilter {
    depth: usize,
    held: std::collections::VecDeque<Frame>,
    saw_flush_cue: Arc<AtomicBool>,
    released_after_flush: Arc<AtomicUsize>,
}

impl FrameFilter for AsyncBufferingFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if props_only {
            // Flush cue: everything held is now ready; the backlog drains
            // through request_frame. Pass the cue itself along untouched.
            self.saw_flush_cue.store(true, Ordering::SeqCst);
            return Ok(Some(frame));
        }
        self.held.push_back(frame);
        if self.held.len() > self.depth {
            return Ok(self.held.pop_front());
        }
        // Simulating in-flight work: nothing to emit for this input yet.
        Ok(None)
    }

    fn request_frame(
        &mut self,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if !self.saw_flush_cue.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let out = self.held.pop_front();
        if out.is_some() {
            self.released_after_flush.fetch_add(1, Ordering::SeqCst);
        }
        Ok(out)
    }
}

/// A `Never` filter that delays frames by one: holds the newest frame,
/// forwards the previous one, and releases the held frame synchronously from
/// `filter_frame` when the end-of-stream cue (props-only marker) arrives —
/// the exact contract the trait's "End of stream" section grants
/// non-producing filters.
struct DelayOneFilter {
    held: Option<Frame>,
    released_on_cue: Arc<AtomicBool>,
}

impl FrameFilter for DelayOneFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if props_only {
            // Flush cue: release the delayed frame (consuming the marker),
            // or pass the cue along if nothing is held.
            return match self.held.take() {
                Some(prev) => {
                    self.released_on_cue.store(true, Ordering::SeqCst);
                    Ok(Some(prev))
                }
                None => Ok(Some(frame)),
            };
        }
        Ok(self.held.replace(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// A `Never` filter is entitled to the end-of-stream cue too: it may hold a
/// frame and can only release it when the props-only marker traverses the
/// chain. Skipping the cue for non-producing pipelines would silently drop
/// its last frame.
#[test]
fn eof_cue_reaches_never_filters_holding_a_frame() {
    let fixture = mpegts_fixture("delay_in.ts", 30);

    let baseline_out = tmp_path("delay_baseline.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "delay baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline produced no frames");

    let released_on_cue = Arc::new(AtomicBool::new(false));
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "delay-one",
        Box::new(DelayOneFilter {
            held: None,
            released_on_cue: released_on_cue.clone(),
        }),
    );

    let delayed_out = tmp_path("delay_delayed.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(delayed_out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "delay transcode",
    );
    assert!(result.is_ok(), "delayed transcode failed: {result:?}");

    assert!(
        released_on_cue.load(Ordering::SeqCst),
        "the Never filter never received the end-of-stream cue"
    );
    let delayed_frames = probe_video_frames(&delayed_out);
    assert_eq!(
        delayed_frames, baseline_frames,
        "a one-frame-delay Never filter lost its held frame at EOF \
         (expected {baseline_frames}, got {delayed_frames})"
    );
}

/// An async filter's end-of-stream backlog must reach the encoder BEFORE the
/// EOF sentinel. The filtergraph sends no props-only marker to an output
/// pipeline when frames flowed, so the pipeline itself has to deliver the
/// flush cue and drain the producing filters before forwarding EOF —
/// otherwise the held frames are silently dropped and the output is short.
#[test]
fn eof_flush_releases_async_filter_backlog() {
    let fixture = mpegts_fixture("flush_in.ts", 30);

    let baseline_out = tmp_path("flush_baseline.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "flush baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline produced no frames");

    let saw_flush_cue = Arc::new(AtomicBool::new(false));
    let released_after_flush = Arc::new(AtomicUsize::new(0));
    let depth = 3usize;

    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "async-buffering",
        Box::new(AsyncBufferingFilter {
            depth,
            held: std::collections::VecDeque::new(),
            saw_flush_cue: saw_flush_cue.clone(),
            released_after_flush: released_after_flush.clone(),
        }),
    );

    let buffered_out = tmp_path("flush_buffered.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(buffered_out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "flush buffered transcode",
    );
    assert!(result.is_ok(), "buffered transcode failed: {result:?}");

    assert!(
        saw_flush_cue.load(Ordering::SeqCst),
        "the pipeline never delivered a props-only flush cue at end of stream"
    );
    assert!(
        released_after_flush.load(Ordering::SeqCst) >= depth,
        "the end-of-stream backlog was not drained through request_frame \
         (released {} of {depth} held frames)",
        released_after_flush.load(Ordering::SeqCst)
    );
    let buffered_frames = probe_video_frames(&buffered_out);
    assert_eq!(
        buffered_frames, baseline_frames,
        "an async filter's backlog was dropped at EOF \
         (expected {baseline_frames} frames, got {buffered_frames})"
    );
}

/// A holder filter is allowed to CONSUME the flush cue (returning its delayed
/// frame instead), which means filters after it in the chain never see that
/// cue. The pipeline must detect the consumed cue and still deliver a fresh
/// one to the rest of the chain at EOF — otherwise an async filter behind a
/// delay filter loses its entire backlog.
#[test]
fn consumed_cue_still_flushes_later_async_filters() {
    let fixture = mpegts_fixture("chain_in.ts", 30);

    let baseline_out = tmp_path("chain_baseline.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "chain baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline produced no frames");

    let released_on_cue = Arc::new(AtomicBool::new(false));
    let saw_flush_cue = Arc::new(AtomicBool::new(false));
    let released_after_flush = Arc::new(AtomicUsize::new(0));
    let depth = 3usize;

    // delay-one (may consume a cue) FOLLOWED BY the async holder: the
    // problematic order, since a consumed cue never reaches the holder.
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "delay-one",
            Box::new(DelayOneFilter {
                held: None,
                released_on_cue: released_on_cue.clone(),
            }),
        )
        .filter(
            "async-buffering",
            Box::new(AsyncBufferingFilter {
                depth,
                held: std::collections::VecDeque::new(),
                saw_flush_cue: saw_flush_cue.clone(),
                released_after_flush: released_after_flush.clone(),
            }),
        );

    let chained_out = tmp_path("chain_chained.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(chained_out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "chained transcode",
    );
    assert!(result.is_ok(), "chained transcode failed: {result:?}");

    assert!(
        saw_flush_cue.load(Ordering::SeqCst),
        "the async filter behind a cue-consuming delay filter never got a flush cue"
    );
    assert!(
        released_after_flush.load(Ordering::SeqCst) >= depth,
        "the async backlog behind a delay filter was not drained \
         (released {} of {depth})",
        released_after_flush.load(Ordering::SeqCst)
    );
    let chained_frames = probe_video_frames(&chained_out);
    assert_eq!(
        chained_frames, baseline_frames,
        "frames were lost at EOF behind a cue-consuming filter \
         (expected {baseline_frames}, got {chained_frames})"
    );
}

/// Passthrough sentinel recording the end-of-stream ordering contract: once
/// its own flush cue has arrived, no real frame may follow (the cascade must
/// have fully drained everything upstream first).
struct CueOrderSentinel {
    cue_seen: Arc<AtomicBool>,
    real_after_cue: Arc<AtomicBool>,
    frames_seen: Arc<AtomicUsize>,
}

impl FrameFilter for CueOrderSentinel {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if props_only {
            self.cue_seen.store(true, Ordering::SeqCst);
        } else {
            self.frames_seen.fetch_add(1, Ordering::SeqCst);
            if self.cue_seen.load(Ordering::SeqCst) {
                self.real_after_cue.store(true, Ordering::SeqCst);
            }
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// A filter downstream of an async holder must receive the upstream backlog
/// BEFORE its own flush cue: the EOF flush cascades in chain order, draining
/// each filter dry before cueing the next one.
#[test]
fn eof_cue_arrives_after_upstream_backlog_drained() {
    let fixture = mpegts_fixture("order_in.ts", 30);

    let saw_flush_cue = Arc::new(AtomicBool::new(false));
    let released_after_flush = Arc::new(AtomicUsize::new(0));
    let cue_seen = Arc::new(AtomicBool::new(false));
    let real_after_cue = Arc::new(AtomicBool::new(false));
    let sentinel_frames = Arc::new(AtomicUsize::new(0));
    let depth = 3usize;

    // async holder FIRST, sentinel SECOND: the sentinel's cue must come only
    // after the holder's backlog has drained through it.
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "async-buffering",
            Box::new(AsyncBufferingFilter {
                depth,
                held: std::collections::VecDeque::new(),
                saw_flush_cue: saw_flush_cue.clone(),
                released_after_flush: released_after_flush.clone(),
            }),
        )
        .filter(
            "cue-order-sentinel",
            Box::new(CueOrderSentinel {
                cue_seen: cue_seen.clone(),
                real_after_cue: real_after_cue.clone(),
                frames_seen: sentinel_frames.clone(),
            }),
        );

    let out = tmp_path("order_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "cue-order transcode",
    );
    assert!(result.is_ok(), "cue-order transcode failed: {result:?}");

    assert!(
        saw_flush_cue.load(Ordering::SeqCst),
        "the async holder never received its flush cue"
    );
    assert!(
        released_after_flush.load(Ordering::SeqCst) >= depth,
        "the async backlog was not drained"
    );
    assert!(
        cue_seen.load(Ordering::SeqCst),
        "the downstream sentinel never received its flush cue"
    );
    assert!(
        !real_after_cue.load(Ordering::SeqCst),
        "a real frame reached a filter AFTER its end-of-stream cue \
         (the flush cascade cued downstream before upstream drained)"
    );
    assert!(
        sentinel_frames.load(Ordering::SeqCst) > 0,
        "the sentinel processed no frames"
    );
}

/// Three delay holders in a row: each stage's cue releases one held frame
/// through the remaining holders, so the cascade must recover every frame —
/// the output may not be short by the chain's total delay depth.
#[test]
fn cascade_recovers_frames_from_stacked_never_holders() {
    let fixture = mpegts_fixture("stack_in.ts", 30);

    let baseline_out = tmp_path("stack_baseline.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "stack baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline produced no frames");

    let released = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ];
    let mut pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO);
    for (i, flag) in released.iter().enumerate() {
        pipeline = pipeline.filter(
            &format!("delay-{i}"),
            Box::new(DelayOneFilter {
                held: None,
                released_on_cue: flag.clone(),
            }),
        );
    }

    let stacked_out = tmp_path("stack_stacked.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(stacked_out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "stacked transcode",
    );
    assert!(result.is_ok(), "stacked transcode failed: {result:?}");

    for (i, flag) in released.iter().enumerate() {
        assert!(
            flag.load(Ordering::SeqCst),
            "delay filter {i} never released its held frame on the cue"
        );
    }
    let stacked_frames = probe_video_frames(&stacked_out);
    assert_eq!(
        stacked_frames, baseline_frames,
        "stacked delay holders lost frames at EOF \
         (expected {baseline_frames}, got {stacked_frames})"
    );
}

/// Holder that parks inside its EOF-cue callback until the test initiates
/// stop(), then returns its held frame — modeling a slow flush callback that
/// a stop overtakes mid-flight.
struct StopSignalingHolder {
    held: Option<Frame>,
    cue_entered: Option<std::sync::mpsc::Sender<()>>,
    stop_initiated: Arc<AtomicBool>,
}

impl FrameFilter for StopSignalingHolder {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if !props_only {
            return Ok(self.held.replace(frame));
        }
        if let Some(tx) = self.cue_entered.take() {
            let _ = tx.send(());
            // Wait for the test to start stop(), then give the stop ample
            // time to publish the terminal status before returning.
            let deadline = Instant::now() + Duration::from_secs(10);
            while !self.stop_initiated.load(Ordering::SeqCst) {
                assert!(
                    Instant::now() < deadline,
                    "test harness never initiated stop()"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            std::thread::sleep(Duration::from_millis(500));
            if let Some(prev) = self.held.take() {
                return Ok(Some(prev));
            }
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// Passthrough that records any invocation made after stop() was initiated.
struct PostStopRecorder {
    stop_initiated: Arc<AtomicBool>,
    calls_after_stop: Arc<AtomicUsize>,
}

impl FrameFilter for PostStopRecorder {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if self.stop_initiated.load(Ordering::SeqCst) {
            self.calls_after_stop.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// Same barrier, but the slow cue callback sits on the LAST filter and
/// returns the marker itself (echo) instead of a real frame: the flush must
/// still report the stop, so the source-delivered marker is not pushed
/// through the user chain again afterwards.
#[test]
fn stop_during_last_stage_marker_echo_blocks_source_marker_traversal() {
    let fixture = mpegts_fixture("stopecho_in.ts", 30);

    let (cue_tx, cue_rx) = std::sync::mpsc::channel();
    let stop_initiated = Arc::new(AtomicBool::new(false));
    let calls_after_stop = Arc::new(AtomicUsize::new(0));

    // Recorder FIRST, echoing holder LAST: after its stage nothing else runs
    // inside the flush, so only the completion verdict protects the chain
    // from the source marker's second traversal.
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "post-stop-recorder",
            Box::new(PostStopRecorder {
                stop_initiated: stop_initiated.clone(),
                calls_after_stop: calls_after_stop.clone(),
            }),
        )
        .filter(
            "stop-signaling-echo",
            Box::new(StopSignalingHolder {
                held: None,
                cue_entered: Some(cue_tx),
                stop_initiated: stop_initiated.clone(),
            }),
        );

    let out = tmp_path("stopecho_out.mp4");
    // Input-side pipeline: the decoder delivers a props-only EOF-timestamp
    // marker before the sentinel, which is exactly the source-marker path
    // under test.
    let scheduler = FfmpegContext::builder()
        .input(Input::from(fixture.as_str()).add_frame_pipeline(pipeline))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    cue_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the EOF flush never delivered a cue to the last filter");

    let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
    let flag = stop_initiated.clone();
    std::thread::spawn(move || {
        flag.store(true, Ordering::SeqCst);
        // Liveness tests: the stop() result is locked elsewhere
        // (lifecycle/log_noise); only the return itself matters here.
        let _ = scheduler.stop();
        let _ = stopped_tx.send(());
    });

    assert!(
        stopped_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
        "stop() did not return while a last-stage cue callback was in flight"
    );
    assert_eq!(
        calls_after_stop.load(Ordering::SeqCst),
        0,
        "the source marker traversed the chain after stop was observable \
         (a marker/None echo from the last stage masked the abort)"
    );
}

/// Once a stop is observable, the EOF flush must not START any new filter
/// call: a frame released by a slow cue callback that a stop overtook is
/// recycled at the gate instead of being routed into the filters behind it.
#[test]
fn stop_during_flush_does_not_start_new_filter_calls() {
    let fixture = mpegts_fixture("stopflush_in.ts", 30);

    let (cue_tx, cue_rx) = std::sync::mpsc::channel();
    let stop_initiated = Arc::new(AtomicBool::new(false));
    let calls_after_stop = Arc::new(AtomicUsize::new(0));

    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "stop-signaling-holder",
            Box::new(StopSignalingHolder {
                held: None,
                cue_entered: Some(cue_tx),
                stop_initiated: stop_initiated.clone(),
            }),
        )
        .filter(
            "post-stop-recorder",
            Box::new(PostStopRecorder {
                stop_initiated: stop_initiated.clone(),
                calls_after_stop: calls_after_stop.clone(),
            }),
        );

    let out = tmp_path("stopflush_out.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from(fixture.as_str()))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .add_frame_pipeline(pipeline),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // The holder is now parked inside its cue callback, waiting for us.
    cue_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the EOF flush never delivered a cue to the holder");

    let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
    let flag = stop_initiated.clone();
    std::thread::spawn(move || {
        flag.store(true, Ordering::SeqCst);
        // Liveness tests: the stop() result is locked elsewhere
        // (lifecycle/log_noise); only the return itself matters here.
        let _ = scheduler.stop();
        let _ = stopped_tx.send(());
    });

    assert!(
        stopped_rx.recv_timeout(Duration::from_secs(30)).is_ok(),
        "stop() did not return while a flush-cue callback was in flight"
    );
    assert_eq!(
        calls_after_stop.load(Ordering::SeqCst),
        0,
        "the flush routed a frame into a downstream filter AFTER stop was observable"
    );
}

/// A generator filter that always has a frame available must not be able to
/// hang stop(): the request_frame drain loop has to notice the stop signal
/// (and the emptied sender list) on every iteration instead of only between
/// input frames.
#[test]
fn stop_releases_saturating_generator_drain_loop() {
    let out = tmp_path("generator_stop.mp4");

    let armed = Arc::new(AtomicBool::new(false));
    let generated = Arc::new(AtomicUsize::new(0));
    let frames_seen = Arc::new(AtomicUsize::new(0));

    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "saturating-generator",
        Box::new(SaturatingGenerator {
            armed: armed.clone(),
            generated: generated.clone(),
            frames_seen: frames_seen.clone(),
            stash: None,
            next_pts: 0,
            time_base: AVRational { num: 0, den: 1 },
            duration: 0,
        }),
    );

    // Unbounded lavfi source: the job runs until stop() ends it.
    let scheduler = FfmpegContext::builder()
        .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .add_frame_pipeline(pipeline),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // Let real frames flow first so the generator has a template to clone.
    wait_until(10, "frames to reach the pipeline filter", || {
        frames_seen.load(Ordering::SeqCst) >= 5
    });

    armed.store(true, Ordering::SeqCst);

    // The drain loop is now saturated: request_frame keeps yielding frames.
    wait_until(10, "the armed generator to be polled", || {
        generated.load(Ordering::SeqCst) >= 3
    });

    assert!(
        !scheduler.is_ended(),
        "job ended on its own before stop(); the saturated drain loop was never \
         the thing stop() had to interrupt"
    );

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // Liveness tests: the stop() result is locked elsewhere
        // (lifecycle/log_noise); only the return itself matters here.
        let _ = scheduler.stop();
        let _ = tx.send(());
    });

    // Generous bound: stop() joins every worker thread, so a spinning drain
    // loop that ignores the stop signal turns this into a timeout.
    assert!(
        rx.recv_timeout(Duration::from_secs(15)).is_ok(),
        "stop() did not return within 15s: the request_frame drain loop is not \
         honoring stop/abort while a generator filter keeps producing"
    );
}

/// Panics on the Nth REAL frame (props-only cues are passed through, not
/// counted) — the worker-panic surfacing probe.
struct PanicOnNthFrame {
    seen: usize,
    panic_at: usize,
}

impl FrameFilter for PanicOnNthFrame {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if !props_only {
            self.seen += 1;
            if self.seen >= self.panic_at {
                panic!("test-injected filter panic on frame {}", self.seen);
            }
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// A panicking FrameFilter used to be swallowed: the pipeline thread died,
/// downstream read the disconnect as EOF, and wait() returned Ok(()) over a
/// truncated output. It must now surface as WorkerPanicked — and the job
/// must still wind down within a bound (the panic teardown cannot hang).
#[test]
fn filter_panic_surfaces_as_worker_panicked() {
    let fixture = mpegts_fixture("panic_in.ts", 30);
    let out = tmp_path("panic_out.mp4");

    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "panic-on-nth",
        Box::new(PanicOnNthFrame {
            seen: 0,
            panic_at: 5,
        }),
    );

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "panicking filter job",
    );

    match result {
        Err(ez_ffmpeg::error::Error::WorkerPanicked(name)) => {
            assert!(
                name.contains("pipeline"),
                "the recorded thread should be the pipeline worker, got '{name}'"
            );
        }
        other => panic!("expected Err(WorkerPanicked), got {other:?}"),
    }
}

/// A FrameFilter whose `Drop` panics (distinct from a `filter_frame` panic: the
/// Drop runs at pipeline TEARDOWN, not during processing).
struct PanicOnDropFilter;

impl FrameFilter for PanicOnDropFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

impl Drop for PanicOnDropFilter {
    fn drop(&mut self) {
        panic!("test-injected FrameFilter Drop panic");
    }
}

/// a FrameFilter's `Drop` panic used to be LOST — the pipeline (which owns
/// the user filters) is a closure CAPTURE and dropped AFTER the ThreadDoneGuard
/// (a body local) had already released the thread slot, so the Drop panic went
/// unrecorded and wait() returned Ok(()) over a failed job. With the pipeline now
/// rebound as a body local that drops BEFORE the guard, the Drop panic surfaces as
/// WorkerPanicked, exactly like a filter_frame panic.
#[test]
fn filter_drop_panic_surfaces_as_worker_panicked() {
    let fixture = mpegts_fixture("drop_panic_in.ts", 30);
    let out = tmp_path("drop_panic_out.mp4");

    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter("panic-on-drop", Box::new(PanicOnDropFilter));

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "drop-panicking filter job",
    );

    match result {
        Err(ez_ffmpeg::error::Error::WorkerPanicked(name)) => {
            assert!(
                name.contains("pipeline"),
                "the recorded thread should be the pipeline worker, got '{name}'"
            );
        }
        other => panic!("expected Err(WorkerPanicked) from the Drop panic, got {other:?}"),
    }
}

/// Shared state for the blocking side-data free callback below.
struct BlockingFreeState {
    entered: AtomicBool,
    release: AtomicBool,
}

/// An `AVBufferRef` free callback (invoked by libav's C teardown when the frame
/// is unref'd) that announces it started, then PARKS until the test releases it.
/// It must never panic — it runs across the `extern "C"` boundary.
unsafe extern "C" fn blocking_side_data_free(opaque: *mut std::ffi::c_void, data: *mut u8) {
    // Reclaim the Arc ref leaked into `opaque` (balances the Arc::into_raw in the
    // injector); it drops at the end of this fn.
    let state = Arc::from_raw(opaque as *const BlockingFreeState);
    state.entered.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !state.release.load(Ordering::SeqCst) {
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    if !data.is_null() {
        av_free(data as *mut std::ffi::c_void);
    }
}

/// Attaches a side-data buffer to `frame` whose free callback is `blocking_side_data_free`
/// bound to `state`. When the frame is later unref'd, the callback parks until the test
/// releases `state` — a controllable proxy for a real hwaccel surface / custom-IO buffer
/// whose free callback blocks.
unsafe fn attach_blocking_side_data(frame: &Frame, state: &Arc<BlockingFreeState>) {
    let data = av_malloc(64) as *mut u8;
    assert!(!data.is_null(), "av_malloc failed in test");
    // Leak one Arc ref into the buffer's opaque; the callback reclaims it.
    let raw = Arc::into_raw(state.clone()) as *mut std::ffi::c_void;
    let buf = av_buffer_create(data, 64, Some(blocking_side_data_free), raw, 0);
    assert!(!buf.is_null(), "av_buffer_create failed in test");
    let sd = av_frame_new_side_data_from_buf(
        frame.as_ptr() as *mut _,
        AVFrameSideDataType::AV_FRAME_DATA_SEI_UNREGISTERED,
        buf,
    );
    assert!(
        !sd.is_null(),
        "av_frame_new_side_data_from_buf failed in test"
    );
}

/// Tags the first `states.len()` real frames with a blocking free callback (one
/// `state` each) and FORWARDS the first `forward_until` real frames, then swallows the
/// rest. It fires `barrier` once real frame `forward_until + 1` arrives — proof the
/// pipeline already returned from `send_frame` for frames 1..=`forward_until` (the
/// pipeline calls `filter_frame(N+1)` only AFTER `send_frame(N)` returned), a genuine
/// happens-before rather than a fixed sleep. Forwarding MORE frames than the
/// downstream `bounded(8)` channel holds forces the receiver to dequeue the earliest
/// (tagged) frame, so it provably lands in the target worker's own queue, not merely
/// the shared channel buffer (which a `crossbeam` bounded channel frees only when its
/// LAST endpoint drops).
struct BlockingBufferInjector {
    states: Vec<Arc<BlockingFreeState>>,
    real_seen: usize,
    forward_until: usize,
    barrier: Option<std::sync::mpsc::Sender<()>>,
}

impl FrameFilter for BlockingBufferInjector {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if props_only {
            return Ok(Some(frame));
        }
        self.real_seen += 1;
        if self.real_seen <= self.states.len() {
            unsafe { attach_blocking_side_data(&frame, &self.states[self.real_seen - 1]) };
            return Ok(Some(frame));
        }
        if self.real_seen <= self.forward_until {
            return Ok(Some(frame));
        }
        if self.real_seen == self.forward_until + 1 {
            if let Some(tx) = self.barrier.take() {
                let _ = tx.send(());
            }
        }
        Ok(None)
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// Swallows every real frame so this graph input never delivers a format, keeping
/// the filtergraph forever in its pre-config (buffering) state.
struct SwallowAllFrames;

impl FrameFilter for SwallowAllFrames {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if props_only {
            return Ok(Some(frame));
        }
        Ok(None)
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// the filtergraph worker's frame-owning resources (an `ifps[i].frame_queue`
/// pre-config backlog frame, an unconsumed `src` frame, an `ofps` held frame, a
/// pooled frame) are `move`-closure CAPTURES. They must be torn down BEFORE the
/// ThreadDoneGuard releases the thread slot — which zeroes the sync counter that
/// stop()/wait()/the async Future gate on — otherwise a queued frame whose
/// `AVBufferRef` free callback BLOCKS runs its teardown AFTER the caller already
/// observed completion, violating counter==0 => teardown complete.
///
/// Construction: a two-input overlay graph whose second input delivers NOTHING (its
/// pipeline swallows every frame), so the graph never configures and input 0's tagged
/// frames — each carrying a side-data buffer whose free callback blocks — accumulate in
/// the worker's pre-config path. The injector FORWARDS 10 frames (tagging the first two)
/// and fires its barrier on FRAME 11. Because the input->filtergraph channel is
/// `bounded(8)`, 10 accepted sends prove the filtergraph DEQUEUED at least two
/// frames, and since the worker pushes each frame into `ifps` before recv'ing the next,
/// frame 1 (the first dequeued) is PROVABLY in the worker's OWN `ifps` queue (a rebound
/// capture), not merely the shared channel buffer (which a `crossbeam` bounded channel
/// frees only when its last endpoint drops).
///
/// Frame 2 at barrier time may be in one of three states: (a) already pushed to `ifps`;
/// (b) the worker's `frame_box` local, past the stop check but not yet pushed; or (c)
/// still the recv'd `result` local, before the stop check — where a stop breaks the loop
/// and frame 2 drops from `result` WITHOUT ever entering `ifps`. The proof does not rely
/// on which: in all three, frame 2 drops BEFORE the guard — states (a) and (b) end up in
/// the rebound `ifps` ((b)'s `frame_box` is moved into `fg_send_frame`, which pushes it
/// into `ifps`, or drops it if that call returns/unwinds), while (c)'s `result` local
/// drops at the stop `break`. The barrier is also a genuine happens-before (the pipeline
/// calls `filter_frame(11)` only after `send_frame(10)` returned), not a fixed sleep.
///
/// The two callbacks park sequentially on the single worker (the first blocks teardown
/// until released). We release the first-parked, then assert with the still-parked SECOND
/// that stop() has not returned. That second callback is always a frame that drops before
/// the guard: in state (c) frame 2's local callback parks first and, once released, frame
/// 1's `ifps` capture callback parks second; otherwise both tagged frames are in `ifps`
/// and drain in order. With the captures rebound as body locals (the fix) the guard is
/// downstream of the frame teardown, so stop() stays blocked; before the fix the guard
/// released
/// first, so stop() returned while the callback was still parked.
#[test]
fn filtergraph_worker_drops_captured_frames_before_releasing_its_slot() {
    let make_state = || {
        Arc::new(BlockingFreeState {
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
        })
    };
    let s1 = make_state();
    let s2 = make_state();
    let (barrier_tx, barrier_rx) = std::sync::mpsc::channel();

    let input0 = Input::from("color=c=red:s=320x240:r=30")
        .set_format("lavfi")
        .add_frame_pipeline(
            FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
                "blocking-injector",
                Box::new(BlockingBufferInjector {
                    states: vec![s1.clone(), s2.clone()],
                    real_seen: 0,
                    forward_until: 10,
                    barrier: Some(barrier_tx),
                }),
            ),
        );
    let input1 = Input::from("color=c=blue:s=320x240:r=30")
        .set_format("lavfi")
        .add_frame_pipeline(
            FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
                .filter("swallow-all", Box::new(SwallowAllFrames)),
        );

    let out = tmp_path("fg_worker_teardown_out.mp4");
    let scheduler = FfmpegContext::builder()
        .input(input0)
        .input(input1)
        .filter_desc("[0:v][1:v]overlay")
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    // Happens-before: the barrier fires on FRAME 11, i.e. after the pipeline returned
    // from send_frame for the first 10 frames. Ten accepted sends through the
    // input->filtergraph bounded(8) channel prove the filtergraph dequeued at least two,
    // so the earliest tagged frame is provably in its own pre-config queue.
    barrier_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("input 0 never had 10 frames accepted by the filtergraph");

    let stop_returned = Arc::new(AtomicBool::new(false));
    let sr = stop_returned.clone();
    std::thread::spawn(move || {
        // Liveness test: the stop() result value is asserted elsewhere; only the
        // return itself matters here.
        let _ = scheduler.stop();
        sr.store(true, Ordering::SeqCst);
    });

    // The callbacks run sequentially on this single worker, so exactly one parks
    // first; which one depends on drop order, so stay agnostic.
    wait_until(
        15,
        "the filtergraph teardown to reach a buffered frame",
        || s1.entered.load(Ordering::SeqCst) || s2.entered.load(Ordering::SeqCst),
    );
    let (first, second) = if s1.entered.load(Ordering::SeqCst) {
        (&s1, &s2)
    } else {
        (&s2, &s1)
    };
    // Release the first-parked callback so teardown advances to the second frame.
    first.release.store(true, Ordering::SeqCst);
    wait_until(15, "teardown to reach the second buffered frame", || {
        second.entered.load(Ordering::SeqCst)
    });

    // The second callback is now parked on an unambiguous capture frame. Give stop()
    // ample time to return IF it is going to: before the fix the guard released the
    // slot before the captured frames dropped, so stop() returns despite the parked
    // callback; with the fix the guard is downstream, so stop() stays blocked.
    std::thread::sleep(Duration::from_secs(2));
    let returned_while_blocked = stop_returned.load(Ordering::SeqCst);
    // Release unconditionally so nothing lingers, even on failure.
    second.release.store(true, Ordering::SeqCst);
    assert!(
        !returned_while_blocked,
        "stop() returned while a buffered frame's free callback was still parked: \
         the filtergraph worker released its thread slot before tearing down its \
         captured frames (counter==0 must imply teardown complete)"
    );

    // Once released, stop() finishes promptly.
    wait_until(15, "stop() to return after the callbacks release", || {
        stop_returned.load(Ordering::SeqCst)
    });
}

/// A FrameFilter whose `init()` parks until the test releases it, then FAILS —
/// modeling an initialization that errors only after upstream frames have already
/// accumulated in this pipeline's receiver.
struct FailInitOnCue {
    proceed: Arc<AtomicBool>,
}

impl FrameFilter for FailInitOnCue {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn init(&mut self, _ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.proceed.load(Ordering::SeqCst) {
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Err("test-injected init failure".into())
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // Never reached: init() fails, so the pipeline never enters run_pipeline.
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// Upstream stage for the init-error test: tags the first real frame with a blocking
/// free callback and forwards EVERY frame. Its `Drop` fires `dropped` — and because a
/// pipeline worker drops its output senders (inside `run_pipeline`) BEFORE it drops the
/// filter, receiving that signal proves this pipeline's sender endpoint is already gone,
/// so the tagged frame downstream is owned SOLELY by the downstream receiver.
struct AttachFirstDropSignal {
    state: Arc<BlockingFreeState>,
    attached: bool,
    dropped: Option<std::sync::mpsc::Sender<()>>,
}

impl FrameFilter for AttachFirstDropSignal {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe { frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() };
        if !props_only && !self.attached {
            self.attached = true;
            unsafe { attach_blocking_side_data(&frame, &self.state) };
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

impl Drop for AttachFirstDropSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.dropped.take() {
            let _ = tx.send(());
        }
    }
}

/// the FramePipeline worker's `frame_receiver`/`frame_senders`/`frame_pool` are
/// only MOVED into `run_pipeline` on the SUCCESS path. If `frame_filter_init` returns
/// Err (or panics), the early return dropped them AFTER the ThreadDoneGuard (they are
/// closure captures), so a frame already buffered in `frame_receiver` carrying a
/// blocking `AVBufferRef` free callback ran its teardown after the caller observed
/// completion — the same teardown-drop-order class, on the init-error path.
///
/// Construction: two chained pipelines on one input (`decoder -> upstream -> downstream`,
/// since a later `add_frame_pipeline` splices in closer to the decoder). The upstream
/// pipeline tags the first frame with a blocking free callback and forwards it. The input
/// is a BOUNDED fixture, so the upstream reaches EOF, ends, and DROPS its output sender
/// (inside `run_pipeline`) then its filter — whose `Drop` fires a signal. Receiving that
/// signal proves the upstream sender endpoint is already gone, so the tagged
/// frame sitting in the downstream pipeline's receiver is owned SOLELY by that receiver
/// (a `crossbeam` bounded channel frees a queued item only when its LAST endpoint drops —
/// otherwise the frame could be torn down under the UPSTREAM's guard, masking the bug).
/// The downstream pipeline's `init()` is parked, so it never drains its receiver — the
/// tagged frame is pinned there. Releasing `init()` makes it FAIL, taking the init-error
/// path. With the captures rebound (the fix) the receiver drops (running the blocking
/// callback) before the guard, so `wait()` stays blocked until we release; before the fix
/// the guard released first and `wait()` could return while the callback was still parked.
#[test]
fn pipeline_init_error_drops_receiver_frames_before_releasing_its_slot() {
    let fixture = mpegts_fixture("init_err_src.ts", 3);
    let blocking = Arc::new(BlockingFreeState {
        entered: AtomicBool::new(false),
        release: AtomicBool::new(false),
    });
    let proceed = Arc::new(AtomicBool::new(false));
    let (drop_tx, drop_rx) = std::sync::mpsc::channel();

    // Added FIRST -> spliced downstream (closer to the filtergraph): the failing init.
    let downstream = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "fail-init-on-cue",
        Box::new(FailInitOnCue {
            proceed: proceed.clone(),
        }),
    );
    // Added SECOND -> spliced upstream (closer to the decoder): tags + forwards, and
    // signals on Drop once its sender endpoint is gone.
    let upstream = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "attach-first-drop-signal",
        Box::new(AttachFirstDropSignal {
            state: blocking.clone(),
            attached: false,
            dropped: Some(drop_tx),
        }),
    );

    let out = tmp_path("pipeline_init_error_out.mp4");
    let scheduler = FfmpegContext::builder()
        .input(
            Input::from(fixture.as_str())
                .add_frame_pipeline(downstream)
                .add_frame_pipeline(upstream),
        )
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    let wait_returned = Arc::new(AtomicBool::new(false));
    let wr = wait_returned.clone();
    std::thread::spawn(move || {
        // Liveness test: the job ends in the injected init error; only the RETURN
        // timing matters here.
        let _ = scheduler.wait();
        wr.store(true, Ordering::SeqCst);
    });

    // The upstream pipeline has finished and DROPPED its sender: the tagged frame in the
    // downstream receiver is now solely owned by that (parked) receiver.
    drop_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("upstream pipeline never finished / dropped its sender");

    // Release init() -> it FAILS -> the downstream worker takes the init-error path.
    proceed.store(true, Ordering::SeqCst);

    // Teardown must reach (and park in) the receiver frame's blocking free callback.
    wait_until(
        15,
        "the init-error teardown to drop the receiver frame",
        || blocking.entered.load(Ordering::SeqCst),
    );

    // While the callback is parked, the worker's slot must NOT be released, so wait()
    // must not have returned. Give it ample time to return IF it is going to.
    std::thread::sleep(Duration::from_secs(2));
    let returned_while_blocked = wait_returned.load(Ordering::SeqCst);
    blocking.release.store(true, Ordering::SeqCst);
    assert!(
        !returned_while_blocked,
        "wait() returned while a frame buffered in a pipeline's receiver was still \
         being torn down (its blocking free callback parked): the pipeline worker \
         released its thread slot before draining its receiver on the init-error path"
    );

    // Once released, wait() finishes promptly.
    wait_until(15, "wait() to return after the callback releases", || {
        wait_returned.load(Ordering::SeqCst)
    });
}

/// Saturates only from its own flush cue onward: `request_frame` yields
/// nothing until `filter_frame` sees a props-only marker, then clones its
/// stashed template forever. This confines the saturation to the ordered
/// EOF flush drain (the regular poll sweep sees `None` all stream long), so
/// the per-filter flush cap is guaranteed to trip at natural end of stream.
struct CueArmedSaturatingGenerator {
    generated: Arc<AtomicUsize>,
    armed: bool,
    /// When armed, answer a LATER props-only marker (the decoder's source
    /// EOF-timestamp marker traversing the chain after the flush) with a
    /// stashed real frame instead of passing it through — the documented
    /// holder behavior of consuming a marker to release held output. A
    /// flush-capped filter doing this must NOT be able to leak that frame
    /// to already-cued downstream filters.
    convert_markers: bool,
    stash: Option<Frame>,
    next_pts: i64,
    time_base: AVRational,
    duration: i64,
}

impl FrameFilter for CueArmedSaturatingGenerator {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            if frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null() {
                if self.armed && self.convert_markers {
                    if let Some(stash) = self.stash.as_ref() {
                        // Consume the marker, release a held real frame
                        // (legitimate holder behavior per the trait docs).
                        let mut clone = Frame::empty();
                        if !clone.as_ptr().is_null()
                            && av_frame_ref(clone.as_mut_ptr(), stash.as_ptr()) >= 0
                        {
                            (*clone.as_mut_ptr()).pts = self.next_pts;
                            (*clone.as_mut_ptr()).time_base = self.time_base;
                            (*clone.as_mut_ptr()).duration = self.duration;
                            self.next_pts += 1;
                            self.generated.fetch_add(1, Ordering::SeqCst);
                            return Ok(Some(clone));
                        }
                    }
                }
                // Own flush cue (or EOF sentinel): saturate from here on.
                self.armed = true;
                return Ok(Some(frame));
            }
        }
        let mut copy = unsafe { Frame::empty() };
        if unsafe { copy.as_ptr().is_null() } {
            return Err("av_frame_alloc failed".into());
        }
        let ret = unsafe { av_frame_ref(copy.as_mut_ptr(), frame.as_ptr()) };
        if ret < 0 {
            return Err(format!("av_frame_ref failed: {ret}").into());
        }
        unsafe {
            self.next_pts = (*frame.as_ptr()).pts + 1;
            self.time_base = (*frame.as_ptr()).time_base;
            self.duration = (*frame.as_ptr()).duration;
        }
        self.stash = Some(copy);
        Ok(Some(frame))
    }

    fn request_frame(
        &mut self,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if !self.armed {
            return Ok(None);
        }
        let Some(stash) = self.stash.as_ref() else {
            return Ok(None);
        };
        let mut clone = unsafe { Frame::empty() };
        if unsafe { clone.as_ptr().is_null() } {
            return Err("av_frame_alloc failed".into());
        }
        let ret = unsafe { av_frame_ref(clone.as_mut_ptr(), stash.as_ptr()) };
        if ret < 0 {
            return Err(format!("av_frame_ref failed: {ret}").into());
        }
        unsafe {
            (*clone.as_mut_ptr()).pts = self.next_pts;
            (*clone.as_mut_ptr()).time_base = self.time_base;
            (*clone.as_mut_ptr()).duration = self.duration;
        }
        self.next_pts += 1;
        self.generated.fetch_add(1, Ordering::SeqCst);
        Ok(Some(clone))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::MayProduce
    }
}

/// A saturating generator must not leak frames past the ordered EOF flush:
/// once the flush drain hits the per-filter cap, the regular poll sweep has
/// to stop pulling from that filter — its next output would reach a
/// downstream filter that already consumed its flush cue, violating the
/// "no real frame after your cue" contract (and, on the null-sentinel path,
/// trail EOF downstream). The generator here saturates from its own flush
/// cue, so the cap trips at natural end of stream while the downstream
/// sentinel records ordering.
#[test]
fn capped_eof_flush_keeps_real_frames_away_from_cued_filters() {
    let fixture = mpegts_fixture("cap_order_in.ts", 5);
    let out = tmp_path("cap_order_out.mp4");

    let generated = Arc::new(AtomicUsize::new(0));
    let cue_seen = Arc::new(AtomicBool::new(false));
    let real_after_cue = Arc::new(AtomicBool::new(false));
    let sentinel_frames = Arc::new(AtomicUsize::new(0));

    // Generator FIRST, ordering sentinel SECOND: the flush cues the
    // generator, drains it into the cap, then cues the sentinel — any
    // generator frame reaching the sentinel afterwards is the violation.
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "cue-armed-generator",
            Box::new(CueArmedSaturatingGenerator {
                generated: generated.clone(),
                armed: false,
                convert_markers: false,
                stash: None,
                next_pts: 0,
                time_base: AVRational { num: 0, den: 1 },
                duration: 0,
            }),
        )
        .filter(
            "cue-order-sentinel",
            Box::new(CueOrderSentinel {
                cue_seen: cue_seen.clone(),
                real_after_cue: real_after_cue.clone(),
                frames_seen: sentinel_frames.clone(),
            }),
        );

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_frame_pipeline(pipeline),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "saturated EOF flush transcode",
    );
    assert!(result.is_ok(), "transcode failed: {result:?}");

    assert!(
        cue_seen.load(Ordering::SeqCst),
        "the downstream sentinel never received its flush cue"
    );
    assert!(
        generated.load(Ordering::SeqCst) >= 1024,
        "the generator never saturated the EOF flush cap (generated {}), \
         the scenario no longer exercises the capped path",
        generated.load(Ordering::SeqCst)
    );
    assert!(
        !real_after_cue.load(Ordering::SeqCst),
        "a capped generator's frame reached a filter AFTER its end-of-stream \
         cue: the poll sweep kept pulling from a filter whose EOF flush hit \
         the cap"
    );
}

/// Rewrites every real video frame into a NON-refcounted equivalent: a fresh
/// AVFrame whose yuv420p planes are carved out of one `av_malloc`'d slab
/// referenced through `data[]` only (`buf[0]` stays null), pixel bytes copied
/// verbatim and props (pts/time_base/duration/...) cloned. This is the frame
/// shape a user `FrameFilter` may legally emit. Props-only cues and the null
/// EOF shell pass through untouched.
///
/// The slab is deliberately leaked: the scheduler only ever unrefs the shell
/// (`av_frame_unref` never frees bare `data[]` pointers), nothing downstream
/// takes ownership of the slab, and freeing it from the filter would race the
/// in-flight frame. Bounded by the fixture length (30 frames of 320x240
/// yuv420p, ~3.5 MiB per test), reclaimed at process exit.
struct NonRefcountedRewriter {
    rewritten: Arc<AtomicUsize>,
}

impl FrameFilter for NonRefcountedRewriter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            let src = frame.as_ptr();
            let props_only = src.is_null()
                || ((*src).buf[0].is_null() && (*src).data.iter().all(|d| d.is_null()));
            if props_only {
                return Ok(Some(frame));
            }
            // The plane carving below is yuv420p-specific; both pipelines in
            // these tests carry yuv420p (mpeg2video decode, mpeg4 encode), so
            // anything else is a broken test setup, not a skippable frame.
            if (*src).format != AVPixelFormat::AV_PIX_FMT_YUV420P as i32 {
                return Err(
                    format!("test expects yuv420p frames, got format {}", (*src).format).into(),
                );
            }
            let h = (*src).height as usize;
            // Full-res luma plane + two half-res chroma planes, kept at the
            // source's own (possibly padded) linesizes so each plane is one copy.
            let ls = [
                (*src).linesize[0] as usize,
                (*src).linesize[1] as usize,
                (*src).linesize[2] as usize,
            ];
            let ph = [h, h.div_ceil(2), h.div_ceil(2)];
            let total = ls[0] * ph[0] + ls[1] * ph[1] + ls[2] * ph[2];
            let slab = av_malloc(total) as *mut u8;
            if slab.is_null() {
                return Err("av_malloc failed".into());
            }

            let mut out = Frame::empty();
            if out.as_ptr().is_null() {
                av_free(slab as *mut std::ffi::c_void);
                return Err("av_frame_alloc failed".into());
            }
            let dst = out.as_mut_ptr();
            let ret = av_frame_copy_props(dst, src);
            if ret < 0 {
                av_free(slab as *mut std::ffi::c_void);
                return Err(format!("av_frame_copy_props failed: {ret}").into());
            }
            (*dst).format = (*src).format;
            (*dst).width = (*src).width;
            (*dst).height = (*src).height;
            let mut offset = 0usize;
            for p in 0..3 {
                (*dst).data[p] = slab.add(offset);
                (*dst).linesize[p] = ls[p] as i32;
                std::ptr::copy_nonoverlapping((*src).data[p], slab.add(offset), ls[p] * ph[p]);
                offset += ls[p] * ph[p];
            }
            assert!(
                (*dst).buf[0].is_null(),
                "the rewritten frame must stay non-refcounted"
            );
            self.rewritten.fetch_add(1, Ordering::SeqCst);
            Ok(Some(out))
        }
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

/// Runs the baseline (no pipeline) and the rewriter transcode for one of the
/// two placements and returns `(baseline_frames, filtered_frames, rewritten)`.
fn transcode_with_rewriter(tag: &str, on_output: bool) -> (i64, i64, usize) {
    let fixture = mpegts_fixture(&format!("nonref_{tag}_in.ts"), 30);

    let baseline_out = tmp_path(&format!("nonref_{tag}_baseline.mp4"));
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(baseline_out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "non-refcounted baseline transcode",
    );
    assert!(result.is_ok(), "baseline transcode failed: {result:?}");
    let baseline_frames = probe_video_frames(&baseline_out);
    assert!(baseline_frames > 0, "baseline produced no frames");

    let rewritten = Arc::new(AtomicUsize::new(0));
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "non-refcounted-rewriter",
        Box::new(NonRefcountedRewriter {
            rewritten: rewritten.clone(),
        }),
    );

    let filtered_out = tmp_path(&format!("nonref_{tag}_filtered.mp4"));
    let mut input = Input::from(fixture.as_str());
    let mut output = Output::from(filtered_out.as_str()).set_video_codec("mpeg4");
    if on_output {
        output = output.add_frame_pipeline(pipeline);
    } else {
        input = input.add_frame_pipeline(pipeline);
    }
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(input)
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "non-refcounted rewriter transcode",
    );
    assert!(result.is_ok(), "rewriter transcode failed: {result:?}");

    (
        baseline_frames,
        probe_video_frames(&filtered_out),
        rewritten.load(Ordering::SeqCst),
    )
}

/// An OUTPUT pipeline always has exactly one sender (the encoder), so
/// `send_frame`'s last-sender branch forwards the filter's ORIGINAL frame —
/// non-refcounted and unnormalized — straight to the encoder task. The
/// encoder's post-open dummy probe used to test `buf[0]` alone, misreading
/// the first such frame as the parameters-only init dummy from close_output
/// and silently dropping it (output short by one frame). The probe must
/// treat only a props-only frame (no buffers AND no data planes) as the
/// dummy; avcodec_send_frame itself is safe with non-refcounted input (it
/// refs internally, copying when the source is not refcounted).
#[test]
fn non_refcounted_filter_frame_is_not_dropped_as_the_encoder_init_dummy() {
    let (baseline_frames, filtered_frames, rewritten) = transcode_with_rewriter("enc", true);
    assert!(
        rewritten > 0,
        "the rewriter never produced a non-refcounted frame; the scenario \
         no longer exercises the encoder-side probe"
    );
    assert_eq!(
        filtered_frames, baseline_frames,
        "a non-refcounted frame from an output-pipeline filter was dropped \
         as the encoder init dummy (expected {baseline_frames}, got \
         {filtered_frames})"
    );
}

/// The same frame shape on an INPUT pipeline feeds the filtergraph gate,
/// which used to test `buf[0]` alone: the first non-refcounted real frame
/// was routed into fg_send_eof — its pixels lost AND `ifp.eof` latched,
/// permanently closing that filtergraph input (silent stream truncation).
/// The gate must classify props-only frames as EOF cues and everything else
/// as real; av_buffersrc_add_frame_flags is safe with non-refcounted input
/// (without KEEP_REF it clones, allocating owned buffers and copying).
#[test]
fn non_refcounted_filter_frame_does_not_truncate_the_filtergraph_input() {
    let (baseline_frames, filtered_frames, rewritten) = transcode_with_rewriter("fg", false);
    assert!(
        rewritten > 0,
        "the rewriter never produced a non-refcounted frame; the scenario \
         no longer exercises the filtergraph-side gate"
    );
    assert_eq!(
        filtered_frames, baseline_frames,
        "a non-refcounted frame from an input-pipeline filter truncated the \
         filtergraph input (expected {baseline_frames}, got {filtered_frames})"
    );
}

/// The source EOF-timestamp marker (input-side pipelines: the decoder sends
/// it right before the null sentinel) traverses the chain AFTER the ordered
/// flush. A filter whose flush drain hit the per-filter cap already consumed
/// its cue and had its backlog discarded — the marker traversal must skip
/// it: by the documented holder semantics a filter may answer a marker with
/// a held REAL frame, which would otherwise land on downstream filters that
/// already consumed their own cue.
#[test]
fn capped_filter_cannot_convert_the_source_marker_into_a_late_real_frame() {
    let fixture = mpegts_fixture("cap_marker_in.ts", 5);
    let out = tmp_path("cap_marker_out.mp4");

    let generated = Arc::new(AtomicUsize::new(0));
    let cue_seen = Arc::new(AtomicBool::new(false));
    let real_after_cue = Arc::new(AtomicBool::new(false));
    let sentinel_frames = Arc::new(AtomicUsize::new(0));

    // INPUT-side pipeline: only input pipelines receive the decoder's
    // EOF-timestamp marker when frames flowed. Converter FIRST (saturates
    // from its cue, then answers the source marker with a real frame),
    // ordering sentinel SECOND.
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter(
            "marker-converting-generator",
            Box::new(CueArmedSaturatingGenerator {
                generated: generated.clone(),
                armed: false,
                convert_markers: true,
                stash: None,
                next_pts: 0,
                time_base: AVRational { num: 0, den: 1 },
                duration: 0,
            }),
        )
        .filter(
            "cue-order-sentinel",
            Box::new(CueOrderSentinel {
                cue_seen: cue_seen.clone(),
                real_after_cue: real_after_cue.clone(),
                frames_seen: sentinel_frames.clone(),
            }),
        );

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).add_frame_pipeline(pipeline))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "capped filter vs source marker traversal",
    );
    assert!(result.is_ok(), "transcode failed: {result:?}");

    assert!(
        cue_seen.load(Ordering::SeqCst),
        "the downstream sentinel never received its flush cue"
    );
    assert!(
        generated.load(Ordering::SeqCst) >= 1024,
        "the generator never saturated the EOF flush cap (generated {})",
        generated.load(Ordering::SeqCst)
    );
    assert!(
        !real_after_cue.load(Ordering::SeqCst),
        "a flush-capped filter converted the source EOF marker into a real \
         frame for a filter that already consumed its cue: the marker \
         traversal must skip capped filters"
    );
}
