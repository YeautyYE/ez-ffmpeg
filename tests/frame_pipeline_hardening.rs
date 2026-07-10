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
use ffmpeg_sys_next::{av_frame_ref, AVRational};
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
        _ctx: &FrameFilterContext,
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
        _ctx: &FrameFilterContext,
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
        _ctx: &FrameFilterContext,
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
    assert!(result.is_ok(), "transcode with recorders failed: {result:?}");

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
    assert!(
        baseline_frames > 0,
        "baseline transcode produced no frames"
    );

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
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe {
            frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null()
        };
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
        _ctx: &FrameFilterContext,
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
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe {
            frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null()
        };
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
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe {
            frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null()
        };
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
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        let props_only = unsafe {
            frame.as_ptr().is_null() || (*frame.as_ptr()).buf[0].is_null()
        };
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
        _ctx: &FrameFilterContext,
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
        .input(
            Input::from(fixture.as_str()).add_frame_pipeline(pipeline),
        )
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
        scheduler.stop();
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
        scheduler.stop();
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
        scheduler.stop();
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
