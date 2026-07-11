//! Regression: a filtergraph whose FIRST input pad is cross-graph bound (fed by
//! another filter_complex's labeled output) and whose SECOND pad is a demuxer
//! stream used to panic inside `build()`. The demuxer bind did
//! `Vec::insert(1, ..)` into the consumer node's still-empty scheduler-input
//! list ("insertion index (is 1) should be <= len (is 0)") because the
//! cross-graph pad before it had been skipped. Scheduler inputs are now
//! pad-indexed, with cross-graph pads left as `None` holes, so `build()`
//! succeeds and the run does not hang (the hole falls back to unchoking all live
//! demuxers).

use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::{AVMediaType, FfmpegContext, FfmpegScheduler, Frame, Input, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_cross_graph_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

fn wait_with_watchdog(
    scheduler: FfmpegScheduler<ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running>,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: wait() thread panicked before reporting")
        }
    }
}

/// FC0 produces the label `mid`; FC1 consumes `mid` (a cross-graph pad — pad 0)
/// plus the demuxer stream `1:v` (pad 1). Building FC1's demuxer bind for pad 1
/// used to `Vec::insert(1, ..)` into an empty list and panic.
fn cross_graph_builder(out: &str) -> FfmpegContext {
    FfmpegContext::builder()
        // Finite (0.2s) sources so the graph configures and the run completes
        // instead of flooding a not-yet-configured pad's pre-config queue.
        .input(Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi"))
        .input(Input::from("color=c=blue:s=64x64:r=15:d=0.2").set_format("lavfi"))
        .filter_desc("[0:v]hue=s=0[mid]")
        .filter_desc("[mid][1:v]overlay[vout]")
        .output(
            Output::from(out)
                .add_stream_map("vout")
                .set_video_codec("mpeg4"),
        )
        .build()
        .expect("cross-graph build must not panic or error")
}

#[test]
fn cross_graph_bound_pad_before_demux_pad_builds_without_panic() {
    // The panic (if any) fires inside build(); reaching this line is the pass.
    let _ctx = cross_graph_builder(&tmp_path("cross_graph_build.mp4"));
}

#[test]
fn cross_graph_bound_pad_before_demux_pad_runs_to_completion() {
    let ctx = cross_graph_builder(&tmp_path("cross_graph_run.mp4"));
    let scheduler = ctx.start().expect("start");
    // The pad-0 hole must fall back to unchoking all live demuxers so the
    // producer graph's demuxer is not choked — the run must complete SUCCESSFULLY
    // (a shifted/mis-routed list would choke the producer demuxer and stall the
    // consumer's pad until the pre-config cap fired, i.e. NOT Ok). The watchdog
    // turns any hang into a failure.
    wait_with_watchdog(scheduler, 30, "cross-graph overlay run").expect("run to completion");
}

/// Counts non-null frames reaching the output encoder.
struct FrameCounter {
    count: Arc<AtomicUsize>,
}

impl FrameFilter for FrameCounter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            if !frame.as_ptr().is_null() && !frame.is_empty() {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(Some(frame))
    }
}

// EOF-sensitive fail-on-revert oracle. `fps` consumes the cross-graph pad `mid`
// DIRECTLY (no overlay/framesync in between to regenerate the EOF), so it re-times
// onto a 30fps grid using the cross-graph buffersrc's OWN closing timestamp. The
// three 15fps producer frames occupy pts 0,1,2 with the tail ending at 3/15, so
// the full 30fps grid over [0, 3/15) is 6 frames. `fg_send_eof` closes the pad
// with av_buffersrc_add_frame_flags(NULL, PUSH), preserving that 3/15 frame-end
// time. Reverting to av_buffersrc_close(AV_NOPTS_VALUE) drops the tail to the last
// frame's START time (2/15), and fps then emits only 4 — so this asserts 6 and
// fails on revert. (An overlay-fronted chain masks the difference because
// framesync extrapolates its own EOF state, which is why the earlier overlay test
// could not distinguish the two.)
#[test]
fn cross_graph_fps_retimes_from_the_preserved_eof_tail() {
    let count = Arc::new(AtomicUsize::new(0));
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "frame_counter",
        Box::new(FrameCounter {
            count: Arc::clone(&count),
        }),
    );
    let out = tmp_path("cross_graph_fps.mp4");
    let ctx = FfmpegContext::builder()
        .input(Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi"))
        .filter_desc("[0:v]hue=s=0[mid]")
        // fps directly consumes the cross-graph pad `mid`, so its EOF re-timing
        // reads the cross-graph buffersrc's own closing timestamp.
        .filter_desc("[mid]fps=30[vout]")
        .output(
            Output::from(out.as_str())
                .add_stream_map("vout")
                .set_video_codec("mpeg4")
                .set_frame_pipelines(vec![pipeline]),
        )
        .build()
        .expect("cross-graph build");
    let scheduler = ctx.start().expect("start");
    wait_with_watchdog(scheduler, 30, "cross-graph fps retime").expect("run to completion");

    // 6 only when the cross-graph EOF preserves the accumulated tail (3/15); a
    // dropped-tail EOF (close at the last frame's start 2/15) re-times fps short to 4.
    assert_eq!(
        count.load(Ordering::SeqCst),
        6,
        "cross-graph EOF must preserve the tail time (3/15) so fps=30 emits the full 6-frame grid"
    );
}
