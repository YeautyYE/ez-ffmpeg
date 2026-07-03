//! PR-6b regression net: fps conversion (video sync) must match the ffmpeg
//! CLI.
//!
//! VSCFR is "CFR that respects the first frame's original timestamp"
//! (auto-selected by the CLI for -r + -copyts): it must NOT pad from ts 0 up
//! to the first frame, but MUST still fill mid-stream gaps and stamp CFR
//! durations exactly like CFR — in ffmpeg_filter.c video_sync_process the
//! VSYNC_VSCFR case falls through into VSYNC_CFR.

use ez_ffmpeg::core::context::output::VSyncMethod;
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{AVRational, FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_video_sync_tests_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// A hang is a test failure, not a suite timeout.
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

#[test]
fn fpsmax_caps_only_framerates_above_the_cap() {
    // CLI parity for -fpsmax (ffmpeg_filter.c choose_out_timebase): a 20fps
    // input with fpsmax 30 keeps its native 20fps — the cap applies only
    // when the rate exceeds it (or is unknown), never unconditionally.
    let fixture = tmp_path("fpsmax_20_fixture.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=20").set_format("lavfi"))
            .output(
                Output::from(fixture.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fpsmax fixture 20fps",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");

    let out = tmp_path("fpsmax_20_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_framerate_max(AVRational { num: 30, den: 1 }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fpsmax below cap",
    );
    assert!(result.is_ok(), "fpsmax task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { avg_frame_rate, .. } => {
            let fps = avg_frame_rate.num as f64 / avg_frame_rate.den as f64;
            assert!(
                (fps - 20.0).abs() < 0.5,
                "a 20fps input under fpsmax 30 must stay 20fps, got {fps}"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    // And a 60fps input IS capped to 30fps.
    let fixture = tmp_path("fpsmax_60_fixture.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=60").set_format("lavfi"))
            .output(
                Output::from(fixture.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(60),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fpsmax fixture 60fps",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");

    let out = tmp_path("fpsmax_60_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_framerate_max(AVRational { num: 30, den: 1 }),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fpsmax above cap",
    );
    assert!(result.is_ok(), "fpsmax task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { avg_frame_rate, .. } => {
            let fps = avg_frame_rate.num as f64 / avg_frame_rate.den as f64;
            assert!(
                (fps - 30.0).abs() < 0.5,
                "a 60fps input under fpsmax 30 must be capped to 30fps, got {fps}"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn vscfr_fills_gaps_but_keeps_initial_offset() {
    // Fixture: exactly 30 frames at 30fps.
    let fixture = tmp_path("vscfr_fixture.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(fixture.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "vscfr fixture",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");

    // Drop every other frame (mid-stream gaps) and shift everything by 2s
    // (initial offset). The CLI (-copyts -r 30, which auto-selects vscfr)
    // produces 29 frames starting at 2s: 15 kept frames plus 14 gap-filling
    // duplicates, and NO duplicates padding out the initial 2 seconds.
    let out = tmp_path("vscfr_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .filter_desc("select='not(mod(n,2))',setpts=PTS+2/TB")
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_framerate(AVRational { num: 30, den: 1 })
                    .set_vsync_method(VSyncMethod::VsyncVscfr),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "vscfr conversion",
    );
    assert!(result.is_ok(), "vscfr task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            nb_frames,
            start_time,
            time_base,
            ..
        } => {
            assert_eq!(
                nb_frames, 29,
                "vscfr must fill mid-stream gaps like CFR \
                 (15 kept frames + 14 duplicates, ffmpeg CLI parity)"
            );
            let start_secs = start_time as f64 * time_base.num as f64 / time_base.den as f64;
            assert!(
                (start_secs - 2.0).abs() < 0.1,
                "vscfr must keep the initial 2s offset instead of \
                 duplicating frames back to ts 0, got start {start_secs}s"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}
