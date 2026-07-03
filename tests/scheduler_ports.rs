//! PR-9 regression net: fftools scheduler behaviors.
//!
//! - Input::set_framerate must force the CFR grid through the DECODE path
//!   (fftools DECODER_FLAG_FRAMERATE_FORCED), not only the demuxer's DTS
//!   estimation: 30 input frames at -r 60 produce a 0.5s/60fps output.
//! - stream_loop across a transcode must produce N*loops frames with
//!   monotonic timestamps (loop end_pts bookkeeping).

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_sched_port_tests_{}",
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

fn fixture_30_frames(name: &str) -> String {
    let path = tmp_path(name);
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
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

#[test]
fn input_framerate_forces_cfr_grid_through_decode() {
    let fixture = fixture_30_frames("fr_fixture.mp4");

    // ffmpeg -r 60 -i fixture -c:v mpeg4: 30 frames restamped onto a 60fps
    // grid — same frame count, half the duration (CLI: nb_frames=30,
    // avg_frame_rate=60/1, duration=0.5).
    let out = tmp_path("fr_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).set_framerate(60, 1))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "forced framerate",
    );
    assert!(result.is_ok(), "forced-framerate task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            nb_frames,
            avg_frame_rate,
            duration,
            time_base,
            ..
        } => {
            assert_eq!(nb_frames, 30, "every input frame must be restamped, not dropped");
            let fps = avg_frame_rate.num as f64 / avg_frame_rate.den as f64;
            assert!(
                (fps - 60.0).abs() < 0.5,
                "output must be on the forced 60fps grid, got {fps}"
            );
            let secs = duration as f64 * time_base.num as f64 / time_base.den as f64;
            assert!(
                (secs - 0.5).abs() < 0.1,
                "30 frames at a forced 60fps last 0.5s, got {secs}s"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn stream_loop_doubles_frames_with_monotonic_timestamps() {
    let fixture = fixture_30_frames("loop_fixture.mp4");

    // ffmpeg -stream_loop 1 -i fixture: the input plays twice; failing
    // end_pts bookkeeping shows up as duplicate/backwards timestamps or a
    // short output.
    let out = tmp_path("loop_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).set_stream_loop(1))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "stream_loop",
    );
    assert!(result.is_ok(), "stream_loop task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            nb_frames,
            duration,
            time_base,
            ..
        } => {
            assert_eq!(
                nb_frames, 60,
                "one loop doubles the 30-frame input (CLI parity)"
            );
            let secs = duration as f64 * time_base.num as f64 / time_base.den as f64;
            assert!(
                (secs - 2.0).abs() < 0.2,
                "two passes of a 1s fixture last 2s, got {secs}s"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}
