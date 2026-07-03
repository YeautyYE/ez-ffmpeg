//! vsync AUTO must resolve to VSCFR for single-stream inputs fed directly by
//! a decoder with zero input_ts_offset (ffmpeg_mux_init.c:817-822), so a
//! late-starting single-stream input is not padded back to ts 0 with
//! duplicate frames.

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_vsync_auto_tests_{}",
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
fn auto_vsync_single_stream_input_is_not_padded_to_zero() {
    // Single-stream mpegts with B-frames: after input start-time correction
    // (min dts -> 0) the first frame in display order still lands at a
    // small positive pts (the B-frame reorder delay). Plain CFR pads that
    // gap with duplicates from 0; the AUTO single-input VSCFR rule keeps
    // the original grid.
    let fixture = tmp_path("single_late.ts");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=green:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(fixture.as_str())
                    .set_format("mpegts")
                    .set_video_codec("mpeg2video")
                    .set_video_codec_opt("bf", "2")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "single-stream fixture",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");

    // CLI: ffmpeg -i single_late.ts -c:v mpeg4 out.mp4 emits exactly 30
    // frames — no duplicates padding the reorder-delay gap
    // (ffmpeg_mux_init.c:817-822 flips CFR to VSCFR for single-stream
    // inputs fed directly by a decoder with zero input_ts_offset).
    let out = tmp_path("single_late_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "auto vsync transcode",
    );
    assert!(result.is_ok(), "transcode failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert_eq!(
                nb_frames, 30,
                "a single-stream input must not gain duplicates for the \
                 B-frame reorder gap (CLI parity)"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}
