//! P0 hardening: filtergraph reinit logging and `/opt=file` option loading.
//!
//! - A mid-stream parameter change (resolution switch inside one input) must
//!   reconfigure the filtergraph and keep every frame, matching the CLI. The
//!   reinit reason logging used to build CStrings with embedded `\0` (always
//!   Err -> unwrap panic) which killed the filter thread on first reinit.
//! - fftools option syntax `/opt=path` loads the option value from a file
//!   (ffmpeg_filter.c filter_opt_apply). file_read used to pass a null
//!   AVBPrint to av_bprint_init (SIGSEGV) and av_freep the buffer pointer by
//!   value (frees whatever the first 8 bytes point at).

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_filter_hardening_tests_{}",
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

fn mpegts_fixture(name: &str, size: &str, frames: u32) -> String {
    let path = tmp_path(name);
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(format!("color=c=blue:s={size}:r=30")).set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_format("mpegts")
                    .set_video_codec("mpeg2video")
                    .set_max_video_frames(frames as i64),
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
fn mid_stream_resolution_change_reconfigures_filtergraph() {
    // Two mpegts segments with different resolutions joined via the concat
    // protocol: the decoder emits 640x480 frames mid-stream, forcing the
    // VIDEO_CHANGED reinit path (fftools fg_send_frame need_reinit).
    let seg_a = mpegts_fixture("reinit_a.ts", "320x240", 30);
    let seg_b = mpegts_fixture("reinit_b.ts", "640x480", 30);

    let out = tmp_path("reinit_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(format!("concat:{seg_a}|{seg_b}")))
            .filter_desc("scale=320:240")
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "mid-stream reinit",
    );
    assert!(result.is_ok(), "reinit transcode failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            // CLI parity: `ffmpeg -i concat:a.ts|b.ts -vf scale=320:240` emits
            // 59 frames (one frame is consumed at the mpeg2 concat boundary by
            // the decoder itself). Before the fix the filter thread died at
            // the first reinit and only ~29 frames survived.
            assert_eq!(
                nb_frames, 59,
                "a mid-stream resolution change must reconfigure the graph, not kill it"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn filter_option_value_loaded_from_file() {
    // fftools `/opt=path` syntax (ffmpeg_filter.c filter_opt_apply): the
    // option value is read from the file at `path`.
    let fixture = mpegts_fixture("optfile_in.ts", "320x240", 30);

    let value_file = tmp_path("scale_w.txt");
    std::fs::write(&value_file, "160").unwrap();

    let out = tmp_path("optfile_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .filter_desc(format!("scale=/w={value_file}:h=120"))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "option from file",
    );
    assert!(result.is_ok(), "option-from-file transcode failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { width, .. } => {
            assert_eq!(width, 160, "scale width must come from the option file");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}
