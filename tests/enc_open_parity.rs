//! fftools enc parity: subtitle encoders open immediately.
//!
//! AVFrame-based (audio/video) encoders open on their first frame, but
//! fftools opens subtitle encoders before entering the receive loop
//! (ffmpeg_enc.c encoder_thread: "Open the subtitle encoders immediately").
//! Waiting for a first frame breaks streams that legitimately produce no
//! packet inside the muxed range — the CLI still writes the stream and the
//! header; first-frame-open reported NoFramesReceived and failed the task.

use ez_ffmpeg::stream_info::{find_subtitle_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_enc_open_tests_{}",
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
fn subtitle_stream_with_no_cues_in_range_still_muxes() {
    // Video fixture: 1s of frames.
    let video = tmp_path("enc_open_video.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(video.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "video fixture",
    );
    assert!(result.is_ok(), "video fixture failed: {result:?}");

    // Subtitle fixture whose only cue starts at t=10s — far outside the 1s
    // output range, so the subtitle encoder never receives a frame.
    let srt = tmp_path("late_cue.srt");
    std::fs::write(
        &srt,
        "1\n00:00:10,000 --> 00:00:11,000\nlate cue\n\n",
    )
    .unwrap();

    // Intermediate mkv whose subtitle track exists but carries ZERO packets
    // (CLI: -map 1:s -c:s srt -t 1 keeps the track, drops the late cue).
    let mkv = tmp_path("empty_sub.mkv");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(video.as_str()))
            .input(Input::from(srt.as_str()))
            .output(
                Output::from(mkv.as_str())
                    .add_stream_map_with_copy("0:v")
                    .add_stream_map("1:s")
                    .set_subtitle_codec("srt")
                    .set_recording_time_us(1_000_000),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "empty subtitle track fixture",
    );
    assert!(result.is_ok(), "mkv fixture failed: {result:?}");

    // CLI: ffmpeg -i empty_sub.mkv -map 0:v -map 0:s -c:s mov_text succeeds
    // and writes both streams even though the subtitle encoder receives no
    // frame at all — fftools opens subtitle encoders before the receive
    // loop (ffmpeg_enc.c encoder_thread), so the muxer still becomes ready.
    let out = tmp_path("enc_open_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(mkv.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map("0:v")
                    .add_stream_map("0:s")
                    .set_video_codec("mpeg4")
                    .set_subtitle_codec("mov_text"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "no-cue subtitle mux",
    );
    assert!(
        result.is_ok(),
        "a subtitle stream with no cues in range must still mux (CLI parity): {result:?}"
    );

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert!(nb_frames > 0, "video stream must carry frames");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    // The subtitle stream must exist in the container even with no cues.
    let sub = find_subtitle_stream_info(&out).expect("failed to probe output");
    assert!(
        sub.is_some(),
        "the subtitle stream must be present in the output"
    );
}
