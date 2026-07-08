//! PR-3 regression net: the subtitle transcode/copy chain.
//!
//! The chain was fully broken: real subtitle packets were swapped for empty
//! flush packets before decoding (dec_task), the auto-mapper rejected
//! explicit codecs and treated "copy" as a missing encoder
//! (map_auto_subtitle), and the decoder's subtitle_header crossed threads as
//! a raw pointer into a freeable context. These tests pin the CLI-aligned
//! behavior end to end.

use ez_ffmpeg::stream_info::{find_subtitle_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_subtitle_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// Two-cue SubRip fixture; the payload strings double as content probes.
fn srt_fixture(name: &str) -> String {
    let path = tmp_path(name);
    std::fs::write(
        &path,
        "1\n00:00:00,000 --> 00:00:01,000\nHello subtitle\n\n\
         2\n00:00:01,000 --> 00:00:02,000\nSecond line\n\n",
    )
    .unwrap();
    path
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

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

#[test]
fn srt_to_mkv_to_srt_roundtrip_keeps_text() {
    let srt_in = srt_fixture("roundtrip_in.srt");
    let mkv = tmp_path("roundtrip.mkv");
    let srt_out = tmp_path("roundtrip_out.srt");

    // Leg 1: srt -> mkv (auto-mapped, default container codec).
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(srt_in.as_str()))
            .output(Output::from(mkv.as_str()))
            .build()
            .unwrap(),
        "srt -> mkv",
    );
    assert!(result.is_ok(), "srt -> mkv transcode failed: {result:?}");
    assert!(
        find_subtitle_stream_info(&mkv)
            .expect("failed to probe mkv")
            .is_some(),
        "mkv output must contain a subtitle stream"
    );

    // Leg 2: mkv -> srt (decode + re-encode back to text).
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(mkv.as_str()))
            .output(Output::from(srt_out.as_str()))
            .build()
            .unwrap(),
        "mkv -> srt",
    );
    assert!(result.is_ok(), "mkv -> srt transcode failed: {result:?}");

    let text = std::fs::read_to_string(&srt_out).expect("srt output missing");
    assert!(
        text.contains("Hello subtitle") && text.contains("Second line"),
        "subtitle text must survive the roundtrip, got:\n{text}"
    );
}

#[test]
fn srt_to_mp4_with_explicit_mov_text() {
    let srt_in = srt_fixture("mov_text_in.srt");
    let out = tmp_path("mov_text.mp4");

    let result = run(
        FfmpegContext::builder()
            .input(Input::from(srt_in.as_str()))
            .output(Output::from(out.as_str()).set_subtitle_codec("mov_text"))
            .build()
            .unwrap(),
        "srt -> mp4 (mov_text)",
    );
    assert!(result.is_ok(), "srt -> mp4 transcode failed: {result:?}");

    match find_subtitle_stream_info(&out)
        .expect("failed to probe mp4")
        .expect("mp4 output must contain a subtitle stream")
    {
        // Two cues plus the mov muxer's trailing blank sample that terminates
        // the last cue — the ffmpeg CLI writes the same three samples.
        StreamInfo::Subtitle { nb_frames, .. } => assert_eq!(
            nb_frames, 3,
            "both subtitle cues (and the mov end marker) must reach the mp4 output"
        ),
        other => panic!("expected subtitle stream info, got {other:?}"),
    }
}

#[test]
fn srt_to_mkv_with_subtitle_copy() {
    let srt_in = srt_fixture("copy_in.srt");
    let out = tmp_path("copy.mkv");

    // "copy" is a stream-copy request, not an encoder name: the mapper must
    // take the streamcopy path instead of failing with encoder-not-found.
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(srt_in.as_str()))
            .output(Output::from(out.as_str()).set_subtitle_codec("copy"))
            .build()
            .unwrap(),
        "srt -> mkv (copy)",
    );
    assert!(result.is_ok(), "subtitle copy failed: {result:?}");

    match find_subtitle_stream_info(&out)
        .expect("failed to probe mkv")
        .expect("mkv output must contain the copied subtitle stream")
    {
        StreamInfo::Subtitle { codec_name, .. } => {
            assert_eq!(codec_name, "subrip", "copy must preserve the input codec")
        }
        other => panic!("expected subtitle stream info, got {other:?}"),
    }
}

#[test]
fn video_and_subtitle_map_together() {
    let srt_in = srt_fixture("av_sub_in.srt");
    let out = tmp_path("video_sub.mkv");

    let result = run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .input(Input::from(srt_in.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(500_000),
            )
            .build()
            .unwrap(),
        "video + subtitle",
    );
    assert!(result.is_ok(), "video+subtitle task failed: {result:?}");
    assert!(
        find_video_stream_info(&out)
            .expect("failed to probe mkv")
            .is_some(),
        "output must contain the video stream"
    );
    assert!(
        find_subtitle_stream_info(&out)
            .expect("failed to probe mkv")
            .is_some(),
        "output must contain the subtitle stream alongside video"
    );

    // Stream presence is not enough (an empty track still probes): prove the
    // cues actually flowed by extracting them back to text.
    let srt_out = tmp_path("av_sub_out.srt");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(out.as_str()))
            .output(Output::from(srt_out.as_str()))
            .build()
            .unwrap(),
        "extract subtitle from muxed output",
    );
    assert!(result.is_ok(), "subtitle extraction failed: {result:?}");
    let text = std::fs::read_to_string(&srt_out).expect("srt output missing");
    assert!(
        text.contains("Hello subtitle"),
        "subtitle cues must reach the muxed output, got:\n{text}"
    );
}
