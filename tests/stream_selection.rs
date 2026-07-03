//! PR-6 regression net: automatic stream selection and streamcopy behavior
//! must match the ffmpeg CLI.
//!
//! - map_auto_video/audio pick the globally best stream across ALL inputs
//!   (highest resolution / most channels), not the first stream of the
//!   first input (ffmpeg_mux_init.c map_auto_video/map_auto_audio).
//! - The auto-map copy path maps exactly ONE stream per media type.
//! - Streamcopy clears a codec_tag the target container cannot represent
//!   instead of failing avformat_write_header (e.g. AVI FMP4 -> MP4).

use ez_ffmpeg::stream_info::{find_all_stream_infos, find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_selection_tests_{}",
        std::process::id()
    ));
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

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

/// 0.3s finite video fixture at the given resolution.
fn video_fixture(name: &str, size: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from(format!("color=c=black:s={size}:r=30")).set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "video fixture",
    )
    .unwrap();
    path
}

/// 0.3s finite audio fixture with the given channel layout.
fn audio_fixture(name: &str, layout: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(
                Input::from(format!(
                    "anullsrc=channel_layout={layout}:sample_rate=44100"
                ))
                .set_format("lavfi"),
            )
            .output(
                Output::from(path.as_str())
                    .set_audio_codec("aac")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "audio fixture",
    )
    .unwrap();
    path
}

fn output_video_width(path: &str) -> i32 {
    match find_video_stream_info(path)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { width, .. } => width,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn auto_map_picks_highest_resolution_video_across_inputs() {
    // The LOW resolution input comes first: first-found selection is wrong.
    let low = video_fixture("low_320.mp4", "320x240");
    let high = video_fixture("high_640.mp4", "640x480");

    let out = tmp_path("picked_video.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(low.as_str()))
            .input(Input::from(high.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "highest-resolution selection",
    );
    assert!(result.is_ok(), "selection task failed: {result:?}");
    assert_eq!(
        output_video_width(&out),
        640,
        "auto mapping must pick the highest-resolution video across all inputs"
    );
}

#[test]
fn auto_map_picks_most_audio_channels_across_inputs() {
    let mono = audio_fixture("mono.mp4", "mono");
    let stereo = audio_fixture("stereo.mp4", "stereo");

    let out = tmp_path("picked_audio.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(mono.as_str()))
            .input(Input::from(stereo.as_str()))
            .output(Output::from(out.as_str()).set_audio_codec("aac"))
            .build()
            .unwrap(),
        "most-channels selection",
    );
    assert!(result.is_ok(), "selection task failed: {result:?}");
    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no audio stream")
    {
        StreamInfo::Audio { nb_channels, .. } => assert_eq!(
            nb_channels, 2,
            "auto mapping must pick the audio stream with the most channels"
        ),
        other => panic!("expected audio stream info, got {other:?}"),
    }
}

#[test]
fn copy_auto_map_selects_a_single_stream() {
    let low = video_fixture("copy_low.mp4", "320x240");
    let high = video_fixture("copy_high.mp4", "640x480");

    let out = tmp_path("copy_single.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(low.as_str()))
            .input(Input::from(high.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("copy"))
            .build()
            .unwrap(),
        "copy single selection",
    );
    assert!(result.is_ok(), "copy task failed: {result:?}");

    let video_streams = find_all_stream_infos(&out)
        .expect("failed to probe output")
        .into_iter()
        .filter(|info| matches!(info, StreamInfo::Video { .. }))
        .count();
    assert_eq!(
        video_streams, 1,
        "auto mapping maps exactly one video stream, like the CLI"
    );
    assert_eq!(
        output_video_width(&out),
        640,
        "the copied stream must also be the globally best one"
    );
}

#[test]
fn avi_copy_to_mp4_clears_incompatible_codec_tag() {
    // The AVI muxer tags mpeg4 as "FMP4"; mp4 cannot represent that tag and
    // its write_header rejects it unless streamcopy clears mismatched tags
    // like the CLI does.
    let avi = tmp_path("source.avi");
    run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(avi.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "avi fixture",
    )
    .unwrap();

    let out = tmp_path("copied.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(avi.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("copy"))
            .build()
            .unwrap(),
        "avi -> mp4 copy",
    );
    assert!(
        result.is_ok(),
        "streamcopy to a container with different tags must succeed: {result:?}"
    );
    assert!(
        find_video_stream_info(&out)
            .expect("failed to probe output")
            .is_some(),
        "copied output must contain the video stream"
    );
}
