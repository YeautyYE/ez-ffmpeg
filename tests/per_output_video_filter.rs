//! Per-output simple video filter (`Output::set_video_filter`, FFmpeg `-vf`).
//!
//! The implicit per-output filtergraph every simple video encode runs through
//! defaults to the passthrough `null` chain; `set_video_filter` replaces that
//! chain for one output. These tests pin:
//! - the filter actually transforms the encoded stream (dimensions),
//! - the scope is per-output (a second output stays untouched),
//! - audio is never routed through the video chain,
//! - every conflicting spelling is a typed build() error, mirroring the CLI
//!   (streamcopy, complex-graph feeds, non-linear or non-video chains).

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_video_filter_tests", name)
}

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

/// `FfmpegContext` has no `Debug` impl, so `unwrap_err()` cannot be used.
fn build_err(result: ez_ffmpeg::error::Result<FfmpegContext>) -> Error {
    match result {
        Ok(_) => panic!("expected build() to fail"),
        Err(err) => err,
    }
}

/// 0.5s 320x240 video-only fixture.
fn video_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(500_000),
            )
            .build()
            .unwrap(),
        "video fixture",
    )
    .unwrap();
    path
}

/// 0.5s fixture with one 320x240 video stream and one stereo AAC stream.
fn av_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30").set_format("lavfi"))
            .input(Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_recording_time_us(500_000),
            )
            .build()
            .unwrap(),
        "av fixture",
    )
    .unwrap();
    path
}

fn video_dimensions(path: &str) -> (i32, i32) {
    match find_video_stream_info(path).unwrap() {
        Some(StreamInfo::Video { width, height, .. }) => (width, height),
        other => panic!("expected a video stream in {path}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Happy path: the chain transforms the encoded video stream.
// ---------------------------------------------------------------------------

#[test]
fn scales_video_to_requested_size() {
    let input = video_fixture("scale_in.mp4");
    let out = tmp_path("scale_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "scale 160x120",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (160, 120));
}

#[test]
fn scale_negative_two_keeps_even_height() {
    let input = video_fixture("scale_neg2_in.mp4");
    let out = tmp_path("scale_neg2_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=100:-2"),
            )
            .build()
            .unwrap(),
        "scale 100:-2",
    )
    .unwrap();
    // 320x240 -> width 100 gives 75, which -2 rounds to the nearest even 76.
    assert_eq!(video_dimensions(&out), (100, 76));
}

#[test]
fn filter_chain_applies_in_order() {
    let input = video_fixture("chain_in.mp4");
    let out = tmp_path("chain_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=200:150,crop=64:64"),
            )
            .build()
            .unwrap(),
        "scale then crop",
    )
    .unwrap();
    // crop after scale: only a chain evaluated left-to-right yields 64x64.
    assert_eq!(video_dimensions(&out), (64, 64));
}

#[test]
fn applies_per_output_not_globally() {
    let input = video_fixture("per_output_in.mp4");
    let filtered = tmp_path("per_output_filtered.mp4");
    let untouched = tmp_path("per_output_untouched.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(filtered.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .output(Output::from(untouched.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "one filtered output, one untouched",
    )
    .unwrap();
    assert_eq!(video_dimensions(&filtered), (160, 120));
    assert_eq!(video_dimensions(&untouched), (320, 240));
}

#[test]
fn audio_stream_passes_unfiltered() {
    let input = av_fixture("audio_pass_in.mp4");
    let out = tmp_path("audio_pass_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "video filtered, audio re-encoded",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (160, 120));
    match find_audio_stream_info(&out).unwrap() {
        Some(StreamInfo::Audio {
            codec_name,
            sample_rate,
            ..
        }) => {
            assert_eq!(codec_name, "aac");
            assert_eq!(sample_rate, 44100);
        }
        other => panic!("expected an audio stream, got {other:?}"),
    }
}

#[test]
fn audio_copy_rides_alongside_video_filter() {
    // The cookbook resize shape: -vf scale=... -c:a copy. The copy map covers
    // only audio, so it must NOT trip the video streamcopy conflict.
    let input = av_fixture("audio_copy_in.mp4");
    let out = tmp_path("audio_copy_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("copy")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "video filtered, audio copied",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (160, 120));
    match find_audio_stream_info(&out).unwrap() {
        Some(StreamInfo::Audio { codec_name, .. }) => assert_eq!(codec_name, "aac"),
        other => panic!("expected the copied audio stream, got {other:?}"),
    }
}

#[test]
fn empty_filter_clears_previous_chain() {
    let input = video_fixture("clear_in.mp4");
    let out = tmp_path("clear_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120")
                    .set_video_filter(""),
            )
            .build()
            .unwrap(),
        "cleared filter",
    )
    .unwrap();
    // The empty string restores the implicit passthrough chain.
    assert_eq!(video_dimensions(&out), (320, 240));
}

// ---------------------------------------------------------------------------
// Conflicts: every unsupported combination is a typed build() error.
// ---------------------------------------------------------------------------

#[test]
fn copy_codec_with_filter_is_rejected() {
    let input = video_fixture("copy_conflict_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("copy_conflict_out.mp4").as_str())
                    .set_video_codec("copy")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::FilterWithStreamCopy(desc))
                if desc == "scale=160:120"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn copy_stream_map_with_filter_is_rejected() {
    let input = video_fixture("map_copy_conflict_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("map_copy_conflict_out.mp4").as_str())
                    .add_stream_map_with_copy("0:v")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::FilterWithStreamCopy(desc))
                if desc == "scale=160:120"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn audio_copy_stream_map_with_filter_is_accepted() {
    // A copy map covering only AUDIO must not be mistaken for the video
    // conflict: the video still auto-maps through the filtered encode path.
    let input = av_fixture("map_audio_copy_in.mp4");
    let out = tmp_path("map_audio_copy_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v")
                    .add_stream_map_with_copy("0:a")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "explicit maps, audio copied, video filtered",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (160, 120));
}

#[test]
fn context_graph_with_filter_is_rejected() {
    let input = video_fixture("complex_conflict_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("complex_conflict_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleAndComplexFilter(desc))
                if desc == "scale=160:120"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn labeled_graph_map_with_filter_is_rejected() {
    let input = video_fixture("labeled_conflict_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("[0:v]hue=s=0[vout]")
            .output(
                Output::from(tmp_path("labeled_conflict_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("[vout]")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleAndComplexFilter(desc))
                if desc == "scale=160:120"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn multi_output_pad_chain_is_rejected() {
    let input = video_fixture("split_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("split_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("split"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape {
                desc,
                inputs: 1,
                outputs: 2,
            }) if desc == "split"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn audio_chain_on_video_stream_is_rejected() {
    let input = video_fixture("anull_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("anull_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("anull"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterMediaTypeMismatch {
                desc,
                found,
                expected,
            }) if desc == "anull" && found == "audio" && expected == "video"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn unknown_filter_name_fails_build() {
    let input = video_fixture("unknown_filter_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unknown_filter_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("no_such_filter_xyz=1"),
            )
            .build(),
    );
    assert!(
        matches!(err, Error::FilterGraphParse(_)),
        "an unknown filter name must fail build() with a parse error, got {err:?}"
    );
}
