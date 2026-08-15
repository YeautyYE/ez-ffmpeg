//! Per-output simple audio filter (`Output::set_audio_filter`, FFmpeg `-af`).
//!
//! Mirrors `per_output_video_filter.rs`: the implicit per-output graph
//! defaults to `anull`; `set_audio_filter` replaces that chain for one
//! output. These tests pin the transform, per-output scope, and the typed
//! conflict errors (streamcopy, unused, media-type mismatch).

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::stream_info::{find_audio_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_audio_filter_tests", name)
}

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

fn build_err(result: ez_ffmpeg::error::Result<FfmpegContext>) -> Error {
    match result {
        Ok(_) => panic!("expected build() to fail"),
        Err(err) => err,
    }
}

fn audio_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_audio_codec("aac")
                    .set_recording_time_us(500_000),
            )
            .build()
            .unwrap(),
        "audio fixture",
    )
    .unwrap();
    path
}

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

fn audio_sample_rate(path: &str) -> i32 {
    match find_audio_stream_info(path).unwrap() {
        Some(StreamInfo::Audio { sample_rate, .. }) => sample_rate,
        other => panic!("expected an audio stream in {path}, got {other:?}"),
    }
}

#[test]
fn aformat_resamples_to_requested_rate() {
    let input = audio_fixture("aformat_in.m4a");
    let out = tmp_path("aformat_out.m4a");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("aac")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build()
            .unwrap(),
        "aformat 16000",
    )
    .unwrap();
    assert_eq!(audio_sample_rate(&out), 16000);
}

#[test]
fn filter_is_per_output() {
    let input = audio_fixture("per_out_in.m4a");
    let filtered = tmp_path("per_out_filtered.m4a");
    let untouched = tmp_path("per_out_untouched.m4a");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(filtered.as_str())
                    .set_audio_codec("aac")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .output(Output::from(untouched.as_str()).set_audio_codec("aac"))
            .build()
            .unwrap(),
        "two outputs, one filtered",
    )
    .unwrap();
    assert_eq!(audio_sample_rate(&filtered), 16000);
    assert_eq!(audio_sample_rate(&untouched), 44100);
}

#[test]
fn video_is_not_routed_through_audio_chain() {
    let input = av_fixture("av_in.mp4");
    let out = tmp_path("av_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build()
            .unwrap(),
        "av with audio filter",
    )
    .unwrap();
    assert_eq!(audio_sample_rate(&out), 16000);
}

#[test]
fn copy_codec_with_filter_is_rejected() {
    let input = audio_fixture("copy_conflict_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("copy_conflict_out.m4a").as_str())
                    .set_audio_codec("copy")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::FilterWithStreamCopy(desc))
                if desc == "aformat=sample_rates=16000"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn copy_stream_map_with_filter_is_rejected() {
    let input = audio_fixture("map_copy_conflict_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("map_copy_conflict_out.m4a").as_str())
                    .add_stream_map_with_copy("0:a")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::FilterWithStreamCopy(desc))
                if desc == "aformat=sample_rates=16000"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn video_copy_stream_map_with_audio_filter_is_accepted() {
    let input = av_fixture("map_video_copy_in.mp4");
    let out = tmp_path("map_video_copy_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .add_stream_map_with_copy("0:v")
                    .add_stream_map("0:a")
                    .set_audio_codec("aac")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build()
            .unwrap(),
        "video copied, audio filtered",
    )
    .unwrap();
    assert_eq!(audio_sample_rate(&out), 16000);
}

#[test]
fn video_filter_as_audio_filter_is_rejected() {
    let input = audio_fixture("type_mismatch_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("type_mismatch_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .set_audio_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterMediaTypeMismatch { desc, expected, .. })
                if desc == "scale=160:120" && *expected == "audio"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn unused_filter_on_video_only_output_is_rejected() {
    let input = av_fixture("unused_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unused_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .disable_audio()
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::AudioFilterUnused(desc))
                if desc == "aformat=sample_rates=16000"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn context_graph_with_audio_filter_is_rejected() {
    let input = audio_fixture("complex_conflict_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("[0:a]anull[aout]")
            .output(
                Output::from(tmp_path("complex_conflict_out.m4a").as_str())
                    .add_stream_map("[aout]")
                    .set_audio_codec("aac")
                    .set_audio_filter("aformat=sample_rates=16000"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleAndComplexFilter(desc))
                if desc == "aformat=sample_rates=16000"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn empty_audio_filter_fails_build() {
    let input = audio_fixture("empty_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("empty_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .set_audio_filter(""),
            )
            .build(),
    );
    let text = err.to_string();
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape { .. })
        ) || matches!(&err, Error::FilterGraphParse(_)),
        "empty -af must fail the build with a typed error, got: {text}"
    );
}

#[test]
fn asplit_is_rejected_as_complex_shape() {
    let input = audio_fixture("asplit_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("asplit_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .set_audio_filter("asplit"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape { desc, .. })
                if desc == "asplit"
        ),
        "unexpected error: {err:?}"
    );
}
