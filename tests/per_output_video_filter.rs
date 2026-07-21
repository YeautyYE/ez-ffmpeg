//! Per-output simple video filter (`Output::set_video_filter`, FFmpeg `-vf`).
//!
//! The implicit per-output filtergraph every simple video encode runs through
//! defaults to the passthrough `null` chain; `set_video_filter` replaces that
//! chain for one output. These tests pin:
//! - the filter actually transforms the encoded stream (dimensions),
//! - the scope is per-output (a second output stays untouched),
//! - audio is never routed through the video chain,
//! - every conflicting spelling is a typed build() error, mirroring the CLI
//!   (streamcopy, complex-graph feeds — through ANY mapping spelling —
//!   non-linear/disconnected/unreachable/non-video chains, and filters no
//!   video stream ended up consuming).

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout, VideoFrame};
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

/// 0.5s 320x240 solid-`color` video-only fixture, for tests that must tell
/// WHICH feed produced the encoded frames by looking at their content.
fn solid_video_fixture(name: &str, color: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from(format!("color=c={color}:s=320x240:r=30")).set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(500_000),
            )
            .build()
            .unwrap(),
        "solid color fixture",
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

/// 0.5s audio-only fixture (stereo AAC in m4a).
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

fn video_dimensions(path: &str) -> (i32, i32) {
    match find_video_stream_info(path).unwrap() {
        Some(StreamInfo::Video { width, height, .. }) => (width, height),
        other => panic!("expected a video stream in {path}, got {other:?}"),
    }
}

/// Mean red and blue channel values of a packed RGB24 frame, for telling a
/// solid-red feed from a solid-blue one after a lossy encode round-trip.
fn mean_red_blue(frame: &VideoFrame) -> (f64, f64) {
    assert_eq!(frame.layout(), PixelLayout::Rgb24);
    let bytes = frame.as_bytes();
    let (mut red, mut blue) = (0u64, 0u64);
    for px in bytes.chunks_exact(3) {
        red += u64::from(px[0]);
        blue += u64::from(px[2]);
    }
    let pixels = (bytes.len() / 3) as f64;
    (red as f64 / pixels, blue as f64 / pixels)
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
fn clear_video_filter_restores_passthrough() {
    let input = video_fixture("clear_in.mp4");
    let out = tmp_path("clear_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120")
                    .clear_video_filter(),
            )
            .build()
            .unwrap(),
        "cleared filter",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (320, 240));
}

#[test]
fn empty_filter_text_fails_like_cli() {
    // -vf "" parity: the CLI fails to parse an empty graph; the empty
    // description must not silently degrade to passthrough.
    let input = video_fixture("empty_vf_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("empty_vf_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(""),
            )
            .build(),
    );
    // Shape or parse error — anything typed, never a silent pass.
    let text = err.to_string();
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape { .. })
        ) || matches!(&err, Error::FilterGraphParse(_)),
        "empty -vf must fail the build with a typed error, got: {text}"
    );
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
            Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape { desc, reason })
                if desc == "split" && reason.contains("found 1 and 2")
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

// ---------------------------------------------------------------------------
// Mapping spellings must not bypass the simple/complex conflict (fftools
// binds unlabeled graph outputs before manual/automatic mapping and then
// errors in ost_get_filters).
// ---------------------------------------------------------------------------

#[test]
fn explicit_map_does_not_bypass_complex_conflict() {
    let input = video_fixture("map_bypass_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("map_bypass_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v")
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
fn disable_video_does_not_bypass_complex_conflict() {
    let input = video_fixture("disable_bypass_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("disable_bypass_out.mp4").as_str())
                    .disable_video()
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
fn multi_output_complex_conflict_hits_the_filtered_output() {
    // A second output must not become a silent escape hatch for the complex
    // graph while output #0 keeps its simple filter: fftools would bind the
    // unlabeled graph to output #0 first and conflict there.
    let input = video_fixture("multi_out_bypass_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("multi_out_bypass_a.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v")
                    .set_video_filter("scale=160:120"),
            )
            .output(Output::from(tmp_path("multi_out_bypass_b.mp4").as_str()).set_video_codec("mpeg4"))
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::SimpleAndComplexFilter(_))
        ),
        "unexpected error: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// A configured filter with no video stream consumed is a typed error,
// never a silent drop.
// ---------------------------------------------------------------------------

#[test]
fn filter_on_audio_only_input_is_rejected() {
    let input = audio_fixture("unused_audio_only_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unused_audio_only_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::VideoFilterUnused(desc))
                if desc == "scale=160:120"
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn filter_with_audio_only_maps_is_rejected() {
    let input = av_fixture("unused_audio_maps_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unused_audio_maps_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .add_stream_map("0:a")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::VideoFilterUnused(_))
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn filter_with_unmatched_optional_video_map_is_rejected() {
    // `0:v?` on an audio-only input matches nothing; the filter then has no
    // stream to bind and must surface as unused, not vanish.
    let input = audio_fixture("unused_optional_map_in.m4a");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unused_optional_map_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .add_stream_map("0:v?")
                    .add_stream_map("0:a")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::VideoFilterUnused(_))
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn filter_with_disable_video_is_rejected() {
    let input = av_fixture("unused_disable_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(tmp_path("unused_disable_out.m4a").as_str())
                    .set_audio_codec("aac")
                    .disable_video()
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(
            &err,
            Error::OpenOutput(OpenOutputError::VideoFilterUnused(_))
        ),
        "unexpected error: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Topology beyond pad counts — disconnected, unreachable and
// zero/multi-pad descriptions are all the promised typed shape error.
// ---------------------------------------------------------------------------

fn shape_reason(input: &str, filter: &str) -> String {
    let err = build_err(
        FfmpegContext::builder()
            .input(input)
            .output(
                Output::from(tmp_path("shape_probe_out.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(filter),
            )
            .build(),
    );
    match err {
        Error::OpenOutput(OpenOutputError::SimpleFilterInvalidShape { desc, reason }) => {
            assert_eq!(desc, filter);
            reason
        }
        other => panic!("expected SimpleFilterInvalidShape for {filter:?}, got: {other:?}"),
    }
}

#[test]
fn disconnected_graph_is_rejected() {
    // Probes as 1 open input + 1 open output, but the pushed stream drains
    // into the sink while the encoder consumes the unrelated source forever.
    let input = video_fixture("shape_disconnected_in.mp4");
    let reason = shape_reason(&input, "nullsink;color=c=black:s=64x64");
    assert!(reason.contains("disconnected sub-graphs"), "reason: {reason}");
}

#[test]
fn connected_but_unreachable_graph_is_rejected() {
    let input = video_fixture("shape_unreachable_in.mp4");
    let reason = shape_reason(
        &input,
        "color=c=black:s=64x64,split[o][aux];[aux][i]overlay,nullsink",
    );
    assert!(reason.contains("no directed path"), "reason: {reason}");
}

#[test]
fn streamselect_dropping_the_input_is_accepted_like_cli() {
    // The applied map=0 relays only the color feed and currently drops [in],
    // yet the graph must be ACCEPTED: streamselect's map is a runtime
    // command (sendcmd / the send-command API can reroute [in] to the output
    // mid-stream), so the applied selection proves nothing about where the
    // input can flow — and ffmpeg (verified against 7.1.3) accepts and runs
    // this exact description, encoding the generator feed. The reachability
    // gate is structural (is [in] wired into the graph that feeds the
    // output?), not a promise that filters keep the frames. The unselected
    // input is 320x240 while the selected color feed is 64x64 — the output
    // dimensions prove the encoder consumed the generator, like the CLI.
    let input = video_fixture("sselect_drop_in.mp4");
    let out = tmp_path("sselect_drop_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(
                        "color=c=red:s=64x64:r=30:d=0.5[bg];\
                         [bg][in]streamselect=inputs=2:map=0",
                    ),
            )
            .build()
            .unwrap(),
        "streamselect dropping the input",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (64, 64));
}

#[test]
fn sendcmd_commanded_streamselect_switches_content() {
    // The runtime-commandable case the structural gate exists for: the graph
    // starts on map=0 (the red generator feed) and a sendcmd entry rewrites
    // the map to 1 at t=0.2s, after which streamselect relays the decoded
    // input — ffmpeg 7.1.3 runs this with a "Success" command reply and
    // switches the encoded content. A probe that pruned routing by the
    // applied map=0 would reject this description even though the input
    // demonstrably reaches the encoder at runtime. Both feeds share one size
    // on purpose (the output link's dimensions are negotiated once and a
    // mid-stream switch must not change them), so the feeds are told apart
    // by content instead: the decoded input is solid blue, the generator
    // solid red, and the decoded output must show red frames before the
    // command and blue frames after it — a dropped or ignored command would
    // leave every frame red.
    let input = solid_video_fixture("sselect_cmd_in.mp4", "blue");
    let out = tmp_path("sselect_cmd_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(
                        "sendcmd=c='0.2 streamselect@sel map 1'[cmd];\
                         color=c=red:s=320x240:r=30:d=0.5[bg];\
                         [bg][cmd]streamselect@sel=inputs=2:map=0",
                    ),
            )
            .build()
            .unwrap(),
        "sendcmd-commanded streamselect",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (320, 240));

    // Decode the encoded output and classify frames on both sides of the
    // command. sendcmd fires on the first frame with pts >= 0.2s and the
    // remap takes effect on the following framesync event, so the exact
    // boundary frame's side is scheduler detail — classify only frames
    // safely clear of it (<= 0.1s and >= 0.3s at 30fps).
    let frames = FrameExtractor::new(out.as_str())
        .pixel(PixelLayout::Rgb24)
        .collect_frames()
        .unwrap();
    let (mut red_before, mut blue_after) = (0usize, 0usize);
    for frame in &frames {
        let pts = frame.pts_us().expect("exported frame has a pts");
        let (r, b) = mean_red_blue(frame);
        if pts <= 100_000 {
            assert!(
                r > b + 64.0,
                "frame at {pts}us predates the command and must come from \
                 the red generator feed, got mean r={r:.0} b={b:.0}"
            );
            red_before += 1;
        } else if pts >= 300_000 {
            assert!(
                b > r + 64.0,
                "frame at {pts}us postdates the command and must come from \
                 the blue decoded input, got mean r={r:.0} b={b:.0}"
            );
            blue_after += 1;
        }
    }
    assert!(
        red_before >= 2,
        "output must cover frames before the 0.2s command, saw {red_before}"
    );
    assert!(
        blue_after >= 2,
        "output must extend past the 0.2s command, saw {blue_after}"
    );
}

#[test]
fn streamselect_relaying_the_input_is_accepted() {
    // Positive control for the pad-granular walk: map=1 selects the open
    // input pad, so the decoded stream IS the output. The unselected color
    // feed is 64x64 while the input is 320x240 — the output dimensions prove
    // which input pad the encoder consumed.
    let input = video_fixture("sselect_relay_in.mp4");
    let out = tmp_path("sselect_relay_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(
                        "color=c=red:s=64x64:r=30:d=0.5[alt];\
                         [alt][in]streamselect=inputs=2:map=1",
                    ),
            )
            .build()
            .unwrap(),
        "streamselect relaying the input",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (320, 240));
}

#[test]
fn overlay_merging_the_input_is_accepted() {
    // A multi-input filter that MERGES its inputs must keep passing when the
    // open input is a secondary pad: frames entering overlay's pad 1 are
    // composited into the output, so the input genuinely influences it.
    let input = video_fixture("overlay_merge_in.mp4");
    let out = tmp_path("overlay_merge_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(
                        "color=c=black:s=320x240:r=30:d=0.5[bg];\
                         [bg][in]overlay=shortest=1",
                    ),
            )
            .build()
            .unwrap(),
        "overlay merging the input",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (320, 240));
}

#[test]
fn concat_appending_the_input_is_accepted() {
    // concat splices every input into the output timeline, so the open input
    // on pad 1 influences the output even though pad 0 is generator-fed.
    let input = video_fixture("concat_append_in.mp4");
    let out = tmp_path("concat_append_out.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter(
                        "color=c=blue:s=320x240:r=30:d=0.2[pre];\
                         [pre][in]concat=n=2:v=1:a=0",
                    ),
            )
            .build()
            .unwrap(),
        "concat appending the input",
    )
    .unwrap();
    assert_eq!(video_dimensions(&out), (320, 240));
}

#[test]
fn zero_input_graph_is_rejected() {
    let input = video_fixture("shape_zero_in_in.mp4");
    let reason = shape_reason(&input, "color=c=red:s=64x64");
    assert!(reason.contains("found 0 and 1"), "reason: {reason}");
}

#[test]
fn zero_output_graph_is_rejected() {
    let input = video_fixture("shape_zero_out_in.mp4");
    let reason = shape_reason(&input, "nullsink");
    assert!(reason.contains("found 1 and 0"), "reason: {reason}");
}

#[test]
fn multi_input_graph_is_rejected() {
    let input = video_fixture("shape_multi_in_in.mp4");
    let reason = shape_reason(&input, "overlay");
    assert!(reason.contains("found 2 and 1"), "reason: {reason}");
}

// ---------------------------------------------------------------------------
// VideoWriter honors the Output-level chain.
// ---------------------------------------------------------------------------

#[test]
fn video_writer_honors_output_video_filter() {
    use ez_ffmpeg::VideoWriter;

    let out = tmp_path("writer_filter_out.mp4");
    let mut writer = VideoWriter::builder(64, 48)
        .open(
            ez_ffmpeg::Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_video_filter("scale=32:24"),
        )
        .unwrap();
    let frame = vec![128u8; 64 * 48 * 4];
    for _ in 0..5 {
        writer.write(&frame).unwrap();
    }
    writer.finish().unwrap();
    assert_eq!(video_dimensions(&out), (32, 24));
}

#[test]
fn video_writer_rejects_conflicting_filter_descriptions() {
    use ez_ffmpeg::VideoWriter;

    let err = VideoWriter::builder(64, 48)
        .filter_desc("hue=s=0")
        .open(
            ez_ffmpeg::Output::from(tmp_path("writer_conflict_out.mp4").as_str())
                .set_video_codec("mpeg4")
                .set_video_filter("scale=32:24"),
        )
        .map(|_| ())
        .unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("exactly one place"),
        "expected the conflicting-filter error, got: {text}"
    );
}

// ---------------------------------------------------------------------------
// Per-output assignment semantics of the simple/complex conflict — an
// output the unlabeled graph does NOT land on keeps its simple filter,
// exactly like FFmpeg 7.1; the output that receives the graph still
// conflicts. Both orders covered.
// ---------------------------------------------------------------------------

#[test]
fn unlabeled_graph_on_earlier_output_frees_a_later_simple_filter() {
    // FFmpeg 7.1-legal layout: output #0 takes the unlabeled complex graph
    // (bound before its explicit map, fftools create_streams order) and has
    // no simple filter; output #1 runs its own -vf. This must SUCCEED.
    let input = video_fixture("legal_layout_in.mp4");
    let graphed = tmp_path("legal_layout_graphed.mp4");
    let filtered = tmp_path("legal_layout_filtered.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(graphed.as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v"),
            )
            .output(
                Output::from(filtered.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "unlabeled graph to output 0, simple filter on output 1",
    )
    .unwrap();
    // Output #0 carries BOTH the complex-graph stream and the mapped stream
    // (fftools binds unlabeled outputs in addition to explicit maps).
    let video_streams = ez_ffmpeg::stream_info::find_all_stream_infos(&graphed)
        .unwrap()
        .into_iter()
        .filter(|info| matches!(info, StreamInfo::Video { .. }))
        .count();
    assert_eq!(video_streams, 2, "output 0 must carry the graph and the mapped stream");
    // Output #1's own filter applied.
    assert_eq!(video_dimensions(&filtered), (160, 120));
}

#[test]
fn filtered_output_receiving_the_graph_still_conflicts() {
    // Order flipped: the FILTERED output comes first, so the unlabeled graph
    // lands on it — fftools' ost_get_filters conflict, and ours.
    let input = video_fixture("legal_layout_flip_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("legal_flip_a.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .output(
                Output::from(tmp_path("legal_flip_b.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v"),
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

// ---------------------------------------------------------------------------
// Streamcopy on the fftools-order candidate output: FFmpeg 7.1 fails at the
// FIRST output the unlabeled graph binds to (ffmpeg_mux_init.c ost_add,
// "Filtering and streamcopy cannot be used together") — the graph must never
// slide past a copy output onto a later encoding output.
// ---------------------------------------------------------------------------

#[test]
fn copy_first_output_cannot_defer_the_graph_to_a_later_encoder() {
    // Output #0 is -c:v copy, so binding the unlabeled graph there is the
    // CLI's fatal filtering/streamcopy conflict. Skipping the copy output
    // and binding the graph to output #1 instead would build a job the CLI
    // rejects (and hand output #2 its simple filter as if the layout were
    // legal).
    let input = video_fixture("copy_candidate_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("copy_candidate_a.mp4").as_str()).set_video_codec("copy"),
            )
            .output(
                Output::from(tmp_path("copy_candidate_b.mp4").as_str()).set_video_codec("mpeg4"),
            )
            .output(
                Output::from(tmp_path("copy_candidate_c.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidArgument)),
        "unexpected error: {err:?}"
    );
}

#[test]
fn copy_candidate_error_precedes_the_filtered_output_conflict() {
    // With -c:v copy on output #0 and the simple filter on output #1, the
    // CLI still dies at output #0 (create_streams handles output files in
    // order): the streamcopy conflict wins, not the simple/complex conflict
    // the graph would hit on the later filtered output.
    let input = video_fixture("copy_candidate_order_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("copy_candidate_order_a.mp4").as_str())
                    .set_video_codec("copy"),
            )
            .output(
                Output::from(tmp_path("copy_candidate_order_b.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build(),
    );
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidArgument)),
        "unexpected error: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Disable flags must not derail fftools-order assignment — FFmpeg 7.1 puts
// an unlabeled graph on a -vn output and still filters the next output.
// Legacy (no set_video_filter) ordering stays pinned bit for bit.
// ---------------------------------------------------------------------------

#[test]
fn disabled_video_output_still_receives_the_graph_in_feature_jobs() {
    // output #0: disable_video + audio map — fftools assigns the unlabeled
    // complex-video graph here DESPITE -vn; output #1 keeps its own -vf.
    let input = av_fixture("disable_assign_in.mp4");
    let graphed = tmp_path("disable_assign_graphed.mp4");
    let filtered = tmp_path("disable_assign_filtered.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(graphed.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .disable_video()
                    .add_stream_map("0:a"),
            )
            .output(
                Output::from(filtered.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .build()
            .unwrap(),
        "disable_video output receives the graph, next output filters",
    )
    .unwrap();
    // Output #0 carries the graph's video stream (despite disable_video)
    // plus the mapped audio.
    let infos = ez_ffmpeg::stream_info::find_all_stream_infos(&graphed).unwrap();
    let videos = infos
        .iter()
        .filter(|info| matches!(info, StreamInfo::Video { .. }))
        .count();
    let audios = infos
        .iter()
        .filter(|info| matches!(info, StreamInfo::Audio { .. }))
        .count();
    assert_eq!(
        (videos, audios),
        (1, 1),
        "output 0 must carry the graph video plus its mapped audio"
    );
    assert_eq!(video_dimensions(&filtered), (160, 120));
}

#[test]
fn disabled_video_mirror_order_still_conflicts_on_the_filtered_output() {
    // Mirror: the FILTERED output comes first, so the graph lands on it and
    // conflicts — the disable_video output later changes nothing.
    let input = av_fixture("disable_assign_flip_in.mp4");
    let err = build_err(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(tmp_path("disable_flip_a.mp4").as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("scale=160:120"),
            )
            .output(
                Output::from(tmp_path("disable_flip_b.m4a").as_str())
                    .set_audio_codec("aac")
                    .disable_video()
                    .add_stream_map("0:a"),
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
fn legacy_jobs_keep_the_mapless_only_unlabeled_binding_order() {
    // Pin the no-feature ordering bit for bit: without set_video_filter
    // anywhere, an output WITH maps is skipped by unlabeled assignment and
    // the graph lands on the next map-less output (the pre-existing crate
    // behavior every released caller relies on).
    let input = video_fixture("legacy_order_in.mp4");
    let mapped = tmp_path("legacy_order_mapped.mp4");
    let graphed = tmp_path("legacy_order_graphed.mp4");
    run(
        FfmpegContext::builder()
            .input(input.as_str())
            .filter_desc("hue=s=0")
            .output(
                Output::from(mapped.as_str())
                    .set_video_codec("mpeg4")
                    .add_stream_map("0:v"),
            )
            .output(Output::from(graphed.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "legacy unlabeled binding order",
    )
    .unwrap();
    let count_videos = |path: &str| {
        ez_ffmpeg::stream_info::find_all_stream_infos(path)
            .unwrap()
            .into_iter()
            .filter(|info| matches!(info, StreamInfo::Video { .. }))
            .count()
    };
    // Legacy: the mapped output gets ONLY its map; the map-less output gets
    // the graph.
    assert_eq!(count_videos(&mapped), 1, "mapped output must not receive the graph");
    assert_eq!(count_videos(&graphed), 1, "map-less output receives the graph");
}
