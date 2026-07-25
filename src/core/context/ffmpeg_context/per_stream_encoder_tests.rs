//! Runtime end-to-end coverage for per-map encoder selection
//! (`StreamMap::codec` / `StreamMap::codec_opt`).
//!
//! Pins the historical blind spot: two same-type tracks of ONE input mapped
//! into ONE output with different encoders (the closest prior coverage was
//! cross-input maps and a parser-only indexed-map case). Also pins the
//! filter-label path, the per-type/per-map precedence chain, and the
//! build-time conflict gates (copy × codec, negative map × codec,
//! implicit-copy × codec options, packet-sink `flags`).

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::{Output, StreamMap};
use crate::core::stream_info::{find_all_stream_infos, StreamInfo};
use crate::error::{Error, OpenOutputError, PacketSinkError};
use std::sync::OnceLock;

fn tmp_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_per_map_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn tmp_path(name: &str) -> String {
    tmp_dir().join(name).to_string_lossy().into_owned()
}

fn run(context: FfmpegContext, scenario: &str) {
    let scheduler = context.start().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(std::time::Duration::from_secs(120)) {
        Ok(result) => result.unwrap_or_else(|e| panic!("{scenario} failed: {e}")),
        Err(_) => panic!("{scenario} did not finish within 120s"),
    }
}

/// One media file carrying TWO audio tracks (mono sine + stereo silence) —
/// the same-input/same-type fixture. Built once per test binary
/// (`OnceLock`), so parallel tests never race on the file.
fn two_audio_fixture() -> String {
    static FIXTURE: OnceLock<String> = OnceLock::new();
    FIXTURE
        .get_or_init(|| {
            let path = tmp_path("two_audio_fixture.mp4");
            run(
                FfmpegContext::builder()
                    .input(
                        Input::from("sine=frequency=440:sample_rate=44100:duration=1")
                            .set_format("lavfi"),
                    )
                    .input(
                        Input::from("anullsrc=channel_layout=stereo:sample_rate=48000")
                            .set_format("lavfi"),
                    )
                    .output(
                        Output::from(path.as_str())
                            .set_audio_codec("aac")
                            .set_recording_time_us(1_000_000)
                            .add_stream_map("0:a")
                            .add_stream_map("1:a"),
                    )
                    .build()
                    .unwrap(),
                "two-audio fixture",
            );
            path
        })
        .clone()
}

fn audio_codec_names(path: &str) -> Vec<String> {
    find_all_stream_infos(path)
        .unwrap()
        .into_iter()
        .filter_map(|info| match info {
            StreamInfo::Audio { codec_name, .. } => Some(codec_name),
            _ => None,
        })
        .collect()
}

fn video_codec_names(path: &str) -> Vec<String> {
    find_all_stream_infos(path)
        .unwrap()
        .into_iter()
        .filter_map(|info| match info {
            StreamInfo::Video { codec_name, .. } => Some(codec_name),
            _ => None,
        })
        .collect()
}

/// The core capability: `-map 0:a:0 -c:a:0 aac -b:a:0 96k -map 0:a:1
/// -c:a:1 flac` — two same-type tracks of one input, one output, two
/// different encoders.
#[test]
fn two_same_type_streams_take_their_own_per_map_encoders() {
    let fixture = two_audio_fixture();
    let out = tmp_path("dual_audio_per_map.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map(StreamMap::new("0:a:0").codec("aac").codec_opt("b", "96k"))
                    .add_stream_map(StreamMap::new("0:a:1").codec("flac")),
            )
            .build()
            .unwrap(),
        "dual-audio per-map encode",
    );
    assert_eq!(audio_codec_names(&out), vec!["aac", "flac"]);
}

/// Precedence: a plain string map keeps the per-type `set_audio_codec`
/// value (and pins the `&str` source-compat path at runtime); the
/// `StreamMap` with a codec overrides it for its own streams only.
#[test]
fn per_map_codec_overrides_the_per_type_default_per_map_only() {
    let fixture = two_audio_fixture();
    let out = tmp_path("precedence_per_map.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("flac")
                    .add_stream_map("0:a:0")
                    .add_stream_map(StreamMap::new("0:a:1").codec("aac")),
            )
            .build()
            .unwrap(),
        "per-type default with per-map override",
    );
    assert_eq!(audio_codec_names(&out), vec!["flac", "aac"]);
}

/// The filter-label path (owner's #53 shape): one video split into two
/// graph outputs, each mapped with its own encoder.
#[test]
fn filter_label_maps_carry_per_map_encoders() {
    let out = tmp_path("split_video_per_map.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=128x72:rate=15:duration=1").set_format("lavfi"))
            .filter_desc("[0:v]split=2[v0][v1]")
            .output(
                Output::from(out.as_str())
                    .add_stream_map(StreamMap::new("[v0]").codec("mpeg4"))
                    .add_stream_map(StreamMap::new("[v1]").codec("mjpeg")),
            )
            .build()
            .unwrap(),
        "split-video per-map encode",
    );
    assert_eq!(video_codec_names(&out), vec!["mpeg4", "mjpeg"]);
}

/// Copy × per-map codec is a typed build() error, not a panic and not a
/// silent drop.
#[test]
fn copy_map_with_per_map_codec_fails_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(
            Output::from(tmp_path("never_copy_conflict.mkv").as_str())
                .add_stream_map_with_copy(StreamMap::new("0:a").codec("aac")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::StreamMapCopyConflict { spec, .. })) => {
            assert_eq!(spec, "0:a");
        }
        other => panic!(
            "expected StreamMapCopyConflict, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

/// A negative (disabling) map cannot carry a per-map codec: the request
/// could never take effect, so it is rejected instead of silently dropped.
#[test]
fn negative_map_with_per_map_codec_fails_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(
            Output::from(tmp_path("never_negative_map.mkv").as_str())
                .add_stream_map("0:a")
                .add_stream_map(StreamMap::new("-0:a").codec("aac")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::InvalidOption(msg))) => {
            assert!(msg.contains("negative"), "got: {msg}");
        }
        other => panic!(
            "expected InvalidOption for the negative map, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

/// Per-map codec options on a map that RESOLVES to stream copy (per-type
/// codec "copy", no per-map codec) fail loud instead of being silently
/// swallowed by the copy path.
#[test]
fn per_map_opts_on_an_implicit_copy_resolution_fail_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(
            Output::from(tmp_path("never_implicit_copy.mkv").as_str())
                .set_audio_codec("copy")
                .add_stream_map(StreamMap::new("0:a").codec_opt("b", "96k")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::InvalidOption(msg))) => {
            assert!(msg.contains("stream copy"), "got: {msg}");
        }
        other => panic!(
            "expected InvalidOption for implicit copy, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

/// The packet-sink global-header policy gate covers the per-map table: a
/// per-map `flags` override must be rejected exactly like the output-level
/// one, or it would clear AV_CODEC_FLAG_GLOBAL_HEADER behind the
/// validation's back.
#[test]
fn packet_sink_rejects_a_per_map_flags_override() {
    use crate::core::packet_sink::PacketSink;
    let result = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=128x72:rate=15:duration=1").set_format("lavfi"))
        .output(Output::new_by_packet_sink(PacketSink::discard()).add_stream_map(
            StreamMap::new("0:v")
                .codec("libx264")
                .codec_opt("flags", "-global_header"),
        ))
        .build();
    match result {
        Err(Error::PacketSink(PacketSinkError::UnsupportedOption(option))) => {
            assert!(option.contains("flags"), "got option {option}");
        }
        other => panic!(
            "expected the flags rejection, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

// ---------------------------------------------------------------------------
// Round-2 gate: copy-flag / negative-map cross-boundary corrections.
// ---------------------------------------------------------------------------

/// Finding 1 — a labeled filter map cannot be stream-copied: it carries no
/// source packets. `codec("copy")` on a filter-label map must fail at
/// build() instead of silently re-encoding.
#[test]
fn filter_label_map_with_copy_codec_fails_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=128x72:rate=15:duration=1").set_format("lavfi"))
        .filter_desc("[0:v]copy[v0]")
        .output(
            Output::from(tmp_path("never_filter_copy.mkv").as_str())
                .add_stream_map(StreamMap::new("[v0]").codec("copy")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::StreamMapCopyConflict { spec, .. })) => {
            assert_eq!(spec, "[v0]");
        }
        other => panic!(
            "expected StreamMapCopyConflict for the filter-label copy, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

/// Finding 2 (the real regression) — a copy stream map must NOT consult the
/// per-type encoder. An unavailable per-type audio encoder must not fail a
/// map that only asks to copy the source audio: the copy is the more
/// specific request and takes the copy path directly.
#[test]
fn copy_map_succeeds_even_when_the_per_type_encoder_is_unavailable() {
    let fixture = two_audio_fixture();
    let out = tmp_path("copy_over_unavailable_encoder.mkv");
    // `no_such_encoder_xyz` is not a real encoder; before the fix,
    // choose_encoder ran first and aborted this copy map with
    // EncoderUnavailable. After the fix the copy path is taken and the
    // source AAC track is passed through untouched.
    run(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("no_such_encoder_xyz")
                    .add_stream_map_with_copy("0:a:0"),
            )
            .build()
            .unwrap(),
        "copy map over an unavailable per-type encoder",
    );
    assert_eq!(audio_codec_names(&out), vec!["aac"]);
}

/// Finding 3 — `StreamMap::new("-0:a").codec("copy")` must not slip past the
/// negative-map guard. `codec("copy")` normalizes to the copy flag, so the
/// guard checks the ORIGINAL request; a negative map carries no encoder
/// intent of any kind.
#[test]
fn negative_map_with_copy_codec_fails_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(
            Output::from(tmp_path("never_negative_copy.mkv").as_str())
                .add_stream_map("0:a")
                .add_stream_map(StreamMap::new("-0:a").codec("copy")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::InvalidOption(msg))) => {
            assert!(msg.contains("negative"), "got: {msg}");
        }
        other => panic!(
            "expected InvalidOption for the negative copy map, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}

/// Finding 4 — a negative FILTER-LABEL map (`"-[v]"`) must be caught by the
/// same guard as `"-0:a"`; its per-map codec/options must not ride into a
/// positive label mapping.
#[test]
fn negative_filter_label_map_with_per_map_codec_fails_at_build() {
    let result = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=128x72:rate=15:duration=1").set_format("lavfi"))
        .filter_desc("[0:v]copy[v]")
        .output(
            Output::from(tmp_path("never_negative_label.mkv").as_str())
                .add_stream_map("[v]")
                .add_stream_map(StreamMap::new("-[v]").codec("mpeg4")),
        )
        .build();
    match result {
        Err(Error::OpenOutput(OpenOutputError::InvalidOption(msg))) => {
            assert!(msg.contains("negative"), "got: {msg}");
        }
        other => panic!(
            "expected InvalidOption for the negative filter-label map, got {:?}",
            other.map(|_| "a built context")
        ),
    }
}
