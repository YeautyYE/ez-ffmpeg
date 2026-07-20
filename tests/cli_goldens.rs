//! Semantic goldens for the six verified CLI-compat shapes (V1–V6).
//!
//! Each golden runs the SAME command twice — through `cli::from_cli_args`
//! (in-process) and through the real ffmpeg CLI — and asserts the outputs
//! agree semantically: stream count and identity, codec names, dimensions,
//! sample rate and channel layout, duration within tolerance, overwrite
//! behavior; V4 additionally documents byte parity (same libavcodec build,
//! one mjpeg frame); V6 uses the HLS-specific oracle (playlist tags, segment
//! topology and naming, per-segment durations, ENDLIST).
//!
//! These tests earn the `verified` status recorded in the compatibility
//! manifest (`VERIFIED_SHAPES` names each test); without them the shapes
//! would have to be demoted to emit-only.
//!
//! Reference-binary resolution follows the `color_goldens::cli_parity_gate`
//! idiom: `EZ_FFMPEG_CLI=<path>` is the STRICT lane (any failure fails the
//! test); with the variable unset, `ffmpeg` from PATH is probed and exactly
//! two conditions skip with a stderr note — no binary, or a libavcodec/
//! libavformat major.minor mismatch with the linked build (semantic parity
//! is only defined against the same library line).

#![cfg(feature = "cli")]

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::cli::from_cli_args;
use ez_ffmpeg::container_info::get_duration_us;
use ez_ffmpeg::stream_info::{find_all_stream_infos, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::process::Command;

const DURATION_TOLERANCE_US: i64 = 200_000;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_cli_goldens", name)
}

fn run(context: FfmpegContext, scenario: &str) {
    wait_with_watchdog(context.start().unwrap(), 120, scenario).unwrap();
}

// ---------------------------------------------------------------------------
// Reference-binary gate.
// ---------------------------------------------------------------------------

enum CliGate {
    Run { bin: String },
    Skip(String),
}

fn cli_gate() -> CliGate {
    use std::io::ErrorKind;
    let pinned = std::env::var("EZ_FFMPEG_CLI").ok();
    let strict = pinned.is_some();
    let bin = pinned.unwrap_or_else(|| "ffmpeg".to_string());
    let out = match Command::new(&bin).arg("-version").output() {
        Err(e) if e.kind() == ErrorKind::NotFound && !strict => {
            return CliGate::Skip(format!("no `{bin}` on PATH"));
        }
        Err(e) => panic!("ffmpeg CLI probe `{bin} -version` failed: {e}"),
        Ok(o) if !o.status.success() => panic!("`{bin} -version` exited with {}", o.status),
        Ok(o) => o,
    };
    let banner = String::from_utf8_lossy(&out.stdout).into_owned();
    let cli_avcodec = parse_lib_version(&banner, "libavcodec")
        .unwrap_or_else(|| panic!("no libavcodec line in `{bin} -version` output"));
    let cli_avformat = parse_lib_version(&banner, "libavformat")
        .unwrap_or_else(|| panic!("no libavformat line in `{bin} -version` output"));
    let ours_avcodec = major_minor(unsafe { ffmpeg_sys_next::avcodec_version() });
    let ours_avformat = major_minor(unsafe { ffmpeg_sys_next::avformat_version() });
    if cli_avcodec != ours_avcodec || cli_avformat != ours_avformat {
        let msg = format!(
            "`{bin}` links avcodec {}.{} / avformat {}.{}, the crate links {}.{} / {}.{} — \
             semantic parity is only defined on the same library line",
            cli_avcodec.0,
            cli_avcodec.1,
            cli_avformat.0,
            cli_avformat.1,
            ours_avcodec.0,
            ours_avcodec.1,
            ours_avformat.0,
            ours_avformat.1
        );
        if strict {
            panic!("{msg} (point EZ_FFMPEG_CLI at a matching binary)");
        }
        return CliGate::Skip(msg);
    }
    CliGate::Run { bin }
}

fn major_minor(v: u32) -> (u32, u32) {
    (v >> 16, (v >> 8) & 0xff)
}

/// Parses the runtime triple of a `lib*` line in `ffmpeg -version` output and
/// returns (major, minor). The triple RIGHT of `/` is what the binary loads.
fn parse_lib_version(banner: &str, lib: &str) -> Option<(u32, u32)> {
    let line = banner.lines().find(|l| l.trim_start().starts_with(lib))?;
    let runtime = line.rsplit('/').next()?;
    let digits: String = runtime
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut it = digits.split('.').filter(|s| !s.is_empty());
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

/// Runs the reference CLI on `args`; any failure is a harness defect.
fn run_reference(bin: &str, args: &[String]) {
    let out = Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `{bin}`: {e}"));
    assert!(
        out.status.success(),
        "reference `{bin} {}` failed:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
}

// ---------------------------------------------------------------------------
// Fixtures (built with the crate itself; lavfi test sources).
// ---------------------------------------------------------------------------

/// 16s 320x240@30 A/V fixture (mpeg4 + stereo AAC), long enough for several
/// HLS segments.
fn av_fixture(name: &str) -> String {
    let path = tmp_path(name);
    if std::path::Path::new(&path).exists() {
        return path;
    }
    run(
        FfmpegContext::builder()
            .input(
                Input::from("testsrc2=size=320x240:rate=30:duration=16").set_format("lavfi"),
            )
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=16")
                    .set_format("lavfi"),
            )
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac"),
            )
            .build()
            .unwrap(),
        "av fixture",
    );
    path
}

/// Multi-audio fixture proving stream identity: audio #0 is MONO 44100
/// (sine), audio #1 is STEREO 48000 (silence). The CLI's best-stream rule
/// ("most channels") must select #1 — the first-vs-best distinction is
/// observable in the output's channel count and sample rate.
fn multi_audio_fixture(name: &str) -> String {
    let path = tmp_path(name);
    if std::path::Path::new(&path).exists() {
        return path;
    }
    run(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30:duration=4").set_format("lavfi"))
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=4")
                    .set_format("lavfi"),
            )
            .input(
                Input::from("anullsrc=channel_layout=stereo:sample_rate=48000")
                    .set_format("lavfi"),
            )
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_recording_time_us(4_000_000)
                    .add_stream_map("0:v")
                    .add_stream_map("1:a")
                    .add_stream_map("2:a"),
            )
            .build()
            .unwrap(),
        "multi-audio fixture",
    );
    let infos = find_all_stream_infos(&path).unwrap();
    let channel_counts: Vec<i32> = infos
        .iter()
        .filter_map(|info| match info {
            StreamInfo::Audio { nb_channels, .. } => Some(*nb_channels),
            _ => None,
        })
        .collect();
    assert_eq!(
        channel_counts,
        vec![1, 2],
        "fixture must carry mono-then-stereo audio for the identity check"
    );
    path
}

// ---------------------------------------------------------------------------
// Comparison helpers.
// ---------------------------------------------------------------------------

struct StreamSummary {
    kind: &'static str,
    codec: String,
    width: i32,
    height: i32,
    sample_rate: i32,
    channels: i32,
}

fn summarize(path: &str) -> Vec<StreamSummary> {
    find_all_stream_infos(path)
        .unwrap_or_else(|e| panic!("probing {path} failed: {e}"))
        .into_iter()
        .map(|info| match info {
            StreamInfo::Video {
                codec_name,
                width,
                height,
                ..
            } => StreamSummary {
                kind: "video",
                codec: codec_name,
                width,
                height,
                sample_rate: 0,
                channels: 0,
            },
            StreamInfo::Audio {
                codec_name,
                sample_rate,
                nb_channels,
                ..
            } => StreamSummary {
                kind: "audio",
                codec: codec_name,
                width: 0,
                height: 0,
                sample_rate,
                channels: nb_channels,
            },
            other => StreamSummary {
                kind: "other",
                codec: format!("{other:?}"),
                width: 0,
                height: 0,
                sample_rate: 0,
                channels: 0,
            },
        })
        .collect()
}

/// The shared semantic oracle: stream identity, codecs, geometry, layout,
/// duration tolerance.
fn assert_same_media(ours: &str, theirs: &str) {
    let a = summarize(ours);
    let b = summarize(theirs);
    assert_eq!(a.len(), b.len(), "stream count differs ({ours} vs {theirs})");
    for (i, (x, y)) in a.iter().zip(&b).enumerate() {
        assert_eq!(x.kind, y.kind, "stream {i} type differs");
        assert_eq!(x.codec, y.codec, "stream {i} codec differs");
        assert_eq!((x.width, x.height), (y.width, y.height), "stream {i} geometry differs");
        assert_eq!(
            (x.sample_rate, x.channels),
            (y.sample_rate, y.channels),
            "stream {i} audio layout differs"
        );
    }
    let da = get_duration_us(ours).unwrap();
    let db = get_duration_us(theirs).unwrap();
    assert!(
        (da - db).abs() <= DURATION_TOLERANCE_US,
        "duration differs beyond tolerance: {da}us vs {db}us"
    );
}

// ---------------------------------------------------------------------------
// The six goldens.
// ---------------------------------------------------------------------------

#[test]
fn golden_v1_transcode() {
    let fixture = av_fixture("golden_v1_in.mp4");
    // The verified shape pins the .mp4 container; the substituted output
    // path must keep the extension.
    let out_ours = tmp_path("golden_v1_ours.mp4");
    let out_theirs = tmp_path("golden_v1_theirs.mp4");
    let args = |out: &str| -> Vec<String> {
        [
            "-i", &fixture, "-c:v", "libx264", "-crf", "23", "-preset", "fast", "-c:a", "aac",
            "-y", out,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    };

    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v1_transcode: {reason}");
            return;
        }
    };

    // Overwrite semantics: -y must truncate a pre-existing garbage file.
    std::fs::write(&out_ours, b"garbage bytes, not media").unwrap();

    let context = from_cli_args(&args(&out_ours)).expect("V1 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v1 ours").unwrap();
    run_reference(&bin, &args(&out_theirs));

    assert_same_media(&out_ours, &out_theirs);
    let ours = summarize(&out_ours);
    assert_eq!(ours.len(), 2);
    assert_eq!(ours[0].codec, "h264");
    assert_eq!(ours[1].codec, "aac");
}

#[test]
fn golden_v2_clip() {
    let fixture = av_fixture("golden_v2_in.mp4");
    let out_ours = tmp_path("golden_v2_ours.mp4");
    let out_theirs = tmp_path("golden_v2_theirs.mp4");
    let args = |out: &str| -> Vec<String> {
        [
            "-ss", "10", "-i", &fixture, "-t", "20", "-c:v", "libx264", "-crf", "23", "-c:a",
            "aac", "-y", out,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    };
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v2_clip: {reason}");
            return;
        }
    };
    let context = from_cli_args(&args(&out_ours)).expect("V2 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v2 ours").unwrap();
    run_reference(&bin, &args(&out_theirs));

    assert_same_media(&out_ours, &out_theirs);
    // -ss 10 on a 16s fixture with -t 20: ~6s remain. Both engines must
    // agree (checked above); pin the absolute window too.
    let duration = get_duration_us(&out_ours).unwrap();
    assert!(
        (5_500_000..=6_500_000).contains(&duration),
        "clip duration {duration}us outside the expected ~6s window"
    );
}

#[test]
fn golden_v3_audio_extract() {
    // Fixture A (crate-built): audio #0 mono 44100 carries the container's
    // DEFAULT disposition, audio #1 stereo 48000 does not. fftools scores
    // candidates channels + 5M*DEFAULT (+100M*NEW_PACKETS) — map_auto_audio,
    // identical in 7.1 and 8.1 — so the DEFAULT bonus makes the mono track
    // the correct selection for BOTH engines.
    let fixture = multi_audio_fixture("golden_v3_in.mp4");
    let out_ours = tmp_path("golden_v3_ours.m4a");
    let out_theirs = tmp_path("golden_v3_theirs.m4a");
    let args = |input: &str, out: &str| -> Vec<String> {
        ["-i", input, "-vn", "-c:a", "aac", "-b:a", "192k", "-y", out]
            .iter()
            .map(|s| s.to_string())
            .collect()
    };
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v3_audio_extract: {reason}");
            return;
        }
    };
    let context =
        from_cli_args(&args(&fixture, &out_ours)).expect("V3 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v3 ours").unwrap();
    run_reference(&bin, &args(&fixture, &out_theirs));

    assert_same_media(&out_ours, &out_theirs);
    let ours = summarize(&out_ours);
    assert_eq!(ours.len(), 1, "-vn output must be audio-only");
    assert_eq!(ours[0].codec, "aac");
    assert_eq!(
        (ours[0].channels, ours[0].sample_rate),
        (1, 44100),
        "the DEFAULT-disposition bonus must select the mono track on fixture A"
    );

    // Fixture B (reference-CLI-built, forced dispositions): audio #0 is
    // STEREO without DEFAULT, audio #1 is MONO with DEFAULT. The correct
    // selection (mono, 44100) is neither the FIRST track nor the
    // MOST-CHANNELS track, so any naive strategy diverges observably here —
    // this is the stream-identity proof the manifest requires for V3.
    let fixture_b = tmp_path("golden_v3b_in.mp4");
    if !std::path::Path::new(&fixture_b).exists() {
        run_reference(
            &bin,
            &[
                "-y", "-v", "error",
                "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30:duration=4",
                "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
                "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100:duration=4",
                "-map", "0:v", "-map", "1:a", "-map", "2:a",
                "-disposition:a:0", "0", "-disposition:a:1", "default",
                "-c:v", "mpeg4", "-c:a", "aac", "-t", "4", &fixture_b,
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        );
    }
    let out_ours_b = tmp_path("golden_v3b_ours.m4a");
    let out_theirs_b = tmp_path("golden_v3b_theirs.m4a");
    let context =
        from_cli_args(&args(&fixture_b, &out_ours_b)).expect("V3 fixture B must pass the gates");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v3b ours").unwrap();
    run_reference(&bin, &args(&fixture_b, &out_theirs_b));

    assert_same_media(&out_ours_b, &out_theirs_b);
    let ours_b = summarize(&out_ours_b);
    assert_eq!(
        (ours_b[0].channels, ours_b[0].sample_rate),
        (1, 44100),
        "selection must follow the DEFAULT bonus to the non-first, non-most-channels track"
    );
}

#[test]
fn golden_v4_thumbnail() {
    let fixture = av_fixture("golden_v4_in.mp4");
    let out_ours = tmp_path("golden_v4_ours.jpg");
    let out_theirs = tmp_path("golden_v4_theirs.jpg");
    let args = |out: &str| -> Vec<String> {
        ["-ss", "5", "-i", &fixture, "-an", "-c:v", "mjpeg", "-frames:v", "1", "-y", out]
            .iter()
            .map(|s| s.to_string())
            .collect()
    };
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v4_thumbnail: {reason}");
            return;
        }
    };
    let context = from_cli_args(&args(&out_ours)).expect("V4 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v4 ours").unwrap();
    run_reference(&bin, &args(&out_theirs));

    assert_same_media(&out_ours, &out_theirs);
    let ours = summarize(&out_ours);
    assert_eq!(ours.len(), 1, "-an single-frame output must be video-only");
    assert_eq!(ours[0].codec, "mjpeg");
    assert_eq!((ours[0].width, ours[0].height), (320, 240));

    // Parity evidence for the image2 `update=1` divergence: the crate
    // auto-enables update mode for -frames:v 1 (the CLI only warns about the
    // missing sequence pattern). The option changes no picture data — proven
    // at the pixel level: both JPEGs decode to identical RGB. Raw byte
    // equality additionally holds when the CLI's libavcodec MICRO version
    // matches the linked one; across micro versions the encoder stamps a
    // different `LavcXX.YY.ZZZ` comment marker into the file, so byte
    // comparison is only defined on the exact same build.
    let (ours_px, ours_dims) = decode_rgb(&out_ours);
    let (theirs_px, theirs_dims) = decode_rgb(&out_theirs);
    assert_eq!(ours_dims, theirs_dims, "thumbnail geometry differs");
    assert_eq!(
        ours_px, theirs_px,
        "thumbnails must decode to identical pixels (same frame, same encode)"
    );
    if cli_avcodec_triple(&bin) == Some(linked_avcodec_triple()) {
        let ours_bytes = std::fs::read(&out_ours).unwrap();
        let theirs_bytes = std::fs::read(&out_theirs).unwrap();
        assert_eq!(
            ours_bytes, theirs_bytes,
            "on the exact same libavcodec build the thumbnail must be byte-identical"
        );
    } else {
        eprintln!(
            "golden_v4_thumbnail: byte comparison skipped (libavcodec micro differs); \
             pixel parity asserted instead"
        );
    }
}

/// Decodes an image/video to tightly packed RGB24 via the crate's extractor.
fn decode_rgb(path: &str) -> (Vec<u8>, (u32, u32)) {
    let frames = ez_ffmpeg::frame_export::FrameExtractor::new(path)
        .collect_frames()
        .unwrap_or_else(|e| panic!("decoding {path} failed: {e}"));
    assert_eq!(frames.len(), 1, "{path} must contain exactly one frame");
    let frame = &frames[0];
    (
        frame.as_bytes().to_vec(),
        (frame.width(), frame.height()),
    )
}

/// Full libavcodec triple of the reference binary, from its version banner.
fn cli_avcodec_triple(bin: &str) -> Option<(u32, u32, u32)> {
    let out = Command::new(bin).arg("-version").output().ok()?;
    let banner = String::from_utf8_lossy(&out.stdout).into_owned();
    let line = banner
        .lines()
        .find(|l| l.trim_start().starts_with("libavcodec"))?;
    let runtime = line.rsplit('/').next()?;
    let digits: String = runtime
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut it = digits.split('.').filter(|s| !s.is_empty());
    Some((
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
    ))
}

fn linked_avcodec_triple() -> (u32, u32, u32) {
    let v = unsafe { ffmpeg_sys_next::avcodec_version() };
    (v >> 16, (v >> 8) & 0xff, v & 0xff)
}

#[test]
fn golden_v5_scale() {
    let fixture = av_fixture("golden_v5_in.mp4");
    let out_ours = tmp_path("golden_v5_ours.mp4");
    let out_theirs = tmp_path("golden_v5_theirs.mp4");
    let args = |out: &str| -> Vec<String> {
        [
            "-i", &fixture, "-vf", "scale=192:-2", "-c:v", "libx264", "-crf", "23", "-preset",
            "fast", "-c:a", "aac", "-y", out,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    };
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v5_scale: {reason}");
            return;
        }
    };
    let context = from_cli_args(&args(&out_ours)).expect("V5 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 120, "golden v5 ours").unwrap();
    run_reference(&bin, &args(&out_theirs));

    assert_same_media(&out_ours, &out_theirs);
    let ours = summarize(&out_ours);
    // 320x240 -> width 192, -2 keeps the 4:3 height even: 144.
    assert_eq!((ours[0].width, ours[0].height), (192, 144));
}

#[test]
fn golden_v6_hls() {
    let fixture = av_fixture("golden_v6_in.mp4");
    let dir_ours = tmp_path("golden_v6_ours");
    let dir_theirs = tmp_path("golden_v6_theirs");
    std::fs::create_dir_all(&dir_ours).unwrap();
    std::fs::create_dir_all(&dir_theirs).unwrap();
    let args = |dir: &str| -> Vec<String> {
        [
            "-i",
            &fixture,
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-f",
            "hls",
            "-hls_time",
            "6",
            "-hls_playlist_type",
            "vod",
            "-hls_list_size",
            "0",
            "-hls_segment_filename",
            &format!("{dir}/seg_%03d.ts"),
            "-y",
            &format!("{dir}/out.m3u8"),
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    };
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping golden_v6_hls: {reason}");
            return;
        }
    };
    let context = from_cli_args(&args(&dir_ours)).expect("V6 must pass every CLI-compat gate");
    wait_with_watchdog(context.start().unwrap(), 180, "golden v6 ours").unwrap();
    run_reference(&bin, &args(&dir_theirs));

    let ours = parse_playlist(&format!("{dir_ours}/out.m3u8"));
    let theirs = parse_playlist(&format!("{dir_theirs}/out.m3u8"));

    // HLS-specific oracle: playlist tags, topology, naming, ENDLIST.
    assert!(ours.vod, "crate playlist must declare EXT-X-PLAYLIST-TYPE:VOD");
    assert!(theirs.vod, "reference playlist must declare VOD");
    assert!(ours.endlist, "crate playlist must end with EXT-X-ENDLIST");
    assert!(theirs.endlist, "reference playlist must end with EXT-X-ENDLIST");
    assert!(
        ours.segments.len() >= 2,
        "the 16s fixture must produce multiple segments, got {:?}",
        ours.segments
    );
    assert_eq!(
        ours.segments, theirs.segments,
        "segment topology/naming must match the CLI"
    );
    assert_eq!(ours.target_duration, theirs.target_duration, "EXT-X-TARGETDURATION differs");
    assert_eq!(
        ours.durations.len(),
        theirs.durations.len(),
        "EXTINF count differs"
    );
    for (i, (a, b)) in ours.durations.iter().zip(&theirs.durations).enumerate() {
        assert!(
            (a - b).abs() <= 0.2,
            "segment {i} duration differs: {a} vs {b}"
        );
    }
    // Every named segment must exist on disk with real payload.
    for seg in &ours.segments {
        let seg_path = format!("{dir_ours}/{seg}");
        let len = std::fs::metadata(&seg_path)
            .unwrap_or_else(|e| panic!("crate segment {seg_path} missing: {e}"))
            .len();
        assert!(len > 0, "crate segment {seg_path} is empty");
    }
}

struct Playlist {
    vod: bool,
    endlist: bool,
    target_duration: Option<u32>,
    durations: Vec<f64>,
    segments: Vec<String>,
}

fn parse_playlist(path: &str) -> Playlist {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("playlist {path} unreadable: {e}"));
    assert!(text.starts_with("#EXTM3U"), "{path} is not an m3u8 playlist");
    let mut playlist = Playlist {
        vod: false,
        endlist: false,
        target_duration: None,
        durations: Vec::new(),
        segments: Vec::new(),
    };
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("#EXT-X-PLAYLIST-TYPE:") {
            playlist.vod = rest == "VOD";
        } else if line == "#EXT-X-ENDLIST" {
            playlist.endlist = true;
        } else if let Some(rest) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
            playlist.target_duration = rest.parse().ok();
        } else if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let secs = rest.trim_end_matches(',');
            playlist.durations.push(secs.parse().unwrap_or(f64::NAN));
        } else if !line.is_empty() && !line.starts_with('#') {
            // Segment URI; keep the basename only (dirs differ by design).
            let name = line.rsplit('/').next().unwrap_or(line).to_string();
            playlist.segments.push(name);
        }
    }
    playlist
}
