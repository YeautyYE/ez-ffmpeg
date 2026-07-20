//! Semantic goldens for the verified shapes — driven BY the manifest.
//!
//! Every test resolves its shape from [`VERIFIED_SHAPES`] and runs the
//! entry's OWN canonical argv (path substitution only) through THREE lanes:
//!
//! 1. the reference ffmpeg CLI,
//! 2. `from_cli_args` (in-process),
//! 3. the compile-pinned EMITTED PROGRAM for the shape
//!    (`examples/cli_emitted_*`, byte-for-byte `emit(canonical_argv)`),
//!    executed in a scratch working directory whose canonical file names
//!    point at the same fixture.
//!
//! All three outputs must agree semantically: stream count and identity,
//! codec names, geometry, sample rate and channel layout, duration within
//! tolerance, and the implicitly copied container metadata (the fixtures
//! carry a global title; the CLI copies it by default and so must both
//! in-process lanes). Every lane's output path is pre-seeded with garbage so
//! `-y` overwrite-through-truncation is exercised on each run. Shape
//! specific oracles stack on top (V2 window, V3 stream identity, V4 pixel
//! parity, V5 geometry, V6 playlist topology plus the base-media oracle over
//! the playlists themselves).
//!
//! Because the runner looks the shape up by id and consumes the manifest's
//! canonical argv, a manifest row cannot claim `verified` without this suite
//! actually executing that command — the linkage is typed, not textual.
//!
//! Reference-binary resolution: `EZ_FFMPEG_CLI=<path>` is the STRICT lane
//! (any failure, including a skip condition, fails the test); with the
//! variable unset, `ffmpeg` from PATH is probed and exactly two conditions
//! skip with a stderr note — no binary, or a libavcodec/libavformat
//! major.minor mismatch with the linked build.

use std::process::Command;

use super::manifest::{GoldenOracle, VerifiedShape, VERIFIED_SHAPES};
use super::{from_cli_args, linked_profile_verified, CliError};
use crate::core::container_info::{get_duration_us, get_metadata};
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::core::stream_info::{find_all_stream_infos, StreamInfo};

const DURATION_TOLERANCE_US: i64 = 200_000;
const FIXTURE_TITLE: &str = "cli compat golden fixture";

/// Overwrite sentinel: LARGER than every artifact any golden produces, so a
/// writer that appended or rewrote in place without truncating would leave a
/// stale tail and fail the size assertion below.
const PRESEED_LEN: usize = 8 * 1024 * 1024;

fn preseed(path: &str) {
    std::fs::write(path, vec![0xABu8; PRESEED_LEN]).unwrap();
}

/// The truncation proof: the final artifact must be strictly smaller than
/// the oversized sentinel it replaced (a valid-media probe alone cannot
/// exclude a stale tail past the new content).
fn assert_truncated(label: &str, path: &str) {
    let len = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("{label}: output {path} missing: {e}"))
        .len() as usize;
    assert!(
        len < PRESEED_LEN,
        "{label}: {path} is {len} bytes, not smaller than the {PRESEED_LEN}-byte sentinel —          the -y overwrite did not truncate"
    );
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_cli_goldens_{}", std::process::id()));
    let dir = dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn run_context(context: FfmpegContext, scenario: &str) {
    let scheduler = context.start().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(std::time::Duration::from_secs(180)) {
        Ok(result) => result.unwrap_or_else(|e| panic!("{scenario} failed: {e}")),
        Err(_) => panic!("{scenario} did not finish within 180s"),
    }
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

fn run_reference(bin: &str, args: &[String], cwd: Option<&std::path::Path>) {
    let mut command = Command::new(bin);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let out = command
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
// Fixtures (crate-built lavfi sources, with a global title so the implicit
// metadata copy is observable).
// ---------------------------------------------------------------------------

/// 16s 320x240@30 A/V fixture (mpeg4 + stereo AAC), long enough for several
/// HLS segments — and KEYFRAME-DENSE by construction: a full-frame negation
/// toggles at every integer second, so the x264 re-encode of every golden
/// places IDR scene cuts each second. That is what makes the V6 oracle
/// sensitive to the `hls_time` lowering: on smooth content x264's default
/// keyint (250 ≈ 8.3s) pins every playlist to the same ~8.3s segmentation
/// regardless of hls_time (measured: hls_time 2/6/7 identical), while on
/// this fixture hls_time 6 yields 6/6/4 (target 6) and the muxer default
/// of 2 would yield eight 2s segments — a dropped lowering cannot pass.
fn av_fixture() -> String {
    let path = std::env::temp_dir()
        .join(format!("ez_ffmpeg_cli_goldens_{}", std::process::id()))
        .join("fixture_av.mp4");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let path = path.to_string_lossy().into_owned();
    if std::path::Path::new(&path).exists() {
        return path;
    }
    run_context(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30:duration=16").set_format("lavfi"))
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=16").set_format("lavfi"),
            )
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("negate=enable='lt(mod(t,2),1)'")
                    .set_audio_codec("aac")
                    .add_metadata("title", FIXTURE_TITLE),
            )
            .build()
            .unwrap(),
        "av fixture",
    );
    path
}

/// Multi-audio fixture for the V3 stream-identity proof: audio #0 is MONO
/// 44100 (sine) and carries the container's DEFAULT disposition, audio #1 is
/// STEREO 48000 (silence). fftools scores candidates
/// `channels + 5M*DEFAULT (+100M*NEW_PACKETS)` — map_auto_audio, identical
/// in 7.1 and 8.1 — so the DEFAULT bonus makes the MONO track the correct
/// selection for both engines, distinguishable from naive most-channels.
fn multi_audio_fixture() -> String {
    let path = std::env::temp_dir()
        .join(format!("ez_ffmpeg_cli_goldens_{}", std::process::id()))
        .join("fixture_multi_audio.mp4");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let path = path.to_string_lossy().into_owned();
    if std::path::Path::new(&path).exists() {
        return path;
    }
    run_context(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30:duration=16").set_format("lavfi"))
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=16").set_format("lavfi"),
            )
            .input(
                Input::from("anullsrc=channel_layout=stereo:sample_rate=48000").set_format("lavfi"),
            )
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_recording_time_us(16_000_000)
                    .add_stream_map("0:v")
                    .add_stream_map("1:a")
                    .add_stream_map("2:a")
                    .add_metadata("title", FIXTURE_TITLE),
            )
            .build()
            .unwrap(),
        "multi-audio fixture",
    );
    let channel_counts: Vec<i32> = find_all_stream_infos(&path)
        .unwrap()
        .iter()
        .filter_map(|info| match info {
            StreamInfo::Audio { nb_channels, .. } => Some(*nb_channels),
            _ => None,
        })
        .collect();
    assert_eq!(
        channel_counts,
        vec![1, 2],
        "fixture must be mono-then-stereo"
    );
    path
}

fn fixture_for(shape_id: &str) -> String {
    match shape_id {
        "V3" => multi_audio_fixture(),
        _ => av_fixture(),
    }
}

// ---------------------------------------------------------------------------
// Comparison oracles.
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

fn container_title(path: &str) -> Option<String> {
    get_metadata(path)
        .unwrap_or_else(|e| panic!("metadata probe of {path} failed: {e}"))
        .into_iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("title"))
        .map(|(_, value)| value)
}

/// The shared semantic oracle over one pair of outputs: stream identity,
/// codecs, geometry, layout, duration tolerance, and the implicitly copied
/// container title.
fn assert_same_media(label: &str, ours: &str, theirs: &str) {
    let a = summarize(ours);
    let b = summarize(theirs);
    assert_eq!(
        a.len(),
        b.len(),
        "{label}: stream count differs ({ours} vs {theirs})"
    );
    for (i, (x, y)) in a.iter().zip(&b).enumerate() {
        assert_eq!(x.kind, y.kind, "{label}: stream {i} type differs");
        assert_eq!(x.codec, y.codec, "{label}: stream {i} codec differs");
        assert_eq!(
            (x.width, x.height),
            (y.width, y.height),
            "{label}: stream {i} geometry differs"
        );
        assert_eq!(
            (x.sample_rate, x.channels),
            (y.sample_rate, y.channels),
            "{label}: stream {i} audio layout differs"
        );
    }
    let da = get_duration_us(ours).unwrap();
    let db = get_duration_us(theirs).unwrap();
    assert!(
        (da - db).abs() <= DURATION_TOLERANCE_US,
        "{label}: duration differs beyond tolerance: {da}us vs {db}us"
    );
    assert_eq!(
        container_title(ours),
        container_title(theirs),
        "{label}: implicitly copied container title differs"
    );
}

// ---------------------------------------------------------------------------
// The manifest-driven runner.
// ---------------------------------------------------------------------------

struct GoldenRun {
    ours_output: String,
    theirs_output: String,
    emitted_dir: std::path::PathBuf,
    emitted_output: String,
    bin: String,
}

/// Materializes the canonical argv for a lane: the canonical input name maps
/// to the fixture, the canonical output name (and V6's segment pattern) map
/// into `dir`.
fn materialize(shape: &VerifiedShape, fixture: &str, dir: &std::path::Path) -> Vec<String> {
    let canonical_input = canonical_input(shape);
    let canonical_output = shape.canonical_argv.last().unwrap();
    shape
        .canonical_argv
        .iter()
        .map(|token| {
            if *token == canonical_input {
                fixture.to_string()
            } else if token == canonical_output || token.contains("%03d") {
                dir.join(token).to_string_lossy().into_owned()
            } else {
                token.to_string()
            }
        })
        .collect()
}

fn canonical_input(shape: &VerifiedShape) -> &'static str {
    let position = shape
        .canonical_argv
        .iter()
        .position(|token| *token == "-i")
        .expect("canonical argv has -i");
    shape.canonical_argv[position + 1]
}

/// Runs the shape's canonical command through all three lanes. Returns
/// `None` when the reference gate skips (lenient mode only).
fn run_shape(shape: &VerifiedShape) -> Option<GoldenRun> {
    // Profile-aware: on a non-verified linked line the semantic lanes cannot
    // run; the honest assertion is that from_cli_args fails with the typed
    // profile error for this shape's canonical command.
    if !linked_profile_verified() {
        let args: Vec<String> = shape.canonical_argv.iter().map(|s| s.to_string()).collect();
        match from_cli_args(&args) {
            Err(CliError::UnverifiedRuntimeProfile { .. }) => {
                eprintln!(
                    "{}: linked profile not verified; asserted the typed profile failure",
                    shape.id
                );
                return None;
            }
            Ok(_) => panic!("{}: runtime must refuse a non-verified profile", shape.id),
            Err(other) => panic!(
                "{}: expected UnverifiedRuntimeProfile on this linked build, got: {other}",
                shape.id
            ),
        }
    }
    let bin = match cli_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(reason) => {
            eprintln!("skipping {}: {reason}", shape.id);
            return None;
        }
    };
    let fixture = fixture_for(shape.id);
    let canonical_output = shape.canonical_argv.last().unwrap().to_string();

    // Lane 2 (in-process) and lane 1 (reference CLI), each in its own dir.
    let ours_dir = tmp_dir(&format!("{}_ours", shape.id));
    let theirs_dir = tmp_dir(&format!("{}_theirs", shape.id));
    let ours_args = materialize(shape, &fixture, &ours_dir);
    let theirs_args = materialize(shape, &fixture, &theirs_dir);
    let ours_output = ours_dir
        .join(&canonical_output)
        .to_string_lossy()
        .into_owned();
    let theirs_output = theirs_dir
        .join(&canonical_output)
        .to_string_lossy()
        .into_owned();

    // Overwrite semantics ride ALL THREE lanes: -y must truncate an
    // oversized pre-existing file into the real artifact (reference CLI
    // included — its overwrite behavior is part of the parity claim).
    preseed(&ours_output);
    preseed(&theirs_output);

    let context = from_cli_args(&ours_args).unwrap_or_else(|e| {
        panic!(
            "{}: from_cli_args rejected the canonical command: {e}",
            shape.id
        )
    });
    run_context(context, shape.id);
    run_reference(&bin, &theirs_args, None);
    assert_truncated(&format!("{} ours", shape.id), &ours_output);
    assert_truncated(&format!("{} theirs", shape.id), &theirs_output);

    // Lane 3: the compile-pinned emitted program, run in a scratch cwd whose
    // canonical file names resolve to the same fixture.
    let emitted_dir = tmp_dir(&format!("{}_emitted", shape.id));
    let emitted_input = emitted_dir.join(canonical_input(shape));
    if !emitted_input.exists() {
        std::fs::copy(&fixture, &emitted_input).unwrap();
    }
    let emitted_output = emitted_dir
        .join(&canonical_output)
        .to_string_lossy()
        .into_owned();
    preseed(&emitted_output);
    let example = example_binary(shape.emitted_example);
    let out = Command::new(&example)
        .current_dir(&emitted_dir)
        .output()
        .unwrap_or_else(|e| panic!("failed to run emitted program {example:?}: {e}"));
    assert!(
        out.status.success(),
        "emitted program {} failed:\n{}",
        shape.emitted_example,
        String::from_utf8_lossy(&out.stderr)
    );
    assert_truncated(&format!("{} emitted", shape.id), &emitted_output);

    // The three-lane oracle: in-process vs CLI, emitted vs CLI.
    assert_same_media(
        &format!("{} ours-vs-cli", shape.id),
        &ours_output,
        &theirs_output,
    );
    assert_same_media(
        &format!("{} emitted-vs-cli", shape.id),
        &emitted_output,
        &theirs_output,
    );

    Some(GoldenRun {
        ours_output,
        theirs_output,
        emitted_dir,
        emitted_output,
        bin,
    })
}

/// Path of the compile-pinned example binary; builds it on demand (running
/// `cargo test --lib` alone does not build examples).
fn example_binary(name: &str) -> std::path::PathBuf {
    let deps_dir = std::env::current_exe().unwrap();
    // target/debug/deps/<test-bin> -> target/debug/examples/<name>
    let debug_dir = deps_dir.parent().unwrap().parent().unwrap();
    let path = debug_dir.join("examples").join(name);
    if path.exists() {
        return path;
    }
    let status = Command::new(env!("CARGO"))
        .args(["build", "--features", "cli", "--example", name])
        .status()
        .expect("failed to spawn cargo to build the emitted example");
    assert!(status.success(), "building example {name} failed");
    assert!(
        path.exists(),
        "example binary {path:?} still missing after build"
    );
    path
}

// ---------------------------------------------------------------------------
// THE golden test: one parameterized runner driven by the manifest. Every
// VERIFIED_SHAPES row is executed against its typed oracle through an
// exhaustive match — a new verified row cannot land without choosing an
// oracle, and a new oracle variant cannot land without assertions here.
// ---------------------------------------------------------------------------

#[test]
fn verified_shapes_pass_their_semantic_goldens() {
    for shape in VERIFIED_SHAPES {
        let Some(run) = run_shape(shape) else {
            continue;
        };
        match shape.oracle {
            GoldenOracle::Transcode => oracle_transcode(&run),
            GoldenOracle::Clip => oracle_clip(&run),
            GoldenOracle::AudioExtract => oracle_audio_extract(&run),
            GoldenOracle::Thumbnail => oracle_thumbnail(&run),
            GoldenOracle::Scale => oracle_scale(&run),
            GoldenOracle::Hls => oracle_hls(&run),
        }
    }
}

fn oracle_transcode(run: &GoldenRun) {
    let ours = summarize(&run.ours_output);
    assert_eq!(ours.len(), 2);
    assert_eq!(ours[0].codec, "h264");
    assert_eq!(ours[1].codec, "aac");
    assert_eq!(
        container_title(&run.ours_output).as_deref(),
        Some(FIXTURE_TITLE),
        "the fixture title must be implicitly copied"
    );
}

fn oracle_clip(run: &GoldenRun) {
    // -ss 10 on a 16s fixture with -t 20: ~6s remain; both engines agreed
    // (three-lane oracle), pin the absolute window too.
    let duration = get_duration_us(&run.ours_output).unwrap();
    assert!(
        (5_500_000..=6_500_000).contains(&duration),
        "clip duration {duration}us outside the expected ~6s window"
    );
}

fn oracle_audio_extract(run: &GoldenRun) {
    let ours = summarize(&run.ours_output);
    assert_eq!(ours.len(), 1, "-vn output must be audio-only");
    assert_eq!(ours[0].codec, "aac");
    // Identity proof on fixture A: the DEFAULT-disposition bonus selects the
    // MONO track (1ch/44100); naive most-channels would give 2/48000.
    assert_eq!(
        (ours[0].channels, ours[0].sample_rate),
        (1, 44100),
        "the DEFAULT-disposition bonus must select the mono track"
    );

    // Fixture B (reference-CLI-built, forced dispositions): audio #0 STEREO
    // without DEFAULT, audio #1 MONO with DEFAULT. The correct selection is
    // neither the FIRST track nor the MOST-CHANNELS track — the full
    // stream-identity discriminator.
    let fixture_b = std::env::temp_dir()
        .join(format!("ez_ffmpeg_cli_goldens_{}", std::process::id()))
        .join("fixture_disposition.mp4");
    let fixture_b = fixture_b.to_string_lossy().into_owned();
    if !std::path::Path::new(&fixture_b).exists() {
        run_reference(
            &run.bin,
            &[
                "-y",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x240:rate=30:duration=4",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=channel_layout=stereo:sample_rate=48000",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=44100:duration=4",
                "-map",
                "0:v",
                "-map",
                "1:a",
                "-map",
                "2:a",
                "-disposition:a:0",
                "0",
                "-disposition:a:1",
                "default",
                "-c:v",
                "mpeg4",
                "-c:a",
                "aac",
                "-t",
                "4",
                &fixture_b,
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
            None,
        );
    }
    let out_ours = tmp_dir("V3b_ours")
        .join("out.m4a")
        .to_string_lossy()
        .into_owned();
    let out_theirs = tmp_dir("V3b_theirs")
        .join("out.m4a")
        .to_string_lossy()
        .into_owned();
    let args = |out: &str| -> Vec<String> {
        [
            "-i", &fixture_b, "-vn", "-c:a", "aac", "-b:a", "192k", "-y", out,
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    };
    let context = from_cli_args(&args(&out_ours)).expect("V3 fixture B must pass the gates");
    run_context(context, "golden v3 fixture B");
    run_reference(&run.bin, &args(&out_theirs), None);
    assert_same_media("V3 fixture B", &out_ours, &out_theirs);
    let ours_b = summarize(&out_ours);
    assert_eq!(
        (ours_b[0].channels, ours_b[0].sample_rate),
        (1, 44100),
        "selection must follow the DEFAULT bonus to the non-first, non-most-channels track"
    );
}

fn oracle_thumbnail(run: &GoldenRun) {
    let ours = summarize(&run.ours_output);
    assert_eq!(ours.len(), 1, "-an single-frame output must be video-only");
    assert_eq!(ours[0].codec, "mjpeg");
    assert_eq!((ours[0].width, ours[0].height), (320, 240));

    // Parity evidence for the image2 `update=1` divergence: the crate
    // auto-enables update mode for -frames:v 1 (the CLI only warns about the
    // missing sequence pattern). The option changes no picture data — proven
    // at the pixel level across all three lanes. Raw byte equality
    // additionally holds when the CLI's libavcodec MICRO version matches the
    // linked one; across micro versions the encoder stamps a different
    // `LavcXX.YY.ZZZ` comment marker into the file, so byte comparison is
    // only defined on the exact same build.
    let (ours_px, ours_dims) = decode_rgb(&run.ours_output);
    let (theirs_px, theirs_dims) = decode_rgb(&run.theirs_output);
    let (emitted_px, emitted_dims) = decode_rgb(&run.emitted_output);
    assert_eq!(ours_dims, theirs_dims, "thumbnail geometry differs");
    assert_eq!(
        emitted_dims, theirs_dims,
        "emitted thumbnail geometry differs"
    );
    assert_eq!(
        ours_px, theirs_px,
        "thumbnails must decode to identical pixels"
    );
    assert_eq!(
        emitted_px, theirs_px,
        "emitted thumbnail pixels must match the CLI"
    );
    if cli_avcodec_triple(&run.bin) == Some(linked_avcodec_triple()) {
        let ours_bytes = std::fs::read(&run.ours_output).unwrap();
        let theirs_bytes = std::fs::read(&run.theirs_output).unwrap();
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

fn oracle_scale(run: &GoldenRun) {
    let ours = summarize(&run.ours_output);
    // 320x240 -> width 1280, -2 keeps the 4:3 height even: 960.
    assert_eq!((ours[0].width, ours[0].height), (1280, 960));
}

fn oracle_hls(run: &GoldenRun) {
    let ours_playlist = run.ours_output.clone();
    let theirs_playlist = run.theirs_output.clone();
    let emitted_playlist = run.emitted_output.clone();
    // run_shape already applied the base-media oracle THROUGH the playlists
    // (the hls demuxer resolves segments), so stream identity, codecs,
    // duration and metadata are covered. Stack the HLS-specific oracle on
    // top: tags, segment topology and naming, per-segment durations,
    // ENDLIST.
    let ours = parse_playlist(&ours_playlist);
    let theirs = parse_playlist(&theirs_playlist);
    let emitted = parse_playlist(&emitted_playlist);

    for (label, playlist) in [("ours", &ours), ("theirs", &theirs), ("emitted", &emitted)] {
        assert!(
            playlist.vod,
            "{label}: playlist must declare EXT-X-PLAYLIST-TYPE:VOD"
        );
        assert!(
            playlist.endlist,
            "{label}: playlist must end with EXT-X-ENDLIST"
        );
    }
    assert!(
        ours.segments.len() >= 2,
        "the 16s fixture must produce multiple segments, got {:?}",
        ours.segments
    );
    assert_eq!(
        ours.segments, theirs.segments,
        "segment topology/naming must match the CLI"
    );
    assert_eq!(
        emitted.segments, theirs.segments,
        "emitted segment topology must match"
    );
    // Segmentation oracle across ALL THREE lanes: target duration and every
    // per-segment duration. On the keyframe-dense fixture these are the
    // fields the hls_time lowering controls (dropping it falls back to the
    // muxer default of 2 and produces eight 2s segments instead).
    assert_eq!(
        ours.target_duration, theirs.target_duration,
        "EXT-X-TARGETDURATION differs"
    );
    assert_eq!(
        emitted.target_duration, theirs.target_duration,
        "emitted EXT-X-TARGETDURATION differs"
    );
    assert_eq!(
        ours.target_duration,
        Some(6),
        "hls_time 6 on the 1s-IDR fixture must yield target duration 6"
    );
    assert_eq!(
        ours.durations.len(),
        theirs.durations.len(),
        "EXTINF count differs"
    );
    assert_eq!(
        emitted.durations.len(),
        theirs.durations.len(),
        "emitted EXTINF count differs"
    );
    for (i, (a, b)) in ours.durations.iter().zip(&theirs.durations).enumerate() {
        assert!(
            (a - b).abs() <= 0.2,
            "segment {i} duration differs: {a} vs {b}"
        );
    }
    for (i, (a, b)) in emitted.durations.iter().zip(&theirs.durations).enumerate() {
        assert!(
            (a - b).abs() <= 0.2,
            "emitted segment {i} duration differs: {a} vs {b}"
        );
    }
    let expected = [6.0, 6.0, 4.0];
    assert_eq!(
        ours.durations.len(),
        expected.len(),
        "expected a 6/6/4 split"
    );
    for (i, (a, e)) in ours.durations.iter().zip(&expected).enumerate() {
        assert!(
            (a - e).abs() <= 0.2,
            "segment {i}: expected ~{e}s on the keyframe-dense fixture, got {a}"
        );
    }
    // Every named segment must exist on disk with real payload, in the
    // in-process and emitted lanes alike.
    let ours_dir = std::path::Path::new(&ours_playlist).parent().unwrap();
    for seg in &ours.segments {
        let seg_path = ours_dir.join(seg);
        let len = std::fs::metadata(&seg_path)
            .unwrap_or_else(|e| panic!("crate segment {seg_path:?} missing: {e}"))
            .len();
        assert!(len > 0, "crate segment {seg_path:?} is empty");
    }
    for seg in &emitted.segments {
        let seg_path = run.emitted_dir.join(seg);
        assert!(seg_path.exists(), "emitted segment {seg_path:?} missing");
    }
}

// ---------------------------------------------------------------------------
// The V5 structural-uniqueness prerequisite (review probe): a two-video
// input must be rejected, not silently filtered over a selected stream.
// ---------------------------------------------------------------------------

#[test]
fn vf_execution_rejects_multi_video_inputs() {
    let dir = tmp_dir("multi_video");
    let fixture = dir.join("two_streams.mp4").to_string_lossy().into_owned();
    if !std::path::Path::new(&fixture).exists() {
        run_context(
            FfmpegContext::builder()
                .input(Input::from("testsrc2=size=320x240:rate=30:duration=1").set_format("lavfi"))
                .input(Input::from("testsrc2=size=160x120:rate=30:duration=1").set_format("lavfi"))
                .output(
                    Output::from(fixture.as_str())
                        .set_video_codec("mpeg4")
                        .add_stream_map("0:v")
                        .add_stream_map("1:v"),
                )
                .build()
                .unwrap(),
            "two-video fixture",
        );
    }
    let out = dir.join("scaled.mp4").to_string_lossy().into_owned();
    let args: Vec<String> = [
        "-i",
        fixture.as_str(),
        "-vf",
        "scale=64:-2",
        "-c:v",
        "libx264",
        "-crf",
        "23",
        "-preset",
        "fast",
        "-c:a",
        "aac",
        "-y",
        out.as_str(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    match from_cli_args(&args) {
        Ok(_) => panic!("a -vf command over a two-video-stream input must be rejected"),
        Err(CliError::AmbiguousFilterSource { video_streams }) => {
            assert_eq!(video_streams, 2);
        }
        Err(CliError::UnverifiedRuntimeProfile { .. }) if !linked_profile_verified() => {}
        Err(other) => panic!("expected AmbiguousFilterSource, got: {other}"),
    }
}

/// The TOCTOU regression: uniqueness must hold on the opening the pipeline
/// executes with. A path first observed with ONE video stream is swapped for
/// two-video content before the run — the in-binding check must still
/// reject, because it validates the demuxers the job actually opened, not
/// any earlier observation.
#[test]
fn vf_uniqueness_is_checked_on_the_executed_opening() {
    let dir = tmp_dir("mutable_input");
    let path = dir.join("mutable.mp4").to_string_lossy().into_owned();

    // Step 1: single-video content at the path; an external observer sees 1.
    run_context(
        FfmpegContext::builder()
            .input(Input::from("testsrc2=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(Output::from(path.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "single-video first content",
    );
    let observed = find_all_stream_infos(&path)
        .unwrap()
        .into_iter()
        .filter(|info| matches!(info, StreamInfo::Video { .. }))
        .count();
    assert_eq!(observed, 1, "precondition: the path starts unique");

    // Step 2: the file mutates to two video streams before the run.
    let two_video = dir.join("two_video_src.mp4").to_string_lossy().into_owned();
    if !std::path::Path::new(&two_video).exists() {
        run_context(
            FfmpegContext::builder()
                .input(Input::from("testsrc2=size=320x240:rate=30:duration=1").set_format("lavfi"))
                .input(Input::from("testsrc2=size=160x120:rate=30:duration=1").set_format("lavfi"))
                .output(
                    Output::from(two_video.as_str())
                        .set_video_codec("mpeg4")
                        .add_stream_map("0:v")
                        .add_stream_map("1:v"),
                )
                .build()
                .unwrap(),
            "two-video replacement content",
        );
    }
    std::fs::copy(&two_video, &path).unwrap();

    // Step 3: the run must reject based on ITS OWN opening.
    let out = dir.join("scaled.mp4").to_string_lossy().into_owned();
    let args: Vec<String> = [
        "-i",
        path.as_str(),
        "-vf",
        "scale=64:-2",
        "-c:v",
        "libx264",
        "-crf",
        "23",
        "-preset",
        "fast",
        "-c:a",
        "aac",
        "-y",
        out.as_str(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    match from_cli_args(&args) {
        Ok(_) => panic!("the mutated two-video input must be rejected"),
        Err(CliError::AmbiguousFilterSource { video_streams }) => {
            assert_eq!(video_streams, 2);
        }
        Err(CliError::UnverifiedRuntimeProfile { .. }) if !linked_profile_verified() => {}
        Err(other) => panic!("expected AmbiguousFilterSource, got: {other}"),
    }
}

/// Mechanism proof for the uniqueness gate: building a `-vf` command through
/// the facade must open its input EXACTLY ONCE. The retired implementation
/// probed with `find_all_stream_infos` and then built — two openings — and
/// this assertion fails against it (verified on the pre-fix commit in a
/// scratch worktree; see the disposition table). URL-keyed counting keeps
/// concurrent tests out of each other's tallies.
#[test]
fn vf_uniqueness_gate_opens_the_input_exactly_once() {
    let dir = tmp_dir("single_opening");
    let fixture = dir
        .join("single_opening_probe.mp4")
        .to_string_lossy()
        .into_owned();
    if !std::path::Path::new(&fixture).exists() {
        run_context(
            FfmpegContext::builder()
                .input(Input::from("testsrc2=size=320x240:rate=30:duration=1").set_format("lavfi"))
                .output(Output::from(fixture.as_str()).set_video_codec("mpeg4"))
                .build()
                .unwrap(),
            "single-opening fixture",
        );
    }
    let opens_of = |path: &str| {
        crate::core::context::ffmpeg_context::open_input::INPUT_OPEN_LOG
            .lock()
            .unwrap()
            .iter()
            .filter(|url| url.as_str() == path)
            .count()
    };
    let before = opens_of(&fixture);
    let out = dir.join("scaled.mp4").to_string_lossy().into_owned();
    let args: Vec<String> = [
        "-i",
        fixture.as_str(),
        "-vf",
        "scale=64:-2",
        "-c:v",
        "libx264",
        "-crf",
        "23",
        "-preset",
        "fast",
        "-c:a",
        "aac",
        "-y",
        out.as_str(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    match from_cli_args(&args) {
        Ok(context) => {
            drop(context);
            assert!(linked_profile_verified());
            assert_eq!(
                opens_of(&fixture) - before,
                1,
                "the uniqueness gate must ride the pipeline's single opening"
            );
        }
        Err(CliError::UnverifiedRuntimeProfile { .. }) if !linked_profile_verified() => {
            // The profile gate fires before any I/O: zero openings.
            assert_eq!(opens_of(&fixture) - before, 0);
        }
        Err(other) => panic!("single-video -vf build failed unexpectedly: {other}"),
    }
}

// ---------------------------------------------------------------------------
// V4 helpers.
// ---------------------------------------------------------------------------

fn decode_rgb(path: &str) -> (Vec<u8>, (u32, u32)) {
    let frames = crate::core::frame_export::FrameExtractor::new(path)
        .collect_frames()
        .unwrap_or_else(|e| panic!("decoding {path} failed: {e}"));
    assert_eq!(frames.len(), 1, "{path} must contain exactly one frame");
    let frame = &frames[0];
    (frame.as_bytes().to_vec(), (frame.width(), frame.height()))
}

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

// ---------------------------------------------------------------------------
// HLS playlist parsing.
// ---------------------------------------------------------------------------

struct Playlist {
    vod: bool,
    endlist: bool,
    target_duration: Option<u32>,
    durations: Vec<f64>,
    segments: Vec<String>,
}

fn parse_playlist(path: &str) -> Playlist {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("playlist {path} unreadable: {e}"));
    assert!(
        text.starts_with("#EXTM3U"),
        "{path} is not an m3u8 playlist"
    );
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
            let name = line.rsplit('/').next().unwrap_or(line).to_string();
            playlist.segments.push(name);
        }
    }
    playlist
}
