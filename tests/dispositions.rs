//! Stream disposition propagation and the `Output::set_disposition` override.
//!
//! GitHub #66: a FLAC cover art stream is an `AV_DISPOSITION_ATTACHED_PIC`
//! video stream, and the FLAC muxer gates picture muxing on the OUTPUT stream
//! carrying that bit (`libavformat/flacenc.c:223`, `:382`). ez-ffmpeg never
//! copied the input stream's disposition onto the output stream, so the cover
//! packet reached the muxer and was silently dropped.
//!
//! FFmpeg's `set_dispositions()` (fftools/ffmpeg_mux_init.c:3037-3101) has three
//! parts, all covered here:
//!   1. inherit the input stream's disposition,
//!   2. when a media type ends up with >1 output stream and none is default,
//!      mark the first non-attached-picture stream of that type default,
//!   3. a user disposition overrides (1) and disables (2) entirely.
//!
//! The public `StreamInfo` API does not expose `AVStream.disposition`, so the
//! probes below re-open the output with `ffmpeg-sys-next`, like
//! `tests/codec_tag.rs`.

mod common;
use common::{tmp_path_in, wait_with_watchdog};

use ez_ffmpeg::{FfmpegContext, Input, Output};
use ffmpeg_sys_next::{
    avformat_close_input, avformat_find_stream_info, avformat_open_input, AVFormatContext,
    AVMediaType, AV_DISPOSITION_ATTACHED_PIC, AV_DISPOSITION_DEFAULT, AV_DISPOSITION_FORCED,
};
use std::ffi::CString;
use std::process::Command;
use std::ptr;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_dispositions", name)
}

fn run(context: FfmpegContext, scenario: &str) {
    wait_with_watchdog(context.start().expect("start"), 60, scenario).expect("job failed");
}

/// `(codec_type, disposition)` per stream, in output order.
fn streams_of(path: &str) -> Vec<(AVMediaType, i32)> {
    unsafe {
        let c_path = CString::new(path).unwrap();
        let mut fmt: *mut AVFormatContext = ptr::null_mut();
        let ret = avformat_open_input(&mut fmt, c_path.as_ptr(), ptr::null_mut(), ptr::null_mut());
        assert!(ret >= 0, "avformat_open_input({path}) failed: {ret}");
        assert!(avformat_find_stream_info(fmt, ptr::null_mut()) >= 0);
        let out = (0..(*fmt).nb_streams as usize)
            .map(|i| {
                let st = *(*fmt).streams.add(i);
                ((*(*st).codecpar).codec_type, (*st).disposition)
            })
            .collect();
        avformat_close_input(&mut fmt);
        out
    }
}

fn has_attached_pic(path: &str) -> bool {
    streams_of(path)
        .iter()
        .any(|(_, d)| d & AV_DISPOSITION_ATTACHED_PIC != 0)
}

fn video_dispositions(path: &str) -> Vec<i32> {
    streams_of(path)
        .into_iter()
        .filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_VIDEO)
        .map(|(_, d)| d)
        .collect()
}

fn audio_dispositions(path: &str) -> Vec<i32> {
    streams_of(path)
        .into_iter()
        .filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_AUDIO)
        .map(|(_, d)| d)
        .collect()
}

/// `Some(true)` when the ffmpeg CLI ran and succeeded, `Some(false)` when it
/// ran and failed, and `None` only when the binary cannot be spawned at all.
///
/// Telling those three apart matters: a present-but-failing CLI (say, a build
/// without the PNG encoder) must surface as a failure, not as a skip, or the
/// scenario silently degrades into a no-op that still reports green.
fn cli_ran(args: &[&str]) -> Option<bool> {
    match Command::new("ffmpeg").args(args).status() {
        Ok(status) => Some(status.success()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => panic!("spawning the ffmpeg CLI failed: {err}"),
    }
}

/// Runs the CLI, asserting success. Returns `false` only when the CLI is
/// absent, which the callers below use as their skip condition.
fn cli(args: &[&str]) -> bool {
    match cli_ran(args) {
        Some(true) => true,
        Some(false) => panic!("ffmpeg CLI invocation failed: {args:?}"),
        None => false,
    }
}

// ---- fixtures ----
//
// Both are built through the ffmpeg CLI: the crate deliberately rejects a
// zero-stream input (`FindStreamError::NoStreamFound`), which is what an
// `ffmetadata` chapter source is, and `-disposition` is not a crate API. An
// environment without the CLI skips; a fixture that builds but lacks the
// property under test asserts, so the scenarios cannot silently no-op.

/// A FLAC whose cover art is an `attached_pic` PNG stream. Returns `None` only
/// when the ffmpeg CLI cannot be spawned; a CLI that runs but fails the render
/// or the remux asserts, so the scenario cannot silently degrade into a no-op.
fn cover_flac_fixture(name: &str) -> Option<String> {
    let png = tmp_path(&format!("{name}.cover.png"));
    if !cli(&[
        "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "color=c=red:s=120x120:d=1",
        "-frames:v", "1", &png,
    ]) {
        return None; // ffmpeg CLI absent -> skip
    }

    let out = tmp_path(&format!("{name}.flac"));
    assert!(
        cli(&[
            "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
            "-i", &png,
            "-map", "0:a", "-map", "1:v",
            "-c:a", "flac", "-c:v", "copy",
            "-disposition:v", "attached_pic",
            &out,
        ]),
        "cover fixture build failed"
    );
    assert!(
        has_attached_pic(&out),
        "fixture built without an attached_pic cover; the scenario below would be a no-op"
    );
    Some(out)
}

/// A matroska with two audio streams and NO default disposition on either.
/// `None` only when the ffmpeg CLI cannot be spawned.
fn two_audio_fixture(name: &str) -> Option<String> {
    let out = tmp_path(&format!("{name}.mkv"));
    if !cli(&[
        "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
        "-f", "lavfi", "-i", "sine=frequency=660:duration=1",
        "-map", "0:a", "-map", "1:a",
        "-disposition:a:0", "0", "-disposition:a:1", "0",
        &out,
    ]) {
        return None; // ffmpeg CLI absent -> skip
    }
    assert!(std::path::Path::new(&out).exists(), "two-audio fixture build failed");
    // Without two default-free audio streams the default-marking scenarios
    // below would assert nothing.
    let disps = audio_dispositions(&out);
    assert_eq!(
        disps.len(),
        2,
        "two-audio fixture did not produce two audio streams: {disps:?}"
    );
    assert!(
        disps.iter().all(|d| d & AV_DISPOSITION_DEFAULT == 0),
        "two-audio fixture carries a default already; the auto-mark scenario would be a no-op: {disps:?}"
    );
    Some(out)
}

/// A matroska with one video and one audio stream, both encoded. The
/// re-encode scenarios need streams the crate will decode and re-encode
/// rather than copy. `None` only when the ffmpeg CLI cannot be spawned.
///
/// `mpeg4` and `aac` are native encoders, so the fixture also builds under the
/// LGPL-minimum CI profile (`--disable-gpl`), where a build with `libx264` is
/// not guaranteed. The re-encode below uses the same pair.
fn reencode_fixture(name: &str) -> Option<String> {
    let out = tmp_path(&format!("{name}.mkv"));
    if !cli(&[
        "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "testsrc=size=160x120:rate=10:duration=1",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
        "-map", "0:v", "-map", "1:a",
        "-c:v", "mpeg4", "-c:a", "aac",
        &out,
    ]) {
        return None; // ffmpeg CLI absent -> skip
    }
    let disps = streams_of(&out);
    assert_eq!(disps.len(), 2, "re-encode fixture must carry two streams: {disps:?}");
    Some(out)
}

// ---- 1. inheritance ----

/// The core of #66: the cover survives a FLAC -> FLAC copy, and the output
/// stream carries `AV_DISPOSITION_ATTACHED_PIC`. Before the fix the output had
/// only the audio stream.
#[test]
fn attached_pic_disposition_is_inherited_flac_to_flac() {
    let Some(input) = cover_flac_fixture("inherit") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("inherit_out.flac");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(Output::from(out.as_str()))
            .build()
            .expect("build"),
        "flac cover copy",
    );

    assert!(
        has_attached_pic(&out),
        "cover art lost: output streams {:?}",
        streams_of(&out)
    );
    // Byte-for-byte the same size as the input proves the picture payload
    // survived, not just the disposition bit.
    let (a, b) = (
        std::fs::metadata(&input).unwrap().len(),
        std::fs::metadata(&out).unwrap().len(),
    );
    assert_eq!(a, b, "cover art payload changed size ({a} -> {b})");
}

/// A source stream's default disposition must reach the output too.
#[test]
fn default_disposition_is_inherited() {
    let Some(input) = two_audio_fixture("default_inherit") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };
    // Give the source a default-marked audio stream. The marking is explicit:
    // an unmarked single stream is NOT auto-defaulted by the CLI (the
    // automatic pass needs a second stream of the type to disambiguate).
    // `two_audio_fixture` already ran the CLI, so its absence is settled and a
    // failure here is a real one.
    let marked = tmp_path("marked.mkv");
    assert!(
        cli(&[
            "-y", "-loglevel", "error", "-i", &input,
            "-map", "0:a:0", "-c", "copy", "-disposition:a:0", "default", &marked,
        ]),
        "the default-disposition remux failed"
    );
    assert_eq!(
        audio_dispositions(&marked),
        vec![AV_DISPOSITION_DEFAULT],
        "the marked fixture must carry a default disposition"
    );

    let out = tmp_path("default_inherit_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(marked.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("copy").set_audio_codec("copy"))
            .build()
            .expect("build"),
        "default inheritance",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![AV_DISPOSITION_DEFAULT],
        "the input's default disposition was not inherited"
    );
}

// ---- 2. automatic default marking ----

/// Two output streams of one type, neither default in the source: the CLI marks
/// the first as default. This is the loop ez-ffmpeg never had.
#[test]
fn first_stream_of_a_type_is_marked_default() {
    let Some(input) = two_audio_fixture("auto_default") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("auto_default_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map_with_copy("0:a:0")
                    .add_stream_map_with_copy("0:a:1"),
            )
            .build()
            .expect("build"),
        "auto default marking",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![AV_DISPOSITION_DEFAULT, 0],
        "the first audio stream must be marked default and the second must not"
    );
}

/// With only one stream of a type there is nothing to disambiguate, so the
/// marking loop must not fire (CLI: `nb_streams[type + 1] < 2`).
#[test]
fn single_stream_of_a_type_is_not_marked_default() {
    let Some(input) = two_audio_fixture("single") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("single_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map_with_copy("0:a:1"),
            )
            .build()
            .expect("build"),
        "single stream no marking",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![0],
        "a lone stream of its type must not gain a default disposition"
    );
}

// ---- 3. the explicit override ----

/// `set_disposition` wins over the inherited value.
#[test]
fn set_disposition_overrides_inherited_value() {
    let Some(input) = cover_flac_fixture("override") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("override_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("copy")
                    .set_audio_codec("copy")
                    .set_disposition("v", "0")
                    .expect("valid specifier"),
            )
            .build()
            .expect("build"),
        "explicit disposition override",
    );

    // Assert the video stream survives with only the inherited bits cleared:
    // "no stream has attached_pic" would also pass if the stream were dropped.
    assert_eq!(
        video_dispositions(&out),
        vec![0],
        "the explicit `0` must leave the video stream present with no flags: {:?}",
        video_dispositions(&out)
    );
}

/// A user disposition that ADDS a flag the input did not carry.
///
/// Both audio streams are mapped explicitly: the crate's auto-mapping takes
/// one stream per media type, so it never produces the two-of-a-type layout
/// this scenario needs.
#[test]
fn set_disposition_adds_a_flag() {
    let Some(input) = two_audio_fixture("add_flag") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("add_flag_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map_with_copy("0:a:0")
                    .add_stream_map_with_copy("0:a:1")
                    .set_disposition("a:1", "default")
                    .expect("valid specifier"),
            )
            .build()
            .expect("build"),
        "explicit disposition add",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![0, AV_DISPOSITION_DEFAULT],
        "the explicit default must land on the second audio stream"
    );
}

/// Setting any disposition disables the automatic marking for every type, not
/// just the type the option named (CLI: a single global `have_manual`).
///
/// The manual `a:0` value clears a bit rather than adding one, which is the
/// sharpest form of the check: the first stream is a candidate Pass 2b would
/// otherwise mark default, so the observed `[0, 0]` proves the automatic pass
/// did not run. A specifier that matches no stream would NOT arm the manual
/// path (`opt_match_per_stream_str` only sets the value on a match), which is
/// why this names a real stream.
#[test]
fn any_manual_disposition_disables_automatic_marking() {
    let Some(input) = two_audio_fixture("manual_global") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("manual_global_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .add_stream_map_with_copy("0:a:0")
                    .add_stream_map_with_copy("0:a:1")
                    .set_disposition("a:0", "0")
                    .expect("valid specifier"),
            )
            .build()
            .expect("build"),
        "manual disposition disables auto marking",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![0, 0],
        "a manual disposition must suppress the automatic default marking"
    );
}

/// An invalid stream specifier is rejected at the builder, like
/// `add_stream_metadata`.
#[test]
fn invalid_disposition_specifier_is_rejected() {
    let output = Output::from("out.mkv").set_disposition("!!!", "default");
    assert!(output.is_err(), "a malformed stream specifier must not be accepted");
}

/// The unqualified form (`-disposition` with no stream specifier) applies to
/// every output stream, matching the CLI. The empty specifier is a wildcard,
/// not the malformed input the plain parser would reject it as.
#[test]
fn unqualified_disposition_applies_to_every_stream() {
    let Some(input) = reencode_fixture("unqualified") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("unqualified_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_disposition("", "forced")
                    .expect("the unqualified form must be accepted"),
            )
            .build()
            .expect("build"),
        "unqualified disposition",
    );

    let disps = streams_of(&out);
    // Assert the layout first: checking only "every stream found is forced"
    // would pass if one of the two streams had disappeared entirely, which is
    // exactly the failure mode an unqualified specifier could regress into.
    assert_eq!(
        disps.iter().filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_VIDEO).count(),
        1,
        "the unqualified scenario must keep its one video stream: {disps:?}"
    );
    assert_eq!(
        disps.iter().filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_AUDIO).count(),
        1,
        "the unqualified scenario must keep its one audio stream: {disps:?}"
    );
    for (media_type, disposition) in &disps {
        assert_ne!(
            disposition & AV_DISPOSITION_FORCED,
            0,
            "the unqualified forced flag did not reach the {media_type:?} stream: {disps:?}"
        );
    }
}

// ---- 4. re-encode ----
//
// An encoder stream is created with `avformat_new_stream(ctx, NULL)`, which
// leaves `codecpar.codec_type` at zero — and zero is `AVMEDIA_TYPE_VIDEO`.
// Every stream in a re-encode therefore looked like a video stream to both
// `set_dispositions` and `StreamSpecifier::matches` until the encoder ran
// `avcodec_parameters_from_context`, which happens well after both. The two
// tests below fail on that bug and pass once the type is recorded at stream
// creation; the older tests above only ever used stream copy, where
// `streamcopy_init` finalizes `codecpar` up front, so they could not see it.

/// A re-encode must honor an explicit per-stream override. With every stream
/// classified as video this dropped silently: `a:0` matched nothing.
#[test]
fn reencode_honors_manual_override() {
    let Some(input) = reencode_fixture("reencode_override") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("reencode_override_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("aac")
                    .set_disposition("a:0", "forced")
                    .expect("valid specifier"),
            )
            .build()
            .expect("build"),
        "re-encode manual override",
    );

    assert_eq!(
        audio_dispositions(&out),
        vec![AV_DISPOSITION_FORCED],
        "the re-encoded stream did not receive its explicit disposition"
    );
}

/// A re-encode of one video and one audio stream must not mark either default:
/// the automatic pass only fires for a media type with more than one output
/// stream. Misclassifying the audio stream as video made the video bucket
/// count 2 and wrongly marked the first stream default.
#[test]
fn reencode_does_not_mark_a_lone_stream_of_each_type_default() {
    let Some(input) = reencode_fixture("reencode_no_default") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("reencode_no_default_out.mkv");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac"),
            )
            .build()
            .expect("build"),
        "re-encode auto default",
    );

    let disps = streams_of(&out);
    assert_eq!(
        disps.iter().filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_VIDEO).count(),
        1,
        "expected exactly one video stream: {disps:?}"
    );
    assert_eq!(
        disps.iter().filter(|(t, _)| *t == AVMediaType::AVMEDIA_TYPE_AUDIO).count(),
        1,
        "expected exactly one audio stream: {disps:?}"
    );
    for (media_type, disposition) in &disps {
        assert_eq!(
            disposition & AV_DISPOSITION_DEFAULT,
            0,
            "a lone {media_type:?} stream gained DEFAULT: {disps:?}"
        );
    }
}

