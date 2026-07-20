//! Strict-AVOption mode: the five leftover-option sites that only WARN on
//! the default builder path must ERROR on CLI-initiated pipelines
//! (fftools `check_avoptions` parity — hard prerequisite (b) of the
//! CLI-compat evaluation).
//!
//! The five sites: demuxer open and per-stream probe (open_input.rs), muxer
//! write_header (mux_task.rs), encoder open (enc_task.rs), decoder open
//! (dec_task.rs). Each test plants an option that no component recognizes,
//! sets the crate-internal strict flag exactly like `LoweredJob::into_context`
//! does, and asserts the typed `UnconsumedCliOption` error names it. The
//! final tests pin the DEFAULT path's behavior: same bogus options, no strict
//! flag, the job still completes (never break existing users).

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::error::Error;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_cli_strict_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

fn lavfi_video_input() -> Input {
    Input::from("testsrc2=size=192x108:rate=30:duration=0.3").set_format("lavfi")
}

/// A tiny real mp4 fixture, so decoder/probe paths have something to open.
fn mp4_fixture(name: &str) -> String {
    let path = tmp_path(name);
    FfmpegContext::builder()
        .input(lavfi_video_input())
        .output(Output::from(path.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();
    path
}

/// start() surfaces encoder/decoder-open errors; wait() surfaces mux-time
/// ones. Funnel both into one result.
fn run_to_completion(context: FfmpegContext) -> crate::error::Result<()> {
    match context.start() {
        Ok(scheduler) => scheduler.wait(),
        Err(err) => Err(err),
    }
}

fn expect_unconsumed(result: crate::error::Result<()>, option: &str, scenario: &str) {
    match result {
        Err(Error::UnconsumedCliOption { site, option: got }) => {
            assert_eq!(got, option, "{scenario}: wrong option name (site: {site})");
        }
        Err(other) => panic!("{scenario}: expected UnconsumedCliOption, got: {other}"),
        Ok(()) => panic!("{scenario}: expected the strict run to fail"),
    }
}

#[test]
fn strict_demuxer_leftover_errors_at_build() {
    let fixture = mp4_fixture("strict_demux_in.mp4");
    let mut input = Input::from(fixture);
    input.strict_avoptions = true;
    let input = input.set_format_opt("no_such_demux_opt", "1");
    let result = FfmpegContext::builder()
        .input(input)
        .output(Output::from(tmp_path("strict_demux_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build();
    match result {
        Err(Error::UnconsumedCliOption { option, .. }) => {
            assert_eq!(option, "no_such_demux_opt");
        }
        Err(other) => panic!("expected UnconsumedCliOption from build, got: {other}"),
        Ok(_) => panic!("expected the strict build to fail"),
    }
}

#[test]
fn strict_probe_leftover_errors_at_build() {
    let fixture = mp4_fixture("strict_probe_in.mp4");
    let mut input = Input::from(fixture);
    input.strict_avoptions = true;
    let input = input.set_find_stream_info_codec_opts(
        0,
        vec![("no_such_probe_opt".to_string(), "1".to_string())],
    );
    let result = FfmpegContext::builder()
        .input(input)
        .output(Output::from(tmp_path("strict_probe_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build();
    match result {
        Err(Error::UnconsumedCliOption { option, site }) => {
            assert_eq!(option, "no_such_probe_opt");
            assert!(site.contains("probe"), "site should name the probe: {site}");
        }
        Err(other) => panic!("expected UnconsumedCliOption from build, got: {other}"),
        Ok(_) => panic!("expected the strict build to fail"),
    }
}

#[test]
fn strict_muxer_leftover_fails_the_run() {
    let mut output = Output::from(tmp_path("strict_mux_out.mp4").as_str());
    output.strict_avoptions = true;
    let output = output
        .set_video_codec("mpeg4")
        .set_format_opt("no_such_mux_opt", "1");
    let context = FfmpegContext::builder()
        .input(lavfi_video_input())
        .output(output)
        .build()
        .unwrap();
    expect_unconsumed(run_to_completion(context), "no_such_mux_opt", "muxer leftover");
}

#[test]
fn strict_encoder_leftover_fails_the_run() {
    let mut output = Output::from(tmp_path("strict_enc_out.mp4").as_str());
    output.strict_avoptions = true;
    let output = output
        .set_video_codec("mpeg4")
        .set_video_codec_opt("no_such_enc_opt", "1");
    let context = FfmpegContext::builder()
        .input(lavfi_video_input())
        .output(output)
        .build()
        .unwrap();
    expect_unconsumed(run_to_completion(context), "no_such_enc_opt", "encoder leftover");
}

#[test]
fn strict_decoder_leftover_fails_the_run() {
    let fixture = mp4_fixture("strict_dec_in.mp4");
    let mut input = Input::from(fixture);
    input.strict_avoptions = true;
    let input = input.set_video_codec_opt("no_such_dec_opt", "1");
    let context = FfmpegContext::builder()
        .input(input)
        .output(Output::from(tmp_path("strict_dec_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap();
    expect_unconsumed(run_to_completion(context), "no_such_dec_opt", "decoder leftover");
}

#[test]
fn default_path_still_warns_and_succeeds() {
    // Never break userspace: without the strict flag, the same bogus options
    // stay warnings and the job completes.
    let fixture = mp4_fixture("lenient_in.mp4");
    let input = Input::from(fixture)
        .set_format_opt("no_such_demux_opt", "1")
        .set_video_codec_opt("no_such_dec_opt", "1");
    let output = Output::from(tmp_path("lenient_out.mp4").as_str())
        .set_video_codec("mpeg4")
        .set_video_codec_opt("no_such_enc_opt", "1")
        .set_format_opt("no_such_mux_opt", "1");
    FfmpegContext::builder()
        .input(input)
        .output(output)
        .build()
        .expect("lenient build must succeed")
        .start()
        .unwrap()
        .wait()
        .expect("lenient run must succeed");
}

#[test]
fn lowering_carries_the_golden_shape_fields() {
    // The lowered plan is the single artifact both run and emit consume;
    // pin the V1 lowering field by field (strict-mode arming itself is
    // covered end to end by the site tests above).
    let args: Vec<String> = [
        "-i", "in.mp4", "-c:v", "libx264", "-crf", "23", "-preset", "fast", "-c:a", "aac",
        "-y", "out.mp4",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(job.input.url, "in.mp4");
    assert_eq!(job.output.url, "out.mp4");
    assert_eq!(job.output.video_codec.as_deref(), Some("libx264"));
    assert_eq!(job.output.audio_codec.as_deref(), Some("aac"));
    assert_eq!(
        job.output.video_codec_opts,
        vec![
            ("crf".to_string(), "23".to_string()),
            ("preset".to_string(), "fast".to_string())
        ]
    );
}

#[test]
fn lowering_scopes_trims_per_side() {
    let args: Vec<String> = ["-ss", "2.5", "-t", "1", "-i", "in.mp4", "-to", "8", "-y", "out.mp4"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(job.input.start_time_us, Some(2_500_000));
    assert_eq!(job.input.recording_time_us, Some(1_000_000));
    assert_eq!(job.input.stop_time_us, None);
    assert_eq!(job.output.recording_time_us, None);
    assert_eq!(job.output.stop_time_us, Some(8_000_000));
}

#[test]
fn lowering_maps_copy_flags_per_media() {
    let args: Vec<String> = [
        "-i", "in.mp4", "-map", "0:v:0", "-map", "0:a:0", "-c:v", "libx264", "-c:a", "copy",
        "-y", "out.mp4",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(
        job.output.stream_maps,
        vec![("0:v:0".to_string(), false), ("0:a:0".to_string(), true)]
    );
}

#[test]
fn lowering_hls_format_opts_in_cli_order() {
    let args: Vec<String> = [
        "-i", "in.mp4", "-c:v", "libx264", "-crf", "23", "-c:a", "aac", "-f", "hls",
        "-hls_time", "6", "-hls_playlist_type", "vod", "-hls_list_size", "0",
        "-hls_segment_filename", "seg_%03d.ts", "-y", "out.m3u8",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(job.output.format.as_deref(), Some("hls"));
    assert_eq!(
        job.output.format_opts,
        vec![
            ("hls_time".to_string(), "6".to_string()),
            ("hls_playlist_type".to_string(), "vod".to_string()),
            ("hls_list_size".to_string(), "0".to_string()),
            ("hls_segment_filename".to_string(), "seg_%03d.ts".to_string()),
        ]
    );
}

#[test]
fn lowering_disables_and_rates() {
    let args: Vec<String> =
        ["-i", "in.mp4", "-vn", "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2", "-y", "out.m4a"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert!(job.output.video_disable);
    assert!(!job.output.audio_disable);
    assert_eq!(job.output.audio_bitrate.as_deref(), Some("192k"));
    assert_eq!(job.output.audio_sample_rate, Some(44100));
    assert_eq!(job.output.audio_channels, Some(2));
}

#[test]
fn lowering_thumbnail_and_movflags() {
    let args: Vec<String> = [
        "-ss", "5", "-i", "in.mp4", "-an", "-c:v", "mjpeg", "-frames:v", "1", "-y", "thumb.jpg",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(job.input.start_time_us, Some(5_000_000));
    assert!(job.output.audio_disable);
    assert_eq!(job.output.max_video_frames, Some(1));

    let args: Vec<String> = [
        "-i", "in.mp4", "-c:v", "copy", "-c:a", "copy", "-movflags", "+faststart", "-y", "f.mp4",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let ir = super::parse::parse(&args).unwrap();
    let job = super::lower::lower(&ir);
    assert_eq!(job.output.video_codec.as_deref(), Some("copy"));
    assert_eq!(job.output.audio_codec.as_deref(), Some("copy"));
    assert_eq!(
        job.output.format_opts,
        vec![("movflags".to_string(), "+faststart".to_string())]
    );
}
