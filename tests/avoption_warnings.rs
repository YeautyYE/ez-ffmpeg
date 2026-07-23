//! The lenient AVOption tier must stay observable: options that no demuxer
//! or muxer consumed are reported as warnings (fftools `check_avoptions`
//! aborts there; the builder API warns and continues). A regression that
//! silently swallows the leftover keys would pass every functional test —
//! these tests pin that the warning actually fires, via a process-global
//! capture logger (`tests/common`).
//!
//! Scope: the demuxer-open site (input options) and the muxer-header site
//! (output format options). Each scenario uses a unique option name so the
//! assertions cannot satisfy each other when tests run in parallel.

mod common;

use common::{assert_warning_containing, init_capture_logger, tmp_path_in};
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn lavfi_input() -> Input {
    Input::from("color=c=black:s=64x64:r=10").set_format("lavfi")
}

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_avoption_warning_tests", name)
}

/// An input option no demuxer recognizes must produce the documented
/// warning at open time (open happens inside `build()`).
#[test]
fn unconsumed_demuxer_option_warns_at_open() {
    init_capture_logger();

    let out = tmp_path("demuxer_opt_out.mp4");
    let ctx = FfmpegContext::builder()
        .input(lavfi_input().set_format_opt("ez_unknown_demuxer_opt", "1"))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(200_000),
        )
        .build();
    // The unknown option is lenient: the job must still build.
    assert!(
        ctx.is_ok(),
        "unknown demuxer option must not fail build(): {:?}",
        ctx.err()
    );

    assert_warning_containing("Option 'ez_unknown_demuxer_opt' was not recognized by input 0");
}

/// An output format option no muxer recognizes must produce the documented
/// warning when the muxer writes its header (run time).
#[test]
fn unconsumed_muxer_option_warns_at_header() {
    init_capture_logger();

    let out = tmp_path("muxer_opt_out.mp4");
    let result = FfmpegContext::builder()
        .input(lavfi_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(200_000)
                .set_format_opt("ez_unknown_muxer_opt", "1"),
        )
        .build()
        .expect("build")
        .start()
        .expect("start")
        .wait();
    // The unknown option is lenient: the job must still succeed.
    assert!(result.is_ok(), "unknown muxer option must not fail the job: {result:?}");

    assert_warning_containing("Option 'ez_unknown_muxer_opt' was not recognized by output 0");
}
