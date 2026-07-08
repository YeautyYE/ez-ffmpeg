//! NEW-SC-03: sws/swr tuning for the auto-inserted scale/aresample filters.
//!
//! `FilterComplex::set_sws_opts`/`set_swr_opts` and `Output::set_sws_opts`/
//! `set_swr_opts` feed FFmpeg's graph-level `AVFilterGraph.scale_sws_opts` /
//! `aresample_swr_opts`. These options are consumed by the scale/aresample
//! filters libavfilter auto-inserts to reconcile format/size/rate mismatches.
//!
//! Strategy: give the auto-inserted filter an option string FFmpeg rejects and
//! assert the graph fails to configure. That failure can only happen if the
//! value actually reached the graph — i.e. it proves the field is applied. A
//! matching opt-free pipeline over the same conversion path must still succeed
//! (default `None` = today's behavior). Finally, two outputs of one graph asking
//! for *different* non-empty sws values must be rejected, because FFmpeg's field
//! is graph-level and we must not silently pick one.

use ez_ffmpeg::core::context::filter_complex::FilterComplex;
use ez_ffmpeg::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_sws_swr_tuning_tests_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// A hang is a test failure, not a suite timeout.
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

/// An option key no scale/aresample filter defines, so `avfilter_init_str`
/// rejects it and the graph fails to configure.
const BOGUS_OPT: &str = "ez_ffmpeg_nonexistent_option=1";

/// Compat / canary: the same rgb24 -> yuv420p auto-scale path, WITHOUT any
/// sws opts, must still succeed. This both proves default `None` is a no-op
/// regression-wise and confirms the mpeg4 encoder is present, so the invalid
/// opts test below fails because of the opts, not a missing codec.
#[test]
fn auto_scale_pipeline_without_opts_succeeds() {
    let out = tmp_path("compat_no_opts.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30:duration=1").set_format("lavfi"))
            // format=rgb24 forces the graph output to rgb24; mpeg4 needs
            // yuv420p, so libavfilter auto-inserts a scale/format filter.
            .filter_desc("format=rgb24")
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(5),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "compat no opts",
    );
    assert!(
        result.is_ok(),
        "an auto-scale pipeline with no sws opts must still succeed: {result:?}"
    );

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert_eq!(nb_frames, 5, "compat pipeline must produce its frames");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

/// Video: an invalid graph-level sws opt (via `FilterComplex::set_sws_opts`)
/// must make the auto-inserted scale filter fail to configure.
#[test]
fn invalid_sws_opts_fails_graph_config() {
    let out = tmp_path("invalid_sws.mp4");
    let filter = FilterComplex::from("format=rgb24").set_sws_opts(BOGUS_OPT);
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30:duration=1").set_format("lavfi"))
            .filter_desc(filter)
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(5),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "invalid sws opts",
    );
    assert!(
        result.is_err(),
        "an invalid scale_sws_opts must fail graph configuration (proves the field is applied)"
    );
}

/// Audio compat / canary: the same 44100 -> 48000 auto-aresample path, WITHOUT
/// any swr opts, must still succeed. Confirms the aac encoder + resample path
/// works, so the invalid-swr test below fails because of the opts, not an
/// unrelated audio runtime failure.
#[test]
fn auto_resample_pipeline_without_opts_succeeds() {
    let out = tmp_path("compat_no_swr_opts.m4a");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=1").set_format("lavfi"),
            )
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("aac")
                    .set_audio_sample_rate(48000)
                    .set_max_audio_frames(20),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "compat no swr opts",
    );
    assert!(
        result.is_ok(),
        "an auto-resample pipeline with no swr opts must still succeed: {result:?}"
    );
    assert!(
        find_audio_stream_info(&out)
            .expect("failed to probe output")
            .is_some(),
        "compat audio pipeline must produce an audio stream"
    );
}

/// Audio: an invalid per-output swr opt (via `Output::set_swr_opts`) must make
/// the auto-inserted aresample filter fail to configure. The 44100 -> 48000
/// sample-rate change forces the aresample insertion.
#[test]
fn invalid_swr_opts_fails_graph_config() {
    let out = tmp_path("invalid_swr.m4a");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from("sine=frequency=440:sample_rate=44100:duration=1").set_format("lavfi"),
            )
            .output(
                Output::from(out.as_str())
                    .set_audio_codec("aac")
                    .set_audio_sample_rate(48000)
                    .set_max_audio_frames(20)
                    .set_swr_opts(BOGUS_OPT),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "invalid swr opts",
    );
    assert!(
        result.is_err(),
        "an invalid aresample_swr_opts must fail graph configuration (proves the field is applied)"
    );
}

/// Conflict: FFmpeg's sws opts are graph-level. When one filtergraph drives two
/// outputs that request DIFFERENT non-empty sws values, we must reject it rather
/// than silently pick one — the resolver returns `FilterGraphParseError::InvalidArgument`.
#[test]
fn conflicting_per_output_sws_opts_is_invalid_argument() {
    let out_a = tmp_path("conflict_a.mp4");
    let out_b = tmp_path("conflict_b.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30:duration=1").set_format("lavfi"))
            // One graph, two output pads -> two OutputFilters bound to it.
            .filter_desc("[0:v]split=2[a][b]")
            .output(
                Output::from(out_a.as_str())
                    .add_stream_map("[a]")
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(5)
                    .set_sws_opts("flags=bilinear"),
            )
            .output(
                Output::from(out_b.as_str())
                    .add_stream_map("[b]")
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(5)
                    .set_sws_opts("flags=bicubic"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "conflicting sws opts",
    );
    assert!(
        result.is_err(),
        "two outputs of one graph with different sws opts must be rejected, not silently merged"
    );
    let err = result.unwrap_err();
    let debug = format!("{err:?}");
    assert!(
        debug.contains("InvalidArgument"),
        "the graph-level sws conflict must surface as InvalidArgument, got: {debug}"
    );
}
