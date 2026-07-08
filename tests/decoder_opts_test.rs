//! NEW-SC-01 / NEW-SC-02 regression net: per-media decoder codec opts on
//! `Input` and `avformat_find_stream_info` control.
//!
//! - The default path is unchanged: probing stays on, stream metadata is
//!   discovered, and decoders still open with the internal `threads=auto`.
//! - `Input::set_video_codec_opt` values reach `avcodec_open2`; an unknown
//!   key is surfaced as a warning, never an error (fftools warns/aborts on
//!   check_avoptions — we warn).
//! - `skip_frame=nokey` on the decoder yields fewer decoded frames than a
//!   default decode of the same fixture.
//! - `set_find_stream_info(false)` still processes a container whose header
//!   exposes complete streams at open (mp4).
//! - An out-of-range `set_find_stream_info_codec_opt` stream index fails at
//!   build time with an error instead of probing garbage.

use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{AVMediaType, FfmpegContext, FfmpegScheduler, Frame, Input, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_decoder_opts_tests_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

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

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

/// 0.3s finite video fixture (mpeg4 in mp4, 30fps, single GOP: one keyframe).
fn video_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "video fixture",
    )
    .unwrap();
    path
}

/// 0.3s raw yuv420p 320x240 @ 30fps elementary stream. A rawvideo file carries
/// no header, so it is the canonical "known-format" input: the demuxer takes
/// width/height/pixel_format/framerate from the caller's input opts, needing
/// no `avformat_find_stream_info` read-ahead at all.
fn rawvideo_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_format("rawvideo")
                    .set_video_codec("rawvideo")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "rawvideo fixture",
    )
    .unwrap();
    path
}

/// Reads the rawvideo fixture as a known-format input: format + dimensions are
/// declared up front, so probing can be safely disabled.
fn rawvideo_known_input(src: &str) -> Input {
    Input::from(src)
        .set_format("rawvideo")
        .set_input_opt("video_size", "320x240")
        .set_input_opt("pixel_format", "yuv420p")
        .set_input_opt("framerate", "30")
}

fn assert_output_has_video(path: &str, scenario: &str) {
    assert!(
        find_video_stream_info(path)
            .expect("failed to probe output")
            .is_some(),
        "scenario `{scenario}`: output must contain a video stream"
    );
}

/// Counts the decoded (post-decode, pre-encode) video frames of an input.
struct FrameCounter {
    count: Arc<AtomicUsize>,
}

impl FrameFilter for FrameCounter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            if !frame.as_ptr().is_null() && !frame.is_empty() {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(Some(frame))
    }
}

/// Transcodes `input` and returns how many video frames its decoder produced.
fn count_decoded_frames(input: Input, out_name: &str, scenario: &str) -> usize {
    let count = Arc::new(AtomicUsize::new(0));
    let counter = FrameCounter {
        count: Arc::clone(&count),
    };
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter("frame_counter", Box::new(counter));
    let out = tmp_path(out_name);
    run(
        FfmpegContext::builder()
            .input(input.add_frame_pipeline(pipeline))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        scenario,
    )
    .unwrap();
    count.load(Ordering::SeqCst)
}

#[test]
fn default_path_still_discovers_metadata_and_transcodes() {
    let src = video_fixture("default_src.mp4");

    // Probing stays on by default: stream metadata must be discovered.
    match find_video_stream_info(&src)
        .expect("failed to probe fixture")
        .expect("fixture has no video stream")
    {
        StreamInfo::Video { width, height, .. } => {
            assert_eq!((width, height), (320, 240));
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    // No decoder opts set: the internal threads=auto default still applies
    // and the transcode succeeds exactly as before.
    let out = tmp_path("default_out.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(src.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "default transcode",
    );
    assert!(result.is_ok(), "default transcode failed: {result:?}");
    assert_output_has_video(&out, "default transcode");
}

#[test]
fn video_decoder_opt_reaches_decoder_open() {
    let src = video_fixture("threads_src.mp4");

    // A user-provided threads value must reach avcodec_open2 (and not be
    // overwritten by the internal auto default) without breaking the run.
    let out = tmp_path("threads_out.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(src.as_str()).set_video_codec_opt("threads", "1"))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "threads=1 decode",
    );
    assert!(result.is_ok(), "decode with threads=1 failed: {result:?}");
    assert_output_has_video(&out, "threads=1 decode");
}

#[test]
fn unknown_decoder_opt_warns_but_does_not_fail() {
    let src = video_fixture("unknown_opt_src.mp4");

    let out = tmp_path("unknown_opt_out.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(
                Input::from(src.as_str())
                    .set_video_codec_opt("definitely_not_a_decoder_option", "1"),
            )
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "unknown decoder opt",
    );
    assert!(
        result.is_ok(),
        "an unrecognized decoder opt must warn, not fail: {result:?}"
    );
    assert_output_has_video(&out, "unknown decoder opt");
}

#[test]
fn skip_frame_nokey_decodes_fewer_frames() {
    let src = video_fixture("skip_frame_src.mp4");

    let default_frames = count_decoded_frames(
        Input::from(src.as_str()),
        "skip_frame_default_out.mp4",
        "default frame count",
    );
    let nokey_frames = count_decoded_frames(
        Input::from(src.as_str()).set_video_codec_opt("skip_frame", "nokey"),
        "skip_frame_nokey_out.mp4",
        "nokey frame count",
    );

    // 0.3s @ 30fps in one mpeg4 GOP: many frames by default, only the
    // keyframe(s) with skip_frame=nokey.
    assert!(
        default_frames >= 5,
        "fixture must decode several frames by default, got {default_frames}"
    );
    assert!(
        nokey_frames >= 1,
        "nokey decode must still produce the keyframe, got {nokey_frames}"
    );
    assert!(
        nokey_frames < default_frames,
        "skip_frame=nokey must decode fewer frames ({nokey_frames}) than default ({default_frames})"
    );
}

#[test]
fn find_stream_info_disabled_opens_known_format_input() {
    // A known-format (rawvideo) input opens fine without probing: dimensions
    // come from the input opts, so with probing disabled the input still
    // opens, passes the nb_streams>0 sanity check, and BUILDS successfully.
    // (Build opens every input and would surface FindStreamError here
    // otherwise.)
    let src = rawvideo_fixture("no_probe_src.yuv");

    let built = FfmpegContext::builder()
        .input(rawvideo_known_input(&src).set_find_stream_info(false))
        .output(Output::from(tmp_path("no_probe_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build();
    // FfmpegContext is not Debug, so only surface the error variant.
    if let Err(e) = built {
        panic!("known-format input with probing disabled must still open: {e:?}");
    }
}

#[test]
fn find_stream_info_disabled_still_transcodes_known_format() {
    // The full low-latency use case: a known-format rawvideo input with
    // probing disabled must still transcode end-to-end, because the caller
    // supplied everything the header would otherwise be probed for.
    let src = rawvideo_fixture("no_probe_transcode_src.yuv");

    let out = tmp_path("no_probe_transcode_out.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(rawvideo_known_input(&src).set_find_stream_info(false))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "probing disabled transcode",
    );
    assert!(
        result.is_ok(),
        "known-format input with probing disabled must still transcode: {result:?}"
    );
    assert_output_has_video(&out, "probing disabled transcode");
}

#[test]
fn probing_codec_opt_is_applied_without_error() {
    let src = video_fixture("probe_opt_src.mp4");

    // A valid probing-only codec opt on a valid stream index must be accepted
    // and consumed by avformat_find_stream_info.
    let out = tmp_path("probe_opt_out.mp4");
    let result = run(
        FfmpegContext::builder()
            .input(Input::from(src.as_str()).set_find_stream_info_codec_opt(
                0,
                "skip_frame",
                "nokey",
            ))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap(),
        "probing codec opt",
    );
    assert!(result.is_ok(), "probing codec opt run failed: {result:?}");
    assert_output_has_video(&out, "probing codec opt");
}

#[test]
fn probing_opt_with_out_of_range_stream_index_fails_at_build() {
    let src = video_fixture("probe_bad_index_src.mp4");

    // The fixture has a single stream; index 99 must be rejected while the
    // input is opened (FfmpegContext::builder().build()).
    let result = FfmpegContext::builder()
        .input(Input::from(src.as_str()).set_find_stream_info_codec_opt(99, "skip_frame", "nokey"))
        .output(Output::from(tmp_path("probe_bad_index_out.mp4").as_str()))
        .build();
    assert!(
        result.is_err(),
        "an out-of-range probing stream index must fail the build"
    );
}
