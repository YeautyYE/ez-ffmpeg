//! PR-2 regression net: tail-frame integrity and false-success paths.
//!
//! - H6: hitting max_frames/recording_time must not drop frames still buffered
//!   inside the encoder (B-frame lookahead tail) — the output must contain
//!   exactly the requested number of frames.
//! - H7: a deferred write_header failure must surface as Err from wait(),
//!   not silently produce a broken file with Ok(()).
//! - Zero-frame streams: FFmpeg CLI initializes the encoder with a dummy
//!   parameters-only frame (fftools close_output + send_to_enc), writes an
//!   empty output and exits 0. ez-ffmpeg must match: no hang, no misleading
//!   encode error, a valid (empty) output.

use ez_ffmpeg::error::{Error, MuxingOperationError};
use ez_ffmpeg::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir =
        std::env::temp_dir().join(format!("ez_ffmpeg_tail_frame_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

fn lavfi_video() -> Input {
    Input::from("color=c=black:s=320x240:r=30").set_format("lavfi")
}

fn lavfi_audio() -> Input {
    Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi")
}

/// A hang is a test failure, not a test timeout: run wait() on a helper
/// thread and panic if it has not returned within `secs`.
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

fn video_nb_frames(path: &str) -> i64 {
    match find_video_stream_info(path)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => nb_frames,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn max_frames_keeps_encoder_buffered_tail() {
    let out = tmp_path("max_frames_bframes.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                // B-frames give the encoder a reorder buffer: the last frames
                // sent before the limit are still inside the encoder when the
                // limit hits, and are lost unless the encoder is flushed.
                .set_video_codec_opt("bf", "2")
                .set_max_video_frames(10),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // The source is infinite: a broken limit shows up as a hang, not an Err.
    let result = wait_with_watchdog(scheduler, 60, "max_frames with B-frames");
    assert!(result.is_ok(), "max_frames task failed: {result:?}");
    let frames = video_nb_frames(&out);
    assert_eq!(
        frames, 10,
        "output must contain exactly max_video_frames frames; \
         a smaller count means the encoder's buffered tail was dropped"
    );
}

#[test]
fn recording_time_keeps_encoder_buffered_tail() {
    let out = tmp_path("recording_time_bframes.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_video_codec_opt("bf", "2")
                // 500ms at 30fps: frames with pts 0..=14/30 → exactly 15.
                .set_recording_time_us(500_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // The source is infinite: a broken limit shows up as a hang, not an Err.
    let result = wait_with_watchdog(scheduler, 60, "recording_time with B-frames");
    assert!(result.is_ok(), "recording_time task failed: {result:?}");
    let frames = video_nb_frames(&out);
    assert_eq!(
        frames, 15,
        "output must contain every frame below the recording_time limit; \
         a smaller count means the encoder's buffered tail was dropped"
    );
}

#[test]
fn deferred_write_header_failure_returns_err() {
    // rawvideo has no tag in the mp4/mov container: avformat_write_header
    // rejects it. The header is written by the deferred mux-init thread, so
    // this exercises the "error only logged, wait() returns Ok" path.
    let out = tmp_path("write_header_fail.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video())
        .output(Output::from(out.as_str()).set_video_codec("rawvideo"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "write_header failure");
    match result {
        Err(Error::Muxing(MuxingOperationError::WriteHeader(_))) => {}
        other => {
            panic!("write_header failure must surface as Err(Muxing(WriteHeader)), got {other:?}")
        }
    }
}

/// The mp4 muxer drops a track that received no packets; a present track must
/// at least be empty. Any actual frames mean dropped frames leaked through.
fn assert_video_stream_empty_or_absent(path: &str, scenario: &str) {
    match find_video_stream_info(path).expect("failed to probe output") {
        None => {}
        Some(StreamInfo::Video { nb_frames, .. }) => assert_eq!(
            nb_frames, 0,
            "scenario `{scenario}`: video stream should be empty"
        ),
        Some(other) => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn zero_frame_stream_completes_like_cli() {
    // Fixture: a short finite video-only file.
    let fixture = tmp_path("fixture_zero_frames.mp4");
    FfmpegContext::builder()
        .input(lavfi_video())
        .output(
            Output::from(fixture.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(500_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    // select=0 drops every decoded frame. CLI semantics: the encoder is
    // initialized from the filtergraph's dummy parameters-only frame, the
    // output is written empty, and the run succeeds (exit 0 + a warning).
    let out = tmp_path("zero_frames_out.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from(fixture.as_str()))
        .filter_desc("select=0")
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "zero-frame stream");
    assert!(
        result.is_ok(),
        "zero-frame stream must complete like FFmpeg CLI (empty output, success), got {result:?}"
    );
    assert!(
        std::fs::metadata(&out).is_ok(),
        "an (empty) output file must still be written"
    );
    assert_video_stream_empty_or_absent(&out, "zero-frame stream");
}

#[test]
fn zero_frame_sibling_does_not_hang_healthy_stream() {
    // Fixture: a short finite file with both video and audio.
    let fixture = tmp_path("fixture_av.mp4");
    FfmpegContext::builder()
        .input(lavfi_video())
        .input(lavfi_audio())
        .output(
            Output::from(fixture.as_str())
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_recording_time_us(500_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    // Video is killed by select=0, audio stays healthy. The video encoder
    // must still open (via the dummy init frame) so the muxer becomes ready;
    // otherwise the audio encoder blocks forever on the pre-mux channel.
    // CLI semantics: success, audio intact, empty/absent video track.
    let out = tmp_path("sibling_out.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from(fixture.as_str()))
        .filter_desc("select=0")
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_audio_codec("aac"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "zero-frame sibling");
    assert!(
        result.is_ok(),
        "a zero-frame sibling stream must not fail or hang the healthy stream, got {result:?}"
    );
    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("audio stream missing from output")
    {
        StreamInfo::Audio { nb_frames, .. } => assert!(
            nb_frames > 0,
            "healthy audio stream must reach the output, got nb_frames={nb_frames}"
        ),
        other => panic!("expected audio stream info, got {other:?}"),
    }
    assert_video_stream_empty_or_absent(&out, "zero-frame sibling");
}

/// Audio filtergraph frame accounting: every audio frame leaving a
/// filtergraph must reach the output. The audio branch moves the sink
/// frame's buffers into the pooled shell via av_frame_move_ref instead of
/// av_frame_ref-copying them; a regression that lost or mis-accounted a frame
/// here would truncate the track. A 44100 -> 48000 sample-rate change forces
/// the auto-aresample filtergraph, so the whole 1s track flows through that
/// audio branch.
#[test]
fn audio_filtergraph_output_is_not_truncated() {
    let out = tmp_path("audio_fg_full.m4a");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:sample_rate=44100:duration=1").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_audio_codec("aac")
                .set_audio_sample_rate(48000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "audio filtergraph full");
    assert!(
        result.is_ok(),
        "audio-through-filtergraph transcode must succeed, got {result:?}"
    );
    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("audio stream missing from output")
    {
        StreamInfo::Audio { nb_frames, .. } => assert!(
            // 1s @ 48000 through aac (frame_size 1024) is ~47 frames. The
            // threshold sits well above 1 but below the true count to tolerate
            // codec priming/padding while still catching a truncated track.
            nb_frames >= 30,
            "the full audio track must reach the output (a move_ref frame-loss \
             regression would truncate it), got nb_frames={nb_frames}"
        ),
        other => panic!("expected audio stream info, got {other:?}"),
    }
}
