//! PR-9 regression net: fftools scheduler behaviors.
//!
//! - Input::set_framerate must force the CFR grid through the DECODE path
//!   (fftools DECODER_FLAG_FRAMERATE_FORCED), not only the demuxer's DTS
//!   estimation: 30 input frames at -r 60 produce a 0.5s/60fps output.
//! - stream_loop across a transcode must produce N*loops frames with
//!   monotonic timestamps (loop end_pts bookkeeping).
//! - The demuxer must stop once every stream's consumers are done
//!   (ffmpeg_demux.c do_send nb_streams_finished bookkeeping): an infinite
//!   source with per-stream frame limits must terminate, not spin forever.

use ez_ffmpeg::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_sched_port_tests_{}",
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

fn fixture_30_frames(name: &str) -> String {
    let path = tmp_path(name);
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "fixture",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");
    path
}

#[test]
fn input_framerate_forces_cfr_grid_through_decode() {
    let fixture = fixture_30_frames("fr_fixture.mp4");

    // ffmpeg -r 60 -i fixture -c:v mpeg4: 30 frames restamped onto a 60fps
    // grid — same frame count, half the duration (CLI: nb_frames=30,
    // avg_frame_rate=60/1, duration=0.5).
    let out = tmp_path("fr_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).set_framerate(60, 1))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "forced framerate",
    );
    assert!(result.is_ok(), "forced-framerate task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            nb_frames,
            avg_frame_rate,
            duration,
            time_base,
            ..
        } => {
            assert_eq!(nb_frames, 30, "every input frame must be restamped, not dropped");
            let fps = avg_frame_rate.num as f64 / avg_frame_rate.den as f64;
            assert!(
                (fps - 60.0).abs() < 0.5,
                "output must be on the forced 60fps grid, got {fps}"
            );
            let secs = duration as f64 * time_base.num as f64 / time_base.den as f64;
            assert!(
                (secs - 0.5).abs() < 0.1,
                "30 frames at a forced 60fps last 0.5s, got {secs}s"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn demuxer_stops_when_all_consumers_are_done() {
    // Infinite lavfi source with one video and one audio stream, both
    // destinations bounded by frame limits. demux_send must report
    // AVERROR_EOF once each stream's own consumers finished — counting
    // *all* destinations against one stream's done-count never terminates
    // (ffmpeg_sched.c demux_send_for_stream: nb_done == ds->nb_dst).
    let out = tmp_path("consumers_done.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from(
                    "testsrc=size=320x240:rate=30[out0];\
                     sine=frequency=440:sample_rate=44100[out1]",
                )
                .set_format("lavfi"),
            )
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_max_video_frames(20)
                    .set_max_audio_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        30,
        "all consumers done",
    );
    assert!(result.is_ok(), "bounded transcode failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert_eq!(nb_frames, 20, "video stream must respect its frame limit");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    // The video consumers finish first (20 frames < 31 aac frames): the
    // demuxer must keep feeding audio afterwards instead of stopping at the
    // first finished stream (ffmpeg_demux.c do_send marks the stream
    // finished and continues until nb_streams_finished == nb_streams_used).
    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no audio stream")
    {
        StreamInfo::Audio { nb_frames, .. } => {
            // 30 encoded frames + the AAC priming packet, same as the CLI
            // (`-frames:v 20 -frames:a 30` probes video=20, audio=31).
            assert_eq!(
                nb_frames, 31,
                "audio must keep flowing after the video consumers finish"
            );
        }
        other => panic!("expected audio stream info, got {other:?}"),
    }
}

#[test]
fn stream_loop_doubles_frames_with_monotonic_timestamps() {
    let fixture = fixture_30_frames("loop_fixture.mp4");

    // ffmpeg -stream_loop 1 -i fixture: the input plays twice; failing
    // end_pts bookkeeping shows up as duplicate/backwards timestamps or a
    // short output.
    let out = tmp_path("loop_out.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).set_stream_loop(1))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "stream_loop",
    );
    assert!(result.is_ok(), "stream_loop task failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            nb_frames,
            duration,
            time_base,
            ..
        } => {
            assert_eq!(
                nb_frames, 60,
                "one loop doubles the 30-frame input (CLI parity)"
            );
            let secs = duration as f64 * time_base.num as f64 / time_base.den as f64;
            assert!(
                (secs - 2.0).abs() < 0.2,
                "two passes of a 1s fixture last 2s, got {secs}s"
            );
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}

#[test]
fn one_finished_stream_does_not_stop_the_demuxer() {
    // A 20s two-stream source where the video consumer finishes after a
    // single frame while the audio consumer still needs ~2.3s of content —
    // too much for the choked channels to have buffered ahead. fftools marks
    // only the exhausted stream finished and keeps demuxing until every used
    // stream is done (ffmpeg_demux.c:751 loop-top skip, :511-530 do_send
    // bookkeeping); bubbling the first per-stream AVERROR_EOF out of the
    // demux loop truncates the audio stream instead.
    let out = tmp_path("finished_stream.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from(
                    "testsrc=size=320x240:rate=30:duration=20[out0];\
                     sine=frequency=440:sample_rate=44100:duration=20[out1]",
                )
                .set_format("lavfi"),
            )
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_max_video_frames(1)
                    .set_max_audio_frames(100),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "one finished stream",
    );
    assert!(result.is_ok(), "two-stream transcode failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert_eq!(nb_frames, 1, "video stream must respect its frame limit");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no audio stream")
    {
        StreamInfo::Audio { nb_frames, .. } => {
            // 100 encoded frames + the AAC priming packet (CLI parity).
            assert_eq!(
                nb_frames, 101,
                "audio must keep flowing after the video stream finished"
            );
        }
        other => panic!("expected audio stream info, got {other:?}"),
    }
}

#[test]
fn stream_loop_survives_a_stream_that_finished_in_an_earlier_pass() {
    // Two-stream 1s fixture, looped twice more. The video consumer is done
    // after a single frame, so every later loop flush meets a dead video
    // destination: it must skip it like the fftools flush loop
    // (ffmpeg_sched.c:1979-1980) instead of failing the whole job, while
    // audio keeps collecting frames across passes.
    let fixture = tmp_path("loop_two_stream_fixture.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from(
                    "testsrc=size=320x240:rate=30:duration=1[out0];\
                     sine=frequency=440:sample_rate=44100:duration=1[out1]",
                )
                .set_format("lavfi"),
            )
            .output(
                Output::from(fixture.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "two-stream loop fixture",
    );
    assert!(result.is_ok(), "fixture task failed: {result:?}");

    let out = tmp_path("loop_survivor.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()).set_stream_loop(2))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_audio_codec("aac")
                    .set_max_video_frames(1)
                    .set_max_audio_frames(100),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "loop with finished stream",
    );
    assert!(
        result.is_ok(),
        "a finished stream must not fail the loop flush: {result:?}"
    );

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert_eq!(nb_frames, 1, "video stream must respect its frame limit");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }

    match find_audio_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no audio stream")
    {
        StreamInfo::Audio { nb_frames, .. } => {
            // 100 encoded frames + the AAC priming packet (CLI parity).
            assert_eq!(
                nb_frames, 101,
                "audio must keep flowing across loop passes"
            );
        }
        other => panic!("expected audio stream info, got {other:?}"),
    }
}
