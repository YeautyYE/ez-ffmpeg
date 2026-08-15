//! Positive media paths that must succeed on the LGPL minimum FFmpeg profile.
//!
//! `#[ignore]` for the same reason as `lgpl_capabilities.rs`: these are the
//! dedicated CI contract, not a skip-guarded local suite. Do not add
//! `if !is_encoder_available(...) { return; }` — a missing native encoder
//! is a profile bug.

use ez_ffmpeg::analysis::{Analysis, AudioDetector, VideoDetector};
use ez_ffmpeg::recipes::{animated_gif, GifOptions};
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output, VideoWriter};
use ffmpeg_sys_next::AVCodecID;

mod common;
use common::{recording_sink, sink_packets, tmp_path_in, wait_with_watchdog, SinkEv};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_lgpl_media", name)
}

fn run(ctx: FfmpegContext, secs: u64, scenario: &str) {
    let scheduler = ctx
        .start()
        .unwrap_or_else(|e| panic!("{scenario}: start: {e}"));
    wait_with_watchdog(scheduler, secs, scenario)
        .unwrap_or_else(|e| panic!("{scenario}: wait: {e}"));
}

#[test]
#[ignore]
fn mpeg4_roundtrip() {
    let out = tmp_path("mpeg4.mp4");
    let ctx = FfmpegContext::builder()
        .input(Input::from("testsrc=size=64x48:rate=15:duration=0.4").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_video_qscale(6),
        )
        .build()
        .expect("mpeg4 build");
    run(ctx, 30, "mpeg4_roundtrip");

    let info = find_video_stream_info(&out)
        .expect("probe")
        .expect("video stream");
    match info {
        StreamInfo::Video { codec_id, .. } => {
            assert_eq!(codec_id, AVCodecID::AV_CODEC_ID_MPEG4);
        }
        other => panic!("expected video stream, got {other:?}"),
    }
}

#[test]
#[ignore]
fn aac_roundtrip() {
    let out = tmp_path("aac.m4a");
    let ctx = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=0.4").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_audio_codec("aac")
                .set_audio_bitrate("64k"),
        )
        .build()
        .expect("aac build");
    run(ctx, 30, "aac_roundtrip");
    assert!(
        std::fs::metadata(&out)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "aac output {out} must be non-empty"
    );
}

#[test]
#[ignore]
fn mjpeg_thumbnail() {
    let out = tmp_path("thumb.jpg");
    let ctx = FfmpegContext::builder()
        .input(Input::from("testsrc=size=64x48:rate=1:duration=1").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mjpeg")
                .set_max_video_frames(1),
        )
        .build()
        .expect("mjpeg build");
    run(ctx, 20, "mjpeg_thumbnail");
    assert!(
        std::fs::metadata(&out)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "thumbnail {out} must be non-empty"
    );
}

#[test]
#[ignore]
fn blackdetect_on_lavfi() {
    let report = Analysis::new(Input::from("color=c=black:s=64x48:r=15:d=0.5").set_format("lavfi"))
        .video_detector(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.1,
            picture_th: 0.98,
        })
        .run()
        .expect("blackdetect must run on the LGPL profile");
    assert!(
        !report.black.is_empty(),
        "solid black lavfi input must produce a black range, got {report:?}"
    );
}

#[test]
#[ignore]
fn silencedetect_on_lavfi() {
    let report = Analysis::new(Input::from("anullsrc=r=44100:cl=mono:d=0.5").set_format("lavfi"))
        .audio_detector(AudioDetector::Silence {
            noise_db: -30.0,
            min_duration_s: 0.1,
            mono: false,
        })
        .run()
        .expect("silencedetect must run on the LGPL profile");
    assert!(
        !report.silence.is_empty(),
        "anullsrc must produce a silence range, got {report:?}"
    );
}

/// Bare MP4 with no video codec selected must encode MPEG-4 Part 2 on a
/// build without libx264 (`movenc` default).
#[test]
#[ignore]
fn bare_mp4_videowriter_uses_mpeg4() {
    let out = tmp_path("bare.mp4");
    let out2 = out.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let mut w = VideoWriter::builder(64, 48)
                .fps(15, 1)
                .open(ez_ffmpeg::Output::from(out2.as_str()))?;
            let frame = vec![0u8; w.frame_size()];
            for _ in 0..8 {
                w.write_owned(frame.clone())?;
            }
            w.finish()
        })();
        let _ = tx.send(result);
    });
    match rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("bare mp4 VideoWriter failed: {e}"),
        Err(_) => panic!("bare mp4 VideoWriter hung"),
    }

    let info = find_video_stream_info(&out)
        .expect("probe")
        .expect("video stream");
    match info {
        StreamInfo::Video {
            codec_id,
            codec_name,
            ..
        } => {
            assert_eq!(
                codec_id,
                AVCodecID::AV_CODEC_ID_MPEG4,
                "bare MP4 default codec must be mpeg4 on LGPL FFmpeg, got {codec_name}"
            );
        }
        other => panic!("expected video stream, got {other:?}"),
    }
}

#[test]
#[ignore]
fn animated_gif_recipe() {
    let out = tmp_path("lgpl.gif");
    animated_gif(
        Input::from("testsrc=size=64x48:rate=12:duration=0.5").set_format("lavfi"),
        Output::from(out.as_str()),
        GifOptions::default(),
    )
    .expect("animated_gif must run on the LGPL profile");
    assert!(
        std::fs::metadata(&out)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "gif output {out} must be non-empty"
    );
}

#[test]
#[ignore]
fn scene_and_ebur128_on_lavfi() {
    let video =
        Analysis::new(Input::from("testsrc=size=64x48:rate=15:duration=0.5").set_format("lavfi"))
            .video_detector(VideoDetector::Scene {
                threshold_pct: 10.0,
            })
            .run()
            .expect("scdet must run on the LGPL profile");
    let _ = video.scenes;

    let audio = Analysis::new(Input::from("sine=frequency=440:duration=0.5").set_format("lavfi"))
        .audio_detector(AudioDetector::Ebur128 { true_peak: false })
        .run()
        .expect("ebur128 must run on the LGPL profile");
    assert!(
        audio.loudness.is_some(),
        "ebur128 must produce a loudness summary, got {audio:?}"
    );
}

#[test]
#[ignore]
fn crop_letterbox_does_not_need_cropdetect() {
    let report = Analysis::new(
        Input::from("color=c=white:s=64x32:r=15:d=0.8,pad=64:48:0:8:black").set_format("lavfi"),
    )
    .video_detector(VideoDetector::Crop {
        limit: 24,
        round: 2,
        reset: 0,
    })
    .run()
    .expect("VideoDetector::Crop must run without cropdetect");
    let crop = report
        .crop
        .expect("letterbox input must produce a crop suggestion");
    assert!(
        crop.y >= 4 && crop.y <= 12,
        "letterbox y={} (want ~8)",
        crop.y
    );
    assert!(
        crop.h >= 24 && crop.h <= 40,
        "letterbox h={} (want ~32)",
        crop.h
    );
}

#[test]
#[ignore]
fn packet_sink_aac_audio_only() {
    let (sink, log) = recording_sink();
    let ctx = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=0.4").set_format("lavfi"))
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .build()
        .expect("aac packet-sink build");
    run(ctx, 30, "packet_sink_aac");

    let events = log.lock().unwrap().clone();
    assert!(
        events.iter().any(|e| matches!(e, SinkEv::Info { .. })),
        "aac packet sink must emit stream info"
    );
    assert!(
        matches!(events.last(), Some(SinkEv::End { .. })),
        "aac packet sink must emit on_end"
    );
    assert!(
        !sink_packets(&log).is_empty(),
        "aac packet sink must deliver at least one packet"
    );
}
