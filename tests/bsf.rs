//! Bitstream-filter (`-bsf`) integration coverage for PR1.
//!
//! Complements the raw-level identity/parse unit tests in
//! `src/raw/bit_stream_filter.rs` with end-to-end scheduler runs:
//!
//! - stream-copy H.264 MP4 -> MPEG-TS with `h264_mp4toannexb` produces valid
//!   Annex B output (the BSF rewrites codecpar before the header, so the muxer
//!   does not double-apply and the copy pipeline succeeds).
//! - the encode path runs a stream through a `null` BSF and still probes fine
//!   (the muxer never auto-inserts `null`, so this isolates the BSF code path).
//! - a nonexistent BSF name fails the job with a clear error, not a panic or a
//!   silent success.
//!
//! Tests that need an H.264 encoder to build a fixture skip gracefully when
//! none is available, mirroring the environment-tolerant style in the rest of
//! `tests/`.

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::process::Command;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_bsf_tests_{}", std::process::id()));
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

/// A software H.264 encoder name linked into this FFmpeg, if any. Hardware
/// encoders are skipped: they fail without the matching device.
fn software_h264_encoder() -> Option<String> {
    let preferred = ["libx264", "libopenh264"];
    let encoders = ez_ffmpeg::codec::get_encoders();
    preferred
        .into_iter()
        .find(|name| encoders.iter().any(|e| e.codec_name == *name))
        .map(|s| s.to_string())
}

/// True if the system `ffmpeg` CLI is on PATH.
fn ffmpeg_cli_available() -> bool {
    Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build a ~1s H.264 MP4 fixture. Prefers the system `ffmpeg` CLI (produces
/// clean AVCC extradata); falls back to ez with a software H.264 encoder.
/// Returns `false` when neither path can produce an H.264 stream.
fn make_h264_mp4(path: &str) -> bool {
    if ffmpeg_cli_available() {
        let ok = Command::new("ffmpeg")
            .args([
                "-y",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x240:rate=30:duration=1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                path,
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok && fixture_is_h264(path) {
            return true;
        }
    }

    if let Some(enc) = software_h264_encoder() {
        let built = FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(path)
                    .set_video_codec(enc)
                    .set_max_video_frames(30),
            )
            .build();
        if let Ok(ctx) = built {
            if let Ok(running) = ctx.start() {
                if wait_with_watchdog(running, 60, "h264 fixture").is_ok() && fixture_is_h264(path) {
                    return true;
                }
            }
        }
    }

    false
}

/// True if `path` holds a readable video stream whose codec is H.264.
fn fixture_is_h264(path: &str) -> bool {
    matches!(
        find_video_stream_info(path),
        Ok(Some(StreamInfo::Video { ref codec_name, .. })) if codec_name == "h264"
    )
}

/// True if `bytes` contains an H.264 Annex B start code (3- or 4-byte).
fn contains_annex_b_start_code(bytes: &[u8]) -> bool {
    bytes.windows(3).any(|w| w == [0x00, 0x00, 0x01])
}

#[test]
fn stream_copy_h264_mp4_to_mpegts_with_mp4toannexb() {
    let fixture = tmp_path("bsf_copy_src.mp4");
    if !make_h264_mp4(&fixture) {
        eprintln!(
            "SKIP stream_copy_h264_mp4_to_mpegts_with_mp4toannexb: \
             no H.264 encoder / ffmpeg CLI available to build the fixture"
        );
        return;
    }

    let out_ts = tmp_path("bsf_copy_out.ts");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from(fixture.as_str()))
            .output(
                Output::from(out_ts.as_str())
                    .set_format("mpegts")
                    .add_stream_map_with_copy("0:v")
                    .set_video_bsf("h264_mp4toannexb"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "h264_mp4toannexb copy",
    );
    assert!(
        result.is_ok(),
        "stream-copy with h264_mp4toannexb failed: {result:?}"
    );

    // The remuxed elementary stream must still be valid H.264.
    assert!(
        fixture_is_h264(&out_ts),
        "MPEG-TS output is not a readable H.264 stream"
    );

    // ... and carry Annex B start codes (the point of h264_mp4toannexb).
    let bytes = std::fs::read(&out_ts).expect("failed to read MPEG-TS output");
    assert!(
        contains_annex_b_start_code(&bytes),
        "MPEG-TS output has no Annex B start code"
    );
}

#[test]
fn encode_path_null_bsf_smoke() {
    // mpeg4 is always available; `null` is a no-op BSF the muxer never
    // auto-inserts, so this exercises the encode -> mux BSF -> write path.
    let out = tmp_path("bsf_encode_null.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_bsf("null")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "encode null bsf",
    );
    assert!(result.is_ok(), "encode with null BSF failed: {result:?}");

    match find_video_stream_info(&out)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => {
            assert!(
                nb_frames > 0,
                "null-BSF re-encode produced no frames (nb_frames={nb_frames})"
            );
        }
        other => panic!("expected a video stream, got {other:?}"),
    }
}

#[test]
fn nonexistent_bsf_fails_the_job() {
    let out = tmp_path("bsf_bad.mp4");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_bsf("definitely_not_a_bsf")
                    .set_max_video_frames(30),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "bad bsf",
    );

    let err = match result {
        Ok(()) => panic!("a nonexistent bitstream filter must fail the job, not succeed"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("bitstream filter"),
        "error should identify the bitstream filter failure, got: {msg}"
    );
}
