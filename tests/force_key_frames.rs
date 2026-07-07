//! Integration coverage for `Output::set_force_key_frames` (FFmpeg
//! `-force_key_frames "0,5,10.5"` list form).
//!
//! The pure parser is unit-tested in `src/core/context/output.rs`. Here we prove
//! the end-to-end behavior against a real encoder:
//!   * a software encoder that honors `pict_type = I` (mpeg4) actually emits a
//!     keyframe at the requested time, and
//!   * the option is a genuine no-op for an audio-only job (the forced list must
//!     never reach an audio encoder).

use ez_ffmpeg::packet_scanner::PacketScanner;
use ez_ffmpeg::stream_info::StreamInfo;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_force_key_frames_tests_{}",
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

/// Encode a ~3s / 30fps clip with mpeg4 and a huge GOP, so the encoder's only
/// natural keyframe is frame 0. `force` optionally sets `-force_key_frames`.
fn encode_video(force: Option<&str>, name: &str) -> String {
    let out = tmp_path(name);
    let mut output = Output::from(out.as_str())
        .set_video_codec("mpeg4")
        // g=600 >> 90 frames: the encoder inserts no periodic keyframe of its own.
        .set_video_codec_opt("g", "600")
        .set_max_video_frames(90);
    if let Some(spec) = force {
        output = output
            .set_force_key_frames(spec)
            .expect("force_key_frames spec should parse");
    }
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        name,
    );
    assert!(result.is_ok(), "video encode `{name}` failed: {result:?}");
    out
}

/// Returns (keyframe presentation times in seconds, whether the first video
/// packet in the file is a keyframe).
fn keyframe_times(path: &str) -> (Vec<f64>, bool) {
    let mut scanner = PacketScanner::open(path).expect("open output for probing");
    let time_base = match scanner.video_stream().expect("output has a video stream") {
        StreamInfo::Video { time_base, .. } => *time_base,
        other => panic!("expected a video stream, got {other:?}"),
    };

    let mut times = Vec::new();
    let mut first_is_keyframe = false;
    let mut seen_video = false;
    for packet in scanner.packets() {
        let info = packet.expect("read packet");
        if !info.is_video() {
            continue;
        }
        let secs = info.pts().unwrap_or(0) as f64 * time_base.num as f64 / time_base.den as f64;
        if !seen_video {
            first_is_keyframe = info.is_keyframe();
            seen_video = true;
        }
        if info.is_keyframe() {
            times.push(secs);
        }
    }
    (times, first_is_keyframe)
}

#[test]
fn forces_a_keyframe_at_the_requested_time() {
    // Baseline: without the option the encoder places a keyframe only at frame 0.
    let control = encode_video(None, "control.mp4");
    // With force_key_frames("1"): an extra keyframe at the first frame whose PTS
    // reaches 1.0s (frame 30 at 30fps).
    let forced = encode_video(Some("1"), "forced.mp4");

    let (control_kfs, control_first_kf) = keyframe_times(&control);
    let (forced_kfs, forced_first_kf) = keyframe_times(&forced);

    // Frame 0 is always a keyframe in both.
    assert!(control_first_kf, "frame 0 must be a keyframe (control)");
    assert!(forced_first_kf, "frame 0 must be a keyframe (forced)");

    // The natural GOP must NOT drop a keyframe near 1s on its own; otherwise the
    // test could not attribute the forced keyframe to the option.
    assert!(
        control_kfs.iter().all(|&t| (t - 1.0).abs() > 0.05),
        "control run must have no natural keyframe near 1s, got {control_kfs:?}"
    );

    // Forcing must produce a keyframe near 1s that is not frame 0.
    assert!(
        forced_kfs
            .iter()
            .any(|&t| t > 0.0 && (t - 1.0).abs() <= 0.05),
        "force_key_frames(\"1\") must yield a keyframe near 1s, got {forced_kfs:?}"
    );

    // And forcing adds keyframes rather than replacing them.
    assert!(
        forced_kfs.len() > control_kfs.len(),
        "forcing must add a keyframe: forced {forced_kfs:?} vs control {control_kfs:?}"
    );
}

fn encode_audio(force: Option<&str>, name: &str) -> String {
    let out = tmp_path(name);
    // pcm_s16le is always available and still runs through the audio encoder path,
    // so this genuinely exercises "the forced list must not reach an audio encoder".
    let mut output = Output::from(out.as_str()).set_audio_codec("pcm_s16le");
    if let Some(spec) = force {
        output = output
            .set_force_key_frames(spec)
            .expect("force_key_frames spec should parse");
    }
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(
                Input::from("sine=frequency=440:duration=2:sample_rate=44100")
                    .set_format("lavfi"),
            )
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        name,
    );
    assert!(result.is_ok(), "audio encode `{name}` failed: {result:?}");
    out
}

fn audio_packet_count(path: &str) -> usize {
    let mut scanner = PacketScanner::open(path).expect("open audio output for probing");
    scanner
        .packets()
        .filter(|p| p.as_ref().map(|i| i.is_audio()).unwrap_or(false))
        .count()
}

#[test]
fn set_force_key_frames_is_a_no_op_for_audio_only() {
    let plain = encode_audio(None, "audio_plain.wav");
    let with_opt = encode_audio(Some("1"), "audio_forced.wav");

    let plain_packets = audio_packet_count(&plain);
    let with_packets = audio_packet_count(&with_opt);

    assert!(plain_packets > 0, "baseline audio output should have packets");
    assert_eq!(
        with_packets, plain_packets,
        "set_force_key_frames must not change audio output \
         ({with_packets} vs {plain_packets} packets)"
    );

    // There is no video stream for the option to have affected.
    let scanner = PacketScanner::open(&with_opt).expect("open audio output");
    assert!(
        scanner.video_stream().is_none(),
        "audio-only output must have no video stream"
    );
    assert!(
        scanner.audio_stream().is_some(),
        "audio-only output must have an audio stream"
    );
}
