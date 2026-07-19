//! End-to-end frame-export tests over synthetic lavfi sources (plus small
//! generated container fixtures where a real time base or stream layout is
//! load-bearing).

use ez_ffmpeg::frame_export::{ColorPolicy, FrameExtractor, PixelLayout, Sampling};
use ez_ffmpeg::Input;

/// A finite synthetic video source. `testsrc2` is a real decode path, so these
/// exercise the whole scheduler pipeline, not just option plumbing.
fn lavfi(spec: &str) -> Input {
    Input::from(spec).set_format("lavfi")
}

#[test]
fn extracts_all_frames_rgb24_tightly_packed() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=64x48:r=10:d=1"))
        .pixel(PixelLayout::Rgb24)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(frames.len(), 10, "1s @ 10fps => 10 frames");
    for (i, f) in frames.iter().enumerate() {
        assert_eq!(f.width(), 64);
        assert_eq!(f.height(), 48);
        assert_eq!(f.layout(), PixelLayout::Rgb24);
        assert_eq!(f.as_bytes().len(), 64 * 48 * 3, "tight RGB24 packing");
        assert_eq!(f.row_bytes(), 64 * 3);
        assert_eq!(f.index() as usize, i, "indices are dense and 0-based");
    }
    // PTS are passed through and monotonic.
    let pts: Vec<i64> = frames.iter().filter_map(|f| f.pts_us()).collect();
    assert_eq!(pts.len(), frames.len(), "every frame has a pts");
    assert!(
        pts.windows(2).all(|w| w[0] < w[1]),
        "pts strictly increasing"
    );
}

#[test]
fn max_frames_never_overshoots() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=32x32:r=30:d=5"))
        .max_frames(7)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(frames.len(), 7, "the sink owns the exact cap");
}

#[test]
fn every_nth_selects_expected_count() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=32x32:r=10:d=1"))
        .sampling(Sampling::EveryNth(3))
        .collect_frames()
        .expect("extraction failed");
    // 10 frames, every 3rd starting at 0 => indices 0,3,6,9.
    assert_eq!(frames.len(), 4);
    assert!(frames
        .iter()
        .enumerate()
        .all(|(i, f)| f.index() as usize == i));
}

#[test]
fn every_sec_thins_to_grid() {
    // 4 seconds @ 10fps, one frame per second => ~4 frames.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=32x32:r=10:d=4"))
        .sampling(Sampling::EverySec(1.0))
        .collect_frames()
        .expect("extraction failed");
    assert!(
        (3..=5).contains(&frames.len()),
        "expected ~4 one-per-second frames, got {}",
        frames.len()
    );
}

#[test]
fn resize_and_gray8() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=128x96:r=10:d=1"))
        .width(64)
        .height(48)
        .pixel(PixelLayout::Gray8)
        .max_frames(3)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(frames.len(), 3);
    for f in &frames {
        assert_eq!((f.width(), f.height()), (64, 48));
        assert_eq!(f.layout(), PixelLayout::Gray8);
        assert_eq!(f.as_bytes().len(), 64 * 48, "Gray8 is 1 byte/pixel");
    }
}

#[test]
fn rgba32_has_alpha_width() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .pixel(PixelLayout::Rgba32)
        .max_frames(1)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].as_bytes().len(), 16 * 16 * 4);
}

#[test]
fn no_video_stream_is_typed_error() {
    // An audio-only lavfi source has no video stream.
    let err = FrameExtractor::new(lavfi("sine=frequency=440:duration=1"))
        .frames()
        .err()
        .expect("should fail with no video stream");
    match err {
        ez_ffmpeg::error::Error::FrameExport(
            ez_ffmpeg::frame_export::FrameExportError::NoVideoStream,
        ) => {}
        other => panic!("expected NoVideoStream, got {other:?}"),
    }
}

/// Waits up to `secs` for the scenario thread's next signal, surfacing a
/// scenario panic as this test's own failure and a missing signal as the
/// named `hang_msg` panic. Returns the handle so later stages can keep it.
fn expect_signal(
    rx: &std::sync::mpsc::Receiver<()>,
    scenario: std::thread::JoinHandle<()>,
    secs: u64,
    hang_msg: &str,
) -> std::thread::JoinHandle<()> {
    use std::sync::mpsc::RecvTimeoutError;
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(()) => scenario,
        Err(RecvTimeoutError::Timeout) => panic!("{hang_msg}"),
        Err(RecvTimeoutError::Disconnected) => match scenario.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(()) => unreachable!("thread cannot exit cleanly without signalling"),
        },
    }
}

#[test]
fn drop_mid_stream_does_not_deadlock() {
    // 300 frames, channel capacity 1 (pinned explicitly — the scenario depends
    // on it): after one frame the sink parks in a blocking send(). Dropping
    // must release it (S6: receiver first, then abort) rather than hang. The
    // scenario runs on its own thread and signals right before dropping, so
    // the 30s deadline measures the TEARDOWN alone (startup and the first
    // decode sit under their own generous deadline) — a teardown regression
    // fails a named assertion instead of hanging the whole test binary until
    // CI kills it.
    let (tx, rx) = std::sync::mpsc::channel();
    let scenario = std::thread::spawn(move || {
        let mut it = FrameExtractor::new(lavfi("testsrc2=s=320x240:r=30:d=10"))
            .channel_capacity(1)
            .frames()
            .expect("start failed");
        let first = it.next().expect("at least one frame").expect("frame ok");
        assert_eq!(first.index(), 0);
        // Consuming one frame freed the only channel slot. Give the producer
        // time to refill it and park in the next blocking send, biasing the
        // drop below into the release-while-parked case (the regression this
        // test exists for) instead of racing a producer that has not blocked
        // yet. Teardown must succeed either way, so this cannot flake.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let _ = tx.send(());
        drop(it);
        let _ = tx.send(());
    });
    let scenario = expect_signal(
        &rx,
        scenario,
        60,
        "frame scenario did not reach its drop point within 60s",
    );
    let scenario = expect_signal(
        &rx,
        scenario,
        30,
        "dropping FrameIter mid-stream did not tear down within 30s (deadlock)",
    );
    scenario.join().expect("scenario thread must exit cleanly");
}

#[test]
fn iterator_is_fused_after_completion() {
    let mut it = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .max_frames(2)
        .frames()
        .expect("start failed");
    let mut count = 0;
    while let Some(item) = it.next() {
        item.expect("frame ok");
        count += 1;
    }
    assert_eq!(count, 2);
    // Fused: further calls keep returning None, no panic.
    assert!(it.next().is_none());
    assert!(it.next().is_none());
}

/// Encodes a small real container so the stream time base differs from 1/fps
/// (mkv uses 1/1000). Lavfi-direct sources have stream tb == 1/fps exactly,
/// which cannot distinguish a pts scaled with the wrong time base.
fn encode_mkv_fixture(path: &str) {
    use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Output};
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=64x48:r=25:d=2"))
            .output(Output::from(path).set_video_codec("mpeg2video"))
            .build()
            .expect("build fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode fixture");
}

#[test]
fn pts_us_is_exact_on_real_container_time_base() {
    let dir = std::env::temp_dir().join(format!("ez_fe_pts_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mkv = dir.join("t.mkv");
    let mkv = mkv.to_str().unwrap();
    encode_mkv_fixture(mkv);

    // 25 fps: frame 25 sits at exactly 1 s. A pts read against the wrong time
    // base (decoder-era best_effort_timestamp vs the post-filter 1/framerate
    // time base) is off by tb_out/stream_tb — 40x for mkv's 1/1000.
    let frames = FrameExtractor::new(mkv)
        .max_frames(30)
        .collect_frames()
        .expect("extract");
    let p25 = frames.get(25).and_then(|f| f.pts_us());
    assert!(
        p25.map(|v| (v - 1_000_000).abs() < 100_000)
            .unwrap_or(false),
        "frame 25 of a 25 fps stream must sit near 1s, got {p25:?}"
    );

    // The same pts path drives EverySec: 2 s at one-per-second must select ~2
    // frames, not every frame (which the 40x scale error would cause).
    let sec = FrameExtractor::new(mkv)
        .sampling(Sampling::EverySec(1.0))
        .collect_frames()
        .expect("everysec");
    assert!(
        (1..=3).contains(&sec.len()),
        "EverySec(1.0) over 2s must select ~2 frames, got {}",
        sec.len()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resolution_guess_multi_video_best_not_first_requires_index() {
    use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Output};
    // A file whose FIRST video stream is NOT the exported (best) one: stream 0
    // carries 2 probed frames (fps=2), stream 1 carries 10 — best-stream
    // selection ranks by probed frame count, so it picks stream 1. The
    // input-side stamp binds to the first video stream, so
    // TaggedOrResolutionGuess without an explicit index must be REJECTED with
    // the typed, actionable error (removing that rejection would silently
    // leave the exported stream unstamped).
    let dir = std::env::temp_dir().join(format!("ez_fe_guess_mv_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("two_video.mkv");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=64x48:r=10:d=1"))
            .filter_desc("[0:v]split=2[t][a];[t]fps=2[b]")
            .output(
                Output::from(path.to_str().unwrap())
                    .set_video_codec("mpeg2video")
                    .add_stream_map("[b]")
                    .add_stream_map("[a]"),
            )
            .build()
            .expect("build sparse+dense two-video fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode sparse+dense two-video fixture");
    let path = path.to_str().unwrap();

    let err = FrameExtractor::new(path)
        .color(ColorPolicy::TaggedOrResolutionGuess)
        .frames()
        .err()
        .expect("guess policy without an index must be rejected on this layout");
    match err {
        ez_ffmpeg::error::Error::FrameExport(
            ez_ffmpeg::frame_export::FrameExportError::InvalidOption(msg),
        ) => {
            assert!(
                msg.contains("video_stream_index"),
                "error must name the fix: {msg}"
            );
        }
        other => panic!("expected typed InvalidOption, got {other:?}"),
    }

    // An explicit index resolves it: the stamp pipeline binds to that stream.
    let frames = FrameExtractor::new(path)
        .color(ColorPolicy::TaggedOrResolutionGuess)
        .video_stream_index(1)
        .collect_frames()
        .expect("explicit index works with the guess policy");
    assert!(!frames.is_empty());

    // Other policies keep working without an index on the same layout (the
    // mismatch is warn-only there; the wrong-bound guard sits on a stream that
    // is never decoded and must not stall or fail the run).
    let frames = FrameExtractor::new(path)
        .collect_frames()
        .expect("default Tagged policy must still work on this layout");
    assert!(!frames.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Encodes `desc` into `path` with the given video codec (plus per-codec
/// options), so tests can build small real-container fixtures.
fn encode_fixture(desc: &str, path: &str, codec: &str, codec_opts: &[(&str, &str)]) {
    use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Output};
    let mut output = Output::from(path).set_video_codec(codec);
    for (k, v) in codec_opts {
        output = output.set_video_codec_opt(*k, *v);
    }
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi(desc))
            .output(output)
            .build()
            .expect("build fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode fixture");
}

#[test]
fn keyframes_only_selects_exactly_the_keyframes() {
    use ez_ffmpeg::packet_scanner::PacketScanner;
    use ez_ffmpeg::stream_info::StreamInfo;

    // 3 s @ 10 fps with GOP 10 => 30 frames whose only keyframes are frames
    // 0/10/20 (0 s, 1 s, 2 s). Scene-change insertion is disabled and B-frames
    // pinned off so the cadence is the GOP grid alone on every encoder build.
    // A real container (mkv, tb 1/1000) keeps the pts path honest.
    let dir = std::env::temp_dir().join(format!("ez_fe_kf_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("gop10.mkv");
    let path = path.to_str().unwrap();
    encode_fixture(
        "testsrc2=s=64x48:r=10:d=3",
        path,
        "mpeg4",
        &[("g", "10"), ("sc_threshold", "1000000000"), ("bf", "0")],
    );

    // Ground truth from the container itself: where the keyframe packets sit.
    let mut scanner = PacketScanner::open(path).expect("open fixture");
    let time_base = match scanner.video_stream().expect("fixture has video") {
        StreamInfo::Video { time_base, .. } => *time_base,
        other => panic!("expected a video stream, got {other:?}"),
    };
    let mut key_pts_us = Vec::new();
    let mut total_packets = 0usize;
    for packet in scanner.packets() {
        let info = packet.expect("read packet");
        if !info.is_video() {
            continue;
        }
        total_packets += 1;
        if info.is_keyframe() {
            let pts = info.pts().expect("video packet pts");
            key_pts_us.push(pts * 1_000_000 * time_base.num as i64 / time_base.den as i64);
        }
    }
    // The fixture must be meaningfully sparse: 3 keyframes out of 30 frames,
    // sitting on the GOP grid. If this ever fails, the ENCODER changed, not
    // the extractor.
    assert_eq!(total_packets, 30, "3s @ 10fps fixture must hold 30 frames");
    assert_eq!(key_pts_us.len(), 3, "GOP 10 => keyframes at frames 0/10/20");
    for (got, want) in key_pts_us.iter().zip([0i64, 1_000_000, 2_000_000]) {
        assert!(
            (got - want).abs() <= 20_000,
            "fixture keyframe at {got}us, expected ~{want}us"
        );
    }

    // KeyframesOnly must yield exactly those frames: same count, same
    // positions — nothing from inside the GOPs.
    let frames = FrameExtractor::new(path)
        .sampling(Sampling::KeyframesOnly)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(
        frames.len(),
        key_pts_us.len(),
        "exactly one exported frame per container keyframe"
    );
    for (i, (f, want)) in frames.iter().zip(&key_pts_us).enumerate() {
        assert_eq!(f.index() as usize, i, "indices are dense and 0-based");
        let got = f.pts_us().expect("exported frame pts");
        assert!(
            (got - want).abs() <= 20_000,
            "exported keyframe {i} at {got}us must match container keyframe at {want}us"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn keyframes_only_intra_only_source_keeps_every_frame() {
    // mjpeg is intra-only: every frame is a keyframe. KeyframesOnly must keep
    // all of them — the decoder-level nokey skip must not discard intra frames
    // and the KEY-flag selection must not over-filter.
    let dir = std::env::temp_dir().join(format!("ez_fe_kf_intra_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("intra.mkv");
    let path = path.to_str().unwrap();
    encode_fixture("testsrc2=s=32x32:r=10:d=1", path, "mjpeg", &[]);

    let frames = FrameExtractor::new(path)
        .sampling(Sampling::KeyframesOnly)
        .collect_frames()
        .expect("extraction failed");
    assert_eq!(
        frames.len(),
        10,
        "every frame of an intra-only 1s @ 10fps stream is a keyframe"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn out_of_bounds_video_stream_index_is_typed_error() {
    let err = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .video_stream_index(99)
        .frames()
        .err()
        .expect("index 99 on a single-stream input must fail");
    match err {
        ez_ffmpeg::error::Error::FrameExport(
            ez_ffmpeg::frame_export::FrameExportError::StreamIndexOutOfBounds { index, count },
        ) => {
            assert_eq!(index, 99);
            assert_eq!(count, 1, "the lavfi source exposes exactly one stream");
        }
        other => panic!("expected StreamIndexOutOfBounds, got {other:?}"),
    }
}

#[test]
fn video_index_pointing_at_audio_stream_is_typed_error() {
    // Audio-only input: stream 0 exists but is not a video stream.
    let err = FrameExtractor::new(lavfi("sine=frequency=440:duration=1"))
        .video_stream_index(0)
        .frames()
        .err()
        .expect("video extraction from an audio stream must fail");
    match err {
        ez_ffmpeg::error::Error::FrameExport(
            ez_ffmpeg::frame_export::FrameExportError::NotAVideoStream { index },
        ) => assert_eq!(index, 0),
        other => panic!("expected NotAVideoStream, got {other:?}"),
    }
}

#[test]
fn start_and_duration_window_bounds_output() {
    // Intra-only mjpeg so the seek lands on the requested time rather than a
    // distant earlier keyframe: 2 s @ 10 fps = 20 frames on a real container.
    let dir = std::env::temp_dir().join(format!("ez_fe_window_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("window.mkv");
    let path = path.to_str().unwrap();
    encode_fixture("testsrc2=s=64x48:r=10:d=2", path, "mjpeg", &[]);

    // Whole file first: the windowed run below is checked against these
    // frames for count AND content.
    let all = FrameExtractor::new(path)
        .collect_frames()
        .expect("full extraction");
    assert_eq!(all.len(), 20, "2s @ 10fps must yield 20 frames unwindowed");
    assert_ne!(
        all[0].as_bytes(),
        all[5].as_bytes(),
        "fixture frames at 0s and 0.5s must differ for the seek check below"
    );

    // [0.5 s, 1.5 s) of a 10 fps stream is exactly frames 5..15. Input-side
    // seeking shifts timestamps so the exported clock restarts at ~0 (FFmpeg
    // `-ss` semantics): the window must bound the count and the whole pts
    // span. Decoding the same coded frame twice is byte-deterministic, so the
    // first windowed frame must equal frame 5 of the full run — proving the
    // seek actually moved the start (a run that ignored start_time_us would
    // also yield 10 frames with pts 0..900ms, but starting at frame 0).
    let frames = FrameExtractor::new(path)
        .start_time_us(500_000)
        .duration_us(1_000_000)
        .collect_frames()
        .expect("windowed extraction");
    assert_eq!(
        frames.len(),
        10,
        "0.5s..1.5s of a 10fps intra-only stream is exactly 10 frames"
    );
    assert_eq!(
        frames[0].as_bytes(),
        all[5].as_bytes(),
        "first windowed frame must be the source frame at 0.5s"
    );
    let pts: Vec<i64> = frames.iter().filter_map(|f| f.pts_us()).collect();
    assert_eq!(pts.len(), frames.len(), "every windowed frame has a pts");
    assert!(
        pts.windows(2).all(|w| w[0] < w[1]),
        "windowed pts strictly increasing: {pts:?}"
    );
    assert!(
        pts.iter().all(|&p| (0..1_000_000).contains(&p)),
        "every windowed pts must fall inside the 1s duration window: {pts:?}"
    );
    let first = *pts.first().unwrap();
    let last = *pts.last().unwrap();
    assert!(
        (0..=20_000).contains(&first),
        "first frame must sit at the window start, got {first}us"
    );
    assert!(
        (880_000..=920_000).contains(&last),
        "last frame must sit at ~0.9s, got {last}us"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn every_sec_with_start_time_skips_gop_lead_in() {
    // Inter-coded fixture with a single keyframe at t=0 (g=30): a 1.5 s start
    // seeks back to that keyframe and the decoder replays 15 lead-in frames
    // with negative re-zeroed pts before the in-graph trim drops them. The
    // input-side selector must not anchor its EverySec grid on (or spend
    // selections on) that lead-in: every delivered frame sits at/after the
    // requested start, anchored at the first surviving frame.
    use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Output};
    let dir = std::env::temp_dir().join(format!("ez_fe_sec_gop_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("gop.mkv");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=16x16:r=10:d=3"))
            .output(
                Output::from(path.to_str().unwrap())
                    .set_video_codec("mpeg2video")
                    .set_video_codec_opt("g", "30"),
            )
            .build()
            .expect("build GOP fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode GOP fixture");

    let frames = FrameExtractor::new(path.to_str().unwrap())
        .sampling(Sampling::EverySec(0.5))
        .start_time_us(1_500_000)
        .collect_frames()
        .expect("extraction");
    // Remaining 1.5 s at one frame per 0.5 s => anchor + 2 grid points.
    assert_eq!(frames.len(), 3, "got {:?}", pts_list(&frames));
    let pts = pts_list(&frames);
    assert!(
        pts.iter().all(|&t| t >= 0),
        "no pre-start frame may be selected: {pts:?}"
    );
    assert!(
        pts.windows(2).all(|w| w[1] - w[0] >= 400_000),
        "selections must follow the 0.5 s grid: {pts:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn pts_list(frames: &[ez_ffmpeg::frame_export::VideoFrame]) -> Vec<i64> {
    frames.iter().filter_map(|f| f.pts_us()).collect()
}
