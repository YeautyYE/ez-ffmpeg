//! End-to-end frame-export tests over synthetic lavfi sources (no fixtures).

use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout, Sampling};
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

#[test]
fn drop_mid_stream_does_not_deadlock() {
    // 300 frames, default channel capacity 1: after one frame the sink is
    // almost certainly parked in a blocking send(). Dropping must release it
    // (S6: receiver first, then abort) rather than hang. Reaching the end of
    // this test IS the assertion (a wrong teardown order would deadlock).
    let mut it = FrameExtractor::new(lavfi("testsrc2=s=320x240:r=30:d=10"))
        .frames()
        .expect("start failed");
    let first = it.next().expect("at least one frame").expect("frame ok");
    assert_eq!(first.index(), 0);
    drop(it);
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
