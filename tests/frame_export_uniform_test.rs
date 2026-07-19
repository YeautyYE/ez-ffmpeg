//! End-to-end tests for `Sampling::UniformN` exact-N sampling over lavfi sources.

use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout, Sampling};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};

fn lavfi(spec: &str) -> Input {
    Input::from(spec).set_format("lavfi")
}

#[test]
fn uniform_n_terminates_unbounded_source_with_hint() {
    // No `:d=` — the lavfi source is INFINITE. The duration hint must double as
    // the demux stop boundary, or this run would decode forever after the grid
    // is covered. Completing at all (with exactly n frames) is the assertion.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10"))
        .sampling(Sampling::UniformN(4))
        .duration_hint_us(400_000)
        .collect_frames()
        .expect("extraction must terminate at the hint boundary");
    assert_eq!(frames.len(), 4);
}

#[test]
fn uniform_n_max_frames_below_n_terminates_unbounded_source() {
    // Infinite source + max_frames < n: once the sink cap fires it must keep
    // forwarding (not dropping) so the encoder-side recording-time cut still
    // sees frames and ends the run. Completing at all is the assertion.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10"))
        .sampling(Sampling::UniformN(4))
        .max_frames(2)
        .duration_hint_us(400_000)
        .collect_frames()
        .expect("must terminate with the cap below n");
    assert_eq!(frames.len(), 2);
}

#[test]
fn uniform_n_multi_video_stream_requires_explicit_index() {
    // Build a file with TWO video streams (split into two mapped outputs).
    let dir = std::env::temp_dir().join(format!("ez_fe_uniform_mv_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("two_video.mkv");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=16x16:r=10:d=0.5"))
            .filter_desc("[0:v]split=2[a][b]")
            .output(
                Output::from(path.to_str().unwrap())
                    .set_video_codec("mpeg2video")
                    .add_stream_map("[a]")
                    .add_stream_map("[b]"),
            )
            .build()
            .expect("build two-video fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode two-video fixture");

    // Default stream selection is ambiguous for UniformN (sampler binds by media
    // type, graph by best stream) => typed error.
    let err = FrameExtractor::new(path.to_str().unwrap())
        .sampling(Sampling::UniformN(4))
        .frames()
        .err()
        .expect("ambiguous multi-video UniformN must be rejected");
    assert!(
        matches!(
            err,
            ez_ffmpeg::error::Error::FrameExport(
                ez_ffmpeg::frame_export::FrameExportError::InvalidOption(_)
            )
        ),
        "got {err:?}"
    );

    // An explicit index resolves the ambiguity.
    let frames = FrameExtractor::new(path.to_str().unwrap())
        .sampling(Sampling::UniformN(4))
        .video_stream_index(0)
        .collect_frames()
        .expect("explicit index works");
    assert_eq!(frames.len(), 4);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn uniform_n_start_only_spans_remaining_content() {
    // 1 s @ 10 fps, intra-only mjpeg (every frame a keyframe, so the seek lands
    // at ~500 ms, not at a distant earlier keyframe). Grid must cover only the
    // REMAINING ~500 ms: 4 targets over 5 real frames => 4 distinct timestamps.
    // (With the whole-file span, half the targets would aim beyond EOF and the
    // tail would collapse into duplicates of the last frame.)
    let dir = std::env::temp_dir().join(format!("ez_fe_uniform_start_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("intra.mkv");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=16x16:r=10:d=1"))
            .output(Output::from(path.to_str().unwrap()).set_video_codec("mjpeg"))
            .build()
            .expect("build fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode fixture");

    let frames = FrameExtractor::new(path.to_str().unwrap())
        .sampling(Sampling::UniformN(4))
        .start_time_us(500_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 4);
    let mut pts: Vec<i64> = frames.iter().filter_map(|f| f.pts_us()).collect();
    pts.dedup();
    assert_eq!(
        pts.len(),
        4,
        "targets must spread over the remaining span, not bunch past EOF: {pts:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn uniform_n_start_on_gop_video_stays_uniform() {
    // Inter-coded fixture: 3 s @ 10 fps mpeg2video with a single keyframe at
    // t=0 (g=30). A 1.5 s start therefore seeks back to the t=0 keyframe and
    // the decoder replays 15 lead-in frames with NEGATIVE re-zeroed pts, which
    // the in-graph trim drops. The sampler must anchor its grid at the first
    // SURVIVING frame — anchoring on the lead-in shifted the whole grid a GOP
    // early, the trim destroyed the stamped targets, and the sink backfilled
    // with consecutive tail frames ([0,100,200,300] ms instead of a uniform
    // spread). The intra-only mjpeg test above cannot catch this: its seek
    // lands exactly on the request.
    let dir = std::env::temp_dir().join(format!("ez_fe_uniform_gop_{}", std::process::id()));
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
        .sampling(Sampling::UniformN(4))
        .start_time_us(1_500_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 4);
    let pts: Vec<i64> = frames.iter().filter_map(|f| f.pts_us()).collect();
    assert_eq!(pts.len(), 4, "every frame carries a timestamp: {pts:?}");
    assert!(
        pts.windows(2).all(|w| w[0] < w[1]),
        "strictly increasing: {pts:?}"
    );
    assert!(pts[0] >= 0, "no pre-start frame may survive: {pts:?}");
    // Ideal targets over the remaining 1.5 s sit ~375 ms apart. The pre-fix
    // failure mode is a consecutive clump (100 ms gaps) at the window head.
    assert!(
        pts.windows(2).all(|w| w[1] - w[0] >= 300_000),
        "targets must stay spread, not clump: {pts:?}"
    );
    assert!(
        pts[3] - pts[0] >= 900_000,
        "grid must cover the remaining span: {pts:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn nonpositive_duration_hint_is_rejected() {
    for bad in [0i64, -1] {
        let err = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
            .sampling(Sampling::UniformN(4))
            .duration_hint_us(bad)
            .frames()
            .err()
            .expect("nonpositive hint must be rejected");
        assert!(
            matches!(
                err,
                ez_ffmpeg::error::Error::FrameExport(
                    ez_ffmpeg::frame_export::FrameExportError::InvalidOption(_)
                )
            ),
            "got {err:?}"
        );
    }
}

#[test]
fn uniform_n_yields_exactly_n() {
    // 2 s @ 10 fps = 20 real frames; ask for 16.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=48x32:r=10:d=2"))
        .sampling(Sampling::UniformN(16))
        .duration_hint_us(2_000_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 16, "UniformN(16) must yield exactly 16");
    for (i, f) in frames.iter().enumerate() {
        assert_eq!(f.index() as usize, i, "dense 0-based indices");
        assert_eq!((f.width(), f.height()), (48, 32));
        assert_eq!(f.as_bytes().len(), 48 * 32 * 3);
    }
    // PTS are non-decreasing (duplicates keep the source frame's pts).
    let pts: Vec<i64> = frames
        .iter()
        .map(|f| f.pts_us().unwrap_or(i64::MIN))
        .collect();
    assert!(
        pts.windows(2).all(|w| w[0] <= w[1]),
        "pts non-decreasing: {pts:?}"
    );
}

#[test]
fn uniform_n_pads_short_input() {
    // 0.5 s @ 10 fps = 5 real frames; ask for 20 => padding by repetition.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=0.5"))
        .sampling(Sampling::UniformN(20))
        .duration_hint_us(500_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 20, "short input pads up to exactly N");
    assert!(frames
        .iter()
        .enumerate()
        .all(|(i, f)| f.index() as usize == i));
    // At least one pts repeats (a real frame was duplicated to pad).
    let pts: Vec<Option<i64>> = frames.iter().map(|f| f.pts_us()).collect();
    assert!(
        pts.windows(2).any(|w| w[0] == w[1]),
        "expected at least one duplicated frame in {pts:?}"
    );
}

#[test]
fn uniform_n_beyond_flush_cap() {
    // The whole point of the dup-COUNT (vs re-emitting tail duplicates, which the
    // EOS drain caps at 1024/filter): 2000 frames out of a 10-frame clip.
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .sampling(Sampling::UniformN(2000))
        .duration_hint_us(1_000_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 2000, "dup-count must honor N far beyond 1024");
    assert_eq!(frames.last().unwrap().index(), 1999);
}

#[test]
fn uniform_n_resizes() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=128x96:r=10:d=1"))
        .sampling(Sampling::UniformN(8))
        .width(64)
        .height(48)
        .pixel(PixelLayout::Gray8)
        .duration_hint_us(1_000_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 8);
    for f in &frames {
        assert_eq!((f.width(), f.height()), (64, 48));
        assert_eq!(f.as_bytes().len(), 64 * 48);
    }
}

#[test]
fn uniform_n_respects_max_frames() {
    let frames = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .sampling(Sampling::UniformN(16))
        .max_frames(5)
        .duration_hint_us(1_000_000)
        .collect_frames()
        .expect("extraction");
    assert_eq!(frames.len(), 5, "the sink cap bounds UniformN too");
}

#[test]
fn uniform_n_without_resolvable_duration_is_typed_error() {
    // lavfi sources report no probeable duration; UniformN needs one.
    let err = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .sampling(Sampling::UniformN(5))
        .frames()
        .err()
        .expect("UniformN without a duration must fail");
    assert!(
        matches!(
            err,
            ez_ffmpeg::error::Error::FrameExport(
                ez_ffmpeg::frame_export::FrameExportError::UnknownDuration
            )
        ),
        "got {err:?}"
    );
}

#[test]
fn uniform_n_uses_file_duration_without_hint() {
    // A real container reports a duration the resolver falls back to.
    let dir = std::env::temp_dir().join(format!("ez_fe_uniform_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("clip.mkv");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(lavfi("testsrc2=s=16x16:r=10:d=1"))
            .output(Output::from(path.to_str().unwrap()).set_video_codec("mpeg2video"))
            .build()
            .expect("build fixture"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("encode fixture");

    let frames = FrameExtractor::new(path.to_str().unwrap())
        .sampling(Sampling::UniformN(5))
        .collect_frames()
        .expect("extraction using file duration");
    assert_eq!(frames.len(), 5);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn uniform_n_zero_is_rejected() {
    let err = FrameExtractor::new(lavfi("testsrc2=s=16x16:r=10:d=1"))
        .sampling(Sampling::UniformN(0))
        .frames()
        .err()
        .expect("UniformN(0) must be rejected");
    assert!(
        matches!(
            err,
            ez_ffmpeg::error::Error::FrameExport(
                ez_ffmpeg::frame_export::FrameExportError::InvalidOption(_)
            )
        ),
        "got {err:?}"
    );
}
