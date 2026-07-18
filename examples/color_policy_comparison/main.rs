use ez_ffmpeg::frame_export::{ColorPolicy, FrameExtractor, VideoFrame, YuvMatrix, YuvRange};
use ez_ffmpeg::Input;

/// A deterministic BT.709-tagged YUV source: `format=yuv420p` guarantees a
/// YUV -> RGB conversion happens, and `setparams` writes the BT.709 tags that
/// properly mastered HD content carries.
const SOURCE: &str = "testsrc2=duration=1:size=640x360:rate=30,\
                      format=yuv420p,\
                      setparams=colorspace=bt709:color_primaries=bt709:color_trc=bt709:range=tv";

/// Why `ColorPolicy::Tagged` is the default: BT.601 vs BT.709 side by side.
///
/// The same first frame of a BT.709-tagged test pattern is converted to RGB
/// three times:
///
/// 1. `Tagged` (the default) — honors the frame's own colorspace tags.
/// 2. `Force(BT.709/limited)` — pins the matrix the tags declare.
/// 3. `Force(BT.601/limited)` — pins the matrix many decode-to-RGB pipelines
///    silently assume for everything.
///
/// Run 1 and 2 must produce identical bytes (proof the tag was honored), while
/// run 3 diverges on saturated pixels — the wrong-matrix error that shows up
/// as subtly shifted colors in ML training data and thumbnails.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tagged = extract_first_frame(ColorPolicy::Tagged)?;
    let forced_709 = extract_first_frame(ColorPolicy::Force {
        matrix: YuvMatrix::Bt709,
        range: YuvRange::Limited,
    })?;
    let forced_601 = extract_first_frame(ColorPolicy::Force {
        matrix: YuvMatrix::Bt601,
        range: YuvRange::Limited,
    })?;

    println!(
        "source: BT.709-tagged {}x{} yuv420p test pattern\n",
        tagged.width(),
        tagged.height()
    );
    report("Tagged vs Force(BT.709/limited)", &tagged, &forced_709);
    report("Tagged vs Force(BT.601/limited)", &tagged, &forced_601);

    println!(
        "\nTagged == Force(BT.709) proves the tag was honored; the BT.601 run\n\
         shows the shift a naive pipeline bakes into every frame of HD content."
    );
    Ok(())
}

/// Runs one extraction over the test source and returns its first frame.
fn extract_first_frame(policy: ColorPolicy) -> Result<VideoFrame, Box<dyn std::error::Error>> {
    let input = Input::from(SOURCE).set_format("lavfi");
    FrameExtractor::new(input)
        .color(policy)
        .max_frames(1)
        .collect_frames()?
        .into_iter()
        .next()
        .ok_or_else(|| "source produced no frames".into())
}

/// Prints max/mean per-byte difference between two same-shape RGB24 frames,
/// plus the share of pixels that differ at all.
fn report(label: &str, a: &VideoFrame, b: &VideoFrame) {
    assert_eq!(a.as_bytes().len(), b.as_bytes().len());
    let (mut max_diff, mut sum_diff) = (0u8, 0u64);
    let mut changed_pixels = 0u64;
    for (pa, pb) in a
        .as_bytes()
        .chunks_exact(3)
        .zip(b.as_bytes().chunks_exact(3))
    {
        let mut pixel_changed = false;
        for (&ca, &cb) in pa.iter().zip(pb) {
            let d = ca.abs_diff(cb);
            max_diff = max_diff.max(d);
            sum_diff += u64::from(d);
            pixel_changed |= d != 0;
        }
        changed_pixels += u64::from(pixel_changed);
    }
    let bytes = a.as_bytes().len() as f64;
    let pixels = bytes / 3.0;
    println!(
        "{label}: max |diff| = {max_diff}, mean |diff| = {:.3}, changed pixels = {:.1}%",
        sum_diff as f64 / bytes,
        100.0 * changed_pixels as f64 / pixels
    );
}
