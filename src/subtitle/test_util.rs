//! Shared helpers for the subtitle test suites.
//!
//! Deterministic-rendering recipe used everywhere: `FontProvider::None` plus
//! an explicit font file, so no system font lookup can influence results.

/// Probes well-known font-file locations; tests skip (with a note) when none
/// exists rather than vendoring a font into the repository.
pub(crate) fn test_font() -> Option<&'static str> {
    [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    .into_iter()
    .find(|p| std::path::Path::new(p).exists())
}

/// A dialogue line visible from t=0s to t=5s.
pub(crate) const HELLO_EVENT: &str =
    "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,Hello subtitle\n";

/// A `\p1` vector-drawing event (rasterizes without any glyph shaping, so its
/// geometry is stable across font/freetype versions).
pub(crate) const DRAWING_EVENT: &str = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\an7\\pos(100,100)\\p1}m 0 0 l 100 0 100 50 0 50{\\p0}\n";

/// Session-unique temp path (tests clean up after themselves best-effort).
pub(crate) fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ez_ffmpeg_subtitle_{}_{name}", std::process::id()))
}

/// Runs the real scheduler: test.mp4 -> rawvideo at `pix_fmt`, optionally
/// with a subtitle frame pipeline on the output and a global filter_desc.
pub(crate) fn transcode_test_mp4(
    out: &std::path::Path,
    filter: Option<crate::subtitle::SubtitleFilter>,
    pix_fmt: &str,
    filter_desc: Option<&str>,
) {
    use crate::core::filter::frame_pipeline_builder::FramePipelineBuilder;
    use crate::{FfmpegContext, Output};

    let mut output = Output::from(out.to_str().expect("utf8 temp path"))
        .set_format("rawvideo")
        .set_video_codec("rawvideo")
        .set_pix_fmt(pix_fmt)
        .disable_audio();
    if let Some(filter) = filter {
        let pipeline: FramePipelineBuilder =
            ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO.into();
        output = output.add_frame_pipeline(pipeline.filter("subtitles", Box::new(filter)));
    }
    let mut builder = FfmpegContext::builder().input("test.mp4");
    if let Some(desc) = filter_desc {
        builder = builder.filter_desc(desc);
    }
    builder
        .output(output)
        .build()
        .expect("build context")
        .start()
        .expect("start scheduler")
        .wait()
        .expect("transcode");
}

/// Byte-wise (max, mean) absolute difference of two equal-length buffers.
pub(crate) fn diff_stats(a: &[u8], b: &[u8]) -> (u8, f64) {
    assert_eq!(a.len(), b.len(), "buffers must have equal length");
    let mut max = 0u8;
    let mut sum = 0u64;
    for (x, y) in a.iter().zip(b) {
        let d = x.abs_diff(*y);
        max = max.max(d);
        sum += u64::from(d);
    }
    (max, sum as f64 / a.len() as f64)
}

/// (max, mean) absolute difference of two u16le sample buffers — byte-wise
/// comparison would misreport carries (511 vs 512 differs by 255 in bytes).
pub(crate) fn diff_stats_u16le(a: &[u8], b: &[u8]) -> (u16, f64) {
    assert_eq!(a.len(), b.len(), "buffers must have equal length");
    assert_eq!(a.len() % 2, 0, "u16le buffers must have even length");
    let mut max = 0u16;
    let mut sum = 0u64;
    for (ca, cb) in a.chunks_exact(2).zip(b.chunks_exact(2)) {
        let x = u16::from_le_bytes([ca[0], ca[1]]);
        let y = u16::from_le_bytes([cb[0], cb[1]]);
        let d = x.abs_diff(y);
        max = max.max(d);
        sum += u64::from(d);
    }
    (max, sum as f64 / (a.len() / 2) as f64)
}

/// Minimal valid ASS script (640x360 playfield, white default style) with the
/// given event lines appended under `[Events]`.
pub(crate) fn minimal_ass(events: &str) -> String {
    format!(
        "[Script Info]\n\
         ScriptType: v4.00+\n\
         PlayResX: 640\n\
         PlayResY: 360\n\
         \n\
         [V4+ Styles]\n\
         Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n\
         Style: Default,DejaVu Sans,48,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,0,2,10,10,10,1\n\
         \n\
         [Events]\n\
         Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n\
         {events}"
    )
}
