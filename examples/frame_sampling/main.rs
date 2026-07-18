use ez_ffmpeg::frame_export::{FrameExtractor, Sampling, VideoFrame};
use ez_ffmpeg::Input;
use std::fs::File;
use std::io::{BufWriter, Write};

/// Frame sampling strategies: `EveryNth`, `EverySec`, and `max_frames`.
///
/// Part 1 exports every 10th decoded frame (`EveryNth`). Part 2 samples one
/// frame per second (`EverySec`), resized to 160 px wide and capped at 8
/// frames, then pastes them side by side into a single thumbnail-strip PPM —
/// the packed, tightly-strided buffers concatenate row by row with no
/// per-pixel work.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or fall back to a synthetic
    // test pattern so the example runs without any input file.
    let arg = std::env::args().nth(1);
    if arg.is_none() {
        println!("usage: frame_sampling [input-file]  (using lavfi testsrc2 fallback)");
    }
    let input = |fallback: &str| match &arg {
        Some(path) => Input::from(path.clone()),
        None => Input::from(fallback).set_format("lavfi"),
    };

    // --- Part 1: every 10th decoded frame ------------------------------------
    // Index-based sampling counts decoder output: decoded frames 0, 10, 20, ...
    // are selected (at 30 fps that is 3 frames per second). `frame.index()` is
    // the export ordinal — selected frames are re-numbered 0, 1, 2, ...
    let mut count = 0u64;
    for frame in FrameExtractor::new(input("testsrc2=duration=2:size=640x360:rate=30"))
        .sampling(Sampling::EveryNth(10))
        .frames()?
    {
        let frame = frame?;
        println!(
            "EveryNth(10) export #{:>2}: {}x{} pts={:?}us",
            frame.index(),
            frame.width(),
            frame.height(),
            frame.pts_us()
        );
        count += 1;
    }
    println!("EveryNth(10) exported {count} frame(s)\n");

    // --- Part 2: one frame per second, as a thumbnail strip -------------------
    // Time-based sampling floats its grid on the first delivered frame and
    // picks the first frame at or after each 1-second boundary. `width(160)`
    // resizes with the aspect ratio kept (height is derived), and
    // `max_frames(8)` stops the whole run after 8 exported frames.
    let thumbs: Vec<VideoFrame> =
        FrameExtractor::new(input("testsrc2=duration=10:size=640x360:rate=30"))
            .sampling(Sampling::EverySec(1.0))
            .width(160)
            .max_frames(8)
            .collect_frames()?;

    for t in &thumbs {
        println!(
            "EverySec(1.0) export #{:>2}: {}x{} pts={:?}us",
            t.index(),
            t.width(),
            t.height(),
            t.pts_us()
        );
    }
    if thumbs.is_empty() {
        println!("no frames sampled; nothing to write");
        return Ok(());
    }

    write_strip("thumbnail_strip.ppm", &thumbs)?;
    println!(
        "wrote thumbnail_strip.ppm ({} thumbnails, {}x{})",
        thumbs.len(),
        thumbs.iter().map(|t| t.width()).sum::<u32>(),
        thumbs[0].height()
    );
    Ok(())
}

/// Pastes same-height RGB24 frames side by side into one binary PPM (P6).
/// Because every frame is tightly packed (`row_bytes = width * 3`), the strip
/// is assembled by copying one row slice per frame per output row.
fn write_strip(path: &str, frames: &[VideoFrame]) -> Result<(), Box<dyn std::error::Error>> {
    let height = frames[0].height();
    if frames.iter().any(|f| f.height() != height) {
        return Err("all thumbnails must share one height".into());
    }
    let total_width: u32 = frames.iter().map(|f| f.width()).sum();

    let mut w = BufWriter::new(File::create(path)?);
    write!(w, "P6\n{total_width} {height}\n255\n")?;
    for row in 0..height as usize {
        for f in frames {
            let stride = f.row_bytes();
            w.write_all(&f.as_bytes()[row * stride..(row + 1) * stride])?;
        }
    }
    w.flush()?;
    Ok(())
}
