use ez_ffmpeg::frame_export::{FrameExtractor, Sampling, VideoFrame};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::fs::File;
use std::io::{BufWriter, Write};

/// Exactly N frames, evenly spread by presentation time: `Sampling::UniformN`.
///
/// UniformN is the fixed-budget sampler VLM/CLIP-style pipelines want: ask for
/// 12 frames and get exactly 12, spread uniformly over the (trimmed) duration.
/// Selection happens pre-filtergraph, so only the chosen frames are scaled.
/// Part 1 pulls 12 thumbnails from a 10-second clip into a 4x3 contact sheet.
/// Part 2 trims the same clip to 0.2 s and still asks for 8: when the window
/// holds fewer distinct frames than requested, nearby frames repeat (each
/// repeat keeps its source frame's `pts_us`), so the count a model pipeline
/// was promised never changes.
///
/// UniformN needs a resolvable duration; when none can be probed (common for
/// live or piped inputs), supply one with `duration_hint_us()`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or generate a 10-second clip so
    // the example runs without any input file.
    let path = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("usage: uniform_thumbnails [input-file]  (generating uniform_source.mp4)");
            generate_source("uniform_source.mp4")?;
            "uniform_source.mp4".to_string()
        }
    };

    // --- Part 1: exactly 12 frames -> 4x3 contact sheet ----------------------
    let thumbs: Vec<VideoFrame> = FrameExtractor::new(path.as_str())
        .sampling(Sampling::UniformN(12))
        .width(160)
        .collect_frames()?;

    for t in &thumbs {
        println!("uniform export #{:>2}: pts={:?}us", t.index(), t.pts_us());
    }
    write_grid("uniform_grid.ppm", &thumbs, 4)?;
    println!(
        "wrote uniform_grid.ppm ({} thumbnails in a 4-column grid)\n",
        thumbs.len()
    );

    // --- Part 2: exact count survives short inputs ---------------------------
    // Trim the input to 0.2 s and still request 8 frames: the grid covers the
    // trimmed window, and if it holds fewer distinct frames than requested,
    // nearby frames repeat to keep the count exact. The generated 30 fps
    // source has ~6 frames in the window, so repeated `pts_us` values appear;
    // a high-frame-rate input may fill all 8 slots with distinct frames.
    let short: Vec<VideoFrame> = FrameExtractor::new(path.as_str())
        .duration_us(200_000)
        .sampling(Sampling::UniformN(8))
        .width(160)
        .collect_frames()?;
    let pts: Vec<Option<i64>> = short.iter().map(|f| f.pts_us()).collect();
    println!("UniformN(8) over a 0.2 s trim -> {} frame(s)", short.len());
    println!("pts list (repeats appear when the window runs short): {pts:?}");
    Ok(())
}

/// Encodes a 10-second test clip with the built-in `mpeg4` encoder, so the
/// fallback works on FFmpeg builds without an external H.264 encoder.
fn generate_source(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=10:size=640x360:rate=30").set_format("lavfi"))
        .output(Output::from(path).set_video_codec("mpeg4"))
        .build()?
        .start()?
        .wait()?;
    Ok(())
}

/// Pastes same-size RGB24 frames into a `cols`-wide grid PPM (P6), filling
/// any unused trailing cells with black.
fn write_grid(
    path: &str,
    frames: &[VideoFrame],
    cols: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = frames.first().ok_or("no frames to lay out")?;
    let (cell_w, cell_h) = (first.width() as usize, first.height() as usize);
    if frames
        .iter()
        .any(|f| f.width() as usize != cell_w || f.height() as usize != cell_h)
    {
        return Err("all thumbnails must share one size".into());
    }
    let rows = frames.len().div_ceil(cols);
    let black_row = vec![0u8; cell_w * 3];

    let mut w = BufWriter::new(File::create(path)?);
    write!(w, "P6\n{} {}\n255\n", cell_w * cols, cell_h * rows)?;
    for grid_row in 0..rows {
        for pixel_row in 0..cell_h {
            for col in 0..cols {
                match frames.get(grid_row * cols + col) {
                    Some(f) => {
                        let stride = f.row_bytes();
                        w.write_all(&f.as_bytes()[pixel_row * stride..(pixel_row + 1) * stride])?;
                    }
                    None => w.write_all(&black_row)?,
                }
            }
        }
    }
    w.flush()?;
    Ok(())
}
