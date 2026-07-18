use ez_ffmpeg::frame_export::{FrameExtractor, Sampling, VideoFrame};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::fs::File;
use std::io::{BufWriter, Write};

/// Fast thumbnails by decoding keyframes only.
///
/// `Sampling::KeyframesOnly` pins the decoder option `skip_frame=nokey`, so the
/// decoder skips every non-key frame instead of decoding the full stream and
/// discarding most of it. On long-GOP content that means one decoded frame per
/// GOP — the same fast path as the encoder-based recipe in
/// `examples/thumbnail_extraction` (Example 3), but here each keyframe arrives
/// as an in-memory RGB frame with its timestamp instead of a JPEG file.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or generate a 12-second clip
    // with the GOP capped at one second so the sparsity is visible.
    let path = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            println!(
                "usage: keyframe_thumbnails [input-file]  (generating keyframe_source.mp4 fallback)"
            );
            generate_source("keyframe_source.mp4")?;
            "keyframe_source.mp4".to_string()
        }
    };

    let mut exported = 0u64;
    for frame in FrameExtractor::new(path.as_str())
        .sampling(Sampling::KeyframesOnly)
        .width(160) // thumbnail size; height keeps the aspect ratio
        .frames()?
    {
        let frame = frame?;
        let name = format!("keyframe_{:03}.ppm", frame.index());
        write_ppm(&name, &frame)?;
        println!(
            "keyframe {:>2}: pts={:>9?}us -> {}",
            frame.index(),
            frame.pts_us(),
            name
        );
        exported += 1;
    }
    println!("exported {exported} keyframe thumbnail(s)");
    Ok(())
}

/// Encodes a 12-second test clip at 10 fps with the GOP capped at 10 frames,
/// so a keyframe lands at least once per second (encoders may insert extra
/// ones at scene cuts). The built-in `mpeg4` encoder keeps the fallback
/// working on FFmpeg builds without an external H.264 encoder.
fn generate_source(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=12:size=640x360:rate=10").set_format("lavfi"))
        .output(
            Output::from(path)
                .set_video_codec("mpeg4")
                .set_video_codec_opt("g", "10"),
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}

/// Writes one RGB24 frame as a binary PPM (P6).
fn write_ppm(path: &str, frame: &VideoFrame) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    write!(w, "P6\n{} {}\n255\n", frame.width(), frame.height())?;
    w.write_all(frame.as_bytes())?;
    w.flush()
}
