use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout, VideoFrame};
use ez_ffmpeg::Input;
use std::fs::File;
use std::io::{BufWriter, Write};

/// Decode a video into owned, tightly packed RGB24 frames and save the first
/// few as PPM images.
///
/// `VideoFrame::as_bytes()` is `width * height * 3` bytes with no row padding,
/// top-down — exactly the payload a binary PPM (P6) expects after its text
/// header, so the frames can be written to disk without any image crate.
/// The same buffer feeds an ndarray view or a tensor directly.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or fall back to a synthetic
    // test pattern so the example runs without any input file.
    let input = match std::env::args().nth(1) {
        Some(path) => Input::from(path),
        None => {
            println!("usage: extract_rgb_frames [input-file]  (using lavfi testsrc2 fallback)");
            Input::from("testsrc2=duration=2:size=640x360:rate=30").set_format("lavfi")
        }
    };

    let mut saved = 0u32;
    for frame in FrameExtractor::new(input)
        .pixel(PixelLayout::Rgb24) // the default, spelled out for clarity
        .max_frames(5) // stop decoding after 5 exported frames
        .frames()?
    {
        let frame = frame?;
        let name = format!("frame_{:03}.ppm", frame.index());
        write_ppm(&name, &frame)?;
        println!(
            "frame {:>3}: {}x{} {:?} pts={:?}us -> {}",
            frame.index(),
            frame.width(),
            frame.height(),
            frame.layout(),
            frame.pts_us(),
            name
        );
        saved += 1;
    }
    println!("saved {saved} PPM file(s)");
    Ok(())
}

/// Writes one RGB24 frame as a binary PPM (P6): a text header followed by the
/// packed pixel bytes as-is.
fn write_ppm(path: &str, frame: &VideoFrame) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    write!(w, "P6\n{} {}\n255\n", frame.width(), frame.height())?;
    w.write_all(frame.as_bytes())?;
    w.flush()
}
