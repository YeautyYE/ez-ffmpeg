use ez_ffmpeg::recipes::{animated_gif, GifOptions};

/// Export a high-quality animated GIF. The recipe builds a single filtergraph
/// that generates one optimised palette for the clip (`palettegen`) and applies
/// it (`paletteuse`), avoiding the banding of a naive GIF conversion.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    animated_gif(
        "test.mp4",
        "output.gif",
        GifOptions {
            fps: 12,
            width: Some(480),
            ..GifOptions::default()
        },
    )?;
    println!("wrote output.gif");
    Ok(())
}
