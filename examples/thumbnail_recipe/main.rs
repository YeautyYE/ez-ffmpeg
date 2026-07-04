use ez_ffmpeg::recipes::{thumbnail, At, ThumbnailOptions};

/// One-shot thumbnail extraction via `recipes::thumbnail`, which owns the
/// graph-level `scale`/`select` plus the single-frame output configuration.
///
/// See also `recipes::sprite_sheet` for a tiled storyboard image.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    thumbnail(
        "test.mp4",
        "thumb.jpg",
        ThumbnailOptions {
            at: At::Sec(12.0),
            width: Some(320),
            ..ThumbnailOptions::default()
        },
    )?;
    println!("wrote thumb.jpg");
    Ok(())
}
