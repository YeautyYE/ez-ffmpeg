use ez_ffmpeg::recipes::{sprite_sheet, thumbnail, At, Every, SpriteSheetOptions, ThumbnailOptions};

/// One-shot thumbnail and sprite-sheet extraction via the `recipes` helpers,
/// which own the graph-level `scale`/`select`/`tile` plus the output config.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A single 320px-wide thumbnail at 12 seconds.
    thumbnail(
        "test.mp4",
        "thumb.jpg",
        ThumbnailOptions {
            at: At::Sec(12.0),
            width: Some(320),
            ..ThumbnailOptions::default()
        },
    )?;

    // A 5x5 storyboard sprite sheet, one 160x90 cell every 2 seconds.
    sprite_sheet(
        "test.mp4",
        "sheet.jpg",
        SpriteSheetOptions {
            grid: (5, 5),
            every: Every::Sec(2.0),
            cell: (160, 90),
            quality: 3,
        },
    )?;

    println!("wrote thumb.jpg and sheet.jpg");
    Ok(())
}
