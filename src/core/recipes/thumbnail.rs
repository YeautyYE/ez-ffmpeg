//! Single-frame thumbnail and storyboard sprite-sheet recipes.
//!
//! These helpers wrap the ez-ffmpeg builder so callers do not have to hand-roll
//! the `select` / `fps` / `scale` / `tile` / `qscale` / `-frames:v` combination
//! themselves. Two workflows are covered:
//!
//! - [`thumbnail`]: extract one still frame (by second, frame index, or percent).
//! - [`sprite_sheet`]: tile several sampled frames into a single storyboard image.
//!
//! The raw [`filter_desc`](crate::FfmpegContext) escape hatch remains available
//! for anything these do not cover (MVP: single thumbnail + sprite sheet only —
//! there is no multi-frame *sequence* API here).

use crate::core::container_info::get_duration_us;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Public options
// ---------------------------------------------------------------------------

/// Where to grab the thumbnail frame from.
///
/// All variants accept ergonomic floating-point entry values but are converted
/// to validated integer microseconds / frame indices internally (NaN, infinity,
/// negative, and overflowing values are rejected — see [`Error::InvalidRecipeArg`]).
#[derive(Debug, Clone, Copy)]
pub enum At {
    /// Wall-clock seconds. Converted to integer microseconds.
    Sec(f64),
    /// Zero-based decoded-frame index.
    ///
    /// Always decodes from the start and selects by number in *decode order*
    /// ([`SeekMode`] is ignored for this variant). Under VFR / B-frames this is
    /// decode order, not a mathematically exact wall-clock position.
    Frame(u64),
    /// Percentage `0..=100` of the container duration.
    ///
    /// Requires a probeable file/URL input (callback inputs are unsupported —
    /// use [`At::Sec`] instead). `100.0` resolves to the very end of the stream
    /// and may yield the last frame or an empty frame depending on the source.
    Percent(f64),
}

/// How the target frame is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekMode {
    /// Seek at the input level via [`Input::set_start_time_us`].
    ///
    /// Cheap for large offsets. Accuracy depends on ez-ffmpeg / FFmpeg's
    /// accurate-seek behavior and the source; it does **not** promise keyframe
    /// alignment or exact-second precision.
    InputSeek,
    /// Decode from the start and pick the frame with a `select` filter.
    ///
    /// Accurate, but decodes everything up to the target — slow for large
    /// offsets in a long file.
    FilterScan,
}

/// Options for [`thumbnail`].
///
/// At least one of `width` / `height` must be set; supplying only one keeps the
/// aspect ratio (the other side is computed as an even number via `scale=-2`).
#[derive(Debug, Clone)]
pub struct ThumbnailOptions {
    /// Position of the frame to extract.
    pub at: At,
    /// Target width in pixels (or `None` to derive it from `height`).
    pub width: Option<u32>,
    /// Target height in pixels (or `None` to derive it from `width`).
    pub height: Option<u32>,
    /// Seek strategy. Defaults to [`SeekMode::InputSeek`].
    pub seek: SeekMode,
    /// `-q:v` fixed quality scale, validated to `2..=31` (lower is better).
    ///
    /// Only affects **lossy** image encoders such as MJPEG (`.jpg`) and WebP
    /// (`.webp`); it is a no-op for lossless encoders like PNG.
    pub quality: u8,
}

impl Default for ThumbnailOptions {
    fn default() -> Self {
        Self {
            at: At::Sec(0.0),
            width: Some(320),
            height: None,
            seek: SeekMode::InputSeek,
            quality: 2,
        }
    }
}

/// Frame sampling cadence for [`sprite_sheet`].
///
/// The sheet always contains exactly `cols * rows` cells; these variants only
/// control *which* frames fill them.
#[derive(Debug, Clone, Copy)]
pub enum Every {
    /// One frame every `n` seconds (`fps = 1000000 / interval_us`).
    Sec(f64),
    /// Every `n`-th decoded frame (`select='not(mod(n,n))'`). `0` is rejected.
    Frames(u64),
    /// Spread the `cols * rows` cells evenly across the whole input duration
    /// (`fps = cols*rows*1000000 / duration_us`), i.e. a full-video storyboard.
    ///
    /// Requires a probeable file/URL input (use [`Every::Sec`] for callback
    /// inputs). The frame count is implicitly the grid size, so there is no
    /// user-supplied count to conflict with the grid.
    EvenlySpread,
}

/// Options for [`sprite_sheet`].
#[derive(Debug, Clone)]
pub struct SpriteSheetOptions {
    /// Grid layout as `(cols, rows)`; both must be `> 0`.
    pub grid: (u32, u32),
    /// Frame sampling cadence.
    pub every: Every,
    /// Per-cell size as `(width, height)` in pixels; both must be `> 0`.
    pub cell: (u32, u32),
    /// `-q:v` fixed quality scale, validated to `2..=31` (see [`ThumbnailOptions::quality`]).
    pub quality: u8,
}

impl Default for SpriteSheetOptions {
    fn default() -> Self {
        Self {
            grid: (5, 5),
            every: Every::EvenlySpread,
            cell: (160, 90),
            quality: 2,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal, validated representations (pure, unit-testable)
// ---------------------------------------------------------------------------

/// A resolved, validated thumbnail position — never a bare `f64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Locator {
    /// Absolute presentation time in microseconds (`>= 0`).
    TimeUs(i64),
    /// Zero-based decoded-frame index.
    Frame(u64),
}

/// A resolved sprite-sheet sampling rule, ready to be rendered to a filter string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpriteSampling {
    /// `fps = 1000000 / interval_us`.
    IntervalUs(i64),
    /// `select='not(mod(n\,stride))'`.
    FrameStride(u64),
    /// `fps = numerator / duration_us`, where `numerator == cols*rows*1000000`.
    EvenSpread { numerator: i64, duration_us: i64 },
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// Validate the `-q:v` quality scale (`2..=31`).
pub(crate) fn validate_quality(quality: u8) -> Result<()> {
    if !(2..=31).contains(&quality) {
        return Err(Error::InvalidRecipeArg(format!(
            "quality must be in 2..=31, got {quality}"
        )));
    }
    Ok(())
}

/// Convert seconds to integer microseconds, rejecting NaN/inf/negative/overflow.
pub(crate) fn sec_to_us(sec: f64) -> Result<i64> {
    if !sec.is_finite() {
        return Err(Error::InvalidRecipeArg(format!(
            "time in seconds must be finite, got {sec}"
        )));
    }
    if sec < 0.0 {
        return Err(Error::InvalidRecipeArg(format!(
            "time in seconds must be non-negative, got {sec}"
        )));
    }
    let us = sec * 1_000_000.0;
    if us > i64::MAX as f64 {
        return Err(Error::InvalidRecipeArg(format!(
            "time {sec}s overflows i64 microseconds"
        )));
    }
    Ok(us as i64)
}

/// Convert a `0..=100` percentage of a known duration to integer microseconds.
pub(crate) fn percent_to_us(percent: f64, duration_us: i64) -> Result<i64> {
    if !percent.is_finite() {
        return Err(Error::InvalidRecipeArg(format!(
            "percent must be finite, got {percent}"
        )));
    }
    if !(0.0..=100.0).contains(&percent) {
        return Err(Error::InvalidRecipeArg(format!(
            "percent must be in 0..=100, got {percent}"
        )));
    }
    if duration_us < 0 {
        return Err(Error::InvalidRecipeArg(format!(
            "input duration is unknown ({duration_us}us); use At::Sec instead of At::Percent"
        )));
    }
    // percent in [0,100], duration_us >= 0 => result in [0, duration_us], fits i64.
    Ok(((percent / 100.0) * duration_us as f64) as i64)
}

/// Build the `scale=...` clause. Errors if both dimensions are `None` or any is `0`.
pub(crate) fn build_scale(width: Option<u32>, height: Option<u32>) -> Result<String> {
    if matches!(width, Some(0)) || matches!(height, Some(0)) {
        return Err(Error::InvalidRecipeArg(format!(
            "scale width/height must be > 0, got width={width:?} height={height:?}"
        )));
    }
    match (width, height) {
        (Some(w), None) => Ok(format!("scale={w}:-2")),
        (None, Some(h)) => Ok(format!("scale=-2:{h}")),
        (Some(w), Some(h)) => Ok(format!("scale={w}:{h}")),
        (None, None) => Err(Error::InvalidRecipeArg(
            "thumbnail requires width or height (both were None)".to_string(),
        )),
    }
}

/// Build the labeled thumbnail filtergraph plus the optional input-seek value.
///
/// Returns `(input_seek_us, filter_desc)` where the graph is always terminated
/// with the `[thumb]` label so the caller can `add_stream_map("[thumb]")`.
pub(crate) fn build_thumbnail_desc(
    locator: &Locator,
    seek: SeekMode,
    scale: &str,
) -> (Option<i64>, String) {
    match locator {
        Locator::TimeUs(us) => match seek {
            // Input-level seek: the first frame after the seek is scaled directly.
            SeekMode::InputSeek => (Some(*us), format!("[0:v]{scale}[thumb]")),
            // Decode from start; `t` is in seconds, so compare against us/1000000
            // (integer-exact rational, no float in the string). `\,` keeps the
            // comma inside gte() from splitting the filter chain.
            SeekMode::FilterScan => (
                None,
                format!("[0:v]select='gte(t\\,{us}/1000000)',{scale}[thumb]"),
            ),
        },
        // Frame index is a decode-order concept: always scan, ignore SeekMode.
        Locator::Frame(n) => (None, format!("[0:v]select='eq(n\\,{n})',{scale}[thumb]")),
    }
}

/// Render just the sampling clause of a sprite filtergraph.
pub(crate) fn sprite_sampling_desc(sampling: &SpriteSampling) -> String {
    match sampling {
        SpriteSampling::IntervalUs(interval_us) => format!("fps=1000000/{interval_us}"),
        SpriteSampling::FrameStride(stride) => format!("select='not(mod(n\\,{stride}))'"),
        SpriteSampling::EvenSpread {
            numerator,
            duration_us,
        } => format!("fps={numerator}/{duration_us}"),
    }
}

/// Build the labeled sprite-sheet filtergraph.
///
/// `tile` is emitted explicitly with `nb_frames` so it collapses exactly
/// `cols*rows` sampled frames into one image. A short source yields a partial
/// sheet (trailing cells stay the tile filter's default fill); an empty source
/// produces no sheet at all.
pub(crate) fn build_sprite_desc(
    sampling: &SpriteSampling,
    cell_w: u32,
    cell_h: u32,
    cols: u32,
    rows: u32,
    nb_frames: u64,
) -> String {
    let sample = sprite_sampling_desc(sampling);
    format!(
        "[0:v]{sample},scale={cell_w}:{cell_h},tile=layout={cols}x{rows}:nb_frames={nb_frames}[thumb]"
    )
}

// ---------------------------------------------------------------------------
// Public recipes
// ---------------------------------------------------------------------------

/// Extract a single thumbnail frame from `input` and write it to `output`.
///
/// The output image format is decided by the `output` URL extension or its
/// `set_format` (`.jpg` -> MJPEG, `.png` -> PNG, `.webp` -> WebP, ...).
///
/// # Errors
/// Returns [`Error::InvalidRecipeArg`] for invalid options (no dimensions,
/// out-of-range quality, non-finite/negative time, a percent position without a
/// probeable input). Build/run failures bubble up as their existing [`Error`]s.
///
/// # Example
/// ```rust,ignore
/// use ez_ffmpeg::recipes::{thumbnail, ThumbnailOptions, At, SeekMode};
///
/// thumbnail(
///     "input.mp4",
///     "thumb.jpg",
///     ThumbnailOptions { at: At::Sec(12.0), width: Some(320), seek: SeekMode::InputSeek, ..Default::default() },
/// )?;
/// ```
pub fn thumbnail(
    input: impl Into<Input>,
    output: impl Into<Output>,
    opts: ThumbnailOptions,
) -> crate::error::Result<()> {
    let input: Input = input.into();
    let output: Output = output.into();

    validate_quality(opts.quality)?;
    let scale = build_scale(opts.width, opts.height)?;

    let locator = match opts.at {
        At::Sec(s) => Locator::TimeUs(sec_to_us(s)?),
        At::Frame(n) => Locator::Frame(n),
        At::Percent(p) => {
            let url = input.url.clone().ok_or_else(|| {
                Error::InvalidRecipeArg(
                    "At::Percent requires a probeable file/URL input; callback inputs are \
                     unsupported — use At::Sec instead"
                        .to_string(),
                )
            })?;
            let duration_us = get_duration_us(url).map_err(|e| {
                Error::InvalidRecipeArg(format!(
                    "At::Percent needs the input duration but probing failed: {e}; use At::Sec instead"
                ))
            })?;
            Locator::TimeUs(percent_to_us(p, duration_us)?)
        }
    };

    let (input_seek_us, filter_desc) = build_thumbnail_desc(&locator, opts.seek, &scale);

    let input = match input_seek_us {
        Some(us) => input.set_start_time_us(us),
        None => input,
    };

    let output = output
        .set_max_video_frames(1_i64)
        .set_video_qscale(opts.quality as i32)
        .add_stream_map("[thumb]");

    FfmpegContext::builder()
        .input(input)
        .filter_desc(filter_desc)
        .output(output)
        .build()?
        .start()?
        .wait()
}

/// Generate one storyboard sprite sheet (a `cols x rows` tile of sampled frames).
///
/// The rendered image is `cols*cell_w` wide by `rows*cell_h` tall. The output
/// image format follows the `output` URL extension / `set_format`.
///
/// # Errors
/// Returns [`Error::InvalidRecipeArg`] for invalid options (zero grid/cell,
/// out-of-range quality, `Every::Frames(0)`, `Every::Sec(0.0)`, a grid that
/// overflows the frame count, or an [`Every::EvenlySpread`] without a probeable
/// input). Build/run failures bubble up as their existing [`Error`]s.
///
/// # Example
/// ```rust,ignore
/// use ez_ffmpeg::recipes::{sprite_sheet, SpriteSheetOptions, Every};
///
/// sprite_sheet(
///     "input.mp4",
///     "storyboard.jpg",
///     SpriteSheetOptions { grid: (5, 5), every: Every::EvenlySpread, cell: (160, 90), quality: 3 },
/// )?;
/// ```
pub fn sprite_sheet(
    input: impl Into<Input>,
    output: impl Into<Output>,
    opts: SpriteSheetOptions,
) -> crate::error::Result<()> {
    let input: Input = input.into();
    let output: Output = output.into();

    validate_quality(opts.quality)?;
    let (cols, rows) = opts.grid;
    let (cell_w, cell_h) = opts.cell;
    validate_grid_cell(cols, rows, cell_w, cell_h)?;

    let nb_frames = (cols as u64).checked_mul(rows as u64).ok_or_else(|| {
        Error::InvalidRecipeArg(format!("grid {cols}x{rows} overflows the frame count"))
    })?;

    let sampling = match opts.every {
        Every::Sec(s) => {
            let interval_us = sec_to_us(s)?;
            if interval_us <= 0 {
                return Err(Error::InvalidRecipeArg(format!(
                    "Every::Sec interval must be > 0 seconds, got {s}"
                )));
            }
            SpriteSampling::IntervalUs(interval_us)
        }
        Every::Frames(n) => {
            if n == 0 {
                return Err(Error::InvalidRecipeArg(
                    "Every::Frames(0) is invalid".to_string(),
                ));
            }
            SpriteSampling::FrameStride(n)
        }
        Every::EvenlySpread => {
            let numerator: i64 = nb_frames
                .checked_mul(1_000_000)
                .and_then(|n| i64::try_from(n).ok())
                .ok_or_else(|| {
                    Error::InvalidRecipeArg(format!(
                        "grid {cols}x{rows} overflows the fps numerator"
                    ))
                })?;
            let url = input.url.clone().ok_or_else(|| {
                Error::InvalidRecipeArg(
                    "Every::EvenlySpread requires a probeable file/URL input; callback inputs \
                     are unsupported — use Every::Sec instead"
                        .to_string(),
                )
            })?;
            let duration_us = get_duration_us(url).map_err(|e| {
                Error::InvalidRecipeArg(format!(
                    "Every::EvenlySpread needs the input duration but probing failed: {e}; \
                     use Every::Sec instead"
                ))
            })?;
            if duration_us <= 0 {
                return Err(Error::InvalidRecipeArg(format!(
                    "input duration is unknown ({duration_us}us); use Every::Sec instead of \
                     Every::EvenlySpread"
                )));
            }
            SpriteSampling::EvenSpread {
                numerator,
                duration_us,
            }
        }
    };

    let filter_desc = build_sprite_desc(&sampling, cell_w, cell_h, cols, rows, nb_frames);

    let output = output
        .set_max_video_frames(1_i64)
        .set_video_qscale(opts.quality as i32)
        .add_stream_map("[thumb]");

    FfmpegContext::builder()
        .input(input)
        .filter_desc(filter_desc)
        .output(output)
        .build()?
        .start()?
        .wait()
}

/// Validate that the grid and cell dimensions are all `> 0`.
pub(crate) fn validate_grid_cell(cols: u32, rows: u32, cell_w: u32, cell_h: u32) -> Result<()> {
    if cols == 0 || rows == 0 {
        return Err(Error::InvalidRecipeArg(format!(
            "sprite grid must be > 0 in both dimensions, got {cols}x{rows}"
        )));
    }
    if cell_w == 0 || cell_h == 0 {
        return Err(Error::InvalidRecipeArg(format!(
            "sprite cell size must be > 0 in both dimensions, got {cell_w}x{cell_h}"
        )));
    }
    // The rendered sheet is `cols*cell_w` by `rows*cell_h`; keep it within
    // FFmpeg's per-dimension limit so an absurd grid fails here, not deep in the
    // muxer with an out-of-memory allocation.
    let out_w = u64::from(cols).checked_mul(u64::from(cell_w));
    let out_h = u64::from(rows).checked_mul(u64::from(cell_h));
    match (out_w, out_h) {
        (Some(w), Some(h)) if w <= 65_535 && h <= 65_535 => {}
        _ => {
            return Err(Error::InvalidRecipeArg(format!(
                "sprite sheet size ({cols}*{cell_w}) x ({rows}*{cell_h}) exceeds the 65535x65535 limit"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests (pure string generation + invalid-args; no media touched)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_width_only() {
        assert_eq!(build_scale(Some(320), None).unwrap(), "scale=320:-2");
    }

    #[test]
    fn scale_height_only() {
        assert_eq!(build_scale(None, Some(240)).unwrap(), "scale=-2:240");
    }

    #[test]
    fn scale_both() {
        assert_eq!(build_scale(Some(320), Some(240)).unwrap(), "scale=320:240");
    }

    #[test]
    fn scale_both_none_errs() {
        assert!(matches!(
            build_scale(None, None),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn scale_zero_errs() {
        assert!(matches!(
            build_scale(Some(0), None),
            Err(Error::InvalidRecipeArg(_))
        ));
        assert!(matches!(
            build_scale(Some(320), Some(0)),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sec_to_us_ok() {
        assert_eq!(sec_to_us(12.0).unwrap(), 12_000_000);
        assert_eq!(sec_to_us(0.0).unwrap(), 0);
        assert_eq!(sec_to_us(1.5).unwrap(), 1_500_000);
    }

    #[test]
    fn sec_to_us_rejects_bad() {
        assert!(sec_to_us(f64::NAN).is_err());
        assert!(sec_to_us(f64::INFINITY).is_err());
        assert!(sec_to_us(f64::NEG_INFINITY).is_err());
        assert!(sec_to_us(-1.0).is_err());
    }

    #[test]
    fn percent_to_us_ok() {
        assert_eq!(percent_to_us(50.0, 10_000_000).unwrap(), 5_000_000);
        assert_eq!(percent_to_us(0.0, 10_000_000).unwrap(), 0);
        // p == 100 resolves to the end of the stream.
        assert_eq!(percent_to_us(100.0, 10_000_000).unwrap(), 10_000_000);
    }

    #[test]
    fn percent_to_us_rejects_bad() {
        assert!(percent_to_us(-1.0, 10_000_000).is_err());
        assert!(percent_to_us(101.0, 10_000_000).is_err());
        assert!(percent_to_us(f64::NAN, 10_000_000).is_err());
        // AV_NOPTS-style unknown duration.
        assert!(percent_to_us(50.0, -1).is_err());
    }

    #[test]
    fn thumbnail_desc_input_seek() {
        let (seek, desc) = build_thumbnail_desc(
            &Locator::TimeUs(12_000_000),
            SeekMode::InputSeek,
            "scale=320:-2",
        );
        assert_eq!(seek, Some(12_000_000));
        assert_eq!(desc, "[0:v]scale=320:-2[thumb]");
    }

    #[test]
    fn thumbnail_desc_filter_scan() {
        let (seek, desc) = build_thumbnail_desc(
            &Locator::TimeUs(12_000_000),
            SeekMode::FilterScan,
            "scale=320:-2",
        );
        assert_eq!(seek, None);
        // single backslash before the comma at runtime.
        assert_eq!(
            desc,
            "[0:v]select='gte(t\\,12000000/1000000)',scale=320:-2[thumb]"
        );
    }

    #[test]
    fn thumbnail_desc_frame_ignores_seek() {
        let (seek, desc) =
            build_thumbnail_desc(&Locator::Frame(7), SeekMode::InputSeek, "scale=-2:240");
        assert_eq!(seek, None);
        assert_eq!(desc, "[0:v]select='eq(n\\,7)',scale=-2:240[thumb]");
    }

    #[test]
    fn sprite_sampling_strings() {
        assert_eq!(
            sprite_sampling_desc(&SpriteSampling::IntervalUs(2_000_000)),
            "fps=1000000/2000000"
        );
        assert_eq!(
            sprite_sampling_desc(&SpriteSampling::FrameStride(10)),
            "select='not(mod(n\\,10))'"
        );
        assert_eq!(
            sprite_sampling_desc(&SpriteSampling::EvenSpread {
                numerator: 25_000_000,
                duration_us: 100_000_000,
            }),
            "fps=25000000/100000000"
        );
    }

    #[test]
    fn sprite_desc_full() {
        let desc = build_sprite_desc(&SpriteSampling::IntervalUs(2_000_000), 160, 90, 5, 5, 25);
        assert_eq!(
            desc,
            "[0:v]fps=1000000/2000000,scale=160:90,tile=layout=5x5:nb_frames=25[thumb]"
        );
    }

    #[test]
    fn sprite_desc_frame_stride() {
        let desc = build_sprite_desc(&SpriteSampling::FrameStride(10), 160, 90, 4, 3, 12);
        assert_eq!(
            desc,
            "[0:v]select='not(mod(n\\,10))',scale=160:90,tile=layout=4x3:nb_frames=12[thumb]"
        );
    }

    // ----- invalid-args on the public entry points (validation precedes any probe/build) -----

    #[test]
    fn thumbnail_rejects_no_dims() {
        let opts = ThumbnailOptions {
            width: None,
            height: None,
            ..Default::default()
        };
        assert!(matches!(
            thumbnail("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn thumbnail_rejects_bad_quality() {
        let lo = ThumbnailOptions {
            quality: 1,
            ..Default::default()
        };
        let hi = ThumbnailOptions {
            quality: 32,
            ..Default::default()
        };
        assert!(matches!(
            thumbnail("x.mp4", "o.jpg", lo),
            Err(Error::InvalidRecipeArg(_))
        ));
        assert!(matches!(
            thumbnail("x.mp4", "o.jpg", hi),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn thumbnail_rejects_bad_time() {
        let neg = ThumbnailOptions {
            at: At::Sec(-1.0),
            ..Default::default()
        };
        let nan = ThumbnailOptions {
            at: At::Sec(f64::NAN),
            ..Default::default()
        };
        assert!(matches!(
            thumbnail("x.mp4", "o.jpg", neg),
            Err(Error::InvalidRecipeArg(_))
        ));
        assert!(matches!(
            thumbnail("x.mp4", "o.jpg", nan),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_zero_grid() {
        let opts = SpriteSheetOptions {
            grid: (0, 5),
            every: Every::Frames(10),
            cell: (160, 90),
            quality: 2,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_oversized_sheet() {
        let opts = SpriteSheetOptions {
            grid: (65_536, 65_536),
            every: Every::Frames(10),
            cell: (1, 1),
            quality: 2,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_zero_cell() {
        let opts = SpriteSheetOptions {
            grid: (5, 5),
            every: Every::Frames(10),
            cell: (160, 0),
            quality: 2,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_frames_zero() {
        let opts = SpriteSheetOptions {
            grid: (5, 5),
            every: Every::Frames(0),
            cell: (160, 90),
            quality: 2,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_sec_zero() {
        let opts = SpriteSheetOptions {
            grid: (5, 5),
            every: Every::Sec(0.0),
            cell: (160, 90),
            quality: 2,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }

    #[test]
    fn sprite_rejects_bad_quality() {
        let opts = SpriteSheetOptions {
            grid: (5, 5),
            every: Every::Frames(10),
            cell: (160, 90),
            quality: 100,
        };
        assert!(matches!(
            sprite_sheet("x.mp4", "o.jpg", opts),
            Err(Error::InvalidRecipeArg(_))
        ));
    }
}
