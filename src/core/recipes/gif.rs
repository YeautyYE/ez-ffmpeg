//! High-quality animated GIF export.
//!
//! A naive `input -> output.gif` conversion produces poor results: the GIF
//! codec is limited to a 256-colour palette, and letting FFmpeg pick that
//! palette per frame yields banding and dithering artefacts. The correct
//! recipe is a single filtergraph that splits the stream in two, builds one
//! optimised palette for the whole clip with `palettegen`, and applies it with
//! `paletteuse`:
//!
//! ```text
//! [0:v]fps=12,scale=480:-1:flags=lanczos,split[a][b];[a]palettegen[p];[b][p]paletteuse=dither=sierra2[v]
//! ```
//!
//! [`animated_gif`] wraps that filtergraph plus the `gif` muxer configuration
//! behind a small typed [`GifOptions`] struct. For anything these knobs do not
//! cover, the raw [`filter_desc`](crate::FfmpegContext::builder) escape hatch
//! remains available.
//!
//! # Example
//!
//! ```rust,ignore
//! use ez_ffmpeg::core::recipes::gif::{animated_gif, GifOptions, GifTrim, Dither};
//!
//! // Default 12 fps, source width, whole clip, sierra2 dither, infinite loop.
//! animated_gif("input.mp4", "output.gif", GifOptions::default())?;
//!
//! // A 3-second, 480px-wide clip starting at 5s, capped at 128 colours.
//! let opts = GifOptions {
//!     fps: 15,
//!     width: Some(480),
//!     trim: Some(GifTrim::from_micros(5_000_000, 3_000_000)),
//!     dither: Dither::Bayer { scale: 3 },
//!     max_colors: Some(128),
//!     ..GifOptions::default()
//! };
//! animated_gif("input.mp4", "clip.gif", opts)?;
//! ```

use std::ffi::CString;
use std::ptr::null;
use std::time::Duration;

use ffmpeg_sys_next::{av_guess_format, avcodec_find_encoder_by_name, avfilter_get_by_name};

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::error::{Error, Result};

/// Error-diffusion / ordered dithering algorithm applied by `paletteuse` when
/// mapping frames onto the generated palette.
///
/// The variants correspond to the `dither` option of FFmpeg's `paletteuse`
/// filter (verified against FFmpeg n7.1). Only the commonly useful subset is
/// exposed; richer control is available through the raw `filter_desc` escape
/// hatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dither {
    /// No dithering (`dither=none`): smallest files, most visible banding.
    None,
    /// Ordered Bayer dithering (`dither=bayer`).
    ///
    /// `scale` maps to `bayer_scale` and must be in `0..=5`; larger values
    /// reduce banding at the cost of a coarser crosshatch pattern.
    Bayer { scale: u8 },
    /// Sierra-2 error diffusion (`dither=sierra2`).
    Sierra2,
    /// Floyd–Steinberg error diffusion (`dither=floyd_steinberg`).
    FloydSteinberg,
}

impl Dither {
    /// The `paletteuse` `dither=` value for this variant.
    fn filter_name(self) -> &'static str {
        match self {
            Dither::None => "none",
            Dither::Bayer { .. } => "bayer",
            Dither::Sierra2 => "sierra2",
            Dither::FloydSteinberg => "floyd_steinberg",
        }
    }
}

/// How many times the animation should repeat, expressed as the GIF muxer's
/// `loop` AVOption.
///
/// FFmpeg encodes this as a NETSCAPE loop count: `0` loops forever, `-1`
/// plays exactly once, and `N` repeats `N` times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GifLoop {
    /// Loop forever (`loop=0`). This is the default.
    Infinite,
    /// Repeat a fixed number of times (`loop=N`).
    ///
    /// Must be in `1..=65535`. `0` is rejected because the GIF muxer would
    /// treat it as [`GifLoop::Infinite`]; use that variant explicitly instead.
    Count(u32),
    /// Play exactly once, no looping (`loop=-1`).
    Once,
}

/// A trim window over the source, stored in microseconds.
///
/// The trim is always applied on the **input** side (via
/// [`Input::set_start_time_us`] + [`Input::set_recording_time_us`]) so that
/// `palettegen` builds its palette from exactly the requested segment rather
/// than from the start of the clip to EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GifTrim {
    /// Offset from the start of the source, in microseconds (`>= 0`).
    pub start_us: i64,
    /// Length of the window, in microseconds (`> 0`).
    pub duration_us: i64,
}

impl GifTrim {
    /// Builds a trim window from raw microsecond values.
    pub fn from_micros(start_us: i64, duration_us: i64) -> Self {
        Self {
            start_us,
            duration_us,
        }
    }

    /// Builds a trim window from [`Duration`]s.
    ///
    /// Durations longer than [`i64::MAX`] microseconds (~292 000 years) are
    /// saturated rather than overflowing; such values are rejected by
    /// validation anyway in any realistic pipeline.
    pub fn from_durations(start: Duration, duration: Duration) -> Self {
        Self {
            start_us: micros_saturating(start),
            duration_us: micros_saturating(duration),
        }
    }
}

impl From<(Duration, Duration)> for GifTrim {
    fn from((start, duration): (Duration, Duration)) -> Self {
        Self::from_durations(start, duration)
    }
}

/// Clamp a [`Duration`] to `i64` microseconds without overflowing.
fn micros_saturating(d: Duration) -> i64 {
    d.as_micros().min(i64::MAX as u128) as i64
}

/// Options controlling [`animated_gif`].
///
/// Use [`GifOptions::default`] and override individual fields with struct
/// update syntax:
///
/// ```rust,ignore
/// let opts = GifOptions { width: Some(320), ..GifOptions::default() };
/// ```
#[derive(Debug, Clone)]
pub struct GifOptions {
    /// Output frame rate (`fps` filter). Must be `> 0`. Default: `12`.
    pub fps: u32,
    /// Target width in pixels; height is chosen automatically to preserve the
    /// aspect ratio (`scale={width}:-1:flags=lanczos`).
    ///
    /// `None` keeps the source width (no scaling). `Some(0)` is rejected.
    /// Default: `None`.
    pub width: Option<u32>,
    /// Optional trim window over the source. `None` encodes the whole clip.
    /// Default: `None`.
    pub trim: Option<GifTrim>,
    /// Dithering algorithm for `paletteuse`. Default: [`Dither::Sierra2`].
    pub dither: Dither,
    /// Palette size for `palettegen` (`max_colors`). Must be in `4..=256`.
    /// `None` uses FFmpeg's default of 256. Default: `None`.
    pub max_colors: Option<u32>,
    /// How many times the animation repeats. Default: [`GifLoop::Infinite`].
    pub loop_: GifLoop,
}

impl Default for GifOptions {
    fn default() -> Self {
        Self {
            fps: 12,
            width: None,
            trim: None,
            dither: Dither::Sierra2,
            max_colors: None,
            loop_: GifLoop::Infinite,
        }
    }
}

/// Encodes `input` into a high-quality animated GIF at `output`.
///
/// The generated filtergraph runs `fps` → (optional) `scale` → `split`, then
/// `palettegen` on one branch and `paletteuse` on the other, labelling the
/// result `[v]`. That single video output is mapped explicitly onto the `gif`
/// muxer, so no audio (which GIF cannot carry) is ever routed to the output.
///
/// # Errors
///
/// Returns [`Error::InvalidRecipeArg`] when [`GifOptions`] are out of range
/// (see the per-field docs) or when this FFmpeg build lacks a filter, muxer or
/// encoder the recipe needs. Any failure during the encode itself is
/// propagated from the underlying scheduler.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::core::recipes::gif::{animated_gif, GifOptions};
///
/// animated_gif("input.mp4", "output.gif", GifOptions::default())?;
/// ```
pub fn animated_gif(
    input: impl Into<Input>,
    output: impl Into<Output>,
    opts: GifOptions,
) -> Result<()> {
    validate(&opts)?;
    check_capabilities()?;

    let filter_desc = build_gif_desc(&opts);

    let input = apply_trim(input.into(), opts.trim);
    let output = configure_output(output.into(), opts.loop_);

    FfmpegContext::builder()
        .input(input)
        .filter_desc(filter_desc)
        .output(output)
        .build()?
        .start()?
        .wait()
}

/// Builds the palette filtergraph description for the given options.
///
/// The output label is always `[v]`; callers map it with
/// `output.add_stream_map("[v]")`. When [`GifOptions::width`] is `None` the
/// `scale` filter is omitted entirely (the source width is kept).
///
/// This function performs no validation — call [`validate`] first.
pub(crate) fn build_gif_desc(opts: &GifOptions) -> String {
    let mut desc = format!("[0:v]fps={}", opts.fps);

    if let Some(width) = opts.width {
        desc.push_str(&format!(",scale={width}:-1:flags=lanczos"));
    }

    desc.push_str(",split[a][b];[a]palettegen");
    if let Some(max_colors) = opts.max_colors {
        desc.push_str(&format!("=max_colors={max_colors}"));
    }

    desc.push_str("[p];[b][p]paletteuse=dither=");
    desc.push_str(opts.dither.filter_name());
    if let Dither::Bayer { scale } = opts.dither {
        desc.push_str(&format!(":bayer_scale={scale}"));
    }

    desc.push_str("[v]");
    desc
}

/// Applies the trim window on the **input** side, if present.
fn apply_trim(input: Input, trim: Option<GifTrim>) -> Input {
    match trim {
        Some(trim) => input
            .set_start_time_us(trim.start_us)
            .set_recording_time_us(trim.duration_us),
        None => input,
    }
}

/// Forces the `gif` muxer, maps only the filtergraph video output `[v]`, and
/// sets the loop count as a muxer AVOption.
fn configure_output(output: Output, gif_loop: GifLoop) -> Output {
    output
        .set_format("gif")
        .add_stream_map("[v]")
        .set_format_opt("loop", loop_option_value(gif_loop))
}

/// The muxer `loop` AVOption value for a [`GifLoop`].
fn loop_option_value(gif_loop: GifLoop) -> String {
    match gif_loop {
        GifLoop::Infinite => "0".to_string(),
        GifLoop::Once => "-1".to_string(),
        GifLoop::Count(count) => count.to_string(),
    }
}

/// Validates [`GifOptions`], returning [`Error::InvalidRecipeArg`] on the first
/// offending field.
fn validate(opts: &GifOptions) -> Result<()> {
    if opts.fps == 0 {
        return Err(Error::InvalidRecipeArg("fps must be greater than 0".to_string()));
    }

    if opts.width == Some(0) {
        return Err(Error::InvalidRecipeArg(
            "width must be greater than 0 (use None to keep the source width)".to_string(),
        ));
    }

    if let Some(max_colors) = opts.max_colors {
        if !(4..=256).contains(&max_colors) {
            return Err(Error::InvalidRecipeArg(format!(
                "max_colors must be in the range 4..=256, got {max_colors}"
            )));
        }
    }

    if let Dither::Bayer { scale } = opts.dither {
        if scale > 5 {
            return Err(Error::InvalidRecipeArg(format!(
                "bayer_scale must be in the range 0..=5, got {scale}"
            )));
        }
    }

    match opts.loop_ {
        GifLoop::Count(0) => {
            return Err(Error::InvalidRecipeArg(
                "loop count of 0 is ambiguous; use GifLoop::Infinite for endless playback"
                    .to_string(),
            ));
        }
        GifLoop::Count(count) if count > 65_535 => {
            return Err(Error::InvalidRecipeArg(format!(
                "loop count must be in the range 1..=65535, got {count}"
            )));
        }
        _ => {}
    }

    if let Some(trim) = opts.trim {
        if trim.start_us < 0 {
            return Err(Error::InvalidRecipeArg(format!(
                "trim start must be >= 0 us, got {}",
                trim.start_us
            )));
        }
        if trim.duration_us <= 0 {
            return Err(Error::InvalidRecipeArg(format!(
                "trim duration must be > 0 us, got {}",
                trim.duration_us
            )));
        }
    }

    Ok(())
}

/// Best-effort check that this FFmpeg build actually provides every filter,
/// the muxer and the encoder the recipe relies on.
///
/// Returning a clear [`Error::InvalidRecipeArg`] here surfaces a missing
/// component up front instead of as an opaque failure deep inside `start()`.
/// The lookups query FFmpeg's static registries and do not require a fully
/// initialised context.
fn check_capabilities() -> Result<()> {
    const REQUIRED_FILTERS: [&str; 5] = ["fps", "scale", "split", "palettegen", "paletteuse"];

    // SAFETY: each lookup receives a valid NUL-terminated CString pointer and
    // only reads FFmpeg's static registries (no context/lifetime concerns);
    // the returned pointers are used solely for null checks, never dereferenced.
    unsafe {
        for name in REQUIRED_FILTERS {
            let c_name = CString::new(name).map_err(|_| {
                Error::InvalidRecipeArg(format!("invalid filter name {name:?}"))
            })?;
            if avfilter_get_by_name(c_name.as_ptr()).is_null() {
                return Err(Error::InvalidRecipeArg(format!(
                    "this FFmpeg build is missing the '{name}' filter required for GIF export"
                )));
            }
        }

        let gif = CString::new("gif").map_err(|_| {
            Error::InvalidRecipeArg("invalid format name \"gif\"".to_string())
        })?;

        if av_guess_format(gif.as_ptr(), null(), null()).is_null() {
            return Err(Error::InvalidRecipeArg(
                "this FFmpeg build is missing the 'gif' muxer".to_string(),
            ));
        }

        if avcodec_find_encoder_by_name(gif.as_ptr()).is_null() {
            return Err(Error::InvalidRecipeArg(
                "this FFmpeg build is missing the 'gif' encoder".to_string(),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desc_default() {
        let desc = build_gif_desc(&GifOptions::default());
        assert_eq!(
            desc,
            "[0:v]fps=12,split[a][b];[a]palettegen[p];[b][p]paletteuse=dither=sierra2[v]"
        );
    }

    #[test]
    fn desc_with_width_scales() {
        let opts = GifOptions {
            width: Some(480),
            ..GifOptions::default()
        };
        assert_eq!(
            build_gif_desc(&opts),
            "[0:v]fps=12,scale=480:-1:flags=lanczos,split[a][b];\
             [a]palettegen[p];[b][p]paletteuse=dither=sierra2[v]"
        );
    }

    #[test]
    fn desc_without_width_omits_scale() {
        let opts = GifOptions {
            width: None,
            ..GifOptions::default()
        };
        assert!(!build_gif_desc(&opts).contains("scale="));
    }

    #[test]
    fn desc_with_max_colors() {
        let opts = GifOptions {
            max_colors: Some(64),
            ..GifOptions::default()
        };
        assert!(build_gif_desc(&opts).contains("[a]palettegen=max_colors=64[p]"));
    }

    #[test]
    fn desc_bayer_carries_scale() {
        let opts = GifOptions {
            dither: Dither::Bayer { scale: 3 },
            ..GifOptions::default()
        };
        assert!(build_gif_desc(&opts).ends_with("paletteuse=dither=bayer:bayer_scale=3[v]"));
    }

    #[test]
    fn desc_dither_names() {
        let name = |d: Dither| {
            let opts = GifOptions {
                dither: d,
                ..GifOptions::default()
            };
            build_gif_desc(&opts)
        };
        assert!(name(Dither::None).ends_with("dither=none[v]"));
        assert!(name(Dither::Sierra2).ends_with("dither=sierra2[v]"));
        assert!(name(Dither::FloydSteinberg).ends_with("dither=floyd_steinberg[v]"));
    }

    #[test]
    fn desc_full_combination() {
        let opts = GifOptions {
            fps: 15,
            width: Some(480),
            trim: Some(GifTrim::from_micros(5_000_000, 3_000_000)),
            dither: Dither::Bayer { scale: 2 },
            max_colors: Some(128),
            loop_: GifLoop::Once,
        };
        assert_eq!(
            build_gif_desc(&opts),
            "[0:v]fps=15,scale=480:-1:flags=lanczos,split[a][b];\
             [a]palettegen=max_colors=128[p];[b][p]paletteuse=dither=bayer:bayer_scale=2[v]"
        );
    }

    #[test]
    fn loop_values() {
        assert_eq!(loop_option_value(GifLoop::Infinite), "0");
        assert_eq!(loop_option_value(GifLoop::Once), "-1");
        assert_eq!(loop_option_value(GifLoop::Count(5)), "5");
    }

    #[test]
    fn configure_output_sets_gif_map_and_loop() {
        let output = configure_output(Output::from("out.gif"), GifLoop::Infinite);
        assert_eq!(output.format.as_deref(), Some("gif"));

        assert_eq!(output.stream_map_specs.len(), 1);
        assert_eq!(output.stream_map_specs[0].linklabel, "[v]");
        assert!(!output.stream_map_specs[0].copy);

        let loop_value = output
            .format_opts
            .as_ref()
            .and_then(|opts| opts.get("loop"))
            .map(String::as_str);
        assert_eq!(loop_value, Some("0"));
    }

    #[test]
    fn configure_output_count_loop() {
        let output = configure_output(Output::from("out.gif"), GifLoop::Count(3));
        let loop_value = output
            .format_opts
            .as_ref()
            .and_then(|opts| opts.get("loop"))
            .map(String::as_str);
        assert_eq!(loop_value, Some("3"));
    }

    #[test]
    fn apply_trim_sets_input_window() {
        let input = apply_trim(
            Input::from("in.mp4"),
            Some(GifTrim::from_micros(1_000_000, 2_000_000)),
        );
        assert_eq!(input.start_time_us, Some(1_000_000));
        assert_eq!(input.recording_time_us, Some(2_000_000));
    }

    #[test]
    fn apply_trim_none_leaves_input_untouched() {
        let input = apply_trim(Input::from("in.mp4"), None);
        assert_eq!(input.start_time_us, None);
        assert_eq!(input.recording_time_us, None);
    }

    #[test]
    fn trim_from_durations_converts_to_micros() {
        let trim = GifTrim::from_durations(Duration::from_secs(1), Duration::from_millis(2500));
        assert_eq!(trim.start_us, 1_000_000);
        assert_eq!(trim.duration_us, 2_500_000);
    }

    #[test]
    fn validate_rejects_zero_fps() {
        let opts = GifOptions {
            fps: 0,
            ..GifOptions::default()
        };
        assert!(matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))));
    }

    #[test]
    fn validate_rejects_zero_width() {
        let opts = GifOptions {
            width: Some(0),
            ..GifOptions::default()
        };
        assert!(matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))));
    }

    #[test]
    fn validate_max_colors_bounds() {
        for bad in [0u32, 3, 257, 1000] {
            let opts = GifOptions {
                max_colors: Some(bad),
                ..GifOptions::default()
            };
            assert!(
                matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))),
                "max_colors={bad} should be rejected"
            );
        }
        for ok in [4u32, 128, 256] {
            let opts = GifOptions {
                max_colors: Some(ok),
                ..GifOptions::default()
            };
            assert!(validate(&opts).is_ok(), "max_colors={ok} should be accepted");
        }
    }

    #[test]
    fn validate_rejects_bayer_scale_above_five() {
        let opts = GifOptions {
            dither: Dither::Bayer { scale: 6 },
            ..GifOptions::default()
        };
        assert!(matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))));

        let ok = GifOptions {
            dither: Dither::Bayer { scale: 5 },
            ..GifOptions::default()
        };
        assert!(validate(&ok).is_ok());
    }

    #[test]
    fn validate_rejects_loop_count_zero() {
        let opts = GifOptions {
            loop_: GifLoop::Count(0),
            ..GifOptions::default()
        };
        assert!(matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))));
    }

    #[test]
    fn validate_rejects_loop_count_over_max() {
        let opts = GifOptions {
            loop_: GifLoop::Count(65_536),
            ..GifOptions::default()
        };
        assert!(matches!(validate(&opts), Err(Error::InvalidRecipeArg(_))));

        let ok = GifOptions {
            loop_: GifLoop::Count(65_535),
            ..GifOptions::default()
        };
        assert!(validate(&ok).is_ok());
    }

    #[test]
    fn validate_rejects_bad_trim() {
        let negative_start = GifOptions {
            trim: Some(GifTrim::from_micros(-1, 1_000_000)),
            ..GifOptions::default()
        };
        assert!(matches!(
            validate(&negative_start),
            Err(Error::InvalidRecipeArg(_))
        ));

        let zero_duration = GifOptions {
            trim: Some(GifTrim::from_micros(0, 0)),
            ..GifOptions::default()
        };
        assert!(matches!(
            validate(&zero_duration),
            Err(Error::InvalidRecipeArg(_))
        ));

        let ok = GifOptions {
            trim: Some(GifTrim::from_micros(0, 1)),
            ..GifOptions::default()
        };
        assert!(validate(&ok).is_ok());
    }
}
