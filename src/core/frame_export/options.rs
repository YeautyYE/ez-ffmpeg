//! Public option enums for frame export: how to sample frames, what packed
//! pixel layout to produce, and how to interpret source color.

/// How to select which decoded frames are exported.
///
/// Index-based modes (`All`, `EveryNth`) count delivered decoder output;
/// time-based modes (`EverySec`) float their grid on the first delivered frame
/// and compare presentation timestamps.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Sampling {
    /// Every decoded frame (the default).
    All,
    /// Every `n`-th decoded frame, starting with the first (`n >= 1`).
    EveryNth(u64),
    /// Starting from the first delivered frame, the first frame at or after each
    /// subsequent `k * seconds` boundary (`k = 1, 2, …`). No synthesis: sparse
    /// sources yield fewer frames, and each source frame is selected at most once.
    EverySec(f64),
    /// Only frames flagged as keyframes. Pins the decoder option
    /// `skip_frame=nokey` for a decode-time fast path, plus a key-flag check as a
    /// belt for codecs that ignore the hint.
    KeyframesOnly,
    /// Exactly `n` frames (fewer only when a lower `max_frames` cap wins),
    /// sampled uniformly by presentation time over the (trimmed) duration —
    /// the standard VLM/CLIP primitive. Short inputs pad by repeating the
    /// nearest displayed frame; each repeat keeps that frame's `pts_us`.
    /// Requires a resolvable duration (see `duration_hint_us`).
    ///
    /// Cost model: the strategy is a single sequential decode of the (trimmed)
    /// input — there is no seek-per-target fast path. Frames that lose the
    /// grid race are dropped pre-filtergraph and pay no scaling, but once the
    /// grid completes the remaining tail (~`1/(2n)` of the span) passes
    /// through the graph — and is scaled — solely to keep the encoder-side
    /// termination path fed.
    UniformN(u32),
}

/// A packed, 8-bit pixel layout. Rows are tight (`width * bytes_per_pixel`, no
/// padding) and top-down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PixelLayout {
    /// 24-bit RGB, 3 bytes per pixel (`R G B`).
    Rgb24,
    /// 32-bit RGBA, 4 bytes per pixel (`R G B A`).
    Rgba32,
    /// 8-bit grayscale, 1 byte per pixel.
    Gray8,
}

impl PixelLayout {
    /// Bytes per pixel for this layout: 3 (`Rgb24`), 4 (`Rgba32`), or 1 (`Gray8`).
    pub fn bytes_per_pixel(self) -> usize {
        match self {
            PixelLayout::Rgb24 => 3,
            PixelLayout::Rgba32 => 4,
            PixelLayout::Gray8 => 1,
        }
    }

    /// The FFmpeg `format` filter pixel-format name that produces this layout.
    pub(crate) fn ffmpeg_format_name(self) -> &'static str {
        match self {
            PixelLayout::Rgb24 => "rgb24",
            PixelLayout::Rgba32 => "rgba",
            PixelLayout::Gray8 => "gray",
        }
    }

    /// The exact `AVPixelFormat` a packed frame of this layout must carry. Used
    /// to reject any pixel format the graph did not produce before packing.
    pub(crate) fn av_pixel_format(self) -> ffmpeg_sys_next::AVPixelFormat {
        use ffmpeg_sys_next::AVPixelFormat::{AV_PIX_FMT_GRAY8, AV_PIX_FMT_RGB24, AV_PIX_FMT_RGBA};
        match self {
            PixelLayout::Rgb24 => AV_PIX_FMT_RGB24,
            PixelLayout::Rgba32 => AV_PIX_FMT_RGBA,
            PixelLayout::Gray8 => AV_PIX_FMT_GRAY8,
        }
    }
}

/// The swscale precision tier for the conversion stage.
///
/// The extractor runs any resize plus the pixel-format conversion as ONE
/// swscale pass, and this tier sets that pass's scaler flags — so it affects
/// resized output too, not only the YUV → RGB step. Color interpretation —
/// which matrix and range are applied ([`ColorPolicy`]) — is identical in
/// both tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ConversionPrecision {
    /// The FFmpeg CLI's default swscale configuration (`flags=bicubic`, no
    /// extra precision flags). This keeps swscale eligible for its fastest
    /// converters — including the unscaled yuv420p → rgb24 special case — and
    /// matches the bytes an `ffmpeg -vf "scale=..,format=.."` run with default
    /// flags produces. This is the default.
    #[default]
    Standard,
    /// Adds `accurate_rnd+full_chroma_int`: accurate rounding plus full
    /// horizontal chroma interpolation. Relative to
    /// [`Standard`](ConversionPrecision::Standard) the mean difference is a
    /// couple of steps per 8-bit channel; individual pixels on sharp chroma
    /// edges can differ by more, because the two tiers reconstruct subsampled
    /// chroma differently — and `accurate_rnd` also changes rounding on
    /// resized output, including single-plane formats like gray. The flags
    /// disable swscale's unscaled fast-path converters, which multiplies the
    /// conversion cost several-fold (at 1080p the conversion, not the decode,
    /// becomes the bottleneck). Opt in when the last bit of rounding and
    /// chroma reconstruction matters more than throughput — e.g. quality
    /// metrics or golden-reference pipelines.
    High,
}

/// A YUV → RGB conversion matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum YuvMatrix {
    /// ITU-R BT.601 (SD).
    Bt601,
    /// ITU-R BT.709 (HD).
    Bt709,
}

impl YuvMatrix {
    /// The swscale `in_color_matrix` token.
    pub(crate) fn in_color_matrix(self) -> &'static str {
        match self {
            YuvMatrix::Bt601 => "bt601",
            YuvMatrix::Bt709 => "bt709",
        }
    }
}

/// The signal range of the source YUV.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum YuvRange {
    /// Limited / "TV" range (Y in 16..=235).
    Limited,
    /// Full / "PC" range (Y in 0..=255).
    Full,
}

impl YuvRange {
    /// The swscale `in_range` token.
    pub(crate) fn in_range(self) -> &'static str {
        match self {
            YuvRange::Limited => "tv",
            YuvRange::Full => "pc",
        }
    }
}

/// How the source color is interpreted during YUV → RGB conversion.
///
/// The single most defensible behavior of this module: [`Tagged`](ColorPolicy::Tagged)
/// honors the frame's own colorspace tags instead of assuming BT.601 for
/// everything (the default many decode-to-RGB paths get wrong for HD/BT.709).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ColorPolicy {
    /// Honor the frame's color tags; untagged frames fall through to swscale's
    /// documented default (BT.601 / limited). This is the default.
    #[default]
    Tagged,
    /// Like [`Tagged`](ColorPolicy::Tagged), but frames with an UNSPECIFIED
    /// colorspace get a per-frame resolution guess: BT.709 when the frame
    /// height is at least 720, BT.601 otherwise (an UNSPECIFIED range is
    /// pinned to limited). Tagged frames are never overridden, and the guess
    /// never freezes — a mid-stream change from untagged to tagged (or a
    /// resolution change) takes effect on that very frame.
    ///
    /// On inputs with multiple video streams, set
    /// [`video_stream_index`](super::FrameExtractor::video_stream_index) if the
    /// exported stream is not the first video stream; the run is otherwise
    /// rejected with a typed error rather than silently leaving the exported
    /// stream unstamped.
    TaggedOrResolutionGuess,
    /// Force a specific interpretation for ALL frames, overriding any tags.
    Force {
        /// The YUV matrix to assume.
        matrix: YuvMatrix,
        /// The signal range to assume.
        range: YuvRange,
    },
}
