//! One-shot "recipe" helpers over the ez-ffmpeg builder for common FFmpeg
//! workflows. Each recipe generates a filtergraph plus output configuration
//! from a small typed options struct and runs it to completion; the raw
//! [`filter_desc`](crate::FfmpegContext) escape hatch remains available for
//! anything these do not cover.
//!
//! - [`thumbnail`] / [`sprite_sheet`]: still-frame and storyboard extraction.
//! - [`animated_gif`]: high-quality GIF export (palettegen/paletteuse).
//! - [`HlsLadder`]: VOD adaptive-bitrate HLS ladder.

pub mod thumbnail;
pub mod gif;
pub mod hls;
pub mod hls_master;

pub use thumbnail::*;
pub use gif::*;
pub use hls::*;
