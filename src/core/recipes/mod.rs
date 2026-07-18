//! One-shot "recipe" helpers over the ez-ffmpeg builder for common FFmpeg
//! workflows. Each recipe generates a filtergraph plus output configuration
//! from a small typed options struct and runs it to completion; the raw
//! [`filter_desc`](crate::FfmpegContext) escape hatch remains available for
//! anything these do not cover.
//!
//! - [`thumbnail`](crate::core::recipes::thumbnail::thumbnail) /
//!   [`sprite_sheet`](crate::core::recipes::thumbnail::sprite_sheet):
//!   still-frame and storyboard extraction.
//! - [`animated_gif`](crate::core::recipes::gif::animated_gif): high-quality
//!   GIF export (palettegen/paletteuse).
//! - [`HlsLadder`](crate::core::recipes::hls::HlsLadder): VOD adaptive-bitrate
//!   HLS ladder.

pub mod gif;
pub mod hls;
pub mod hls_master;
pub mod thumbnail;

pub use gif::*;
pub use hls::*;
pub use thumbnail::*;
