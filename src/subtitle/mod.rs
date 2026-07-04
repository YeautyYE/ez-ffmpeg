//! Native subtitle burn-in (ASS/SRT) rendered by a pure-Rust engine inside
//! the frame pipeline.
//!
//! FFmpeg's own `subtitles`/`ass` filters are only available when the
//! *linked* FFmpeg build was configured with `--enable-libass`; this module
//! renders subtitles itself — parser, shaper, rasterizer and blender are
//! all Rust (fontdb + ttf-parser + rustybuzz + zeno) — so it works with any
//! FFmpeg build, needs no libass at build or run time, and additionally
//! accepts in-memory scripts (no temp files) and explicit font files (no
//! fontconfig lookup races).

pub(crate) mod ass;
pub(crate) mod backend;
pub(crate) mod blend;
mod filter;
pub(crate) mod layout;
mod loader;
mod options;
pub(crate) mod render;
mod source;

pub use filter::SubtitleFilter;
pub use options::{FontProvider, SubtitleError, SubtitleFilterBuilder, TextShaping};

#[cfg(test)]
pub(crate) mod test_util;

#[cfg(test)]
mod bench_kernels;
