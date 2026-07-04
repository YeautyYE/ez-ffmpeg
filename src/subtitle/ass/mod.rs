//! Pure-Rust ASS/SSA script engine. First slice: the script parser.
//!
//! This is the input layer of the pure-Rust subtitle renderer that will
//! implement [`super::backend::SubtitleRenderer`] without linking libass.
//! Parsing is a faithful port of libass 0.17.1 (`ass.c`), preserving its
//! VSFilter-compatible behavior on purpose, including the quirks:
//!
//! - numbers wrap modulo 2^32, `&H`/`0x` prefixes select hex, trailing junk
//!   is ignored, invalid values parse as 0;
//! - the styles list always starts with a built-in `Default` style; style
//!   lookups search backwards and fall back to the default style;
//! - `Format:` lines drive field order; unknown fields are skipped; short
//!   lines keep defaults; `Text` swallows the raw rest of the line;
//! - SSA scripts get the VSFilter fixups (BackColour → OutlineColour,
//!   AlphaLevel, alignment remaps), ASS alignments are numpad-converted;
//! - custom format lines and legacy FFmpeg-converted scripts default
//!   `ScaledBorderAndShadow` to yes;
//! - unknown section headers do NOT switch sections (their lines bleed
//!   into the current one), parsing stops at the first NUL byte, and BOMs
//!   at line starts are skipped.
//!
//! Semantics were verified against the libass sources function by function
//! (`process_text` / `process_line` / `process_style` /
//! `process_event_tail` / `ass_process_force_style` / `decode_font`);
//! resolution defaulting and rendering-time behavior are deliberately NOT
//! part of the parser — they belong to the renderer.

mod drawing;
mod fields;
mod parser;
mod tags;
mod types;
mod value;

pub(crate) use drawing::{parse_drawing, DrawCmd, Drawing, Point6};
pub(crate) use parser::{apply_force_style, parse, ScriptParser};
pub(crate) use tags::{parse_tag_block, FontSizeArg, KaraokeKind, Tag};
pub(crate) use types::{Event, Script};
pub(crate) use value::{numpad2align, Color};
pub(crate) use value::{VALIGN_CENTER, VALIGN_SUB, VALIGN_TOP};
