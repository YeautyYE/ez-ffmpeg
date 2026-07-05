//! `crate::raw` — safe RAII newtypes that concentrate raw FFmpeg FFI pointers
//! behind a small, audited boundary.
//!
//! Rung-2 of the unsafe-hardening effort introduces this module so the raw
//! `ffmpeg-sys-next` pointer ownership lives in one place instead of being spread
//! across the crate. The pilot member is [`FormatContext`], which owns
//! `*mut AVFormatContext` behind a single typed discriminant (replacing the
//! `AVFormatContextBox` boolean fan-out). Later increments fold the output and
//! custom-IO owners here too.

pub(crate) mod format_context;

pub(crate) use format_context::{FormatContext, Mode};
