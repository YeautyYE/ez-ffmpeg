//! `crate::raw` — safe RAII newtypes that concentrate raw FFmpeg FFI pointers
//! behind a small, audited boundary.
//!
//! Rung-2 of the unsafe-hardening effort introduces this module so the raw
//! `ffmpeg-sys-next` pointer ownership lives in one place instead of being spread
//! across the crate. The pilot member is [`FormatContext`], which owns
//! `*mut AVFormatContext` behind a single typed discriminant (replacing the
//! `AVFormatContextBox` boolean fan-out). [`FilterGraph`] follows the same
//! playbook for `*mut AVFilterGraph`, whose manual `avfilter_graph_free` calls
//! were hand-balanced across early returns (and missed on some `?` paths).

// `filter_graph` is entirely FFI and its sole non-test consumer
// (`init_filter_graph`) is `#[cfg(not(docsrs))]`, so gate the whole owner out of
// the docsrs stub build rather than leave it dead there (mirrors how the crate
// gates its other FFI-only code). `format_context` stays compiled: it is still
// partially used under docsrs.
#[cfg(not(docsrs))]
pub(crate) mod filter_graph;
pub(crate) mod format_context;

#[cfg(not(docsrs))]
pub(crate) use filter_graph::FilterGraph;
pub(crate) use format_context::{FormatContext, Mode};
