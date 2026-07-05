//! `crate::raw` — safe RAII newtypes that concentrate raw FFmpeg FFI pointers
//! behind a small, audited boundary.
//!
//! Rung-2 of the unsafe-hardening effort introduces this module so the raw
//! `ffmpeg-sys-next` pointer ownership lives in one place instead of being spread
//! across the crate. The pilot member is [`FormatContext`], which owns
//! `*mut AVFormatContext` behind a single typed discriminant (replacing the
//! `AVFormatContextBox` boolean fan-out). [`FilterGraph`] and [`FilterInOut`]
//! follow the same playbook for `*mut AVFilterGraph` and the parser's
//! `*mut AVFilterInOut` lists, whose manual `avfilter_graph_free` /
//! `avfilter_inout_free` calls were hand-balanced across early returns (and
//! missed on some `?` paths).

// `filter_graph` and `filter_inout` are entirely FFI and their only non-test
// consumers (`init_filter_graph` et al.) are `#[cfg(not(docsrs))]`, so gate the
// owners out of the docsrs stub build rather than leave them dead there (mirrors
// how the crate gates its other FFI-only code). `format_context` stays compiled:
// it is still partially used under docsrs.
#[cfg(not(docsrs))]
pub(crate) mod filter_graph;
#[cfg(not(docsrs))]
pub(crate) mod filter_inout;
pub(crate) mod format_context;

#[cfg(not(docsrs))]
pub(crate) use filter_graph::FilterGraph;
#[cfg(not(docsrs))]
pub(crate) use filter_inout::FilterInOut;
pub(crate) use format_context::{FormatContext, Mode};
