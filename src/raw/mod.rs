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

// `filter_inout` is entirely FFI and its only non-test consumers are
// `#[cfg(not(docsrs))]`, so gate it out of the docsrs stub build. `filter_graph`
// must instead stay compiled: the filter worker's slot and `cleanup_filtergraph`
// (which have no docsrs stub) name `Option<FilterGraph>` even under docsrs, so
// the type must exist there — its FFI methods are then dead-code-suppressed
// under docsrs (see the module attribute in filter_graph.rs). `format_context`
// likewise stays compiled (still partially used under docsrs).
pub(crate) mod filter_graph;
#[cfg(not(docsrs))]
pub(crate) mod filter_inout;
pub(crate) mod format_context;

pub(crate) use filter_graph::FilterGraph;
#[cfg(not(docsrs))]
pub(crate) use filter_inout::FilterInOut;
pub(crate) use format_context::{FormatContext, Mode};
