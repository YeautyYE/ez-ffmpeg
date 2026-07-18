//! Detection / measurement result surfacing.
//!
//! FFmpeg's detector filters (`blackdetect`, `silencedetect`, `scdet`,
//! `cropdetect`, `ebur128`) produce their value as **data** — attached to
//! frames as `lavfi.*` metadata. This module reads that metadata off the
//! frame pipeline and surfaces it as typed
//! [`MetadataEvent`](crate::core::analysis::event::MetadataEvent)s (via a
//! [`MetadataEventFilter`](crate::core::analysis::filter::MetadataEventFilter)
//! streamed to a user [`EventSink`](crate::core::analysis::filter::EventSink))
//! or as a folded [`AnalysisReport`](crate::core::analysis::report::AnalysisReport)
//! from the one-shot [`Analysis`](crate::core::analysis::runner::Analysis) runner.
//!
//! Only per-frame metadata is covered; end-of-run log summaries such as
//! `loudnorm print_format=json` are out of scope (use `ebur128` metadata for
//! loudness instead).

pub mod detector;
pub mod event;
pub mod filter;
pub mod report;
pub mod runner;

pub use detector::*;
pub use event::*;
pub use filter::*;
pub use report::*;
pub use runner::*;
