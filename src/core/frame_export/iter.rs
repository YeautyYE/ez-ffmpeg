//! [`FrameIter`]: a fused iterator over exported frames with load-bearing
//! teardown ordering (S6).

use super::error::FrameExportError;
use super::frame::VideoFrame;
use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Running};
use crate::core::scheduler::owned_run_iter::OwnedRunIter;
use crate::error::Error;
use crossbeam_channel::Receiver;

/// Re-surfaces a typed [`FrameExportError`] that crossed the frame-filter
/// boundary. The module's own filters (color guard, UniformN sampler) return
/// their typed errors boxed, which the pipeline wraps as
/// [`Error::FrameFilterProcess`]; unwrapping here keeps the public contract —
/// runtime failures match `Error::FrameExport(..)` exactly like open-time
/// ones. Anything else passes through unchanged.
fn map_terminal_error(e: Error) -> Error {
    match e {
        Error::FrameFilterProcess(boxed) => match boxed.downcast::<FrameExportError>() {
            Ok(typed) => Error::FrameExport(*typed),
            Err(other) => Error::FrameFilterProcess(other),
        },
        other => other,
    }
}

/// An iterator over exported [`VideoFrame`]s.
///
/// Yields `Ok(frame)` per exported frame. When the pipeline finishes (or dies),
/// the iterator joins the scheduler once: a worker error surfaces as a single
/// terminal `Err`, after which the iterator is fused and yields `None` forever.
///
/// Dropping the iterator early tears the run down cleanly. Teardown drops the
/// receiver *before* aborting the scheduler (S6): the sink can be parked in a
/// blocking `send()`, and only dropping the receiver unblocks it so the abort
/// can join the workers. `Drop` may block until in-flight FFmpeg calls return
/// (no fixed bound on stalled network IO).
pub struct FrameIter {
    inner: OwnedRunIter<VideoFrame>,
}

impl FrameIter {
    pub(crate) fn new(rx: Receiver<VideoFrame>, scheduler: FfmpegScheduler<Running>) -> Self {
        Self {
            inner: OwnedRunIter::new(rx, scheduler, map_terminal_error),
        }
    }
}

impl Iterator for FrameIter {
    type Item = Result<VideoFrame, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl std::iter::FusedIterator for FrameIter {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_export_error_is_unwrapped_from_filter_process() {
        let boxed: Box<dyn std::error::Error + Send + Sync> =
            Box::new(FrameExportError::HdrRequiresToneMapping);
        let mapped = map_terminal_error(Error::FrameFilterProcess(boxed));
        assert!(matches!(
            mapped,
            Error::FrameExport(FrameExportError::HdrRequiresToneMapping)
        ));
    }

    #[test]
    fn foreign_filter_process_error_passes_through() {
        let boxed: Box<dyn std::error::Error + Send + Sync> = "some other failure".into();
        let mapped = map_terminal_error(Error::FrameFilterProcess(boxed));
        assert!(matches!(mapped, Error::FrameFilterProcess(_)));
    }

    #[test]
    fn non_filter_errors_pass_through() {
        let mapped = map_terminal_error(Error::EOF);
        assert!(matches!(mapped, Error::EOF));
    }
}
