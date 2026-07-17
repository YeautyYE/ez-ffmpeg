//! [`SampleIter`]: a fused iterator over exported audio chunks with the same
//! load-bearing teardown ordering as [`FrameIter`](super::FrameIter) (S6).

use super::chunk::AudioChunk;
use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Running};
use crate::error::Error;
use crossbeam_channel::Receiver;

/// An iterator over exported [`AudioChunk`]s.
///
/// Yields `Ok(chunk)` per exported chunk. When the pipeline finishes (or dies),
/// the iterator joins the scheduler once: a worker error surfaces as a single
/// terminal `Err`, after which the iterator is fused and yields `None` forever.
///
/// Dropping the iterator early tears the run down cleanly. Teardown drops the
/// receiver *before* aborting the scheduler (S6): the sink can be parked in a
/// blocking `send()`, and only dropping the receiver unblocks it so the abort
/// can join the workers. `Drop` may block until in-flight FFmpeg calls return
/// (no fixed bound on stalled network IO).
pub struct SampleIter {
    rx: Option<Receiver<AudioChunk>>,
    scheduler: Option<FfmpegScheduler<Running>>,
    terminated: bool,
}

impl SampleIter {
    pub(crate) fn new(rx: Receiver<AudioChunk>, scheduler: FfmpegScheduler<Running>) -> Self {
        Self {
            rx: Some(rx),
            scheduler: Some(scheduler),
            terminated: false,
        }
    }

    /// Joins the scheduler exactly once and maps its result to at most one
    /// terminal error. Called when the channel disconnects.
    fn finish(&mut self) -> Option<Result<AudioChunk, Error>> {
        self.terminated = true;
        // Drop the receiver before joining so a parked sender is released.
        self.rx = None;
        match self.scheduler.take() {
            Some(scheduler) => match scheduler.wait() {
                Ok(()) => None,
                Err(e) => Some(Err(e)),
            },
            None => None,
        }
    }
}

impl Iterator for SampleIter {
    type Item = Result<AudioChunk, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminated {
            return None;
        }
        let recv = match self.rx.as_ref() {
            Some(rx) => rx.recv(),
            None => return self.finish(),
        };
        match recv {
            Ok(chunk) => Some(Ok(chunk)),
            // Channel closed: the pipeline finished or died. Join once.
            Err(_) => self.finish(),
        }
    }
}

impl std::iter::FusedIterator for SampleIter {}

impl Drop for SampleIter {
    fn drop(&mut self) {
        // S6: drop the receiver FIRST (unblocks a sink parked in send()), THEN
        // abort the scheduler (which joins the workers). Reversed order can
        // deadlock — see FrameIter::drop for the full rationale.
        self.rx = None;
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.abort();
        }
    }
}
