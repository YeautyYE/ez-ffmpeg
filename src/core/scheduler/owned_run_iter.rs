//! Crate-internal owned-run iterator: pairs a bounded event channel with the
//! [`FfmpegScheduler<Running>`] that feeds it. The public wrappers
//! (frame export's `FrameIter`, the packet sink's `PacketEventIter`) both
//! delegate to this one implementation so the join-exactly-once logic and
//! the load-bearing teardown order live in a single place.

use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Running};
use crate::error::Error;
use crossbeam_channel::Receiver;

/// Streams items out of `rx` while owning the scheduler of the run that
/// produces them. When the channel disconnects (the pipeline finished or
/// died), the scheduler is joined exactly once and a job error surfaces as
/// one terminal `Err`; afterwards the iterator is fused.
pub(crate) struct OwnedRunIter<T> {
    rx: Option<Receiver<T>>,
    scheduler: Option<FfmpegScheduler<Running>>,
    terminated: bool,
    /// Maps the scheduler's terminal error before it surfaces as the single
    /// terminal `Err` (identity for wrappers with no domain-specific
    /// mapping).
    map_terminal_error: fn(Error) -> Error,
}

impl<T> OwnedRunIter<T> {
    pub(crate) fn new(
        rx: Receiver<T>,
        scheduler: FfmpegScheduler<Running>,
        map_terminal_error: fn(Error) -> Error,
    ) -> Self {
        Self {
            rx: Some(rx),
            scheduler: Some(scheduler),
            terminated: false,
            map_terminal_error,
        }
    }

    /// Joins the scheduler exactly once and maps its result to at most one
    /// terminal error. Called when the channel disconnects.
    fn finish(&mut self) -> Option<Result<T, Error>> {
        self.terminated = true;
        // Drop the receiver before joining so a parked sender is released.
        self.rx = None;
        match self.scheduler.take() {
            Some(scheduler) => match scheduler.wait() {
                Ok(()) => None,
                Err(e) => Some(Err((self.map_terminal_error)(e))),
            },
            None => None,
        }
    }
}

impl<T> Iterator for OwnedRunIter<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminated {
            return None;
        }
        let recv = match self.rx.as_ref() {
            Some(rx) => rx.recv(),
            None => return self.finish(),
        };
        match recv {
            Ok(item) => Some(Ok(item)),
            // Channel closed: the pipeline finished or died. Join once.
            Err(_) => self.finish(),
        }
    }
}

impl<T> std::iter::FusedIterator for OwnedRunIter<T> {}

impl<T> Drop for OwnedRunIter<T> {
    fn drop(&mut self) {
        // S6: drop the receiver FIRST (unblocks a sender parked in the
        // bounded channel send), THEN abort the scheduler (which joins the
        // workers). Reversed order can deadlock: the sending worker holds
        // the pipeline inside a callback and only observes the abort between
        // callbacks, but it is blocked inside send().
        self.rx = None;
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.abort();
        }
    }
}
