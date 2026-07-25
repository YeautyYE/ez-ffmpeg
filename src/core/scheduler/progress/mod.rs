//! Typed, pull-based progress reporting for running FFmpeg jobs.
//!
//! [`FfmpegScheduler::progress_handle`] (available in the `Running` and
//! `Paused` states) returns a [`ProgressHandle`]: a cheap, cloneable,
//! thread-safe, read-only view of the job. Call [`ProgressHandle::snapshot`]
//! at any cadence you like to obtain a [`Progress`] value — the typed
//! counterpart of the fields the FFmpeg CLI prints on its `-progress` line
//! (`frame=`, `fps=`, `out_time=`, `total_size=`, `bitrate=`, `speed=`),
//! reported **per output** so multi-output jobs (an HLS ladder, a
//! file + preview pair) do not collapse into one misleading scalar.
//!
//! Design invariants:
//!
//! - **Pull, not push.** No callbacks, no extra threads: workers update a few
//!   shared atomics on their existing hot paths, and `snapshot()` merely reads
//!   them. The EXPENSIVE size query is kept off the per-packet path: an
//!   observed job takes only a cheap logical-write-offset sample
//!   (`avio_seek` `SEEK_CUR`) per committed packet — never `avio_size`, so a
//!   custom-IO output's seek callback is never invoked per packet — while an
//!   unobserved job skips even that. The precise `avio_size` query is
//!   confined to the once-per-job header and trailer samples.
//! - **Post-fixup truth.** [`OutputProgress::out_time_us`] is fed from the
//!   muxer write path *after* timestamp fixup and *after* the packet was
//!   successfully committed to the muxer — it reports what actually landed in
//!   the output, unlike scheduler-internal balancing watermarks. Stream-copy
//!   (remux) jobs are therefore fully visible, exactly like encoded ones.
//! - **Lifetime safe.** The handle holds only plain `std` synchronization
//!   state behind `Arc`s — no FFmpeg pointers. Once the scheduler is
//!   consumed or dropped, snapshots keep returning the final frozen values.
//!
//! [`FfmpegScheduler::progress_handle`]: crate::FfmpegScheduler::progress_handle

mod handle;
mod snapshot;
mod state;
mod tracker;

#[cfg(test)]
mod tests;

pub use handle::ProgressHandle;
pub use snapshot::{OutputProgress, Progress};
pub use state::ProgressState;

pub(crate) use tracker::{OutputTelemetry, ProgressTracker};
