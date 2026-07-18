//! A headless video frame source: the context-side half of
//! [`VideoWriter`](crate::VideoWriter). It owns the ingress receiver that the
//! writer facade feeds and the filtergraph sender a decoder would normally
//! hold; `start()` hands it to a counted frame-source worker
//! (`scheduler::frame_source_task`) that turns pushed byte buffers into
//! pool-backed `AVFrame`s and forwards them to the graph's buffersrc pad.

use crate::core::context::FrameBox;
use crossbeam_channel::{Receiver, Sender};
use ffmpeg_sys_next::AVPixelFormat;

/// Fixed per-stream parameters of a pushed CFR video source, resolved and
/// validated by the writer builder before the context is constructed.
#[derive(Clone, Copy)]
pub(crate) struct FrameSourceParams {
    pub(crate) width: i32,
    pub(crate) height: i32,
    pub(crate) pix_fmt: AVPixelFormat,
    pub(crate) fps_num: i32,
    pub(crate) fps_den: i32,
}

/// One frame-push input of an [`FfmpegContext`](super::ffmpeg_context::FfmpegContext),
/// parallel to a `Demuxer` but with no `AVFormatContext` behind it. Consumed by
/// `FfmpegScheduler::start()`, which spawns the worker LAST so the entire
/// consumer chain (filter -> encoder -> mux) already exists.
pub(crate) struct FrameSource {
    /// Tightly packed frames from the facade; the facade holds the sole
    /// sender, and dropping it is the healthy end-of-stream signal.
    pub(crate) ingress: Receiver<Vec<u8>>,
    /// Cloned producer end of the filtergraph's bounded frame channel — the
    /// same channel a decoder would push into (`FilterGraph::get_src_sender`).
    pub(crate) fg_sender: Sender<FrameBox>,
    pub(crate) params: FrameSourceParams,
}
