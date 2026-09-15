//! A headless video frame source: the context-side half of
//! [`VideoWriter`](crate::VideoWriter) and of frame-push inputs. It owns the
//! ingress receiver that the push facade feeds and the filtergraph sender a
//! decoder would normally hold; `start()` hands it to a counted frame-source
//! worker (`scheduler::frame_source_task`) that turns pushed byte buffers
//! into pool-backed `AVFrame`s and forwards them to the graph's buffersrc pad.

use crate::core::context::FrameBox;
use crossbeam_channel::{Receiver, Sender};
use ffmpeg_sys_next::{AVPixelFormat, AVRational};

/// One pushed frame: tightly packed pixel bytes plus the timestamps the
/// worker stamps verbatim onto the built `AVFrame`. The producing facade owns
/// the timing policy — [`VideoWriter`](crate::VideoWriter) generates the CFR
/// grid (`pts = ordinal`, `duration = 1`), a VFR push handle forwards the
/// caller's timestamps — so the worker itself has a single, branch-free
/// stamping path.
pub(crate) struct PushedFrame {
    pub(crate) data: Vec<u8>,
    /// Presentation timestamp in the source's `time_base` ticks.
    pub(crate) pts: i64,
    /// Frame duration in the same ticks; `0` means unknown (FFmpeg
    /// convention: downstream derives it from timestamp deltas).
    pub(crate) duration: i64,
}

/// Fixed per-stream parameters of a pushed video source, resolved and
/// validated by the owning builder before the context is constructed.
#[derive(Clone, Copy)]
pub(crate) struct FrameSourceParams {
    pub(crate) width: i32,
    pub(crate) height: i32,
    pub(crate) pix_fmt: AVPixelFormat,
    /// Time base stamped on every built frame (and on the zero-frame
    /// fallback): `1/fps` for a CFR writer, the caller's tick unit for a
    /// VFR push input.
    pub(crate) time_base: AVRational,
    /// Constant frame rate advertised to the filtergraph
    /// (`AVBufferSrcParameters.frame_rate`) and to downstream `FrameData`.
    /// `None` for VFR sources: the graph must not report a rate the pushed
    /// timestamps do not follow — `choose_out_timebase` would otherwise
    /// quantize the encoder time base to it.
    pub(crate) framerate: Option<AVRational>,
}

/// One frame-push input of an [`FfmpegContext`](super::ffmpeg_context::FfmpegContext),
/// parallel to a `Demuxer` but with no `AVFormatContext` behind it. Consumed by
/// `FfmpegScheduler::start()`, which spawns the worker LAST so the entire
/// consumer chain (filter -> encoder -> mux) already exists.
pub(crate) struct FrameSource {
    /// Timed frames from the facade; the facade holds the sole sender, and
    /// dropping it is the healthy end-of-stream signal.
    pub(crate) ingress: Receiver<PushedFrame>,
    /// Cloned producer end of the filtergraph's bounded frame channel — the
    /// same channel a decoder would push into (`FilterGraph::get_src_sender`).
    pub(crate) fg_sender: Sender<FrameBox>,
    /// Input-pad index of the consuming graph, stamped on every outgoing
    /// `FrameData.fg_input_index` (frames and the EOF marker alike). The
    /// graph's frame channel is shared by all of its pads and the filter
    /// task routes each `FrameBox` by that field — a wrong pad here feeds
    /// another input's buffersrc and starves this one. Always 0 for the
    /// writer's probe-validated single-input graph; a frame-push input
    /// carries whichever pad claimed it.
    pub(crate) fg_input_index: usize,
    pub(crate) params: FrameSourceParams,
}
