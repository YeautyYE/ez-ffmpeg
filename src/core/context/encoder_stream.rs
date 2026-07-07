use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex};
use crate::core::context::output::VSyncMethod;
use crate::core::context::pre_mux_queue::PreMuxQueueSender;
use crate::core::context::{FrameBox, PacketBox, Stream};
use crate::core::scheduler::sync_queue::SyncQueue;
use crossbeam_channel::{Receiver, Sender};
use ffmpeg_sys_next::{AVCodec, AVMediaType, AVStream};

/// `-shortest` frame-level sync-queue handle for one encoded-A/V stream.
///
/// The `queue` + `sq_finished` `Arc`s are **shared** across a mux's encoder
/// threads (all encoded-A/V members point at the same `sq_enc`); `sq_idx` is this
/// stream's own slot. The paired `Condvar` wakes drain-phase encoders when any
/// peer advances the queue head (see `enc_task` Architecture B).
#[derive(Clone)]
pub(crate) struct EncSyncHandle {
    pub(crate) queue: Arc<(Mutex<SyncQueue<FrameBox>>, Condvar)>,
    /// This stream's slot in `sq_enc` (from `SyncQueue::add_stream`).
    pub(crate) sq_idx: usize,
    /// One flag per `sq_enc` stream; set when the engine cascade-finishes that
    /// stream, so a truncated encoder observes it and enters its drain phase.
    pub(crate) sq_finished: Arc<[AtomicBool]>,
}

/// fftools: `OutputStream` + `MuxStream` (ffmpeg.h / ffmpeg_mux.h).
#[derive(Clone)]
pub(crate) struct EncoderStream {
    pub(crate) stream_index: usize,
    pub(crate) stream: Stream,
    pub(crate) codec_type: AVMediaType,
    pub(crate) encoder: *const AVCodec,
    pub(crate) vsync_method: Option<VSyncMethod>,
    pub(crate) qscale: Option<i32>,
    /// Sorted forced-keyframe times in microseconds (`AV_TIME_BASE_Q`); empty = off.
    /// Video only — populated by the muxer's `add_enc_stream` gate.
    pub(crate) forced_kf_pts: Vec<i64>,
    src: Option<Receiver<FrameBox>>,
    dst: Option<Sender<PacketBox>>,
    dst_pre: Option<PreMuxQueueSender>,
    mux_start_gate: Option<Arc<crate::core::context::MuxStartGate>>,
    /// `-shortest` sync-queue handle; `None` unless the mux built `sq_enc` and this
    /// stream is an encoded-A/V member. Set by the scheduler before `enc_init`.
    sync_queue: Option<EncSyncHandle>,
}

impl EncoderStream {
    pub(crate) fn new(
        stream_index: usize,
        stream: *mut AVStream,
        codec_type: AVMediaType,
        encoder: *const AVCodec,
        vsync_method: Option<VSyncMethod>,
        qscale: Option<i32>,
        forced_kf_pts: Vec<i64>,
        src: Receiver<FrameBox>,
        dst: Sender<PacketBox>,
        dst_pre: PreMuxQueueSender,
        mux_start_gate: Arc<crate::core::context::MuxStartGate>,
    ) -> Self {
        Self {
            stream_index,
            stream: Stream { inner: stream },
            codec_type,
            encoder,
            vsync_method,
            qscale,
            forced_kf_pts,
            src: Some(src),
            dst: Some(dst),
            dst_pre: Some(dst_pre),
            mux_start_gate: Some(mux_start_gate),
            sync_queue: None,
        }
    }

    /// Attach this stream's `-shortest` sync-queue handle (scheduler, before `enc_init`).
    pub(crate) fn set_sync_queue(&mut self, handle: EncSyncHandle) {
        self.sync_queue = Some(handle);
    }

    /// Take the `-shortest` handle for the encoder thread (`None` when not participating).
    pub(crate) fn take_sync_queue(&mut self) -> Option<EncSyncHandle> {
        self.sync_queue.take()
    }

    pub(crate) fn take_src(&mut self) -> Receiver<FrameBox> {
        self.src.take().unwrap()
    }

    pub(crate) fn take_dst(&mut self) -> Sender<PacketBox> {
        self.dst.take().unwrap()
    }

    pub(crate) fn take_dst_pre(&mut self) -> PreMuxQueueSender {
        self.dst_pre.take().unwrap()
    }

    pub(crate) fn take_mux_start_gate(&mut self) -> Arc<crate::core::context::MuxStartGate> {
        self.mux_start_gate.take().unwrap()
    }

    pub fn replace_src(&mut self, new_src: Receiver<FrameBox>) -> Receiver<FrameBox> {
        let old_src = self.src.take().unwrap();
        self.src = Some(new_src);
        old_src
    }
}
