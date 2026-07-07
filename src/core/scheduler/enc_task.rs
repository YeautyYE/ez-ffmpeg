use crate::core::context::encoder_stream::EncoderStream;
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::pre_mux_queue::{packet_payload_size, PreMuxQueueSender};
use crate::core::context::{CodecContext, FrameBox, PacketBox, PacketData};
use crate::core::scheduler::sync_queue::SyncQueue;
use crate::error::Error::{Encoding, OpenEncoder};
use crate::error::{AllocPacketError, EncodeSubtitleError, EncodingError, EncodingOperationError, OpenEncoderError, OpenEncoderOperationError, OpenOutputError};
use crate::core::scheduler::ffmpeg_scheduler::{
    frame_is_null, is_stopping, packet_is_null, set_scheduler_error, wait_until_not_paused, STATUS_ABORT,
};
use crate::core::scheduler::input_controller::InputController;
use crate::hwaccel::hw_device_get_by_type;
use crate::util::ffmpeg_utils::{av_err2str, hashmap_to_avdictionary, DictGuard};
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender};
use ffmpeg_next::packet::Mut;
use ffmpeg_next::{Frame, Packet};
use ffmpeg_sys_next::AVCodecID::{
    AV_CODEC_ID_ASS, AV_CODEC_ID_CODEC2, AV_CODEC_ID_DVB_SUBTITLE, AV_CODEC_ID_MJPEG,
};
use ffmpeg_sys_next::AVFieldOrder::{
    AV_FIELD_BB, AV_FIELD_BT, AV_FIELD_PROGRESSIVE, AV_FIELD_TB, AV_FIELD_TT,
};
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVPictureType::{AV_PICTURE_TYPE_I, AV_PICTURE_TYPE_NONE};
use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE;
use ffmpeg_sys_next::AVSampleFormat::AV_SAMPLE_FMT_NONE;
#[cfg(all(not(docsrs), not(ffmpeg_8_0)))]
use ffmpeg_sys_next::AVSideDataProps::AV_SIDE_DATA_PROP_GLOBAL;
#[cfg(all(not(docsrs), not(ffmpeg_8_0)))]
use ffmpeg_sys_next::av_frame_side_data_desc;
#[cfg(not(docsrs))]
use ffmpeg_sys_next::{av_channel_layout_copy, av_frame_side_data_clone, AV_CODEC_FLAG_COPY_OPAQUE, AV_CODEC_FLAG_FRAME_DURATION, AV_FRAME_FLAG_INTERLACED, AV_FRAME_FLAG_TOP_FIELD_FIRST, AV_FRAME_SIDE_DATA_FLAG_UNIQUE};
use ffmpeg_sys_next::{av_add_q, av_buffer_ref, av_compare_ts, av_cpu_max_align, av_frame_copy_props, av_frame_get_buffer, av_frame_ref, av_get_bytes_per_sample, av_get_pix_fmt_name, av_opt_set_dict2, av_rescale_q, av_sample_fmt_is_planar, av_samples_copy, av_shrink_packet, avcodec_alloc_context3, avcodec_encode_subtitle, avcodec_get_hw_config, avcodec_open2, avcodec_parameters_from_context, avcodec_receive_packet, avcodec_send_frame, AVBufferRef, AVCodecContext, AVFrame, AVHWFramesContext, AVMediaType, AVRational, AVStream, AVSubtitle, AVERROR, AVERROR_EOF, AVERROR_EXPERIMENTAL, AV_CODEC_CAP_ENCODER_REORDERED_OPAQUE, AV_CODEC_CAP_PARAM_CHANGE, AV_CODEC_FLAG_INTERLACED_DCT, AV_CODEC_FLAG_INTERLACED_ME, AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX, AV_CODEC_HW_CONFIG_METHOD_HW_FRAMES_CTX, AV_NOPTS_VALUE, AV_OPT_SEARCH_CHILDREN, AV_PKT_FLAG_TRUSTED, AV_TIME_BASE_Q, EAGAIN};
use ffmpeg_sys_next::av_mallocz;
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[cfg(docsrs)]
pub(crate) fn enc_init(
    mux_idx: usize,
    mut enc_stream: EncoderStream,
    ready_sender: Option<Sender<i32>>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    bits_per_raw_sample: Option<i32>,
    max_video_frames: Option<i64>,
    max_audio_frames: Option<i64>,
    max_subtitle_frames: Option<i64>,
    video_codec_opts: &Option<HashMap<CString, CString>>,
    audio_codec_opts: &Option<HashMap<CString, CString>>,
    subtitle_codec_opts: &Option<HashMap<CString, CString>>,
    oformat_flags: i32,
    frame_pool: ObjPool<Frame>,
    packet_pool: ObjPool<Packet>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    enc_handle_sender: Sender<std::thread::JoinHandle<()>>,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    _input_controller: Arc<InputController>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
pub(crate) fn enc_init(
    mux_idx: usize,
    mut enc_stream: EncoderStream,
    ready_sender: Option<Sender<i32>>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    bits_per_raw_sample: Option<i32>,
    max_video_frames: Option<i64>,
    max_audio_frames: Option<i64>,
    max_subtitle_frames: Option<i64>,
    video_codec_opts: &Option<HashMap<CString, CString>>,
    audio_codec_opts: &Option<HashMap<CString, CString>>,
    subtitle_codec_opts: &Option<HashMap<CString, CString>>,
    oformat_flags: i32,
    frame_pool: ObjPool<Frame>,
    packet_pool: ObjPool<Packet>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    enc_handle_sender: Sender<std::thread::JoinHandle<()>>,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    input_controller: Arc<InputController>,
) -> crate::error::Result<()> {
    let enc_ctx = unsafe { avcodec_alloc_context3(enc_stream.encoder) };
    if enc_ctx.is_null() {
        return Err(OpenEncoder(
            OpenEncoderOperationError::ContextAllocationError(OpenEncoderError::OutOfMemory),
        ));
    }

    if let Some(qscale) = enc_stream.qscale {
        if qscale > 0 {
            unsafe {
                (*enc_ctx).flags |= ffmpeg_sys_next::AV_CODEC_FLAG_QSCALE as i32;
                (*enc_ctx).global_quality = ffmpeg_sys_next::FF_QP2LAMBDA * qscale;
            }
        }
    }

    if oformat_flags & ffmpeg_sys_next::AVFMT_GLOBALHEADER != 0 {
       unsafe { (*enc_ctx).flags |= ffmpeg_sys_next::AV_CODEC_FLAG_GLOBAL_HEADER as i32; }
    }
    let enc_ctx_box = CodecContext::new(enc_ctx);

    let max_frames = get_max_frames(enc_stream.codec_type, max_video_frames, max_audio_frames, max_subtitle_frames);

    set_encoder_opts(&enc_stream, video_codec_opts, audio_codec_opts, subtitle_codec_opts, &enc_ctx_box)?;

    let receiver = enc_stream.take_src();
    let pkt_sender = enc_stream.take_dst();
    let pre_pkt_sender = enc_stream.take_dst_pre();
    let mux_start_gate = enc_stream.take_mux_start_gate();

    // Forced-keyframe times ride inside EncoderStream (video only, empty = off).
    // Move them out before the encoder thread starts, mirroring take_src() above.
    let forced_kf_pts = std::mem::take(&mut enc_stream.forced_kf_pts);

    // -shortest frame-level sync-queue handle: `Some` only for an encoded-A/V
    // member of a mux built with `sq_enc`. Moved into the encoder thread; drives
    // the Architecture-B producer-then-drainer loop below. `None` keeps today's
    // verbatim encode path byte-for-byte.
    let enc_sync = enc_stream.take_sync_queue();

    let stream_box = enc_stream.stream;
    let stream_index = enc_stream.stream_index;

    let encoder_name = unsafe { CStr::from_ptr((*enc_stream.encoder).name).to_str().unwrap_or("unknown") };

    // Slot claimed before spawn; the guard releases it on any exit path.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(thread_sync.clone(), scheduler_status.clone());

    let result = std::thread::Builder::new().name(format!("encoder{stream_index}:{mux_idx}:{encoder_name}")).spawn(move || unsafe {
        let _thread_done = thread_done_guard;
        let enc_ctx_box = enc_ctx_box;
        let stream_box = stream_box;

        let mut opened = false;
        let mut finished = false;
        let mut frames_sent = 0;
        let mut samples_sent = 0;

        // audio
        let mut frame_samples = 0;
        let mut align_mask = 0;
        let mut samples_queued = 0;
        let mut audio_frame_queue: VecDeque<FrameBox> = VecDeque::new();
        let mut is_finished = false;

        // Per-encoder forced-keyframe cursor (fftools KeyframeForceCtx, list subset).
        let mut forced_kf = ForcedKeyframes {
            pts: forced_kf_pts,
            index: 0,
        };

        // ===== Architecture B (sq_enc / -shortest): producer-then-drainer =====
        // Runs only for an encoded-A/V member of an sq_enc mux. The encoder both
        // produces (receive -> queue) and drains (pop its own now-releasable frames
        // and encode them). It never exits at producer-EOF while it still holds
        // frames a slower peer will later release: it enters a drain phase and waits
        // on the queue Condvar. The sync-queue lock is held ONLY for in-memory
        // VecDeque/timestamp ops, never across frame_encode (which may park on the
        // pre-mux queue), so it lies on no wait-for cycle. `enc_sync == None` skips
        // this entirely and runs the verbatim loop below, byte-for-byte.
        if let Some(h) = enc_sync.as_ref() {
            let (sq_lock, sq_cv) = (&h.queue.0, &h.queue.1);
            let sq_idx = h.sq_idx;
            let sq_finished: &[AtomicBool] = &h.sq_finished;
            // This stream's own balancer flag, published at producer-EOF below.
            let source_finished = h.source_finished.as_ref();
            const DRAIN_TICK: Duration = Duration::from_millis(100);
            let mut last_tb = AVRational { num: 1, den: 1 };
            let mut producer_eof = false;
            // stop feeding: recording_time limit, encode error, natural encoder EOF,
            // or external abort -> skip further draining, proceed to the shared flush.
            let mut stop = false;

            // ---- producer + live drain ----
            while !stop {
                // (0) cascade-finish: a shorter peer truncated this stream.
                if sq_finished[sq_idx].load(Ordering::Acquire) {
                    break; // -> drain phase (the peer already signalled the finish)
                }
                let sync_frame = receive_frame(&mut opened, &receiver, &frame_pool, enc_ctx_box.as_mut_ptr(), stream_box.inner,
                                                   &ready_sender, &bits_per_raw_sample, &mut frame_samples, &mut align_mask, &mut samples_queued, &mut audio_frame_queue,
                                                   &mut samples_sent, &mut frames_sent, &mut is_finished,
                                                   &scheduler_status, &scheduler_result);
                let frame_box_opt = match sync_frame {
                    SyncFrame::FrameBox(fb) => {
                        // A null frame is the upstream EOF marker (the filtergraph
                        // sends it explicitly; the audio path also re-emits it out
                        // of `process_audio_queue`), NOT a real frame — it must not
                        // enter the sync queue (`sq_frame_end_tb` would deref a null
                        // AVFrame). Release it and treat it exactly like `Break`:
                        // producer-EOF -> drain phase, then the encoder is flushed
                        // (send NULL) by the shared flush below since `finished`
                        // stays false.
                        if frame_is_null(&fb.frame) {
                            frame_pool.release(fb.frame);
                            producer_eof = true;
                            break;
                        }
                        Some(fb)
                    }
                    // timeout/dummy: no new frame, but a peer may have advanced the
                    // head -> still drain our own now-releasable frames.
                    SyncFrame::Continue => None,
                    SyncFrame::Break => { producer_eof = true; break; }
                };

                let mut local: Vec<FrameBox> = Vec::new();
                {
                    let mut q = sq_lock.lock().unwrap();
                    if let Some(fb) = frame_box_opt {
                        let (end_ts, tb, nb_samples) = sq_frame_end_tb(&fb);
                        last_tb = tb;
                        q.send(sq_idx, Some(fb), end_ts, tb, nb_samples);
                    }
                    q.drain_releasable_into(sq_idx, &mut local);
                    if local.is_empty() {
                        // nothing released -> the true EAGAIN point: heartbeat may
                        // fake-advance a stalled laggard, then retry (FFmpeg sq_receive).
                        q.heartbeat();
                        q.drain_releasable_into(sq_idx, &mut local);
                    }
                    sq_propagate_and_notify(&mut q, sq_finished, sq_cv);
                }
                for fb in local {
                    if sq_encode_drained(enc_ctx_box.as_mut_ptr(), fb, start_time_us, recording_time_us,
                                         &pkt_sender, &pre_pkt_sender, &mux_start_gate, stream_box.inner,
                                         &packet_pool, &mut forced_kf, &frame_pool,
                                         &scheduler_status, &scheduler_result, &mut finished) {
                        stop = true;
                    }
                }
            }

            // ---- drain phase: my producer is done — I hit producer-EOF or a
            // shorter peer cascade-cut me — but I may still hold frames a slower
            // peer will release. Do not exit until my FIFO is drained. ----
            if !stop {
                // Publish this stream's balancer flag and re-balance BEFORE
                // draining. A stream that has stopped producing MUST leave the
                // input balancer at once — whether it hit producer-EOF or was
                // cascade-cut by a shorter peer (`sq_finished`) — or its now-stale
                // `last_dts` keeps choking a peer this drain waits on, deadlocking
                // a job with 2+ encoded streams. FFmpeg reaches the same publish
                // for both cases: it forwards the sync-queue EOF up the pipeline
                // into `send_to_enc_sq(!frame)` (ffmpeg_sched.c:1916-1933, plus the
                // decoder/filter EOF forwards at :2423-2435 / :2694-2698). The mux
                // also publishes on its side, but only after this drain completes —
                // too late to break the choke.
                if let Some(sf) = source_finished {
                    sf.store(true, Ordering::Release);
                    input_controller.update_locked(&scheduler_status);
                }

                if producer_eof && !sq_finished[sq_idx].load(Ordering::Acquire) {
                    let mut local: Vec<FrameBox> = Vec::new();
                    {
                        let mut q = sq_lock.lock().unwrap();
                        q.send(sq_idx, None, None, last_tb, 0); // finish + cascade
                        q.drain_releasable_into(sq_idx, &mut local);
                        sq_propagate_and_notify(&mut q, sq_finished, sq_cv);
                    }
                    for fb in local {
                        if sq_encode_drained(enc_ctx_box.as_mut_ptr(), fb, start_time_us, recording_time_us,
                                             &pkt_sender, &pre_pkt_sender, &mux_start_gate, stream_box.inner,
                                             &packet_pool, &mut forced_kf, &frame_pool,
                                             &scheduler_status, &scheduler_result, &mut finished) {
                            stop = true;
                        }
                    }
                }
                while !stop {
                    let mut local: Vec<FrameBox> = Vec::new();
                    let done;
                    {
                        let mut q = sq_lock.lock().unwrap();
                        q.heartbeat();
                        q.drain_releasable_into(sq_idx, &mut local);
                        sq_propagate_and_notify(&mut q, sq_finished, sq_cv);
                        done = q.is_stream_drained(sq_idx)
                            || is_stopping(scheduler_status.load(Ordering::Acquire));
                        if !done && local.is_empty() {
                            // wait for a peer's head advance; wait_timeout atomically
                            // releases the lock while blocked (deadlock-proof Lemma 1).
                            let _ = sq_cv.wait_timeout(q, DRAIN_TICK).unwrap();
                            continue;
                        }
                    }
                    for fb in local {
                        if sq_encode_drained(enc_ctx_box.as_mut_ptr(), fb, start_time_us, recording_time_us,
                                             &pkt_sender, &pre_pkt_sender, &mux_start_gate, stream_box.inner,
                                             &packet_pool, &mut forced_kf, &frame_pool,
                                             &scheduler_status, &scheduler_result, &mut finished) {
                            stop = true;
                        }
                    }
                    if done { break; }
                }
            }
        }

        while enc_sync.is_none() {
            let sync_frame = receive_frame(&mut opened, &receiver, &frame_pool, enc_ctx_box.as_mut_ptr(), stream_box.inner,
                                               &ready_sender, &bits_per_raw_sample, &mut frame_samples, &mut align_mask, &mut samples_queued, &mut audio_frame_queue,
                                               &mut samples_sent, &mut frames_sent, &mut is_finished,
                                               &scheduler_status, &scheduler_result);

            let mut receive_frame_box = match sync_frame {
                SyncFrame::FrameBox(frame_box) => frame_box,
                SyncFrame::Continue => continue,
                SyncFrame::Break => break
            };

            let result = frame_encode(
                enc_ctx_box.as_mut_ptr(),
                receive_frame_box.frame.as_mut_ptr(),
                start_time_us,
                recording_time_us,
                &pkt_sender,
                &pre_pkt_sender,
                &mux_start_gate,
                stream_box.inner,
                &packet_pool,
                &mut forced_kf,
            );
            frame_pool.release(receive_frame_box.frame);
            let status = match result {
                Err(e) => {
                    if is_stopping(scheduler_status.load(Ordering::Acquire))
                        && matches!(e, Encoding(EncodingOperationError::MuxerFinished))
                    {
                        // stop()/abort(): the muxer legitimately exited first; a
                        // user-requested shutdown must not be recorded as a failure.
                        debug!("Encoder stopping: muxer already finished");
                    } else {
                        error!("Error encoding a frame: {}", e);
                        set_scheduler_error(&scheduler_status, &scheduler_result, e);
                    }
                    break;
                }
                Ok(status) => status,
            };

            match status {
                EncodeStatus::Eof => {
                    trace!("Encoder returned EOF, finishing");
                    finished = true;
                    break;
                }
                EncodeStatus::LimitReached => {
                    // recording_time hit: stop feeding the encoder, but frames
                    // already sent may still sit in its reorder buffer — leave
                    // `finished` false so the flush below drains them.
                    debug!("sq: {stream_index} recording_time reached");
                    break;
                }
                EncodeStatus::Continue => {}
            }

            if let Some(max_frames) = max_frames.as_ref() {
                if frames_sent >= *max_frames {
                    // Same as LimitReached: the last frames sent are still
                    // buffered inside the encoder; do not skip the flush.
                    debug!("sq: {stream_index} frames_max {max_frames} reached");
                    break;
                }
            }
        }

        // Encoder flush logic - handles three different exit scenarios:
        //
        // SCENARIO 1: finished=true (normal completion)
        //   - Encoder received NULL frame from upstream (frame_encode() processed it)
        //   - encode_frame() already called avcodec_send_frame(NULL)
        //   - All buffered packets (B-frames, etc.) have been drained via receive_packet() loop
        //   - Encoder is completely flushed, no additional flush needed
        //
        // SCENARIO 2: finished=false, limit reached (max_frames / recording_time)
        //   - Every accepted frame is within the limit, but the last ones may
        //     still sit in the encoder's reorder buffer (B-frame lookahead)
        //   - Flush, or the tail of the output is silently dropped; drained
        //     packets only cover frames sent before the limit, so the limit
        //     still holds (fftools flushes after its encode loop the same way)
        //
        // SCENARIO 3: finished=false (early exit: abort or disconnect)
        //   - Encoder did NOT receive NULL frame (thread exited early via abort() or Disconnected)
        //   - Encoder buffer may still contain unencoded frames (B-frames waiting for future frames)
        //   - If NOT abort: actively flush encoder to drain buffered frames
        //   - If abort: skip flush (user doesn't care about incomplete output)
        //
        // Subtitle encoders buffer nothing (avcodec_encode_subtitle is
        // stateless) and do not implement the send_frame API: flushing one
        // would only produce a spurious EINVAL — fftools' frame_encode notes
        // "no flushing for subtitles".
        if !finished && opened && (*enc_ctx_box.as_mut_ptr()).codec_type != AVMEDIA_TYPE_SUBTITLE {
            let status = scheduler_status.load(Ordering::Acquire);

            if status != STATUS_ABORT {
                // Actively flush encoder: send NULL frame and drain all buffered packets
                let enc_ctx = enc_ctx_box.as_mut_ptr();
                let stream = stream_box.inner;

                // Send NULL frame to signal EOF
                let ret = avcodec_send_frame(enc_ctx, null_mut());
                if ret < 0 && ret != AVERROR_EOF {
                    error!("Error flushing encoder: {}", av_err2str(ret));
                } else {
                    // Drain all buffered packets (B-frames, delayed frames, etc.)
                    // Note: These are real packets with data, NOT empty marker packets
                    loop {
                        let packet_result = packet_pool.get();
                        if packet_result.is_err() {
                            error!("Failed to allocate packet for flushing");
                            break;
                        }
                        let mut packet = packet_result.unwrap();
                        let pkt = packet.as_mut_ptr();

                        let ret = avcodec_receive_packet(enc_ctx, pkt);
                        if ret == AVERROR_EOF {
                            trace!("Encoder flushing completed");
                            packet_pool.release(packet);
                            break;
                        }
                        if ret == AVERROR(EAGAIN) {
                            packet_pool.release(packet);
                            break;
                        }
                        if ret < 0 {
                            error!("Error receiving flushed packet: {}", av_err2str(ret));
                            packet_pool.release(packet);
                            break;
                        }

                        (*pkt).time_base = (*enc_ctx).time_base;
                        (*pkt).flags |= AV_PKT_FLAG_TRUSTED;

                        // Send flushed packet to mux (real data packet, not empty marker)
                        if let Err(_) = send_to_mux(PacketBox {
                            packet,
                            packet_data: PacketData {
                                dts_est: 0,
                                codec_type: (*enc_ctx).codec_type,
                                output_stream_index: (*stream).index,
                                is_copy: false,
                            },
                        }, &pkt_sender, &pre_pkt_sender, &mux_start_gate) {
                            debug!("send flushed packet failed, mux already finished");
                            break;
                        }
                    }
                }
            } else {
                debug!("Encoder detected abort, skipping flush for stream {}", stream_index);
            }
        }

        // Send empty packet marker to signal stream end to muxer
        //
        // IMPORTANT: This empty packet is REQUIRED in ALL scenarios:
        // - SCENARIO 1 (finished=true): After normal encoding completion
        // - SCENARIO 2 (finished=false, no abort): After flushing buffered frames
        //
        // Why needed after flush? Because flushed packets contain REAL DATA (B-frames, etc.),
        // not empty markers. Muxer uses `packet.is_empty()` to detect stream completion,
        // so we must send an explicit empty packet to signal "no more data from this encoder".
        {
            let enc_ctx = enc_ctx_box.as_mut_ptr();
            let stream = stream_box.inner;

            let mut packet = Packet::empty();
            (*packet.as_mut_ptr()).stream_index = stream_index as i32;
            if let Err(e) = send_to_mux(PacketBox {
                packet,
                packet_data: PacketData {
                    dts_est: 0,
                    codec_type: (*enc_ctx).codec_type,
                    output_stream_index: (*stream).index,
                    is_copy: false,
                },
            }, &pkt_sender, &pre_pkt_sender, &mux_start_gate) {
                if is_stopping(scheduler_status.load(Ordering::Acquire)) {
                    // Normal shutdown ordering: muxer exits before the encoder's
                    // end-of-stream marker; not an error and not a task failure.
                    debug!("end packet not delivered, muxer already finished: {e}");
                } else {
                    error!("Error sending end packet: {}", e);
                    set_scheduler_error(
                        &scheduler_status,
                        &scheduler_result,
                        Encoding(EncodingOperationError::MuxerFinished),
                    );
                }
            }
        }

        debug!("Encoder finished.");
    });
    match result {
        Err(e) => {
            error!("Encoder thread exited with error: {e}");
            return Err(OpenEncoderOperationError::ThreadExited.into());
        }
        Ok(handle) => {
            // Hand the join handle to this stream's muxer: the muxer joins
            // its encoders before freeing the output AVFormatContext, so an
            // encoder can never outlive the streams it references.
            let _ = enc_handle_sender.send(handle);
        }
    }

    Ok(())
}

/// Outcome of pushing one frame through the encoder.
enum EncodeStatus {
    /// Frame consumed; more may follow.
    Continue,
    /// recording_time boundary hit before this frame: stop encoding. Frames
    /// already sent may still sit in the encoder and must be flushed.
    LimitReached,
    /// EOF fully processed: the encoder has been drained, nothing to flush.
    Eof,
}

enum SyncFrame {
    FrameBox(FrameBox),
    Continue,
    Break,
}

fn receive_from(
    receiver: &Receiver<FrameBox>,
    scheduler_status: &Arc<AtomicUsize>,
) -> Result<FrameBox, SyncFrame> {
    match receiver.recv_timeout(Duration::from_millis(100)) {
        Ok(frame_box) => {
            if is_stopping(wait_until_not_paused(scheduler_status)) {
                info!("Encoder receiver end command, finishing.");
                return Err(SyncFrame::Break);
            }
            Ok(frame_box)
        },
        Err(e) if e == RecvTimeoutError::Disconnected => {
            debug!("Source[decoder/filtergraph/pipeline] thread exit.");
            Err(SyncFrame::Break)
        }
        Err(_) => Err(SyncFrame::Continue),
    }
}

fn process_audio_queue(
    frame_samples: i32,
    samples_queued: &mut i32,
    audio_frame_queue: &mut VecDeque<FrameBox>,
    frame_pool: &ObjPool<Frame>,
    align_mask: usize,
    samples_sent: &mut i64,
    frames_sent: &mut i64,
    is_finished: &mut bool,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>
) -> Result<Option<FrameBox>, ()> {
    if let Some(peek) = audio_frame_queue.front() {
        if frame_samples <= *samples_queued || *is_finished {
            let nb_samples = if *is_finished {
                std::cmp::min(frame_samples, *samples_queued)
            } else {
                frame_samples
            };
            *samples_sent += nb_samples as i64;
            *frames_sent = *samples_sent / frame_samples as i64;

            return if *is_finished && frame_is_null(&peek.frame) {
                Ok(audio_frame_queue.pop_front())
            } else {
                unsafe {
                    if nb_samples != (*peek.frame.as_ptr()).nb_samples
                        || !frame_is_aligned(align_mask, peek.frame.as_ptr())
                    {
                        match receive_samples(
                            samples_queued,
                            audio_frame_queue,
                            nb_samples,
                            frame_pool,
                            align_mask,
                        ) {
                            Err(ret) => {
                                error!("Error receive audio frame: {}", av_err2str(ret));
                                set_scheduler_error(
                                    scheduler_status,
                                    scheduler_result,
                                    Encoding(EncodingOperationError::ReceiveAudioError(
                                        crate::error::EncodingError::from(ret),
                                    )),
                                );
                                Err(())
                            }
                            Ok(frame) => Ok(Some(frame)),
                        }
                    } else {
                        *samples_queued -= (*peek.frame.as_ptr()).nb_samples;
                        Ok(audio_frame_queue.pop_front())
                    }
                }
            }
        }
    }
    Ok(None)
}

fn receive_frame(
    opened: &mut bool,
    receiver: &Receiver<FrameBox>,
    frame_pool: &ObjPool<Frame>,
    enc_ctx: *mut AVCodecContext,
    stream: *mut AVStream,
    ready_sender: &Option<Sender<i32>>,
    bits_per_raw_sample: &Option<i32>,
    frame_samples: &mut i32,
    align_mask: &mut usize,
    samples_queued: &mut i32,
    audio_frame_queue: &mut VecDeque<FrameBox>,
    samples_sent: &mut i64,
    frames_sent: &mut i64,
    is_finished: &mut bool,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>
) -> SyncFrame {
    let mut frame_box = if !*opened {
        let mut frame_box = match receive_from(receiver, scheduler_status) {
            Ok(frame) => frame,
            Err(SyncFrame::Break) => {
                // The source disconnected before delivering the first frame:
                // like the EOF-marker (null frame) case below, the encoder never
                // opened, so this output stream never becomes ready and the muxer
                // aborts on the ready-channel disconnect WITHOUT an error —
                // reporting a false success. Record the failure so wait() surfaces
                // it, unless we are shutting down (a clean stop is not a failure).
                if !is_stopping(scheduler_status.load(Ordering::Acquire)) {
                    let output_stream_index = unsafe { (*stream).index };
                    error!("Source disconnected before any frame for output stream {output_stream_index}; encoder never opened");
                    set_scheduler_error(
                        scheduler_status,
                        scheduler_result,
                        OpenEncoder(OpenEncoderOperationError::NoFramesReceived),
                    );
                }
                return SyncFrame::Break;
            }
            Err(sync) => return sync,
        };

        if frame_is_null(&frame_box.frame) {
            frame_pool.release(frame_box.frame);
            // EOF before the first frame: nothing was decoded for this stream,
            // the encoder never opened and the muxer can never become ready.
            // Record a real error so wait() does not report a false success.
            let output_stream_index = unsafe { (*stream).index };
            error!("No frames were received for output stream {output_stream_index}; encoder never opened");
            set_scheduler_error(
                scheduler_status,
                scheduler_result,
                OpenEncoder(OpenEncoderOperationError::NoFramesReceived),
            );
            return SyncFrame::Break;
        }

        if let Err(e) = enc_open(enc_ctx, stream, &mut frame_box, ready_sender.clone(), *bits_per_raw_sample) {
            frame_pool.release(frame_box.frame);
            error!("Open encoder error: {e}");
            set_scheduler_error(scheduler_status, scheduler_result, e);
            return SyncFrame::Break;
        }
        *opened = true;
        unsafe {
            if (*enc_ctx).codec_type == AVMEDIA_TYPE_AUDIO {
                *frame_samples = (*enc_ctx).frame_size;
                *align_mask = av_cpu_max_align() - 1;
            }
        }

        // A data-less frame is a dummy carrying only encoder init parameters:
        // the filtergraph sends one when a stream produced no frames at all
        // (close_output), so the encoder can open and the muxer become ready.
        // Mirror fftools ffmpeg_sched.c send_to_enc(): open with it, then
        // discard it — there is nothing to encode.
        // SAFETY: the frame pointer is non-null — the frame_is_null early
        // return above already handled the null (EOF marker) case.
        if unsafe { (*frame_box.frame.as_ptr()).buf[0].is_null() } {
            frame_pool.release(frame_box.frame);
            return SyncFrame::Continue;
        }

        frame_box
    } else {
        if *frame_samples > 0 && !audio_frame_queue.is_empty() {
            match process_audio_queue(
                *frame_samples,
                samples_queued,
                audio_frame_queue,
                frame_pool,
                *align_mask,
                samples_sent,
                frames_sent,
                is_finished,
                scheduler_status,
                scheduler_result,
            ) {
                Ok(Some(frame)) => return SyncFrame::FrameBox(frame),
                Err(_) => return SyncFrame::Break,
                _ => {}
            }
        }

        

        match receive_from(receiver, scheduler_status) {
            Ok(frame) => frame,
            Err(sync) => return sync,
        }
    };

    if *frame_samples > 0 {
        *is_finished = frame_is_null(&frame_box.frame);
        if !*is_finished {
            unsafe {
                (*frame_box.frame.as_mut_ptr()).duration = av_rescale_q(
                    (*frame_box.frame.as_ptr()).nb_samples as i64,
                    AVRational {
                        num: 1,
                        den: (*frame_box.frame.as_ptr()).sample_rate,
                    },
                    (*frame_box.frame.as_ptr()).time_base,
                );
                *samples_queued += (*frame_box.frame.as_ptr()).nb_samples;
            }
        }
        audio_frame_queue.push_back(frame_box);

        if *samples_queued < *frame_samples && !*is_finished {
            return SyncFrame::Continue;
        }
        match process_audio_queue(
            *frame_samples,
            samples_queued,
            audio_frame_queue,
            frame_pool,
            *align_mask,
            samples_sent,
            frames_sent,
            is_finished,
            scheduler_status,
            scheduler_result,
        ) {
            Ok(Some(frame)) => SyncFrame::FrameBox(frame),
            Ok(None) => SyncFrame::Continue,
            Err(_) => SyncFrame::Break,
        }
    } else {
        *frames_sent += 1;
        SyncFrame::FrameBox(frame_box)
    }
}

/// `-shortest` (sq_enc): `(end_ts, time_base, nb_samples)` for a frame entering the
/// sync queue. `end_ts = pts + duration` (FFmpeg `frame_end`), `None` when pts is
/// unset. `nb_samples` is the audio sample count (0 for video) for the engine's
/// `frames_max` accounting. Duration is already filled upstream (audio in
/// `receive_frame`/`process_audio_queue`; video by the filtergraph).
unsafe fn sq_frame_end_tb(fb: &FrameBox) -> (Option<i64>, AVRational, i32) {
    let f = fb.frame.as_ptr();
    let pts = (*f).pts;
    let end_ts = if pts == AV_NOPTS_VALUE { None } else { Some(pts + (*f).duration) };
    (end_ts, (*f).time_base, (*f).nb_samples)
}

/// `-shortest` (sq_enc): after a `send`/`heartbeat` under the sync-queue lock,
/// mark every stream the engine just cascade-finished in `sq_finished` (so a
/// truncated encoder observes it and enters its drain phase) and wake any
/// drain-phase peers whose head just advanced. In-memory ops only.
fn sq_propagate_and_notify(q: &mut SyncQueue<FrameBox>, sq_finished: &[AtomicBool], cv: &Condvar) {
    let mut newly = Vec::new();
    q.newly_finished(&mut newly);
    for j in newly {
        sq_finished[j].store(true, Ordering::Release);
    }
    cv.notify_all();
}

/// `-shortest` (sq_enc): encode one frame drained from the queue. The sync-queue
/// lock is already released, so `frame_encode` may park on the pre-mux queue with
/// no lock held (Architecture B). Mirrors the verbatim loop's encode + status
/// handling; returns `true` when the encoder should stop feeding (natural EOF,
/// recording_time limit, or a real error) so the caller proceeds to the shared
/// flush, and sets `finished` on a natural EOF.
unsafe fn sq_encode_drained(
    enc_ctx: *mut AVCodecContext,
    mut fb: FrameBox,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    pkt_sender: &Sender<PacketBox>,
    pre_pkt_sender: &PreMuxQueueSender,
    mux_start_gate: &Arc<crate::core::context::MuxStartGate>,
    stream: *mut AVStream,
    packet_pool: &ObjPool<Packet>,
    forced_kf: &mut ForcedKeyframes,
    frame_pool: &ObjPool<Frame>,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    finished: &mut bool,
) -> bool {
    let result = frame_encode(
        enc_ctx,
        fb.frame.as_mut_ptr(),
        start_time_us,
        recording_time_us,
        pkt_sender,
        pre_pkt_sender,
        mux_start_gate,
        stream,
        packet_pool,
        forced_kf,
    );
    frame_pool.release(fb.frame);
    match result {
        Err(e) => {
            if is_stopping(scheduler_status.load(Ordering::Acquire))
                && matches!(e, Encoding(EncodingOperationError::MuxerFinished))
            {
                debug!("Encoder stopping: muxer already finished");
            } else {
                error!("Error encoding a frame: {}", e);
                set_scheduler_error(scheduler_status, scheduler_result, e);
            }
            true
        }
        Ok(EncodeStatus::Eof) => {
            trace!("Encoder returned EOF, finishing");
            *finished = true;
            true
        }
        Ok(EncodeStatus::LimitReached) => true,
        Ok(EncodeStatus::Continue) => false,
    }
}

fn set_encoder_opts(enc_stream: &EncoderStream, video_codec_opts: &Option<HashMap<CString, CString>>, audio_codec_opts: &Option<HashMap<CString, CString>>, subtitle_codec_opts: &Option<HashMap<CString, CString>>, enc_ctx_box: &CodecContext) -> crate::error::Result<()> {
    let mut encoder_opts = DictGuard::new(if enc_stream.codec_type == AVMEDIA_TYPE_VIDEO {
        hashmap_to_avdictionary(video_codec_opts)
    } else if enc_stream.codec_type == AVMEDIA_TYPE_AUDIO {
        hashmap_to_avdictionary(audio_codec_opts)
    } else if enc_stream.codec_type == AVMEDIA_TYPE_SUBTITLE {
        hashmap_to_avdictionary(subtitle_codec_opts)
    } else {
        null_mut()
    });
    if !encoder_opts.as_ptr().is_null() {
        let ret = unsafe {
            av_opt_set_dict2(
                enc_ctx_box.as_mut_ptr() as *mut libc::c_void,
                encoder_opts.as_double_ptr(),
                AV_OPT_SEARCH_CHILDREN,
            )
        };
        if ret < 0 {
            error!("Error applying encoder options: {}", av_err2str(ret));
            return Err(OpenEncoder(
                OpenEncoderOperationError::ContextAllocationError(OpenEncoderError::from(ret)),
            ));
        }
        // Entries no encoder option matched are user typos (fftools
        // check_avoptions errors out; we surface them as warnings). The
        // guard frees the leftovers, which previously leaked.
        for key in encoder_opts.leftover_keys() {
            warn!(
                "Option '{key}' was not recognized by encoder for stream {}",
                enc_stream.stream_index
            );
        }
    }

    // Default software encoders to auto (multi-)threading, matching the ffmpeg
    // CLI: fftools/ffmpeg_mux_init.c sets thread_count = 0 ("auto") on the
    // encoder context when the user did not pass a `threads` option. Without
    // this, libavcodec's default of thread_count = 1 pins the encode stage —
    // usually the most expensive stage of a software transcode — to a single
    // core. Hardware encoders ignore thread_count, so no codec gating beyond
    // media type is needed; subtitles are excluded. A user-supplied `threads`
    // (applied above via the opts dict) is preserved by only defaulting when
    // absent.
    if enc_stream.codec_type == AVMEDIA_TYPE_VIDEO
        || enc_stream.codec_type == AVMEDIA_TYPE_AUDIO
    {
        let codec_opts = if enc_stream.codec_type == AVMEDIA_TYPE_VIDEO {
            video_codec_opts
        } else {
            audio_codec_opts
        };
        let user_set_threads = codec_opts
            .as_ref()
            .is_some_and(|m| m.keys().any(|k| k.as_bytes() == b"threads"));
        if !user_set_threads {
            unsafe {
                (*enc_ctx_box.as_mut_ptr()).thread_count = 0;
            }
        }
    }

    Ok(())
}

fn get_max_frames(codec_type: AVMediaType, max_video_frames: Option<i64>, max_audio_frames: Option<i64>, max_subtitle_frames: Option<i64>) -> Option<i64> {
    if codec_type == AVMEDIA_TYPE_VIDEO {
        max_video_frames
    } else if codec_type == AVMEDIA_TYPE_AUDIO {
        max_audio_frames
    } else if codec_type == AVMEDIA_TYPE_SUBTITLE {
        max_subtitle_frames
    } else {
        None
    }
}

#[cfg(docsrs)]
unsafe fn receive_samples(
    samples_queued: &mut i32,
    audio_frame_queue: &mut VecDeque<FrameBox>,
    nb_samples: i32,
    frame_pool: &ObjPool<Frame>,
    align_mask: usize,
) -> Result<FrameBox, i32> {
    Err(ffmpeg_sys_next::AVERROR_BUG)
}

#[cfg(not(docsrs))]
unsafe fn receive_samples(
    samples_queued: &mut i32,
    audio_frame_queue: &mut VecDeque<FrameBox>,
    nb_samples: i32,
    frame_pool: &ObjPool<Frame>,
    align_mask: usize,
) -> Result<FrameBox, i32> {
    assert!(*samples_queued >= nb_samples);

    let Ok(mut dst) = frame_pool.get() else {
        return Err(AVERROR(ffmpeg_sys_next::ENOMEM));
    };

    let mut src_box = audio_frame_queue.front_mut().unwrap();
    let src = &mut src_box.frame;

    if (*src.as_ptr()).nb_samples > nb_samples && frame_is_aligned(align_mask, src.as_ptr()) {
        let ret = av_frame_ref(dst.as_mut_ptr(), src.as_ptr());
        if ret < 0 {
            frame_pool.release(dst);
            return Err(ret);
        }

        (*dst.as_mut_ptr()).nb_samples = nb_samples;
        offset_audio(src.as_mut_ptr(), nb_samples);
        *samples_queued -= nb_samples;

        (*dst.as_mut_ptr()).duration = av_rescale_q(
            nb_samples as i64,
            AVRational {
                num: 1,
                den: (*dst.as_ptr()).sample_rate,
            },
            (*dst.as_ptr()).time_base,
        );

        let frame_data = src_box.frame_data.clone();
        return Ok(FrameBox {
            frame: dst,
            frame_data,
        });
    }

    // otherwise allocate a new frame and copy the data
    let mut ret = av_channel_layout_copy(
        &mut (*dst.as_mut_ptr()).ch_layout,
        &(*src.as_ptr()).ch_layout,
    );
    if ret < 0 {
        frame_pool.release(dst);
        return Err(ret);
    }
    (*dst.as_mut_ptr()).format = (*src.as_ptr()).format;
    (*dst.as_mut_ptr()).nb_samples = nb_samples;

    ret = av_frame_get_buffer(dst.as_mut_ptr(), 0);
    if ret < 0 {
        frame_pool.release(dst);
        return Err(ret);
    }

    ret = av_frame_copy_props(dst.as_mut_ptr(), src.as_ptr());
    if ret < 0 {
        frame_pool.release(dst);
        return Err(ret);
    }

    let frame_data = src_box.frame_data.clone();

    (*dst.as_mut_ptr()).nb_samples = 0;
    while (*dst.as_ptr()).nb_samples < nb_samples {
        src_box = audio_frame_queue.front_mut().unwrap();
        let src = &mut src_box.frame;

        let to_copy = std::cmp::min(
            nb_samples - (*dst.as_ptr()).nb_samples,
            (*src.as_ptr()).nb_samples,
        );

        let dst_sample_fmt =
            crate::util::format_convert::sample_fmt_from_raw((*dst.as_ptr()).format)
                .ok_or(AVERROR(ffmpeg_sys_next::EINVAL))?;
        av_samples_copy(
            (*dst.as_ptr()).extended_data,
            (*src.as_ptr()).extended_data,
            (*dst.as_ptr()).nb_samples,
            0,
            to_copy,
            (*dst.as_ptr()).ch_layout.nb_channels,
            dst_sample_fmt,
        );

        if to_copy < (*src.as_ptr()).nb_samples {
            offset_audio(src.as_mut_ptr(), to_copy);
        } else {
            audio_frame_queue.pop_front();
        }

        *samples_queued -= to_copy;
        (*dst.as_mut_ptr()).nb_samples += to_copy;
    }

    (*dst.as_mut_ptr()).duration = av_rescale_q(
        nb_samples as i64,
        AVRational {
            num: 1,
            den: (*dst.as_ptr()).sample_rate,
        },
        (*dst.as_ptr()).time_base,
    );

    Ok(FrameBox {
        frame: dst,
        frame_data,
    })
}

#[cfg(docsrs)]
fn enc_open(
    enc_ctx: *mut AVCodecContext,
    stream: *mut AVStream,
    frame_box: &mut FrameBox,
    ready_sender: Option<Sender<i32>>,
    bits_per_raw_sample: Option<i32>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
fn enc_open(
    enc_ctx: *mut AVCodecContext,
    stream: *mut AVStream,
    frame_box: &mut FrameBox,
    ready_sender: Option<Sender<i32>>,
    bits_per_raw_sample: Option<i32>,
) -> crate::error::Result<()> {
    unsafe {
        let enc = (*enc_ctx).codec;

        let frame = frame_box.frame.as_mut_ptr();
        if (*enc_ctx).codec_type == AVMEDIA_TYPE_VIDEO
            || (*enc_ctx).codec_type == AVMEDIA_TYPE_AUDIO
        {
            // FFmpeg 7.x: global side data still rides on the frames coming
            // out of the filtergraph, so scan them (fftools n7.1 enc_open).
            #[cfg(not(ffmpeg_8_0))]
            for i in 0..(*frame).nb_side_data {
                let desc = av_frame_side_data_desc((**(*frame).side_data.offset(i as isize)).type_);

                if ((*desc).props & AV_SIDE_DATA_PROP_GLOBAL as u32) == 0 {
                    continue;
                }

                let ret = av_frame_side_data_clone(
                    &mut (*enc_ctx).decoded_side_data,
                    &mut (*enc_ctx).nb_decoded_side_data,
                    *(*frame).side_data.offset(i as isize),
                    AV_FRAME_SIDE_DATA_FLAG_UNIQUE as u32,
                );
                if ret < 0 {
                    return Err(OpenEncoder(
                        OpenEncoderOperationError::FrameSideDataCloneError(
                            OpenEncoderError::OutOfMemory,
                        ),
                    ));
                }
            }

            // FFmpeg 8+: the filtergraph negotiates global side data itself
            // and it arrives on the FrameData sidecar instead of the frames
            // (fftools enc_open after 7b18beb477).
            #[cfg(ffmpeg_8_0)]
            if let Some(sd_list) = frame_box.frame_data.side_data.as_ref() {
                for sd in sd_list.iter() {
                    let ret = av_frame_side_data_clone(
                        &mut (*enc_ctx).decoded_side_data,
                        &mut (*enc_ctx).nb_decoded_side_data,
                        sd,
                        AV_FRAME_SIDE_DATA_FLAG_UNIQUE as u32,
                    );
                    if ret < 0 {
                        return Err(OpenEncoder(
                            OpenEncoderOperationError::FrameSideDataCloneError(
                                OpenEncoderError::OutOfMemory,
                            ),
                        ));
                    }
                }
            }

            (*enc_ctx).time_base = (*frame).time_base;
            if let Some(framerate) = frame_box.frame_data.framerate {
                (*enc_ctx).framerate = framerate;
                (*stream).avg_frame_rate = framerate;
            }
        }

        match (*enc_ctx).codec_type {
            AVMEDIA_TYPE_AUDIO => {
                // A malformed init frame must fail the task with a clear
                // error, not abort the process (the old assert! shadowed the
                // graceful branch below it).
                if (*frame).format == AV_SAMPLE_FMT_NONE as i32
                    || (*frame).sample_rate <= 0
                    || (*frame).ch_layout.nb_channels <= 0
                {
                    return Err(OpenOutputError::UnknownFrameFormat.into());
                }
                (*enc_ctx).sample_fmt =
                    crate::util::format_convert::sample_fmt_from_raw((*frame).format)
                        .ok_or(OpenOutputError::UnknownFrameFormat)?;
                (*enc_ctx).sample_rate = (*frame).sample_rate;
                let ret = av_channel_layout_copy(&mut (*enc_ctx).ch_layout, &(*frame).ch_layout);
                if ret < 0 {
                    return Err(OpenEncoder(
                        OpenEncoderOperationError::ChannelLayoutCopyError(
                            OpenEncoderError::OutOfMemory,
                        ),
                    ));
                }

                if let Some(bits_per_raw_sample) = bits_per_raw_sample {
                    (*enc_ctx).bits_per_raw_sample = bits_per_raw_sample;
                } else {
                    (*enc_ctx).bits_per_raw_sample = std::cmp::min(
                        frame_box.frame_data.bits_per_raw_sample,
                        av_get_bytes_per_sample((*enc_ctx).sample_fmt) << 3,
                    );
                }
            }
            AVMEDIA_TYPE_VIDEO => {
                if (*frame).format == AV_PIX_FMT_NONE as i32
                    || (*frame).width <= 0
                    || (*frame).height <= 0
                {
                    return Err(OpenOutputError::UnknownFrameFormat.into());
                }

                (*enc_ctx).width = (*frame).width;
                (*enc_ctx).height = (*frame).height;
                (*enc_ctx).sample_aspect_ratio = (*frame).sample_aspect_ratio;
                (*stream).sample_aspect_ratio = (*frame).sample_aspect_ratio;

                (*enc_ctx).pix_fmt =
                    crate::util::format_convert::pix_fmt_from_raw((*frame).format)
                        .ok_or(OpenOutputError::UnknownFrameFormat)?;

                if let Some(bits_per_raw_sample) = bits_per_raw_sample {
                    (*enc_ctx).bits_per_raw_sample = bits_per_raw_sample;
                } else {
                    // Checked descriptor lookup: a null descriptor (invalid /
                    // unknown pix_fmt) must not be dereferenced.
                    let depth = crate::util::format_convert::pix_fmt_desc_from_raw(
                        (*frame).format,
                    )
                    .ok_or(OpenOutputError::UnknownFrameFormat)?
                    .comp[0]
                        .depth;
                    (*enc_ctx).bits_per_raw_sample =
                        std::cmp::min(frame_box.frame_data.bits_per_raw_sample, depth);
                }

                (*enc_ctx).color_range = (*frame).color_range;
                (*enc_ctx).color_primaries = (*frame).color_primaries;
                (*enc_ctx).color_trc = (*frame).color_trc;
                (*enc_ctx).colorspace = (*frame).colorspace;
                (*enc_ctx).chroma_sample_location = (*frame).chroma_location;

                if ((*enc_ctx).flags as u32
                    & (AV_CODEC_FLAG_INTERLACED_DCT | AV_CODEC_FLAG_INTERLACED_ME))
                    != 0
                    || ((*frame).flags & AV_FRAME_FLAG_INTERLACED) != 0
                {
                    let top_field_first = ((*frame).flags & AV_FRAME_FLAG_TOP_FIELD_FIRST) != 0;

                    if (*enc).id == AV_CODEC_ID_MJPEG {
                        (*enc_ctx).field_order = if top_field_first {
                            AV_FIELD_TT
                        } else {
                            AV_FIELD_BB
                        };
                    } else {
                        (*enc_ctx).field_order = if top_field_first {
                            AV_FIELD_TB
                        } else {
                            AV_FIELD_BT
                        };
                    }
                } else {
                    (*enc_ctx).field_order = AV_FIELD_PROGRESSIVE;
                }
            }
            AVMEDIA_TYPE_SUBTITLE => {
                (*enc_ctx).time_base = AV_TIME_BASE_Q;

                if (*enc_ctx).width == 0 {
                    (*enc_ctx).width = frame_box.frame_data.input_stream_width;
                    (*enc_ctx).height = frame_box.frame_data.input_stream_height;
                }

                if let Some(header) = frame_box.frame_data.subtitle_header.as_deref() {
                    /* ASS code assumes this buffer is null terminated so add extra byte. */
                    let subtitle_header = av_mallocz(header.len() + 1) as *mut u8;
                    if subtitle_header.is_null() {
                        return Err(OpenEncoder(
                            OpenEncoderOperationError::SettingSubtitleError(
                                OpenEncoderError::OutOfMemory,
                            ),
                        ));
                    }
                    std::ptr::copy_nonoverlapping(header.as_ptr(), subtitle_header, header.len());
                    (*enc_ctx).subtitle_header = subtitle_header;
                    (*enc_ctx).subtitle_header_size = header.len() as i32;
                }
            }
            // fftools av_assert0(0) (ffmpeg_enc.c:303): the mapping layer
            // never routes DATA/ATTACHMENT into an encoder, but a library
            // fails the task instead of aborting on the broken invariant.
            _ => return Err(OpenEncoder(OpenEncoderOperationError::UnsupportedMediaType)),
        }

        if (*enc).capabilities as u32 & AV_CODEC_CAP_ENCODER_REORDERED_OPAQUE != 0 {
            (*enc_ctx).flags |= AV_CODEC_FLAG_COPY_OPAQUE as i32;
        }
        (*enc_ctx).flags |= AV_CODEC_FLAG_FRAME_DURATION as i32;

        let frames_ref = if (*enc_ctx).codec_type == AVMEDIA_TYPE_VIDEO
            || (*enc_ctx).codec_type == AVMEDIA_TYPE_AUDIO
        {
            (*frame).hw_frames_ctx
        } else {
            null_mut()
        };
        let ret = hw_device_setup_for_encode(enc_ctx, frames_ref);
        if ret < 0 {
            error!("Encoding hardware device setup failed");
            return Err(OpenEncoder(OpenEncoderOperationError::HwSetupError(
                OpenEncoderError::OutOfMemory,
            )));
        }

        let ret = avcodec_open2(enc_ctx, enc, null_mut());
        if ret < 0 {
            if ret != AVERROR_EXPERIMENTAL {
                error!("Error while opening encoder - maybe incorrect parameters such as bit_rate, rate, width or height.");
            }
            return Err(OpenEncoder(OpenEncoderOperationError::CodecOpenError(
                OpenEncoderError::OutOfMemory,
            )));
        }

        if (*enc_ctx).bit_rate != 0
            && (*enc_ctx).bit_rate < 1000
            && (*enc_ctx).codec_id != AV_CODEC_ID_CODEC2
        /* don't complain about 700 bit/s modes */
        {
            warn!("The bitrate parameter is set too low. It takes bits/s as argument, not kbits/s");
        }

        let ret = avcodec_parameters_from_context((*stream).codecpar, enc_ctx);
        if ret < 0 {
            error!("Error initializing the output stream codec context.");
            return Err(OpenEncoder(
                OpenEncoderOperationError::CodecParametersError(OpenEncoderError::OutOfMemory),
            ));
        }

        // copy timebase while removing common factors
        if (*stream).time_base.num <= 0 || (*stream).time_base.den <= 0 {
            (*stream).time_base = av_add_q((*enc_ctx).time_base, AVRational { num: 0, den: 1 });
        }

        if let Some(ready_sender) = ready_sender {
            let _ = ready_sender.send((*stream).index);
        }
    }

    Ok(())
}

unsafe fn hw_device_setup_for_encode(
    enc_ctx: *mut AVCodecContext,
    mut frames_ref: *mut AVBufferRef,
) -> i32 {
    let mut dev = None;

    if !frames_ref.is_null()
        && (*((*frames_ref).data as *mut AVHWFramesContext)).format == (*enc_ctx).pix_fmt
    {
        // Matching format, will try to use hw_frames_ctx.
    } else {
        frames_ref = null_mut();
    }

    let mut i = 0;
    loop {
        let config = avcodec_get_hw_config((*enc_ctx).codec, i);
        if config.is_null() {
            break;
        }

        if !frames_ref.is_null()
            && (*config).methods & AV_CODEC_HW_CONFIG_METHOD_HW_FRAMES_CTX as i32 != 0
            && ((*config).pix_fmt == AV_PIX_FMT_NONE || (*config).pix_fmt == (*enc_ctx).pix_fmt)
        {
            trace!(
                "Using input frames context (format {}) with {} encoder.",
                CStr::from_ptr(av_get_pix_fmt_name((*enc_ctx).pix_fmt))
                    .to_str()
                    .unwrap_or("[unknow / Invalid UTF-8]"),
                CStr::from_ptr((*(*enc_ctx).codec).name)
                    .to_str()
                    .unwrap_or("[unknow codec / Invalid UTF-8]")
            );
            (*enc_ctx).hw_frames_ctx = av_buffer_ref(frames_ref);
            if (*enc_ctx).hw_frames_ctx.is_null() {
                return AVERROR(ffmpeg_sys_next::ENOMEM);
            }
            return 0;
        }

        if dev.is_none() && (*config).methods & AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX as i32 != 0
        {
            dev = hw_device_get_by_type((*config).device_type);
        }

        i += 1;
    }

    if let Some(dev) = dev {
        trace!(
            "Using device %s (type {}) with {} encoder.",
            dev.name,
            CStr::from_ptr((*(*enc_ctx).codec).name)
                .to_str()
                .unwrap_or("[unknow codec / Invalid UTF-8]")
        );
        (*enc_ctx).hw_device_ctx = av_buffer_ref(dev.device_ref);
        if (*enc_ctx).hw_device_ctx.is_null() {
            return AVERROR(ffmpeg_sys_next::ENOMEM);
        }
    }

    0
}

#[cfg(not(docsrs))]
unsafe fn offset_audio(f: *mut AVFrame, nb_samples: i32) {
    // A decoded audio frame always carries a valid sample format; guard the
    // conversion anyway so an out-of-range value is a safe no-op (leave the
    // frame untouched) instead of transmuting into an invalid enum (UB) or
    // reaching the `bps > 0` assert below with bps == 0.
    let Some(sample_fmt) = crate::util::format_convert::sample_fmt_from_raw((*f).format) else {
        return;
    };
    let planar = av_sample_fmt_is_planar(sample_fmt);
    let planes = if planar != 0 {
        (*f).ch_layout.nb_channels
    } else {
        1
    };
    let bps = av_get_bytes_per_sample(sample_fmt);
    let offset = (nb_samples
        * bps
        * if planar != 0 {
            1
        } else {
            (*f).ch_layout.nb_channels
        }) as usize;

    assert!(bps > 0);
    assert!(nb_samples < (*f).nb_samples);

    for i in 0..planes as usize {
        std::ptr::write(
            (*f).extended_data.add(i),
            (*(*f).extended_data.add(i)).add(offset),
        );
        if i < (*f).data.len() {
            *(*f).data.get_unchecked_mut(i) = *(*f).extended_data.add(i);
        }
    }

    (*f).linesize[0] -= offset as i32;
    (*f).nb_samples -= nb_samples;

    (*f).duration = av_rescale_q(
        (*f).nb_samples as i64,
        AVRational {
            num: 1,
            den: (*f).sample_rate,
        },
        (*f).time_base,
    );

    (*f).pts += av_rescale_q(
        nb_samples as i64,
        AVRational {
            num: 1,
            den: (*f).sample_rate,
        },
        (*f).time_base,
    );
}

unsafe fn frame_is_aligned(align_mask: usize, frame: *const AVFrame) -> bool {
    assert!((*frame).nb_samples > 0);
    assert!(align_mask > 0);

    let data_ptr = (*frame).data[0] as usize;
    let linesize = (*frame).linesize[0] as usize;

    if (data_ptr & align_mask) == 0 && (linesize & align_mask) == 0 && linesize > align_mask {
        return true;
    }

    false
}

/// fftools `KeyframeForceCtx` (list subset): sorted forced-keyframe times in
/// `AV_TIME_BASE_Q` microseconds plus the cursor into them. One per video encoder;
/// `index` advances across frames as forced times are consumed. Empty `pts` = off.
#[cfg(not(docsrs))]
struct ForcedKeyframes {
    /// Sorted ascending, microseconds.
    pts: Vec<i64>,
    /// Cursor into `pts`.
    index: usize,
}

#[cfg(not(docsrs))]
fn frame_encode(
    enc_ctx: *mut AVCodecContext,
    frame: *mut AVFrame,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    pkt_sender: &Sender<PacketBox>,
    pre_pkt_sender: &PreMuxQueueSender,
    mux_start_gate: &Arc<crate::core::context::MuxStartGate>,
    stream: *mut AVStream,
    packet_pool: &ObjPool<Packet>,
    forced_kf: &mut ForcedKeyframes,
) -> crate::error::Result<EncodeStatus> {
    unsafe {
        if (*enc_ctx).codec_type == AVMEDIA_TYPE_SUBTITLE {
            let subtitle = if !frame.is_null() && !(*frame).buf[0].is_null() {
                (*(*frame).buf[0]).data as *const AVSubtitle
            } else {
                null()
            };

            return if !subtitle.is_null() && (*subtitle).num_rects != 0 {
                do_subtitle_out(
                    enc_ctx,
                    subtitle,
                    start_time_us,
                    recording_time_us,
                    pkt_sender,
                    pre_pkt_sender,
                    mux_start_gate,
                    stream,
                )
            } else {
                Ok(EncodeStatus::Continue)
            };
        }

        if !frame.is_null() {
            if let Some(recording_time_us) = recording_time_us {
                if av_compare_ts(
                    (*frame).pts,
                    (*frame).time_base,
                    recording_time_us,
                    AV_TIME_BASE_Q,
                ) >= 0
                {
                    debug!("Reached the target time: {recording_time_us} us, frame time: {} us. Ending the recording.", (*frame).pts);
                    return Ok(EncodeStatus::LimitReached);
                }
            }

            if (*enc_ctx).codec_type == AVMEDIA_TYPE_VIDEO {
                (*frame).quality = (*enc_ctx).global_quality;
                (*frame).pict_type = AV_PICTURE_TYPE_NONE;

                // fftools forced_kf_apply (list form): request an IDR at the first
                // frame whose PTS reaches the next forced time. `if`, not `while` —
                // at most one advance per frame, matching the CLI. The empty-list and
                // NOPTS checks are defensive supersets that never force spuriously.
                if !forced_kf.pts.is_empty()
                    && (*frame).pts != AV_NOPTS_VALUE
                    && forced_kf.index < forced_kf.pts.len()
                    && av_compare_ts(
                        (*frame).pts,
                        (*frame).time_base,
                        forced_kf.pts[forced_kf.index],
                        AV_TIME_BASE_Q,
                    ) >= 0
                {
                    (*frame).pict_type = AV_PICTURE_TYPE_I;
                    forced_kf.index += 1;
                }
            } else if (*(*enc_ctx).codec).capabilities & AV_CODEC_CAP_PARAM_CHANGE as i32 == 0
                && (*enc_ctx).ch_layout.nb_channels != (*frame).ch_layout.nb_channels
            {
                error!("Audio channel count changed and encoder does not support parameter changes");
                return Ok(EncodeStatus::Continue);
            }
        }
        encode_frame(enc_ctx, frame, pkt_sender,  pre_pkt_sender, mux_start_gate, stream, packet_pool)
            .map(|eof| if eof { EncodeStatus::Eof } else { EncodeStatus::Continue })
    }
}

#[cfg(not(docsrs))]
unsafe fn do_subtitle_out(
    enc_ctx: *mut AVCodecContext,
    sub: *const AVSubtitle,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    pkt_sender: &Sender<PacketBox>,
    pre_pkt_sender: &PreMuxQueueSender,
    mux_start_gate: &Arc<crate::core::context::MuxStartGate>,
    stream: *mut AVStream,
) -> crate::error::Result<EncodeStatus> {
    let subtitle_out_max_size = 1024 * 1024;
    if (*sub).pts == AV_NOPTS_VALUE {
        return Err(Encoding(EncodingOperationError::SubtitleNotPts));
    }
    if let Some(start_time_us) = start_time_us {
        if (*sub).pts < start_time_us {
            return Ok(EncodeStatus::Continue);
        }
    }

    let nb = if (*enc_ctx).codec_id == AV_CODEC_ID_DVB_SUBTITLE {
        2
    } else if (*enc_ctx).codec_id == AV_CODEC_ID_ASS {
        std::cmp::max((*sub).num_rects, 1)
    } else {
        1
    };

    let mut pts = (*sub).pts;
    if let Some(start_time_us) = start_time_us {
        pts -= start_time_us;
    }
    for i in 0..nb {
        let mut local_sub = *sub;
        if let Some(recording_time_us) = recording_time_us {
            if av_compare_ts(pts, AV_TIME_BASE_Q, recording_time_us, AV_TIME_BASE_Q) >= 0 {
                return Ok(EncodeStatus::LimitReached);
            }
        }

        let mut packet = Packet::new(subtitle_out_max_size);
        if packet_is_null(&packet) {
            return Err(Encoding(EncodingOperationError::AllocPacket(
                AllocPacketError::OutOfMemory,
            )));
        }
        let pkt = packet.as_mut_ptr();

        local_sub.pts = pts;
        // start_display_time is required to be 0
        local_sub.pts += av_rescale_q(
            (*sub).start_display_time as i64,
            AVRational { num: 1, den: 1000 },
            AV_TIME_BASE_Q,
        );
        local_sub.end_display_time -= (*sub).start_display_time;
        local_sub.start_display_time = 0;

        if (*enc_ctx).codec_id == AV_CODEC_ID_DVB_SUBTITLE && i == 1 {
            local_sub.num_rects = 0;
        } else if (*enc_ctx).codec_id == AV_CODEC_ID_ASS && (*sub).num_rects > 0 {
            local_sub.num_rects = 1;
            local_sub.rects = local_sub.rects.add(i as usize);
        }

        let subtitle_out_size =
            avcodec_encode_subtitle(enc_ctx, (*pkt).data, (*pkt).size, &local_sub);
        if subtitle_out_size < 0 {
            error!("Subtitle encoding failed");
            return Err(Encoding(EncodingOperationError::EncodeSubtitle(
                EncodeSubtitleError::from(subtitle_out_size),
            )));
        }

        av_shrink_packet(pkt, subtitle_out_size);

        (*pkt).time_base = AV_TIME_BASE_Q;
        (*pkt).pts = (*sub).pts;
        (*pkt).duration = av_rescale_q(
            (*sub).end_display_time as i64,
            AVRational { num: 1, den: 1000 },
            (*pkt).time_base,
        );
        if (*enc_ctx).codec_id == AV_CODEC_ID_DVB_SUBTITLE {
            /* XXX: the pts correction is handled here. Maybe handling
            it in the codec would be better */
            if i == 0 {
                (*pkt).pts += av_rescale_q(
                    (*sub).start_display_time as i64,
                    AVRational { num: 1, den: 1000 },
                    (*pkt).time_base,
                );
            } else {
                (*pkt).pts += av_rescale_q(
                    (*sub).end_display_time as i64,
                    AVRational { num: 1, den: 1000 },
                    (*pkt).time_base,
                );
            }
        }
        (*pkt).dts = (*pkt).pts;

        if let Err(_) = send_to_mux(PacketBox {
            packet,
            packet_data: PacketData {
                dts_est: 0,
                codec_type: (*enc_ctx).codec_type,
                output_stream_index: (*stream).index,
                is_copy: false,
            },
        }, pkt_sender, pre_pkt_sender, mux_start_gate) {
            debug!("send subtitle packet failed, mux already finished");
            return Err(Encoding(EncodingOperationError::MuxerFinished));
        }
    }

    Ok(EncodeStatus::Continue)
}

#[cfg(not(docsrs))]
unsafe fn encode_frame(
    enc_ctx: *mut AVCodecContext,
    frame: *mut AVFrame,
    pkt_sender: &Sender<PacketBox>,
    pre_pkt_sender: &PreMuxQueueSender,
    mux_start_gate: &Arc<crate::core::context::MuxStartGate>,
    stream: *mut AVStream,
    packet_pool: &ObjPool<Packet>,
) -> crate::error::Result<bool> {
    if !frame.is_null()
        && (*frame).sample_aspect_ratio.num != 0 {
            (*enc_ctx).sample_aspect_ratio = (*frame).sample_aspect_ratio;
        }

    let ret = avcodec_send_frame(enc_ctx, frame);
    if ret < 0 && !(ret == AVERROR_EOF && frame.is_null()) {
        if ret == AVERROR_EOF && frame.is_null(){
            trace!("EOF reached, no more frames to encode.");
        } else {
            error!(
            "Error submitting {:?} frame to the encoder",
            (*enc_ctx).codec_type
        );
            return Err(Encoding(EncodingOperationError::SendFrameError(
                EncodingError::from(ret),
            )));
        }
    }

    loop {
        let mut packet = packet_pool.get()?;
        let pkt = packet.as_mut_ptr();

        let ret = avcodec_receive_packet(enc_ctx, pkt);

        (*pkt).time_base = (*enc_ctx).time_base;

        if ret == AVERROR(EAGAIN) {
            return Ok(false);
        } else if ret < 0 {
            if ret == AVERROR_EOF {
                trace!("EOF reached. No more packets to receive.");
                return Ok(true);
            }
            error!("{:?} encoding failed", (*enc_ctx).codec_type);
            return Err(Encoding(EncodingOperationError::ReceivePacketError(
                EncodingError::from(ret),
            )));
        }

        (*pkt).flags |= AV_PKT_FLAG_TRUSTED;

        if let Err(_) = send_to_mux(PacketBox {
            packet,
            packet_data: PacketData {
                dts_est: 0,
                codec_type: (*enc_ctx).codec_type,
                output_stream_index: (*stream).index,
                is_copy: false,
            },
        }, pkt_sender, pre_pkt_sender, mux_start_gate) {
            debug!("send packet failed, mux already finished");
            return Err(Encoding(EncodingOperationError::MuxerFinished));
        }
    }
}


fn send_to_mux(
    packet_box: PacketBox,
    pkt_sender: &Sender<PacketBox>,
    pre_pkt_sender: &PreMuxQueueSender,
    mux_start_gate: &Arc<crate::core::context::MuxStartGate>,
) -> Result<(), SendError<PacketBox>> {
    use crate::core::context::PreSendOutcome;

    if mux_start_gate.is_started() {
        return pkt_sender.send(packet_box);
    }

    // The gate serializes "started?" with pre-queue admission: without it a
    // packet parked between the muxer's drain and its gate flip would never
    // be delivered.
    //
    // A full pre-queue parks on the queue condvar (PERF-12; replaces the old
    // 10ms sleep ticks) and re-runs the gated send_pre — the authoritative
    // admission / gate-started / disconnected check — after every wake.
    // Wakes come from the mux-start drain and from the receiver dropping
    // (stop or mux-init failure promptly resolves Disconnected, handled as
    // MuxerFinished, not a job failure); the bounded wait slice is only a
    // lost-notify safety net. The deadline guards a muxer that never starts
    // and is never stopped — e.g. a single-input job where this very
    // backpressure keeps the demuxer from ever reaching a sparse stream's
    // first packet, so no drain can ever come. FFmpeg fails such jobs
    // immediately ("Too many packets buffered for output stream"); parking
    // ~60s first keeps multi-input slow starts (a second input taking
    // seconds to open) succeeding, then fails with the same remedy.
    let mut packet_box = packet_box;
    let deadline = Instant::now() + PRE_MUX_FULL_BACKSTOP;
    loop {
        packet_box = match mux_start_gate.send_pre(pre_pkt_sender, packet_box) {
            PreSendOutcome::Sent => return Ok(()),
            PreSendOutcome::Started(pb) => return pkt_sender.send(pb),
            PreSendOutcome::Full(pb) => pb,
            PreSendOutcome::Disconnected(pb) => return Err(SendError(pb)),
        };
        if Instant::now() >= deadline {
            error!(
                "pre-mux queue stayed full for {}s before the muxer started; raise \
                 Output::set_max_muxing_queue_size / Output::set_muxing_queue_data_threshold, \
                 or check that every mapped output stream receives data",
                PRE_MUX_FULL_BACKSTOP.as_secs()
            );
            return Err(SendError(packet_box));
        }
        let size = packet_payload_size(&packet_box);
        pre_pkt_sender.wait_for_space(size, PRE_MUX_FULL_WAIT_SLICE);
    }
}

/// How long an encoder parks on a full pre-mux queue waiting for the muxer to
/// open before giving up. Termination normally comes from the pre-queue
/// receiver disconnecting (stop / mux-init failure) long before this; the
/// deadline only guards a muxer that never starts and is never stopped.
const PRE_MUX_FULL_BACKSTOP: Duration = Duration::from_secs(60);

/// Upper bound on one condvar wait while parked on a full pre-mux queue.
/// Purely a lost-notify safety net (mirrors the conc-06 pause gate); real
/// wakes come from the drain and from receiver drop.
const PRE_MUX_FULL_WAIT_SLICE: Duration = Duration::from_millis(200);