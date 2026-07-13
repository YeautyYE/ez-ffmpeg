use crate::core::context::decoder_stream::DecoderStream;
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::{null_frame, CodecContext, FrameBox, FrameData, PacketBox};
use crate::core::scheduler::ffmpeg_scheduler::{
    is_stopping, packet_is_null, set_scheduler_error, wait_until_not_paused,
};
use crate::error::DecodingOperationError::DecodeSubtitleError;
use crate::error::Error::{Bug, Decoding, OpenDecoder};
use crate::error::{
    DecodingError, DecodingOperationError, Error, OpenDecoderError, OpenDecoderOperationError,
};
use crate::hwaccel::HWAccelID::{HwaccelAuto, HwaccelGeneric};
use crate::hwaccel::{
    hw_device_get_by_name, hw_device_get_by_type, hw_device_init_from_type,
    hw_device_match_by_codec, HWAccelID,
};
use crate::util::ffmpeg_utils::av_rescale_q_rnd;
use crate::util::ffmpeg_utils::{av_err2str, hashmap_to_avdictionary, DictGuard};
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::{RecvTimeoutError, Sender};
use ffmpeg_next::packet::{Mut, Ref};
use ffmpeg_next::{Frame, Packet};
use ffmpeg_sys_next::AVHWDeviceType::AV_HWDEVICE_TYPE_QSV;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVRounding::AV_ROUND_UP;
use ffmpeg_sys_next::AVSubtitleType::SUBTITLE_BITMAP;
use ffmpeg_sys_next::{
    av_buffer_create, av_buffer_ref, av_calloc, av_dict_set, av_frame_apply_cropping,
    av_frame_copy_props, av_frame_move_ref, av_frame_ref, av_frame_unref, av_free, av_freep,
    av_gcd, av_hwdevice_get_type_name, av_hwframe_transfer_data, av_inv_q, av_mallocz, av_memdup,
    av_mul_q, av_opt_set_dict2, av_pix_fmt_desc_get, av_rescale_delta, av_rescale_q, av_strdup,
    avcodec_alloc_context3, avcodec_decode_subtitle2, avcodec_default_get_buffer2,
    avcodec_flush_buffers, avcodec_get_hw_config, avcodec_open2, avcodec_parameters_to_context,
    avcodec_receive_frame, avcodec_send_packet, avsubtitle_free, AVCodec, AVCodecContext, AVFrame,
    AVHWDeviceType, AVMediaType, AVPixelFormat, AVRational, AVSubtitle, AVSubtitleRect, AVERROR,
    AVERROR_EOF, AVPALETTE_SIZE, AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX, AV_FRAME_CROP_UNALIGNED,
    AV_FRAME_FLAG_CORRUPT, AV_NOPTS_VALUE, AV_PIX_FMT_FLAG_HWACCEL, AV_TIME_BASE_Q, EAGAIN, EINVAL,
    ENOMEM,
};
#[cfg(not(docsrs))]
use ffmpeg_sys_next::{av_channel_layout_copy, AV_CODEC_FLAG_COPY_OPAQUE, FF_THREAD_FRAME};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

#[cfg(docsrs)]
pub(crate) fn dec_init(
    demux_idx: usize,
    dec_stream: &mut DecoderStream,
    exit_on_error: Option<bool>,
    frame_pool: ObjPool<Frame>,
    packet_pool: ObjPool<Packet>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
pub(crate) fn dec_init(
    demux_idx: usize,
    dec_stream: &mut DecoderStream,
    exit_on_error: Option<bool>,
    frame_pool: ObjPool<Frame>,
    packet_pool: ObjPool<Packet>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    let receiver = dec_stream.take_src();

    // A stream with no downstream consumer is never decoded, so skip it before
    // requiring a decoder. Decoder-less streams (e.g. `tmcd`/`data`) legitimately
    // carry a null codec pointer; the null-codec guard below used to run first and
    // abort the whole job even when such a stream is only demuxed/stream-copied or
    // dropped (GitHub #43). `connect_stream` only wires a source for streams routed
    // to a decoder, so `receiver.is_none()` precisely marks the not-decoded case.
    if receiver.is_none() {
        debug!(
            "Demuxer:{demux_idx} stream:[{}] not be used. skip.",
            dec_stream.stream_index
        );
        return Ok(());
    }
    let receiver = receiver.unwrap();

    let codec_ptr = dec_stream.codec.as_ptr();
    if codec_ptr.is_null() {
        error!(
            "Decoder codec pointer is null for stream {}",
            dec_stream.stream_index
        );
        return Err(OpenDecoder(
            OpenDecoderOperationError::ContextAllocationError(OpenDecoderError::OutOfMemory),
        ));
    }

    let codec_name_ptr = unsafe { (*codec_ptr).name };
    if codec_name_ptr.is_null() {
        error!(
            "Decoder codec name pointer is null for stream {}",
            dec_stream.stream_index
        );
        return Err(OpenDecoder(
            OpenDecoderOperationError::ContextAllocationError(OpenDecoderError::OutOfMemory),
        ));
    }

    let decoder_name = unsafe { CStr::from_ptr(codec_name_ptr).to_str().unwrap_or("unknown") };

    let dp = DecoderParameter::new(dec_stream);
    let dp_arc = Arc::new(Mutex::new(dp));
    dec_open(dp_arc.clone(), dec_stream, null_mut())?;

    let senders = dec_stream.take_dsts();
    let exit_on_error = exit_on_error.unwrap_or(false);

    let dp_arc_worker = dp_arc.clone();

    // Slot claimed before spawn; the guard releases it on any exit path.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(
        thread_sync.clone(),
        scheduler_status.clone(),
        scheduler_result.clone(),
    );
    // The AVCodecContext's opaque holds one raw Arc refcount (installed by
    // dec_open for get_format). Constructed BEFORE the spawn and moved into
    // the closure: a failed spawn drops the closure — and with it this guard
    // — so the reclaim happens on spawn failure, worker unwind, and normal
    // exit alike, with no reliance on any code (even a log call) running
    // after the failure. drop_opaque_ptr is idempotent.
    let opaque_reclaim = DecOpaqueReclaimGuard {
        dp_arc: dp_arc.clone(),
    };

    let result = std::thread::Builder::new()
        .name(format!(
            "decoder{}:{demux_idx}:{decoder_name}",
            dec_stream.stream_index,
        ))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            let dp_arc = dp_arc_worker;
            let _opaque_reclaim = opaque_reclaim;
            // `receiver`/`senders` are `move`-closure CAPTURES; rebind them as
            // body locals declared AFTER the guard so they drop BEFORE it. For a
            // `crossbeam_channel::bounded` channel the queued items are freed only when
            // the LAST endpoint drops, so an undrained HW `FrameBox` in the
            // decoder->filter channel (whose `AVBufferRef` release callback can block)
            // would otherwise be torn down after this worker released its slot — i.e.
            // after the sync counter that stop()/wait() gate on already hit zero.
            let receiver = receiver;
            let senders = senders;
            let input_status = false;
            let mut err_exit = false;

            loop {
                if input_status {
                    break;
                }

                let result = receiver.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    info!("Decoder receiver end command, finishing.");
                    break;
                }

                if let Err(e) = result {
                    if e == RecvTimeoutError::Disconnected {
                        debug!("Demuxer thread exit.");
                        // input_status = true;
                        break;
                    }
                    continue;
                }

                let packet_box = result.unwrap();

                unsafe {
                    let have_data = !input_status
                        && !packet_is_null(&packet_box.packet)
                        && (!(*packet_box.packet.as_ptr()).buf.is_null()
                            || (*packet_box.packet.as_ptr()).side_data_elems != 0
                            || (*packet_box.packet.as_ptr()).opaque as usize as i32
                                == PacketOpaque::PktOpaqueSubHeartbeat as i32
                            || (*packet_box.packet.as_ptr()).opaque as usize as i32
                                == PacketOpaque::PktOpaqueFixSubDuration as i32);

                    let mut flush_buffers = !input_status && !have_data;
                    if !have_data {
                        match flush_buffers {
                            true => trace!("Decoder thread received flush packet"),
                            false => trace!("Decoder thread received EOF packet"),
                        }
                    }

                    if let Err(e) = packet_decode(
                        &dp_arc,
                        exit_on_error,
                        packet_box,
                        &packet_pool,
                        &frame_pool,
                        &senders,
                        &scheduler_status,
                    ) {
                        if e == Error::Exit {
                            flush_buffers = false;
                        }

                        if e == Error::Exit || e == Error::EOF {
                            trace!(
                                "Decoder returned EOF, {}",
                                if flush_buffers {
                                    "resetting"
                                } else {
                                    "finishing"
                                }
                            );
                            if !flush_buffers {
                                break;
                            }

                            /* report last frame duration to the scheduler */
                            let dp = dp_arc.clone();
                            let dp = dp.lock().unwrap();

                            avcodec_flush_buffers(dp.dec_ctx.as_mut_ptr());
                        } else {
                            err_exit = true;
                            error!("Error processing packet in decoder: {e}");
                            set_scheduler_error(&scheduler_status, &scheduler_result, e);
                            break;
                        }
                    }
                }
            }

            // on success send EOF timestamp to our downstreams
            if !err_exit {
                // Failures here must still fall through to dec_done below —
                // skipping it starves downstream of the finish signal.
                // Losing the EOF marker itself only costs the last-frame
                // duration hint.
                if let Ok(mut frame) = frame_pool.get() {
                    unsafe {
                        {
                            let dp = dp_arc.clone();
                            let dp = dp.lock().unwrap();
                            (*frame.as_mut_ptr()).opaque =
                                FrameOpaque::FrameOpaqueEof as i32 as *mut c_void;
                            (*frame.as_mut_ptr()).pts = if dp.last_frame_pts == AV_NOPTS_VALUE {
                                AV_NOPTS_VALUE
                            } else {
                                dp.last_frame_pts + dp.last_frame_duration_est
                            };
                            (*frame.as_mut_ptr()).time_base = dp.last_frame_tb;
                        }
                        let frame_box = dec_frame_to_box(dp_arc.clone(), frame);
                        if let Err(e) = dec_send(frame_box, &frame_pool, &senders) {
                            if e != Error::EOF {
                                error!("Error signalling EOF: {e}");
                                set_scheduler_error(&scheduler_status, &scheduler_result, e);
                            }
                        }
                    }
                } else {
                    warn!("Failed to allocate the EOF marker frame, skipping EOF timestamp");
                }

                {
                    let dp = dp_arc.clone();
                    let dp = dp.lock().unwrap();
                    let err_rate = if dp.dec.frames_decoded != 0 || dp.dec.decode_errors != 0 {
                        dp.dec.decode_errors as f64
                            / (dp.dec.frames_decoded + dp.dec.decode_errors) as f64
                    } else {
                        0.0
                    };
                    let max_error_rate = 2.0 / 3.0;
                    if err_rate > max_error_rate {
                        // Mirrors FFmpeg's -max_error_rate contract: exceeding the
                        // rate must fail the task, not just log.
                        error!("Decoder error rate {err_rate} exceeds maximum {max_error_rate}");
                        set_scheduler_error(
                            &scheduler_status,
                            &scheduler_result,
                            Decoding(crate::error::DecodingOperationError::ErrorRateExceeded),
                        );
                    } else if err_rate != 0.0 {
                        debug!("Decoder error rate {err_rate}");
                    }
                }
            }

            dec_done(&dp_arc, &senders);

            // The opaque refcount is reclaimed by _opaque_reclaim at scope end
            // (normal and unwind alike).
            debug!("Decoder finished.");
        });
    if let Err(e) = result {
        error!("Decoder thread exited with error: {e}");
        // The closure (and the reclaim guard inside it) was already dropped
        // by the failed spawn — the opaque refcount is reclaimed by then.
        return Err(OpenDecoderOperationError::ThreadExited.into());
    }

    Ok(())
}

/// Reclaims the decoder context's `opaque` Arc refcount when the worker exits
/// — by return or by unwind. Without the unwind half, a panicking decoder
/// leaked its `DecoderParameter` (and the `AVCodecContext`, hardware frame
/// contexts, and buffers it owns) forever.
struct DecOpaqueReclaimGuard {
    dp_arc: Arc<Mutex<DecoderParameter>>,
}

impl Drop for DecOpaqueReclaimGuard {
    fn drop(&mut self) {
        self.dp_arc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drop_opaque_ptr();
    }
}

#[cfg(docsrs)]
unsafe fn transcode_subtitles(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    exit_on_error: bool,
    packet_box: PacketBox,
    packet_pool: &ObjPool<Packet>,
    frame_pool: &ObjPool<Frame>,
    senders: &Vec<Sender<FrameBox>>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn transcode_subtitles(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    exit_on_error: bool,
    mut packet_box: PacketBox,
    packet_pool: &ObjPool<Packet>,
    frame_pool: &ObjPool<Frame>,
    senders: &Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
) -> crate::error::Result<()> {
    if !packet_is_null(&packet_box.packet)
        && (*packet_box.packet.as_ptr()).stream_index >= 0
        && (*packet_box.packet.as_ptr()).opaque as usize as i32
            == PacketOpaque::PktOpaqueSubHeartbeat as i32
    {
        let Ok(mut frame) = frame_pool.get() else {
            return Err(Decoding(DecodingOperationError::FrameAllocationError(
                DecodingError::OutOfMemory,
            )));
        };
        (*frame.as_mut_ptr()).pts = (*packet_box.packet.as_ptr()).pts;
        (*frame.as_mut_ptr()).time_base = (*packet_box.packet.as_ptr()).time_base;
        (*frame.as_mut_ptr()).opaque = PacketOpaque::PktOpaqueSubHeartbeat as i32 as *mut c_void;

        let frame_box = dec_frame_to_box(dp_arc, frame);

        let result = dec_send(frame_box, frame_pool, senders);
        packet_pool.release(packet_box.packet);
        return match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if e == Error::EOF {
                    Err(Error::Exit)
                } else {
                    Err(e)
                }
            }
        };
    } else if !packet_is_null(&packet_box.packet)
        && (*packet_box.packet.as_ptr()).stream_index >= 0
        && (*packet_box.packet.as_ptr()).opaque as usize as i32
            == PacketOpaque::PktOpaqueFixSubDuration as i32
    {
        //TODO
        let _ret = fix_sub_duration_heartbeat(
            dp_arc,
            av_rescale_q(
                (*packet_box.packet.as_ptr()).pts,
                (*packet_box.packet.as_ptr()).time_base,
                AV_TIME_BASE_Q,
            ),
        );
        packet_pool.release(packet_box.packet);
        // return ret;
        return Ok(());
    }

    // Only EOF/flush sentinels (null packet or the demuxer's stream_index < 0
    // flush marker) are replaced with an empty packet to drain the decoder;
    // real packets must be decoded as-is (matches ffmpeg_dec.c's `!pkt`).
    let mut packet_is_eof = false;
    if packet_is_null(&packet_box.packet) || (*packet_box.packet.as_ptr()).stream_index < 0 {
        let Ok(packet) = packet_pool.get() else {
            return Err(Decoding(DecodingOperationError::PacketAllocationError(
                DecodingError::OutOfMemory,
            )));
        };
        packet_box.packet = packet;
        packet_is_eof = true;
    };

    let dp = dp_arc.clone();
    let mut dp = dp.lock().unwrap();
    // `raw::Subtitle` owns the decoded AVSubtitle: avcodec_decode_subtitle2 fills
    // its rects, and Drop frees them exactly once on every path below — the
    // frame-pool-exhaustion and wrap-failure early returns included — so there is
    // no manual avsubtitle_free and no `?`/early-return leak.
    let mut subtitle = crate::raw::Subtitle::zeroed();
    let mut got_output: libc::c_int = 0;
    let ret = avcodec_decode_subtitle2(
        dp.dec_ctx.as_mut_ptr(),
        subtitle.as_mut_ptr(),
        &mut got_output,
        packet_box.packet.as_mut_ptr(),
    );
    packet_pool.release(packet_box.packet);
    if ret < 0 {
        error!("Error decoding subtitles: {}", av_err2str(ret));
        dp.dec.decode_errors += 1;
        return if exit_on_error {
            Err(Decoding(DecodeSubtitleError(DecodingError::from(ret))))
        } else {
            Ok(())
        };
    }

    if got_output == 0 {
        return if !packet_is_eof {
            Ok(())
        } else {
            Err(Error::EOF)
        };
    }

    dp.dec.frames_decoded += 1;

    let Ok(mut frame) = frame_pool.get() else {
        return Err(Decoding(DecodingOperationError::FrameAllocationError(
            DecodingError::OutOfMemory,
        )));
    };
    // XXX the queue for transferring data to consumers runs
    // on AVFrames, so we wrap AVSubtitle in an AVBufferRef and put that
    // inside the frame
    // eventually, subtitles should be switched to use AVFrames natively
    // On success (copy = false) subtitle_wrap_frame moves the contents into the
    // frame buffer and zeroes our struct, so this owner's later Drop is a no-op;
    // on failure `?` returns and Drop frees the still-owned rects.
    subtitle_wrap_frame(frame.as_mut_ptr(), subtitle.as_mut_ptr(), false)?;

    (*frame.as_mut_ptr()).width = (*dp.dec_ctx.as_ptr()).width;
    (*frame.as_mut_ptr()).height = (*dp.dec_ctx.as_ptr()).height;
    std::mem::drop(dp);

    process_subtitle(dp_arc, frame, frame_pool, senders)
}

unsafe fn process_subtitle(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    frame: Frame,
    frame_pool: &ObjPool<Frame>,
    senders: &Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
) -> crate::error::Result<()> {
    let frame_ptr = frame.as_ptr();
    if (*frame_ptr).buf[0].is_null() {
        return Ok(());
    }

    let subtitle = (*(*frame_ptr).buf[0]).data as *mut AVSubtitle;

    //TODO
    // if (dp->flags & DECODER_FLAG_FIX_SUB_DURATION)

    if subtitle.is_null() {
        return Ok(());
    }

    let frame_box = dec_frame_to_box(dp_arc, frame);

    match dec_send(frame_box, frame_pool, senders) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e == Error::EOF {
                Err(Error::Exit)
            } else {
                Err(e)
            }
        }
    }
}

#[cfg(not(docsrs))]
fn subtitle_wrap_frame(
    frame: *mut AVFrame,
    subtitle: *mut AVSubtitle,
    copy: bool,
) -> crate::error::Result<()> {
    unsafe {
        let mut sub: *mut AVSubtitle;

        if copy {
            sub = av_mallocz(size_of::<AVSubtitle>()) as *mut AVSubtitle;
            if sub.is_null() {
                return Err(Decoding(DecodingOperationError::SubtitleAllocationError(
                    DecodingError::OutOfMemory,
                )));
            }

            let ret = copy_av_subtitle(sub, subtitle);
            if ret < 0 {
                av_freep(&mut sub as *mut _ as *mut c_void);
                return Err(Decoding(DecodingOperationError::CopySubtitleError(
                    DecodingError::from(ret),
                )));
            }
        } else {
            sub = av_memdup(subtitle as *const c_void, std::mem::size_of::<AVSubtitle>())
                as *mut AVSubtitle;
            if sub.is_null() {
                return Err(Decoding(DecodingOperationError::SubtitleAllocationError(
                    DecodingError::OutOfMemory,
                )));
            }
            std::ptr::write(subtitle, std::mem::zeroed()); // Clear the source subtitle
        }

        let buf = av_buffer_create(
            sub as *mut u8,
            std::mem::size_of::<AVSubtitle>(),
            Some(subtitle_free),
            null_mut(),
            0,
        );

        if buf.is_null() {
            avsubtitle_free(sub);
            av_freep(&mut sub as *mut _ as *mut c_void);
            return Err(Decoding(DecodingOperationError::SubtitleAllocationError(
                DecodingError::OutOfMemory,
            )));
        }

        (*frame).buf[0] = buf;

        Ok(())
    }
}

unsafe extern "C" fn subtitle_free(_: *mut c_void, data: *mut u8) {
    let sub = data as *mut AVSubtitle;
    avsubtitle_free(sub);
    av_free(sub as *mut c_void);
}

unsafe fn copy_av_subtitle(dst: *mut AVSubtitle, src: *const AVSubtitle) -> i32 {
    let mut tmp = AVSubtitle {
        format: (*src).format,
        start_display_time: (*src).start_display_time,
        end_display_time: (*src).end_display_time,
        num_rects: 0,
        rects: null_mut(),
        pts: (*src).pts,
    };

    if (*src).num_rects == 0 {
        *dst = tmp;
        return 0; // Success
    }

    tmp.rects = av_calloc(
        (*src).num_rects as usize,
        std::mem::size_of::<*mut AVSubtitleRect>(),
    ) as *mut *mut AVSubtitleRect;
    if tmp.rects.is_null() {
        return AVERROR(ENOMEM);
    }

    for i in 0..(*src).num_rects as usize {
        let src_rect = *(*src).rects.add(i);
        let dst_rect: *mut AVSubtitleRect =
            av_mallocz(std::mem::size_of::<AVSubtitleRect>()) as *mut AVSubtitleRect;
        if dst_rect.is_null() {
            avsubtitle_free(&mut tmp);
            return AVERROR(ENOMEM);
        }

        tmp.rects.add(i).write(dst_rect);
        (*dst_rect).type_ = (*src_rect).type_;
        (*dst_rect).flags = (*src_rect).flags;
        (*dst_rect).x = (*src_rect).x;
        (*dst_rect).y = (*src_rect).y;
        (*dst_rect).w = (*src_rect).w;
        (*dst_rect).h = (*src_rect).h;
        (*dst_rect).nb_colors = (*src_rect).nb_colors;

        // Deep copy text
        if !(*src_rect).text.is_null() {
            (*dst_rect).text = av_strdup((*src_rect).text);
            if (*dst_rect).text.is_null() {
                avsubtitle_free(&mut tmp);
                return AVERROR(ENOMEM);
            }
        }

        // Deep copy ASS
        if !(*src_rect).ass.is_null() {
            (*dst_rect).ass = av_strdup((*src_rect).ass);
            if (*dst_rect).ass.is_null() {
                avsubtitle_free(&mut tmp);
                return AVERROR(ENOMEM);
            }
        }

        // Deep copy data
        for j in 0..4 {
            let buf_size = if (*src_rect).type_ == SUBTITLE_BITMAP && j == 1 {
                AVPALETTE_SIZE
            } else {
                (*src_rect).h * (*src_rect).linesize[j as usize]
            };

            if !(*src_rect).data[j as usize].is_null() {
                (*dst_rect).data[j as usize] = av_memdup(
                    (*src_rect).data[j as usize] as *const c_void,
                    buf_size as usize,
                ) as *mut u8;
                if (*dst_rect).data[j as usize].is_null() {
                    avsubtitle_free(&mut tmp);
                    return AVERROR(ENOMEM);
                }
                (*dst_rect).linesize[j as usize] = (*src_rect).linesize[j as usize];
            }
        }

        tmp.num_rects += 1;
    }

    *dst = tmp;
    0 // Success
}

fn fix_sub_duration_heartbeat(_dp_arc: Arc<Mutex<DecoderParameter>>, _signal_pts: i64) -> i32 {
    0
}

unsafe fn dec_send(
    mut frame_box: FrameBox,
    frame_pool: &ObjPool<Frame>,
    senders: &Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
) -> crate::error::Result<()> {
    let mut nb_done = 0;
    for (i, (sender, fg_input_index, finished_flag_list)) in senders.iter().enumerate() {
        if !finished_flag_list.is_empty()
            && *fg_input_index < finished_flag_list.len()
            && finished_flag_list[*fg_input_index].load(Ordering::Acquire)
        {
            nb_done += 1;
            continue;
        }
        if i < senders.len() - 1 {
            let Ok(mut to_send) = frame_pool.get() else {
                return Err(Decoding(DecodingOperationError::FrameAllocationError(
                    DecodingError::OutOfMemory,
                )));
            };

            let mut frame_data = frame_box.frame_data.clone();
            frame_data.fg_input_index = *fg_input_index;

            // frame may sometimes contain props only,
            // e.g. to signal EOF timestamp
            if !(*frame_box.frame.as_ptr()).buf[0].is_null() {
                let ret = av_frame_ref(to_send.as_mut_ptr(), frame_box.frame.as_ptr());
                if ret < 0 {
                    return Err(Decoding(DecodingOperationError::FrameRefError(
                        DecodingError::OutOfMemory,
                    )));
                }
            } else {
                let ret = av_frame_copy_props(to_send.as_mut_ptr(), frame_box.frame.as_ptr());
                if ret < 0 {
                    return Err(Decoding(DecodingOperationError::FrameCopyPropsError(
                        DecodingError::OutOfMemory,
                    )));
                }
            };

            let frame_box = FrameBox {
                frame: to_send,
                frame_data,
            };
            if let Err(_) = sender.send(frame_box) {
                debug!("Decoder send frame failed, destination already finished");
                nb_done += 1;
                continue;
            }
        } else {
            frame_box.frame_data.fg_input_index = *fg_input_index;

            if let Err(_) = sender.send(frame_box) {
                debug!("Decoder send frame failed, destination already finished");
                nb_done += 1;
            }
            break;
        }
    }

    if nb_done == senders.len() {
        Err(Error::EOF)
    } else {
        Ok(())
    }
}

unsafe fn dec_frame_to_box(dp_arc: Arc<Mutex<DecoderParameter>>, frame: Frame) -> FrameBox {
    let dp = dp_arc.lock().unwrap();
    let dec_ctx = dp.dec_ctx.as_ptr();

    FrameBox {
        frame,
        frame_data: FrameData {
            framerate: Some((*dec_ctx).framerate),
            bits_per_raw_sample: (*dec_ctx).bits_per_raw_sample,
            input_stream_width: (*dec_ctx).width,
            input_stream_height: (*dec_ctx).height,
            subtitle_header: dp.dec.subtitle_header.clone(),
            fg_input_index: usize::MAX,
            side_data: None,
        },
    }
}

#[repr(i32)]
enum PacketOpaque {
    PktOpaqueSubHeartbeat = 1,
    PktOpaqueFixSubDuration,
}

#[cfg(docsrs)]
fn dec_open(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    dec_stream: &DecoderStream,
    param_out: *mut AVFrame,
) -> crate::error::Result<()> {
    Ok(())
}

/// Builds the decoder options dict from the user's per-media opts
/// (`Input::set_video_codec_opt` and friends, threaded through
/// `DecoderStream::codec_opts`).
///
/// ez-ffmpeg has always opened decoders with `threads=auto`; keep that default
/// by injecting it when — and only when — the user did not provide their own
/// `threads` value (`AV_DICT_DONT_OVERWRITE` semantics). Returns the guard
/// owning the dict plus whether `threads` was injected internally, so the
/// caller can avoid reporting the injected entry as an unrecognized user
/// option.
#[cfg(not(docsrs))]
fn build_decoder_opts(codec_opts: &Option<HashMap<CString, CString>>) -> (DictGuard, bool) {
    let mut dec_opts = DictGuard::new(hashmap_to_avdictionary(codec_opts));
    let user_set_threads = codec_opts
        .as_ref()
        .is_some_and(|opts| opts.keys().any(|key| key.as_bytes() == b"threads"));
    if !user_set_threads {
        // Both literals are NUL-free; unwrap cannot fail.
        let key = CString::new("threads").unwrap();
        let value = CString::new("auto").unwrap();
        // SAFETY: the guard owns a valid (possibly null) dict pointer that
        // av_dict_set allocates or extends in place.
        unsafe {
            av_dict_set(
                dec_opts.as_double_ptr(),
                key.as_ptr(),
                value.as_ptr(),
                ffmpeg_sys_next::AV_DICT_DONT_OVERWRITE,
            );
        }
    }
    (dec_opts, !user_set_threads)
}

/// Reclaims the `Arc<Mutex<DecoderParameter>>` refcount stashed in an
/// `AVCodecContext.opaque` if `dec_open` fails after installing it.
///
/// The `opaque` back-reference (an `Arc::into_raw`) is installed **before**
/// `avcodec_open2`, because some hardware decoders call `get_format` during
/// open and that callback both needs a live `opaque` and records
/// `hwaccel_pix_fmt` as a side effect. If any step after the install fails and
/// returns early, that refcount would otherwise leak (the normal reclaim,
/// `drop_opaque_ptr`, runs only once the context reaches its long-lived owner).
/// This guard reclaims it via `Arc::from_raw` on drop; a successful handoff
/// calls [`disarm`](Self::disarm) so the live decoder owns the reference and
/// `drop_opaque_ptr` releases it at shutdown instead.
#[cfg(not(docsrs))]
struct OpaqueGuard {
    dec_ctx: *mut AVCodecContext,
}

#[cfg(not(docsrs))]
impl OpaqueGuard {
    /// Give up responsibility for the `opaque` Arc: the context handed off
    /// successfully and the live decoder now owns the refcount.
    fn disarm(mut self) {
        self.dec_ctx = null_mut();
    }
}

#[cfg(not(docsrs))]
impl Drop for OpaqueGuard {
    fn drop(&mut self) {
        if self.dec_ctx.is_null() {
            return;
        }
        // SAFETY: armed only after `(*dec_ctx).opaque = Arc::into_raw(...)`, so
        // `opaque` is a live `Arc<Mutex<DecoderParameter>>` pointer here. Reclaim
        // that one refcount exactly once (mirrors `drop_opaque_ptr`), then null
        // the field so nothing else treats it as live.
        unsafe {
            let dp_ptr = (*self.dec_ctx).opaque as *const Mutex<DecoderParameter>;
            if !dp_ptr.is_null() {
                let _ = Arc::from_raw(dp_ptr);
                (*self.dec_ctx).opaque = null_mut();
            }
        }
    }
}

#[cfg(not(docsrs))]
fn dec_open(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    dec_stream: &DecoderStream,
    param_out: *mut AVFrame,
) -> crate::error::Result<()> {
    unsafe {
        let dec_ctx = avcodec_alloc_context3(dec_stream.codec.as_ptr());
        if dec_ctx.is_null() {
            return Err(OpenDecoder(
                OpenDecoderOperationError::ContextAllocationError(OpenDecoderError::OutOfMemory),
            ));
        }
        // Take ownership of the freshly-allocated context immediately. `dec_ctx`
        // stays as a raw alias for FFI reads/calls below, but every early return
        // from here on frees the context via this owner's `Drop` — no manual
        // `avcodec_free_context` on the fallible paths, and no leak if a step
        // past `avcodec_open2` (e.g. the channel-layout copy) fails.
        let dec_ctx_owner = CodecContext::new(dec_ctx);

        // Per-input decoder log demotion (Input::set_log_level_offset).
        (*dec_ctx).log_level_offset = dec_stream.log_level_offset;

        let mut ret = avcodec_parameters_to_context(dec_ctx, (*dec_stream.stream.inner).codecpar);
        if ret < 0 {
            error!("Error initializing the decoder context.");
            return Err(OpenDecoder(
                OpenDecoderOperationError::ParameterApplicationError(OpenDecoderError::from(ret)),
            ));
        }

        (*dec_ctx).pkt_timebase = dec_stream.time_base;

        // Install the decode-time callbacks and the decoder-parameter
        // back-reference BEFORE `avcodec_open2`: some hardware decoders invoke
        // `get_format` during open, and that callback needs a live `opaque` and
        // records `hwaccel_pix_fmt` as a side effect used later by the frame
        // path — deferring it past open would silently drop that side effect.
        //
        // SAFETY: `Arc::into_raw` preserves the refcount; `opaque` stays a live
        // pointer until reclaimed exactly once. On success that reclaim is
        // `drop_opaque_ptr` at decoder shutdown; on any early return below it is
        // `opaque_guard`'s Drop (armed on the next line). `get_format` clones and
        // restores the Arc to keep the count balanced.
        (*dec_ctx).get_format = Some(get_format_callback);
        (*dec_ctx).get_buffer2 = Some(get_buffer_callback);
        let dp_ptr = Arc::into_raw(dp_arc.clone());
        (*dec_ctx).opaque = dp_ptr as *mut libc::c_void;
        let opaque_guard = OpaqueGuard { dec_ctx };

        {
            let dp_arc_clone = dp_arc.clone();
            let mut dp = dp_arc_clone.lock().unwrap();
            ret = hw_device_setup_for_decode(&mut dp, dec_stream.codec.as_ptr(), dec_ctx);
            if ret < 0 {
                error!(
                    "Hardware device setup failed for decoder: {}",
                    av_err2str(ret)
                );
                return Err(OpenDecoder(OpenDecoderOperationError::HwSetupError(
                    OpenDecoderError::from(ret),
                )));
            }
        }

        // Build the options dict only after hw setup (so the failure path
        // above can never leak it). The guard owns the dict on every path:
        // `av_opt_set_dict2` consumes the entries it applies and leaves the
        // leftovers behind, which the guard frees on drop.
        let (mut dec_opts, injected_threads) = build_decoder_opts(&dec_stream.codec_opts);
        ret = av_opt_set_dict2(
            dec_ctx as *mut c_void,
            dec_opts.as_double_ptr(),
            ffmpeg_sys_next::AV_OPT_SEARCH_CHILDREN,
        );
        if ret < 0 {
            error!("Error applying decoder options: {}", av_err2str(ret));
            return Err(OpenDecoder(
                OpenDecoderOperationError::ParameterApplicationError(OpenDecoderError::from(ret)),
            ));
        }
        // The internally injected `threads` default must not be blamed on the
        // user. Entries no decoder option matched are user typos (fftools
        // check_avoptions errors out; we surface them as warnings, mirroring
        // the encoder path in enc_task::set_encoder_opts).
        if injected_threads {
            // The literal is NUL-free; unwrap cannot fail.
            dec_opts.remove(&CString::new("threads").unwrap());
        }
        for key in dec_opts.leftover_keys() {
            warn!(
                "Option '{key}' was not recognized by decoder for stream {}",
                dec_stream.stream_index
            );
        }

        (*dec_ctx).flags |= AV_CODEC_FLAG_COPY_OPAQUE as i32;
        // we apply cropping outselves
        {
            let dp_arc_clone = dp_arc.clone();
            let mut dp = dp_arc_clone.lock().unwrap();
            dp.apply_cropping = (*dec_ctx).apply_cropping;
        }
        (*dec_ctx).apply_cropping = 0;

        ret = avcodec_open2(dec_ctx, dec_stream.codec.as_ptr(), null_mut());
        if ret < 0 {
            error!("Error while opening decoder: {}", av_err2str(ret));
            return Err(OpenDecoder(OpenDecoderOperationError::DecoderOpenError(
                OpenDecoderError::from(ret),
            )));
        }

        if !(*dec_ctx).hw_device_ctx.is_null() {
            // Update decoder extra_hw_frames option to account for the
            // frames held in queues inside the ffmpeg utility.  This is
            // called after avcodec_open2() because the user-set value of
            // extra_hw_frames becomes valid in there, and we need to add
            // this on top of it.

            // DEFAULT_FRAME_THREAD_QUEUE_SIZE = 8
            let extra_frames = 8;
            if (*dec_ctx).extra_hw_frames >= 0 {
                (*dec_ctx).extra_hw_frames += extra_frames;
            } else {
                (*dec_ctx).extra_hw_frames = extra_frames;
            }
        }

        {
            let dp_arc_clone = dp_arc.clone();
            let mut dp = dp_arc_clone.lock().unwrap();
            // Own a copy of the subtitle header: dec_ctx (and the buffer it
            // points to) is freed when the decoder exits, while encoders read
            // the header later (matches ffmpeg_dec.c owning its own copy).
            dp.dec.subtitle_header =
                if (*dec_ctx).subtitle_header.is_null() || (*dec_ctx).subtitle_header_size <= 0 {
                    None
                } else {
                    Some(Arc::from(std::slice::from_raw_parts(
                        (*dec_ctx).subtitle_header as *const u8,
                        (*dec_ctx).subtitle_header_size as usize,
                    )))
                };
        }

        if !param_out.is_null() {
            if (*dec_ctx).codec_type == AVMEDIA_TYPE_AUDIO {
                (*param_out).format = (*dec_ctx).sample_fmt as i32;
                (*param_out).sample_rate = (*dec_ctx).sample_rate;

                ret = av_channel_layout_copy(&mut (*param_out).ch_layout, &(*dec_ctx).ch_layout);
                if ret < 0 {
                    return Err(OpenDecoder(
                        OpenDecoderOperationError::ChannelLayoutCopyError(OpenDecoderError::from(
                            ret,
                        )),
                    ));
                }
            } else if (*dec_ctx).codec_type == AVMEDIA_TYPE_VIDEO {
                (*param_out).format = (*dec_ctx).pix_fmt as i32;
                (*param_out).width = (*dec_ctx).width;
                (*param_out).height = (*dec_ctx).height;
                (*param_out).sample_aspect_ratio = (*dec_ctx).sample_aspect_ratio;
                (*param_out).colorspace = (*dec_ctx).colorspace;
                (*param_out).color_range = (*dec_ctx).color_range;
            }

            (*param_out).time_base = (*dec_ctx).pkt_timebase;
        }

        // All fallible setup has succeeded. Hand the context to its long-lived
        // owner first, then give up the guard: from here the live decoder owns
        // the `opaque` Arc refcount, released exactly once by `drop_opaque_ptr()`
        // at shutdown. `dec_ctx_owner` is MOVED into `dp.dec_ctx` (the prior null
        // owner there drops as a no-op), so the context is freed exactly once.
        // Parking the owner before `disarm` keeps a single custodian of the
        // context across the hand-off even if a panic were to intervene.
        dp_arc.lock().unwrap().dec_ctx = dec_ctx_owner;
        opaque_guard.disarm();
    }

    Ok(())
}

fn hw_device_setup_for_decode(
    dp: &mut MutexGuard<DecoderParameter>,
    codec: *const AVCodec,
    dec_ctx: *mut AVCodecContext,
) -> i32 {
    let mut dev = None;
    let mut err = 0;
    let mut auto_device = false;
    let mut device_type = AVHWDeviceType::AV_HWDEVICE_TYPE_NONE;

    let hwaccel_device = dp.hwaccel_device.clone();
    if let Some(hwaccel_device) = &hwaccel_device {
        dev = hw_device_get_by_name(hwaccel_device);
        match &dev {
            None => {
                if dp.hwaccel_id == HWAccelID::HwaccelAuto {
                    auto_device = true;
                } else if dp.hwaccel_id == HWAccelID::HwaccelGeneric {
                    device_type = dp.hwaccel_device_type;
                    (err, dev) =
                        hw_device_init_from_type(device_type, Some(hwaccel_device.clone()));
                } else {
                    // This will be dealt with by API-specific initialisation
                    // (using hwaccel_device), so nothing further needed here.
                    return 0;
                }
            }
            Some(dev) => {
                if dp.hwaccel_id == HWAccelID::HwaccelAuto {
                    dp.hwaccel_device_type = dev.device_type;
                } else if dp.hwaccel_device_type != dev.device_type {
                    unsafe {
                        let dev_device_name = av_hwdevice_get_type_name(dev.device_type);
                        let dev_device_name = CStr::from_ptr(dev_device_name).to_str();
                        let dp_device_name = av_hwdevice_get_type_name(dp.hwaccel_device_type);
                        let dp_device_name = CStr::from_ptr(dp_device_name).to_str();
                        if let (Ok(dev_device_name), Ok(dp_device_name)) =
                            (dev_device_name, dp_device_name)
                        {
                            error!("Invalid hwaccel device specified for decoder: device {} of type {} is not usable with hwaccel {}.",
                        dev.name, dp_device_name, dev_device_name);
                        }
                    }

                    return AVERROR(EINVAL);
                }
            }
        }
    } else if dp.hwaccel_id == HWAccelID::HwaccelAuto {
        auto_device = true;
    } else if dp.hwaccel_id == HWAccelID::HwaccelGeneric {
        device_type = dp.hwaccel_device_type;
        dev = hw_device_get_by_type(device_type);

        // When "-qsv_device device" is used, an internal QSV device named
        // as "__qsv_device" is created. Another QSV device is created too
        // if "-init_hw_device qsv=name:device" is used. There are 2 QSV devices
        // if both "-qsv_device device" and "-init_hw_device qsv=name:device"
        // are used, hw_device_get_by_type(AV_HWDEVICE_TYPE_QSV) returns NULL.
        // To keep back-compatibility with the removed ad-hoc libmfx setup code,
        // call hw_device_get_by_name("__qsv_device") to select the internal QSV
        // device.
        if dev.is_none() && device_type == AV_HWDEVICE_TYPE_QSV {
            dev = hw_device_get_by_name("__qsv_device");
        }

        if dev.is_none() {
            (err, dev) = hw_device_init_from_type(device_type, None);
        };
    } else {
        dev = hw_device_match_by_codec(codec);
        if dev.is_none() {
            // No device for this codec, but not using generic hwaccel
            // and therefore may well not need one - ignore.
            return 0;
        }
    }

    if auto_device {
        if unsafe { avcodec_get_hw_config(codec, 0).is_null() } {
            // Decoder does not support any hardware devices.
            return 0;
        }

        let mut i = 0;
        loop {
            let config = unsafe { avcodec_get_hw_config(codec, i) };
            if config.is_null() {
                break;
            }

            device_type = unsafe { (*config).device_type };
            dev = hw_device_get_by_type(device_type);
            if let Some(dev) = &dev {
                unsafe {
                    let dev_device_type = av_hwdevice_get_type_name(device_type);
                    if let Ok(dev_device_type) = CStr::from_ptr(dev_device_type).to_str() {
                        info!(
                            "Using auto hwaccel type {dev_device_type} with existing device {}.",
                            dev.name
                        );
                    }
                }
                break;
            }

            i += 1;
        }

        // Only try creating a new device when no existing one matched;
        // a failed creation attempt must not clobber a found device
        // (ffmpeg_dec.c hw auto: `for (i = 0; !dev; i++)`).
        i = 0;
        while dev.is_none() {
            let config = unsafe { avcodec_get_hw_config(codec, i) };
            if config.is_null() {
                break;
            }

            device_type = unsafe { (*config).device_type };
            // Try to make a new device of this type.
            (err, dev) = hw_device_init_from_type(device_type, dp.hwaccel_device.clone());
            if err < 0 {
                // Can't make a device of this type.
                i += 1;
                continue;
            }

            unsafe {
                let dev_device_type = av_hwdevice_get_type_name(device_type);
                if let Ok(dev_device_type) = CStr::from_ptr(dev_device_type).to_str() {
                    match &dp.hwaccel_device {
                        Some(hwaccel_device) => {
                            info!("Using auto hwaccel type {dev_device_type} with new device created from {hwaccel_device}.");
                        }
                        None => {
                            info!("Using auto hwaccel type {dev_device_type} with new default device.");
                        }
                    }
                }
            }
            break;
        }
        if dev.is_some() {
            dp.hwaccel_device_type = device_type;
        } else {
            info!("Auto hwaccel disabled: no device found.");
            dp.hwaccel_id = HWAccelID::HwaccelNone;
            return 0;
        }
    }

    match dev {
        None => {
            unsafe {
                let dev_device_type = av_hwdevice_get_type_name(device_type);
                let dev_device_type = CStr::from_ptr(dev_device_type).to_str();
                let codec_name = (*codec).name;
                let codec_name = CStr::from_ptr(codec_name).to_str();
                if let (Ok(dev_device_type), Ok(codec_name)) = (dev_device_type, codec_name) {
                    info!("No device available for decoder: device type {dev_device_type} needed for codec {codec_name}.");
                }
            }
            err
        }
        Some(dev) => unsafe {
            (*dec_ctx).hw_device_ctx = av_buffer_ref(dev.device_ref());
            if (*dec_ctx).hw_device_ctx.is_null() {
                return AVERROR(ENOMEM);
            }
            0
        },
    }
}

unsafe extern "C" fn get_format_callback(
    s: *mut AVCodecContext,
    pix_fmts: *const AVPixelFormat,
) -> AVPixelFormat {
    // Everything — including the trace! for the null-argument case, which
    // runs a user-installed log hook that can panic — must stay inside a
    // catch_unwind: unwinding across this extern "C" boundary is undefined
    // behavior.
    if s.is_null() || pix_fmts.is_null() || (*s).opaque.is_null() {
        let _ = std::panic::catch_unwind(|| trace!("get pixel format: none"));
        return AVPixelFormat::AV_PIX_FMT_NONE;
    }

    // SAFETY: Retrieve Arc<Mutex<DecoderParameter>> from FFmpeg's opaque field.
    // This callback is invoked synchronously by FFmpeg from the decoder thread.
    // The Arc lifecycle is managed as follows:
    // 1. Arc::from_raw reconstructs the Arc from the raw pointer (takes ownership)
    // 2. Arc::clone increments the reference count
    // 3. Arc::into_raw stores one reference back (does not decrement)
    // 4. The cloned Arc (dp_arc) is used in this function and dropped at scope end
    // Net effect: reference count unchanged, pointer remains valid for future callbacks
    let dp_ptr = (*s).opaque as *const Mutex<DecoderParameter>;
    let dp_arc = Arc::from_raw(dp_ptr);
    let dp_ptr = Arc::into_raw(dp_arc.clone());
    (*s).opaque = dp_ptr as *mut libc::c_void;

    // This callback runs across the extern "C" boundary: a panic unwinding out
    // of it is undefined behavior. The scan below can panic through the log
    // hook (a user-installed logger backs trace!), so contain it and report
    // "no usable format" instead. The mutex recovers from poisoning rather
    // than panicking — a worker that died elsewhere must not cascade here.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut dp = dp_arc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut i = 0;
        while *pix_fmts.add(i) != AVPixelFormat::AV_PIX_FMT_NONE {
            let mut config = null();
            let desc = av_pix_fmt_desc_get(*pix_fmts.add(i));

            if desc.is_null() || (*desc).flags & AV_PIX_FMT_FLAG_HWACCEL as u64 == 0 {
                break;
            }
            if dp.hwaccel_id == HwaccelGeneric || dp.hwaccel_id == HwaccelAuto {
                let mut j = 0;
                loop {
                    config = avcodec_get_hw_config((*s).codec, j);
                    if config.is_null() {
                        break;
                    }
                    if (*config).methods as u32 & AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX as u32
                        == 0
                    {
                        j += 1;
                        continue;
                    }
                    if (*config).pix_fmt == *pix_fmts.add(i) {
                        break;
                    }
                    j += 1;
                }
            }

            if !config.is_null() && (*config).device_type == dp.hwaccel_device_type {
                dp.hwaccel_pix_fmt = *pix_fmts.add(i);
                break;
            }

            i += 1;
        }

        let format = *pix_fmts.add(i);
        trace!("get pixel format: {:?}", format);

        format
    }));

    match result {
        Ok(format) => format,
        Err(_) => AVPixelFormat::AV_PIX_FMT_NONE,
    }
}

unsafe extern "C" fn get_buffer_callback(
    dec_ctx: *mut AVCodecContext,
    frame: *mut AVFrame,
    flags: libc::c_int,
) -> libc::c_int {
    if dec_ctx.is_null() || frame.is_null() {
        return AVERROR(EINVAL);
    }

    /*let dp_ptr = (*dec_ctx).opaque as *const Arc<Mutex<DecoderParameter>>;
    let dp_arc = Arc::clone(&*dp_ptr);

    let mut dp = dp_arc.lock().unwrap();*/

    // for multiview video, store the output mask in frame opaque
    /*if dp.view_map.len() > 0 {
        let sd = av_frame_get_side_data(frame, AV_FRAME_DATA_VIEW_ID);
        let view_id = if !sd.is_null() {
            *(sd.as_ref().unwrap().data as *const i32)
        } else {
            0
        };

        for i in 0..dp.view_map.len() {
            if dp.view_map[i].id == view_id {
                (*frame).opaque = dp.view_map[i].out_mask as *mut c_void;
                break;
            }
        }
    }*/

    avcodec_default_get_buffer2(dec_ctx, frame, flags)
}

struct DecoderParameter {
    dec: Decoder,

    dec_ctx: CodecContext,
    // dec_ctx: *mut AVCodecContext,

    // override output video sample aspect ratio with this value
    sar_override: AVRational,
    framerate_in: AVRational,
    framerate_forced: bool,

    apply_cropping: i32,

    hwaccel_id: HWAccelID,
    hwaccel_device_type: AVHWDeviceType,
    hwaccel_device: Option<String>,
    hwaccel_output_format: AVPixelFormat,
    hwaccel_pix_fmt: AVPixelFormat,

    // pts/estimated duration of the last decoded frame
    // * in decoder timebase for video,
    // * in last_frame_tb (may change during decoding) for audio
    last_frame_pts: i64,
    last_frame_duration_est: i64,
    last_frame_tb: AVRational,
    last_filter_in_rescale_delta: i64,
    last_frame_sample_rate: i32,
    // view_map: Vec<ViewMap>,
}

// SAFETY: DecoderParameter contains a raw pointer (dec_ctx) but is safe to
// Send/Sync because:
// 1. The decoder thread has exclusive ownership of the AVCodecContext during decoding
// 2. DecoderParameter is wrapped in Arc<Mutex<>> ensuring synchronized access
// 3. The scheduler architecture guarantees that FFmpeg contexts are only accessed from
//    their owning thread, with data passed via crossbeam channels
// 4. Raw pointers are only dereferenced within the decoder thread or FFmpeg callbacks
//    which are invoked synchronously from the decoder thread
unsafe impl Send for DecoderParameter {}

/*struct ViewMap {
    id: i32,
    out_mask: i32,
}*/

impl DecoderParameter {
    fn drop_opaque_ptr(&self) {
        let dec_ctx = self.dec_ctx.as_ptr();
        if !dec_ctx.is_null() {
            unsafe {
                let dec_ctx = dec_ctx as *mut ffmpeg_sys_next::AVCodecContext;
                let dp_ptr = (*dec_ctx).opaque as *const Mutex<DecoderParameter>;
                if !dp_ptr.is_null() {
                    let _dp_arc = Arc::from_raw(dp_ptr);
                    // Null it out so the reclaim is idempotent: the worker's
                    // RAII guard and any failure-path caller must never turn
                    // a second call into a refcount underflow.
                    (*dec_ctx).opaque = null_mut();
                }
            }
        }
    }

    fn new(dec_stream: &mut DecoderStream) -> Self {
        Self {
            dec: Decoder {
                media_type: dec_stream.codec_type,
                subtitle_header: None,
                frames_decoded: 0,
                samples_decoded: 0,
                decode_errors: 0,
            },
            dec_ctx: CodecContext::null(),

            sar_override: unsafe { (*(*dec_stream.stream.inner).codecpar).sample_aspect_ratio },
            framerate_in: dec_stream.avg_framerate,
            framerate_forced: dec_stream.framerate_forced,
            apply_cropping: 0,
            hwaccel_id: dec_stream.hwaccel_id,
            hwaccel_device_type: dec_stream.hwaccel_device_type,
            hwaccel_device: dec_stream.hwaccel_device.clone(),
            hwaccel_output_format: dec_stream.hwaccel_output_format,
            hwaccel_pix_fmt: AVPixelFormat::AV_PIX_FMT_NONE,
            last_frame_pts: 0,
            last_frame_duration_est: 0,
            last_frame_tb: AVRational { num: 1, den: 1 },
            last_filter_in_rescale_delta: 0,
            last_frame_sample_rate: 0,
            // view_map: vec![],
        }
    }
}

struct Decoder {
    #[allow(dead_code)]
    media_type: AVMediaType,

    /// Owned copy of dec_ctx.subtitle_header, captured right after
    /// avcodec_open2; outlives the decoder context for downstream encoders.
    subtitle_header: Option<Arc<[u8]>>,

    // number of frames/samples retrieved from the decoder
    frames_decoded: u64,
    samples_decoded: u64,
    decode_errors: u64,
}

fn dec_done(
    dp_arc: &Arc<Mutex<DecoderParameter>>,
    senders: &Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
) {
    for (sender, fg_input_index, finished_flag_list) in senders {
        if !finished_flag_list.is_empty()
            && *fg_input_index < finished_flag_list.len()
            && finished_flag_list[*fg_input_index].load(Ordering::Acquire)
        {
            continue;
        }

        let mut frame_box = unsafe { dec_frame_to_box(dp_arc.clone(), null_frame()) };
        frame_box.frame_data.fg_input_index = *fg_input_index;
        if let Err(_) = sender.send(frame_box) {
            debug!("Decoder send EOF failed, destination already finished");
        }
    }
}

/*const fn fferrtag(a: u8, b: u8, c: u8, d: u8) -> i32 {
    -(((a as i32) & 0xFF) << 24
        | ((b as i32) & 0xFF) << 16
        | ((c as i32) & 0xFF) << 8
        | ((d as i32) & 0xFF))
}

const FFMPEG_ERROR_RATE_EXCEEDED: i32 = fferrtag(b'E', b'R', b'E', b'D');*/

#[repr(i32)]
enum FrameOpaque {
    #[allow(dead_code)]
    FrameOpaqueSubHeartbeat = 1,
    FrameOpaqueEof,
    #[allow(dead_code)]
    FrameOpaqueSendCommand,
}

/// Generous flat floor for the consecutive *no-progress* decode-error ceiling (see
/// [`decode_stall_limit`]). It is the whole ceiling for every decoder except one
/// running FFmpeg's own frame threading, and is set far above any realistic
/// undelivered backlog we cannot otherwise read (an external decoder's reorder plus
/// internal pipeline is at most a few dozen to low hundreds of pictures). It is a
/// fixed constant rather than a per-decoder size because the one field that could
/// size it — `thread_count` — is not trustworthy for an external decoder (see
/// [`decode_stall_limit`]); `AVCodecContext.delay` is unused for the same reason.
#[cfg(not(docsrs))]
const DECODE_STALL_FLOOR: u32 = 1024;

/// Head-room added above `thread_count` for a native frame-thread drain, covering
/// the reorder delay and end-of-drain slack a decoder can hold beyond its raw worker
/// count. Its only cost is that a wedged frame-threaded decoder spins this many extra
/// no-op receives before terminating.
#[cfg(not(docsrs))]
const DECODE_STALL_MARGIN: u32 = 32;

/// Per-decoder ceiling on consecutive no-progress decode errors before the drain
/// declares the decoder wedged. It is a HEURISTIC: its only job is to stop a decoder
/// that returns a persistent error without ever consuming input (e.g. a libjxl or
/// libdav1d build that keeps returning the same error without clearing its buffered
/// data) from looping forever, without mis-firing on one still draining a legitimate
/// burst of errors.
///
/// The ceiling is gated on the *active* threading type:
///   * **Native frame threading** (`active_thread_type & FF_THREAD_FRAME`): FFmpeg
///     runs `thread_count` of its own frame-thread workers, each holding at most one
///     in-flight picture, so a drain can legitimately return up to ~`thread_count`
///     consecutive error results (e.g. a batch of invalid frames flushed at EOF).
///     Here `thread_count` is FFmpeg's real worker count, so the ceiling grows with
///     it: `thread_count + DECODE_STALL_MARGIN`.
///   * **Everything else** (external decoders, slice threading, no threading): the
///     flat [`DECODE_STALL_FLOOR`]. An external decoder does its own threading and
///     may report an *inflated* `thread_count` — larger than the workers it truly
///     runs, up to the requested `INT_MAX` — while `active_thread_type` stays 0, so
///     trusting the raw field there would balloon the ceiling to billions and reduce
///     the guard to the very hang it exists to prevent. The gate excludes that case.
///
/// A negative or zero `thread_count` clamps to the floor; the add is saturating.
#[cfg(not(docsrs))]
fn decode_stall_limit(active_thread_type: i32, thread_count: i32) -> u32 {
    if active_thread_type & FF_THREAD_FRAME != 0 {
        (thread_count.max(0) as u32)
            .saturating_add(DECODE_STALL_MARGIN)
            .max(DECODE_STALL_FLOOR)
    } else {
        DECODE_STALL_FLOOR
    }
}

#[cfg(not(docsrs))]
struct DecodeErrorBudget {
    stalls: u32,
    last_frame_num: i64,
    limit: u32,
}

#[cfg(not(docsrs))]
impl DecodeErrorBudget {
    fn new(frame_num: i64, active_thread_type: i32, thread_count: i32) -> Self {
        Self {
            stalls: 0,
            last_frame_num: frame_num,
            limit: decode_stall_limit(active_thread_type, thread_count),
        }
    }

    /// A successfully decoded frame is progress, so the stall run resets.
    fn on_frame(&mut self, frame_num: i64) {
        self.last_frame_num = frame_num;
        self.stalls = 0;
    }

    /// Record an ignored decode error; returns whether the drain must give up.
    /// `exit_on_error` gives up on the first error. Otherwise an error that still
    /// advanced `frame_num` is progress (resets the run); the drain tolerates
    /// `self.limit` consecutive no-progress errors and gives up on the next one
    /// (matching FFmpeg's `> limit` guard).
    fn on_error(&mut self, exit_on_error: bool, frame_num: i64) -> bool {
        if exit_on_error {
            return true;
        }
        if frame_num != self.last_frame_num {
            self.last_frame_num = frame_num;
            self.stalls = 0;
        } else {
            self.stalls += 1;
        }
        self.stalls > self.limit
    }
}

#[cfg(not(docsrs))]
unsafe fn packet_decode(
    dp_arc: &Arc<Mutex<DecoderParameter>>,
    exit_on_error: bool,
    packet_box: PacketBox,
    packet_pool: &ObjPool<Packet>,
    frame_pool: &ObjPool<Frame>,
    senders: &Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
    scheduler_status: &Arc<AtomicUsize>,
) -> crate::error::Result<()> {
    let dec_ctx = {
        let dp = dp_arc.clone();
        let dp = dp.lock().unwrap();
        dp.dec_ctx.as_mut_ptr()
    };

    if !dec_ctx.is_null() && (*dec_ctx).codec_type == AVMEDIA_TYPE_SUBTITLE {
        return transcode_subtitles(
            dp_arc.clone(),
            exit_on_error,
            packet_box,
            packet_pool,
            frame_pool,
            senders,
        );
    }

    // With fate-indeo3-2, we're getting 0-sized packets before EOF for some
    // reason. This seems like a semi-critical bug. Don't trigger EOF, and
    // skip the packet.
    if !packet_is_null(&packet_box.packet)
        && (*packet_box.packet.as_ptr()).stream_index >= 0
        && packet_box.packet.is_empty()
    {
        return Ok(());
    }

    //TODO DECODER_FLAG_TS_UNRELIABLE

    let mut ret = if (*packet_box.packet.as_ptr()).stream_index < 0 {
        avcodec_send_packet(dec_ctx, null())
    } else {
        avcodec_send_packet(dec_ctx, packet_box.packet.as_ptr())
    };
    if ret < 0 && !(ret == AVERROR_EOF && (*packet_box.packet.as_ptr()).stream_index < 0) {
        // In particular, we don't expect AVERROR(EAGAIN), because we read all
        // decoded frames with avcodec_receive_frame() until done.
        if ret == AVERROR(EAGAIN) {
            error!("A decoder returned an unexpected error code. This is a bug, please report it.");
            packet_pool.release(packet_box.packet);
            return Err(Bug);
        }
        error!(
            "Error submitting {} to decoder: {}",
            if (*packet_box.packet.as_ptr()).stream_index < 0 {
                "EOF"
            } else {
                "packet"
            },
            av_err2str(ret)
        );

        packet_pool.release(packet_box.packet);
        if ret != AVERROR_EOF {
            let dp = dp_arc.clone();
            let mut dp = dp.lock().unwrap();
            dp.dec.decode_errors += 1;
            if !exit_on_error {
                return Ok(());
            };
        }

        return Err(Decoding(DecodingOperationError::SendPacketError(
            DecodingError::from(ret),
        )));
    }

    packet_pool.release(packet_box.packet);

    // Bound consecutive no-progress decode errors within this packet's drain so
    // a decoder stuck returning errors without consuming input cannot spin
    // forever (reset whenever the decoder advances; see DecodeErrorBudget).
    let mut error_budget = DecodeErrorBudget::new(
        (*dec_ctx).frame_num,
        (*dec_ctx).active_thread_type,
        (*dec_ctx).thread_count,
    );

    loop {
        // Bail out of the drain if a stop was requested. The worker's own stop
        // check only runs between packets, so without this a decoder that keeps
        // producing (or erroring on) frames from one packet could not be
        // interrupted mid-drain.
        if is_stopping(scheduler_status.load(Ordering::Acquire)) {
            return Ok(());
        }

        let mut outputs_mask = 1;

        let Ok(mut frame) = frame_pool.get() else {
            return Err(Decoding(DecodingOperationError::FrameAllocationError(
                DecodingError::OutOfMemory,
            )));
        };

        ret = avcodec_receive_frame(dec_ctx, frame.as_mut_ptr());
        if ret == AVERROR(EAGAIN) {
            // Drain done for this packet: the pooled shell was not moved
            // onward, so recycle it instead of letting Drop free it — otherwise
            // every decoded packet drains one shell from the ObjPool and the
            // next get() re-allocates (ffapi-05). Mirrors filter_task's sink.
            frame_pool.release(frame);
            return Ok(());
        } else if ret == AVERROR_EOF {
            frame_pool.release(frame);
            return Err(Error::EOF);
        } else if ret < 0 {
            error!("Decoding error: {}", av_err2str(ret));
            {
                let dp = dp_arc.clone();
                let mut dp = dp.lock().unwrap();
                dp.dec.decode_errors += 1;
            }
            frame_pool.release(frame);

            // `exit_on_error` fails on the first error. Otherwise keep draining:
            // a transient error clears on the next receive, and the drain must
            // still reach EAGAIN/EOF so a frame-threaded decoder's already
            // decoded frames and the EOF-flush reset (dec_init's stream_loop
            // path) are not skipped. An error that still advanced the decoder
            // (e.g. AV_CODEC_FLAG_DROPCHANGED dropping a frame) is progress; only
            // a genuine stall — a decoder that never consumes input on error —
            // is bounded and gives up here rather than spinning forever.
            if error_budget.on_error(exit_on_error, (*dec_ctx).frame_num) {
                return Err(Decoding(DecodingOperationError::ReceiveFrameError(
                    DecodingError::from(ret),
                )));
            }
            continue;
        }

        // A successfully received frame is progress, so the stall budget resets.
        error_budget.on_frame((*dec_ctx).frame_num);

        if (*frame.as_ptr()).decode_error_flags != 0
            || ((*frame.as_ptr()).flags & AV_FRAME_FLAG_CORRUPT != 0)
        {
            if exit_on_error {
                error!("corrupt decoded frame");
                return Err(Decoding(DecodingOperationError::CorruptFrame));
            } else {
                warn!("corrupt decoded frame");
            }
        }

        let mut frame_box = dec_frame_to_box(dp_arc.clone(), frame);
        // fdemux_parameter.dec.pts                 = (*frame).pts;
        // fdemux_parameter.dec.tb                  = dec->pkt_timebase;
        // fdemux_parameter.dec.frame_num           = dec->frame_num - 1;

        (*frame_box.frame.as_mut_ptr()).time_base = (*dec_ctx).pkt_timebase;

        if (*dec_ctx).codec_type == AVMEDIA_TYPE_AUDIO {
            let dp = dp_arc.clone();
            let mut dp = dp.lock().unwrap();
            dp.dec.samples_decoded += (*frame_box.frame.as_ptr()).nb_samples as u64;

            audio_ts_process(dp, frame_box.frame.as_mut_ptr());
        } else if let Err(e) = video_frame_process(
            dp_arc.clone(),
            frame_box.frame.as_mut_ptr(),
            &mut outputs_mask,
            frame_pool,
        ) {
            error!("Error while processing the decoded data");
            return Err(e);
        }

        {
            let dp = dp_arc.clone();
            let mut dp = dp.lock().unwrap();
            dp.dec.frames_decoded += 1;
        }

        if let Err(e) = dec_send(frame_box, frame_pool, senders) {
            return if e == Error::EOF {
                Err(Error::Exit)
            } else {
                Err(e)
            };
        }
    }
}

#[cfg(not(docsrs))]
unsafe fn video_frame_process(
    dp_arc: Arc<Mutex<DecoderParameter>>,
    frame: *mut AVFrame,
    outputs_mask: &mut usize,
    frame_pool: &ObjPool<Frame>,
) -> crate::error::Result<()> {
    let mut dp = dp_arc.lock().unwrap();

    if (*frame).format == dp.hwaccel_pix_fmt as i32 {
        let err = hwaccel_retrieve_data(&dp, frame, frame_pool);
        if err < 0 {
            return Err(Decoding(DecodingOperationError::HWRetrieveDataError(
                DecodingError::from(err),
            )));
        }
    }

    (*frame).pts = (*frame).best_effort_timestamp;

    // forced fixed framerate: drop container timestamps and stamp frames on
    // the CFR grid (ffmpeg_dec.c:398-403); the extrapolation below assigns
    // consecutive pts values in 1/framerate units
    if dp.framerate_forced {
        (*frame).pts = AV_NOPTS_VALUE;
        (*frame).duration = 1;
        (*frame).time_base = av_inv_q(dp.framerate_in);
    }

    // no timestamp available - extrapolate from previous frame duration
    if (*frame).pts == AV_NOPTS_VALUE {
        (*frame).pts = if dp.last_frame_pts == AV_NOPTS_VALUE {
            0
        } else {
            dp.last_frame_pts + dp.last_frame_duration_est
        }
    }

    // update timestamp history
    dp.last_frame_duration_est = video_duration_estimate(&dp, frame);
    dp.last_frame_pts = (*frame).pts;
    dp.last_frame_tb = (*frame).time_base;

    if dp.sar_override.num != 0 {
        (*frame).sample_aspect_ratio = dp.sar_override;
    }

    if dp.apply_cropping != 0 {
        let ret = av_frame_apply_cropping(frame, AV_FRAME_CROP_UNALIGNED as i32);
        if ret < 0 {
            error!("Error applying decoder cropping");
            return Err(Decoding(DecodingOperationError::CroppingError(
                DecodingError::from(ret),
            )));
        }
    }

    if !(*frame).opaque.is_null() {
        *outputs_mask = (*frame).opaque as usize;
    }

    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn video_duration_estimate(dp: &MutexGuard<DecoderParameter>, frame: *mut AVFrame) -> i64 {
    let mut codec_duration = 0;
    // difference between this and last frame's timestamps
    let ts_diff: i64 = if (*frame).pts != AV_NOPTS_VALUE && dp.last_frame_pts != AV_NOPTS_VALUE {
        (*frame).pts - dp.last_frame_pts
    } else {
        -1
    };

    // XXX lavf currently makes up frame durations when they are not provided by
    // the container. As there is no way to reliably distinguish real container
    // durations from the fake made-up ones, we use heuristics based on whether
    // the container has timestamps. Eventually lavf should stop making up
    // durations, then this should be simplified.

    // frame duration is unreliable (typically guessed by lavf) when it is equal
    // to 1 and the actual duration of the last frame is more than 2x larger
    let duration_unreliable = (*frame).duration == 1 && ts_diff > 2 * (*frame).duration;

    // prefer frame duration for containers with timestamps; a forced
    // framerate always wins (ffmpeg_dec.c:306-308 fr_forced)
    if dp.framerate_forced || ((*frame).duration > 0 && !duration_unreliable) {
        return (*frame).duration;
    }

    if (*dp.dec_ctx.as_ptr()).framerate.den != 0 && (*dp.dec_ctx.as_ptr()).framerate.num != 0 {
        let fields = (*frame).repeat_pict + 2;
        let field_rate = av_mul_q(
            (*dp.dec_ctx.as_ptr()).framerate,
            AVRational { num: 2, den: 1 },
        );
        codec_duration = av_rescale_q(fields as i64, av_inv_q(field_rate), (*frame).time_base);
    }

    // when timestamps are available, repeat last frame's actual duration
    // (i.e. pts difference between this and last frame)
    if ts_diff > 0 {
        return ts_diff;
    }

    // try frame/codec duration
    if (*frame).duration > 0 {
        return (*frame).duration;
    }
    if codec_duration > 0 {
        return codec_duration;
    }

    // try average framerate
    if dp.framerate_in.num != 0 && dp.framerate_in.den != 0 {
        let d = av_rescale_q(1, av_inv_q(dp.framerate_in), (*frame).time_base);
        if d > 0 {
            return d;
        }
    }

    // last resort is last frame's estimated duration, and 1
    std::cmp::max(dp.last_frame_duration_est, 1)
}

unsafe fn hwaccel_retrieve_data(
    dp: &MutexGuard<DecoderParameter>,
    input: *mut AVFrame,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    let output_format = dp.hwaccel_output_format;

    if (*input).format == output_format as i32 {
        // Nothing to do.
        return 0;
    }

    let Ok(mut output) = frame_pool.get() else {
        return AVERROR(ffmpeg_sys_next::ENOMEM);
    };

    (*output.as_mut_ptr()).format = output_format as i32;

    let mut err = av_hwframe_transfer_data(output.as_mut_ptr(), input, 0);
    if err < 0 {
        error!("Failed to transfer data to output frame: {err}");
        frame_pool.release(output);
        return err;
    }

    err = av_frame_copy_props(output.as_mut_ptr(), input);
    if err < 0 {
        frame_pool.release(output);
        return err;
    }

    av_frame_unref(input);
    av_frame_move_ref(input, output.as_mut_ptr());
    frame_pool.release(output);

    0
}

#[cfg(not(docsrs))]
unsafe fn audio_ts_process(mut dp: MutexGuard<DecoderParameter>, frame: *mut AVFrame) {
    let tb_filter = AVRational {
        num: 1,
        den: (*frame).sample_rate,
    };

    // on samplerate change, choose a new internal timebase for timestamp
    // generation that can represent timestamps from all the samplerates
    // seen so far
    let tb = audio_samplerate_update(&mut dp, frame);
    let pts_pred = if dp.last_frame_pts == AV_NOPTS_VALUE {
        0
    } else {
        dp.last_frame_pts + dp.last_frame_duration_est
    };

    if (*frame).pts == AV_NOPTS_VALUE {
        (*frame).pts = pts_pred;
        (*frame).time_base = tb;
    } else if dp.last_frame_pts != AV_NOPTS_VALUE
        && (*frame).pts > av_rescale_q_rnd(pts_pred, tb, (*frame).time_base, AV_ROUND_UP as u32)
    {
        // there was a gap in timestamps, reset conversion state
        dp.last_filter_in_rescale_delta = AV_NOPTS_VALUE;
    }

    (*frame).pts = av_rescale_delta(
        (*frame).time_base,
        (*frame).pts,
        tb,
        (*frame).nb_samples,
        &mut dp.last_filter_in_rescale_delta,
        tb,
    );

    dp.last_frame_pts = (*frame).pts;
    dp.last_frame_duration_est = av_rescale_q((*frame).nb_samples as i64, tb_filter, tb);

    // finally convert to filtering timebase
    (*frame).pts = av_rescale_q((*frame).pts, tb, tb_filter);
    (*frame).duration = (*frame).nb_samples as i64;
    (*frame).time_base = tb_filter;
}

#[cfg(not(docsrs))]
unsafe fn audio_samplerate_update(
    dp: &mut MutexGuard<DecoderParameter>,
    frame: *mut AVFrame,
) -> AVRational {
    let prev = dp.last_frame_tb.den;
    let sr = (*frame).sample_rate;

    if (*frame).sample_rate == dp.last_frame_sample_rate {
        return dp.last_frame_tb;
    }

    let gcd = av_gcd(prev as i64, sr as i64);

    let mut tb_new = if prev as i64 / gcd >= (i32::MAX / sr) as i64 {
        warn!("Audio timestamps cannot be represented exactly after sample rate change: {prev} -> {sr}");

        // LCM of 192000, 44100, allows to represent all common samplerates
        AVRational {
            num: 1,
            den: 28224000,
        }
    } else {
        AVRational {
            num: 1,
            den: (prev as i64 / gcd) as i32 * sr,
        }
    };

    // keep the frame timebase if it is strictly better than
    // the samplerate-defined one
    if (*frame).time_base.num == 1
        && (*frame).time_base.den > tb_new.den
        && (*frame).time_base.den % tb_new.den == 0
    {
        tb_new = (*frame).time_base;
    }

    if dp.last_frame_pts != AV_NOPTS_VALUE {
        dp.last_frame_pts = av_rescale_q(dp.last_frame_pts, dp.last_frame_tb, tb_new);
    }
    dp.last_frame_duration_est = av_rescale_q(dp.last_frame_duration_est, dp.last_frame_tb, tb_new);

    dp.last_frame_tb = tb_new;
    dp.last_frame_sample_rate = (*frame).sample_rate;

    dp.last_frame_tb
}

#[cfg(all(test, not(docsrs)))]
mod tests {
    use super::build_decoder_opts;
    use super::{decode_stall_limit, DecodeErrorBudget};
    use ffmpeg_sys_next::{
        av_dict_count, av_dict_get, AV_DICT_MATCH_CASE, FF_THREAD_FRAME, FF_THREAD_SLICE,
    };
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};

    // The ceiling grows with thread_count ONLY under native frame threading;
    // everything else (external decoders, slice threading, none) pins to the flat
    // floor. Literal asserts pin both the value and the gate, so shrinking the floor
    // OR dropping the FF_THREAD_FRAME gate fails here.
    #[test]
    fn decode_stall_limit_floors_then_grows_with_frame_threads() {
        // Native frame threading expands with the (real) worker count...
        assert_eq!(decode_stall_limit(FF_THREAD_FRAME, 1200), 1232);
        // ...but a small / zero / negative count still pins to the floor (a negative
        // clamps without underflowing u32).
        assert_eq!(decode_stall_limit(FF_THREAD_FRAME, 0), 1024);
        assert_eq!(decode_stall_limit(FF_THREAD_FRAME, -7), 1024);
        // i32::MAX + 32 stays within u32 (no overflow within the i32 input domain).
        assert_eq!(
            decode_stall_limit(FF_THREAD_FRAME, i32::MAX),
            i32::MAX as u32 + 32
        );
        // The gate itself: an external decoder reporting an inflated thread_count
        // while active_thread_type is 0 must NOT balloon the ceiling — it stays at the
        // floor. Dropping the gate makes this ~2.1 billion.
        assert_eq!(decode_stall_limit(0, i32::MAX), 1024);
        // Slice threading is not frame threading, so it also uses the floor.
        assert_eq!(decode_stall_limit(FF_THREAD_SLICE, 1200), 1024);
    }

    // `new` wires the ceiling through `decode_stall_limit(active_thread_type,
    // thread_count)` — the real production path, not a bypassed constant.
    #[test]
    fn new_budget_uses_the_thread_count_ceiling() {
        assert_eq!(DecodeErrorBudget::new(0, FF_THREAD_FRAME, 1200).limit, 1232);
        // An inflated external thread_count is gated down to the floor.
        assert_eq!(DecodeErrorBudget::new(0, 0, i32::MAX).limit, 1024);
    }

    // exit_on_error gives up on the very first error, regardless of progress.
    #[test]
    fn exit_on_error_gives_up_on_the_first_error() {
        // Explicit tiny limit: exit_on_error must fire before the budget matters.
        let mut budget = DecodeErrorBudget {
            stalls: 0,
            last_frame_num: 0,
            limit: 3,
        };
        assert!(budget.on_error(true, 0));
    }

    // An error that still advanced `frame_num` is progress, not a stall — e.g.
    // AV_CODEC_FLAG_DROPCHANGED drops a frame and returns AVERROR_INPUT_CHANGED.
    // No number of such errors may ever trip the budget, even far past the limit.
    #[test]
    fn advancing_frame_num_is_progress_not_a_stall() {
        let mut budget = DecodeErrorBudget {
            stalls: 0,
            last_frame_num: 0,
            limit: 3,
        };
        for i in 1..=(3 * 10) {
            assert!(
                !budget.on_error(false, i),
                "an error that advanced frame_num is progress, not a stall"
            );
        }
    }

    // A genuine stall (frame_num never advances) gives up only PAST the limit, and
    // a decoded frame in between resets the run (a full run of tolerated stalls, a
    // frame, another full run, then one over). An explicit tiny limit pins the
    // off-by-one and reset independently of the production floor.
    #[test]
    fn stall_budget_fires_only_on_a_true_stall_and_resets_on_a_frame() {
        let mut budget = DecodeErrorBudget {
            stalls: 0,
            last_frame_num: 0,
            limit: 3,
        };

        for _ in 0..3 {
            assert!(
                !budget.on_error(false, 0),
                "up to the limit the drain keeps going"
            );
        }
        // A decoded frame resets the stall run.
        budget.on_frame(1);
        for _ in 0..3 {
            assert!(!budget.on_error(false, 1), "the frame reset the stall run");
        }
        // The (limit + 1)-th consecutive no-progress error finally gives up.
        assert!(
            budget.on_error(false, 1),
            "a true stall past the limit gives up"
        );
    }

    // A frame_num-advancing error resets a run that has ALREADY accrued stalls (the
    // reset lives in on_error's advancing branch, not only in on_frame). Without it,
    // the first no-progress error after the advance would fire immediately.
    #[test]
    fn an_advancing_error_resets_an_existing_stall_run() {
        let mut budget = DecodeErrorBudget {
            stalls: 0,
            last_frame_num: 0,
            limit: 3,
        };
        // Accrue a partial run at frame_num 0.
        for _ in 0..3 {
            assert!(
                !budget.on_error(false, 0),
                "within the budget at frame_num 0"
            );
        }
        // An error that advanced frame_num is progress: it must reset the run (and is
        // itself tolerated), NOT count as the fatal (limit + 1)-th stall.
        assert!(
            !budget.on_error(false, 1),
            "the advancing error resets the run and is tolerated"
        );
        // A full fresh run of `limit` is now tolerated again...
        for _ in 0..3 {
            assert!(!budget.on_error(false, 1), "fresh run after the reset");
        }
        // ...and only the next one gives up. (Drop the reset and the first
        // post-advance error would already have fired, failing the loop above.)
        assert!(
            budget.on_error(false, 1),
            "past the limit after the reset gives up"
        );
    }

    fn dict_value(guard: &crate::util::ffmpeg_utils::DictGuard, key: &str) -> Option<String> {
        let key = CString::new(key).unwrap();
        // SAFETY: the guard owns a valid (possibly null) dict; av_dict_get
        // tolerates null and returns entries owned by the dict.
        unsafe {
            let entry = av_dict_get(
                guard.as_ptr(),
                key.as_ptr(),
                std::ptr::null(),
                AV_DICT_MATCH_CASE,
            );
            if entry.is_null() {
                None
            } else {
                Some(
                    CStr::from_ptr((*entry).value)
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        }
    }

    fn opts(pairs: &[(&str, &str)]) -> Option<HashMap<CString, CString>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (CString::new(*k).unwrap(), CString::new(*v).unwrap()))
                .collect(),
        )
    }

    #[test]
    fn no_user_opts_injects_threads_auto() {
        let (guard, injected) = build_decoder_opts(&None);
        assert!(injected, "threads must be injected when the user set none");
        assert_eq!(dict_value(&guard, "threads").as_deref(), Some("auto"));
        // SAFETY: guard owns the dict.
        assert_eq!(unsafe { av_dict_count(guard.as_ptr()) }, 1);
    }

    #[test]
    fn user_threads_value_is_not_overwritten() {
        let (guard, injected) = build_decoder_opts(&opts(&[("threads", "1")]));
        assert!(!injected, "a user-provided threads value must be kept");
        assert_eq!(dict_value(&guard, "threads").as_deref(), Some("1"));
    }

    #[test]
    fn user_opts_are_merged_with_injected_threads() {
        let (guard, injected) = build_decoder_opts(&opts(&[("skip_frame", "nokey")]));
        assert!(injected);
        assert_eq!(dict_value(&guard, "skip_frame").as_deref(), Some("nokey"));
        assert_eq!(dict_value(&guard, "threads").as_deref(), Some("auto"));
    }

    #[test]
    fn injected_threads_is_removed_before_leftover_reporting() {
        // Mirrors dec_open: unrecognized user opts stay behind after
        // av_opt_set_dict2, but the internally injected `threads` entry must
        // never surface in the leftover warnings.
        let (mut guard, injected) = build_decoder_opts(&opts(&[("no_such_opt", "1")]));
        assert!(injected);
        if injected {
            guard.remove(&CString::new("threads").unwrap());
        }
        assert_eq!(guard.leftover_keys(), vec!["no_such_opt".to_string()]);
    }
}
