use crate::core::context::decoder_stream::DecoderStream;
use crate::core::context::demuxer::{CopyMuxHandle, Demuxer};
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::{PacketBox, PacketData};
use crate::core::scheduler::enc_task::{send_to_mux, SendToMuxError};
use crate::core::scheduler::ffmpeg_scheduler::{
    is_stopping, packet_is_null, set_scheduler_error, wait_until_not_paused,
};
use crate::core::scheduler::input_controller::SchNode;
use crate::error::Error::{Demuxing, Encoding};
use crate::error::{DemuxingError, DemuxingOperationError, EncodingOperationError};
use crate::util::ffmpeg_utils::av_err2str;
use crate::util::ffmpeg_utils::av_rescale_q_rnd;
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::Sender;
use ffmpeg_next::packet::{Mut, Ref};
use ffmpeg_next::Packet;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVRounding::AV_ROUND_NEAR_INF;
#[cfg(not(docsrs))]
use ffmpeg_sys_next::AV_CODEC_PROP_FIELDS;
use ffmpeg_sys_next::{
    av_compare_ts, av_gettime_relative, av_inv_q, av_mul_q, av_packet_ref, av_q2d, av_read_frame,
    av_rescale, av_rescale_q, av_stream_get_parser, av_usleep, avformat_seek_file,
    AVCodecDescriptor, AVFormatContext, AVMediaType, AVPacket, AVRational, AVStream, AVERROR,
    AVERROR_EOF, AVFMT_TS_DISCONT, AV_NOPTS_VALUE, AV_PKT_FLAG_CORRUPT, AV_TIME_BASE,
    AV_TIME_BASE_Q, EAGAIN,
};
use libc::{c_int, c_uint};
use log::{debug, error, info, warn};
use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(docsrs)]
pub(crate) fn demux_init(
    demux_idx: usize,
    demux: &mut Demuxer,
    independent_readrate: bool,
    packet_pool: ObjPool<Packet>,
    demux_node: Arc<SchNode>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
pub(crate) fn demux_init(
    demux_idx: usize,
    demux: &mut Demuxer,
    independent_readrate: bool,
    packet_pool: ObjPool<Packet>,
    demux_node: Arc<SchNode>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    if demux.destination_is_empty() {
        warn!(
            "The input:{} does not need to be sent to the destination, skip",
            demux.url
        );
        return Ok(());
    }

    let copy_ts = demux.copy_ts;
    let demux_parameter = DemuxerParameter::new(demux);

    // Take sole ownership of the input context out of the Demuxer. Moving the
    // `FormatContext` into the worker closure below IS the ownership transfer —
    // the compiler guarantees a single owner, so no null-the-source dance and no
    // separate box are needed. The Demuxer's field is now `None`; if the worker
    // never spawns, `in_fmt_ctx` drops here and frees exactly once.
    let in_fmt_ctx = demux
        .in_fmt_ctx
        .take()
        .expect("demux worker started without an input context");

    #[cfg(windows)]
    let hwaccel = { demux.hwaccel.take() };

    let format_name = unsafe {
        CStr::from_ptr((*(*in_fmt_ctx.as_ptr()).iformat).name)
            .to_str()
            .unwrap_or("unknown")
    };

    // Claim the thread slot before spawning so stop()/wait() can never
    // observe a zero counter while this worker is about to run; the guard
    // releases it on any exit path, including panic.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(
        thread_sync.clone(),
        scheduler_status.clone(),
        scheduler_result.clone(),
    );

    let result = std::thread::Builder::new()
        .name(format!("demuxer{demux_idx}:{format_name}"))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            // Move the FormatContext into the worker; it Drops (frees the input
            // context, custom-IO-aware) when this closure ends — the terminal free.
            let in_fmt_ctx = in_fmt_ctx;
            // `demux_parameter` (a `move`-closure CAPTURE) owns the packet-channel
            // senders (`dsts`). Rebind it as a body local declared AFTER the guard so it
            // drops BEFORE it — for a `crossbeam_channel::bounded` channel the queued
            // `PacketBox`es are freed only when the LAST endpoint drops, so leaving these
            // senders as captures would tear the packets down after this worker already
            // released its slot (after the counter stop()/wait() gate on hit zero).
            let mut demux_parameter = demux_parameter;
            let mut is_started = false;
            // Mirrors FFmpeg's Demuxer.nb_streams_warn: warn once per unexpected stream id.
            let mut nb_streams_warn = demux_parameter.demux_streams.len();
            demux_parameter.wallclock_start = unsafe { av_gettime_relative() };

            loop {
                // Architecture Y': stop any `-shortest` follower the muxer's
                // `sq_mux` has cascade-finished, before blocking on the next
                // read. Runs every iteration so a live/sparse follower is
                // retired on the next packet from any stream.
                if demux_check_source_finished(&mut demux_parameter) {
                    debug!("All consumers done (shortest follower cascade); finishing.");
                    break;
                }

                let mut send_flags = 0usize;
                let mut packet = match packet_pool.get() {
                    Ok(packet) => packet,
                    Err(e) => {
                        error!("get packet error on demuxing: {e}");
                        break;
                    }
                };

                unsafe {
                    let mut ret = av_read_frame(in_fmt_ctx.as_ptr(), packet.as_mut_ptr());
                    if ret == AVERROR(EAGAIN) {
                        if is_stopping(wait_until_not_paused(&scheduler_status)) {
                            info!("Demuxer receiver end command, finishing.");
                            break;
                        }
                        packet_pool.release(packet);
                        av_usleep(10000);
                        continue;
                    }

                    if is_stopping(wait_until_not_paused(&scheduler_status)) {
                        info!("Demuxer receiver end command, finishing.");
                        break;
                    }

                    if ret < 0 {
                        if ret == AVERROR_EOF {
                            debug!("EOF while reading input");
                        } else {
                            error!("Error during demuxing: {}", av_err2str(ret));
                            ret = if !is_started || demux_parameter.exit_on_error {
                                ret
                            } else {
                                0
                            };
                        }

                        if ret == AVERROR_EOF {
                            ret = 0;
                        }

                        if demux_parameter.stream_loop != 0 {
                            // Windows-specific CUDA handling logic
                            #[cfg(windows)]
                            let should_skip_packet_send = hwaccel.as_deref() == Some("cuda");

                            // On non-Windows platforms, always send the packet
                            #[cfg(not(windows))]
                            let should_skip_packet_send = false;

                            // Assign the OUTER ret: a shadowing binding here
                            // hid seek/flush failures from the error check
                            // below, so a failed loop restart broke silently.
                            ret = if should_skip_packet_send {
                                // Skip sending the flush packet when using CUDA on Windows
                                // This avoids the "cuvid decode callback error" issue that occurs during loop iterations
                                // Testing showed that after the third loop iteration, avcodec_receive_frame would consistently
                                // return AVERROR_EXTERNAL with the internal error "cuvid decode callback error"
                                0 // Assume success
                            } else {
                                /* signal looping to our consumers by setting stream_index to -1 (flush packet) */
                                (*packet.as_mut_ptr()).stream_index = -1;
                                let packet_box = PacketBox {
                                    packet,
                                    packet_data: PacketData {
                                        dts_est: 0,
                                        codec_type: AVMediaType::AVMEDIA_TYPE_UNKNOWN,
                                        output_stream_index: 0,
                                        is_copy: false,
                                    },
                                };
                                demux_send(&mut demux_parameter, packet_box, &packet_pool, 0, &demux_node, &scheduler_status, &scheduler_result, independent_readrate)
                            };

                            // Common seek operation for both cases
                            if ret >= 0 {
                                ret = seek_to_start(&mut demux_parameter, in_fmt_ctx.as_ptr());
                                if ret >= 0 {
                                    continue;
                                }
                            }
                            /* fallthrough to the error path */
                        }

                        // AVERROR_EXIT is a normal stop (the scheduler asked
                        // us to quit mid-send), never a task failure
                        // (matches ffmpeg_demux.c:780-781 EOF/EXIT handling).
                        if ret != 0 && ret != ffmpeg_sys_next::AVERROR_EXIT {
                            set_scheduler_error(
                                &scheduler_status,
                                &scheduler_result,
                                Demuxing(DemuxingOperationError::ReadFrameError(
                                    DemuxingError::from(ret),
                                )),
                            );
                        }

                        break;
                    }

                    if demux_parameter.demux_streams.len()
                        <= (*packet.as_ptr()).stream_index as usize
                    {
                        let stream_index = (*packet.as_ptr()).stream_index as usize;
                        if stream_index >= nb_streams_warn {
                            warn!("Incorrect stream id:{stream_index}, ignoring its packets");
                            nb_streams_warn = stream_index + 1;
                        }
                        packet_pool.release(packet);
                        continue;
                    }

                    // Discarded and finished streams are dropped before the
                    // corrupt check and any ts_fixup/readrate/send
                    // bookkeeping, like the loop-top skip in fftools
                    // (ffmpeg_demux.c:751 precedes the corrupt handling at
                    // :756-766): a bad packet on a dead stream must not
                    // fail streams that are still consuming.
                    {
                        let ds = &demux_parameter.demux_streams
                            [(*packet.as_ptr()).stream_index as usize];
                        if ds.discard || ds.finished {
                            packet_pool.release(packet);
                            continue;
                        }
                    }

                    if (*packet.as_ptr()).flags & AV_PKT_FLAG_CORRUPT != 0 {
                        if demux_parameter.exit_on_error {
                            error!(
                                "corrupt input packet in stream {}",
                                (*packet.as_ptr()).stream_index
                            );
                            packet_pool.release(packet);
                            // exit_on_error promises a failing result, not a
                            // silent early stop (fftools aborts here).
                            set_scheduler_error(
                                &scheduler_status,
                                &scheduler_result,
                                Demuxing(DemuxingOperationError::ReadFrameError(
                                    DemuxingError::from(ffmpeg_sys_next::AVERROR_INVALIDDATA),
                                )),
                            );
                            break;
                        } else {
                            warn!(
                                "corrupt input packet in stream {}",
                                (*packet.as_ptr()).stream_index
                            );
                        }
                    }

                    is_started = true;
                    ret = input_packet_process(
                        &mut demux_parameter,
                        in_fmt_ctx.as_ptr(),
                        packet.as_mut_ptr(),
                        &mut send_flags,
                        copy_ts,
                    );
                    if ret < 0 {
                        break;
                    }

                    // Captured after ts_fixup: the raw packet's time_base is
                    // {0,1} and its pts is not wrap-corrected — using those
                    // made every stream_loop duration computation garbage.
                    demux_parameter.end_pts = Timestamp {
                        ts: (*packet.as_ptr()).pts,
                        tb: (*packet.as_ptr()).time_base,
                    };

                    if let Some(readrate) = demux_parameter.readrate {
                        if readrate != 0.0 {
                            readrate_sleep(
                                &demux_parameter,
                                (*in_fmt_ctx.as_ptr()).nb_streams,
                                readrate,
                            );
                        }
                    }

                    {
                        let ds = demux_parameter
                            .demux_streams
                            .get_mut((*packet.as_ptr()).stream_index as usize)
                            .unwrap();
                        let packet_box = PacketBox {
                            packet,
                            packet_data: PacketData {
                                dts_est: ds.dts,
                                codec_type: ds.codec_type,
                                output_stream_index: 0,
                                is_copy: false,
                            },
                        };
                        ret = demux_send(&mut demux_parameter, packet_box, &packet_pool, send_flags, &demux_node, &scheduler_status, &scheduler_result, independent_readrate);

                        if ret < 0 {
                            break;
                        }
                    }
                }
            }

            if is_started {
                demux_done(&mut demux_parameter, &packet_pool, &scheduler_status, &scheduler_result);
            }

            let node = demux_node.as_ref();
            let SchNode::Demux {
                waiter: _, task_exited
            } = node else { unreachable!() };
            task_exited.store(true, Ordering::Release);
            debug!("Demuxer finished.");
        });
    if let Err(e) = result {
        error!("Demuxer thread exited with error: {e}");
        return Err(DemuxingOperationError::ThreadExited.into());
    }

    Ok(())
}

fn demux_done(
    demux_parameter: &mut DemuxerParameter,
    packet_pool: &ObjPool<Packet>,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
) {
    for ds in &demux_parameter.demux_streams {
        for (i, (packet_dst, input_stream_index, output_stream_index)) in
            demux_parameter.dsts.iter().enumerate()
        {
            // Disjoint field borrows: `dsts` (shared, the iterator),
            // `dsts_finished` (mutable, below), and `dst_mux_gate` (shared, at
            // the call) are different fields, so all coexist.
            let dst_finished = &mut demux_parameter.dsts_finished[i];

            if ds.stream_index != *input_stream_index {
                continue;
            }

            let result = packet_pool.get();
            if let Err(e) = result {
                warn!("Demuxer done alloc packet failed: {}", e);
                continue;
            }
            let mut packet = result.unwrap();
            unsafe { (*packet.as_mut_ptr()).stream_index = -1 };

            let packet_box = PacketBox {
                packet,
                packet_data: PacketData {
                    dts_est: ds.dts,
                    codec_type: ds.codec_type,
                    output_stream_index: 0,
                    is_copy: false,
                },
            };

            let ret = unsafe {
                demux_stream_send_to_dst(
                    packet_box,
                    packet_dst,
                    output_stream_index,
                    dst_finished,
                    demux_parameter.dst_mux_gate[i].as_ref(),
                    0,
                    scheduler_status,
                    scheduler_result,
                )
            };
            if ret == AVERROR_EOF {
                // Normal teardown: this destination already finished (max_frames /
                // recording_time / downstream exit). FFmpeg's demux_done ignores
                // AVERROR_EOF here as well (fftools/ffmpeg_sched.c).
                debug!("demux_done: dst {i} (input stream {input_stream_index}) already finished");
            } else if ret < 0 {
                warn!(
                    "demux_done: failed to send flush packet for input stream {input_stream_index}, ret={ret}"
                );
            }
        }
    }
}

const READRATE_INITIAL_BURST: f32 = 0.5;
unsafe fn readrate_sleep(demux_parameter: &DemuxerParameter, nb_streams: c_uint, readrate: f32) {
    let file_start = 0;
    let burst_until = (AV_TIME_BASE as f32 * READRATE_INITIAL_BURST) as i64;

    for i in 0..nb_streams {
        let option = demux_parameter.demux_streams.get(i as usize);
        if let Some(ds) = option {
            let mut stream_ts_offset = if ds.first_dts != AV_NOPTS_VALUE {
                ds.first_dts
            } else {
                0
            };
            stream_ts_offset = std::cmp::max(stream_ts_offset, file_start);
            let pts = av_rescale(ds.dts, 1000000, AV_TIME_BASE as i64);
            let now = ((av_gettime_relative() - demux_parameter.wallclock_start) as f32 * readrate)
                as i64
                + stream_ts_offset;
            if pts - burst_until > now {
                av_usleep((pts - burst_until - now) as u32);
            }
        }
    }
}

unsafe fn input_packet_process(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    pkt: *mut AVPacket,
    send_flags: &mut usize,
    copy_ts: bool,
) -> c_int {
    ts_fixup(demux_parameter, in_fmt_ctx, pkt, copy_ts);

    if let Some(recording_time_us) = demux_parameter.recording_time_us {
        if recording_time_us != i64::MAX {
            let mut start_time = 0;
            if copy_ts {
                start_time += demux_parameter.start_time_us.unwrap_or(0);
                // FFmpeg CLI: start_time += start_at_zero ? 0 : f->start_time_effective;
                start_time += demux_parameter.start_time_effective;
            }
            let ds = demux_parameter
                .demux_streams
                .get_mut((*pkt).stream_index as usize)
                .unwrap();
            if ds.dts >= recording_time_us + start_time {
                *send_flags |= DEMUX_SEND_STREAMCOPY_EOF;
            }
        }
    }

    // ds->data_size += pkt->size;
    // ds->nb_packets++;

    0
}

#[cfg(docsrs)]
unsafe fn ts_fixup(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    pkt: *mut AVPacket,
    copy_ts: bool,
) {
}

#[cfg(not(docsrs))]
unsafe fn ts_fixup(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    pkt: *mut AVPacket,
    copy_ts: bool,
) {
    let streams = (*in_fmt_ctx).streams;
    let ist = *streams.offset((*pkt).stream_index as isize);
    let start_time = demux_parameter.start_time_effective;
    (*pkt).time_base = (*ist).time_base;

    {
        let ds = demux_parameter
            .demux_streams
            .get_mut((*pkt).stream_index as usize)
            .unwrap();

        if !ds.wrap_correction_done && start_time != AV_NOPTS_VALUE && (*ist).pts_wrap_bits < 64 {
            let stime = av_rescale_q(start_time, AV_TIME_BASE_Q, (*pkt).time_base);
            let stime2 = stime + (1u64 << (*ist).pts_wrap_bits) as i64;
            ds.wrap_correction_done = true;

            if stime2 > stime
                && (*pkt).dts != AV_NOPTS_VALUE
                && (*pkt).dts > stime + (1i64 << ((*ist).pts_wrap_bits - 1))
            {
                (*pkt).dts -= (1u64 << (*ist).pts_wrap_bits) as i64;
                ds.wrap_correction_done = false;
            }
            if stime2 > stime
                && (*pkt).pts != AV_NOPTS_VALUE
                && (*pkt).pts > stime + (1i64 << ((*ist).pts_wrap_bits - 1))
            {
                (*pkt).pts -= (1u64 << (*ist).pts_wrap_bits) as i64;
                ds.wrap_correction_done = false;
            }
        }
    }

    if (*pkt).dts != AV_NOPTS_VALUE {
        (*pkt).dts += av_rescale_q(demux_parameter.ts_offset, AV_TIME_BASE_Q, (*pkt).time_base);
    }
    if (*pkt).pts != AV_NOPTS_VALUE {
        (*pkt).pts += av_rescale_q(demux_parameter.ts_offset, AV_TIME_BASE_Q, (*pkt).time_base);
    }

    // Apply timestamp scaling (after ts_offset, before duration)
    // FFmpeg source: ffmpeg_demux.c:404-406 (FFmpeg 7.x)
    // Note: C's `int64_t *= double` truncates toward zero, Rust's `as i64` behaves the same.
    let ts_scale = demux_parameter.ts_scale;
    if ts_scale != 1.0 {
        if (*pkt).pts != AV_NOPTS_VALUE {
            (*pkt).pts = ((*pkt).pts as f64 * ts_scale) as i64;
        }
        if (*pkt).dts != AV_NOPTS_VALUE {
            (*pkt).dts = ((*pkt).dts as f64 * ts_scale) as i64;
        }
    }

    let duration = av_rescale_q(
        demux_parameter.duration.ts,
        demux_parameter.duration.tb,
        (*pkt).time_base,
    );

    if (*pkt).pts != AV_NOPTS_VALUE {
        // audio decoders take precedence for estimating total file duration
        let pkt_duration = if demux_parameter.have_audio_dec {
            0
        } else {
            (*pkt).duration
        };

        (*pkt).pts += duration;

        // update max/min pts that will be used to compute total file duration
        // when using -stream_loop
        if demux_parameter.max_pts.ts == AV_NOPTS_VALUE
            || av_compare_ts(
                demux_parameter.max_pts.ts,
                demux_parameter.max_pts.tb,
                (*pkt).pts + pkt_duration,
                (*pkt).time_base,
            ) < 0
        {
            demux_parameter.max_pts = Timestamp {
                ts: (*pkt).pts + pkt_duration,
                tb: (*pkt).time_base,
            };
        }
        if demux_parameter.min_pts.ts == AV_NOPTS_VALUE
            || av_compare_ts(
                demux_parameter.min_pts.ts,
                demux_parameter.min_pts.tb,
                (*pkt).pts,
                (*pkt).time_base,
            ) > 0
        {
            demux_parameter.min_pts = Timestamp {
                ts: (*pkt).pts,
                tb: (*pkt).time_base,
            };
        }
    }

    if (*pkt).dts != AV_NOPTS_VALUE {
        (*pkt).dts += duration;
    }

    // detect and try to correct for timestamp discontinuities
    ts_discontinuity_process(demux_parameter, in_fmt_ctx, ist, pkt, copy_ts);

    // update estimated/predicted dts
    ist_dts_update(demux_parameter, ist, pkt);
}

#[cfg(docsrs)]
unsafe fn ist_dts_update(
    demux_parameter: &mut DemuxerParameter,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
) {
}

#[cfg(not(docsrs))]
unsafe fn ist_dts_update(
    demux_parameter: &mut DemuxerParameter,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
) {
    let ds = demux_parameter
        .demux_streams
        .get_mut((*pkt).stream_index as usize)
        .unwrap();

    let par = (*ist).codecpar;

    let framerate = demux_parameter.framerate;

    if !ds.saw_first_ts {
        // Use stream's avg_frame_rate (metadata) for initial DTS — NOT the forced framerate.
        // CLI: ist->st->avg_frame_rate (ffmpeg_demux.c:303), here ist IS the AVStream.
        let avg_frame_rate = (*ist).avg_frame_rate;
        ds.dts = if avg_frame_rate.num != 0 {
            (((-(*par).video_delay) * AV_TIME_BASE) as f64 / av_q2d(avg_frame_rate)) as i64
        } else {
            0
        };
        ds.first_dts = ds.dts;

        if (*pkt).pts != AV_NOPTS_VALUE {
            ds.dts += av_rescale_q((*pkt).pts, (*pkt).time_base, AV_TIME_BASE_Q);
            ds.first_dts = ds.dts;
        }
        ds.saw_first_ts = true;
    }

    if ds.next_dts == AV_NOPTS_VALUE {
        ds.next_dts = ds.dts;
    }

    if (*pkt).dts != AV_NOPTS_VALUE {
        ds.dts = av_rescale_q((*pkt).dts, (*pkt).time_base, AV_TIME_BASE_Q);
        ds.next_dts = ds.dts;
    }

    ds.dts = ds.next_dts;
    match (*par).codec_type {
        AVMEDIA_TYPE_AUDIO => {
            if (*par).sample_rate != 0 {
                ds.next_dts +=
                    (AV_TIME_BASE as i64 * (*par).frame_size as i64) / (*par).sample_rate as i64;
            } else {
                ds.next_dts += av_rescale_q((*pkt).duration, (*pkt).time_base, AV_TIME_BASE_Q);
            }
        }
        AVMEDIA_TYPE_VIDEO => {
            if framerate.num != 0 {
                let time_base_q = AV_TIME_BASE_Q;
                let next_dts = av_rescale_q(ds.next_dts, time_base_q, av_inv_q(framerate));
                ds.next_dts = av_rescale_q(next_dts + 1, av_inv_q(framerate), time_base_q);
            } else if (*pkt).duration != 0 {
                ds.next_dts += av_rescale_q((*pkt).duration, (*pkt).time_base, AV_TIME_BASE_Q);
            } else if (*par).framerate.num != 0 {
                let field_rate = av_mul_q((*par).framerate, AVRational { num: 2, den: 1 });
                let mut fields = 2;

                if !ds.codec_desc.is_null()
                    && ((*ds.codec_desc).props & AV_CODEC_PROP_FIELDS) != 0
                    && !av_stream_get_parser(ist).is_null()
                {
                    fields = 1 + (*av_stream_get_parser(ist)).repeat_pict;
                }

                ds.next_dts += av_rescale_q(fields as i64, av_inv_q(field_rate), AV_TIME_BASE_Q);
            }
        }
        _ => {}
    }
}

#[cfg(docsrs)]
unsafe fn ts_discontinuity_process(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
) {
}

#[cfg(not(docsrs))]
unsafe fn ts_discontinuity_process(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
    copy_ts: bool,
) {
    let offset = av_rescale_q(
        demux_parameter.ts_offset_discont,
        AV_TIME_BASE_Q,
        (*pkt).time_base,
    );

    // apply previously-detected timestamp-discontinuity offset
    // (to all streams, not just audio/video)
    if (*pkt).dts != AV_NOPTS_VALUE {
        (*pkt).dts += offset;
    }
    if (*pkt).pts != AV_NOPTS_VALUE {
        (*pkt).pts += offset;
    }

    // detect timestamp discontinuities for audio/video
    if ((*(*ist).codecpar).codec_type == AVMEDIA_TYPE_VIDEO
        || (*(*ist).codecpar).codec_type == AVMEDIA_TYPE_AUDIO)
        && (*pkt).dts != AV_NOPTS_VALUE
    {
        ts_discontinuity_detect(demux_parameter, in_fmt_ctx, ist, pkt, copy_ts);
    }
}

#[cfg(docsrs)]
unsafe fn ts_discontinuity_detect(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
) {
}

#[cfg(not(docsrs))]
unsafe fn ts_discontinuity_detect(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
    ist: *mut AVStream,
    pkt: *mut AVPacket,
    copy_ts: bool,
) {
    let ds = demux_parameter
        .demux_streams
        .get_mut((*pkt).stream_index as usize)
        .unwrap();

    let fmt_is_discont = (*(*in_fmt_ctx).iformat).flags & AVFMT_TS_DISCONT;

    let mut disable_discontinuity_correction = copy_ts;
    let pkt_dts = av_rescale_q_rnd(
        (*pkt).dts,
        (*pkt).time_base,
        AV_TIME_BASE_Q,
        AV_ROUND_NEAR_INF as u32,
    );

    if copy_ts && ds.next_dts != AV_NOPTS_VALUE && fmt_is_discont != 0 && (*ist).pts_wrap_bits < 60
    {
        let wrap_dts = av_rescale_q_rnd(
            (*pkt).dts + (1i64 << (*ist).pts_wrap_bits),
            (*pkt).time_base,
            AV_TIME_BASE_Q,
            AV_ROUND_NEAR_INF as u32,
        );
        if (wrap_dts - ds.next_dts).abs() < (pkt_dts - ds.next_dts).abs() / 10 {
            disable_discontinuity_correction = false;
        }
    }

    const DTS_DELTA_THRESHOLD: i64 = 10;
    if ds.next_dts != AV_NOPTS_VALUE && !disable_discontinuity_correction {
        let mut delta = pkt_dts - ds.next_dts;
        if fmt_is_discont != 0 {
            if delta.abs() > DTS_DELTA_THRESHOLD * AV_TIME_BASE as i64
                || (pkt_dts + (AV_TIME_BASE / 10) as i64) < ds.dts
            {
                demux_parameter.ts_offset_discont -= delta;
                warn!(
                    "timestamp discontinuity (stream id={}): {}, new offset= {}",
                    (*ist).id,
                    delta,
                    demux_parameter.ts_offset_discont
                );
                (*pkt).dts -= av_rescale_q(delta, AV_TIME_BASE_Q, (*pkt).time_base);
                if (*pkt).pts != AV_NOPTS_VALUE {
                    (*pkt).pts -= av_rescale_q(delta, AV_TIME_BASE_Q, (*pkt).time_base);
                }
            }
        } else {
            const DTS_ERROR_THRESHOLD: i64 = 108000;
            if delta.abs() > DTS_ERROR_THRESHOLD * AV_TIME_BASE as i64 {
                warn!(
                    "DTS {}, next:{} st:{} invalid dropping",
                    (*pkt).dts,
                    ds.next_dts,
                    (*pkt).stream_index
                );
                (*pkt).dts = AV_NOPTS_VALUE;
            }
            if (*pkt).pts != AV_NOPTS_VALUE {
                let pkt_pts = av_rescale_q((*pkt).pts, (*pkt).time_base, AV_TIME_BASE_Q);
                delta = pkt_pts - ds.next_dts;
                if delta.abs() > DTS_ERROR_THRESHOLD * AV_TIME_BASE as i64 {
                    warn!(
                        "PTS {}, next:{} invalid dropping st:{}",
                        (*pkt).pts,
                        ds.next_dts,
                        (*pkt).stream_index
                    );
                    (*pkt).pts = AV_NOPTS_VALUE;
                }
            }
        }
    } else if ds.next_dts == AV_NOPTS_VALUE
        && !copy_ts
        && fmt_is_discont != 0
        && demux_parameter.last_ts != AV_NOPTS_VALUE
    {
        let delta = pkt_dts - demux_parameter.last_ts;
        if delta.abs() > DTS_DELTA_THRESHOLD * AV_TIME_BASE as i64 {
            demux_parameter.ts_offset_discont -= delta;
            debug!(
                "Inter stream timestamp discontinuity {}, new offset= {}",
                delta, demux_parameter.ts_offset_discont
            );
            (*pkt).dts -= av_rescale_q(delta, AV_TIME_BASE_Q, (*pkt).time_base);
            if (*pkt).pts != AV_NOPTS_VALUE {
                (*pkt).pts -= av_rescale_q(delta, AV_TIME_BASE_Q, (*pkt).time_base);
            }
        }
    }

    demux_parameter.last_ts = av_rescale_q((*pkt).dts, (*pkt).time_base, AV_TIME_BASE_Q);
}

struct DemuxStreamParameter {
    codec_type: AVMediaType,
    stream_index: usize,
    codec_desc: *const AVCodecDescriptor,

    wrap_correction_done: bool,
    saw_first_ts: bool,
    ///< dts of the first packet read for this stream (in AV_TIME_BASE units)
    first_dts: i64,

    next_dts: i64,
    ///< dts of the last packet read for this stream (in AV_TIME_BASE units)
    dts: i64,

    ///< no consumer is bound to this stream; its packets are dropped at the
    ///< demux loop top (ffmpeg_demux.c:751, flipped on binding at :904-906)
    discard: bool,
    ///< all consumers of this stream saw EOF (ffmpeg_demux.c:523)
    finished: bool,
}

// SAFETY: codec_desc points to static FFmpeg codec descriptor data
// (read-only), and the demuxer thread owns the value exclusively. Sync is
// intentionally NOT implemented: nothing shares &DemuxStreamParameter
// across threads.
unsafe impl Send for DemuxStreamParameter {}
impl DemuxStreamParameter {
    fn new(ds: &DecoderStream) -> Self {
        Self {
            codec_type: ds.codec_type,
            stream_index: ds.stream_index,
            codec_desc: ds.codec_desc,
            wrap_correction_done: false,
            saw_first_ts: false,
            first_dts: AV_NOPTS_VALUE,
            next_dts: AV_NOPTS_VALUE,
            dts: 0,
            discard: true,
            finished: false,
        }
    }
}
struct DemuxerParameter {
    dsts_finished: Vec<bool>,
    have_audio_dec: bool,

    wallclock_start: i64,
    /**
     * Extra timestamp offset added by discontinuity handling.
     */
    ts_offset_discont: i64,
    last_ts: i64,

    start_time_effective: i64,
    ts_offset: i64,

    readrate: Option<f32>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    exit_on_error: bool,
    stream_loop: i32,

    /// Timestamp scale factor for pts/dts values.
    /// Applied after ts_offset addition. Default is 1.0.
    ///
    /// FFmpeg CLI: `-itsscale <scale>`
    /// FFmpeg source: `ffmpeg_demux.c:404-406` (FFmpeg 7.x)
    ts_scale: f64,

    /// Forced framerate for the input video stream.
    /// When `num != 0`, overrides DTS estimation to use framerate-based grid.
    /// When `{0, 0}` (default), packet duration is used, matching FFmpeg CLI
    /// behavior when `-r` is not specified.
    ///
    /// FFmpeg CLI: `-r <rate>`
    /// FFmpeg source: `ffmpeg.h:452`, `ffmpeg_demux.c:329-333` (FFmpeg 7.x)
    framerate: AVRational,

    end_pts: Timestamp,

    /* duration of the looped segment of the input file */
    duration: Timestamp,
    /* pts with the smallest/largest values ever seen */
    min_pts: Timestamp,
    max_pts: Timestamp,

    demux_streams: Vec<DemuxStreamParameter>,

    dsts: Vec<(Sender<PacketBox>, usize, Option<usize>)>,

    /// For each input stream index, the indices into `dsts` that consume that
    /// stream. Built once in `new`; never mutated on the demux hot path, so
    /// fan-out is an indexed slice walk with no per-packet allocation (PERF-7).
    per_stream_dsts: Vec<Vec<usize>>,

    /// Streams with at least one bound consumer (ffmpeg_demux.c:906).
    nb_streams_used: usize,
    /// Streams whose consumers have all finished (ffmpeg_demux.c:525).
    nb_streams_finished: usize,

    /// Parallel to `dsts`: a `-shortest` streamcopy destination's mux
    /// `source_finished` flag (Architecture Y'), or `None`. When set by the mux
    /// (its `sq_mux` cascade-finished this follower), the demux retires that
    /// destination — reusing the exact per-dst `dsts_finished` accounting the
    /// channel-disconnect path uses, so even a live/unbounded follower stops.
    dst_source_finished: Vec<Option<Arc<AtomicBool>>>,
    /// `true` iff any `dst_source_finished` is `Some`; the loop-top scan
    /// early-outs on this so the common (non-shortest) path pays a single bool
    /// test per iteration.
    has_source_finished_dsts: bool,

    /// Parallel to `dsts`: a streamcopy destination's muxer gate + pre-mux
    /// queue (`Some`), or `None` for a decoder destination. When set, the demux
    /// routes that destination's packets through the encoders' deferred-start
    /// gate instead of blocking on the shared live queue before the muxer
    /// starts — fftools sends demux->mux copy packets through the same
    /// `sch_send` path as encoders (ffmpeg_sched.c:2038-2077).
    dst_mux_gate: Vec<Option<CopyMuxHandle>>,
}

// SAFETY: all raw pointers live in DemuxStreamParameter (see its SAFETY
// comment) and the demuxer thread owns the value exclusively; the channel
// senders are Send by themselves. Sync is intentionally NOT implemented.
unsafe impl Send for DemuxerParameter {}
impl DemuxerParameter {
    fn new(demux: &mut Demuxer) -> Self {
        let dsts = demux.take_dsts();
        let dsts_finished = vec![false; dsts.len()];
        let dst_source_finished = demux.take_dst_source_finished();
        debug_assert_eq!(dst_source_finished.len(), dsts.len());
        let has_source_finished_dsts = dst_source_finished.iter().any(|x| x.is_some());
        // Carried in lockstep with `dsts` (built alongside `add_packet_dst` /
        // `connect_stream`), so `dst_mux_gate[i]` always describes `dsts[i]`.
        let dst_mux_gate = demux.take_dst_mux_gate();
        debug_assert_eq!(dst_mux_gate.len(), dsts.len());

        let nb_streams = unsafe { (*demux.in_fmt_ctx_ptr()).nb_streams } as usize;
        let mut demux_streams: Vec<DemuxStreamParameter> = Vec::with_capacity(nb_streams);
        for i in 0..nb_streams {
            let stream = demux.get_stream(i);
            demux_streams.push(DemuxStreamParameter::new(stream))
        }

        // Precompute, for each input stream, the `dsts` indices that consume it.
        // The mapping is immutable after construction, so the demux hot path can
        // index this instead of scanning + collecting all destinations per
        // packet (PERF-7).
        let mut per_stream_dsts: Vec<Vec<usize>> = vec![Vec::new(); nb_streams];
        for (dst_i, (_, input_stream_index, _)) in dsts.iter().enumerate() {
            debug_assert!(*input_stream_index < nb_streams);
            per_stream_dsts[*input_stream_index].push(dst_i);
        }

        let have_audio_dec = demux_streams.iter().any(|ds| {
            ds.codec_type == AVMEDIA_TYPE_AUDIO && !per_stream_dsts[ds.stream_index].is_empty()
        });

        // fftools flips ds->discard when a consumer binds to the stream and
        // counts it as used (ffmpeg_demux.c:904-906 ist_use).
        let mut nb_streams_used = 0;
        for ds in &mut demux_streams {
            ds.discard = per_stream_dsts[ds.stream_index].is_empty();
            if !ds.discard {
                nb_streams_used += 1;
            }
        }

        Self {
            dsts_finished,
            have_audio_dec,
            wallclock_start: 0,
            ts_offset_discont: 0,
            last_ts: 0,
            start_time_effective: demux.start_time_effective,
            ts_offset: demux.ts_offset,
            readrate: demux.readrate,
            start_time_us: demux.start_time_us,
            recording_time_us: demux.recording_time_us,
            exit_on_error: demux.exit_on_error.unwrap_or(false),
            stream_loop: demux.stream_loop.unwrap_or(0),
            ts_scale: demux.ts_scale,
            framerate: demux.framerate,

            end_pts: Default::default(),

            duration: Timestamp {
                ts: 0,
                tb: AVRational { num: 1, den: 1 },
            },
            min_pts: Default::default(),
            max_pts: Default::default(),
            demux_streams,
            dsts,
            per_stream_dsts,
            nb_streams_used,
            nb_streams_finished: 0,
            dst_source_finished,
            has_source_finished_dsts,
            dst_mux_gate,
        }
    }
}

/// Architecture Y': retire any `-shortest` streamcopy follower whose muxer has
/// cascade-finished it (`source_finished` is set). Reuses the exact
/// per-destination `dsts_finished` + `nb_streams_finished` accounting the
/// channel-disconnect path uses (`demux_send_for_stream`), so the stop is
/// per-destination — a second output copying the same input stream (its own
/// `dsts` entry, `None` flag) is untouched. Called once per demux-loop
/// iteration, so a sparse or blocked follower is stopped on the next read from
/// ANY stream — the same bound `recording_time` already ships. Returns `true`
/// when every used stream is finished and the demuxer must exit.
fn demux_check_source_finished(dp: &mut DemuxerParameter) -> bool {
    if !dp.has_source_finished_dsts {
        return false;
    }
    // `Vec::new()` does not allocate until the first push, so a scan that retires
    // nothing (the common tick) stays alloc-free.
    let mut finished_streams = Vec::new();
    scan_source_finished_dsts(
        &dp.dst_source_finished,
        &dp.dsts,
        &dp.per_stream_dsts,
        &mut dp.dsts_finished,
        &mut finished_streams,
    );
    for s in finished_streams {
        let ds = &mut dp.demux_streams[s];
        if !ds.finished {
            ds.finished = true;
            dp.nb_streams_finished += 1;
        }
    }
    dp.nb_streams_finished == dp.nb_streams_used
}

/// Pure core of [`demux_check_source_finished`] (unit-testable, no FFI): retire
/// every destination whose `source_finished` flag is set, and record each input
/// stream that thereby became fully finished (all its destinations done), each at
/// most once per scan. Mirrors the per-dst / per-stream accounting of
/// `demux_send_for_stream` (a stream finishes only when `nb_done == dst_count`),
/// so it is inherently per-destination — a second output copying the same input
/// stream (its own `dsts` entry with a `None` flag) is never touched.
fn scan_source_finished_dsts(
    dst_source_finished: &[Option<Arc<AtomicBool>>],
    dsts: &[(Sender<PacketBox>, usize, Option<usize>)],
    per_stream_dsts: &[Vec<usize>],
    dsts_finished: &mut [bool],
    finished_streams: &mut Vec<usize>,
) {
    for dst_i in 0..dst_source_finished.len() {
        let Some(flag) = &dst_source_finished[dst_i] else {
            continue;
        };
        if dsts_finished[dst_i] || !flag.load(Ordering::Acquire) {
            continue;
        }
        // Retire this destination exactly like the disconnect path.
        dsts_finished[dst_i] = true;
        let in_stream = dsts[dst_i].1;
        if per_stream_dsts[in_stream]
            .iter()
            .all(|&di| dsts_finished[di])
        {
            finished_streams.push(in_stream);
        }
    }
}

#[derive(Clone)]
struct Timestamp {
    ts: i64,
    tb: AVRational,
}

impl Default for Timestamp {
    fn default() -> Self {
        Self {
            ts: AV_NOPTS_VALUE,
            tb: AVRational { num: 1, den: 1 },
        }
    }
}

unsafe fn seek_to_start(
    demux_parameter: &mut DemuxerParameter,
    in_fmt_ctx: *mut AVFormatContext,
) -> i32 {
    let start_time = demux_parameter.start_time_us.unwrap_or(0);
    let ret = avformat_seek_file(in_fmt_ctx, -1, i64::MIN, start_time, start_time, 0);
    if ret < 0 {
        return ret;
    }

    // A && (B || C): with the old (A && B) || C grouping an end_pts of
    // AV_NOPTS_VALUE could overwrite a valid max_pts via the C arm
    // (matches ffmpeg_demux.c:191-194).
    if demux_parameter.end_pts.ts != AV_NOPTS_VALUE
        && (demux_parameter.max_pts.ts == AV_NOPTS_VALUE
            || av_compare_ts(
                demux_parameter.max_pts.ts,
                demux_parameter.max_pts.tb,
                demux_parameter.end_pts.ts,
                demux_parameter.end_pts.tb,
            ) < 0)
    {
        demux_parameter.max_pts = demux_parameter.end_pts.clone();
    }

    if demux_parameter.max_pts.ts != AV_NOPTS_VALUE {
        let min_pts = if demux_parameter.min_pts.ts == AV_NOPTS_VALUE {
            0
        } else {
            demux_parameter.min_pts.ts
        };
        demux_parameter.duration.ts = demux_parameter.max_pts.ts
            - av_rescale_q(
                min_pts,
                demux_parameter.min_pts.tb,
                demux_parameter.max_pts.tb,
            );
    }
    demux_parameter.duration.tb = demux_parameter.max_pts.tb;

    if demux_parameter.stream_loop > 0 {
        demux_parameter.stream_loop -= 1;
    }

    let loop_status = if demux_parameter.stream_loop > 0 {
        format!("Remaining loops: {}", demux_parameter.stream_loop)
    } else if demux_parameter.stream_loop == 0 {
        "Last loop".to_string()
    } else {
        "Infinite loop mode".to_string()
    };

    debug!("Repositioning stream to starting point: position={start_time}μs, {loop_status}");

    ret
}

unsafe fn demux_send(
    demux_parameter: &mut DemuxerParameter,
    packet_box: PacketBox,
    packet_pool: &ObjPool<Packet>,
    flags: usize,
    demux_node: &Arc<SchNode>,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    independent_readrate: bool,
) -> i32 {
    let node = demux_node.as_ref();
    let SchNode::Demux { waiter, .. } = node else {
        unreachable!()
    };
    let wait_time = waiter.wait_with_scheduler_status(scheduler_status, independent_readrate);
    if is_stopping(wait_until_not_paused(scheduler_status)) {
        return ffmpeg_sys_next::AVERROR_EXIT;
    }
    if independent_readrate && wait_time != 0 {
        if let Some(readrate) = demux_parameter.readrate {
            if readrate != 0.0 {
                let fix_wallclock_start = demux_parameter.wallclock_start + wait_time;
                debug!("FFmpeg on-demand scheduling caused the initial wallclock_start to not meet the specified readrate:{readrate}. Adjusting wallclock_start from {} to {fix_wallclock_start}",
                    demux_parameter.wallclock_start);
                demux_parameter.wallclock_start = fix_wallclock_start;
            }
        }
    }

    // flush the downstreams after seek
    if (*packet_box.packet.as_ptr()).stream_index == -1 {
        packet_pool.release(packet_box.packet);
        return demux_flush(
            packet_pool,
            &demux_parameter.dsts,
            &mut demux_parameter.dsts_finished,
        );
    }

    let stream_index = (*packet_box.packet.as_ptr()).stream_index as usize;
    let ret = demux_send_for_stream(
        demux_parameter,
        packet_box,
        packet_pool,
        flags,
        scheduler_status,
        scheduler_result,
    );
    if ret == AVERROR_EOF {
        // One exhausted stream must not stop the demuxer while other streams
        // still have consumers (ffmpeg_demux.c:511-530 do_send).
        let ds = &mut demux_parameter.demux_streams[stream_index];
        if !ds.finished {
            debug!("All consumers of stream {stream_index} are done");
            ds.finished = true;
            demux_parameter.nb_streams_finished += 1;
        }
        if demux_parameter.nb_streams_finished == demux_parameter.nb_streams_used {
            debug!("All consumers are done");
            return AVERROR_EOF;
        }
        return 0;
    }
    ret
}

unsafe fn demux_send_for_stream(
    demux_parameter: &mut DemuxerParameter,
    packet_box: PacketBox,
    packet_pool: &ObjPool<Packet>,
    flags: usize,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> i32 {
    let stream_index = (*packet_box.packet.as_ptr()).stream_index as usize;

    // The precomputed per-stream destination list is read first, then the
    // parallel fields are borrowed disjointly: `dsts` / `dst_mux_gate` (shared
    // metadata) and `dsts_finished` (mutable) are different fields, so the hot
    // path indexes them instead of scanning + collecting every destination per
    // packet (PERF-7).
    let Some(send_dsts) = demux_parameter.per_stream_dsts.get(stream_index) else {
        packet_pool.release(packet_box.packet);
        return 0;
    };

    let dsts = &demux_parameter.dsts;
    let dst_mux_gate = &demux_parameter.dst_mux_gate;
    let dsts_finished = &mut demux_parameter.dsts_finished;

    // Discarded streams are already skipped at the demux loop top
    // (ffmpeg_demux.c:751); this backstop keeps an empty destination list
    // from counting as "all consumers done".
    if send_dsts.is_empty() {
        packet_pool.release(packet_box.packet);
        return 0;
    }

    let dst_count = send_dsts.len();
    let mut nb_done = 0usize;

    // First pass: count already-finished destinations and find the last still-
    // active one. A finished destination gets no packet_pool.get()/av_packet_ref
    // (NEW-DP-03) — the old path ref'd then immediately dropped for it.
    let mut move_pos = None;
    for (pos, &dst_i) in send_dsts.iter().enumerate() {
        if dsts_finished[dst_i] {
            nb_done += 1;
        } else {
            move_pos = Some(pos);
        }
    }

    let Some(move_pos) = move_pos else {
        debug_assert_eq!(nb_done, dst_count);
        packet_pool.release(packet_box.packet);
        return AVERROR_EOF;
    };

    // Second pass: send only active destinations. The last active one receives
    // the owned packet_box (move); the rest get an av_packet_ref'd copy.
    for (pos, &dst_i) in send_dsts.iter().enumerate() {
        if dsts_finished[dst_i] {
            continue;
        }

        let (packet_dst, _, output_stream_index) = &dsts[dst_i];
        let dst_finished = &mut dsts_finished[dst_i];

        if pos == move_pos {
            let ret = demux_stream_send_to_dst(
                packet_box,
                packet_dst,
                output_stream_index,
                dst_finished,
                dst_mux_gate[dst_i].as_ref(),
                flags,
                scheduler_status,
                scheduler_result,
            );
            if ret == AVERROR_EOF {
                nb_done += 1;
            } else if ret < 0 {
                return ret;
            }
            // EOF means THIS stream's consumers are all done: compare against
            // this stream's destination count, not every destination of the
            // demuxer — otherwise a stream whose consumers had all finished
            // kept the whole demuxer spinning (ffmpeg_sched.c
            // demux_send_for_stream: nb_done == ds->nb_dst).
            return if nb_done == dst_count { AVERROR_EOF } else { 0 };
        }

        let Ok(mut to_send) = packet_pool.get() else {
            packet_pool.release(packet_box.packet);
            return AVERROR(ffmpeg_sys_next::ENOMEM);
        };

        let packet_data = packet_box.packet_data;

        let mut ret = av_packet_ref(to_send.as_mut_ptr(), packet_box.packet.as_ptr());
        if ret < 0 {
            packet_pool.release(to_send);
            packet_pool.release(packet_box.packet);
            return ret;
        }

        ret = demux_stream_send_to_dst(
            PacketBox {
                packet: to_send,
                packet_data,
            },
            packet_dst,
            output_stream_index,
            dst_finished,
            dst_mux_gate[dst_i].as_ref(),
            flags,
            scheduler_status,
            scheduler_result,
        );
        if ret == AVERROR_EOF {
            nb_done += 1;
        } else if ret < 0 {
            packet_pool.release(packet_box.packet);
            return ret;
        }
    }

    unreachable!("move_pos was selected from a still-active destination")
}

const DEMUX_SEND_STREAMCOPY_EOF: usize = 1 << 0;

/// Non-EOF fatal sentinel: a streamcopy pre-mux queue stayed full past the
/// deadline. The demux loop must abort the job (the real error is recorded via
/// `set_scheduler_error` at the send site), NOT treat it as a normal per-stream
/// EOF — otherwise the copy stream would be silently truncated. Distinct from
/// `AVERROR_EOF` so `demux_send_for_stream` propagates it as a hard error.
const DEMUX_SEND_MUX_QUEUE_FULL: i32 = ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);

/// Outcome of routing one demux->mux packet to its destination.
enum DstSend {
    /// Delivered: parked pre-start, drained on the live queue, or accepted by a
    /// decoder channel.
    Delivered,
    /// Receiver gone — downstream completed. Benign: FFmpeg returns
    /// `AVERROR_EOF` silently here (ffmpeg_sched.c).
    Finished,
    /// A streamcopy pre-mux queue stayed full past the deadline: FATAL, the same
    /// "Too many packets buffered for output stream" an encoder reports.
    QueueFull,
}

/// Send `packet_box` to one destination. A streamcopy destination (`mux_gate`
/// `Some`) routes through the encoders' `MuxStartGate` + pre-mux queue, so a
/// dense copy stream parks pre-start and drains in DTS order instead of
/// blocking the shared single-threaded demuxer on the live bounded queue before
/// the muxer starts (that block was the deadlock: the demuxer could never reach
/// the packet that opens a co-muxed encoder). A decoder destination (`None`)
/// keeps the direct bounded send. fftools routes copy and encode through the
/// same `sch_send` path (ffmpeg_sched.c:2038-2077).
fn route_dst_send(
    packet_box: PacketBox,
    packet_dst: &Sender<PacketBox>,
    mux_gate: Option<&CopyMuxHandle>,
) -> DstSend {
    match mux_gate {
        Some(handle) => {
            match send_to_mux(packet_box, packet_dst, &handle.pre_sender, &handle.gate) {
                Ok(()) => DstSend::Delivered,
                Err(SendToMuxError::Disconnected(_)) => DstSend::Finished,
                Err(SendToMuxError::QueueFull(_)) => DstSend::QueueFull,
            }
        }
        None => match packet_dst.send(packet_box) {
            Ok(()) => DstSend::Delivered,
            Err(_) => DstSend::Finished,
        },
    }
}

unsafe fn demux_stream_send_to_dst(
    mut packet_box: PacketBox,
    packet_dst: &Sender<PacketBox>,
    output_stream_index: &Option<usize>,
    dst_finished: &mut bool,
    mux_gate: Option<&CopyMuxHandle>,
    flags: usize,
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> i32 {
    if *dst_finished {
        return AVERROR_EOF;
    }

    if !packet_is_null(&packet_box.packet)
        && output_stream_index.is_some()
        && (flags & DEMUX_SEND_STREAMCOPY_EOF) != 0
    {
        unsafe {
            (*packet_box.packet.as_mut_ptr()).stream_index = -1;
        }
        *dst_finished = true;
    }

    if let Some(output_stream_index) = output_stream_index {
        if (flags & DEMUX_SEND_STREAMCOPY_EOF) == 0 {
            (*packet_box.packet.as_mut_ptr()).stream_index = *output_stream_index as i32;
        }
        packet_box.packet_data.output_stream_index = *output_stream_index as i32;
        packet_box.packet_data.is_copy = true;
    }

    if *dst_finished {
        // Copy EOF marker (a real dts): still routed through the gate so it
        // drains in DTS order with the stream's data packets.
        match route_dst_send(packet_box, packet_dst, mux_gate) {
            // Receiver gone = downstream completed; a normal data-flow signal
            // (FFmpeg returns AVERROR_EOF silently here).
            DstSend::Delivered | DstSend::Finished => {}
            DstSend::QueueFull => {
                set_scheduler_error(
                    scheduler_status,
                    scheduler_result,
                    Encoding(EncodingOperationError::MuxQueueFull),
                );
                return DEMUX_SEND_MUX_QUEUE_FULL;
            }
        }

        return AVERROR_EOF;
    }

    match route_dst_send(packet_box, packet_dst, mux_gate) {
        DstSend::Delivered => 0,
        DstSend::Finished => {
            debug!("Demuxer dst finished, stop sending");
            *dst_finished = true;
            AVERROR_EOF
        }
        DstSend::QueueFull => {
            // Streamcopy pre-mux queue stayed full past the deadline: FATAL,
            // never a silent truncation. Record the error and return a non-EOF
            // fatal so the demux loop aborts the job.
            *dst_finished = true;
            set_scheduler_error(
                scheduler_status,
                scheduler_result,
                Encoding(EncodingOperationError::MuxQueueFull),
            );
            DEMUX_SEND_MUX_QUEUE_FULL
        }
    }
}

unsafe fn demux_flush(
    packet_pool: &ObjPool<Packet>,
    dsts: &Vec<(Sender<PacketBox>, usize, Option<usize>)>,
    dsts_finished: &mut [bool],
) -> i32 {
    // let ts = AV_NOPTS_VALUE;
    // let tb = AVRational{ num: 0, den: 0 };
    // let max_end_ts = Timestamp { ts: AV_NOPTS_VALUE, tb: AVRational { num: 0, den: 0 } };

    for (i, (packet_dst, _input_stream_index, output_stream_index)) in dsts.iter().enumerate() {
        // Finished and non-decoder destinations are skipped, like the
        // fftools flush loop (ffmpeg_sched.c:1979-1980): a consumer that
        // completed in an earlier pass must not fail the next loop.
        if dsts_finished[i] || output_stream_index.is_some() {
            continue;
        }

        let Ok(mut packet) = packet_pool.get() else {
            return AVERROR(ffmpeg_sys_next::ENOMEM);
        };
        (*packet.as_mut_ptr()).stream_index = -1;

        let packet_box = PacketBox {
            packet,
            packet_data: PacketData {
                dts_est: 0,
                codec_type: AVMediaType::AVMEDIA_TYPE_UNKNOWN,
                output_stream_index: 0,
                is_copy: false,
            },
        };

        if let Err(_) = packet_dst.send(packet_box) {
            debug!("Demuxer dst finished, skipping flush packet");
            dsts_finished[i] = true;
        }

        //TODO max_end_ts
    }

    0
}

#[cfg(test)]
mod tests {
    use ffmpeg_sys_next::{
        av_inv_q, av_rescale_q, AVRational, AV_NOPTS_VALUE, AV_TIME_BASE, AV_TIME_BASE_Q,
    };

    /// Apply ts_scale to a timestamp value.
    /// Returns the scaled timestamp, or AV_NOPTS_VALUE if input is AV_NOPTS_VALUE.
    ///
    /// This mirrors the ts_scale logic in FFmpeg CLI's `ffmpeg_demux.c`:
    /// - If ts_scale == 1.0, no change
    /// - Otherwise, multiply and truncate toward zero (same as C's int64_t *= double)
    fn apply_ts_scale(ts: i64, ts_scale: f64) -> i64 {
        if ts == AV_NOPTS_VALUE {
            return AV_NOPTS_VALUE;
        }
        if ts_scale == 1.0 {
            return ts;
        }
        (ts as f64 * ts_scale) as i64
    }

    #[test]
    fn ts_scale_default_no_change() {
        // ts_scale = 1.0 should not modify timestamps
        assert_eq!(apply_ts_scale(1000, 1.0), 1000);
        assert_eq!(apply_ts_scale(0, 1.0), 0);
        assert_eq!(apply_ts_scale(-1000, 1.0), -1000);
    }

    #[test]
    fn ts_scale_double() {
        // ts_scale = 2.0 should double timestamps
        assert_eq!(apply_ts_scale(1000, 2.0), 2000);
        assert_eq!(apply_ts_scale(500, 2.0), 1000);
        assert_eq!(apply_ts_scale(-500, 2.0), -1000);
    }

    #[test]
    fn ts_scale_half() {
        // ts_scale = 0.5 should halve timestamps (truncate toward zero)
        assert_eq!(apply_ts_scale(1000, 0.5), 500);
        assert_eq!(apply_ts_scale(1001, 0.5), 500); // truncates, not rounds
        assert_eq!(apply_ts_scale(-1000, 0.5), -500);
        assert_eq!(apply_ts_scale(-1001, 0.5), -500); // truncates toward zero
    }

    #[test]
    fn ts_scale_fractional() {
        // ts_scale = 1.5 should multiply and truncate
        assert_eq!(apply_ts_scale(100, 1.5), 150);
        assert_eq!(apply_ts_scale(101, 1.5), 151); // 101 * 1.5 = 151.5 -> 151
        assert_eq!(apply_ts_scale(-100, 1.5), -150);
        assert_eq!(apply_ts_scale(-101, 1.5), -151); // -101 * 1.5 = -151.5 -> -151
    }

    #[test]
    fn ts_scale_zero() {
        // ts_scale = 0.0 should make all timestamps zero
        assert_eq!(apply_ts_scale(1000, 0.0), 0);
        assert_eq!(apply_ts_scale(-1000, 0.0), 0);
        assert_eq!(apply_ts_scale(i64::MAX, 0.0), 0);
    }

    #[test]
    fn ts_scale_preserves_nopts() {
        // AV_NOPTS_VALUE should always be preserved
        assert_eq!(apply_ts_scale(AV_NOPTS_VALUE, 1.0), AV_NOPTS_VALUE);
        assert_eq!(apply_ts_scale(AV_NOPTS_VALUE, 2.0), AV_NOPTS_VALUE);
        assert_eq!(apply_ts_scale(AV_NOPTS_VALUE, 0.5), AV_NOPTS_VALUE);
        assert_eq!(apply_ts_scale(AV_NOPTS_VALUE, 0.0), AV_NOPTS_VALUE);
    }

    #[test]
    fn ts_scale_negative_scale() {
        // Negative scale should negate timestamps (truncate toward zero)
        assert_eq!(apply_ts_scale(1000, -1.0), -1000);
        assert_eq!(apply_ts_scale(-1000, -1.0), 1000);
        assert_eq!(apply_ts_scale(100, -0.5), -50);
    }

    #[test]
    fn ts_scale_large_values() {
        // Test with large timestamp values (common in media files)
        let large_pts: i64 = 90000 * 3600; // 1 hour at 90kHz timebase
        assert_eq!(apply_ts_scale(large_pts, 2.0), large_pts * 2);
        assert_eq!(apply_ts_scale(large_pts, 0.5), large_pts / 2);
    }

    #[test]
    fn ts_scale_nan_inf_behavior() {
        // Document Rust's behavior for NaN/Inf (differs from C):
        // - Rust `as i64` saturates: NaN -> 0, Inf -> i64::MAX, -Inf -> i64::MIN
        // - C behavior is undefined for these cases
        // These tests document the actual behavior, not necessarily "correct" behavior

        // NaN * any = NaN, Rust maps NaN to 0
        assert_eq!(apply_ts_scale(1000, f64::NAN), 0);

        // Inf * positive = Inf, Rust saturates to i64::MAX
        assert_eq!(apply_ts_scale(1000, f64::INFINITY), i64::MAX);

        // -Inf * positive = -Inf, Rust saturates to i64::MIN
        assert_eq!(apply_ts_scale(1000, f64::NEG_INFINITY), i64::MIN);

        // Inf * negative = -Inf
        assert_eq!(apply_ts_scale(-1000, f64::INFINITY), i64::MIN);
    }

    #[test]
    fn ts_scale_precision_edge_cases() {
        // Test near f64 precision limits (2^53 is max exact integer in f64)
        let near_precision_limit: i64 = (1i64 << 52) + 1; // Just above 2^52

        // At this scale, f64 can still represent the value exactly
        let result = apply_ts_scale(near_precision_limit, 1.0);
        assert_eq!(result, near_precision_limit);

        // Test with value that may lose precision when converted to f64
        let large_value: i64 = (1i64 << 53) + 1;
        // After f64 conversion and back, precision loss may occur
        let scaled = apply_ts_scale(large_value, 1.0);
        // With scale=1.0, we skip the conversion, so value is preserved
        assert_eq!(scaled, large_value);

        // With scale != 1.0, conversion happens and precision may be lost
        let scaled_2x = apply_ts_scale(large_value, 2.0);
        // The result may not be exactly large_value * 2 due to f64 precision
        // We just verify it's in a reasonable range
        assert!(scaled_2x > large_value);
    }

    #[test]
    fn ts_scale_overflow_saturation() {
        // Test overflow behavior: Rust saturates instead of wrapping
        // i64::MAX * 2.0 overflows, Rust saturates to i64::MAX
        assert_eq!(apply_ts_scale(i64::MAX, 2.0), i64::MAX);

        // i64::MIN * 2.0 overflows negative, Rust saturates to i64::MIN
        assert_eq!(apply_ts_scale(i64::MIN, 2.0), i64::MIN);

        // Large positive * large scale
        assert_eq!(apply_ts_scale(i64::MAX / 2, 3.0), i64::MAX);
    }

    /// Simulates packet with separate pts and dts handling
    /// This mirrors the actual code structure more closely
    fn apply_ts_scale_to_packet(pts: i64, dts: i64, ts_scale: f64) -> (i64, i64) {
        let new_pts = apply_ts_scale(pts, ts_scale);
        let new_dts = apply_ts_scale(dts, ts_scale);
        (new_pts, new_dts)
    }

    #[test]
    fn ts_scale_pts_dts_independent() {
        // Test that pts and dts are scaled independently
        // Case 1: Both valid
        let (pts, dts) = apply_ts_scale_to_packet(1000, 900, 2.0);
        assert_eq!(pts, 2000);
        assert_eq!(dts, 1800);

        // Case 2: Only pts is AV_NOPTS_VALUE
        let (pts, dts) = apply_ts_scale_to_packet(AV_NOPTS_VALUE, 900, 2.0);
        assert_eq!(pts, AV_NOPTS_VALUE);
        assert_eq!(dts, 1800);

        // Case 3: Only dts is AV_NOPTS_VALUE
        let (pts, dts) = apply_ts_scale_to_packet(1000, AV_NOPTS_VALUE, 2.0);
        assert_eq!(pts, 2000);
        assert_eq!(dts, AV_NOPTS_VALUE);

        // Case 4: Both are AV_NOPTS_VALUE
        let (pts, dts) = apply_ts_scale_to_packet(AV_NOPTS_VALUE, AV_NOPTS_VALUE, 2.0);
        assert_eq!(pts, AV_NOPTS_VALUE);
        assert_eq!(dts, AV_NOPTS_VALUE);
    }

    // --- Framerate DTS estimation tests ---
    //
    // Mirrors the VIDEO branch in `dts_estimated_process` (lines 566-572):
    //   if framerate.num != 0 {
    //       let next_dts = av_rescale_q(ds.next_dts, AV_TIME_BASE_Q, av_inv_q(framerate));
    //       ds.next_dts = av_rescale_q(next_dts + 1, av_inv_q(framerate), AV_TIME_BASE_Q);
    //   }

    /// Pure-function version of the framerate-based next_dts calculation.
    /// Given `current_dts` in AV_TIME_BASE units and a forced `framerate`,
    /// returns the next_dts after one frame.
    fn compute_next_dts_with_framerate(current_dts: i64, framerate: AVRational) -> i64 {
        assert!(framerate.num != 0, "framerate.num must be non-zero");
        let time_base_q = AV_TIME_BASE_Q;
        let inv_fr = unsafe { av_inv_q(framerate) };
        let next_dts = unsafe { av_rescale_q(current_dts, time_base_q, inv_fr) };
        unsafe { av_rescale_q(next_dts + 1, inv_fr, time_base_q) }
    }

    #[test]
    fn framerate_dts_30fps() {
        // 30 fps: each frame = 1/30 s = 33333.33.. us
        let fr = AVRational { num: 30, den: 1 };
        let next = compute_next_dts_with_framerate(0, fr);
        // Expected: ~33333 us (1/30 of AV_TIME_BASE)
        let expected = AV_TIME_BASE as i64 / 30;
        assert!(
            (next - expected).abs() <= 1,
            "30fps: next={next}, expected={expected}"
        );
    }

    #[test]
    fn framerate_dts_24000_1001() {
        // 23.976 fps (NTSC film): framerate = 24000/1001
        let fr = AVRational {
            num: 24000,
            den: 1001,
        };
        let next = compute_next_dts_with_framerate(0, fr);
        // Expected: 1001/24000 * 1_000_000 = 41708.33.. us
        let expected_us = (1001.0 / 24000.0 * AV_TIME_BASE as f64) as i64;
        assert!(
            (next - expected_us).abs() <= 1,
            "23.976fps: next={next}, expected~={expected_us}"
        );
    }

    #[test]
    fn framerate_dts_25fps() {
        // 25 fps (PAL): each frame = 40000 us
        let fr = AVRational { num: 25, den: 1 };
        let next = compute_next_dts_with_framerate(0, fr);
        assert_eq!(next, 40000, "25fps: next={next}, expected=40000");
    }

    #[test]
    fn framerate_dts_consecutive_frames() {
        // Simulate 3 consecutive frames at 30fps
        let fr = AVRational { num: 30, den: 1 };
        let dts0 = 0i64;
        let dts1 = compute_next_dts_with_framerate(dts0, fr);
        let dts2 = compute_next_dts_with_framerate(dts1, fr);
        let dts3 = compute_next_dts_with_framerate(dts2, fr);

        // Each frame should be ~33333us apart
        let frame_dur = AV_TIME_BASE as i64 / 30;
        assert!((dts1 - dts0 - frame_dur).abs() <= 1);
        assert!((dts2 - dts1 - frame_dur).abs() <= 1);
        assert!((dts3 - dts2 - frame_dur).abs() <= 1);
        // After 3 frames, should be close to 3 * frame_dur
        assert!(
            (dts3 - 3 * frame_dur).abs() <= 3,
            "3 frames at 30fps: dts3={dts3}, expected~={}",
            3 * frame_dur
        );
    }

    #[test]
    fn framerate_dts_nonzero_start() {
        // Start from a non-zero DTS (e.g., 1 second in)
        let fr = AVRational { num: 24, den: 1 };
        let start_dts = AV_TIME_BASE as i64; // 1 second
        let next = compute_next_dts_with_framerate(start_dts, fr);
        let expected = start_dts + AV_TIME_BASE as i64 / 24;
        assert!(
            (next - expected).abs() <= 1,
            "24fps from 1s: next={next}, expected~={expected}"
        );
    }

    #[test]
    fn framerate_dts_60fps() {
        // 60 fps: each frame = 16666.67 us
        let fr = AVRational { num: 60, den: 1 };
        let next = compute_next_dts_with_framerate(0, fr);
        let expected = AV_TIME_BASE as i64 / 60;
        assert!(
            (next - expected).abs() <= 1,
            "60fps: next={next}, expected={expected}"
        );
    }

    /// Mirrors the initial DTS calculation for first frame:
    ///   dts = ((-video_delay) * AV_TIME_BASE) as f64 / av_q2d(avg_frame_rate)
    fn compute_initial_dts(video_delay: i32, avg_frame_rate: AVRational) -> i64 {
        if avg_frame_rate.num != 0 {
            let fr_d = avg_frame_rate.num as f64 / avg_frame_rate.den as f64;
            ((-video_delay as i64 * AV_TIME_BASE as i64) as f64 / fr_d) as i64
        } else {
            0
        }
    }

    #[test]
    fn initial_dts_no_delay() {
        // No B-frames (video_delay=0): initial DTS should be 0
        let dts = compute_initial_dts(0, AVRational { num: 30, den: 1 });
        assert_eq!(dts, 0);
    }

    #[test]
    fn initial_dts_with_bframes() {
        // video_delay=1 (1 B-frame): initial DTS should be negative by one frame
        let fr = AVRational { num: 30, den: 1 };
        let dts = compute_initial_dts(1, fr);
        let expected = -(AV_TIME_BASE as i64 / 30);
        assert!(
            (dts - expected).abs() <= 1,
            "video_delay=1 at 30fps: dts={dts}, expected={expected}"
        );
    }

    #[test]
    fn initial_dts_no_framerate() {
        // avg_frame_rate.num == 0: falls back to 0
        let dts = compute_initial_dts(1, AVRational { num: 0, den: 1 });
        assert_eq!(dts, 0);
    }

    // Architecture Y' scan: a `-shortest` follower's `source_finished` retires
    // only ITS destination (per-destination), and its input stream finishes only
    // when every one of that stream's destinations is done.
    #[test]
    fn source_finished_scan_is_per_destination() {
        use super::scan_source_finished_dsts;
        use crate::core::context::PacketBox;
        use crossbeam_channel::{bounded, Sender};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let sender = || -> Sender<PacketBox> { bounded::<PacketBox>(1).0 };

        // Input stream 0 -> dst0 (shortest, flagged) AND dst1 (plain, no flag).
        // Input stream 1 -> dst2 (shortest, flagged).
        let f0 = Arc::new(AtomicBool::new(false));
        let f2 = Arc::new(AtomicBool::new(false));
        let dst_source_finished: Vec<Option<Arc<AtomicBool>>> =
            vec![Some(f0.clone()), None, Some(f2.clone())];
        let dsts: Vec<(Sender<PacketBox>, usize, Option<usize>)> = vec![
            (sender(), 0, Some(0)),
            (sender(), 0, Some(1)),
            (sender(), 1, Some(2)),
        ];
        let per_stream_dsts: Vec<Vec<usize>> = vec![vec![0, 1], vec![2]];
        let mut dsts_finished = vec![false; 3];

        // Nothing signalled: no-op.
        let mut finished = Vec::new();
        scan_source_finished_dsts(
            &dst_source_finished,
            &dsts,
            &per_stream_dsts,
            &mut dsts_finished,
            &mut finished,
        );
        assert!(finished.is_empty());
        assert_eq!(dsts_finished, vec![false, false, false]);

        // Flag stream 0's follower: only dst0 retires; the plain co-copy (dst1)
        // stays live, so stream 0 is NOT finished (per-destination isolation).
        f0.store(true, Ordering::Release);
        let mut finished = Vec::new();
        scan_source_finished_dsts(
            &dst_source_finished,
            &dsts,
            &per_stream_dsts,
            &mut dsts_finished,
            &mut finished,
        );
        assert!(
            finished.is_empty(),
            "co-copy still live -> stream 0 not finished"
        );
        assert_eq!(dsts_finished, vec![true, false, false]);

        // Flag stream 1's follower: its sole dst retires -> stream 1 finishes.
        f2.store(true, Ordering::Release);
        let mut finished = Vec::new();
        scan_source_finished_dsts(
            &dst_source_finished,
            &dsts,
            &per_stream_dsts,
            &mut dsts_finished,
            &mut finished,
        );
        assert_eq!(finished, vec![1]);
        assert_eq!(dsts_finished, vec![true, false, true]);
    }

    // Two followers on the SAME input stream both flip in one scan: the stream is
    // reported finished exactly once (not once per destination).
    #[test]
    fn source_finished_scan_reports_stream_once() {
        use super::scan_source_finished_dsts;
        use crate::core::context::PacketBox;
        use crossbeam_channel::{bounded, Sender};
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let sender = || -> Sender<PacketBox> { bounded::<PacketBox>(1).0 };

        // Input stream 0 -> dst0, dst1 (two shortest outputs), both flagged.
        let fa = Arc::new(AtomicBool::new(true));
        let fb = Arc::new(AtomicBool::new(true));
        let dst_source_finished = vec![Some(fa), Some(fb)];
        let dsts: Vec<(Sender<PacketBox>, usize, Option<usize>)> =
            vec![(sender(), 0, Some(0)), (sender(), 0, Some(1))];
        let per_stream_dsts: Vec<Vec<usize>> = vec![vec![0, 1]];
        let mut dsts_finished = vec![false; 2];

        let mut finished = Vec::new();
        scan_source_finished_dsts(
            &dst_source_finished,
            &dsts,
            &per_stream_dsts,
            &mut dsts_finished,
            &mut finished,
        );
        assert_eq!(finished, vec![0], "stream 0 finished once, not per-dst");
        assert_eq!(dsts_finished, vec![true, true]);

        // A second scan reports nothing (both dsts already retired).
        let mut finished = Vec::new();
        scan_source_finished_dsts(
            &dst_source_finished,
            &dsts,
            &per_stream_dsts,
            &mut dsts_finished,
            &mut finished,
        );
        assert!(finished.is_empty());
    }
}
