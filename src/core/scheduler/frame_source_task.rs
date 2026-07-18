//! Counted frame-source worker for [`VideoWriter`](crate::VideoWriter) jobs:
//! turns tightly packed byte buffers from the facade's ingress channel into
//! pool-backed CFR `AVFrame`s and pushes them into the filtergraph's bounded
//! frame channel — exactly where a decoder would.
//!
//! Teardown contract (the load-bearing part):
//! - The worker claims a `thread_sync` slot BEFORE spawn and releases it via
//!   `ThreadDoneGuard` on every exit path, so `wait()`/`abort()`/the guards
//!   join it like any other counted worker.
//! - Both channel directions are time-boxed: `recv_timeout(100ms)` on ingress
//!   and `send_timeout(100ms)` on the filter channel, each followed by an
//!   `is_stopping` poll. The worker therefore self-wakes on `STATUS_END`/
//!   `STATUS_ABORT` within one poll interval and needs no entry in any
//!   demux-keyed wake set.
//! - End of stream is an explicit in-band EOF marker (a `null_frame()`
//!   `FrameBox`), enqueued on healthy ingress close BEFORE the filtergraph
//!   sender drops. Sender disconnection alone does NOT close the buffersrc
//!   (`filter_task` merely breaks its loop and flushes outputs), which would
//!   lose frames still buffered inside filters like `reverse` and skip the
//!   zero-frame fallback configuration. The null marker closes the source at
//!   its accumulated frame-end time — exact for this CFR source, because
//!   every pushed frame carries `duration = 1` tick.
//! - Frames come from the shared scheduler `frame_pool` so shells recycle
//!   (`ObjPool::release` runs `av_frame_unref` before storing, so a reused
//!   shell carries no stale reference); a locally built frame references no
//!   scheduler-owned state, so a `FrameBox` still queued at teardown frees
//!   safely whenever the last channel endpoint drops.

use crate::core::context::frame_source::{FrameSource, FrameSourceParams};
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::{null_frame, FrameBox, FrameData};
use crate::core::scheduler::ffmpeg_scheduler::{
    is_stopping, set_scheduler_error, wait_until_not_paused,
};
use crate::error::{AllocFrameError, Error};
use crate::util::ffmpeg_utils::av_err2str;
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::{RecvTimeoutError, SendTimeoutError, Sender};
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{av_frame_get_buffer, av_image_copy, av_image_fill_arrays, AVRational};
use log::{debug, error};
use std::ptr::null_mut;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Spawns the counted frame-source worker. Called by `start()` AFTER every
/// consumer (filter/encoder/mux) exists, inside the `StartFailGuard` window:
/// the pre-claimed slot is joined on any start failure, and the worker's
/// status polls make that join terminate without any wake-set entry.
pub(crate) fn frame_source_init(
    index: usize,
    frame_source: FrameSource,
    frame_pool: ObjPool<Frame>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    // Slot claimed before spawn; the guard releases it on any exit path.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(
        thread_sync.clone(),
        scheduler_status.clone(),
        scheduler_result.clone(),
    );

    let result = std::thread::Builder::new()
        .name(format!("framesource{index}"))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            // The channel endpoints and pool are `move`-closure captures, but
            // `_thread_done` is a body local. Rust drops body locals BEFORE
            // captures, so without this rebind the guard would release the
            // thread slot (the counter wait()/stop() gate on) before the
            // endpoints dropped. Rebinding them as body locals declared AFTER
            // the guard makes them drop BEFORE it on every exit path — the
            // same ordering the filter worker relies on (filter_task).
            let ingress = frame_source.ingress;
            let fg_sender = frame_source.fg_sender;
            let params = frame_source.params;
            let frame_pool = frame_pool;

            let mut nb_frames: i64 = 0;
            loop {
                let result = ingress.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    debug!("Frame source received end command, finishing.");
                    return;
                }

                let data = match result {
                    Ok(data) => data,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                };

                let frame = match build_video_frame(&frame_pool, &params, &data, nb_frames) {
                    Ok(frame) => frame,
                    Err(e) => {
                        error!("Frame source failed to build a frame: {e}");
                        set_scheduler_error(&scheduler_status, &scheduler_result, e);
                        return;
                    }
                };
                nb_frames += 1;

                let frame_box = FrameBox {
                    frame,
                    frame_data: frame_data_for(&params),
                };
                if !send_with_status_poll(&fg_sender, frame_box, &scheduler_status, &frame_pool) {
                    return;
                }
            }

            // Healthy ingress close (finish()/Drop dropped the facade sender,
            // all queued frames drained above): enqueue the explicit EOF
            // marker BEFORE this worker's fg_sender drops. This is the only
            // EOF mechanism — see the module docs for why sender-drop alone
            // is not one. An abort/stop racing in suppresses the marker via
            // the status poll inside the send loop, which is teardown-benign.
            let eof_marker = FrameBox {
                frame: null_frame(),
                frame_data: frame_data_for(&params),
            };
            send_with_status_poll(&fg_sender, eof_marker, &scheduler_status, &frame_pool);
            debug!("Frame source finished after {nb_frames} frame(s).");
        });
    if let Err(e) = result {
        // The failed spawn dropped the closure and its captures on this
        // thread: the guard released the pre-claimed slot, the channel
        // endpoints closed.
        error!("Frame source thread exited with error: {e}");
        return Err(Error::FrameSourceThreadExited);
    }

    Ok(())
}

/// Sends one `FrameBox` into the bounded filtergraph channel, re-checking the
/// scheduler status every 100 ms while the channel is full. Returns `false`
/// when the job is stopping or the filter worker is gone; the undelivered
/// frame goes back to the pool (a no-op for the null EOF marker).
fn send_with_status_poll(
    sender: &Sender<FrameBox>,
    mut frame_box: FrameBox,
    scheduler_status: &Arc<AtomicUsize>,
    frame_pool: &ObjPool<Frame>,
) -> bool {
    loop {
        match sender.send_timeout(frame_box, Duration::from_millis(100)) {
            Ok(()) => return true,
            Err(SendTimeoutError::Timeout(returned)) => {
                if is_stopping(wait_until_not_paused(scheduler_status)) {
                    debug!("Frame source received end command while sending.");
                    frame_pool.release(returned.frame);
                    return false;
                }
                frame_box = returned;
            }
            Err(SendTimeoutError::Disconnected(returned)) => {
                debug!("Frame source: filtergraph receiver is gone.");
                frame_pool.release(returned.frame);
                return false;
            }
        }
    }
}

fn frame_data_for(params: &FrameSourceParams) -> FrameData {
    FrameData {
        framerate: Some(AVRational {
            num: params.fps_num,
            den: params.fps_den,
        }),
        bits_per_raw_sample: 0,
        input_stream_width: params.width,
        input_stream_height: params.height,
        subtitle_header: None,
        // Validated single-input graph: the source always feeds pad 0.
        fg_input_index: 0,
        side_data: None,
    }
}

/// Builds one CFR video frame from a tightly packed byte buffer.
///
/// The shell comes from the shared pool (unref'd: no format, no buffers), so
/// `av_frame_get_buffer` allocates fresh writable planes with its own row
/// alignment and inter-plane padding. A flat memcpy of the tight user buffer
/// would therefore interleave rows with padding garbage; instead
/// `av_image_fill_arrays` lays the descriptor's plane pointers/linesizes over
/// the tight source and `av_image_copy` copies plane by plane honoring both
/// linesizes.
///
/// Stamping: `pts = ordinal`, `duration = 1` tick, `time_base = fps_den/fps_num`
/// — every frame advances exactly one frame interval (CFR contract).
fn build_video_frame(
    frame_pool: &ObjPool<Frame>,
    params: &FrameSourceParams,
    data: &[u8],
    pts: i64,
) -> crate::error::Result<Frame> {
    let mut frame = frame_pool.get()?;
    // SAFETY: `frame` is a live unref'd AVFrame shell owned by this function;
    // dimensions/format were validated at open(); `data` outlives the copy and
    // its length was validated against the tight layout of exactly these
    // parameters (frame_size), which av_image_fill_arrays recomputes here.
    unsafe {
        let f = frame.as_mut_ptr();
        (*f).format = params.pix_fmt as i32;
        (*f).width = params.width;
        (*f).height = params.height;
        let ret = av_frame_get_buffer(f, 0);
        if ret < 0 {
            error!("av_frame_get_buffer failed: {}", av_err2str(ret));
            frame_pool.release(frame);
            return Err(AllocFrameError::OutOfMemory.into());
        }

        let mut src_data: [*mut u8; 4] = [null_mut(); 4];
        let mut src_linesize: [libc::c_int; 4] = [0; 4];
        let ret = av_image_fill_arrays(
            src_data.as_mut_ptr(),
            src_linesize.as_mut_ptr(),
            data.as_ptr(),
            params.pix_fmt,
            params.width,
            params.height,
            1,
        );
        if ret < 0 {
            // Unreachable after open()-time validation of format and
            // dimensions; surface an invariant break rather than garbage.
            error!("av_image_fill_arrays failed: {}", av_err2str(ret));
            frame_pool.release(frame);
            return Err(Error::Bug);
        }

        av_image_copy(
            (*f).data.as_ptr(),
            (*f).linesize.as_ptr(),
            src_data.as_ptr() as *const *const u8,
            src_linesize.as_ptr(),
            params.pix_fmt,
            params.width,
            params.height,
        );

        (*f).pts = pts;
        (*f).duration = 1;
        (*f).time_base = AVRational {
            num: params.fps_den,
            den: params.fps_num,
        };
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::scheduler::ffmpeg_scheduler::{frame_is_null, unref_frame};
    use ffmpeg_sys_next::AVPixelFormat::{AV_PIX_FMT_GRAY8, AV_PIX_FMT_YUV420P};
    use ffmpeg_sys_next::{av_frame_alloc, av_image_get_buffer_size};

    fn test_new_frame() -> crate::error::Result<Frame> {
        let f = unsafe { av_frame_alloc() };
        assert!(!f.is_null(), "av_frame_alloc failed in test");
        Ok(unsafe { Frame::wrap(f) })
    }

    fn test_pool() -> ObjPool<Frame> {
        ObjPool::new(1, test_new_frame, unref_frame, frame_is_null).expect("frame pool")
    }

    fn params(pix_fmt: ffmpeg_sys_next::AVPixelFormat, w: i32, h: i32) -> FrameSourceParams {
        FrameSourceParams {
            width: w,
            height: h,
            pix_fmt,
            fps_num: 30,
            fps_den: 1,
        }
    }

    fn tight_size(pix_fmt: ffmpeg_sys_next::AVPixelFormat, w: i32, h: i32) -> usize {
        unsafe { av_image_get_buffer_size(pix_fmt, w, h, 1) as usize }
    }

    /// Odd-width gray8: the tight stride (65) differs from the padded frame
    /// linesize, so a flat memcpy would shear rows. Verify every row landed at
    /// its linesize offset with its exact content.
    #[test]
    fn fill_respects_linesize_for_odd_width() {
        let pool = test_pool();
        let p = params(AV_PIX_FMT_GRAY8, 65, 3);
        let data: Vec<u8> = (0..tight_size(p.pix_fmt, 65, 3))
            .map(|i| (i % 251) as u8)
            .collect();
        let frame = build_video_frame(&pool, &p, &data, 7).expect("build");
        unsafe {
            let f = frame.as_ptr();
            assert!((*f).linesize[0] >= 65, "padded linesize expected");
            for y in 0..3usize {
                let row =
                    std::slice::from_raw_parts((*f).data[0].add(y * (*f).linesize[0] as usize), 65);
                assert_eq!(row, &data[y * 65..y * 65 + 65], "row {y} content");
            }
            assert_eq!((*f).pts, 7);
            assert_eq!((*f).duration, 1);
            assert_eq!((*f).time_base.num, 1);
            assert_eq!((*f).time_base.den, 30);
        }
    }

    /// Planar odd-geometry yuv420p (65x49): all three planes must land intact
    /// with chroma dimensions ceil(w/2) x ceil(h/2).
    #[test]
    fn fill_copies_all_planes_for_odd_yuv420p() {
        let pool = test_pool();
        let (w, h) = (65i32, 49i32);
        let p = params(AV_PIX_FMT_YUV420P, w, h);
        let (cw, ch) = (33usize, 25usize);
        let y_size = (w * h) as usize;
        let c_size = cw * ch;
        let mut data = vec![0u8; tight_size(p.pix_fmt, w, h)];
        assert_eq!(data.len(), y_size + 2 * c_size);
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i * 7 % 253) as u8;
        }
        let frame = build_video_frame(&pool, &p, &data, 0).expect("build");
        unsafe {
            let f = frame.as_ptr();
            let planes = [
                (0usize, w as usize, h as usize, 0usize),
                (1, cw, ch, y_size),
                (2, cw, ch, y_size + c_size),
            ];
            for (idx, pw, ph, base) in planes {
                let ls = (*f).linesize[idx] as usize;
                assert!(ls >= pw, "plane {idx} linesize");
                for y in 0..ph {
                    let row = std::slice::from_raw_parts((*f).data[idx].add(y * ls), pw);
                    assert_eq!(
                        row,
                        &data[base + y * pw..base + y * pw + pw],
                        "plane {idx} row {y}"
                    );
                }
            }
        }
    }

    /// A recycled shell (released with buffers attached) must come back clean
    /// and refill correctly — the pool's unref_fn is what discharges the old
    /// buffers.
    #[test]
    fn recycled_shell_refills_cleanly() {
        let pool = test_pool();
        let p = params(AV_PIX_FMT_GRAY8, 8, 2);
        let data_a = vec![0xAA; tight_size(p.pix_fmt, 8, 2)];
        let frame = build_video_frame(&pool, &p, &data_a, 0).expect("first build");
        pool.release(frame); // unrefs, stores the shell
        let data_b = vec![0x55; tight_size(p.pix_fmt, 8, 2)];
        let frame = build_video_frame(&pool, &p, &data_b, 1).expect("recycled build");
        unsafe {
            let f = frame.as_ptr();
            let row = std::slice::from_raw_parts((*f).data[0], 8);
            assert_eq!(row, &data_b[..8]);
            assert_eq!((*f).pts, 1);
        }
    }
}
