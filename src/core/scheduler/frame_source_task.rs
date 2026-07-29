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
//! - Plane buffers recycle too, through a worker-local `PlanePool`; worker
//!   exit uninits the pool, which is safe while downstream stages (or queued
//!   `FrameBox`es) still hold pooled buffers — see the `PlanePool` docs.

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
use ffmpeg_sys_next::{
    av_buffer_pool_get, av_buffer_pool_init, av_buffer_pool_uninit, av_frame_get_buffer,
    av_image_copy, av_image_fill_arrays, AVBufferPool, AVFrame, AVRational,
};
use log::{debug, error};
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Marks this frame source as done producing on EVERY worker exit path
/// (drained, stopped, filter gone, build error, panic): the progress API
/// reports `Finishing` only once every input producer has retired, so the
/// flag must never be leaked by an early return. Declared after the
/// `ThreadDoneGuard` rebind in the worker, so it flips BEFORE the thread
/// slot is released.
struct MarkExitedOnDrop(Arc<AtomicBool>);

impl Drop for MarkExitedOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

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
    producer_exited: Arc<AtomicBool>,
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
            let _thread_done = thread_done_guard.activate();
            // Progress producer-exit flag: body local declared AFTER the
            // thread-done guard, so on every exit (including unwind) it
            // flips BEFORE the slot release the guard performs.
            let _producer_exited = MarkExitedOnDrop(producer_exited);
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

            // Recycles the large plane buffers across frames; armed lazily
            // from the first built frame, so zero-frame jobs never touch it.
            // A body local like the endpoints above: it drops (pool uninit)
            // before the guard on every exit path.
            let mut plane_pool = PlanePool::empty();

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

                let frame = match build_video_frame(
                    &frame_pool,
                    &mut plane_pool,
                    &params,
                    &data,
                    nb_frames,
                ) {
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

            // Healthy ingress close (finish() dropped the facade sender, all
            // queued frames drained above): enqueue the explicit EOF
            // marker BEFORE this worker's fg_sender drops. This is the only
            // EOF mechanism — see the module docs for why sender-drop alone
            // is not one. An abort/stop racing in — including the facade's
            // Drop/abort, which closes ingress and immediately aborts —
            // usually suppresses the marker via the status poll inside the
            // send loop. That poll only re-checks status after a send times
            // out, though, so a marker that slips through the race window
            // still lands in a pipeline that is itself already tearing
            // down — benign either way.
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

/// Per-job recycler for the packed plane buffers `build_video_frame` fills,
/// replacing a multi-MiB `av_buffer_alloc` + first-touch page faults + free
/// per pushed frame with reuse from a lock-free `AVBufferPool`. Armed lazily
/// from the FIRST frame's `av_frame_get_buffer` product — that frame is the
/// layout template, so zero-frame jobs never touch FFmpeg here — and pooled
/// frames reproduce the captured layout verbatim.
///
/// Keyed to the open()-fixed `FrameSourceParams`: the ingress channel
/// carries bare bytes, so a mid-job geometry change is structurally
/// impossible today. If a future ingress variant adds per-frame geometry,
/// recreate on mismatch — uninit the old pool (outstanding buffers are
/// unaffected) and re-arm from the next template frame, as the wgpu
/// `OutputFramePool` does per geometry key.
///
/// The pool only parks buffers that were once simultaneously in flight (the
/// bounded filter channel and filter/encoder holds pin the same buffers at
/// peak today), so peak live-buffer count is unchanged; RSS holds near that
/// high-water mark between frames instead of sawtoothing. Parked buffers
/// free at worker exit (`Drop`), in-flight ones at their last unref.
///
/// Deliberately `!Send` (`NonNull` field): used only on the worker thread.
/// Other threads release pooled buffers through FFmpeg's refcounting —
/// thread-safe with the default pool allocator — never through this struct.
struct PlanePool {
    /// Live pool once armed; `None` before the first frame and after a
    /// failed arming (permanent fallback to `av_frame_get_buffer`).
    pool: Option<NonNull<AVBufferPool>>,
    /// Template linesizes, captured verbatim.
    linesize: [i32; 4],
    /// Byte offset of each present plane inside the single packed buffer
    /// (`None` for planes the format does not use). Captured, never derived:
    /// this inherits whatever `get_video_buffer` did — plane padding bumps,
    /// absolute alignment, in-buffer palettes like pal8's.
    offset: [Option<usize>; 4],
    /// Flips on the one-shot arming attempt, success or not.
    armed: bool,
    /// Builds served from the pool. Test-only probe mirroring
    /// `ObjPool::idle_count`: reuse assertions must not trust pointer
    /// equality alone, which allocator address reuse can satisfy.
    #[cfg(test)]
    pooled_builds: usize,
}

impl PlanePool {
    /// Pool that has seen no frame: zero FFmpeg calls and a no-op `Drop`, so
    /// a job that ends before any push allocates exactly like today.
    fn empty() -> Self {
        PlanePool {
            pool: None,
            linesize: [0; 4],
            offset: [None; 4],
            armed: false,
            #[cfg(test)]
            pooled_builds: 0,
        }
    }

    /// The init-failure end state (armed, no pool): a real
    /// `av_buffer_pool_init` failure is not injectable through FFmpeg, so
    /// tests construct its aftermath directly.
    #[cfg(test)]
    fn disarmed() -> Self {
        PlanePool {
            armed: true,
            ..Self::empty()
        }
    }

    /// Attaches a pooled plane buffer to the unref'd shell `f`, reproducing
    /// the captured template layout. `Ok(false)`: pool not armed (first
    /// frame, or permanent fallback) — the caller must allocate via
    /// `av_frame_get_buffer`. `Err`: the pool's internal allocation failed,
    /// the same OOM the fallback path would surface.
    ///
    /// # Safety
    /// `f` must point to a live, unref'd `AVFrame` shell (no buffers, data
    /// pointers null), exactly what `ObjPool<Frame>` hands out.
    unsafe fn attach(&mut self, f: *mut AVFrame) -> Result<bool, AllocFrameError> {
        let Some(pool) = self.pool else {
            return Ok(false);
        };
        // SAFETY: av_buffer_pool_get is thread-safe by contract and returns
        // a refcount-1 WRITABLE ref — the same writability state
        // av_frame_get_buffer yields via av_buffer_alloc (no READONLY flag on
        // this path), so downstream av_frame_make_writable decisions are
        // unchanged. The offsets were captured from a real same-size buffer
        // laid out by FFmpeg itself, so base + offset stays in bounds; the
        // default pool allocator IS av_buffer_alloc — the template's own
        // allocator — so the base alignment class transfers.
        let buf = av_buffer_pool_get(pool.as_ptr());
        if buf.is_null() {
            return Err(AllocFrameError::OutOfMemory);
        }
        (*f).buf[0] = buf;
        let base = (*buf).data;
        for i in 0..4 {
            (*f).linesize[i] = self.linesize[i];
            if let Some(off) = self.offset[i] {
                (*f).data[i] = base.add(off);
            }
        }
        // Defensive mirror of get_video_buffer (an unref'd shell already has
        // this default).
        (*f).extended_data = (*f).data.as_mut_ptr();
        #[cfg(test)]
        {
            self.pooled_builds += 1;
        }
        Ok(true)
    }

    /// One-shot arming from the first template-built frame: captures the
    /// buffer size, linesizes, and per-plane offsets, then initialises the
    /// pool. Any failure leaves `pool == None` with `armed == true` — a
    /// silent, permanent fallback to per-frame allocation, adding no error
    /// identity the unpooled path doesn't already have.
    ///
    /// # Safety
    /// `f` must point to a live `AVFrame` that just succeeded
    /// `av_frame_get_buffer` for the job's fixed params.
    unsafe fn try_arm_from(&mut self, f: *const AVFrame) {
        if self.armed {
            return;
        }
        self.armed = true;
        let buf0 = (*f).buf[0];
        if buf0.is_null() {
            return;
        }
        let base = (*buf0).data as usize;
        // AVBufferRef.size is c_int in older FFmpeg majors' bindings and
        // size_t in newer ones. Widen through i128 for the sign check and
        // the usize bounds copy so both bindings compile; keep the native
        // value for av_buffer_pool_init, whose parameter tracks the same
        // type.
        let buf_size = (*buf0).size;
        let size_wide = buf_size as i128;
        if size_wide <= 0 {
            return;
        }
        let buf_extent = size_wide as usize;
        let mut offset = [None; 4];
        for (i, slot) in offset.iter_mut().enumerate() {
            let d = (*f).data[i];
            if d.is_null() {
                continue;
            }
            // get_video_buffer packs every plane (palette included) into the
            // single buf[0]; any other layout is one this pool cannot
            // reproduce, so stay on the fallback.
            match (d as usize).checked_sub(base) {
                Some(off) if off < buf_extent => *slot = Some(off),
                _ => return,
            }
        }
        // SAFETY: the default alloc callback (None) keeps FFmpeg's documented
        // lock-free thread-safety for pool get and buffer release; buf_size
        // is the template buffer's own size.
        let pool = av_buffer_pool_init(buf_size, None);
        let Some(pool) = NonNull::new(pool) else {
            // Tiny pool-struct OOM while the big frame alloc succeeded:
            // today's path has no pool at all, so this maps to plain success.
            debug!("av_buffer_pool_init failed; frame source keeps per-frame plane buffers");
            return;
        };
        self.linesize = [
            (*f).linesize[0],
            (*f).linesize[1],
            (*f).linesize[2],
            (*f).linesize[3],
        ];
        self.offset = offset;
        self.pool = Some(pool);
    }
}

impl Drop for PlanePool {
    fn drop(&mut self) {
        if let Some(pool) = self.pool {
            // SAFETY: `pool` came from av_buffer_pool_init and is released
            // exactly once. uninit only MARKS the pool freeable — documented
            // safe while buffers are still in use — and the pool frees itself
            // when the last outstanding buffer returns. So the worker can
            // exit while filter/encoder still hold pooled frames or
            // undelivered FrameBoxes sit in the bounded channel; their
            // eventual unref on any thread is thread-safe with the default
            // allocator in use here.
            unsafe {
                let mut p = pool.as_ptr();
                av_buffer_pool_uninit(&mut p);
            }
        }
    }
}

/// Builds one CFR video frame from a tightly packed byte buffer.
///
/// The shell comes from the shared pool (unref'd: no format, no buffers).
/// The first frame allocates fresh writable planes via `av_frame_get_buffer`
/// — FFmpeg's own row alignment and inter-plane padding — and arms
/// `plane_pool` with that exact layout; later frames attach a recycled
/// buffer reproducing it (see [`PlanePool`]). Either way the planes are
/// padded, so a flat memcpy of the tight user buffer would interleave rows
/// with padding garbage; instead `av_image_fill_arrays` lays the
/// descriptor's plane pointers/linesizes over the tight source and
/// `av_image_copy` copies plane by plane honoring both linesizes — shared,
/// unchanged code for both buffer paths.
///
/// Stamping: `pts = ordinal`, `duration = 1` tick, `time_base = fps_den/fps_num`
/// — every frame advances exactly one frame interval (CFR contract).
fn build_video_frame(
    frame_pool: &ObjPool<Frame>,
    plane_pool: &mut PlanePool,
    params: &FrameSourceParams,
    data: &[u8],
    pts: i64,
) -> crate::error::Result<Frame> {
    let mut frame = frame_pool.get()?;
    // SAFETY: `frame` is a live unref'd AVFrame shell owned by this function;
    // dimensions/format were validated at open(); `data` outlives the copy and
    // its length was validated against the tight layout of exactly these
    // parameters (frame_size), which av_image_fill_arrays recomputes here.
    // A pooled buffer carries the template's size and captured offsets, so
    // every plane write av_image_copy performs stays in bounds exactly as it
    // did for the template frame.
    unsafe {
        let f = frame.as_mut_ptr();
        (*f).format = params.pix_fmt as i32;
        (*f).width = params.width;
        (*f).height = params.height;
        match plane_pool.attach(f) {
            Ok(true) => {}
            Ok(false) => {
                // Template/fallback path: fresh planes from FFmpeg; the
                // first success arms the pool with the real layout.
                let ret = av_frame_get_buffer(f, 0);
                if ret < 0 {
                    error!("av_frame_get_buffer failed: {}", av_err2str(ret));
                    frame_pool.release(frame);
                    return Err(AllocFrameError::OutOfMemory.into());
                }
                plane_pool.try_arm_from(f);
            }
            Err(e) => {
                // Pool get fails only when its internal av_buffer_alloc
                // fails — the same OOM the path above surfaces.
                error!("av_buffer_pool_get returned null");
                frame_pool.release(frame);
                return Err(e.into());
            }
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
    use ffmpeg_sys_next::AVPixelFormat::{
        AV_PIX_FMT_GRAY8, AV_PIX_FMT_NV12, AV_PIX_FMT_PAL8, AV_PIX_FMT_YUV420P, AV_PIX_FMT_YUVA420P,
    };
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

    /// Row-walks every plane of `frame` against the tight align=1 source
    /// `data`, whose per-plane geometry is `planes = [(bytes_per_row, rows)]`
    /// in buffer order (pal8's palette is one 1024-byte row).
    fn assert_frame_planes(frame: &Frame, planes: &[(usize, usize)], data: &[u8], ctx: &str) {
        unsafe {
            let f = frame.as_ptr();
            let mut base = 0usize;
            for (idx, &(bpr, rows)) in planes.iter().enumerate() {
                let ls = (*f).linesize[idx] as usize;
                for y in 0..rows {
                    let row = std::slice::from_raw_parts((*f).data[idx].add(y * ls), bpr);
                    assert_eq!(
                        row,
                        &data[base + y * bpr..base + (y + 1) * bpr],
                        "{ctx}: plane {idx} row {y}"
                    );
                }
                base += bpr * rows;
            }
            assert_eq!(base, data.len(), "{ctx}: plane table must cover the source");
        }
    }

    /// Asserts the frame's planes land inside its packed `buf[0]` without
    /// overlap: each active plane's span `[offset, offset + rows*linesize)`
    /// stays within the buffer extent and follows its predecessor's span in
    /// buffer order. This is the address-independent core of the layout
    /// contract — exact offsets depend on where the allocator placed the
    /// buffer (see `pool_fallback_builds_identical_frames`), but bounds,
    /// ordering, and disjointness never do.
    fn assert_plane_spans_disjoint(frame: &Frame, planes: &[(usize, usize)], ctx: &str) {
        let (offsets, linesize, extent, _, _) = frame_layout(frame);
        let mut prev_end = 0usize;
        for (idx, &(bpr, rows)) in planes.iter().enumerate() {
            let off = offsets[idx].unwrap_or_else(|| panic!("{ctx}: plane {idx} missing"));
            let ls = linesize[idx] as usize;
            assert!(bpr <= ls, "{ctx}: plane {idx} row wider than its linesize");
            assert!(
                off >= prev_end,
                "{ctx}: plane {idx} at {off} overlaps its predecessor ending at {prev_end}"
            );
            let end = off + rows * ls;
            assert!(
                (end as i128) <= extent,
                "{ctx}: plane {idx} span ends at {end}, past the buffer extent {extent}"
            );
            prev_end = end;
        }
    }

    /// Captures a frame's buffer-layout contract: the active-plane mask with
    /// each active plane's offset from `buf[0].data`, all linesizes, the
    /// buffer extent, `extended_data` aliasing `data`, and buffer
    /// writability — the facts `PlanePool::attach` must reproduce verbatim
    /// from its template.
    fn frame_layout(frame: &Frame) -> ([Option<usize>; 8], [i32; 8], i128, bool, bool) {
        unsafe {
            let f = frame.as_ptr();
            let buf0 = (*f).buf[0];
            assert!(!buf0.is_null(), "frame must own a packed buf[0]");
            let base = (*buf0).data as usize;
            let mut offsets = [None; 8];
            for (i, slot) in offsets.iter_mut().enumerate() {
                let d = (*f).data[i];
                if !d.is_null() {
                    *slot = Some(
                        (d as usize)
                            .checked_sub(base)
                            .expect("plane pointer below buffer base"),
                    );
                }
            }
            (
                offsets,
                (*f).linesize,
                (*buf0).size as i128,
                std::ptr::eq((*f).extended_data, (*f).data.as_ptr()),
                ffmpeg_sys_next::av_buffer_is_writable(buf0) == 1,
            )
        }
    }

    /// Odd-width gray8: the tight stride (65) differs from the padded frame
    /// linesize, so a flat memcpy would shear rows. Verify every row landed at
    /// its linesize offset with its exact content.
    #[test]
    fn fill_respects_linesize_for_odd_width() {
        let pool = test_pool();
        let mut plane_pool = PlanePool::empty();
        let p = params(AV_PIX_FMT_GRAY8, 65, 3);
        let data: Vec<u8> = (0..tight_size(p.pix_fmt, 65, 3))
            .map(|i| (i % 251) as u8)
            .collect();
        let frame = build_video_frame(&pool, &mut plane_pool, &p, &data, 7).expect("build");
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
        let mut plane_pool = PlanePool::empty();
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
        let frame = build_video_frame(&pool, &mut plane_pool, &p, &data, 0).expect("build");
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

    /// The time-boxed send is the worker's liveness mechanism toward a
    /// stalled filter channel: with the channel full and a terminal status
    /// published, it must give up within one poll interval instead of
    /// blocking, releasing the undelivered frame.
    #[test]
    fn blocked_send_observes_terminal_status() {
        use crate::core::scheduler::ffmpeg_scheduler::STATUS_END;
        use std::sync::atomic::AtomicUsize;
        use std::time::Instant;

        let pool = test_pool();
        let p = params(AV_PIX_FMT_GRAY8, 8, 2);
        let boxed = |pool: &ObjPool<Frame>| FrameBox {
            frame: pool.get().unwrap(),
            frame_data: frame_data_for(&p),
        };

        let (tx, rx) = crossbeam_channel::bounded::<FrameBox>(1);
        tx.send(boxed(&pool)).unwrap(); // channel now full; receiver held, never read
        let status = Arc::new(AtomicUsize::new(STATUS_END));

        let start = Instant::now();
        let delivered = send_with_status_poll(&tx, boxed(&pool), &status, &pool);
        assert!(!delivered, "terminal status must abort a blocked send");
        // Generous hang-detection bound (one poll interval is 100 ms): this
        // pins liveness, not latency, and must not flake under machine load.
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the abort must land within a few poll intervals, took {:?}",
            start.elapsed()
        );
        drop(rx);
    }

    /// A vanished filter worker (receiver dropped) must fail the send
    /// immediately, not hang.
    #[test]
    fn disconnected_filter_channel_fails_send() {
        use crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN;
        use std::sync::atomic::AtomicUsize;

        let pool = test_pool();
        let p = params(AV_PIX_FMT_GRAY8, 8, 2);
        let (tx, rx) = crossbeam_channel::bounded::<FrameBox>(1);
        drop(rx);
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let frame_box = FrameBox {
            frame: pool.get().unwrap(),
            frame_data: frame_data_for(&p),
        };
        assert!(!send_with_status_poll(&tx, frame_box, &status, &pool));
    }

    /// Full teardown liveness of a REAL spawned worker parked in a healthy
    /// full-channel send: the filter channel (capacity 1, receiver held but
    /// never read) fills, the worker parks in `send_timeout` under
    /// STATUS_RUN, then a terminal status is published — exactly what
    /// RunningGuard/StartFailGuard/abort do — and the guard-side
    /// `wait_for_all_threads` join must complete. This pins the wake-set-free
    /// teardown design at the thread level, not just the helper level.
    #[test]
    fn worker_parked_in_full_send_exits_on_terminal_status() {
        use crate::core::scheduler::ffmpeg_scheduler::{STATUS_END, STATUS_RUN};
        use crate::util::thread_synchronizer::ThreadSynchronizer;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Mutex;
        use std::time::Instant;

        let pool = ObjPool::new(4, test_new_frame, unref_frame, frame_is_null).expect("pool");
        let p = params(AV_PIX_FMT_GRAY8, 8, 2);
        let size = tight_size(p.pix_fmt, 8, 2);

        let (ingress_tx, ingress_rx) = crossbeam_channel::bounded::<Vec<u8>>(4);
        let (fg_tx, fg_rx) = crossbeam_channel::bounded::<FrameBox>(1);
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let thread_sync = ThreadSynchronizer::new();
        let result = Arc::new(Mutex::new(None));

        frame_source_init(
            0,
            FrameSource {
                ingress: ingress_rx,
                fg_sender: fg_tx,
                params: p,
            },
            pool,
            status.clone(),
            thread_sync.clone(),
            result.clone(),
            Arc::new(AtomicBool::new(false)),
        )
        .expect("spawn");

        // Frame 1 fills the 1-slot filter channel; frame 2 parks the worker
        // inside send_timeout (the receiver is deliberately never read).
        ingress_tx.send(vec![1u8; size]).unwrap();
        ingress_tx.send(vec![2u8; size]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !(fg_rx.is_full() && ingress_tx.is_empty()) {
            assert!(
                Instant::now() < deadline,
                "worker never reached the parked send"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        // Give the worker time to actually enter the send park (it has frame
        // 2 in hand and nowhere else to go).
        std::thread::sleep(Duration::from_millis(300));

        // Publish the terminal status (what every guard/abort path does) and
        // require the join to complete while the channel stays full and the
        // ingress sender stays alive.
        status.store(STATUS_END, Ordering::Release);
        let (tx, rx) = std::sync::mpsc::channel();
        let sync2 = thread_sync.clone();
        std::thread::spawn(move || {
            sync2.wait_for_all_threads();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(30))
            .expect("worker parked in a full-channel send must exit on terminal status");
        assert!(
            result.lock().unwrap().is_none(),
            "a status-driven exit is not an error"
        );
        drop(fg_rx);
        drop(ingress_tx);
    }

    /// A recycled shell (released with buffers attached) must come back clean
    /// and refill correctly — the pool's unref_fn is what discharges the old
    /// buffers. The second build also goes through the armed plane pool, so
    /// this doubles as an unref'd-shell-refills-through-the-pool check.
    #[test]
    fn recycled_shell_refills_cleanly() {
        let pool = test_pool();
        let mut plane_pool = PlanePool::empty();
        let p = params(AV_PIX_FMT_GRAY8, 8, 2);
        let data_a = vec![0xAA; tight_size(p.pix_fmt, 8, 2)];
        let frame = build_video_frame(&pool, &mut plane_pool, &p, &data_a, 0).expect("first build");
        pool.release(frame); // unrefs, stores the shell
        let data_b = vec![0x55; tight_size(p.pix_fmt, 8, 2)];
        let frame =
            build_video_frame(&pool, &mut plane_pool, &p, &data_b, 1).expect("recycled build");
        unsafe {
            let f = frame.as_ptr();
            let row = std::slice::from_raw_parts((*f).data[0], 8);
            assert_eq!(row, &data_b[..8]);
            assert_eq!((*f).pts, 1);
        }
    }

    /// Steady-state plane reuse: frame 1 (template path) arms the pool,
    /// frame 2 draws a fresh pooled buffer, and after frame 2's release its
    /// buffer must come back for frame 3. The cfg(test) counter is the
    /// primary signal — pointer equality alone could be satisfied by
    /// allocator address reuse — while the data[0] match is corroborating
    /// evidence, deterministic here because the pool (not the allocator)
    /// held that buffer and single-threaded pool reuse is LIFO.
    #[test]
    fn pooled_rebuild_reuses_plane_buffer() {
        let pool = test_pool();
        let mut plane_pool = PlanePool::empty();
        let p = params(AV_PIX_FMT_GRAY8, 65, 3);
        let planes: &[(usize, usize)] = &[(65, 3)];
        let tight = tight_size(p.pix_fmt, 65, 3);
        let pat = |seed: u8| -> Vec<u8> {
            (0..tight)
                .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
                .collect()
        };

        let data_a = pat(1);
        let f1 = build_video_frame(&pool, &mut plane_pool, &p, &data_a, 0).expect("frame 1");
        assert_eq!(plane_pool.pooled_builds, 0, "frame 1 is the template build");
        assert!(plane_pool.pool.is_some(), "first build must arm the pool");
        assert_frame_planes(&f1, planes, &data_a, "frame 1");

        let data_b = pat(2);
        let f2 = build_video_frame(&pool, &mut plane_pool, &p, &data_b, 1).expect("frame 2");
        assert_eq!(plane_pool.pooled_builds, 1, "frame 2 must be pooled");
        assert_frame_planes(&f2, planes, &data_b, "frame 2");
        let f2_plane0 = unsafe { (*f2.as_ptr()).data[0] as usize };
        assert_eq!(unsafe { (*f2.as_ptr()).pts }, 1);

        // Releasing frame 2 unrefs the last reference to its plane buffer,
        // which hands the buffer back to the pool rather than the allocator.
        pool.release(f2);

        let data_c = pat(3);
        let f3 = build_video_frame(&pool, &mut plane_pool, &p, &data_c, 2).expect("frame 3");
        assert_eq!(plane_pool.pooled_builds, 2, "frame 3 must be pooled");
        let f3_plane0 = unsafe { (*f3.as_ptr()).data[0] as usize };
        assert_eq!(
            f3_plane0, f2_plane0,
            "frame 3 must reuse frame 2's pooled plane buffer"
        );
        assert_frame_planes(&f3, planes, &data_c, "frame 3");
        assert_frame_planes(&f1, planes, &data_a, "frame 1 after reuse");
    }

    /// Pooled frames must reproduce the template's layout and land content
    /// byte-exactly across plane-count extremes at odd geometry: gray8 (1
    /// plane), nv12 (2, interleaved chroma), yuv420p (3), yuva420p (4), and
    /// pal8 (palette living inside the same packed buffer). Frame 1 is the
    /// template; frames 2-3 are pooled.
    #[test]
    fn pooled_layout_matches_template_across_formats() {
        let cases: &[(ffmpeg_sys_next::AVPixelFormat, &[(usize, usize)])] = &[
            (AV_PIX_FMT_GRAY8, &[(65, 49)]),
            (AV_PIX_FMT_NV12, &[(65, 49), (66, 25)]),
            (AV_PIX_FMT_YUV420P, &[(65, 49), (33, 25), (33, 25)]),
            (
                AV_PIX_FMT_YUVA420P,
                &[(65, 49), (33, 25), (33, 25), (65, 49)],
            ),
            (AV_PIX_FMT_PAL8, &[(65, 49), (1024, 1)]),
        ];
        for &(fmt, planes) in cases {
            let pool = test_pool();
            let mut plane_pool = PlanePool::empty();
            let p = params(fmt, 65, 49);
            let tight = tight_size(fmt, 65, 49);
            assert_eq!(
                tight,
                planes.iter().map(|&(bpr, rows)| bpr * rows).sum::<usize>(),
                "{fmt:?}: plane table out of sync with FFmpeg's tight layout"
            );
            let mut template_layout = None;
            for n in 0..3i64 {
                let data: Vec<u8> = (0..tight)
                    .map(|i| ((i * 7 + n as usize * 31) % 251) as u8)
                    .collect();
                let frame = build_video_frame(&pool, &mut plane_pool, &p, &data, n).expect("build");
                unsafe {
                    let f = frame.as_ptr();
                    assert_eq!((*f).pts, n, "{fmt:?} frame {n}: pts");
                    assert_eq!((*f).duration, 1, "{fmt:?} frame {n}: duration");
                }
                // Verbatim layout-contract pin: pooled frames must reproduce
                // the template's active-plane mask, buffer-relative offsets,
                // linesizes, buffer extent, extended_data aliasing, and
                // writability — not merely land equivalent pixels somewhere.
                let layout = frame_layout(&frame);
                match &template_layout {
                    None => template_layout = Some(layout),
                    Some(template) => assert_eq!(
                        &layout, template,
                        "{fmt:?} frame {n}: pooled layout must match the template verbatim"
                    ),
                }
                assert_frame_planes(&frame, planes, &data, &format!("{fmt:?} frame {n}"));
                pool.release(frame);
            }
            assert_eq!(
                plane_pool.pooled_builds, 2,
                "{fmt:?}: frames 2-3 must build through the pool"
            );
        }
    }

    /// The deferred-teardown contract: worker exit drops the `PlanePool`
    /// (`av_buffer_pool_uninit`) while downstream stages may still hold
    /// pooled frames — FFmpeg keeps every outstanding buffer valid and
    /// frees the pool itself only when the last one returns. Hold a pooled
    /// frame across the drop, verify its planes are intact, then release it
    /// as the true last owner.
    #[test]
    fn pooled_buffer_outlives_the_pool() {
        let pool = test_pool();
        let mut plane_pool = PlanePool::empty();
        let p = params(AV_PIX_FMT_YUV420P, 65, 49);
        let planes: &[(usize, usize)] = &[(65, 49), (33, 25), (33, 25)];
        let tight = tight_size(p.pix_fmt, 65, 49);
        let data_a: Vec<u8> = (0..tight).map(|i| (i % 249) as u8).collect();
        let data_b: Vec<u8> = (0..tight).map(|i| (i % 247) as u8).collect();

        let f1 = build_video_frame(&pool, &mut plane_pool, &p, &data_a, 0).expect("template");
        let f2 = build_video_frame(&pool, &mut plane_pool, &p, &data_b, 1).expect("pooled");
        assert_eq!(plane_pool.pooled_builds, 1, "frame 2 must be pooled");

        // Worker exit while the pooled frame is still in flight downstream.
        drop(plane_pool);

        assert_frame_planes(&f2, planes, &data_b, "pooled frame after pool drop");
        // The last unref of the pooled buffer, after uninit, frees buffer
        // and pool together (leak/UAF visible to the sanitizer lane).
        pool.release(f2);
        pool.release(f1);
    }

    /// The permanent-fallback state (armed with no pool, the aftermath of a
    /// failed pool init) must keep building frames exactly like today's
    /// unpooled path: byte-exact content, zero pooled builds, no late arming.
    #[test]
    fn pool_fallback_builds_identical_frames() {
        let pool = test_pool();
        let mut plane_pool = PlanePool::disarmed();
        let p = params(AV_PIX_FMT_YUV420P, 65, 49);
        let planes: &[(usize, usize)] = &[(65, 49), (33, 25), (33, 25)];
        let tight = tight_size(p.pix_fmt, 65, 49);
        for n in 0..3i64 {
            let data: Vec<u8> = (0..tight)
                .map(|i| ((i * 3 + n as usize * 17) % 250) as u8)
                .collect();
            let frame = build_video_frame(&pool, &mut plane_pool, &p, &data, n).expect("build");
            if n == 0 {
                // The disarmed path must carry the same buffer-layout
                // contract as a genuine unpooled build, not merely
                // equivalent pixels. Plane OFFSETS are deliberately not
                // compared: FFmpeg aligns each plane pointer to its
                // absolute address, so on builds whose av_malloc
                // alignment class is smaller than that plane alignment
                // (aarch64: 16 vs 32) the buffer-relative offsets of two
                // independent allocations legitimately differ by the
                // bases' alignment slack. Placement is pinned instead by
                // the span assertions below (in-bounds, ordered,
                // non-overlapping) plus the row-walk over the content —
                // together they reject a short buffer, a reordered or
                // overlapping plane table, and sheared rows.
                let mut fresh = PlanePool::empty();
                let twin =
                    build_video_frame(&pool, &mut fresh, &p, &data, n).expect("unpooled twin");
                let (offsets, linesize, extent, aliased, writable) = frame_layout(&frame);
                let (t_offsets, t_linesize, t_extent, t_aliased, t_writable) = frame_layout(&twin);
                assert_eq!(
                    (linesize, extent, aliased, writable),
                    (t_linesize, t_extent, t_aliased, t_writable),
                    "fallback must match a normal unpooled frame's deterministic layout"
                );
                assert_eq!(
                    offsets.map(|o| o.is_some()),
                    t_offsets.map(|o| o.is_some()),
                    "fallback must populate the same plane set"
                );
                assert_plane_spans_disjoint(&twin, planes, "unpooled twin");
                pool.release(twin);
            }
            assert_plane_spans_disjoint(&frame, planes, &format!("fallback frame {n}"));
            assert_frame_planes(&frame, planes, &data, &format!("fallback frame {n}"));
            assert_eq!(unsafe { (*frame.as_ptr()).pts }, n);
            pool.release(frame);
        }
        assert_eq!(
            plane_pool.pooled_builds, 0,
            "every fallback build must take the av_frame_get_buffer path"
        );
        assert!(plane_pool.pool.is_none(), "a failed arming is permanent");
    }
}
