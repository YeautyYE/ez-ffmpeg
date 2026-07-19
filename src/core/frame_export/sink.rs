//! The output-side export sink.
//!
//! [`ExportSink`] is a `Never`-mode [`FrameFilter`] mounted on the null output's
//! frame pipeline, downstream of `scale`/`format` and the vsync passthrough
//! (S4). Sampling happens on the INPUT side (the UniformN sampler and the
//! EveryNth/EverySec selector run pre-filtergraph, so dropped frames never pay
//! the conversion); this sink only re-checks `KeyframesOnly` (the post-graph
//! `AV_FRAME_FLAG_KEY` belt over the decoder fast path), expands `UniformN`
//! dup-counts, packs each delivered frame into an owned tight buffer, and
//! streams it over a bounded channel. It owns the exact `max_frames` cap (S5);
//! the encoder-side `set_max_video_frames` is only an upstream terminator.

use super::frame::VideoFrame;
use super::options::{PixelLayout, Sampling};
use super::sampler::EMIT_COUNT_KEY;
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use crate::util::frame_utils::ensure_software_format;
use crossbeam_channel::{Receiver, Sender};
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_dict_get, av_dict_set, av_rescale_q, AVFrame, AVMediaType, AVMediaType::AVMEDIA_TYPE_VIDEO,
    AVRational, AV_FRAME_FLAG_KEY, AV_NOPTS_VALUE,
};
use std::ffi::{CStr, CString};

const US_PER_SEC: AVRational = AVRational {
    num: 1,
    den: 1_000_000,
};

/// Recycles packed payload buffers between delivered frames.
///
/// Every [`VideoFrame`] carries a clone of `tx` and offers its buffer back on
/// drop; `pack_bytes` reuses a returned buffer instead of allocating (and
/// zeroing) a fresh one per frame. After warmup an iterate-and-drop consumer
/// circulates 2–3 buffers with no per-frame allocation at all.
/// [`VideoFrame::into_vec`] detaches its buffer, which is simply a pool miss —
/// the next frame allocates fresh.
///
/// The channel is bounded, so a consumer that hoards frames (or clones made by
/// the UniformN dup expansion) can never grow the pool past `slots` parked
/// buffers; overflow and post-teardown returns degrade to a plain free.
struct BufferPool {
    tx: Sender<Vec<u8>>,
    rx: Receiver<Vec<u8>>,
}

impl BufferPool {
    fn new(slots: usize) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(slots.max(1));
        Self { tx, rx }
    }

    /// The sender handed to each `VideoFrame` as its drop-time return path.
    fn recycler(&self) -> Sender<Vec<u8>> {
        self.tx.clone()
    }

    /// An EMPTY buffer with at least `total` capacity: a recycled one when a
    /// fitting buffer is parked, else a fresh allocation. Parked buffers that
    /// no longer fit the live geometry are freed rather than kept: undersized
    /// ones cannot serve the request, and ones over 4x the request (a
    /// mid-stream resolution drop) would otherwise pin their full allocation —
    /// think one 8K RGB24 buffer at ~100 MB — for the rest of the run. The 4x
    /// bound reuses across modest changes (1080p -> 720p) while a large drop
    /// (4K -> thumbnail) converges the pool to the new size within `slots`
    /// takes.
    fn take(&self, total: usize) -> Vec<u8> {
        let max_keep = total.saturating_mul(4);
        while let Ok(mut buf) = self.rx.try_recv() {
            if buf.capacity() >= total && buf.capacity() <= max_keep {
                buf.clear();
                return buf;
            }
        }
        Vec::with_capacity(total)
    }
}

/// Packs post-graph frames and streams them to the consumer.
pub(crate) struct ExportSink {
    tx: Sender<VideoFrame>,
    sampling: Sampling,
    layout: PixelLayout,
    max_frames: Option<u64>,
    /// True for `UniformN`: selection happened on the input side, so this sink
    /// reads the `EMIT_COUNT` dup-count and expands rather than selecting.
    uniform: bool,
    /// Frames emitted so far (becomes each frame's `index`, enforces the cap).
    emitted: u64,
    /// Set once the receiver is gone or the cap is reached; stops emitting.
    done: bool,
    /// Payload-buffer recycler shared with every delivered [`VideoFrame`].
    pool: BufferPool,
}

impl ExportSink {
    pub(crate) fn new(
        tx: Sender<VideoFrame>,
        sampling: Sampling,
        layout: PixelLayout,
        max_frames: Option<u64>,
    ) -> Self {
        let uniform = matches!(sampling, Sampling::UniformN(_));
        // Steady state has up to `capacity` frames parked in the user channel,
        // one held by the consumer, and one being packed — capacity + 2 slots
        // bound the pool without ever starving it.
        let pool = BufferPool::new(tx.capacity().unwrap_or(1).saturating_add(2));
        Self {
            tx,
            sampling,
            layout,
            max_frames,
            uniform,
            emitted: 0,
            done: false,
            pool,
        }
    }

    /// Reads a POST-FILTER frame's presentation time in microseconds, or `None`
    /// when it has no usable timestamp.
    ///
    /// Deliberately reads `pts` only: the filtergraph output stage rewrites
    /// `pts` into its own output time base (1/framerate for video), while
    /// `best_effort_timestamp` still carries the decoder-era number in the
    /// ORIGINAL stream time base. Pairing that stale value with the rewritten
    /// `time_base` mis-scales by tb_out/stream_tb on any real container whose
    /// stream tb differs from 1/fps (mkv 1/1000, mp4 1/12800, ...) — lavfi
    /// sources mask it because their stream tb equals 1/fps exactly.
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame` pointer.
    unsafe fn frame_pts_us(p: *const AVFrame) -> Option<i64> {
        let tb = (*p).time_base;
        if tb.den == 0 {
            return None;
        }
        let pts = (*p).pts;
        if pts == AV_NOPTS_VALUE {
            return None;
        }
        Some(av_rescale_q(pts, tb, US_PER_SEC))
    }

    /// Post-graph selection re-check. Only `KeyframesOnly` still decides here:
    /// its input side is the `skip_frame=nokey` decoder fast path, and this
    /// flag check is the belt over it (the KEY flag survives the graph). Every
    /// other mode was fully decided on the input side — the UniformN sampler
    /// and the EveryNth/EverySec selector — so unselected frames never reach
    /// the graph's `scale`/`format` conversion at all.
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame` pointer.
    unsafe fn select(&self, p: *const AVFrame) -> bool {
        match self.sampling {
            Sampling::KeyframesOnly => (*p).flags & AV_FRAME_FLAG_KEY != 0,
            _ => true,
        }
    }

    /// Sends one `VideoFrame`, updating the emit count / done flag. Returns
    /// `false` when the sink should stop emitting (cap hit or receiver gone).
    fn deliver(&mut self, w: u32, h: u32, pts_us: Option<i64>, data: Vec<u8>) -> bool {
        if let Some(max) = self.max_frames {
            if self.emitted >= max {
                self.done = true;
                return false;
            }
        }
        let vf = VideoFrame::new(
            w,
            h,
            self.layout,
            pts_us,
            self.emitted,
            data,
            Some(self.pool.recycler()),
        );
        match self.tx.send(vf) {
            Ok(()) => {
                self.emitted += 1;
                true
            }
            Err(_) => {
                self.done = true;
                false
            }
        }
    }
}

impl FrameFilter for ExportSink {
    fn media_type(&self) -> AVMediaType {
        AVMEDIA_TYPE_VIDEO
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        mut frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // End-of-stream flush markers pass through untouched; never exported.
        if frame_is_eof_marker(&frame) {
            return Ok(Some(frame));
        }
        // SAFETY: null-checked before any deref.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(Some(frame));
        }
        // After the cap is hit or the receiver disconnects, keep FORWARDING
        // (without packing/delivery): encoder-side enforcement (frame cap,
        // recording time) must keep seeing frames, or an unbounded UniformN run
        // whose sink finished early would starve it and never terminate. In
        // uniform mode strip the stale count so nothing re-interprets it.
        if self.done {
            if self.uniform {
                let _ = unsafe { read_and_strip_emit_count(&mut frame) };
            }
            return Ok(Some(frame));
        }

        let pts_us = unsafe { Self::frame_pts_us(p) };

        if self.uniform {
            // The input-side sampler already selected this frame and stamped the
            // number of copies to emit (1 when absent). Pack once, clone the rest.
            let count = unsafe { read_and_strip_emit_count(&mut frame) };
            // Defense in depth: even a corrupted or injected count can never
            // deliver more than the UniformN contract's n frames in total.
            let count = match self.sampling {
                Sampling::UniformN(n) => count.min((n as u64).saturating_sub(self.emitted)),
                _ => count,
            };
            if count == 0 {
                return Ok(Some(frame));
            }
            let (w, h, mut bytes) = unsafe { pack_bytes(p, self.layout, &self.pool)? };
            let mut remaining = count;
            while remaining > 0 {
                remaining -= 1;
                let data = if remaining == 0 {
                    std::mem::take(&mut bytes)
                } else {
                    bytes.clone()
                };
                if !self.deliver(w, h, pts_us, data) {
                    break;
                }
            }
            // Forward so the run drains to EOF (no encoder cap for UniformN).
            return Ok(Some(frame));
        }

        if !unsafe { self.select(p) } {
            // Non-uniform modes only: a dropped frame does not reach the
            // encoder, so `set_max_video_frames` counts SELECTED frames 1:1.
            // (UniformN never takes this path; there the encoder sees unique
            // frames while delivery counts duplicate expansions.)
            return Ok(None);
        }
        let (w, h, bytes) = unsafe { pack_bytes(p, self.layout, &self.pool)? };
        if self.deliver(w, h, pts_us, bytes) {
            // Forward the (unpacked) frame so the upstream terminator counts it.
            Ok(Some(frame))
        } else {
            // Cap reached or receiver gone. Forward for a clean wind-down.
            Ok(Some(frame))
        }
    }
}

/// Reads (and strips) the `EMIT_COUNT` dup-count from a frame's metadata,
/// defaulting to 1 when absent or malformed.
///
/// # Safety
/// `frame` must be a valid owned frame whose metadata dict we may modify.
unsafe fn read_and_strip_emit_count(frame: &mut Frame) -> u64 {
    let key = CString::new(EMIT_COUNT_KEY).expect("literal has no NUL");
    let p = frame.as_mut_ptr();
    let entry = av_dict_get((*p).metadata, key.as_ptr(), std::ptr::null(), 0);
    let count = if entry.is_null() {
        1
    } else {
        CStr::from_ptr((*entry).value)
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1)
            .max(1)
    };
    // Strip ALL matches (a MULTIKEY-built dict may hold several) so nothing
    // downstream re-interprets the key. Each av_dict_set(NULL) removes one
    // entry, so the loop strictly shrinks the dict.
    while !av_dict_get((*p).metadata, key.as_ptr(), std::ptr::null(), 0).is_null() {
        if av_dict_set(&mut (*p).metadata, key.as_ptr(), std::ptr::null(), 0) < 0 {
            break;
        }
    }
    count
}

/// Packs a single packed-format plane into an owned, tight `(width, height,
/// bytes)` buffer (recycled from `pool` when possible). Handles negative
/// `linesize` (bottom-up frames) and rejects sizes that would overflow
/// `usize`/`isize`. The graph pins `format=<pix>`, so a non-software or
/// mismatched format here is an internal invariant violation, surfaced as an
/// error rather than undefined behavior.
///
/// # Safety
/// `p` must be a valid, non-null `AVFrame` whose `data[0]`/`linesize[0]` describe
/// a single packed plane of `format` pixels.
unsafe fn pack_bytes(
    p: *const AVFrame,
    layout: PixelLayout,
    pool: &BufferPool,
) -> Result<(u32, u32, Vec<u8>), FrameFilterError> {
    ensure_software_format((*p).format)
        .map_err(|e| -> FrameFilterError { format!("frame export: {e}").into() })?;
    let expected = layout.av_pixel_format() as i32;
    if (*p).format != expected {
        return Err(format!(
            "frame export: expected pixel format {expected}, got {}",
            (*p).format
        )
        .into());
    }

    let width = (*p).width;
    let height = (*p).height;
    if width <= 0 || height <= 0 {
        return Err(format!("frame export: non-positive frame dimensions {width}x{height}").into());
    }
    let bpp = layout.bytes_per_pixel();
    let w = width as usize;
    let h = height as usize;
    let row_bytes = w
        .checked_mul(bpp)
        .ok_or_else(|| -> FrameFilterError { "frame export: row size overflow".into() })?;
    let total = row_bytes
        .checked_mul(h)
        .ok_or_else(|| -> FrameFilterError { "frame export: frame size overflow".into() })?;

    let base = (*p).data[0];
    if base.is_null() {
        return Err("frame export: packed plane pointer is null".into());
    }
    let linesize = (*p).linesize[0] as isize;
    let linesize_abs = linesize.unsigned_abs();
    if linesize_abs < row_bytes {
        return Err(format!(
            "frame export: linesize {linesize} smaller than {row_bytes} packed row bytes"
        )
        .into());
    }
    // Bound the source footprint so `row as isize * linesize` cannot overflow
    // `isize` (a real risk only on 32-bit targets, where a frame this large could
    // not be allocated anyway, but the pointer arithmetic must stay defined).
    let src_footprint = h
        .saturating_sub(1)
        .checked_mul(linesize_abs)
        .and_then(|x| x.checked_add(row_bytes))
        .ok_or_else(|| -> FrameFilterError { "frame export: source footprint overflow".into() })?;
    if src_footprint > isize::MAX as usize {
        return Err("frame export: source footprint exceeds isize::MAX".into());
    }

    // Recycled-or-fresh EMPTY buffer with `total` capacity: rows are appended
    // into reserved capacity, so no zero pass ever touches the payload (the
    // old `vec![0u8; total]` wrote every frame's 6 MB twice).
    let mut out = pool.take(total);
    debug_assert!(out.is_empty() && out.capacity() >= total);
    for row in 0..h {
        // With negative linesize, data[0] points to the top row and lower rows
        // sit at lower addresses; `base + row*linesize` walks them top-down.
        let src_row = base.offset(row as isize * linesize);
        // SAFETY: `src_row` starts one packed row; the linesize check above
        // guarantees at least `row_bytes` readable bytes there and the
        // `src_footprint` bound keeps every offset inside the plane.
        let src = std::slice::from_raw_parts(src_row, row_bytes);
        // Never reallocates (capacity >= total); appends exactly `row_bytes`.
        out.extend_from_slice(src);
    }
    debug_assert_eq!(out.len(), total);
    Ok((width as u32, height as u32, out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::av_frame_alloc;
    use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_RGB24;

    /// A minimal owned frame whose only meaningful field is `flags`.
    fn flag_frame(key: bool) -> Frame {
        // SAFETY: fresh alloc; wrapped so Drop frees it.
        unsafe {
            let p = av_frame_alloc();
            assert!(!p.is_null());
            if key {
                (*p).flags |= AV_FRAME_FLAG_KEY;
            }
            Frame::wrap(p)
        }
    }

    /// A dropped `VideoFrame` hands its buffer back to the pool; the next
    /// `take` reuses that very allocation instead of allocating fresh.
    #[test]
    fn dropped_frame_recycles_its_buffer() {
        let pool = BufferPool::new(2);
        let mut first = pool.take(12);
        first.extend_from_slice(&[7u8; 12]);
        let ptr = first.as_ptr();
        let vf = VideoFrame::new(
            2,
            2,
            PixelLayout::Rgb24,
            None,
            0,
            first,
            Some(pool.recycler()),
        );
        drop(vf);
        let reused = pool.take(12);
        assert!(reused.is_empty(), "recycled buffers come back empty");
        assert_eq!(
            reused.as_ptr(),
            ptr,
            "the same allocation must circulate through the pool"
        );
    }

    /// `into_vec` detaches the allocation: the caller keeps it, the pool
    /// misses, and the payload survives untouched.
    #[test]
    fn into_vec_detaches_from_the_pool() {
        let pool = BufferPool::new(2);
        let mut buf = pool.take(3);
        buf.extend_from_slice(&[1, 2, 3]);
        let vf = VideoFrame::new(1, 1, PixelLayout::Rgb24, None, 0, buf, Some(pool.recycler()));
        let owned = vf.into_vec();
        assert_eq!(owned, vec![1, 2, 3]);
        assert!(
            pool.rx.try_recv().is_err(),
            "a detached buffer must never reach the pool"
        );
    }

    /// A parked buffer that no longer fits (resolution change) is freed, not
    /// returned undersized — `take` always yields >= the requested capacity.
    #[test]
    fn undersized_parked_buffers_are_skipped() {
        let pool = BufferPool::new(2);
        pool.recycler().try_send(Vec::with_capacity(4)).unwrap();
        let buf = pool.take(1024);
        assert!(buf.capacity() >= 1024);
        assert!(
            pool.rx.try_recv().is_err(),
            "the undersized buffer must have been drained and dropped"
        );
    }

    /// A parked buffer far larger than the live frame (mid-stream resolution
    /// drop) is freed, not kept: the pool must not pin one obsolete jumbo
    /// allocation per slot for the rest of the run.
    #[test]
    fn oversized_parked_buffers_are_freed_on_shrink() {
        let pool = BufferPool::new(2);
        pool.recycler()
            .try_send(Vec::with_capacity(1 << 20))
            .unwrap();
        let buf = pool.take(1024);
        assert!(buf.capacity() >= 1024);
        assert!(
            buf.capacity() < (1 << 20),
            "the jumbo buffer must have been dropped, not returned"
        );
        assert!(
            pool.rx.try_recv().is_err(),
            "the jumbo buffer must not remain parked either"
        );
    }

    /// Modest geometry changes stay inside the 4x keep bound and reuse the
    /// parked allocation.
    #[test]
    fn modestly_larger_parked_buffers_are_reused() {
        let pool = BufferPool::new(2);
        let big = Vec::with_capacity(8192);
        let ptr = big.as_ptr();
        pool.recycler().try_send(big).unwrap();
        let buf = pool.take(4096); // 8192 <= 4 * 4096: within the keep bound
        assert_eq!(buf.as_ptr(), ptr, "a within-bound buffer must be reused");
    }

    /// With the pool full, a dropping frame frees its buffer quietly instead
    /// of blocking or panicking (the `try_send` overflow path).
    #[test]
    fn full_pool_drop_degrades_to_free() {
        let pool = BufferPool::new(1);
        pool.recycler().try_send(Vec::with_capacity(8)).unwrap();
        let vf = VideoFrame::new(
            1,
            1,
            PixelLayout::Rgb24,
            None,
            0,
            vec![0u8; 3],
            Some(pool.recycler()),
        );
        drop(vf);
        let parked = pool.take(8);
        assert!(parked.capacity() >= 8);
        assert!(
            pool.rx.try_recv().is_err(),
            "the overflow buffer must have been freed, not parked"
        );
    }

    /// Frames dropped on another thread still recycle — the channel is the
    /// cross-thread return path (consumers routinely drop frames off the
    /// packing thread).
    #[test]
    fn cross_thread_drop_recycles() {
        let pool = BufferPool::new(2);
        let mut buf = pool.take(12);
        buf.extend_from_slice(&[9u8; 12]);
        let ptr = buf.as_ptr() as usize;
        let vf = VideoFrame::new(2, 2, PixelLayout::Rgb24, None, 0, buf, Some(pool.recycler()));
        std::thread::spawn(move || drop(vf)).join().expect("drop thread");
        let reused = pool.take(12);
        assert_eq!(
            reused.as_ptr() as usize,
            ptr,
            "a cross-thread drop must land back in the pool"
        );
    }

    /// Backs a minimal packed-RGB24 AVFrame with caller-owned bytes. No
    /// AVBuffer refs are attached, so dropping the frame never frees
    /// `backing` — the test's Vec stays the owner.
    unsafe fn synthetic_rgb24(backing: *mut u8, w: i32, h: i32, linesize: i32) -> Frame {
        let p = av_frame_alloc();
        assert!(!p.is_null());
        (*p).format = AV_PIX_FMT_RGB24 as i32;
        (*p).width = w;
        (*p).height = h;
        (*p).data[0] = backing;
        (*p).linesize[0] = linesize;
        Frame::wrap(p)
    }

    /// Padded positive stride: packing must copy exactly `row_bytes` per row
    /// and skip the padding tail.
    #[test]
    fn pack_skips_positive_stride_padding() {
        // 3x2 RGB24: 9 payload bytes per row, stride 12 (3 pad bytes 0xEE).
        let mut backing = vec![0xEEu8; 24];
        for row in 0..2usize {
            for i in 0..9usize {
                backing[row * 12 + i] = (row * 9 + i) as u8;
            }
        }
        let pool = BufferPool::new(1);
        // SAFETY: `backing` outlives the frame; geometry matches the buffer.
        let (w, h, bytes) = unsafe {
            let f = synthetic_rgb24(backing.as_mut_ptr(), 3, 2, 12);
            pack_bytes(f.as_ptr(), PixelLayout::Rgb24, &pool).expect("pack")
        };
        assert_eq!((w, h), (3, 2));
        let expected: Vec<u8> = (0u8..18).collect();
        assert_eq!(bytes, expected, "payload rows only, no stride padding");
    }

    /// Negative linesize (bottom-up storage): `data[0]` points at the
    /// display-top row, which sits at the END of the allocation; walking
    /// `base + row * linesize` must still deliver rows in display order.
    #[test]
    fn pack_walks_negative_stride_top_down() {
        // Memory layout: [display-bottom row][display-top row], stride 12.
        let mut backing = vec![0xEEu8; 24];
        for i in 0..9usize {
            backing[i] = 100 + i as u8; // display bottom
            backing[12 + i] = i as u8; // display top
        }
        let pool = BufferPool::new(1);
        // SAFETY: base points at the top row (last stride slot); row 1 walks
        // back to offset 0, still inside `backing`.
        let (_, _, bytes) = unsafe {
            let f = synthetic_rgb24(backing.as_mut_ptr().add(12), 3, 2, -12);
            pack_bytes(f.as_ptr(), PixelLayout::Rgb24, &pool).expect("pack")
        };
        let mut expected: Vec<u8> = (0u8..9).collect();
        expected.extend(100u8..109);
        assert_eq!(bytes, expected, "display order: top row then bottom row");
    }

    /// The KEY-flag check is what keeps `KeyframesOnly` correct even when a
    /// decoder ignores `skip_frame=nokey` and delivers every frame: key frames
    /// pass the selection, delta frames do not.
    #[test]
    fn keyframes_only_selects_by_key_flag() {
        let (tx, _rx) = crossbeam_channel::bounded(1);
        let sink = ExportSink::new(tx, Sampling::KeyframesOnly, PixelLayout::Rgb24, None);
        let key = flag_frame(true);
        let delta = flag_frame(false);
        // SAFETY: both frames are valid allocations owned by this test.
        unsafe {
            assert!(
                sink.select(key.as_ptr()),
                "a KEY-flagged frame must be selected"
            );
            assert!(
                !sink.select(delta.as_ptr()),
                "a delta frame must not be selected"
            );
        }
    }
}
