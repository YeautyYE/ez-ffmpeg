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
use crossbeam_channel::Sender;
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
}

impl ExportSink {
    pub(crate) fn new(
        tx: Sender<VideoFrame>,
        sampling: Sampling,
        layout: PixelLayout,
        max_frames: Option<u64>,
    ) -> Self {
        let uniform = matches!(sampling, Sampling::UniformN(_));
        Self {
            tx,
            sampling,
            layout,
            max_frames,
            uniform,
            emitted: 0,
            done: false,
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
        let vf = VideoFrame::new(w, h, self.layout, pts_us, self.emitted, data);
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
            let (w, h, mut bytes) = unsafe { pack_bytes(p, self.layout)? };
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
        let (w, h, bytes) = unsafe { pack_bytes(p, self.layout)? };
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
/// bytes)` buffer. Handles negative `linesize` (bottom-up frames) and rejects
/// sizes that would overflow `usize`/`isize`. The graph pins `format=<pix>`, so
/// a non-software or mismatched format here is an internal invariant violation,
/// surfaced as an error rather than undefined behavior.
///
/// # Safety
/// `p` must be a valid, non-null `AVFrame` whose `data[0]`/`linesize[0]` describe
/// a single packed plane of `format` pixels.
unsafe fn pack_bytes(
    p: *const AVFrame,
    layout: PixelLayout,
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

    let mut out = vec![0u8; total];
    for row in 0..h {
        // With negative linesize, data[0] points to the top row and lower rows
        // sit at lower addresses; `base + row*linesize` walks them top-down.
        let src_row = base.offset(row as isize * linesize);
        let dst = out.as_mut_ptr().add(row * row_bytes);
        std::ptr::copy_nonoverlapping(src_row, dst, row_bytes);
    }
    Ok((width as u32, height as u32, out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::av_frame_alloc;

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
