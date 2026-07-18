//! The output-side export sink.
//!
//! [`ExportSink`] is a `Never`-mode [`FrameFilter`] mounted on the null output's
//! frame pipeline, downstream of `scale`/`format` and the vsync passthrough
//! (S4). Per frame it: forwards end-of-stream markers untouched, applies the
//! sampling decision (or, for `UniformN`, reads the input-side sampler's
//! dup-count), packs the selected frame into an owned tight buffer, and delivers
//! it over a bounded channel. It owns the exact `max_frames` cap (S5); the
//! encoder-side `set_max_video_frames` is only an upstream terminator.

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
    /// Countdown to the next `EveryNth` selection (0 means take this frame).
    countdown: u64,
    /// Frames emitted so far (becomes each frame's `index`, enforces the cap).
    emitted: u64,
    /// Next `EverySec` grid target in microseconds. `i128` so a boundary past
    /// `i64::MAX` moves beyond every possible PTS (correctly ending selection)
    /// instead of saturating and re-selecting.
    next_target_us: Option<i128>,
    /// `EverySec` step in microseconds (`> 0`, validated at build time).
    every_sec_us: i64,
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
        let every_sec_us = match sampling {
            Sampling::EverySec(s) => (s * 1_000_000.0) as i64,
            _ => 0,
        };
        let uniform = matches!(sampling, Sampling::UniformN(_));
        Self {
            tx,
            sampling,
            layout,
            max_frames,
            uniform,
            countdown: 0,
            emitted: 0,
            next_target_us: None,
            every_sec_us,
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

    /// Decides whether to select this frame, advancing the sampling state.
    /// (Not used for `UniformN`, whose selection happens on the input side.)
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame` pointer.
    unsafe fn select(&mut self, p: *const AVFrame, pts_us: Option<i64>) -> bool {
        match self.sampling {
            Sampling::All => true,
            Sampling::EveryNth(n) => {
                // n >= 1 is validated at build time; guard anyway. A countdown
                // (rather than `seen % n`) keeps this MSRV-1.80 safe — u64
                // `is_multiple_of` is only stable since 1.87.
                let n = n.max(1);
                if self.countdown == 0 {
                    self.countdown = n - 1;
                    true
                } else {
                    self.countdown -= 1;
                    false
                }
            }
            Sampling::KeyframesOnly => (*p).flags & AV_FRAME_FLAG_KEY != 0,
            Sampling::EverySec(_) => {
                let pts = match pts_us {
                    Some(v) => v as i128,
                    None => return false,
                };
                let step = self.every_sec_us.max(1) as i128;
                match self.next_target_us {
                    // First delivered frame anchors the grid (C1).
                    None => {
                        self.next_target_us = Some(pts + step);
                        true
                    }
                    Some(target) => {
                        if pts >= target {
                            // Advance in O(1) to the smallest grid point strictly
                            // past this frame. All-i128, unclamped: a huge gap can
                            // neither hang nor overflow, and a boundary beyond
                            // i64::MAX simply exceeds every future PTS. Each source
                            // frame is selected at most once even across many steps.
                            let advance = ((pts - target) / step + 1) * step;
                            self.next_target_us = Some(target + advance);
                            true
                        } else {
                            false
                        }
                    }
                }
            }
            // Input-side selection; the sink expands the dup-count instead.
            Sampling::UniformN(_) => true,
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

        if !unsafe { self.select(p, pts_us) } {
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
