//! The output-side export sink.
//!
//! [`ExportSink`] is a `Never`-mode [`FrameFilter`] mounted on the null output's
//! frame pipeline, downstream of `scale`/`format` and the vsync passthrough
//! (S4). Per frame it: forwards end-of-stream markers untouched, applies the
//! sampling decision, packs the selected frame into an owned tight buffer, and
//! delivers it over a bounded channel. It owns the exact `max_frames` cap (S5);
//! the encoder-side `set_max_video_frames` is only an upstream terminator.

use super::frame::VideoFrame;
use super::options::{PixelLayout, Sampling};
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use crate::util::frame_utils::ensure_software_format;
use crossbeam_channel::Sender;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_rescale_q, AVFrame, AVMediaType, AVMediaType::AVMEDIA_TYPE_VIDEO, AVRational,
    AV_FRAME_FLAG_KEY, AV_NOPTS_VALUE,
};

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
        Self {
            tx,
            sampling,
            layout,
            max_frames,
            countdown: 0,
            emitted: 0,
            next_target_us: None,
            every_sec_us,
            done: false,
        }
    }

    /// Reads a frame's presentation time in microseconds, or `None` when it has
    /// no usable timestamp.
    ///
    /// # Safety
    /// `p` must be a valid, non-null `AVFrame` pointer.
    unsafe fn frame_pts_us(p: *const AVFrame) -> Option<i64> {
        let tb = (*p).time_base;
        if tb.den == 0 {
            return None;
        }
        let raw = (*p).best_effort_timestamp;
        let raw = if raw == AV_NOPTS_VALUE { (*p).pts } else { raw };
        if raw == AV_NOPTS_VALUE {
            return None;
        }
        Some(av_rescale_q(raw, tb, US_PER_SEC))
    }

    /// Decides whether to select this frame, advancing the sampling state.
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
        frame: Frame,
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
        // After the cap is hit or the receiver disconnects, drop the rest so the
        // encoder cap / Drop-abort ends the run without further work.
        if self.done {
            return Ok(None);
        }

        let pts_us = unsafe { Self::frame_pts_us(p) };
        if !unsafe { self.select(p, pts_us) } {
            // Dropped pre-graph would be ideal; here it simply does not reach the
            // encoder, so `set_max_video_frames` counts selected frames 1:1.
            return Ok(None);
        }

        // The sink owns the exact public cap (S5).
        if let Some(max) = self.max_frames {
            if self.emitted >= max {
                self.done = true;
                return Ok(None);
            }
        }

        let vf = unsafe { pack_frame(p, self.layout, pts_us, self.emitted)? };
        match self.tx.send(vf) {
            Ok(()) => {
                self.emitted += 1;
                // Forward the (unpacked) frame so the upstream terminator counts it.
                Ok(Some(frame))
            }
            Err(_) => {
                // Receiver gone: stop emitting, let the run wind down.
                self.done = true;
                Ok(Some(frame))
            }
        }
    }
}

/// Packs a single packed-format plane into an owned, tight `VideoFrame`.
///
/// Handles negative `linesize` (bottom-up frames) and rejects sizes that would
/// overflow `usize`. The graph pins `format=<pix>`, so a non-software or
/// mismatched format here is an internal invariant violation, surfaced as an
/// error rather than undefined behavior.
///
/// # Safety
/// `p` must be a valid, non-null `AVFrame` whose `data[0]`/`linesize[0]` describe
/// a single packed plane of `format` pixels.
unsafe fn pack_frame(
    p: *const AVFrame,
    layout: PixelLayout,
    pts_us: Option<i64>,
    index: u64,
) -> Result<VideoFrame, FrameFilterError> {
    ensure_software_format((*p).format)
        .map_err(|e| -> FrameFilterError { format!("frame export: {e}").into() })?;
    // The graph pins `format=<pix>`, so anything else is an internal invariant
    // breach. Reject the EXACT format (not just "is software"): a leaked frame of
    // a different packed layout would be packed with the wrong bytes-per-pixel and
    // could overread a minimally-backed final row.
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
    Ok(VideoFrame::new(
        width as u32,
        height as u32,
        layout,
        pts_us,
        index,
        out,
    ))
}
