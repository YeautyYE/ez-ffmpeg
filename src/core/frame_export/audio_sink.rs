//! The output-side PCM sample sink.
//!
//! [`SampleSink`] is a `Never`-mode [`FrameFilter`] mounted on the null output's
//! audio frame pipeline, downstream of the `aformat` resample/format stage. Per
//! frame it forwards end-of-stream markers untouched, copies the interleaved
//! `f32` samples into an owned [`AudioChunk`], and delivers it over a bounded
//! channel. There is no `max_*` cap in v1 — a time window
//! ([`duration_us`](super::SampleExtractor::duration_us)) covers the real use.

use super::chunk::AudioChunk;
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::util::ffmpeg_utils::frame_is_eof_marker;
use crossbeam_channel::Sender;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_channel_layout_describe, av_rescale_q, AVChannelLayout, AVFrame, AVMediaType,
    AVMediaType::AVMEDIA_TYPE_AUDIO, AVRational, AVSampleFormat::AV_SAMPLE_FMT_FLT, AV_NOPTS_VALUE,
};

const US_PER_SEC: AVRational = AVRational {
    num: 1,
    den: 1_000_000,
};

/// Copies post-`aformat` audio frames into owned chunks and streams them.
pub(crate) struct SampleSink {
    tx: Sender<AudioChunk>,
    /// Chunks emitted so far (becomes each chunk's `index`).
    emitted: u64,
    /// Set once the receiver is gone; stops emitting.
    done: bool,
}

impl SampleSink {
    pub(crate) fn new(tx: Sender<AudioChunk>) -> Self {
        Self {
            tx,
            emitted: 0,
            done: false,
        }
    }

    /// Reads a frame's presentation time in microseconds, or `None` when it has
    /// no usable timestamp.
    ///
    /// Uses the post-filter `pts` in the frame's own `time_base`, NOT
    /// `best_effort_timestamp`. The output-side aformat / aresample stage rewrites
    /// `pts`, `time_base`, and `duration` to the output sample-rate time base
    /// (`filter_task/runtime.rs`) but leaves the decoder-set `best_effort_timestamp`
    /// in its original stream time base; pairing that stale value with the
    /// rewritten `time_base` mis-scales every timestamp when resampling
    /// (e.g. 44.1 kHz → 16 kHz). The buffersink always sets `pts`.
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
}

impl FrameFilter for SampleSink {
    fn media_type(&self) -> AVMediaType {
        AVMEDIA_TYPE_AUDIO
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
        // After the receiver disconnects, drop the rest so the Drop-abort ends
        // the run without further work.
        if self.done {
            return Ok(None);
        }

        let pts_us = unsafe { Self::frame_pts_us(p) };
        let chunk = match unsafe { pack_chunk(p, pts_us, self.emitted)? } {
            Some(c) => c,
            // A zero-sample frame carries nothing to export; forward it so the
            // stream still terminates normally.
            None => return Ok(Some(frame)),
        };
        match self.tx.send(chunk) {
            Ok(()) => {
                self.emitted += 1;
                // Forward the frame so downstream terminators count it.
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

/// Copies one packed-`f32` audio frame into an owned interleaved [`AudioChunk`].
/// Returns `Ok(None)` for a zero-sample frame.
///
/// The graph pins `aformat=sample_fmts=flt` AND the encoder is `pcm_f32le`
/// (`sample_fmts={flt}`), so a non-`flt` (e.g. planar `fltp`, or `s16` if the
/// codec pin were dropped) frame here is an internal invariant breach — surfaced
/// as an error rather than silently mis-read. This is the audio twin of the
/// video sink's exact-pixel-format check.
///
/// # Safety
/// `p` must be a valid, non-null `AVFrame` describing packed audio samples.
unsafe fn pack_chunk(
    p: *const AVFrame,
    pts_us: Option<i64>,
    index: u64,
) -> Result<Option<AudioChunk>, FrameFilterError> {
    let expected = AV_SAMPLE_FMT_FLT as i32;
    if (*p).format != expected {
        return Err(format!(
            "sample export: expected packed f32 (AV_SAMPLE_FMT_FLT = {expected}), \
             got sample format {}",
            (*p).format
        )
        .into());
    }

    let nb_samples = (*p).nb_samples;
    let channels = (*p).ch_layout.nb_channels;
    if channels <= 0 {
        return Err(format!("sample export: non-positive channel count {channels}").into());
    }
    if nb_samples <= 0 {
        return Ok(None);
    }
    let sample_rate = (*p).sample_rate;
    if sample_rate <= 0 {
        return Err(format!("sample export: non-positive sample rate {sample_rate}").into());
    }
    // FFmpeg's own description of the frame's layout ("stereo", "5.1", …),
    // same vocabulary as the packet-sink stream info; empty when the layout
    // cannot be described.
    let channel_layout = describe_channel_layout(&(*p).ch_layout);

    // Total interleaved f32 count and its byte length, both overflow-checked.
    let n = (nb_samples as usize)
        .checked_mul(channels as usize)
        .ok_or_else(|| -> FrameFilterError { "sample export: sample count overflow".into() })?;
    let byte_len = n
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| -> FrameFilterError { "sample export: buffer size overflow".into() })?;

    let base = (*p).data[0];
    if base.is_null() {
        return Err("sample export: sample plane pointer is null".into());
    }
    // Packed audio occupies a single plane; `linesize[0]` is the (padded) byte
    // size of that plane and must cover the interleaved samples we read.
    let linesize = (*p).linesize[0];
    if linesize < 0 || (linesize as usize) < byte_len {
        return Err(format!(
            "sample export: linesize {linesize} smaller than {byte_len} packed sample bytes"
        )
        .into());
    }

    // Copy the interleaved samples out. `copy_nonoverlapping` from a `*const u8`
    // is alignment-agnostic on the source (FFmpeg aligns audio planes, but we do
    // not rely on that for `f32` reads). `AV_SAMPLE_FMT_FLT` is native-endian
    // float, so a host `f32` copy is correct on every target.
    let mut out = vec![0f32; n];
    std::ptr::copy_nonoverlapping(base, out.as_mut_ptr() as *mut u8, byte_len);

    Ok(Some(AudioChunk::new(
        pts_us,
        index,
        sample_rate as u32,
        channels as u16,
        channel_layout,
        out,
    )))
}

/// FFmpeg's textual channel-layout description (e.g. "stereo", "5.1"), empty
/// when the layout cannot be described. Mirrors the packet-sink helper of the
/// same name so both taps report layouts in the same vocabulary — except this
/// one always returns the complete description regardless of length, where
/// the sibling still truncates at a fixed capacity.
///
/// # Safety
/// `layout` must point to a valid, initialized `AVChannelLayout`, alive for
/// the call.
unsafe fn describe_channel_layout(layout: *const AVChannelLayout) -> String {
    describe_channel_layout_with_capacity(layout, 128)
}

/// `describe_channel_layout`, with the first-attempt buffer capacity exposed
/// so a too-small attempt can be exercised directly by tests.
///
/// `av_channel_layout_describe` NUL-terminates whatever prefix fits and
/// returns the number of bytes the full description needs (its length plus
/// the terminator); when that exceeds `capacity`, the first attempt was
/// truncated and this retries exactly once with a buffer sized to the
/// reported requirement. The layout does not change between the two calls,
/// so the retry always fits; a negative return, or a still-too-small report
/// on the retry, falls back to an empty string, same as the ordinary failure
/// case.
///
/// # Safety
/// `layout` must point to a valid, initialized `AVChannelLayout`, alive for
/// the call.
unsafe fn describe_channel_layout_with_capacity(
    layout: *const AVChannelLayout,
    capacity: usize,
) -> String {
    let mut buf = vec![0u8; capacity];
    let n = av_channel_layout_describe(layout, buf.as_mut_ptr() as *mut libc::c_char, buf.len());
    if n < 0 {
        return String::new();
    }
    if n as usize > buf.len() {
        // Truncated: the required size is exact, so a retry at that capacity
        // always fits — the layout is immutable across the two calls.
        buf = vec![0u8; n as usize];
        let retried =
            av_channel_layout_describe(layout, buf.as_mut_ptr() as *mut libc::c_char, buf.len());
        if retried < 0 || retried as usize > buf.len() {
            return String::new();
        }
    }
    match std::ffi::CStr::from_bytes_until_nul(&buf) {
        Ok(c) => c.to_string_lossy().into_owned(),
        Err(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::{av_channel_layout_default, av_channel_layout_uninit};

    /// A too-small first attempt still recovers the complete description via
    /// the retry, and a first attempt that already fits skips the retry
    /// entirely — both land on the same string.
    #[test]
    fn describe_channel_layout_with_capacity_recovers_the_full_description() {
        // SAFETY: zeroed then immediately initialized by `av_channel_layout_default`
        // before any read; uninitialized again once the test is done with it.
        let mut layout: AVChannelLayout = unsafe { std::mem::zeroed() };
        unsafe { av_channel_layout_default(&mut layout, 2) };

        // "stereo\0" needs 7 bytes; a 4-byte first attempt must truncate and retry.
        let via_retry = unsafe { describe_channel_layout_with_capacity(&layout, 4) };
        assert_eq!(via_retry, "stereo");

        // The default capacity fits "stereo" on the first attempt; no retry needed.
        let via_first_attempt = unsafe { describe_channel_layout_with_capacity(&layout, 128) };
        assert_eq!(via_first_attempt, "stereo");

        unsafe { av_channel_layout_uninit(&mut layout) };
    }
}
