//! Checked conversions from raw FFmpeg `c_int` format values to their typed
//! enums.
//!
//! `AVFrame.format`, `AVCodecContext.pix_fmt`/`sample_fmt` and friends are
//! stored as plain `c_int`. Turning one into an `AVPixelFormat` /
//! `AVSampleFormat` with a bare `std::mem::transmute` is **undefined behavior**
//! whenever the integer is outside the enum's valid discriminant range (a
//! corrupt/adversarial input frame, or a format value from a newer FFmpeg than
//! the one we were built against).
//!
//! These helpers centralize the range check that must precede any such
//! conversion, replacing the ad-hoc `transmute` call sites scattered across the
//! scheduler. They mirror the already-safe pattern in
//! [`crate::util::frame_utils::ensure_software_format`].

// Callers live in the FFI-only scheduler, which is compiled out on docs.rs
// (the `docsrs` cfg); suppress the resulting dead-code noise there.
#![cfg_attr(docsrs, allow(dead_code))]

use ffmpeg_sys_next::{
    av_pix_fmt_desc_get, AVPixFmtDescriptor, AVPixelFormat, AVSampleFormat,
};

/// Converts a raw `AVFrame.format` / `AVCodecContext.pix_fmt` integer into a
/// typed [`AVPixelFormat`], returning `None` when the value is outside the
/// enum's valid discriminant range (`0..AV_PIX_FMT_NB`).
pub(crate) fn pix_fmt_from_raw(raw: i32) -> Option<AVPixelFormat> {
    if raw < 0 || raw >= AVPixelFormat::AV_PIX_FMT_NB as i32 {
        return None;
    }
    // SAFETY: AVPixelFormat is a contiguous #[repr(i32)] enum covering
    // 0..AV_PIX_FMT_NB; the range check above guarantees `raw` is a valid
    // discriminant.
    Some(unsafe { std::mem::transmute::<i32, AVPixelFormat>(raw) })
}

/// Converts a raw `AVFrame.format` / `AVCodecContext.sample_fmt` integer into a
/// typed [`AVSampleFormat`], returning `None` when the value is outside the
/// enum's valid discriminant range (`0..AV_SAMPLE_FMT_NB`).
pub(crate) fn sample_fmt_from_raw(raw: i32) -> Option<AVSampleFormat> {
    if raw < 0 || raw >= AVSampleFormat::AV_SAMPLE_FMT_NB as i32 {
        return None;
    }
    // SAFETY: AVSampleFormat is a contiguous #[repr(i32)] enum covering
    // 0..AV_SAMPLE_FMT_NB; the range check above guarantees `raw` is valid.
    Some(unsafe { std::mem::transmute::<i32, AVSampleFormat>(raw) })
}

/// Checked pixel-format descriptor lookup. Returns a `'static` reference to
/// FFmpeg's descriptor table entry, or `None` for an out-of-range format value
/// or one FFmpeg has no descriptor for — instead of dereferencing a null
/// descriptor pointer.
pub(crate) fn pix_fmt_desc_from_raw(raw: i32) -> Option<&'static AVPixFmtDescriptor> {
    let fmt = pix_fmt_from_raw(raw)?;
    // SAFETY: `fmt` is a valid discriminant; av_pix_fmt_desc_get returns a
    // pointer into a static descriptor table (valid for the whole program) or
    // null. `as_ref` yields None on null.
    unsafe { av_pix_fmt_desc_get(fmt).as_ref() }
}
