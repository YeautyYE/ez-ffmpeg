//! Safe RAII ownership of an `AVSubtitle` and its decoder-allocated rects.
//!
//! `avcodec_decode_subtitle2` fills a caller-provided `AVSubtitle` with heap
//! allocations — the `rects` array and, per rect, its `data` / `text` / `ass` —
//! which the caller must release with `avsubtitle_free`. That "output struct you
//! must remember to free" is a leak surface: any `?` or early return between the
//! decode call and the free leaks the rects.
//!
//! [`Subtitle`] owns the `AVSubtitle` and frees it on [`Drop`], so it is freed
//! exactly once on every path. It mirrors the other `crate::raw` owners with one
//! twist: it owns the struct **by value**, not a pointer — FFmpeg fills it in
//! place and `avsubtitle_free` frees the inner allocations. A zeroed struct
//! (`num_rects = 0`, `rects = null`) is a valid empty state that
//! `avsubtitle_free` treats as a no-op; that is both the freshly-constructed
//! state and the state left behind after the contents are moved into a frame
//! buffer (`av_memdup` of the struct + zeroing the source).
//!
//! Naming follows the raw-module convention (drop the `AV` prefix, like
//! [`crate::raw::FormatContext`]); consumers refer to it path-qualified as
//! `raw::Subtitle`.

use ffmpeg_sys_next::{avsubtitle_free, AVSubtitle};

/// Sole owner of an `AVSubtitle`'s decoder-allocated contents.
///
/// Move-only (no `Clone`/`Copy`): one owner ⇒ one [`Drop`] ⇒ the rects are
/// freed exactly once. Ownership of the contents can be handed off by moving the
/// bytes out (`av_memdup`) and zeroing the struct through
/// [`as_mut_ptr`](Self::as_mut_ptr); this owner then holds an empty struct and
/// its `Drop` becomes a no-op.
pub(crate) struct Subtitle {
    sub: AVSubtitle,
}

impl Subtitle {
    /// A zeroed `AVSubtitle`, ready to be filled via
    /// [`as_mut_ptr`](Self::as_mut_ptr).
    ///
    /// The all-zero bit pattern is `AVSubtitle`'s valid empty state
    /// (`num_rects = 0`, `rects = null`) — exactly what
    /// `avcodec_decode_subtitle2` expects for its output parameter.
    pub(crate) fn zeroed() -> Self {
        // SAFETY: `AVSubtitle` is a plain C struct of integers plus a `rects`
        // pointer; an all-zero bit pattern is a valid, empty inhabitant (null
        // rects, zero counts).
        Self {
            sub: unsafe { std::mem::zeroed() },
        }
    }

    /// Borrow the owned subtitle as `*mut` for FFI: the decode output parameter,
    /// field reads, and moving the contents out.
    ///
    /// # Safety
    ///
    /// The returned pointer must not be freed and must not outlive `self`. The
    /// caller may move the contents out (e.g. `av_memdup` the struct then write
    /// a zeroed struct back through this pointer); afterwards this owner holds a
    /// zeroed struct and its `Drop` is a no-op.
    pub(crate) unsafe fn as_mut_ptr(&mut self) -> *mut AVSubtitle {
        &mut self.sub
    }
}

impl Drop for Subtitle {
    fn drop(&mut self) {
        // SAFETY: sole owner. `avsubtitle_free` frees the `rects` array and each
        // rect's `data`/`text`/`ass`, then zeroes the struct; on an empty or
        // moved-out subtitle (`rects = null`) it is a no-op.
        unsafe {
            avsubtitle_free(&mut self.sub);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_of_zeroed_is_a_noop() {
        // A zeroed owner (null rects, zero counts) must drop without freeing any
        // real allocation: `avsubtitle_free` iterates 0 rects and frees a null
        // `rects` pointer, a well-defined no-op.
        let _ = Subtitle::zeroed();
    }
}
