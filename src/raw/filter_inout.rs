//! Safe RAII ownership of a raw `*mut AVFilterInOut` list.
//!
//! An `AVFilterInOut` linked list is not allocated by this crate: libavfilter's
//! `avfilter_graph_segment_apply` / `avfilter_graph_parse_ptr` write a freshly
//! allocated list into a caller-supplied `*mut *mut AVFilterInOut` out-parameter,
//! and the caller must free the whole list with `avfilter_inout_free`. That
//! "output param you must remember to free" is a classic leak surface: any `?`
//! or early return between the parse call and the free leaks the list.
//!
//! [`FilterInOut`] owns the list head and frees it on [`Drop`], so it is freed
//! exactly once on every path. It mirrors [`crate::raw::FilterGraph`], with one
//! twist: the value starts [`empty`](FilterInOut::empty) (null) and is filled by
//! handing FFmpeg [`as_out_ptr`](FilterInOut::as_out_ptr). Unlike `FilterGraph`
//! it is intentionally **not** `Send`: these lists live and die inside a single
//! parse function and never cross threads.

use std::ptr::null_mut;

use ffmpeg_sys_next::{avfilter_inout_free, AVFilterInOut};

/// Sole owner of an `*mut AVFilterInOut` list head.
///
/// Move-only (no `Clone`/`Copy`): one owner ⇒ one [`Drop`] ⇒ the list is freed
/// exactly once. `avfilter_inout_free` frees the entire linked list (every node
/// and its `name`), so owning the head owns the whole list.
pub(crate) struct FilterInOut {
    ptr: *mut AVFilterInOut,
}

impl FilterInOut {
    /// An empty (null) owner, to be filled via [`as_out_ptr`](Self::as_out_ptr)
    /// by an FFmpeg out-parameter.
    #[inline]
    pub(crate) fn empty() -> Self {
        Self { ptr: null_mut() }
    }

    /// The `*mut *mut AVFilterInOut` slot to hand FFmpeg as an out-parameter
    /// (`avfilter_graph_segment_apply`, `avfilter_graph_parse_ptr`). FFmpeg
    /// writes the allocated list head here; this owner then frees it on drop.
    ///
    /// # Safety
    ///
    /// Must be called on an `empty()` owner (its pointer still null). FFmpeg
    /// overwrites the slot, so calling this when the owner already holds a list
    /// would leak that existing list.
    #[inline]
    pub(crate) unsafe fn as_out_ptr(&mut self) -> *mut *mut AVFilterInOut {
        &mut self.ptr
    }

    /// Borrow the list head for walking (`(*head).next`, `.name`, `.filter_ctx`).
    ///
    /// Returns `*mut` because the libavfilter walk APIs take `*mut AVFilterInOut`.
    /// Yields null for an empty list, which the callers already treat as "no
    /// entries".
    ///
    /// # Safety
    ///
    /// The returned pointer must not be freed and must not outlive `self`.
    #[inline]
    pub(crate) unsafe fn as_ptr(&self) -> *mut AVFilterInOut {
        self.ptr
    }
}

impl Drop for FilterInOut {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // SAFETY: sole owner. `avfilter_inout_free` frees the whole list and
        // nulls the pointer; the null check keeps it a no-op for an empty owner.
        // It only frees the list nodes, never the `filter_ctx` they point at
        // (those belong to the graph), so this is independent of graph teardown.
        unsafe {
            avfilter_inout_free(&mut self.ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_of_empty_is_a_noop() {
        // An empty (null) owner must be a pure no-op on drop (no free call).
        // Plain-Rust check with no FFI, so it also compiles under `--cfg docsrs`.
        let _ = FilterInOut::empty();
    }
}
