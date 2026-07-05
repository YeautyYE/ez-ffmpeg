//! Safe RAII ownership of a raw `*mut AVFilterGraph`.
//!
//! [`FilterGraph`] owns a libavfilter graph allocated by `avfilter_graph_alloc`
//! and frees it — along with every filter context it contains — via
//! `avfilter_graph_free` on [`Drop`]. It mirrors [`crate::raw::FormatContext`]:
//! move-only, single owner, single free, so the graph is released exactly once
//! on every return path. This replaces the hand-balanced `avfilter_graph_free`
//! calls that had to be repeated before each early return — and were missed on
//! some `?` paths, leaking the graph.
//!
//! Naming: this is the raw owning handle. It deliberately shares the short name
//! `FilterGraph` with the raw-module convention (drop the `AV` prefix, like
//! `FormatContext`), and is therefore distinct from the domain descriptor
//! [`crate::core::context::filter_graph::FilterGraph`]. Consumers that see both
//! refer to this one path-qualified as `raw::FilterGraph`.

// Compiled even under docsrs: the filter worker's slot and `cleanup_filtergraph`
// (which have no docsrs stub) name `Option<FilterGraph>` there. Under docsrs the
// real allocators are `#[cfg(not(docsrs))]`, so `alloc`/`as_ptr` are never
// called and would warn — suppress that (docsrs only; the normal build still
// catches real dead code).
#![cfg_attr(docsrs, allow(dead_code))]

use ffmpeg_sys_next::{avfilter_graph_alloc, avfilter_graph_free, AVFilterGraph};

/// Sole owner of a `*mut AVFilterGraph`.
///
/// Move-only (no `Clone`/`Copy`): the compiler therefore guarantees a single
/// owner and thus a single [`Drop`], which is what makes the "no double-free /
/// no leak" argument hold by construction. Ownership is transferred by moving
/// the value; there is no path that copies the raw pointer into a second owner.
pub(crate) struct FilterGraph {
    ptr: *mut AVFilterGraph,
}

// SAFETY: the graph is only dereferenced from the thread that owns the
// `FilterGraph`. The crate moves the value between threads (e.g. into the filter
// worker) but never shares it, and installs no thread-affine state that would
// make the move unsound. `Send` only, never `Sync` — concurrent access to the
// underlying graph would be unsound.
unsafe impl Send for FilterGraph {}

impl FilterGraph {
    /// Allocate a new, empty filter graph.
    ///
    /// Returns `None` on allocation failure — `avfilter_graph_alloc` returns
    /// null on OOM, which is surfaced as `None` rather than wrapping a null
    /// owner.
    pub(crate) fn alloc() -> Option<Self> {
        // SAFETY: `avfilter_graph_alloc` has no preconditions and returns null
        // on OOM; we never construct an owner around a null pointer here.
        let ptr = unsafe { avfilter_graph_alloc() };
        if ptr.is_null() {
            None
        } else {
            Some(Self { ptr })
        }
    }

    /// Borrow the raw pointer for FFI (`avfilter_graph_*` calls, field reads and
    /// writes).
    ///
    /// Returns `*mut` because every libavfilter entry point takes
    /// `*mut AVFilterGraph`.
    ///
    /// # Safety
    ///
    /// The returned pointer must not be freed and must not outlive `self`.
    pub(crate) unsafe fn as_ptr(&self) -> *mut AVFilterGraph {
        self.ptr
    }
}

impl Drop for FilterGraph {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // SAFETY: sole owner. `avfilter_graph_free` frees the graph and all its
        // filter contexts and nulls the pointer; the null check above keeps it a
        // no-op if the owner was never allocated.
        unsafe {
            avfilter_graph_free(&mut self.ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::null_mut;

    #[test]
    fn drop_of_null_is_a_noop() {
        // A null-pointer owner must be a pure no-op on drop (no free call).
        // Plain-Rust check with no FFI, so it also compiles under `--cfg docsrs`.
        let _ = FilterGraph { ptr: null_mut() };
    }
}
