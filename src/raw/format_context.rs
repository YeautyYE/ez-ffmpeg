//! Safe RAII ownership of a raw `*mut AVFormatContext`.
//!
//! [`FormatContext`] replaces the `is_input` + `is_set_callback` boolean pair on
//! the legacy `AVFormatContextBox` with a single typed [`Mode`] discriminant, so
//! the four mutually-exclusive teardown paths are selected by variant rather than
//! by a runtime boolean fan-out. The input-vs-output axis in particular is now a
//! type-level fact: `avformat_close_input` and `avformat_free_context` are not
//! interchangeable, and encoding the choice in the type removes the class of bug
//! where the wrong one is called.
//!
//! The teardown itself is **not** reimplemented here — [`Drop`] delegates to the
//! existing crate helpers [`in_fmt_ctx_free`] / [`out_fmt_ctx_free`], which own
//! the custom-IO ordering (capture `pb` before `avformat_close_input`, reclaim the
//! callback `Box` and AVIO buffer after). This is deliberately a thin ownership
//! wrapper, not a rewrite of the free logic.

use std::ptr::null_mut;

use ffmpeg_sys_next::AVFormatContext;

use crate::core::context::{in_fmt_ctx_free, out_fmt_ctx_free};

/// Teardown discriminant for [`FormatContext`].
///
/// Each variant maps to exactly one of the four `(is_input, is_set_callback)`
/// combinations the legacy free helpers dispatch on:
///
/// | variant | free call |
/// |---|---|
/// | [`Mode::Input`] | `in_fmt_ctx_free(ptr, false)` |
/// | [`Mode::InputCustomIo`] | `in_fmt_ctx_free(ptr, true)` |
/// | [`Mode::Output`] | `out_fmt_ctx_free(ptr, false)` |
/// | [`Mode::OutputCustomIo`] | `out_fmt_ctx_free(ptr, true)` |
///
/// Only [`Mode::Input`] is constructed by the current Rung-2 pilot (the read-only
/// input path). The remaining variants are wired when `Muxer`/`Demuxer` migrate;
/// `#[allow(dead_code)]` keeps the pilot warning-free until then.
#[allow(dead_code)]
enum Mode {
    /// Input context opened from a URL/path (no custom AVIO).
    Input,
    /// Input context with a custom read/seek AVIO callback whose `opaque` `Box`
    /// and AVIO buffer must be reclaimed on drop.
    InputCustomIo,
    /// Output context written to a file (no custom AVIO).
    Output,
    /// Output context with a custom write/seek AVIO callback.
    OutputCustomIo,
}

/// Sole owner of a `*mut AVFormatContext`.
///
/// Move-only (no `Clone`/`Copy`): the compiler therefore guarantees a single owner
/// and thus a single [`Drop`], which is what makes the "no double-free" argument
/// hold by construction. Ownership is transferred by moving the value; there is no
/// path in the pilot that copies the raw pointer into a second owner.
pub(crate) struct FormatContext {
    ptr: *mut AVFormatContext,
    mode: Mode,
}

// SAFETY: same reasoning as `AVFormatContextBox`'s own `unsafe impl Send`. The
// pointer is only dereferenced from the thread that owns the `FormatContext`; the
// crate moves the value between threads but never shares it, and the pilot path
// installs no thread-affine callbacks. `Send` only, never `Sync` — concurrent
// access to the underlying context would be unsound.
unsafe impl Send for FormatContext {}

impl FormatContext {
    /// Take ownership of an already-opened **input** context that installs no
    /// custom AVIO.
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null and have been returned by a successful
    /// `avformat_open_input` (optionally followed by `avformat_find_stream_info`).
    /// Ownership transfers to the returned value: the caller must not free `ptr`
    /// again, and must not retain another owner of it.
    pub(crate) unsafe fn from_input(ptr: *mut AVFormatContext) -> Self {
        Self {
            ptr,
            mode: Mode::Input,
        }
    }

    /// Take ownership of an already-opened **input** context that uses a custom
    /// read/seek AVIO callback (`AVFMT_FLAG_CUSTOM_IO`).
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null, opened with a custom `AVIOContext` already wired
    /// into `(*ptr).pb`, whose `opaque` is a `Box`-allocated callback state owned
    /// by this context. On drop the AVIO context, its buffer, and that `Box` are
    /// reclaimed (via `in_fmt_ctx_free(ptr, true)`). Ownership transfers to the
    /// returned value; the caller must not free `ptr` (or its `pb`) again.
    // Wired in PR-B (Demuxer/input custom-IO migration).
    #[allow(dead_code)]
    pub(crate) unsafe fn from_input_custom_io(ptr: *mut AVFormatContext) -> Self {
        Self {
            ptr,
            mode: Mode::InputCustomIo,
        }
    }

    /// Take ownership of an already-allocated **output** context written to a
    /// file/URL (no custom AVIO).
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null and returned by a successful
    /// `avformat_alloc_output_context2`. On drop, a file-backed `pb` is closed
    /// (`avio_closep`, unless `AVFMT_NOFILE`) and the context freed
    /// (`out_fmt_ctx_free(ptr, false)` → `avformat_free_context`). Ownership
    /// transfers to the returned value; the caller must not free `ptr` again.
    // Wired in PR-C (Muxer/output migration).
    #[allow(dead_code)]
    pub(crate) unsafe fn from_output(ptr: *mut AVFormatContext) -> Self {
        Self {
            ptr,
            mode: Mode::Output,
        }
    }

    /// Take ownership of an already-allocated **output** context that uses a
    /// custom write/seek AVIO callback (`AVFMT_FLAG_CUSTOM_IO`).
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null with a custom `AVIOContext` already wired into
    /// `(*ptr).pb` (its `opaque` a `Box`-allocated callback state owned by this
    /// context). Constructing this **before** `pb` is wired would leak the AVIO +
    /// `Box` on drop, since teardown reads `(*ptr).pb`. On drop the AVIO context,
    /// its buffer, and the `Box` are reclaimed then the context is freed
    /// (`out_fmt_ctx_free(ptr, true)`). Ownership transfers to the returned value.
    // Wired in PR-C (Muxer/output custom-IO migration).
    #[allow(dead_code)]
    pub(crate) unsafe fn from_output_custom_io(ptr: *mut AVFormatContext) -> Self {
        Self {
            ptr,
            mode: Mode::OutputCustomIo,
        }
    }

    /// Borrow the raw pointer for FFI that reads or advances the context
    /// (`av_read_frame`, `avformat_seek_file`, `av_find_best_stream`, field reads).
    ///
    /// Returns `*mut` because every relevant FFmpeg entry point takes `*mut
    /// AVFormatContext` even for read-only use.
    ///
    /// # Safety
    ///
    /// The returned pointer must not be freed and must not outlive `self`.
    pub(crate) unsafe fn as_ptr(&self) -> *mut AVFormatContext {
        self.ptr
    }
}

impl Drop for FormatContext {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // Delegates to the exact helpers `AVFormatContextBox::drop` uses; the
        // custom-IO ordering lives inside them and is not duplicated here.
        match self.mode {
            Mode::Input => in_fmt_ctx_free(self.ptr, false),
            Mode::InputCustomIo => in_fmt_ctx_free(self.ptr, true),
            Mode::Output => out_fmt_ctx_free(self.ptr, false),
            Mode::OutputCustomIo => out_fmt_ctx_free(self.ptr, true),
        }
        self.ptr = null_mut();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_of_null_is_a_noop() {
        // A null pointer must be a pure no-op on drop (no free call) for EVERY
        // variant. Plain-Rust check with no FFI, so it also compiles under
        // `--cfg docsrs`.
        for mode in [
            Mode::Input,
            Mode::InputCustomIo,
            Mode::Output,
            Mode::OutputCustomIo,
        ] {
            let _ = FormatContext {
                ptr: null_mut(),
                mode,
            };
        }
    }
}
