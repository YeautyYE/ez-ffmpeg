//! Shared, thread-safe handles surfaced to users: live shader parameters
//! and cumulative per-stage timing statistics.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Cumulative per-stage timings, readable while the filter runs via
/// [`crate::wgpu_filter::WgpuFrameFilter::stats_handle`].
#[derive(Clone, Copy, Debug, Default)]
pub struct WgpuFilterStats {
    /// Number of frames fully processed (readback completed).
    pub frames: u64,
    /// CPU time spent uploading planes/uniforms and encoding GPU passes.
    pub upload_secs: f64,
    /// Wall time the pipeline thread spent blocked waiting for GPU results.
    /// With `frames_in_flight > 1`, GPU work overlaps the next frame's CPU
    /// work, so this is typically much smaller than the actual GPU time.
    pub gpu_secs: f64,
    /// CPU time spent copying mapped readback bytes into output frames.
    pub download_secs: f64,
    /// Hardware input frames imported zero-copy (dmabuf).
    pub hw_import_frames: u64,
    /// Hardware input frames downloaded to system memory instead — with
    /// `hw_zero_copy_input` enabled, a nonzero count reveals the import
    /// fallback engaging (a large per-frame perf cliff that a log line
    /// alone cannot make visible mid-stream).
    pub hw_download_frames: u64,
    /// Wall time spent in `av_hwframe_transfer_data` downloads (previously
    /// folded into `upload_secs`, which hid the fallback's cost).
    pub hw_download_secs: f64,
}

/// Live handle for updating shader parameters from any thread while the
/// filter runs inside a pipeline. Created via
/// [`crate::wgpu_filter::WgpuFrameFilter::params_handle`].
pub struct WgpuParamsHandle<P: bytemuck::Pod> {
    pub(crate) bytes: Arc<Mutex<Vec<u8>>>,
    pub(crate) dirty: Arc<AtomicBool>,
    pub(crate) _marker: PhantomData<P>,
}

impl<P: bytemuck::Pod> WgpuParamsHandle<P> {
    /// Replaces the parameter value; the new bytes are uploaded before the
    /// next frame is rendered.
    pub fn set(&self, value: P) {
        // Tolerate a poisoned lock (the byte buffer is always left in a
        // valid state) so a panicked pipeline thread cannot take the user's
        // control thread down with it.
        let mut bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bytes.clear();
        bytes.extend_from_slice(bytemuck::bytes_of(&value));
        self.dirty.store(true, Ordering::Release);
    }

    /// Reads, modifies and writes back the parameter value as one atomic
    /// step — the lock is held across the closure, so concurrent `set` /
    /// `update` calls from other threads serialize instead of losing
    /// writes (`update(|p| p.gain += 0.1)` never overwrites a value it
    /// did not see). Because the lock is held, calling `set`/`update` on
    /// any handle to the same filter from inside the closure deadlocks.
    pub fn update(&self, f: impl FnOnce(&mut P)) {
        let mut bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Unaligned read: `Vec<u8>` does not guarantee alignment for `P`.
        let mut value: P = bytemuck::pod_read_unaligned(&bytes);
        f(&mut value);
        bytes.clear();
        bytes.extend_from_slice(bytemuck::bytes_of(&value));
        self.dirty.store(true, Ordering::Release);
    }
}

/// Parameter state shared between the filter, its params handles, and the
/// per-frame upload path.
pub(crate) struct SharedParams {
    pub(crate) bytes: Arc<Mutex<Vec<u8>>>,
    pub(crate) dirty: Arc<AtomicBool>,
    pub(crate) len: usize,
}

impl SharedParams {
    pub(crate) fn new(initial: Vec<u8>) -> Self {
        Self {
            len: initial.len(),
            bytes: Arc::new(Mutex::new(initial)),
            dirty: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, PartialEq, Debug, bytemuck::Pod, bytemuck::Zeroable)]
    struct Pair {
        a: f32,
        b: f32,
    }

    #[test]
    fn update_reads_modifies_and_marks_dirty() {
        let shared = SharedParams::new(bytemuck::bytes_of(&Pair { a: 1.0, b: 2.0 }).to_vec());
        let handle = WgpuParamsHandle::<Pair> {
            bytes: Arc::clone(&shared.bytes),
            dirty: Arc::clone(&shared.dirty),
            _marker: PhantomData,
        };
        shared.dirty.store(false, Ordering::Release);
        handle.update(|p| p.b += 40.0);
        assert!(shared.dirty.load(Ordering::Acquire));
        let bytes = shared
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let read: Pair = bytemuck::pod_read_unaligned(&bytes);
        assert_eq!(read, Pair { a: 1.0, b: 42.0 });
    }
}
