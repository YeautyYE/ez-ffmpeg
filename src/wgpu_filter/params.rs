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
