//! [`WgpuFrameFilter`]: the public GPU frame filter, its builder, and the
//! in-flight output queue that overlaps GPU work with CPU work.

use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::FrameFilter;
use crate::util::frame_utils::ensure_software_format;
use crate::wgpu_filter::frame_io;
use crate::wgpu_filter::gpu_state::{GpuState, OutputGeometry};
use crate::wgpu_filter::params::SharedParams;
use crate::wgpu_filter::shaders;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::AVMediaType;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use crate::wgpu_filter::params::{WgpuFilterStats, WgpuParamsHandle};

/// Upper bound for [`WgpuFrameFilterBuilder::frames_in_flight`].
const MAX_FRAMES_IN_FLIGHT: usize = 4;

/// Builder for [`WgpuFrameFilter`].
pub struct WgpuFrameFilterBuilder {
    fragment_shader: String,
    output_size: Option<(u32, u32)>,
    params_bytes: Vec<u8>,
    frames_in_flight: usize,
}

impl WgpuFrameFilterBuilder {
    /// Full WGSL fragment shader module. Contract (see also the identity
    /// template in the module docs): entry point
    /// `@fragment fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32>`
    /// with bindings `@group(0)`: `texture1` (converted RGBA input),
    /// `sampler1`, `ez: EzUniforms { play_time, width, height, _pad }`.
    /// Optional user parameters: `@group(1) @binding(0) var<uniform> ...`.
    pub fn shader_wgsl(mut self, source: impl Into<String>) -> Self {
        self.fragment_shader = source.into();
        self
    }

    /// Output frame size; defaults to the input size. The effect pass renders
    /// at this size while sampling the input-sized texture (i.e. resizing).
    pub fn output_size(mut self, width: u32, height: u32) -> Self {
        self.output_size = Some((width, height));
        self
    }

    /// Initial value for the user parameter uniform (`@group(1) @binding(0)`).
    /// Use [`WgpuFrameFilter::params_handle`] after `build()` to update it live.
    pub fn params<P: bytemuck::Pod>(mut self, initial: P) -> Self {
        self.params_bytes = bytemuck::bytes_of(&initial).to_vec();
        self
    }

    /// How many frames may be in flight on the GPU at once (1..=4).
    ///
    /// The default of 2 overlaps frame N's GPU work and readback with frame
    /// N+1's CPU work, hiding most of the GPU roundtrip cost. Output order is
    /// always preserved; results delayed past their `filter_frame` call are
    /// delivered through `request_frame`, which pipelines poll continuously.
    /// Set 1 for strictly synchronous behavior where every `filter_frame`
    /// call returns its own frame (lowest latency, e.g. live streaming).
    pub fn frames_in_flight(mut self, count: usize) -> Self {
        self.frames_in_flight = count;
        self
    }

    pub fn build(self) -> Result<WgpuFrameFilter, String> {
        if self.fragment_shader.is_empty() {
            return Err("WgpuFrameFilter requires a fragment shader (shader_wgsl)".to_string());
        }
        if let Some((w, h)) = self.output_size {
            if w == 0 || h == 0 {
                return Err(format!("Invalid output size {w}x{h}"));
            }
        }
        // `is_multiple_of` would need Rust 1.87; keep `%` for the older MSRV.
        #[allow(clippy::manual_is_multiple_of)]
        if self.params_bytes.len() % 4 != 0 {
            return Err(format!(
                "Params type size must be a multiple of 4 bytes (got {}); \
                 add explicit padding fields to the struct",
                self.params_bytes.len()
            ));
        }
        if !(1..=MAX_FRAMES_IN_FLIGHT).contains(&self.frames_in_flight) {
            return Err(format!(
                "frames_in_flight must be within 1..={MAX_FRAMES_IN_FLIGHT} (got {})",
                self.frames_in_flight
            ));
        }
        Ok(WgpuFrameFilter {
            fragment_shader: self.fragment_shader,
            output_size: self.output_size,
            frames_in_flight: self.frames_in_flight,
            params: SharedParams::new(self.params_bytes),
            stats: Arc::new(Mutex::new(WgpuFilterStats::default())),
            gpu: None,
            pending: VecDeque::new(),
        })
    }
}

/// GPU-accelerated video frame filter backed by wgpu (Vulkan/Metal/DX12/GL).
///
/// Successor to `OpenGLFrameFilter`: headless-capable (no windowing system
/// required), converts YUV<->RGB on the GPU with the correct color matrix
/// (BT.601/BT.709, limited/full range), supports output resizing and live
/// parameter updates.
///
/// Input formats: YUV420P, YUV422P, YUV444P (plus their full-range J
/// variants) and NV12 CPU frames. Output is always YUV420P; 4:2:2/4:4:4
/// inputs are chroma-downsampled on the GPU.
///
/// By default up to two frames are in flight on the GPU (see
/// [`WgpuFrameFilterBuilder::frames_in_flight`]), so readback of one frame
/// overlaps the next frame's upload; output order is always preserved.
///
/// The filter consumes and produces CPU frames, so it works with any
/// decoder/encoder combination; hardware frames are rejected with guidance.
pub struct WgpuFrameFilter {
    fragment_shader: String,
    output_size: Option<(u32, u32)>,
    frames_in_flight: usize,
    params: SharedParams,
    stats: Arc<Mutex<WgpuFilterStats>>,
    gpu: Option<GpuState>,
    pending: VecDeque<PendingOutput>,
}

/// A frame submitted to the GPU whose readback has not completed yet.
struct InFlightFrame {
    staging: wgpu::Buffer,
    submission: wgpu::SubmissionIndex,
    map_rx: Receiver<Result<(), wgpu::BufferAsyncError>>,
    /// Input frame kept alive as the property donor (pts, color tags, ...).
    src_props: Frame,
    /// Geometry snapshot so the readback stays valid even if the shared
    /// resources are rebuilt for a new input size in the meantime.
    geo: OutputGeometry,
}

/// Output queue entry. Frames leave from the front strictly in arrival
/// order; a `Gpu` entry morphs into `Done` in place once its readback
/// completes, so pass-through frames can never overtake GPU frames.
enum PendingOutput {
    /// Submitted to the GPU, result pending.
    Gpu(InFlightFrame),
    /// Ready to hand downstream: a completed GPU result or a pass-through
    /// frame (props-only EOF markers, degenerate frames).
    Done(Frame),
}

impl WgpuFrameFilter {
    pub fn builder() -> WgpuFrameFilterBuilder {
        WgpuFrameFilterBuilder {
            fragment_shader: String::new(),
            output_size: None,
            params_bytes: Vec::new(),
            frames_in_flight: 2,
        }
    }

    /// Creates a filter from a WGSL fragment shader with default settings,
    /// mirroring `OpenGLFrameFilter::new_simple`.
    pub fn new_simple(fragment_shader_wgsl: impl Into<String>) -> Result<Self, String> {
        Self::builder().shader_wgsl(fragment_shader_wgsl).build()
    }

    /// Identity filter (pass-through through the full GPU pipeline); useful
    /// for measuring the pipeline's fixed cost.
    pub fn new_identity() -> Result<Self, String> {
        Self::new_simple(shaders::IDENTITY_FS)
    }

    /// Typed handle for live parameter updates. `P` must match the type given
    /// to [`WgpuFrameFilterBuilder::params`].
    pub fn params_handle<P: bytemuck::Pod>(&self) -> Result<WgpuParamsHandle<P>, String> {
        if std::mem::size_of::<P>() != self.params.len {
            return Err(format!(
                "Params type size mismatch: builder was given {} bytes, handle type has {} bytes",
                self.params.len,
                std::mem::size_of::<P>()
            ));
        }
        Ok(WgpuParamsHandle {
            bytes: Arc::clone(&self.params.bytes),
            dirty: Arc::clone(&self.params.dirty),
            _marker: PhantomData,
        })
    }

    /// Shared handle to cumulative per-stage timing statistics.
    pub fn stats_handle(&self) -> Arc<Mutex<WgpuFilterStats>> {
        Arc::clone(&self.stats)
    }

    fn gpu_pending_count(&self) -> usize {
        self.pending
            .iter()
            .filter(|p| matches!(p, PendingOutput::Gpu(_)))
            .count()
    }

    /// Completes the oldest in-flight GPU frame in place: it keeps its output
    /// queue position, morphing from `Gpu` to `Done`. With `block = false`
    /// this returns `Ok(false)` when the result is not ready yet; with
    /// `block = true` it always resolves (or errors).
    fn complete_oldest_gpu(&mut self, block: bool) -> Result<bool, String> {
        let Some(pos) = self
            .pending
            .iter()
            .position(|p| matches!(p, PendingOutput::Gpu(_)))
        else {
            return Ok(true);
        };

        {
            let gpu = self.gpu.as_ref().ok_or("WgpuFrameFilter not initialized")?;
            let PendingOutput::Gpu(slot) = &self.pending[pos] else {
                unreachable!("position() matched a Gpu entry");
            };
            let t_wait = Instant::now();
            if !wait_for_map(&gpu.device, slot, block)? {
                return Ok(false);
            }
            if let Ok(mut stats) = self.stats.lock() {
                stats.gpu_secs += t_wait.elapsed().as_secs_f64();
            }
        }

        let Some(PendingOutput::Gpu(slot)) = self.pending.remove(pos) else {
            unreachable!("pos still points at the Gpu entry checked above");
        };
        let t_download = Instant::now();
        let mapped = slot.staging.slice(..).get_mapped_range();
        let built = frame_io::build_output_frame(&mapped, &slot.geo, &slot.src_props);
        drop(mapped);
        slot.staging.unmap();
        let frame = built?;
        let download_secs = t_download.elapsed().as_secs_f64();

        // Recycle the staging buffer while it matches the current geometry;
        // buffers from an older geometry are dropped (the pool was refilled
        // when the resources were rebuilt).
        if let Some(res) = self.gpu.as_mut().and_then(|g| g.resources.as_mut()) {
            if res.buf_size == slot.geo.buf_size && res.staging_pool.len() < self.frames_in_flight
            {
                res.staging_pool.push(slot.staging);
            }
        }

        if let Ok(mut stats) = self.stats.lock() {
            stats.frames += 1;
            stats.download_secs += download_secs;
        }
        self.pending.insert(pos, PendingOutput::Done(frame));
        Ok(true)
    }

    /// Pops the next output frame if one is (or can be made) available while
    /// respecting arrival order.
    fn next_output(&mut self, block: bool) -> Result<Option<Frame>, String> {
        loop {
            match self.pending.front() {
                None => return Ok(None),
                Some(PendingOutput::Done(_)) => {
                    let Some(PendingOutput::Done(frame)) = self.pending.pop_front() else {
                        unreachable!("front() matched Done");
                    };
                    return Ok(Some(frame));
                }
                Some(PendingOutput::Gpu(_)) => {
                    if !self.complete_oldest_gpu(block)? {
                        return Ok(None);
                    }
                    // The front entry is now Done; loop around to pop it.
                }
            }
        }
    }
}

/// Waits (or polls) for a staging buffer map to complete. Returns `Ok(false)`
/// when `block` is false and the map is not ready yet.
fn wait_for_map(
    device: &wgpu::Device,
    slot: &InFlightFrame,
    block: bool,
) -> Result<bool, String> {
    fn check(
        rx: &Receiver<Result<(), wgpu::BufferAsyncError>>,
    ) -> Result<Option<()>, String> {
        match rx.try_recv() {
            Ok(Ok(())) => Ok(Some(())),
            Ok(Err(e)) => Err(format!("Staging buffer map failed: {e:?}")),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err("Staging map callback dropped".to_string()),
        }
    }

    if check(&slot.map_rx)?.is_some() {
        return Ok(true);
    }
    if !block {
        device
            .poll(wgpu::PollType::Poll)
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
        return Ok(check(&slot.map_rx)?.is_some());
    }
    loop {
        device
            .poll(wgpu::PollType::WaitForSubmissionIndex(
                slot.submission.clone(),
            ))
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
        if check(&slot.map_rx)?.is_some() {
            return Ok(true);
        }
        // The submission has executed but the map callback has not fired
        // yet; a full wait pumps the remaining callbacks.
        device
            .poll(wgpu::PollType::Wait)
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
    }
}

impl FrameFilter for WgpuFrameFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn init(&mut self, _ctx: &FrameFilterContext) -> Result<(), String> {
        let gpu = GpuState::new(&self.fragment_shader, self.params.len)?;
        self.gpu = Some(gpu);
        Ok(())
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, String> {
        // SAFETY: probing only reads pointers/scalars of a live frame.
        let bypass = unsafe {
            frame.as_ptr().is_null()
                || frame.is_empty()
                || (*frame.as_ptr()).width <= 0
                || (*frame.as_ptr()).height <= 0
        };
        if bypass {
            // Props-only frames (e.g. the EOF timestamp marker decoders send
            // through pipelines) pass through untouched. They are queued so
            // they leave in arrival order behind any in-flight GPU frames.
            self.pending.push_back(PendingOutput::Done(frame));
            return self.next_output(false);
        }

        // SAFETY: `frame` is a live frame; scalar field reads only.
        let raw = unsafe { frame.as_ptr() };
        let pix_fmt = ensure_software_format(unsafe { (*raw).format })
            .map_err(|e| format!("WgpuFrameFilter: {e}"))?;
        let (layout, full_range) =
            frame_io::detect_format(pix_fmt, unsafe { (*raw).color_range })?;
        let (in_w, in_h) = unsafe { ((*raw).width as u32, (*raw).height as u32) };
        let matrix_id = frame_io::matrix_id_for(unsafe { (*raw).colorspace }, in_h as i32);

        let gpu = self.gpu.as_mut().ok_or("WgpuFrameFilter not initialized")?;

        // create_texture failures bypass error scopes and panic the pipeline
        // thread through wgpu's uncaptured-error handler; reject oversized
        // frames here with actionable guidance instead.
        let (out_w, out_h) = self.output_size.unwrap_or((in_w, in_h));
        let max_dim = gpu.device.limits().max_texture_dimension_2d;
        if in_w > max_dim || in_h > max_dim || out_w > max_dim || out_h > max_dim {
            return Err(format!(
                "Frame size {in_w}x{in_h} (output {out_w}x{out_h}) exceeds the device's \
                 maximum texture dimension of {max_dim}; downscale first, e.g. insert \
                 `scale` in filter_desc before this pipeline"
            ));
        }
        gpu.ensure_resources(in_w, in_h, layout, self.output_size, self.frames_in_flight);

        // Cap in-flight work: when the staging pool is exhausted, block for
        // the oldest result (it keeps its position in the output queue).
        while self
            .gpu
            .as_ref()
            .and_then(|g| g.resources.as_ref())
            .is_some_and(|r| r.staging_pool.is_empty())
        {
            if self.gpu_pending_count() == 0 {
                return Err(
                    "WgpuFrameFilter internal error: staging pool exhausted with no \
                     in-flight frames"
                        .to_string(),
                );
            }
            self.complete_oldest_gpu(true)?;
        }

        let (staging, geo) = {
            let res = self
                .gpu
                .as_mut()
                .and_then(|g| g.resources.as_mut())
                .expect("resources ensured above");
            (
                res.staging_pool.pop().expect("pool refilled above"),
                res.geometry(),
            )
        };

        let t_upload = Instant::now();
        let gpu = self.gpu.as_ref().expect("initialized above");
        let submitted = frame_io::upload_and_encode(
            gpu,
            &frame,
            layout,
            matrix_id,
            full_range,
            &staging,
            &self.params,
        );
        let (submission, map_rx) = match submitted {
            Ok(v) => v,
            Err(e) => {
                // Keep the pool consistent even though the job is failing.
                if let Some(res) = self.gpu.as_mut().and_then(|g| g.resources.as_mut()) {
                    res.staging_pool.push(staging);
                }
                return Err(e);
            }
        };
        if let Ok(mut stats) = self.stats.lock() {
            stats.upload_secs += t_upload.elapsed().as_secs_f64();
        }

        self.pending.push_back(PendingOutput::Gpu(InFlightFrame {
            staging,
            submission,
            map_rx,
            src_props: frame,
            geo,
        }));

        // Synchronous mode returns its own frame; async mode returns whatever
        // is ready without blocking (delayed results flow via request_frame).
        let block = self.frames_in_flight == 1;
        self.next_output(block)
    }

    fn request_frame(&mut self, _ctx: &FrameFilterContext) -> Result<Option<Frame>, String> {
        self.next_output(false)
    }

    fn uninit(&mut self, _ctx: &FrameFilterContext) {
        // Abandon any in-flight readbacks; wgpu unmaps and frees buffers on
        // drop, and pass-through frames are simply released.
        self.pending.clear();
        self.gpu = None;
    }
}
