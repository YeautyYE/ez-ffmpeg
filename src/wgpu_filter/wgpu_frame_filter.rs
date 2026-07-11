//! [`WgpuFrameFilter`]: the public GPU frame filter, its builder, and the
//! in-flight output queue that overlaps GPU work with CPU work.

use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError};
use crate::util::frame_utils::{ensure_software_format, is_hw_format};
use crate::wgpu_filter::frame_io::{self, HwMappedFrame, PlaneLayout};
use crate::wgpu_filter::gpu_state::{create_staging, GpuState, OutputGeometry, StagingSlot};
use crate::wgpu_filter::hw_interop;
use crate::wgpu_filter::params::SharedParams;
use crate::wgpu_filter::shaders;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::AVMediaType;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, TryRecvError};
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
    zero_copy_readback: bool,
    hw_zero_copy_input: bool,
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

    /// Experimental: hand output frames to the pipeline whose planes point
    /// directly into the GPU readback buffer, skipping the per-frame CPU
    /// copy into a freshly allocated AVFrame.
    ///
    /// The readback buffer stays mapped and lent out until every reference
    /// to the frame is dropped (typically right after encoding), then
    /// returns to the filter for reuse. Frames are marked read-only; FFmpeg
    /// code that needs to write them copies first (`av_frame_make_writable`).
    ///
    /// Trade-offs: peak GPU-visible memory grows from `frames_in_flight`
    /// readback buffers to one per frame simultaneously alive downstream —
    /// the pipeline channel plus whatever the encoder holds internally
    /// (e.g. libx264's default lookahead keeps ~40 frames, so 4K output
    /// pins ~500 MB until those frames are consumed). Downstream consumers
    /// also read from GPU-mapped memory, which can be slower than system
    /// RAM on discrete cards. Best suited to integrated GPUs with unified
    /// memory and consumers that release frames promptly.
    pub fn zero_copy_readback(mut self, enabled: bool) -> Self {
        self.zero_copy_readback = enabled;
        self
    }

    /// Experimental: import hardware decoder frames (VAAPI) directly into
    /// GPU textures over DRM PRIME dmabufs, skipping the download to system
    /// memory and the re-upload entirely.
    ///
    /// Requires the Vulkan backend with dmabuf-import extensions (Linux;
    /// RADV/ANV). When unavailable — or when a particular frame cannot be
    /// imported — hardware frames transparently fall back to
    /// `av_hwframe_transfer_data` download, so enabling this is always safe.
    /// Software input frames are unaffected.
    ///
    /// Marked experimental because wgpu tracks imported textures as
    /// uninitialized and its first layout transition is from `UNDEFINED`,
    /// which the Vulkan spec allows to discard contents. On drivers where
    /// video dmabuf surfaces carry no compression metadata (verified on
    /// RADV) the transition preserves texels; validate on your target
    /// driver before production use.
    pub fn hw_zero_copy_input(mut self, enabled: bool) -> Self {
        self.hw_zero_copy_input = enabled;
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
            zero_copy_readback: self.zero_copy_readback,
            hw_zero_copy_input: self.hw_zero_copy_input,
            params: SharedParams::new(self.params_bytes),
            stats: Arc::new(Mutex::new(WgpuFilterStats::default())),
            gpu: None,
            pending: VecDeque::new(),
            copy_output_pool: None,
            recycle: mpsc::channel(),
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
/// The filter consumes CPU frames and produces CPU frames, so it works with
/// any decoder/encoder combination. Hardware decoder frames are accepted
/// too: they are downloaded to system memory automatically, or imported
/// zero-copy via [`WgpuFrameFilterBuilder::hw_zero_copy_input`].
/// On unified-memory GPUs, [`WgpuFrameFilterBuilder::zero_copy_readback`]
/// can additionally skip the readback copy into system RAM.
pub struct WgpuFrameFilter {
    fragment_shader: String,
    output_size: Option<(u32, u32)>,
    frames_in_flight: usize,
    zero_copy_readback: bool,
    hw_zero_copy_input: bool,
    params: SharedParams,
    stats: Arc<Mutex<WgpuFilterStats>>,
    gpu: Option<GpuState>,
    pending: VecDeque<PendingOutput>,
    /// Per-geometry pool backing the default copy-path output frames, so each
    /// frame reuses a pooled `AVBufferRef` instead of a fresh per-frame
    /// allocation. Rebuilt when the output geometry changes.
    copy_output_pool: Option<frame_io::OutputFramePool>,
    /// Return path for zero-copy staging slots: the AVBuffer free callback
    /// sends the slot from whichever thread drops the last frame reference;
    /// `filter_frame` drains it back into the staging pool.
    recycle: (mpsc::Sender<StagingSlot>, Receiver<StagingSlot>),
}

/// A frame submitted to the GPU whose readback has not completed yet.
struct InFlightFrame {
    staging: StagingSlot,
    submission: wgpu::SubmissionIndex,
    /// Submission time, for detecting a wedged device during polling.
    submitted_at: Instant,
    /// Input frame kept alive as the property donor (pts, color tags, ...).
    src_props: Frame,
    /// For zero-copy hardware input: the mapped DRM frame and imported
    /// textures, which must stay alive until this submission's GPU reads
    /// complete (the decoder must not recycle the surface underneath them).
    _hw: Option<HwMappedFrame>,
    /// Geometry snapshot so the readback stays valid even if the shared
    /// resources are rebuilt for a new input size in the meantime.
    geo: OutputGeometry,
}

/// Output queue entry. Frames leave from the front strictly in arrival
/// order; a `Gpu` entry morphs into `Done` in place once its readback
/// completes, so pass-through frames can never overtake GPU frames.
// The size difference between variants is fine: the queue holds at most
// frames_in_flight + a few marker entries, never enough for Box overhead
// to pay off.
#[allow(clippy::large_enum_variant)]
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
            zero_copy_readback: false,
            hw_zero_copy_input: false,
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

    /// Returns released zero-copy staging buffers to the pool. Buffers whose
    /// size no longer matches the current geometry are dropped, as is any
    /// surplus beyond the pool cap (downstream can briefly hold more frames
    /// than the pool wants to keep across an idle stretch).
    fn drain_recycled(&mut self) {
        // frames_in_flight covers submission; the extra headroom mirrors the
        // pipeline's bounded channel, where released frames arrive in bursts.
        let cap = self.frames_in_flight + 8;
        let res = self.gpu.as_mut().and_then(|g| g.resources.as_mut());
        match res {
            Some(res) => {
                while let Ok(slot) = self.recycle.1.try_recv() {
                    if slot.buf_size == res.buf_size && res.staging_pool.len() < cap {
                        res.staging_pool.push(slot);
                    }
                }
            }
            None => while self.recycle.1.try_recv().is_ok() {},
        }
    }

    /// Returns the copy-path output pool for `geo`, rebuilding it when the
    /// geometry changed. Replacing the pool drops the old one
    /// (`av_buffer_pool_uninit`); frames already handed downstream keep their
    /// own `AVBufferRef`, so the old buffers survive until those refs drop.
    fn copy_output_pool_for(
        &mut self,
        geo: OutputGeometry,
    ) -> Result<&frame_io::OutputFramePool, String> {
        let stale = match &self.copy_output_pool {
            Some(pool) => pool.key() != geo,
            None => true,
        };
        if stale {
            self.copy_output_pool = Some(frame_io::OutputFramePool::new(geo)?);
        }
        Ok(self.copy_output_pool.as_ref().expect("pool set above"))
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
        let frame = if self.zero_copy_readback {
            // Lend the mapped staging buffer out inside the frame; it comes
            // back through `recycle` when the last reference drops.
            frame_io::build_output_frame_zero_copy(
                slot.staging,
                &slot.geo,
                &slot.src_props,
                &self.recycle.0,
            )?
        } else {
            let mapped = slot.staging.buffer.slice(..).get_mapped_range();
            // Fold the pool lookup into `built` so the staging buffer is always
            // unmapped below, even if the pool has to be (re)built and fails.
            let built = self
                .copy_output_pool_for(slot.geo)
                .and_then(|pool| pool.build_frame(&mapped, &slot.src_props));
            drop(mapped);
            slot.staging.buffer.unmap();
            let frame = built?;

            // Recycle the staging buffer while it matches the current
            // geometry; buffers from an older geometry are dropped (the pool
            // was refilled when the resources were rebuilt).
            if let Some(res) = self.gpu.as_mut().and_then(|g| g.resources.as_mut()) {
                if res.buf_size == slot.geo.buf_size
                    && res.staging_pool.len() < self.frames_in_flight
                {
                    res.staging_pool.push(slot.staging);
                }
            }
            frame
        };
        let download_secs = t_download.elapsed().as_secs_f64();

        if let Ok(mut stats) = self.stats.lock() {
            stats.frames += 1;
            stats.download_secs += download_secs;
        }
        self.pending.insert(pos, PendingOutput::Done(frame));
        Ok(true)
    }

    /// Pops the next output frame if one is (or can be made) available while
    /// respecting arrival order.
    fn next_output(&mut self, block: bool) -> Result<Option<Frame>, FrameFilterError> {
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

/// Upper bound on readback progress: a submission older than this whose map
/// callback has not fired means the device is wedged (hung driver, lost
/// device with no error callback). Both the blocking wait and the
/// non-blocking drain polls enforce it, so a wedged device surfaces as a
/// filter error instead of a silent hang; the scheduler cannot interrupt a
/// thread that is inside a filter call, and the completed work a healthy
/// device delivers on the next poll is never affected. The blocking path
/// checks the deadline between polls, and a single `device.poll(Wait…)` can
/// itself block for wgpu-core's internal 60 s cleanup timeout — so on a
/// wedged device the error may surface after up to two of those waits,
/// later than this constant but still bounded.
const GPU_COMPLETION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Waits (or polls) for a staging buffer map to complete. Returns `Ok(false)`
/// when `block` is false and the map is not ready yet.
fn wait_for_map(device: &wgpu::Device, slot: &InFlightFrame, block: bool) -> Result<bool, String> {
    fn check(slot: &InFlightFrame) -> Result<Option<()>, String> {
        match slot.staging.map_result() {
            Ok(Ok(())) => Ok(Some(())),
            Ok(Err(e)) => Err(format!("Staging buffer map failed: {e:?}")),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err("Staging map callback dropped".to_string()),
        }
    }

    if check(slot)?.is_some() {
        return Ok(true);
    }
    if !block {
        device
            .poll(wgpu::PollType::Poll)
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
        if check(slot)?.is_some() {
            return Ok(true);
        }
        if slot.submitted_at.elapsed() > GPU_COMPLETION_TIMEOUT {
            return Err(format!(
                "GPU readback made no progress for {}s; the device appears hung",
                GPU_COMPLETION_TIMEOUT.as_secs()
            ));
        }
        return Ok(false);
    }
    let deadline = Instant::now() + GPU_COMPLETION_TIMEOUT;
    loop {
        device
            .poll(wgpu::PollType::WaitForSubmissionIndex(
                slot.submission.clone(),
            ))
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
        if check(slot)?.is_some() {
            return Ok(true);
        }
        // The submission has executed but the map callback has not fired
        // yet; a full wait pumps the remaining callbacks.
        device
            .poll(wgpu::PollType::Wait)
            .map_err(|e| format!("Device poll failed: {e:?}"))?;
        if Instant::now() > deadline {
            return Err(format!(
                "GPU readback made no progress for {}s; the device appears hung",
                GPU_COMPLETION_TIMEOUT.as_secs()
            ));
        }
    }
}

impl FrameFilter for WgpuFrameFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn init(&mut self, _ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        let gpu = GpuState::new(
            &self.fragment_shader,
            self.params.len,
            self.hw_zero_copy_input,
        )?;
        self.gpu = Some(gpu);
        Ok(())
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // A props-only marker (buf[0] null — see frame_is_eof_marker, which
        // avoids the data[0] check that would misclassify hardware frames whose
        // surface lives in data[3]) or a degenerate frame bypasses GPU work.
        let bypass = crate::util::ffmpeg_utils::frame_is_eof_marker(&frame)
            || unsafe {
                // SAFETY: not a marker, so the frame is non-null with live buffers.
                (*frame.as_ptr()).width <= 0 || (*frame.as_ptr()).height <= 0
            };
        if bypass {
            // Props-only frames (e.g. the EOF timestamp marker decoders send
            // through pipelines) pass through untouched. They are queued so
            // they leave in arrival order behind any in-flight GPU frames.
            //
            // A marker also announces that nothing will be submitted behind
            // it, so resolve every in-flight readback NOW: after the source
            // disconnects, the pipeline loop (`run_pipeline`) exits on its
            // first request_frame sweep that produces nothing, and `uninit`
            // would then discard whatever is still pending. A non-blocking
            // sweep cannot be relied on to pick these up in time — the GPU
            // may need far longer than the loop's exit window.
            while self.gpu_pending_count() > 0 {
                self.complete_oldest_gpu(true)?;
            }
            self.pending.push_back(PendingOutput::Done(frame));
            return self.next_output(false);
        }

        // SAFETY: `frame` is a live frame; scalar field reads only.
        let raw = unsafe { frame.as_ptr() };

        // Hardware frames (hwaccel_output_format vaapi/cuda/...): try the
        // zero-copy dmabuf import when enabled, otherwise download to system
        // memory. Their data pointers are GPU handles that must never be
        // read as CPU planes.
        let mut hw_mapped: Option<HwMappedFrame> = None;
        let frame = if is_hw_format(unsafe { (*raw).format }) {
            if self.hw_zero_copy_input {
                let gpu = self.gpu.as_ref().ok_or("WgpuFrameFilter not initialized")?;
                match frame_io::import_hw_frame(gpu, &frame) {
                    Ok(mapped) => hw_mapped = Some(mapped),
                    Err(reason) => hw_interop::warn_import_failed_once(&reason),
                }
            }
            if hw_mapped.is_some() {
                frame
            } else {
                let t_download = Instant::now();
                let sw = frame_io::download_hw_frame(&frame)
                    .map_err(|e| format!("WgpuFrameFilter: {e}"))?;
                if let Ok(mut stats) = self.stats.lock() {
                    stats.upload_secs += t_download.elapsed().as_secs_f64();
                }
                sw
            }
        } else {
            frame
        };

        // SAFETY: `frame` is a live frame; scalar field reads only.
        let raw = unsafe { frame.as_ptr() };
        let (layout, full_range) = match &hw_mapped {
            // Imported hardware frames are NV12 by construction; hw frames
            // carry the same color_range field as software ones.
            Some(_) => (
                PlaneLayout::Nv12,
                unsafe { (*raw).color_range } == ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_JPEG,
            ),
            None => {
                let pix_fmt = ensure_software_format(unsafe { (*raw).format })
                    .map_err(|e| format!("WgpuFrameFilter: {e}"))?;
                frame_io::detect_format(pix_fmt, unsafe { (*raw).color_range })?
            }
        };
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
            )
            .into());
        }
        gpu.ensure_resources(in_w, in_h, layout, self.output_size, self.frames_in_flight);

        // Return zero-copy staging buffers that downstream frame owners have
        // released since the last call.
        self.drain_recycled();

        if self.zero_copy_readback {
            // Zero-copy: buffers come back only when downstream drops its
            // frame, so pool emptiness cannot regulate (waiting on it could
            // deadlock against a slow consumer). Cap the submitted GPU work
            // instead and allocate staging on demand below.
            while self.gpu_pending_count() >= self.frames_in_flight {
                self.complete_oldest_gpu(true)?;
            }
        } else {
            // Cap in-flight work: when the staging pool is exhausted, block
            // for the oldest result (it keeps its position in the output
            // queue and returns its buffer to the pool).
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
                            .into(),
                    );
                }
                self.complete_oldest_gpu(true)?;
            }
        }

        let (mut staging, geo) = {
            let gpu = self.gpu.as_mut().expect("initialized above");
            let res = gpu.resources.as_mut().expect("resources ensured above");
            let geo = res.geometry();
            let staging = match res.staging_pool.pop() {
                Some(slot) => slot,
                None => StagingSlot::new(
                    create_staging(&gpu.device, geo.buf_size, gpu.direct_pack),
                    geo.buf_size,
                ),
            };
            (staging, geo)
        };

        let t_upload = Instant::now();
        let gpu = self.gpu.as_ref().expect("initialized above");
        let submitted = match &hw_mapped {
            Some(mapped) => {
                // Imported planes bind straight into the NV12 convert pass;
                // no CPU-side upload happens on this path.
                let bind = gpu.hw_convert_bind(&mapped.imported.tex_y, &mapped.imported.tex_uv);
                frame_io::encode_and_submit(
                    gpu,
                    gpu.convert_pipeline(PlaneLayout::Nv12),
                    &bind,
                    &frame,
                    matrix_id,
                    full_range,
                    &mut staging,
                    &self.params,
                )
            }
            None => frame_io::upload_and_encode(
                gpu,
                &frame,
                layout,
                matrix_id,
                full_range,
                &mut staging,
                &self.params,
            ),
        };
        let submission = match submitted {
            Ok(v) => v,
            Err(e) => {
                // Keep the pool consistent even though the job is failing.
                if let Some(res) = self.gpu.as_mut().and_then(|g| g.resources.as_mut()) {
                    res.staging_pool.push(staging);
                }
                return Err(e.into());
            }
        };
        if let Ok(mut stats) = self.stats.lock() {
            stats.upload_secs += t_upload.elapsed().as_secs_f64();
        }

        self.pending.push_back(PendingOutput::Gpu(InFlightFrame {
            staging,
            submission,
            submitted_at: Instant::now(),
            src_props: frame,
            _hw: hw_mapped,
            geo,
        }));

        // Synchronous mode returns its own frame; async mode returns whatever
        // is ready without blocking (delayed results flow via request_frame).
        let block = self.frames_in_flight == 1;
        self.next_output(block)
    }

    fn request_frame(
        &mut self,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        self.next_output(false)
    }

    fn uninit(&mut self, _ctx: &mut FrameFilterContext) {
        // Abandon any in-flight readbacks; wgpu unmaps and frees buffers on
        // drop, and pass-through frames are simply released. Zero-copy
        // frames still alive downstream keep their staging buffer through
        // the AVBuffer free callback; once the filter itself is dropped the
        // recycle receiver goes with it and returned buffers just drop.
        self.pending.clear();
        self.drain_recycled();
        self.gpu = None;
    }
}
