//! [`WgpuFrameFilter`]: the public GPU frame filter, its builder, and the
//! in-flight output queue that overlaps GPU work with CPU work.

use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError};
use crate::util::frame_utils::{ensure_software_format, is_hw_format};
use crate::wgpu_filter::error::WgpuFilterError;
use crate::wgpu_filter::frame_io::{self, HwMappedFrame, PlaneLayout};
use crate::wgpu_filter::gpu_state::{
    create_staging, EffectSource, GpuState, OutputGeometry, StagingSlot,
};
use crate::wgpu_filter::params::SharedParams;
use crate::wgpu_filter::shaders;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::AVMediaType;
use log::warn;
use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub use crate::wgpu_filter::params::{WgpuFilterStats, WgpuParamsHandle};

/// Upper bound for [`WgpuFrameFilterBuilder::frames_in_flight`].
const MAX_FRAMES_IN_FLIGHT: usize = 4;

/// Consecutive dmabuf import failures after which the per-frame import
/// attempt is skipped for the current decoder hwframes context. A failed
/// import is expensive (an export ioctl, a descriptor parse and a driver
/// query, all torn down again) while the download fallback produces
/// identical output, so after three consecutive failures — deterministic
/// or transient alike — the filter stops paying that cost for this
/// context: a streak of transient failures means sustained resource
/// distress, where per-frame doomed import attempts only add churn.
/// One- or two-frame blips self-heal (any success resets the count); a
/// new hwframes context (seek, resolution change) or a successful re-pin
/// after an unpinned stretch re-arms the attempt.
const HW_IMPORT_FAILURE_LATCH: u32 = 3;

/// Owned reference to the decoder hwframes context whose identity backs
/// [`WgpuFrameFilter::hw_frames_ctx_key`]. Holding the reference keeps the
/// allocation alive, so the allocator cannot hand the same address to a
/// different context while the key still points at it — without the pin,
/// a freed-and-reallocated context could silently inherit the previous
/// context's latched failure count.
struct HwFramesCtxPin(*mut ffmpeg_sys_next::AVBufferRef);

// SAFETY: `AVBufferRef` reference counts are atomic and `av_buffer_unref`
// is callable from any thread; the pin never dereferences the payload.
unsafe impl Send for HwFramesCtxPin {}

impl Drop for HwFramesCtxPin {
    fn drop(&mut self) {
        // SAFETY: `self.0` is null or a reference owned by this pin;
        // av_buffer_unref handles both and nulls the pointer.
        unsafe { ffmpeg_sys_next::av_buffer_unref(&mut self.0) };
    }
}

/// Which domain the user effect shader runs in.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EffectDomain {
    /// Convert to RGBA first; the shader is a complete fragment module.
    Rgba,
    /// Run directly on raw YUV code values; the shader is an `ez_effect`
    /// body wrapped by a library prelude.
    Yuv,
}

/// Builder for [`WgpuFrameFilter`].
pub struct WgpuFrameFilterBuilder {
    fragment_shader: String,
    effect_domain: EffectDomain,
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
        self.effect_domain = EffectDomain::Rgba;
        self
    }

    /// YUV passthrough mode: the effect runs directly on **raw YUV code
    /// values**, skipping the YUV->RGBA convert pass, the RGBA intermediate
    /// texture and the RGB->YUV math in the pack pass. For color effects
    /// that are natural in YUV (tone curves, LUTs, luma sharpening) this
    /// removes one input-sized render pass per frame. No matrix or range
    /// math is applied anywhere: at unchanged output size an untouched luma
    /// plane is reproduced bit-for-bit (super-black/super-white included),
    /// while subsampled chroma is still resampled — see the caveats below.
    ///
    /// `body` is not a complete module: it defines
    ///
    /// ```wgsl
    /// fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
    ///     return ez_sample_yuv(coord); // identity
    /// }
    /// ```
    ///
    /// and may call the library prelude (bindings themselves are private —
    /// the same body compiles against planar and NV12 inputs):
    /// - `ez_sample_yuv(coord) -> vec3<f32>` — (Y, U, V) code values in
    ///   0..1, sampled at a normalized coordinate,
    /// - `ez_luma(coord) -> f32`, `ez_chroma(coord) -> vec2<f32>`,
    /// - `ez_input_size() -> vec2<f32>`, `ez_chroma_size() -> vec2<f32>`,
    ///   `ez_output_size() -> vec2<f32>`,
    /// - `ez_play_time() -> f32`,
    /// - `ez_full_range() -> bool` — whether the code values are full range
    ///   (JPEG / J-format input) rather than limited (16..235 luma). The
    ///   library never range-converts in this mode; effects that assume
    ///   headroom must branch on this themselves.
    ///
    /// User parameters keep working: declare
    /// `@group(1) @binding(0) var<uniform> ...` in the body, with the group
    /// index spelled as the literal `1`. `@group(0)` is reserved for the
    /// prelude, and group indices are const-expressions (`0u`, `1-1` and
    /// named consts all name group 0) that a lexical check cannot evaluate —
    /// so `build()` rejects any `@group(...)` whose argument is not the
    /// literal `1` (comments stripped; an unused duplicate binding would
    /// otherwise silently alias the prelude's resources).
    ///
    /// Caveats:
    /// - This is *raw code-value* processing, not lossless chroma: with
    ///   subsampled input (4:2:0/4:2:2/NV12) chroma is still bilinearly
    ///   upsampled for sampling and box-downsampled by the 4:2:0 pack, so a
    ///   pure identity effect reproduces luma exactly but resamples chroma.
    /// - The body must not define `fs_main` (the library appends the entry
    ///   point) and gets no RGBA texture: `textureSample(texture1, ...)`
    ///   style shaders belong to [`Self::shader_wgsl`].
    pub fn shader_yuv_wgsl(mut self, body: impl Into<String>) -> Self {
        self.fragment_shader = body.into();
        self.effect_domain = EffectDomain::Yuv;
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
    /// After three consecutive import failures the filter stops attempting
    /// imports for the current decoder hwframes context (deterministic
    /// failures would otherwise burn a fallback round-trip on every frame);
    /// a new hwframes context re-arms the attempt. Software input frames are
    /// unaffected.
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

    pub fn build(self) -> Result<WgpuFrameFilter, WgpuFilterError> {
        let invalid = |msg: String| Err(WgpuFilterError::InvalidOption(msg));
        if self.fragment_shader.is_empty() {
            return invalid(
                "WgpuFrameFilter requires a fragment shader (shader_wgsl or shader_yuv_wgsl)"
                    .to_string(),
            );
        }
        if self.effect_domain == EffectDomain::Yuv {
            if !shaders::body_defines_ez_effect(&self.fragment_shader) {
                return invalid(
                    "shader_yuv_wgsl takes a shader body defining \
                     `fn ez_effect(coord: vec2<f32>) -> vec3<f32>`, not a complete fragment \
                     module; see the shader_yuv_wgsl documentation"
                        .to_string(),
                );
            }
            if shaders::body_declares_reserved_group(&self.fragment_shader) {
                return invalid(
                    "shader_yuv_wgsl bodies may only declare the params group, spelled \
                     literally `@group(1)` — @group(0) is reserved for the library prelude \
                     (an unused duplicate would silently alias its resources), and group \
                     indices are const-expressions (`0u`, `1-1`, a named const all name \
                     group 0), so any spelling other than the literal `1` is rejected"
                        .to_string(),
                );
            }
        }
        if let Some((w, h)) = self.output_size {
            if w == 0 || h == 0 {
                return invalid(format!("Invalid output size {w}x{h}"));
            }
        }
        // `is_multiple_of` would need Rust 1.87; keep `%` for the older MSRV.
        #[allow(clippy::manual_is_multiple_of)]
        if self.params_bytes.len() % 4 != 0 {
            return invalid(format!(
                "Params type size must be a multiple of 4 bytes (got {}); \
                 add explicit padding fields to the struct",
                self.params_bytes.len()
            ));
        }
        if !(1..=MAX_FRAMES_IN_FLIGHT).contains(&self.frames_in_flight) {
            return invalid(format!(
                "frames_in_flight must be within 1..={MAX_FRAMES_IN_FLIGHT} (got {})",
                self.frames_in_flight
            ));
        }
        Ok(WgpuFrameFilter {
            fragment_shader: self.fragment_shader,
            effect_domain: self.effect_domain,
            output_size: self.output_size,
            frames_in_flight: self.frames_in_flight,
            zero_copy_readback: self.zero_copy_readback,
            hw_zero_copy_input: self.hw_zero_copy_input,
            hw_import_failures: 0,
            hw_frames_ctx_key: 0,
            hw_frames_ctx_pin: None,
            warned_import_reasons: HashSet::new(),
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
    effect_domain: EffectDomain,
    output_size: Option<(u32, u32)>,
    frames_in_flight: usize,
    zero_copy_readback: bool,
    hw_zero_copy_input: bool,
    /// Consecutive dmabuf import failures for the current decoder hwframes
    /// context. At [`HW_IMPORT_FAILURE_LATCH`] the per-frame import attempt
    /// (an av_hwframe_map export + parse + driver query, all torn down
    /// again on failure) is skipped and frames go straight to the download
    /// fallback; a new hwframes context re-arms the attempt.
    hw_import_failures: u32,
    /// Identity of the decoder hwframes context the failure count belongs
    /// to (the AVHWFramesContext data pointer; 0 = none seen yet).
    hw_frames_ctx_key: usize,
    /// Keeps the context behind `hw_frames_ctx_key` alive so its address
    /// cannot be recycled for a different context (see [`HwFramesCtxPin`]).
    hw_frames_ctx_pin: Option<HwFramesCtxPin>,
    /// Import-failure reasons already warned about: each distinct reason
    /// logs once per filter instance (a process-wide latch would silence a
    /// second instance or a mid-stream reason change entirely).
    warned_import_reasons: HashSet<String>,
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
    /// Effective color range the passes ran with, stamped onto the output
    /// frame. `copy_props` alone would lose it for J-format input: the
    /// output is plain YUV420P, which no longer implies full range.
    full_range: bool,
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
            effect_domain: EffectDomain::Rgba,
            output_size: None,
            params_bytes: Vec::new(),
            frames_in_flight: 2,
            zero_copy_readback: false,
            hw_zero_copy_input: false,
        }
    }

    /// Creates a filter from a WGSL fragment shader with default settings,
    /// mirroring `OpenGLFrameFilter::new_simple`.
    pub fn new_simple(fragment_shader_wgsl: impl Into<String>) -> Result<Self, WgpuFilterError> {
        Self::builder().shader_wgsl(fragment_shader_wgsl).build()
    }

    /// Identity filter (pass-through through the full GPU pipeline); useful
    /// for measuring the pipeline's fixed cost.
    pub fn new_identity() -> Result<Self, WgpuFilterError> {
        Self::new_simple(shaders::IDENTITY_FS)
    }

    /// Typed handle for live parameter updates. `P` must match the type given
    /// to [`WgpuFrameFilterBuilder::params`].
    pub fn params_handle<P: bytemuck::Pod>(&self) -> Result<WgpuParamsHandle<P>, WgpuFilterError> {
        if std::mem::size_of::<P>() != self.params.len {
            return Err(WgpuFilterError::ParamsTypeMismatch(format!(
                "builder was given {} bytes, handle type has {} bytes",
                self.params.len,
                std::mem::size_of::<P>()
            )));
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
                while let Ok(mut slot) = self.recycle.1.try_recv() {
                    if slot.buf_size == res.buf_size && res.staging_pool.len() < cap {
                        // A slot lent out across a resource rebuild can come
                        // back with a bind group targeting the previous
                        // generation's out_view; drop it here so an idle pool
                        // slot cannot pin dead textures.
                        slot.clear_direct_pack_bind_if_stale(res.resource_generation);
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

        let Some(PendingOutput::Gpu(mut slot)) = self.pending.remove(pos) else {
            unreachable!("pos still points at the Gpu entry checked above");
        };
        let t_download = Instant::now();
        let mut frame = if self.zero_copy_readback {
            // Drop the cached direct-pack bind group only when the
            // resources were rebuilt under it: a stale bind lent out
            // inside the frame would pin the old `out_view` downstream,
            // while a current-generation one is reused on the slot's next
            // submission (no per-frame create_bind_group).
            match self.gpu.as_ref().and_then(|g| g.resources.as_ref()) {
                Some(res) => slot
                    .staging
                    .clear_direct_pack_bind_if_stale(res.resource_generation),
                None => slot.staging.clear_direct_pack_bind(),
            }
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
        // Stamp the effective range explicitly: the RGBA pack ran its range
        // math with this flag, and the YUV identity pack applied no matrix
        // or range conversion — either way the output is plain YUV420P, so
        // a J-format input's implicit full range would otherwise be lost.
        // SAFETY: `frame` was just built and is exclusively owned here.
        unsafe {
            (*frame.as_mut_ptr()).color_range = if slot.full_range {
                ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_JPEG
            } else {
                ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_MPEG
            };
        }
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
        let source = match self.effect_domain {
            EffectDomain::Rgba => EffectSource::Rgba(&self.fragment_shader),
            EffectDomain::Yuv => EffectSource::Yuv(&self.fragment_shader),
        };
        let gpu = GpuState::new(source, self.params.len, self.hw_zero_copy_input)?;
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
                // A failed import is not free: an av_hwframe_map export
                // (driver ioctls + dmabuf fds), a descriptor parse and a
                // possible driver query, all torn down again — on top of
                // the download the frame needs anyway. Latch the attempt
                // off after consecutive failures; a new decoder hwframes
                // context (seek, resolution change) re-arms it.
                // SAFETY: `frame` is a live frame; pointer field read only.
                let ctx = unsafe { (*raw).hw_frames_ctx };
                let ctx_key = if ctx.is_null() {
                    0
                } else {
                    // SAFETY: non-null `hw_frames_ctx` is a valid AVBufferRef.
                    unsafe { (*ctx).data as usize }
                };
                if ctx_key != self.hw_frames_ctx_key {
                    self.hw_frames_ctx_key = ctx_key;
                    self.hw_import_failures = 0;
                    self.hw_frames_ctx_pin = None;
                }
                if !ctx.is_null()
                    && self
                        .hw_frames_ctx_pin
                        .as_ref()
                        .is_none_or(|pin| pin.0.is_null())
                {
                    // SAFETY: `ctx` is the frame's live hwframes context;
                    // av_buffer_ref only bumps its refcount. A null return
                    // (OOM) leaves the pin inert, and this branch retries on
                    // the next frame, so the guard heals once memory does.
                    let pin = HwFramesCtxPin(unsafe { ffmpeg_sys_next::av_buffer_ref(ctx) });
                    if !pin.0.is_null() {
                        // Identity continuity is unprovable across an
                        // unpinned stretch (the address may have been
                        // recycled for a different context), so a successful
                        // (re-)pin conservatively re-arms the failure latch.
                        // Steady state hits this once per context, right
                        // after the key-change reset above (a no-op there).
                        self.hw_import_failures = 0;
                    }
                    self.hw_frames_ctx_pin = Some(pin);
                }
                if self.hw_import_failures < HW_IMPORT_FAILURE_LATCH {
                    let gpu = self.gpu.as_ref().ok_or("WgpuFrameFilter not initialized")?;
                    match frame_io::import_hw_frame(gpu, &frame) {
                        Ok(mapped) => {
                            self.hw_import_failures = 0;
                            hw_mapped = Some(mapped);
                        }
                        Err(reason) => {
                            self.hw_import_failures += 1;
                            // Each distinct reason logs once per instance.
                            if !self.warned_import_reasons.contains(&reason) {
                                warn!(
                                    "hw zero-copy input failed ({reason}); falling back to \
                                     downloading hardware frames to system memory"
                                );
                                self.warned_import_reasons.insert(reason);
                            }
                            if self.hw_import_failures == HW_IMPORT_FAILURE_LATCH {
                                warn!(
                                    "hw zero-copy input: {HW_IMPORT_FAILURE_LATCH} consecutive \
                                     import failures; skipping further attempts for this decoder \
                                     context (frames use the download fallback)"
                                );
                            }
                        }
                    }
                }
            }
            if hw_mapped.is_some() {
                if let Ok(mut stats) = self.stats.lock() {
                    stats.hw_import_frames += 1;
                }
                frame
            } else {
                let t_download = Instant::now();
                let sw = frame_io::download_hw_frame(&frame)
                    .map_err(|e| format!("WgpuFrameFilter: {e}"))?;
                // Capture before taking the stats lock, so reader contention
                // is not billed as download time.
                let download_secs = t_download.elapsed().as_secs_f64();
                if let Ok(mut stats) = self.stats.lock() {
                    stats.hw_download_frames += 1;
                    stats.hw_download_secs += download_secs;
                }
                sw
            }
        } else {
            // A software frame ends any hardware sequence: release the
            // context pin so a permanent HW->SW switch cannot retain the
            // decoder's surface pool until uninit, and re-arm the import
            // gate for a future hardware sequence.
            if self.hw_frames_ctx_key != 0 {
                self.hw_frames_ctx_key = 0;
                self.hw_import_failures = 0;
                self.hw_frames_ctx_pin = None;
            }
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
                // Imported planes bind straight into the NV12 input slot of
                // whichever pass consumes them (convert in RGBA mode, the
                // effect itself in YUV mode); no CPU-side upload happens.
                let bind = gpu.hw_nv12_bind(&mapped.imported.tex_y, &mapped.imported.tex_uv);
                frame_io::encode_and_submit(
                    gpu,
                    PlaneLayout::Nv12,
                    Some(&bind),
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
            full_range,
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
        self.hw_frames_ctx_pin = None;
    }
}
