//! Built-in, named GPU effects with typed parameters.
//!
//! Each constructor returns a typed builder over a preloaded
//! [`WgpuFrameFilter`](crate::wgpu_filter::WgpuFrameFilter) shader:
//!
//! ```rust,ignore
//! use ez_ffmpeg::wgpu_filter::effects::{adjust, AdjustParams};
//!
//! let effect = adjust(AdjustParams {
//!     saturation: 1.2,
//!     temperature: 0.15,
//!     ..AdjustParams::default()
//! })
//! .build()?;
//! let params = effect.params_handle(); // typed, updatable while running
//! // pipeline.filter("adjust", Box::new(effect))
//! ```
//!
//! The catalog (all operate on gamma-encoded RGB, like FFmpeg's `eq`):
//! - [`adjust`] — brightness/contrast/saturation, exposure, gamma,
//!   vibrance, white balance,
//! - [`beauty`] / [`portrait`] — heuristic skin smoothing + whitening
//!   (`beauty_lite` grade, see the module docs there),
//! - [`sharpen`] — luma unsharp mask,
//! - [`transform`] — mirror/flip/rotate/scale/translate,
//! - [`pixelate`] — mosaic blocks,
//! - [`soft_blur`] — soft focus / privacy blur.
//!
//! Every params struct is `#[repr(C)]` + [`bytemuck::Pod`] with a mild
//! product preset as `Default`; out-of-range values are clamped in the
//! shader, never rejected. Live updates go through the handle returned by
//! [`Effect::params_handle`] — [`WgpuParamsHandle::set`] replaces the
//! value, [`WgpuParamsHandle::update`] read-modify-writes it atomically.
//!
//! # One effect per filter — avoid chaining instances
//!
//! Each [`Effect`] owns a full GPU pipeline: YUV upload, convert, effect,
//! pack, readback. Chaining two effects doubles all of that per frame.
//! Prefer one effect whose parameters cover the look ([`adjust`] bundles
//! eight controls; [`portrait`] fuses smoothing, whitening and
//! brightening) over stacking instances.

mod adjust;
mod beauty;
mod pixelate;
mod sharpen;
mod soft_blur;
mod transform;

#[cfg(test)]
mod tests;

pub use adjust::{adjust, AdjustParams};
pub use beauty::{beauty, portrait, BeautyBuilder, BeautyParams, BeautyQuality};
pub use pixelate::{pixelate, PixelateParams};
pub use sharpen::{sharpen, SharpenParams};
pub use soft_blur::{soft_blur, SoftBlurParams};
pub use transform::{transform, TransformParams};

use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::wgpu_filter::error::WgpuFilterError;
use crate::wgpu_filter::params::{WgpuFilterStats, WgpuParamsHandle};
use crate::wgpu_filter::wgpu_frame_filter::{WgpuFrameFilter, WgpuFrameFilterBuilder};
use ffmpeg_next::Frame;
use ffmpeg_sys_next::AVMediaType;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

/// Shared module prefix for the built-in effect shaders: the standard
/// `@group(0)` contract of [`WgpuFrameFilterBuilder::shader_wgsl`].
/// `textureDimensions(texture1)` is the INPUT size; `ez.width`/`ez.height`
/// are the OUTPUT size — neighborhood taps must step in input texels.
const EFFECT_HEADER: &str = r#"
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms {
    play_time: f32,
    width: f32,
    height: f32,
    _pad: f32,
};
@group(0) @binding(2) var<uniform> ez: EzUniforms;
"#;

/// Assembles a complete effect module: shared header, then the effect's
/// `@group(1)` params struct and `fs_main`. Plain concatenation — the
/// pieces are static, only the beauty shader varies by quality.
fn effect_module(params_and_body: &str) -> String {
    let mut out = String::with_capacity(EFFECT_HEADER.len() + params_and_body.len());
    out.push_str(EFFECT_HEADER);
    out.push_str(params_and_body);
    out
}

/// A built-in effect: a [`WgpuFrameFilter`] whose parameter type is fixed
/// at build time, so [`Self::params_handle`] needs no type argument and
/// cannot be mismatched against a same-sized foreign struct.
///
/// Implements [`FrameFilter`] by delegation — box it straight into a
/// pipeline: `pipeline.filter("adjust", Box::new(effect))`.
pub struct Effect<P: bytemuck::Pod> {
    filter: WgpuFrameFilter,
    _params: PhantomData<P>,
}

impl<P: bytemuck::Pod> Effect<P> {
    /// Typed handle for live parameter updates from any thread.
    pub fn params_handle(&self) -> WgpuParamsHandle<P> {
        self.filter
            .params_handle::<P>()
            .expect("effect params type is bound at build time")
    }

    /// Shared handle to cumulative per-stage timing statistics.
    pub fn stats_handle(&self) -> Arc<Mutex<WgpuFilterStats>> {
        self.filter.stats_handle()
    }

    /// Unwraps the underlying [`WgpuFrameFilter`], giving up the typed
    /// params binding.
    pub fn into_inner(self) -> WgpuFrameFilter {
        self.filter
    }
}

impl<P: bytemuck::Pod + Send> FrameFilter for Effect<P> {
    fn media_type(&self) -> AVMediaType {
        self.filter.media_type()
    }

    fn init(&mut self, ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        self.filter.init(ctx)
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        self.filter.filter_frame(frame, ctx)
    }

    fn request_frame(
        &mut self,
        ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        self.filter.request_frame(ctx)
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        self.filter.request_frame_mode()
    }

    fn uninit(&mut self, ctx: &mut FrameFilterContext) {
        self.filter.uninit(ctx)
    }
}

/// Builder shared by every effect: pipeline knobs pass through to
/// [`WgpuFrameFilterBuilder`], the shader and params type stay fixed.
pub struct EffectBuilder<P: bytemuck::Pod> {
    inner: WgpuFrameFilterBuilder,
    params: P,
}

impl<P: bytemuck::Pod> EffectBuilder<P> {
    fn new(shader: String, params: P) -> Self {
        Self {
            inner: WgpuFrameFilter::builder().shader_wgsl(shader),
            params,
        }
    }

    /// Replaces the initial parameter value.
    pub fn params(mut self, params: P) -> Self {
        self.params = params;
        self
    }

    /// Output frame size; defaults to the input size. See
    /// [`WgpuFrameFilterBuilder::output_size`].
    pub fn output_size(mut self, width: u32, height: u32) -> Self {
        self.inner = self.inner.output_size(width, height);
        self
    }

    /// GPU/CPU overlap depth (1..=4). See
    /// [`WgpuFrameFilterBuilder::frames_in_flight`].
    pub fn frames_in_flight(mut self, count: usize) -> Self {
        self.inner = self.inner.frames_in_flight(count);
        self
    }

    /// Experimental zero-copy readback. See
    /// [`WgpuFrameFilterBuilder::zero_copy_readback`].
    pub fn zero_copy_readback(mut self, enabled: bool) -> Self {
        self.inner = self.inner.zero_copy_readback(enabled);
        self
    }

    /// Experimental dmabuf import of hardware frames. See
    /// [`WgpuFrameFilterBuilder::hw_zero_copy_input`].
    pub fn hw_zero_copy_input(mut self, enabled: bool) -> Self {
        self.inner = self.inner.hw_zero_copy_input(enabled);
        self
    }

    pub fn build(self) -> Result<Effect<P>, WgpuFilterError> {
        let filter = self.inner.params(self.params).build()?;
        Ok(Effect {
            filter,
            _params: PhantomData,
        })
    }
}
