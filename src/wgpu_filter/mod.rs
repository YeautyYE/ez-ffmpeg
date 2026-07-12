//! GPU-accelerated frame filtering backed by [wgpu](https://wgpu.rs)
//! (Vulkan/Metal/DX12/GL). Successor to the deprecated `opengl` module.
//!
//! Compared to the OpenGL filter, this module:
//! - runs headless (no X11/Wayland/display connection required),
//! - converts YUV<->RGB on the GPU with the correct color matrix
//!   (BT.601/BT.709, limited/full range) instead of `sws_scale` on the CPU,
//! - supports output resizing, live parameter updates, and per-stage timing
//!   statistics,
//! - overlaps GPU work with CPU work by default (up to two frames in
//!   flight; see `WgpuFrameFilterBuilder::frames_in_flight`) while always
//!   preserving output order,
//! - is `Send` without unsafe: all wgpu handles are thread-safe.
//!
//! # Example
//!
//! ```rust,ignore
//! use ez_ffmpeg::wgpu_filter::WgpuFrameFilter;
//! use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
//! use ez_ffmpeg::AVMediaType;
//!
//! let shader = r#"
//!     @group(0) @binding(0) var texture1: texture_2d<f32>;
//!     @group(0) @binding(1) var sampler1: sampler;
//!     struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
//!     @group(0) @binding(2) var<uniform> ez: EzUniforms;
//!
//!     @fragment
//!     fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
//!         let color = textureSample(texture1, sampler1, tex_coord);
//!         let gray = dot(color.rgb, vec3<f32>(0.299, 0.587, 0.114));
//!         return vec4<f32>(vec3<f32>(gray), 1.0);
//!     }
//! "#;
//!
//! let filter = WgpuFrameFilter::new_simple(shader)?;
//! let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
//! let pipeline = pipeline.filter("wgpu", Box::new(filter));
//! // output.add_frame_pipeline(pipeline);
//! ```
//!
//! # YUV passthrough mode
//!
//! By default the effect shader sees RGBA: the pipeline converts YUV to
//! RGBA on the GPU, runs the effect, and packs back to YUV420P. For color
//! effects that are natural in YUV (tone curves, LUTs, luma sharpening),
//! [`WgpuFrameFilterBuilder::shader_yuv_wgsl`] runs the effect directly on
//! **raw YUV code values** instead — no convert pass, no RGBA intermediate,
//! no matrix/range math. The shader is a body defining
//! `fn ez_effect(coord: vec2<f32>) -> vec3<f32>` over a small prelude
//! (`ez_sample_yuv`, `ez_luma`, `ez_chroma`, `ez_full_range`, ...):
//!
//! ```rust,ignore
//! let filter = WgpuFrameFilter::builder()
//!     .shader_yuv_wgsl(
//!         r#"
//!         fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
//!             var yuv = ez_sample_yuv(coord);
//!             yuv.x = clamp(yuv.x * 1.1, 0.0, 1.0); // brighten luma only
//!             return yuv;
//!         }
//!         "#,
//!     )
//!     .build()?;
//! ```
//!
//! This is raw *code-value* processing, not lossless chroma: with
//! subsampled input the 4:2:0 output still resamples chroma (bilinear up,
//! box down), and resizing resamples every plane. What the mode guarantees
//! is that no matrix or range math is applied anywhere — at unchanged
//! output size an untouched luma plane survives bit-for-bit, super-black/
//! super-white included.
//!
//! **Feature flag**: only available with the `wgpu` feature.
//!
//! **Input formats**: YUV420P, YUV422P, YUV444P (plus their full-range J
//! variants) and NV12 CPU frames. Hardware frames (e.g.
//! `set_hwaccel_output_format("vaapi")`) are downloaded to system memory
//! automatically, or imported zero-copy over DRM PRIME dmabufs when
//! `WgpuFrameFilterBuilder::hw_zero_copy_input` is enabled (Linux/Vulkan,
//! experimental). Other formats need a `format=yuv420p` conversion in
//! `filter_desc` first. Output is always YUV420P (4:2:2/4:4:4 inputs are
//! chroma-downsampled on the GPU), tagged with the effective input color
//! range (J-format input yields a full-range-tagged output).

pub mod effects;
pub mod wgpu_frame_filter;

mod error;
mod frame_io;
mod gpu_state;
mod hw_interop;
mod params;
pub(crate) mod shaders;
#[cfg(test)]
mod tests;

pub use error::WgpuFilterError;
pub use params::{WgpuFilterStats, WgpuParamsHandle};
pub use wgpu_frame_filter::{WgpuFrameFilter, WgpuFrameFilterBuilder};
