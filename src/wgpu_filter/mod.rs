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
//! **Feature flag**: only available with the `wgpu` feature.
//!
//! **Input formats**: YUV420P, YUV422P, YUV444P (plus their full-range J
//! variants) and NV12 CPU frames. Hardware frames are rejected with
//! guidance; other formats need a `format=yuv420p` conversion in
//! `filter_desc` first. Output is always YUV420P (4:2:2/4:4:4 inputs are
//! chroma-downsampled on the GPU).

pub mod wgpu_frame_filter;

mod frame_io;
mod gpu_state;
mod params;
pub(crate) mod shaders;
#[cfg(test)]
mod tests;

pub use params::{WgpuFilterStats, WgpuParamsHandle};
pub use wgpu_frame_filter::{WgpuFrameFilter, WgpuFrameFilterBuilder};
