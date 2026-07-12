//! Mosaic pixelation: replaces each block with the color at its center.

use super::{effect_module, EffectBuilder};

/// Parameters for [`pixelate`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PixelateParams {
    /// Square block size in input pixels, `1.0..=256.0`. `1.0` is a
    /// pass-through.
    pub block_size: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad0: f32,
    #[doc(hidden)]
    pub _pad1: f32,
    #[doc(hidden)]
    pub _pad2: f32,
}

impl PixelateParams {
    pub fn with_block_size(block_size: f32) -> Self {
        Self {
            block_size,
            ..Self::default()
        }
    }
}

impl Default for PixelateParams {
    fn default() -> Self {
        Self {
            block_size: 16.0,
            _pad0: 0.0,
            _pad1: 0.0,
            _pad2: 0.0,
        }
    }
}

pub(super) const PIXELATE_BODY: &str = r#"
struct PixelateParams {
    block_size: f32,
    _pad0: f32,
    _pad1: f32,
    _pad2: f32,
};
@group(1) @binding(0) var<uniform> p: PixelateParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let dims = vec2<f32>(textureDimensions(texture1));
    let bs = clamp(p.block_size, 1.0, 256.0);
    if (bs <= 1.0) {
        // True pass-through: keep normal bilinear sampling so combining
        // block_size=1 with output resizing still resamples smoothly.
        return vec4<f32>(textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb, 1.0);
    }
    // Clip the block to the frame and take the center of what remains: an
    // edge remnant samples the middle of its visible pixels (a nominal
    // center outside the frame would otherwise clamp to the border texel).
    let block_start = floor(tex_coord * dims / bs) * bs;
    let block_end = min(block_start + vec2<f32>(bs), dims);
    let center = (block_start + block_end) * 0.5;
    // Snap to the nearest texel center so the bilinear sampler fetches one
    // texel instead of blending neighbors.
    let texel = (floor(center) + 0.5) / dims;
    return vec4<f32>(textureSampleLevel(texture1, sampler1, texel, 0.0).rgb, 1.0);
}
"#;

/// Mosaic pixelation. See [`PixelateParams`].
pub fn pixelate(params: PixelateParams) -> EffectBuilder<PixelateParams> {
    EffectBuilder::new(effect_module(PIXELATE_BODY), params)
}
