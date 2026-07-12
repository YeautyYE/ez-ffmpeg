//! Luma unsharp mask: sharpens perceived detail without amplifying
//! chroma fringing.

use super::{effect_module, EffectBuilder};

/// Parameters for [`sharpen`]. `Default` is a gentle, broadcast-safe
/// amount.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SharpenParams {
    /// High-frequency gain, `0.0..=3.0`; `0.0` disables the effect.
    pub amount: f32,
    /// Tap spacing in input pixels, `0.5..=4.0`. Larger values sharpen
    /// coarser structures.
    pub radius: f32,
    /// Luma contrast below which no sharpening is applied (noise gate),
    /// `0.0..=0.5`. The gate fades in smoothly to avoid banding.
    pub threshold: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works: `SharpenParams { amount: 1.0, ..Default::default() }`).
    #[doc(hidden)]
    pub _pad: f32,
}

impl SharpenParams {
    /// `amount` sets the strength; the other controls keep their defaults.
    pub fn with_amount(amount: f32) -> Self {
        Self {
            amount,
            ..Self::default()
        }
    }
}

impl Default for SharpenParams {
    fn default() -> Self {
        Self {
            amount: 0.6,
            radius: 1.0,
            threshold: 0.0,
            _pad: 0.0,
        }
    }
}

pub(super) const SHARPEN_BODY: &str = r#"
struct SharpenParams {
    amount: f32,
    radius: f32,
    threshold: f32,
    _pad: f32,
};
@group(1) @binding(0) var<uniform> p: SharpenParams;

const LUMA_W: vec3<f32> = vec3<f32>(0.2126, 0.7152, 0.0722);

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    // Neighborhood steps are in INPUT texels (ez.width/height is the
    // output size, which differs when resizing).
    let step = clamp(p.radius, 0.5, 4.0) / vec2<f32>(textureDimensions(texture1));
    let center = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;

    // Diagonal 4-tap box blur of luma around the center.
    var blur = 0.0;
    blur += dot(textureSampleLevel(texture1, sampler1, tex_coord + vec2<f32>(-1.0, -1.0) * step, 0.0).rgb, LUMA_W);
    blur += dot(textureSampleLevel(texture1, sampler1, tex_coord + vec2<f32>(1.0, -1.0) * step, 0.0).rgb, LUMA_W);
    blur += dot(textureSampleLevel(texture1, sampler1, tex_coord + vec2<f32>(-1.0, 1.0) * step, 0.0).rgb, LUMA_W);
    blur += dot(textureSampleLevel(texture1, sampler1, tex_coord + vec2<f32>(1.0, 1.0) * step, 0.0).rgb, LUMA_W);
    blur *= 0.25;

    let high = dot(center, LUMA_W) - blur;
    // Soft noise gate: fade the boost in above the threshold instead of
    // switching it on (a hard cut bands on gradients).
    let threshold = clamp(p.threshold, 0.0, 0.5);
    let gate = smoothstep(threshold, threshold * 2.0 + 0.005, abs(high));
    let boost = high * clamp(p.amount, 0.0, 3.0) * gate;

    return vec4<f32>(clamp(center + vec3<f32>(boost), vec3<f32>(0.0), vec3<f32>(1.0)), 1.0);
}
"#;

/// Luma unsharp-mask sharpening. See [`SharpenParams`].
pub fn sharpen(params: SharpenParams) -> EffectBuilder<SharpenParams> {
    EffectBuilder::new(effect_module(SHARPEN_BODY), params)
}
