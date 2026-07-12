//! Soft focus / privacy blur: a single-pass 13-tap disc blur.

use super::{effect_module, EffectBuilder};

/// Parameters for [`soft_blur`]. `Default` is a soft-focus look; raise
/// both values for privacy blurring.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SoftBlurParams {
    /// Blur radius in input pixels, `0.0..=32.0`. The 13 taps spread over
    /// this distance, so very large radii trade smoothness for ringing;
    /// for stronger obscuring also reduce `output_size`.
    pub radius: f32,
    /// Mix between the original (`0.0`) and the blurred (`1.0`) image.
    pub strength: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad0: f32,
    #[doc(hidden)]
    pub _pad1: f32,
}

impl Default for SoftBlurParams {
    fn default() -> Self {
        Self {
            radius: 4.0,
            strength: 0.6,
            _pad0: 0.0,
            _pad1: 0.0,
        }
    }
}

impl SoftBlurParams {
    /// A strong preset for privacy blurring (license plates, bystanders).
    pub fn privacy() -> Self {
        Self {
            radius: 12.0,
            strength: 1.0,
            ..Self::default()
        }
    }
}

pub(super) const SOFT_BLUR_BODY: &str = r#"
struct SoftBlurParams {
    radius: f32,
    strength: f32,
    _pad0: f32,
    _pad1: f32,
};
@group(1) @binding(0) var<uniform> p: SoftBlurParams;

// Center + inner ring (6 taps, half radius) + outer ring (6 taps),
// rotated against each other to avoid hexagonal banding.
const TAPS: u32 = 12u;
const OFFS: array<vec2<f32>, 12> = array<vec2<f32>, 12>(
    vec2<f32>(0.5, 0.0),
    vec2<f32>(0.25, 0.433),
    vec2<f32>(-0.25, 0.433),
    vec2<f32>(-0.5, 0.0),
    vec2<f32>(-0.25, -0.433),
    vec2<f32>(0.25, -0.433),
    vec2<f32>(0.866, 0.5),
    vec2<f32>(0.0, 1.0),
    vec2<f32>(-0.866, 0.5),
    vec2<f32>(-0.866, -0.5),
    vec2<f32>(0.0, -1.0),
    vec2<f32>(0.866, -0.5),
);

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let step = clamp(p.radius, 0.0, 32.0) / vec2<f32>(textureDimensions(texture1));
    let center = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;

    // Center weight 2, inner ring 1.25, outer ring 0.75.
    var sum = center * 2.0;
    var wsum = 2.0;
    for (var i = 0u; i < TAPS; i = i + 1u) {
        let w = select(0.75, 1.25, i < 6u);
        sum += textureSampleLevel(texture1, sampler1, tex_coord + OFFS[i] * step, 0.0).rgb * w;
        wsum += w;
    }
    let blurred = sum / wsum;

    let c = mix(center, blurred, clamp(p.strength, 0.0, 1.0));
    return vec4<f32>(c, 1.0);
}
"#;

/// Soft focus / privacy blur. See [`SoftBlurParams`].
pub fn soft_blur(params: SoftBlurParams) -> EffectBuilder<SoftBlurParams> {
    EffectBuilder::new(effect_module(SOFT_BLUR_BODY), params)
}
