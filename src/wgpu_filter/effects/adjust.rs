//! Color adjustment: brightness, contrast, saturation, exposure, gamma,
//! vibrance and white balance in one pass.

use super::{effect_module, EffectBuilder};

/// Parameters for [`adjust`]. `Default` is neutral — the effect passes
/// frames through visually unchanged.
///
/// All controls operate on gamma-encoded RGB (like FFmpeg's `eq` filter)
/// and are clamped to their documented range in the shader.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct AdjustParams {
    /// Additive lift, `-1.0..=1.0`; `0.0` is neutral.
    pub brightness: f32,
    /// Multiplier around mid-gray, `0.0..=3.0`; `1.0` is neutral.
    pub contrast: f32,
    /// `0.0` (grayscale) `..=3.0`; `1.0` is neutral.
    pub saturation: f32,
    /// Exposure in stops (post-decode, gamma space), `-4.0..=4.0`;
    /// `0.0` is neutral.
    pub exposure_ev: f32,
    /// Output gamma, `0.2..=5.0`; `1.0` is neutral. Values above `1.0`
    /// brighten mids (applied as `pow(c, 1.0 / gamma)`).
    pub gamma: f32,
    /// Saturation boost weighted toward muted colors, `-1.0..=1.0`;
    /// `0.0` is neutral. Skin tones shift less than with `saturation`.
    pub vibrance: f32,
    /// Warm/cool shift, `-1.0` (blue) `..=1.0` (orange); `0.0` is neutral.
    pub temperature: f32,
    /// Green/magenta shift, `-1.0` (green) `..=1.0` (magenta); `0.0` is
    /// neutral.
    pub tint: f32,
}

impl Default for AdjustParams {
    fn default() -> Self {
        Self {
            brightness: 0.0,
            contrast: 1.0,
            saturation: 1.0,
            exposure_ev: 0.0,
            gamma: 1.0,
            vibrance: 0.0,
            temperature: 0.0,
            tint: 0.0,
        }
    }
}

pub(super) const ADJUST_BODY: &str = r#"
struct AdjustParams {
    brightness: f32,
    contrast: f32,
    saturation: f32,
    exposure_ev: f32,
    gamma: f32,
    vibrance: f32,
    temperature: f32,
    tint: f32,
};
@group(1) @binding(0) var<uniform> p: AdjustParams;

const LUMA_W: vec3<f32> = vec3<f32>(0.2126, 0.7152, 0.0722);

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    var c = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;

    // Exposure, then white balance as opposing channel gains.
    c = c * exp2(clamp(p.exposure_ev, -4.0, 4.0));
    let temp = clamp(p.temperature, -1.0, 1.0) * 0.1;
    let tint = clamp(p.tint, -1.0, 1.0) * 0.1;
    c = c * vec3<f32>(1.0 + temp, 1.0 - tint, 1.0 - temp);

    // Tone: lift, contrast around mid-gray, then gamma.
    c = c + vec3<f32>(clamp(p.brightness, -1.0, 1.0));
    c = (c - vec3<f32>(0.5)) * clamp(p.contrast, 0.0, 3.0) + vec3<f32>(0.5);
    c = clamp(c, vec3<f32>(0.0), vec3<f32>(1.0));
    c = pow(c, vec3<f32>(1.0 / clamp(p.gamma, 0.2, 5.0)));

    // Color: saturation, then vibrance (stronger where saturation is low).
    let gray = vec3<f32>(dot(c, LUMA_W));
    c = mix(gray, c, clamp(p.saturation, 0.0, 3.0));
    let sat = max(max(c.r, c.g), c.b) - min(min(c.r, c.g), c.b);
    let vib = clamp(p.vibrance, -1.0, 1.0) * (1.0 - clamp(sat, 0.0, 1.0));
    c = mix(vec3<f32>(dot(c, LUMA_W)), c, 1.0 + vib);

    return vec4<f32>(clamp(c, vec3<f32>(0.0), vec3<f32>(1.0)), 1.0);
}
"#;

/// Color adjustment effect. See [`AdjustParams`] for the controls.
///
/// ```rust,ignore
/// let effect = effects::adjust(AdjustParams {
///     contrast: 1.1,
///     vibrance: 0.3,
///     ..AdjustParams::default()
/// })
/// .build()?;
/// ```
pub fn adjust(params: AdjustParams) -> EffectBuilder<AdjustParams> {
    EffectBuilder::new(effect_module(ADJUST_BODY), params)
}
