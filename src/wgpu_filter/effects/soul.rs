//! Soul / astral-projection ghost: a scaled copy of the frame drifts
//! outward while fading, restarting every cycle.

use super::{effect_module, EffectBuilder};

/// Parameters for [`soul`]. `Default` is a clearly visible but not
/// overwhelming ghost.
///
/// Each cycle lasts `period` seconds: the ghost starts as an exact copy
/// (scale 1, opacity `max_alpha`), grows to `max_scale` and fades to
/// transparent, then the cycle restarts. At the exact cycle start the
/// ghost coincides with the image, so the output equals the input there.
///
/// The animation is driven by frame timestamps (`pts` × `time_base`);
/// frames without timestamps render the cycle-start (identity) phase.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SoulParams {
    /// Cycle length in seconds, `0.1..=10.0`.
    pub period: f32,
    /// Ghost opacity at cycle start, `0.0..=1.0`; it decays linearly to
    /// zero across the cycle.
    pub max_alpha: f32,
    /// Ghost scale reached at cycle end, `1.0..=2.0`.
    pub max_scale: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad: f32,
}

impl Default for SoulParams {
    fn default() -> Self {
        Self {
            period: 0.9,
            max_alpha: 0.35,
            max_scale: 1.25,
            _pad: 0.0,
        }
    }
}

pub(super) const SOUL_BODY: &str = r#"
struct SoulParams {
    period: f32,
    max_alpha: f32,
    max_scale: f32,
    _pad: f32,
};
@group(1) @binding(0) var<uniform> p: SoulParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let base = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;

    let period = clamp(p.period, 0.1, 10.0);
    let progress = fract(max(ez.play_time, 0.0) / period);
    let alpha = clamp(p.max_alpha, 0.0, 1.0) * (1.0 - progress);
    let scale = 1.0 + (clamp(p.max_scale, 1.0, 2.0) - 1.0) * progress;

    // Sample the ghost from a center-scaled coordinate; clamping to the
    // frame edge-extends the ghost where the enlarged copy runs past the
    // border, exactly like the effect this ports.
    let ghost_coord = clamp(
        vec2<f32>(0.5) + (tex_coord - vec2<f32>(0.5)) / scale,
        vec2<f32>(0.0),
        vec2<f32>(1.0),
    );
    let ghost = textureSampleLevel(texture1, sampler1, ghost_coord, 0.0).rgb;

    return vec4<f32>(mix(base, ghost, alpha), 1.0);
}
"#;

/// Soul / astral-projection ghost. See [`SoulParams`].
pub fn soul(params: SoulParams) -> EffectBuilder<SoulParams> {
    EffectBuilder::new(effect_module(SOUL_BODY), params)
}
