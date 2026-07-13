//! Water-surface wave: a travelling sine ripple displaces the sampling
//! coordinates on both axes.

use super::{effect_module, EffectBuilder};

/// Parameters for [`wave`]. `Default` is a gentle ripple.
///
/// Each output coordinate is displaced by a sine of the *other* axis:
/// `x += amplitude·cos(y·frequency·2π + speed·t)` and then
/// `y += amplitude·sin(x·frequency·2π + speed·t)`, so vertical lines wave
/// horizontally and vice versa. `amplitude = 0` is the identity.
///
/// The animation is driven by frame timestamps (`pts` × `time_base`);
/// frames without timestamps render the `t = 0` phase (a static ripple).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct WaveParams {
    /// Displacement as a fraction of the frame, `0.0..=0.05`.
    pub amplitude: f32,
    /// Ripple count across the frame, `0.0..=20.0` full cycles.
    pub frequency: f32,
    /// Ripple travel speed in radians per second, `0.0..=50.0`.
    pub speed: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad: f32,
}

impl Default for WaveParams {
    fn default() -> Self {
        Self {
            amplitude: 0.008,
            frequency: 2.0,
            speed: 1.5,
            _pad: 0.0,
        }
    }
}

pub(super) const WAVE_BODY: &str = r#"
struct WaveParams {
    amplitude: f32,
    frequency: f32,
    speed: f32,
    _pad: f32,
};
@group(1) @binding(0) var<uniform> p: WaveParams;

const TAU: f32 = 6.28318530718;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let amp = clamp(p.amplitude, 0.0, 0.05);
    let freq = clamp(p.frequency, 0.0, 20.0) * TAU;
    let t = max(ez.play_time, 0.0) * clamp(p.speed, 0.0, 50.0);

    var c = tex_coord;
    c.x += cos(c.y * freq + t) * amp;
    c.y += sin(c.x * freq + t) * amp;

    // Displaced coordinates can leave the frame by up to `amp`;
    // clamping edge-extends there, like the effect this ports.
    let s = textureSampleLevel(
        texture1,
        sampler1,
        clamp(c, vec2<f32>(0.0), vec2<f32>(1.0)),
        0.0,
    ).rgb;
    return vec4<f32>(s, 1.0);
}
"#;

/// Travelling sine ripple. See [`WaveParams`].
pub fn wave(params: WaveParams) -> EffectBuilder<WaveParams> {
    EffectBuilder::new(effect_module(WAVE_BODY), params)
}
