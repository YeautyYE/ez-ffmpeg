//! Camera sway: a slow translate/zoom oscillation that fakes handheld or
//! breathing motion over static footage.

use super::{effect_module, EffectBuilder};

/// Parameters for [`sway`]. `Default` is a gentle drift; use the presets
/// for the two classic looks.
///
/// The image oscillates as `x = x_amplitude·sin(speed·t)`,
/// `y = y_amplitude·cos(speed·t)` (a circular sweep when both are set)
/// while the scale pulses by `zoom_amplitude`. A constant overscan zoom
/// is derived from the amplitudes so the wander never samples outside
/// the frame — no black borders at any phase.
///
/// The animation is driven by frame timestamps (`pts` × `time_base`);
/// frames without timestamps render the `t = 0` phase.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SwayParams {
    /// Horizontal travel as a fraction of the width, `0.0..=0.05`.
    pub x_amplitude: f32,
    /// Vertical travel as a fraction of the height, `0.0..=0.05`.
    pub y_amplitude: f32,
    /// Zoom pulse amplitude, `0.0..=0.2`; the scale swings between
    /// `1-zoom_amplitude` and `1+zoom_amplitude` around the overscan.
    pub zoom_amplitude: f32,
    /// Oscillation speed in radians per second, `0.0..=50.0`; one full
    /// sway takes `2π/speed` seconds.
    pub speed: f32,
}

impl Default for SwayParams {
    fn default() -> Self {
        Self {
            x_amplitude: 0.004,
            y_amplitude: 0.003,
            zoom_amplitude: 0.01,
            speed: 1.2,
        }
    }
}

impl SwayParams {
    /// Slow breathing zoom, no translation.
    pub fn breathe() -> Self {
        Self {
            x_amplitude: 0.0,
            y_amplitude: 0.0,
            zoom_amplitude: 0.02,
            speed: 1.0,
        }
    }

    /// Quick small circular shake, like handheld footage.
    pub fn handheld() -> Self {
        Self {
            x_amplitude: 0.008,
            y_amplitude: 0.006,
            zoom_amplitude: 0.0,
            speed: 6.0,
        }
    }
}

pub(super) const SWAY_BODY: &str = r#"
struct SwayParams {
    x_amplitude: f32,
    y_amplitude: f32,
    zoom_amplitude: f32,
    speed: f32,
};
@group(1) @binding(0) var<uniform> p: SwayParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let xa = clamp(p.x_amplitude, 0.0, 0.05);
    let ya = clamp(p.y_amplitude, 0.0, 0.05);
    let za = clamp(p.zoom_amplitude, 0.0, 0.2);
    let t = max(ez.play_time, 0.0) * clamp(p.speed, 0.0, 50.0);

    // Overscan so the sampling window stays inside the frame at every
    // phase: after dividing by `scale` the half-window is 0.5/scale, and
    // the translate adds up to max(xa, ya); the window fits exactly when
    // the minimum scale is 1/(1 - 2·max). The zoom pulse divides the
    // scale by up to (1 - za), so that factor is compensated too.
    let overscan = 1.0 / ((1.0 - 2.0 * max(xa, ya)) * (1.0 - za));
    let scale = overscan * (1.0 + za * sin(t));

    var c = vec2<f32>(0.5) + (tex_coord - vec2<f32>(0.5)) / scale;
    c += vec2<f32>(xa * sin(t), ya * cos(t));

    let s = textureSampleLevel(texture1, sampler1, c, 0.0).rgb;
    return vec4<f32>(s, 1.0);
}
"#;

/// Camera sway / breathing zoom. See [`SwayParams`].
pub fn sway(params: SwayParams) -> EffectBuilder<SwayParams> {
    EffectBuilder::new(effect_module(SWAY_BODY), params)
}
