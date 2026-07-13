//! Swirl / tornado: rotates the image around a center, strongest at the
//! center and fading quadratically to nothing at the rim.

use super::{effect_module, EffectBuilder};

/// Parameters for [`swirl`]. `Default` is a static moderate vortex in the
/// frame center; [`SwirlParams::tornado`] animates it.
///
/// Pixels inside `radius` rotate around the center by
/// `twist·(1 - r/radius)²` radians — full twist at the center, zero at
/// the rim, so the effect blends seamlessly into the untouched outside.
/// With `speed > 0` the twist oscillates as `twist·sin(speed·t)`,
/// swinging the vortex back and forth through its mirror image.
///
/// The animation is driven by frame timestamps (`pts` × `time_base`);
/// frames without timestamps render the `t = 0` phase, which for an
/// animated swirl (`speed > 0`) is the untwisted identity.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SwirlParams {
    /// Vortex center x as a fraction of the width, `0.0..=1.0`.
    pub center_x: f32,
    /// Vortex center y as a fraction of the height, `0.0..=1.0`.
    pub center_y: f32,
    /// Vortex radius as a fraction of the frame height, `0.0..=2.0`.
    /// The falloff circle is physically round regardless of aspect.
    pub radius: f32,
    /// Peak rotation at the center in radians, `-12.0..=12.0`; positive
    /// twists counterclockwise on screen (the same convention as
    /// [`TransformParams::rotate`](super::TransformParams::rotate)).
    pub twist: f32,
    /// Oscillation speed in radians per second, `0.0..=50.0`; `0.0`
    /// holds a constant twist.
    pub speed: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad0: f32,
    #[doc(hidden)]
    pub _pad1: f32,
    #[doc(hidden)]
    pub _pad2: f32,
}

impl Default for SwirlParams {
    fn default() -> Self {
        Self {
            center_x: 0.5,
            center_y: 0.5,
            radius: 0.4,
            twist: 2.5,
            speed: 0.0,
            _pad0: 0.0,
            _pad1: 0.0,
            _pad2: 0.0,
        }
    }
}

impl SwirlParams {
    /// An animated vortex that winds up and unwinds, tornado-style.
    pub fn tornado() -> Self {
        Self {
            radius: 0.35,
            twist: 4.0,
            speed: 2.0,
            ..Self::default()
        }
    }
}

pub(super) const SWIRL_BODY: &str = r#"
struct SwirlParams {
    center_x: f32,
    center_y: f32,
    radius: f32,
    twist: f32,
    speed: f32,
    _pad0: f32,
    _pad1: f32,
    _pad2: f32,
};
@group(1) @binding(0) var<uniform> p: SwirlParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let center = vec2<f32>(
        clamp(p.center_x, 0.0, 1.0),
        clamp(p.center_y, 0.0, 1.0),
    );
    let radius = clamp(p.radius, 0.0, 2.0);

    var twist = clamp(p.twist, -12.0, 12.0);
    let speed = clamp(p.speed, 0.0, 50.0);
    if (speed > 0.0) {
        twist *= sin(max(ez.play_time, 0.0) * speed);
    }

    // Work in a space where distances are fractions of the frame height,
    // so the vortex circle is physically round at any aspect ratio.
    let aspect = ez.width / max(ez.height, 1.0);
    var c = tex_coord - center;
    c.x *= aspect;

    let r = length(c);
    if (r < radius && radius > 0.0) {
        let falloff = 1.0 - r / radius;
        // Inverse mapping, same matrix and convention as the transform
        // effect: rotating the SAMPLING position by +theta shows the
        // image rotated counterclockwise to the viewer.
        let theta = twist * falloff * falloff;
        let cs = cos(theta);
        let sn = sin(theta);
        c = vec2<f32>(c.x * cs - c.y * sn, c.x * sn + c.y * cs);
    }

    c.x /= aspect;
    let s = textureSampleLevel(
        texture1,
        sampler1,
        clamp(c + center, vec2<f32>(0.0), vec2<f32>(1.0)),
        0.0,
    ).rgb;
    return vec4<f32>(s, 1.0);
}
"#;

/// Swirl / tornado vortex. See [`SwirlParams`].
pub fn swirl(params: SwirlParams) -> EffectBuilder<SwirlParams> {
    EffectBuilder::new(effect_module(SWIRL_BODY), params)
}
