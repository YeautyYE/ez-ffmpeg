//! Magnifier: a circular lens enlarges the image under it, blending
//! smoothly from full magnification at the center to none at the rim.

use super::{effect_module, EffectBuilder};

/// Parameters for [`magnifier`]. `Default` is a lens in the frame center;
/// move it live via the params handle to sweep it around.
///
/// Inside `radius` the sampling coordinates contract toward the lens
/// center by a factor that eases (smoothstep) from `magnification` at
/// the center to 1 at the rim — a convex lens without a hard edge.
/// Outside the radius the image is untouched.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct MagnifierParams {
    /// Lens center x as a fraction of the width, `0.0..=1.0`.
    pub center_x: f32,
    /// Lens center y as a fraction of the height, `0.0..=1.0`.
    pub center_y: f32,
    /// Lens radius as a fraction of the frame height, `0.0..=1.0`.
    /// The lens is physically round regardless of aspect.
    pub radius: f32,
    /// Peak zoom at the lens center, `1.0..=5.0`.
    pub magnification: f32,
}

impl Default for MagnifierParams {
    fn default() -> Self {
        Self {
            center_x: 0.5,
            center_y: 0.5,
            radius: 0.25,
            magnification: 1.8,
        }
    }
}

pub(super) const MAGNIFIER_BODY: &str = r#"
struct MagnifierParams {
    center_x: f32,
    center_y: f32,
    radius: f32,
    magnification: f32,
};
@group(1) @binding(0) var<uniform> p: MagnifierParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let center = vec2<f32>(
        clamp(p.center_x, 0.0, 1.0),
        clamp(p.center_y, 0.0, 1.0),
    );
    let radius = clamp(p.radius, 0.0, 1.0);
    let magnification = clamp(p.magnification, 1.0, 5.0);

    // Measure the lens circle in fractions of the frame height so it is
    // physically round at any aspect ratio.
    let aspect = ez.width / max(ez.height, 1.0);
    var d = tex_coord - center;
    d.x *= aspect;
    let r = length(d);

    var c = tex_coord;
    if (r < radius && radius > 0.0) {
        // Ease the zoom from `magnification` at the center to 1 at the
        // rim; dividing the offset contracts sampling toward the center.
        let zoom = mix(magnification, 1.0, smoothstep(0.0, 1.0, r / radius));
        c = center + (tex_coord - center) / zoom;
    }

    let s = textureSampleLevel(texture1, sampler1, c, 0.0).rgb;
    return vec4<f32>(s, 1.0);
}
"#;

/// Circular magnifying lens. See [`MagnifierParams`].
pub fn magnifier(params: MagnifierParams) -> EffectBuilder<MagnifierParams> {
    EffectBuilder::new(effect_module(MAGNIFIER_BODY), params)
}
