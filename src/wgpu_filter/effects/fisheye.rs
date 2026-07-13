//! Fisheye: whole-frame barrel distortion — the center bulges toward the
//! viewer while the borders compress, like an ultra-wide lens.

use super::{effect_module, EffectBuilder};

/// Parameters for [`fisheye`]. `Default` is a moderate fisheye.
///
/// The radial mapping is `r_src = r·(1 + strength·r²)/(1 + strength)`
/// with `r` normalized so the frame corners sit at 1 — corners map
/// exactly onto corners, and every interior point samples at a smaller
/// radius along its own ray, so the frame stays fully covered with no
/// black borders at any strength. `strength = 0` is the identity.
/// (A negative "pincushion" strength is NOT offered: it would need to
/// sample beyond the frame edge everywhere but the corners.)
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct FisheyeParams {
    /// Barrel amount, `0.0..=3.0`: larger bulges the center out more,
    /// `0.0` is neutral.
    pub strength: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad0: f32,
    #[doc(hidden)]
    pub _pad1: f32,
    #[doc(hidden)]
    pub _pad2: f32,
}

impl Default for FisheyeParams {
    fn default() -> Self {
        Self {
            strength: 0.6,
            _pad0: 0.0,
            _pad1: 0.0,
            _pad2: 0.0,
        }
    }
}

impl FisheyeParams {
    /// A fisheye with the given strength.
    pub fn with_strength(strength: f32) -> Self {
        Self {
            strength,
            ..Self::default()
        }
    }
}

pub(super) const FISHEYE_BODY: &str = r#"
struct FisheyeParams {
    strength: f32,
    _pad0: f32,
    _pad1: f32,
    _pad2: f32,
};
@group(1) @binding(0) var<uniform> p: FisheyeParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    // Barrel only: a negative strength would sample past the frame edge
    // everywhere but the corners (and folds the mapping below -1/3).
    let strength = clamp(p.strength, 0.0, 3.0);

    // Work in aspect-corrected space, normalized so the frame corners
    // are at distance 1 — that pins the corners in place below.
    let aspect = ez.width / max(ez.height, 1.0);
    let half_diag = length(vec2<f32>(aspect, 1.0)) * 0.5;
    var c = tex_coord - vec2<f32>(0.5);
    c.x *= aspect;
    c /= half_diag;

    let r = length(c);
    // r_src/r as a function of r²: 1 at the corners (r = 1), 1/(1+s) at
    // the center — sampling contracts there, so the center magnifies.
    // For s >= 0 the factor is <= 1 everywhere, so the source point stays
    // on the segment from the center to the pixel — always in frame.
    let factor = (1.0 + strength * r * r) / (1.0 + strength);
    c *= factor;

    c *= half_diag;
    c.x /= aspect;
    let s = textureSampleLevel(
        texture1,
        sampler1,
        clamp(c + vec2<f32>(0.5), vec2<f32>(0.0), vec2<f32>(1.0)),
        0.0,
    ).rgb;
    return vec4<f32>(s, 1.0);
}
"#;

/// Fisheye / barrel distortion. See [`FisheyeParams`].
pub fn fisheye(params: FisheyeParams) -> EffectBuilder<FisheyeParams> {
    EffectBuilder::new(effect_module(FISHEYE_BODY), params)
}
