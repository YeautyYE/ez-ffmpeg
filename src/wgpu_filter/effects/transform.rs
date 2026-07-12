//! Geometric transform: mirror, flip, rotate, scale and translate in one
//! inverse-mapped sampling pass.

use super::{effect_module, EffectBuilder};

/// Parameters for [`transform`]. `Default` is the identity transform.
///
/// The output pixel at `(x, y)` is inverse-mapped through translate →
/// rotate → scale (all around the frame center), then mirrored; source
/// coordinates that fall outside the frame render black.
///
/// Rotation happens in *output* physical space, so it is aspect-correct
/// (circles stay circles) whenever the input and output aspect ratios
/// match — always true at the default output size. With a different
/// output aspect the pipeline's stretch-to-fill resize composes with the
/// rotation and distorts; for the common "rotate 90° into a swapped
/// frame" case the distortion cancels exactly by also setting
/// `scale_x = w as f32 / h as f32`, `scale_y = h as f32 / w as f32`
/// (`w`/`h` = the input size) alongside `output_size(h, w)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct TransformParams {
    /// Rotation around the center in radians, counterclockwise.
    pub rotate: f32,
    /// Horizontal zoom, `0.05..=20.0`; `1.0` is neutral.
    pub scale_x: f32,
    /// Vertical zoom, `0.05..=20.0`; `1.0` is neutral.
    pub scale_y: f32,
    /// Horizontal shift as a fraction of the width; positive moves the
    /// image right.
    pub translate_x: f32,
    /// Vertical shift as a fraction of the height; positive moves the
    /// image down.
    pub translate_y: f32,
    /// `1` mirrors horizontally (left-right).
    pub mirror_x: u32,
    /// `1` flips vertically (top-bottom).
    pub mirror_y: u32,
    /// GPU uniform padding; keep `0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad: u32,
}

impl Default for TransformParams {
    fn default() -> Self {
        Self {
            rotate: 0.0,
            scale_x: 1.0,
            scale_y: 1.0,
            translate_x: 0.0,
            translate_y: 0.0,
            mirror_x: 0,
            mirror_y: 0,
            _pad: 0,
        }
    }
}

impl TransformParams {
    /// Horizontal mirror (the webcam/selfie flip).
    pub fn mirrored() -> Self {
        Self {
            mirror_x: 1,
            ..Self::default()
        }
    }

    /// Uniform zoom around the center.
    pub fn zoomed(scale: f32) -> Self {
        Self {
            scale_x: scale,
            scale_y: scale,
            ..Self::default()
        }
    }
}

pub(super) const TRANSFORM_BODY: &str = r#"
struct TransformParams {
    rotate: f32,
    scale_x: f32,
    scale_y: f32,
    translate_x: f32,
    translate_y: f32,
    mirror_x: u32,
    mirror_y: u32,
    _pad: u32,
};
@group(1) @binding(0) var<uniform> p: TransformParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    var c = tex_coord - vec2<f32>(0.5);
    c -= vec2<f32>(p.translate_x, p.translate_y);

    // Rotate in square space (aspect-corrected) so circles stay circles.
    // Inverse mapping in y-down texture space: applying R(theta) to the
    // SAMPLING position shows the image rotated by R(-theta), which in a
    // y-down frame reads as counterclockwise to the viewer — matching the
    // documented positive direction.
    let aspect = ez.width / max(ez.height, 1.0);
    c.x *= aspect;
    let cs = cos(p.rotate);
    let sn = sin(p.rotate);
    c = vec2<f32>(c.x * cs - c.y * sn, c.x * sn + c.y * cs);
    c.x /= aspect;

    c /= vec2<f32>(
        clamp(p.scale_x, 0.05, 20.0),
        clamp(p.scale_y, 0.05, 20.0),
    );
    c += vec2<f32>(0.5);

    if (p.mirror_x != 0u) {
        c.x = 1.0 - c.x;
    }
    if (p.mirror_y != 0u) {
        c.y = 1.0 - c.y;
    }

    // Out-of-frame sources render black. Sample with an explicit level:
    // implicit-derivative sampling is invalid after non-uniform flow, and
    // the transformed coordinates make derivatives meaningless anyway.
    let inside = all(c >= vec2<f32>(0.0)) && all(c <= vec2<f32>(1.0));
    let s = textureSampleLevel(texture1, sampler1, clamp(c, vec2<f32>(0.0), vec2<f32>(1.0)), 0.0);
    return select(vec4<f32>(0.0, 0.0, 0.0, 1.0), vec4<f32>(s.rgb, 1.0), inside);
}
"#;

/// Geometric transform. See [`TransformParams`].
pub fn transform(params: TransformParams) -> EffectBuilder<TransformParams> {
    EffectBuilder::new(effect_module(TRANSFORM_BODY), params)
}
