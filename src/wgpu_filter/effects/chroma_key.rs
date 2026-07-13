//! Chroma key: green-screen extraction with spill suppression, composited
//! over a solid background color.
//!
//! The keying math follows the two reference real-time implementations —
//! FFmpeg's `chromakey` and OBS Studio's chroma key: the pixel and the key
//! color are both projected to the CbCr chroma plane (so lighting changes
//! on the screen barely move the key), the chroma distance is box-filtered
//! over a 3×3 neighborhood (against 4:2:0 chroma blocking and noise), and
//! the matte is FFmpeg's linear ramp `(distance - similarity) / blend`.
//! Spill suppression is FFmpeg `despill`'s clamp: the key's dominant
//! channel is pulled down toward the mean of the other two.
//!
//! The pipeline outputs alpha-less YUV, so the extracted foreground is
//! composited over `background` here instead of an alpha channel.

use super::{effect_module, EffectBuilder};

/// Parameters for [`chroma_key`]. `Default` keys a pure-green screen and
/// composites the foreground over black.
///
/// `similarity` and `blend` use FFmpeg `chromakey` semantics on FFmpeg's
/// scale (the same RGB→CbCr coefficients and `√(d²/2)` normalization, so
/// `1.0` spans the full CbCr plane diagonal): distance ≤ `similarity` is
/// background, the next `blend` of distance ramps linearly to foreground,
/// and `blend ≈ 0` degenerates to a hard threshold. One structural
/// difference: FFmpeg compares the source's RAW chroma plane codes
/// against a BT.601-converted key, while this shader decodes the pixel to
/// RGB first and projects both sides through the one BT.601 conversion.
/// The two agree (within quantization) only on full-range BT.601 input;
/// on any other range/matrix combination FFmpeg's raw-code comparison
/// carries a small built-in bias this implementation does not reproduce —
/// treat ported `ffmpeg -vf chromakey=...` values as a close starting
/// point, not bit-identical.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ChromaKeyParams {
    /// Key color red, `0.0..=1.0`.
    pub key_r: f32,
    /// Key color green, `0.0..=1.0`.
    pub key_g: f32,
    /// Key color blue, `0.0..=1.0`.
    pub key_b: f32,
    /// Chroma distance keyed as background, `0.00001..=1.0`. Larger keys
    /// more; neutral gray sits ≈ 0.38 from pure green on this scale.
    pub similarity: f32,
    /// Width of the soft edge past `similarity`, `0.0..=1.0`; `0.0` is a
    /// hard binary matte.
    pub blend: f32,
    /// Spill suppression strength, `0.0..=1.0`: how strongly the key's
    /// dominant channel is clamped toward the mean of the other two in
    /// surviving foreground (`1.0` = FFmpeg `despill` defaults).
    pub spill: f32,
    /// Background red, `0.0..=1.0`.
    pub bg_r: f32,
    /// Background green, `0.0..=1.0`.
    pub bg_g: f32,
    /// Background blue, `0.0..=1.0`.
    pub bg_b: f32,
    /// GPU uniform padding; keep `0.0` (public so field-update syntax
    /// works).
    #[doc(hidden)]
    pub _pad0: f32,
    #[doc(hidden)]
    pub _pad1: f32,
    #[doc(hidden)]
    pub _pad2: f32,
}

impl Default for ChromaKeyParams {
    fn default() -> Self {
        Self::green_screen()
    }
}

impl ChromaKeyParams {
    /// Keys a pure-green screen (`#00FF00`) over black.
    pub fn green_screen() -> Self {
        Self {
            key_r: 0.0,
            key_g: 1.0,
            key_b: 0.0,
            similarity: 0.3,
            blend: 0.06,
            spill: 1.0,
            bg_r: 0.0,
            bg_g: 0.0,
            bg_b: 0.0,
            _pad0: 0.0,
            _pad1: 0.0,
            _pad2: 0.0,
        }
    }

    /// Keys a pure-blue screen (`#0000FF`) over black.
    pub fn blue_screen() -> Self {
        Self {
            key_r: 0.0,
            key_g: 0.0,
            key_b: 1.0,
            ..Self::green_screen()
        }
    }

    /// Replaces the background color the foreground is composited over.
    pub fn with_background(mut self, r: f32, g: f32, b: f32) -> Self {
        self.bg_r = r;
        self.bg_g = g;
        self.bg_b = b;
        self
    }
}

pub(super) const CHROMA_KEY_BODY: &str = r#"
struct ChromaKeyParams {
    key_r: f32,
    key_g: f32,
    key_b: f32,
    similarity: f32,
    blend: f32,
    spill: f32,
    bg_r: f32,
    bg_g: f32,
    bg_b: f32,
    _pad0: f32,
    _pad1: f32,
    _pad2: f32,
};
@group(1) @binding(0) var<uniform> p: ChromaKeyParams;

// CbCr rows of FFmpeg `chromakey`'s RGB->UV conversion (libavfilter
// RGB_TO_U/RGB_TO_V, full-range BT.601), so distances land on FFmpeg's
// scale. The +0.5 offset cancels in the subtraction below. (FFmpeg
// compares the source's RAW chroma plane codes against this key
// conversion; here the already-decoded RGB pixel is re-projected instead,
// so the two match — within quantization — only on full-range BT.601
// input, and any other range/matrix carries a small bias on FFmpeg's
// side that this shader does not reproduce.)
fn cbcr(rgb: vec3<f32>) -> vec2<f32> {
    return vec2<f32>(
        dot(rgb, vec3<f32>(-0.16874, -0.33126, 0.5)) + 0.5,
        dot(rgb, vec3<f32>(0.5, -0.41869, -0.08131)) + 0.5,
    );
}

// Chroma-plane distance on FFmpeg's numeric scale: sqrt((dCb² + dCr²)/2),
// so 1.0 spans the full CbCr diagonal; imported `similarity` values are
// starting points subject to the range/matrix caveat above. Keying on
// chroma alone (not RGB) is what makes the matte survive shadows and
// uneven lighting on the physical screen.
fn chroma_dist(rgb: vec3<f32>, key: vec2<f32>) -> f32 {
    return length(cbcr(rgb) - key) * 0.70710678;
}

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let rgb = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;
    let key_rgb = clamp(
        vec3<f32>(p.key_r, p.key_g, p.key_b),
        vec3<f32>(0.0),
        vec3<f32>(1.0),
    );
    let key = cbcr(key_rgb);

    // 3x3 box filter over the chroma DISTANCE (not the color), the OBS
    // tap pattern: 4 bilinear taps at half-pixel offsets count double,
    // plus the center. Averaging the distance suppresses 4:2:0 chroma
    // blocking and noise in the matte edge.
    let texel = 1.0 / vec2<f32>(textureDimensions(texture1));
    let h = texel * 0.5;
    let p0 = vec2<f32>(texel.x, h.y);
    let p1 = vec2<f32>(h.x, -texel.y);
    var d = chroma_dist(textureSampleLevel(texture1, sampler1, tex_coord - p0, 0.0).rgb, key)
        + chroma_dist(textureSampleLevel(texture1, sampler1, tex_coord + p0, 0.0).rgb, key)
        + chroma_dist(textureSampleLevel(texture1, sampler1, tex_coord - p1, 0.0).rgb, key)
        + chroma_dist(textureSampleLevel(texture1, sampler1, tex_coord + p1, 0.0).rgb, key);
    d = (d * 2.0 + chroma_dist(rgb, key)) / 9.0;

    // FFmpeg's matte ramp; blend ~ 0 degenerates to a hard threshold.
    let similarity = clamp(p.similarity, 0.00001, 1.0);
    let blend = clamp(p.blend, 0.0, 1.0);
    var alpha: f32;
    if (blend > 0.0001) {
        alpha = clamp((d - similarity) / blend, 0.0, 1.0);
    } else {
        alpha = select(0.0, 1.0, d > similarity);
    }

    // FFmpeg `despill` clamp (mix = 0.5), aimed at the key's dominant
    // channel: self-gating, so it is safe on every foreground pixel.
    let spill = clamp(p.spill, 0.0, 1.0);
    var fg = rgb;
    if (key_rgb.g >= key_rgb.r && key_rgb.g >= key_rgb.b) {
        fg.g -= spill * max(rgb.g - 0.5 * (rgb.r + rgb.b), 0.0);
    } else if (key_rgb.b >= key_rgb.r) {
        fg.b -= spill * max(rgb.b - 0.5 * (rgb.r + rgb.g), 0.0);
    } else {
        fg.r -= spill * max(rgb.r - 0.5 * (rgb.g + rgb.b), 0.0);
    }

    let bg = clamp(
        vec3<f32>(p.bg_r, p.bg_g, p.bg_b),
        vec3<f32>(0.0),
        vec3<f32>(1.0),
    );
    return vec4<f32>(mix(bg, fg, alpha), 1.0);
}
"#;

/// Chroma key / green-screen extraction. See [`ChromaKeyParams`].
pub fn chroma_key(params: ChromaKeyParams) -> EffectBuilder<ChromaKeyParams> {
    EffectBuilder::new(effect_module(CHROMA_KEY_BODY), params)
}
