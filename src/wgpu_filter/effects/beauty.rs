//! `beauty_lite`: heuristic skin smoothing, whitening and brightening.
//!
//! The "lite" is deliberate. Smoothing is an edge-preserving luma
//! bilateral gated by a *heuristic* skin mask (YCbCr skin cluster ×
//! saturation × edge confidence) — it reliably softens skin but can also
//! soften skin-toned wood or sand, and it cannot know where a face is.
//! Production-grade beautification needs face segmentation, which is out
//! of scope for a single-texture shader. Within those limits the defaults
//! are tuned to be safe: hair, eyes, brows and background detail keep
//! their edges.

use super::{effect_module, Effect, EffectBuilder};
use crate::wgpu_filter::error::WgpuFilterError;

/// Tap count of the smoothing kernel, chosen at build time.
///
/// A runtime tap count would still execute the full loop on every GPU
/// wavefront, so the kernel is baked into the shader instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BeautyQuality {
    /// 9 taps (center + 8). For integrated GPUs at 1080p60.
    Fast,
    /// 13 taps (center + 12). Smoother plateaus; discrete GPUs, or
    /// integrated GPUs at 720p.
    #[default]
    Balanced,
}

/// Parameters for [`beauty_lite`]. `Default` is a mild, streaming-safe
/// preset; [`portrait`] starts from a stronger one.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct BeautyParams {
    /// Skin smoothing strength, `0.0..=1.0`.
    pub smooth: f32,
    /// Skin whitening (highlight-compressed lift, skin-masked),
    /// `0.0..=1.0`.
    pub whiten: f32,
    /// Global gentle brightening, `0.0..=1.0`.
    pub brighten: f32,
    /// Fraction of removed high-frequency luma added back to avoid the
    /// "plastic skin" look, `0.0..=1.0`.
    pub detail_preserve: f32,
}

impl Default for BeautyParams {
    fn default() -> Self {
        Self {
            smooth: 0.5,
            whiten: 0.2,
            brighten: 0.1,
            detail_preserve: 0.3,
        }
    }
}

/// Shared prologue: params, luma weights, skin-mask helper.
const BEAUTY_PROLOGUE: &str = r#"
// `smoothing` mirrors the host struct's `smooth` field (`smooth` is a
// reserved WGSL keyword); layout matching is positional.
struct BeautyParams {
    smoothing: f32,
    whiten: f32,
    brighten: f32,
    detail_preserve: f32,
};
@group(1) @binding(0) var<uniform> p: BeautyParams;

const LUMA_W: vec3<f32> = vec3<f32>(0.2126, 0.7152, 0.0722);
// Bilateral range falloff: 1 / (2 * sigma^2) with sigma = 0.08 luma.
const INV_2SR2: f32 = 78.125;
// Tap spacing in input pixels.
const SPREAD: f32 = 2.0;

// Heuristic skin confidence from gamma-encoded RGB: distance to the
// BT.601 YCbCr skin cluster, times a saturation band (skip near-gray
// walls and neon), all in 0..1.
fn skin_confidence(c: vec3<f32>) -> f32 {
    let cb = -0.168736 * c.r - 0.331264 * c.g + 0.5 * c.b;
    let cr = 0.5 * c.r - 0.418688 * c.g - 0.081312 * c.b;
    let dcb = (cb + 0.05) / 0.11;
    let dcr = (cr - 0.11) / 0.09;
    let cluster = 1.0 - smoothstep(1.0, 2.2, dcb * dcb + dcr * dcr);
    let sat = max(max(c.r, c.g), c.b) - min(min(c.r, c.g), c.b);
    let sat_band = smoothstep(0.02, 0.08, sat) * (1.0 - smoothstep(0.5, 0.75, sat));
    return cluster * sat_band;
}
"#;

/// `Fast`: 8 neighbor taps (3x3 ring).
const BEAUTY_TAPS_FAST: &str = r#"
const N_TAPS: u32 = 8u;
const OFFS: array<vec2<f32>, 8> = array<vec2<f32>, 8>(
    vec2<f32>(1.0, 0.0), vec2<f32>(-1.0, 0.0),
    vec2<f32>(0.0, 1.0), vec2<f32>(0.0, -1.0),
    vec2<f32>(1.0, 1.0), vec2<f32>(-1.0, 1.0),
    vec2<f32>(1.0, -1.0), vec2<f32>(-1.0, -1.0),
);
const WTS: array<f32, 8> = array<f32, 8>(
    1.0, 1.0, 1.0, 1.0, 0.7, 0.7, 0.7, 0.7,
);
"#;

/// `Balanced`: the `Fast` ring plus 4 far axis taps.
const BEAUTY_TAPS_BALANCED: &str = r#"
const N_TAPS: u32 = 12u;
const OFFS: array<vec2<f32>, 12> = array<vec2<f32>, 12>(
    vec2<f32>(1.0, 0.0), vec2<f32>(-1.0, 0.0),
    vec2<f32>(0.0, 1.0), vec2<f32>(0.0, -1.0),
    vec2<f32>(1.0, 1.0), vec2<f32>(-1.0, 1.0),
    vec2<f32>(1.0, -1.0), vec2<f32>(-1.0, -1.0),
    vec2<f32>(2.0, 0.0), vec2<f32>(-2.0, 0.0),
    vec2<f32>(0.0, 2.0), vec2<f32>(0.0, -2.0),
);
const WTS: array<f32, 12> = array<f32, 12>(
    1.0, 1.0, 1.0, 1.0, 0.7, 0.7, 0.7, 0.7, 0.4, 0.4, 0.4, 0.4,
);
"#;

const BEAUTY_MAIN: &str = r#"
@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    // Neighborhood steps are in INPUT texels; ez.width/height is the
    // output size and would be wrong when resizing.
    let step = SPREAD / vec2<f32>(textureDimensions(texture1));
    let center = textureSampleLevel(texture1, sampler1, tex_coord, 0.0).rgb;
    let luma_c = dot(center, LUMA_W);

    // Edge-preserving luma smoothing: spatial weight x range weight.
    var sum = luma_c;
    var wsum = 1.0;
    for (var i = 0u; i < N_TAPS; i = i + 1u) {
        let l = dot(textureSampleLevel(texture1, sampler1, tex_coord + OFFS[i] * step, 0.0).rgb, LUMA_W);
        let d = l - luma_c;
        let w = WTS[i] * exp(-d * d * INV_2SR2);
        sum += l * w;
        wsum += w;
    }
    let luma_s = sum / wsum;
    let high = luma_c - luma_s;

    // Gate by skin confidence and back off on strong edges (jawlines,
    // glasses, hair strands keep their contrast).
    let edge_keep = 1.0 - smoothstep(0.05, 0.15, abs(high));
    let mask = skin_confidence(center) * edge_keep;
    let strength = clamp(p.smoothing, 0.0, 1.0) * mask;

    // Smooth the luma, then hand back part of the removed detail.
    var luma_out = mix(luma_c, luma_s, strength);
    luma_out += high * clamp(p.detail_preserve, 0.0, 1.0) * strength;

    // Apply as a luma ratio so hue and saturation stay put.
    var c = center * (luma_out / max(luma_c, 1e-3));

    // Whitening (skin-masked) and brightening (global): both use the
    // highlight-compressed curve c + k*c*(1-c), which cannot clip.
    let whiten = clamp(p.whiten, 0.0, 1.0) * mask;
    c = c + whiten * 0.35 * c * (1.0 - c);
    let brighten = clamp(p.brighten, 0.0, 1.0);
    c = c + brighten * 0.25 * c * (1.0 - c);

    return vec4<f32>(clamp(c, vec3<f32>(0.0), vec3<f32>(1.0)), 1.0);
}
"#;

pub(super) fn beauty_shader(quality: BeautyQuality) -> String {
    let taps = match quality {
        BeautyQuality::Fast => BEAUTY_TAPS_FAST,
        BeautyQuality::Balanced => BEAUTY_TAPS_BALANCED,
    };
    let mut body = String::with_capacity(BEAUTY_PROLOGUE.len() + taps.len() + BEAUTY_MAIN.len());
    body.push_str(BEAUTY_PROLOGUE);
    body.push_str(taps);
    body.push_str(BEAUTY_MAIN);
    effect_module(&body)
}

/// Builder for [`beauty_lite`]: [`EffectBuilder`] plus the build-time
/// [`BeautyQuality`] choice.
pub struct BeautyBuilder {
    eb: EffectBuilder<BeautyParams>,
    quality: BeautyQuality,
}

impl BeautyBuilder {
    /// Smoothing kernel size (default [`BeautyQuality::Balanced`]).
    pub fn quality(mut self, quality: BeautyQuality) -> Self {
        self.quality = quality;
        self
    }

    /// Replaces the initial parameter value.
    pub fn params(mut self, params: BeautyParams) -> Self {
        self.eb = self.eb.params(params);
        self
    }

    /// See [`EffectBuilder::output_size`].
    pub fn output_size(mut self, width: u32, height: u32) -> Self {
        self.eb = self.eb.output_size(width, height);
        self
    }

    /// See [`EffectBuilder::frames_in_flight`].
    pub fn frames_in_flight(mut self, count: usize) -> Self {
        self.eb = self.eb.frames_in_flight(count);
        self
    }

    /// See [`EffectBuilder::zero_copy_readback`].
    pub fn zero_copy_readback(mut self, enabled: bool) -> Self {
        self.eb = self.eb.zero_copy_readback(enabled);
        self
    }

    /// See [`EffectBuilder::hw_zero_copy_input`].
    pub fn hw_zero_copy_input(mut self, enabled: bool) -> Self {
        self.eb = self.eb.hw_zero_copy_input(enabled);
        self
    }

    pub fn build(mut self) -> Result<Effect<BeautyParams>, WgpuFilterError> {
        // The kernel is baked into the shader, so it is (re)generated here
        // where the final quality choice is known.
        self.eb.inner = self.eb.inner.shader_wgsl(beauty_shader(self.quality));
        self.eb.build()
    }
}

/// Heuristic skin smoothing + whitening. The `_lite` suffix is the
/// quality contract: the heuristic skin mask can misjudge skin-toned wood
/// or sand, and production-grade beautification needs face segmentation
/// (see the module docs). See [`BeautyParams`] and [`BeautyQuality`].
pub fn beauty_lite(params: BeautyParams) -> BeautyBuilder {
    BeautyBuilder {
        eb: EffectBuilder::new(beauty_shader(BeautyQuality::default()), params),
        quality: BeautyQuality::default(),
    }
}

/// [`beauty_lite`] with a stronger preset: the fused portrait look
/// (smoothing + whitening + brightening) for talking-head streams.
pub fn portrait() -> BeautyBuilder {
    beauty_lite(BeautyParams {
        smooth: 0.65,
        whiten: 0.35,
        brighten: 0.2,
        detail_preserve: 0.35,
    })
}
