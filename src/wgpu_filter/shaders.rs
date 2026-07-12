//! Built-in WGSL shaders for the wgpu frame filter pipeline.
//!
//! Pipeline: upload YUV planes -> [convert pass] RGBA intermediate ->
//! [user effect pass] RGBA output -> [pack pass] YUV420P storage buffer.

/// Fullscreen-triangle vertex shader shared by the convert and effect passes.
/// Emits `@location(0) tex_coord` with (0,0) at the image top-left.
pub(crate) const FULLSCREEN_VS: &str = r#"
struct VsOut {
    @builtin(position) pos: vec4<f32>,
    @location(0) tex_coord: vec2<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) idx: u32) -> VsOut {
    var out: VsOut;
    let uv = vec2<f32>(f32((idx << 1u) & 2u), f32(idx & 2u));
    out.pos = vec4<f32>(uv * 2.0 - 1.0, 0.0, 1.0);
    out.tex_coord = vec2<f32>(uv.x, 1.0 - uv.y);
    return out;
}
"#;

/// Shared YUV->RGB math for the convert pass.
const CONVERT_COMMON: &str = r#"
struct ConvertUniforms {
    matrix_id: u32,   // 0 = BT.601, 1 = BT.709
    full_range: u32,  // 0 = limited (MPEG), 1 = full (JPEG)
    _pad0: u32,
    _pad1: u32,
};

fn yuv_to_rgb(cu: ConvertUniforms, yuv_in: vec3<f32>) -> vec3<f32> {
    var y = yuv_in.x;
    var u = yuv_in.y - 0.5;
    var v = yuv_in.z - 0.5;
    if (cu.full_range == 0u) {
        y = (y - 16.0 / 255.0) * (255.0 / 219.0);
        u = u * (255.0 / 224.0);
        v = v * (255.0 / 224.0);
    }
    var rgb: vec3<f32>;
    if (cu.matrix_id == 1u) {
        rgb = vec3<f32>(
            y + 1.5748 * v,
            y - 0.187324 * u - 0.468124 * v,
            y + 1.8556 * u,
        );
    } else {
        rgb = vec3<f32>(
            y + 1.402 * v,
            y - 0.344136 * u - 0.714136 * v,
            y + 1.772 * u,
        );
    }
    return clamp(rgb, vec3<f32>(0.0), vec3<f32>(1.0));
}
"#;

/// Convert pass fragment shader for planar YUV (three R8 planes). Chroma is
/// sampled with normalized coordinates, so the same shader covers 4:2:0,
/// 4:2:2 and 4:4:4 — the chroma texture dimensions alone decide the ratio.
pub(crate) fn convert_fs_planar() -> String {
    format!(
        r#"{CONVERT_COMMON}
@group(0) @binding(0) var tex_y: texture_2d<f32>;
@group(0) @binding(1) var tex_u: texture_2d<f32>;
@group(0) @binding(2) var tex_v: texture_2d<f32>;
@group(0) @binding(3) var samp: sampler;
@group(0) @binding(4) var<uniform> cu: ConvertUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {{
    let y = textureSample(tex_y, samp, tex_coord).r;
    let u = textureSample(tex_u, samp, tex_coord).r;
    let v = textureSample(tex_v, samp, tex_coord).r;
    return vec4<f32>(yuv_to_rgb(cu, vec3<f32>(y, u, v)), 1.0);
}}
"#
    )
}

/// Convert pass fragment shader for NV12 (R8 luma plane + RG8 interleaved chroma).
pub(crate) fn convert_fs_nv12() -> String {
    format!(
        r#"{CONVERT_COMMON}
@group(0) @binding(0) var tex_y: texture_2d<f32>;
@group(0) @binding(1) var tex_uv: texture_2d<f32>;
@group(0) @binding(3) var samp: sampler;
@group(0) @binding(4) var<uniform> cu: ConvertUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {{
    let y = textureSample(tex_y, samp, tex_coord).r;
    let uv = textureSample(tex_uv, samp, tex_coord).rg;
    return vec4<f32>(yuv_to_rgb(cu, vec3<f32>(y, uv.x, uv.y)), 1.0);
}}
"#
    )
}

/// Compute shader packing the RGBA output texture into tightly-strided
/// YUV420P planes inside one storage buffer (u32 words, 4-byte row alignment).
/// Each invocation packs an 8x2 luma block (4 Y words, 1 U word, 1 V word).
pub(crate) const PACK_CS: &str = r#"
struct PackUniforms {
    width: u32,
    height: u32,
    y_stride_words: u32,
    c_stride_words: u32,
    u_offset_words: u32,
    v_offset_words: u32,
    matrix_id: u32,   // 0 = BT.601, 1 = BT.709
    full_range: u32,  // 0 = limited, 1 = full
};

@group(0) @binding(0) var src: texture_2d<f32>;
@group(0) @binding(1) var<uniform> pu: PackUniforms;
@group(0) @binding(2) var<storage, read_write> out_buf: array<u32>;

fn rgb_to_yuv(c: vec3<f32>) -> vec3<f32> {
    var yuv: vec3<f32>;
    if (pu.matrix_id == 1u) {
        let y = 0.2126 * c.r + 0.7152 * c.g + 0.0722 * c.b;
        yuv = vec3<f32>(y, (c.b - y) / 1.8556 + 0.5, (c.r - y) / 1.5748 + 0.5);
    } else {
        let y = 0.299 * c.r + 0.587 * c.g + 0.114 * c.b;
        yuv = vec3<f32>(y, (c.b - y) / 1.772 + 0.5, (c.r - y) / 1.402 + 0.5);
    }
    if (pu.full_range == 0u) {
        yuv.x = yuv.x * (219.0 / 255.0) + 16.0 / 255.0;
        yuv.y = (yuv.y - 0.5) * (224.0 / 255.0) + 0.5;
        yuv.z = (yuv.z - 0.5) * (224.0 / 255.0) + 0.5;
    }
    return clamp(yuv, vec3<f32>(0.0), vec3<f32>(1.0));
}

fn load_px(x: u32, y: u32) -> vec3<f32> {
    let cx = min(x, pu.width - 1u);
    let cy = min(y, pu.height - 1u);
    return textureLoad(src, vec2<u32>(cx, cy), 0).rgb;
}

fn quantize(v: f32) -> u32 {
    return u32(clamp(v, 0.0, 1.0) * 255.0 + 0.5);
}

@compute @workgroup_size(8, 8, 1)
fn cs_main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let bx = gid.x;
    let by = gid.y;
    let x0 = bx * 8u;
    let y0 = by * 2u;
    if (x0 >= pu.width || y0 >= pu.height) {
        return;
    }

    // Process the block as four 2x2 quads, one at a time: each quad
    // contributes two adjacent luma bytes per row and one chroma sample,
    // so only four RGB values are ever live (a whole-8x2 preload keeps a
    // 48-float working set that spills on register-limited GPUs). Every
    // texel is still fetched exactly once, edge-clamped by load_px, and
    // the per-sample arithmetic (operand order included) is unchanged.
    let cw = (pu.width + 1u) / 2u;
    let ch = (pu.height + 1u) / 2u;
    let write_row1 = (y0 + 1u) < pu.height;
    let write_chroma = (by < ch) && (bx * 4u < cw);
    // Zero-initialized accumulators: [row][word].
    var words: array<array<u32, 2>, 2>;
    var uw = 0u;
    var vw = 0u;
    for (var q = 0u; q < 4u; q = q + 1u) {
        let px = x0 + q * 2u;
        // Quads only move right: the first fully out-of-range quad ends
        // the block (its luma bytes and chroma sample would all be OOB).
        if (px >= pu.width) {
            break;
        }
        let p00 = load_px(px, y0);
        let p10 = load_px(px + 1u, y0);
        let p01 = load_px(px, y0 + 1u);
        let p11 = load_px(px + 1u, y0 + 1u);

        // Luma: two bytes per row into word q/2 at byte offset (q%2)*2.
        let w = q / 2u;
        let shift = (q % 2u) * 16u;
        var row0 = quantize(rgb_to_yuv(p00).x);
        if (px + 1u < pu.width) {
            row0 = row0 | (quantize(rgb_to_yuv(p10).x) << 8u);
        }
        words[0][w] = words[0][w] | (row0 << shift);
        if (write_row1) {
            var row1 = quantize(rgb_to_yuv(p01).x);
            if (px + 1u < pu.width) {
                row1 = row1 | (quantize(rgb_to_yuv(p11).x) << 8u);
            }
            words[1][w] = words[1][w] | (row1 << shift);
        }

        // Chroma sample q: the quad average. Edge quads average clamped
        // (replicated) loads, exactly like the block form did.
        if (write_chroma && (bx * 4u + q) < cw) {
            let avg = (p00 + p10 + p01 + p11) * 0.25;
            let yuv = rgb_to_yuv(avg);
            uw = uw | (quantize(yuv.y) << (q * 8u));
            vw = vw | (quantize(yuv.z) << (q * 8u));
        }
    }

    // Luma words: a fully out-of-range word must not be written at all —
    // in the flat buffer, index yy*stride + stride aliases row yy+1 word 0.
    for (var w = 0u; w < 2u; w = w + 1u) {
        if (x0 + w * 4u >= pu.width) {
            break;
        }
        out_buf[y0 * pu.y_stride_words + bx * 2u + w] = words[0][w];
        if (write_row1) {
            out_buf[(y0 + 1u) * pu.y_stride_words + bx * 2u + w] = words[1][w];
        }
    }
    // Chroma: one u32 word of 4 subsampled samples per block; same
    // no-fully-OOB-word rule.
    if (write_chroma) {
        out_buf[pu.u_offset_words + by * pu.c_stride_words + bx] = uw;
        out_buf[pu.v_offset_words + by * pu.c_stride_words + bx] = vw;
    }
}
"#;

/// Identity effect shader, also serving as the minimal template for the
/// user-shader contract (see module docs).
pub(crate) const IDENTITY_FS: &str = r#"
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms {
    play_time: f32,
    width: f32,
    height: f32,
    _pad: f32,
};
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    return textureSample(texture1, sampler1, tex_coord);
}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    /// Every built-in WGSL module must parse and validate offline: a shader
    /// error would otherwise surface only at pipeline creation on a live
    /// GPU device, which no CI lane has. The validator is the same naga
    /// wgpu compiles these strings with at runtime.
    #[test]
    fn builtin_shaders_parse_and_validate() {
        let planar = convert_fs_planar();
        let nv12 = convert_fs_nv12();
        let modules: [(&str, &str); 5] = [
            ("fullscreen_vs", FULLSCREEN_VS),
            ("convert_fs_planar", planar.as_str()),
            ("convert_fs_nv12", nv12.as_str()),
            ("pack_cs", PACK_CS),
            ("identity_fs", IDENTITY_FS),
        ];
        for (name, source) in modules {
            let module = naga::front::wgsl::parse_str(source)
                .unwrap_or_else(|e| panic!("{name}: WGSL parse failed:\n{e}"));
            naga::valid::Validator::new(
                naga::valid::ValidationFlags::all(),
                naga::valid::Capabilities::all(),
            )
            .validate(&module)
            .unwrap_or_else(|e| panic!("{name}: WGSL validation failed: {e:?}"));
        }
    }
}
