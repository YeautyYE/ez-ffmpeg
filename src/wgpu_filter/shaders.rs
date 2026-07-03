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

    // Load the 8x2 block once (edge-clamped, exactly load_px semantics);
    // both the luma words and the chroma averages read these registers, so
    // no texel is fetched twice.
    var rgb: array<array<vec3<f32>, 8>, 2>;
    for (var row = 0u; row < 2u; row = row + 1u) {
        for (var col = 0u; col < 8u; col = col + 1u) {
            rgb[row][col] = load_px(x0 + col, y0 + row);
        }
    }

    // Luma: two rows, two u32 words (4 pixels each) per row.
    for (var row = 0u; row < 2u; row = row + 1u) {
        let yy = y0 + row;
        if (yy >= pu.height) {
            break;
        }
        for (var w = 0u; w < 2u; w = w + 1u) {
            let word_x0 = x0 + w * 4u;
            // A fully out-of-range word must not be written at all: in the flat
            // buffer, index yy*stride + stride aliases row yy+1 word 0.
            if (word_x0 >= pu.width) {
                break;
            }
            var word = 0u;
            for (var b = 0u; b < 4u; b = b + 1u) {
                let xx = word_x0 + b;
                var val = 0u;
                if (xx < pu.width) {
                    val = quantize(rgb_to_yuv(rgb[row][w * 4u + b]).x);
                }
                word = word | (val << (b * 8u));
            }
            out_buf[yy * pu.y_stride_words + bx * 2u + w] = word;
        }
    }

    // Chroma: one row per block, one u32 word of 4 subsampled samples. The
    // 2x2 sources are the same block pixels loaded above (edge clamping
    // included, since load_px clamped them at load time).
    let cw = (pu.width + 1u) / 2u;
    let ch = (pu.height + 1u) / 2u;
    let cy = by;
    if (cy >= ch) {
        return;
    }
    // Same aliasing hazard as the luma tail: never write a fully OOB word.
    if (bx * 4u >= cw) {
        return;
    }
    var uw = 0u;
    var vw = 0u;
    for (var b = 0u; b < 4u; b = b + 1u) {
        let cx = bx * 4u + b;
        var uval = 0u;
        var vval = 0u;
        if (cx < cw) {
            let avg = (rgb[0][b * 2u] + rgb[0][b * 2u + 1u]
                + rgb[1][b * 2u] + rgb[1][b * 2u + 1u]) * 0.25;
            let yuv = rgb_to_yuv(avg);
            uval = quantize(yuv.y);
            vval = quantize(yuv.z);
        }
        uw = uw | (uval << (b * 8u));
        vw = vw | (vval << (b * 8u));
    }
    out_buf[pu.u_offset_words + cy * pu.c_stride_words + bx] = uw;
    out_buf[pu.v_offset_words + cy * pu.c_stride_words + bx] = vw;
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
