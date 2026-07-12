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

/// Compute shader packing the effect output texture into tightly-strided
/// YUV420P planes inside one storage buffer (u32 words, 4-byte row alignment).
/// Each invocation packs an 8x2 luma block (4 Y words, 1 U word, 1 V word).
///
/// Two pack modes: RGB (matrix + range transform) for the RGBA effect
/// pipeline, and identity for the YUV passthrough pipeline, where the
/// texture already holds raw YUV code values and no matrix or range
/// conversion is applied (the 4:2:0 pack still box-averages chroma).
pub(crate) const PACK_CS: &str = r#"
struct PackUniforms {
    width: u32,
    height: u32,
    y_stride_words: u32,
    c_stride_words: u32,
    u_offset_words: u32,
    v_offset_words: u32,
    matrix_id: u32,   // 0 = BT.601, 1 = BT.709 (RGB mode only)
    full_range: u32,  // 0 = limited, 1 = full (RGB mode only)
    pack_mode: u32,   // 0 = RGB -> YUV, 1 = identity (raw YUV code values)
    _pad0: u32,
    _pad1: u32,
    _pad2: u32,
};

@group(0) @binding(0) var src: texture_2d<f32>;
@group(0) @binding(1) var<uniform> pu: PackUniforms;
@group(0) @binding(2) var<storage, read_write> out_buf: array<u32>;

fn texel_to_yuv(c: vec3<f32>) -> vec3<f32> {
    if (pu.pack_mode == 1u) {
        // Identity: the texel channels ARE (Y, U, V) code values.
        return c;
    }
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
        var row0 = quantize(texel_to_yuv(p00).x);
        if (px + 1u < pu.width) {
            row0 = row0 | (quantize(texel_to_yuv(p10).x) << 8u);
        }
        words[0][w] = words[0][w] | (row0 << shift);
        if (write_row1) {
            var row1 = quantize(texel_to_yuv(p01).x);
            if (px + 1u < pu.width) {
                row1 = row1 | (quantize(texel_to_yuv(p11).x) << 8u);
            }
            words[1][w] = words[1][w] | (row1 << shift);
        }

        // Chroma sample q: the quad average. Edge quads average clamped
        // (replicated) loads, exactly like the block form did.
        if (write_chroma && (bx * 4u + q) < cw) {
            let avg = (p00 + p10 + p01 + p11) * 0.25;
            let yuv = texel_to_yuv(avg);
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

/// Shared prelude functions of the YUV passthrough effect shader. The
/// layout-specific piece (bindings + `ez_chroma` + `ez_chroma_size`) is
/// prepended by [`yuv_effect_fs_planar`]/[`yuv_effect_fs_nv12`]; the user
/// body only ever calls these functions, so one body compiles against both
/// plane layouts.
const YUV_PRELUDE_COMMON: &str = r#"
fn ez_luma(coord: vec2<f32>) -> f32 {
    return textureSample(ez_tex_y, ez_samp, coord).r;
}
fn ez_sample_yuv(coord: vec2<f32>) -> vec3<f32> {
    return vec3<f32>(ez_luma(coord), ez_chroma(coord));
}
fn ez_input_size() -> vec2<f32> {
    return vec2<f32>(textureDimensions(ez_tex_y));
}
fn ez_output_size() -> vec2<f32> {
    return vec2<f32>(ez_yuv_uniforms.out_w, ez_yuv_uniforms.out_h);
}
fn ez_play_time() -> f32 {
    return ez_yuv_uniforms.play_time;
}
fn ez_full_range() -> bool {
    return ez_yuv_uniforms.range_flag > 0.5;
}
"#;

/// Epilogue appended after the user body: the user defines `ez_effect`, the
/// library owns the fragment entry point and pads the alpha channel.
const YUV_EPILOGUE: &str = r#"
@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    return vec4<f32>(ez_effect(tex_coord), 1.0);
}
"#;

/// Layout-specific prelude head for planar input (three R8 planes).
const YUV_HEADER_PLANAR: &str = r#"struct EzYuvUniforms {
    play_time: f32,
    out_w: f32,
    out_h: f32,
    range_flag: f32,
};
@group(0) @binding(0) var ez_tex_y: texture_2d<f32>;
@group(0) @binding(1) var ez_tex_u: texture_2d<f32>;
@group(0) @binding(2) var ez_tex_v: texture_2d<f32>;
@group(0) @binding(3) var ez_samp: sampler;
@group(0) @binding(4) var<uniform> ez_yuv_uniforms: EzYuvUniforms;

fn ez_chroma(coord: vec2<f32>) -> vec2<f32> {
    return vec2<f32>(
        textureSample(ez_tex_u, ez_samp, coord).r,
        textureSample(ez_tex_v, ez_samp, coord).r,
    );
}
fn ez_chroma_size() -> vec2<f32> {
    return vec2<f32>(textureDimensions(ez_tex_u));
}
"#;

/// Layout-specific prelude head for NV12 input (R8 luma + RG8 chroma).
const YUV_HEADER_NV12: &str = r#"struct EzYuvUniforms {
    play_time: f32,
    out_w: f32,
    out_h: f32,
    range_flag: f32,
};
@group(0) @binding(0) var ez_tex_y: texture_2d<f32>;
@group(0) @binding(1) var ez_tex_uv: texture_2d<f32>;
@group(0) @binding(3) var ez_samp: sampler;
@group(0) @binding(4) var<uniform> ez_yuv_uniforms: EzYuvUniforms;

fn ez_chroma(coord: vec2<f32>) -> vec2<f32> {
    return textureSample(ez_tex_uv, ez_samp, coord).rg;
}
fn ez_chroma_size() -> vec2<f32> {
    return vec2<f32>(textureDimensions(ez_tex_uv));
}
"#;

fn assemble_yuv_module(header: &str, body: &str) -> String {
    let mut module = String::with_capacity(
        header.len() + YUV_PRELUDE_COMMON.len() + body.len() + YUV_EPILOGUE.len(),
    );
    module.push_str(header);
    module.push_str(YUV_PRELUDE_COMMON);
    module.push_str(body);
    module.push_str(YUV_EPILOGUE);
    module
}

/// Full YUV passthrough effect module for planar input. `body` is the
/// user-provided WGSL defining `fn ez_effect(coord: vec2<f32>) -> vec3<f32>`.
pub(crate) fn yuv_effect_fs_planar(body: &str) -> String {
    assemble_yuv_module(YUV_HEADER_PLANAR, body)
}

/// Full YUV passthrough effect module for NV12 input.
pub(crate) fn yuv_effect_fs_nv12(body: &str) -> String {
    assemble_yuv_module(YUV_HEADER_NV12, body)
}

/// Identity body for the YUV passthrough contract: samples the input at the
/// same coordinate and returns its code values unchanged. Used by tests as
/// the minimal conforming shader body.
#[cfg(test)]
pub(crate) const YUV_IDENTITY_BODY: &str = r#"
fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
    return ez_sample_yuv(coord);
}
"#;

/// Strips `//` line comments and nested `/* */` block comments so the
/// lexical body checks below cannot be fooled by commented-out code. WGSL
/// has no string literals, which keeps this a plain scanner; an unterminated
/// block comment swallows the rest (naga rejects such a module anyway).
///
/// A `//` comment ends at any WGSL line break — LF, VT, FF, CR, NEL, LS or
/// PS — not just LF, so `// hidden\r<code>` cannot smuggle `<code>` past
/// the checks (naga starts a fresh line at each of these).
fn strip_wgsl_comments(src: &str) -> String {
    fn is_line_break(c: char) -> bool {
        matches!(
            c,
            '\n' | '\x0B' | '\x0C' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}'
        )
    }
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '/' {
            match chars.peek() {
                Some('/') => {
                    for c2 in chars.by_ref() {
                        if is_line_break(c2) {
                            out.push(c2);
                            break;
                        }
                    }
                    continue;
                }
                Some('*') => {
                    chars.next();
                    let mut depth = 1u32;
                    let mut prev = ' ';
                    while depth > 0 {
                        let Some(c2) = chars.next() else { break };
                        if prev == '/' && c2 == '*' {
                            depth += 1;
                            prev = ' ';
                        } else if prev == '*' && c2 == '/' {
                            depth -= 1;
                            prev = ' ';
                        } else {
                            prev = c2;
                        }
                    }
                    // Keep token separation where the comment sat.
                    out.push(' ');
                    continue;
                }
                _ => {}
            }
        }
        out.push(c);
    }
    out
}

fn ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// WGSL blankspace (spec "blankspace" pattern): ASCII whitespace plus NEL,
/// the LTR/RTL marks and the LS/PS separators. Rust's `str::trim_start`
/// does NOT cover U+200E/U+200F (category Cf, not White_Space), yet naga
/// separates tokens at them like at a space — so the scanners below must
/// use this predicate or `@\u{200E}group(0)` slips through while a legal
/// `fn\u{200E}ez_effect` is falsely rejected.
fn wgsl_blank(c: char) -> bool {
    matches!(
        c,
        ' ' | '\t'
            | '\n'
            | '\x0B'
            | '\x0C'
            | '\r'
            | '\u{0085}'
            | '\u{200E}'
            | '\u{200F}'
            | '\u{2028}'
            | '\u{2029}'
    )
}

fn trim_wgsl_start(s: &str) -> &str {
    s.trim_start_matches(wgsl_blank)
}

/// Whether a YUV effect body defines `fn ez_effect(...)` — the function the
/// generated `fs_main` calls. A lexical check (comments stripped, `fn` and
/// the name matched as whole words, `(` required), so `// fn ez_effect` or
/// `fn ez_effect_helper` do not count; used by `build()` to fail early with
/// a friendly message instead of a shader diagnostic at init.
pub(crate) fn body_defines_ez_effect(body: &str) -> bool {
    let stripped = strip_wgsl_comments(body);
    let s = stripped.as_str();
    let mut search = 0;
    while let Some(rel) = s[search..].find("fn") {
        let at = search + rel;
        search = at + 2;
        if s[..at].chars().next_back().is_some_and(ident_char) {
            continue; // tail of a longer identifier
        }
        let rest = &s[at + 2..];
        let trimmed = trim_wgsl_start(rest);
        if trimmed.len() == rest.len() {
            continue; // no separator after `fn` (e.g. `fnez_effect`)
        }
        if let Some(after_name) = trimmed.strip_prefix("ez_effect") {
            if !after_name.starts_with(ident_char) && trim_wgsl_start(after_name).starts_with('(') {
                return true;
            }
        }
    }
    false
}

/// Whether a YUV effect body declares a binding group other than the
/// literal `@group(1)` params group. `@group(0)` is reserved for the
/// library prelude, and naga only rejects a duplicate binding when BOTH
/// declarations are statically used — an unused-looking alias could
/// silently sample the prelude's resources. Group indices are
/// const-expressions (`0u`, `0x0`, `1-1`, `u32(0)` and a named const all
/// name group 0), which a lexical scan cannot evaluate, so the check is an
/// allowlist instead: after comment stripping, every `@group(...)`
/// argument must be the single token `1`; any other spelling is rejected
/// at `build()`.
pub(crate) fn body_declares_reserved_group(body: &str) -> bool {
    let stripped = strip_wgsl_comments(body);
    let s = stripped.as_str();
    let mut search = 0;
    while let Some(rel) = s[search..].find('@') {
        let at = search + rel;
        search = at + 1;
        let rest = trim_wgsl_start(&s[at + 1..]);
        let Some(args) = rest.strip_prefix("group") else {
            continue;
        };
        // `group` must end there (`@groups` is some other attribute).
        if args.starts_with(ident_char) {
            continue;
        }
        let args = trim_wgsl_start(args);
        let Some(inner) = args.strip_prefix('(') else {
            continue;
        };
        // Take the argument up to the matching `)` — `u32(0)` nests.
        let mut depth = 1u32;
        let mut end = None;
        for (i, c) in inner.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else {
            return true; // unterminated argument — reject conservatively
        };
        if inner[..end].trim_matches(wgsl_blank) != "1" {
            return true;
        }
    }
    false
}

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
        let yuv_planar = yuv_effect_fs_planar(YUV_IDENTITY_BODY);
        let yuv_nv12 = yuv_effect_fs_nv12(YUV_IDENTITY_BODY);
        let modules: [(&str, &str); 7] = [
            ("fullscreen_vs", FULLSCREEN_VS),
            ("convert_fs_planar", planar.as_str()),
            ("convert_fs_nv12", nv12.as_str()),
            ("pack_cs", PACK_CS),
            ("identity_fs", IDENTITY_FS),
            ("yuv_effect_fs_planar", yuv_planar.as_str()),
            ("yuv_effect_fs_nv12", yuv_nv12.as_str()),
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

    #[test]
    fn ez_effect_detection_matches_definitions_only() {
        assert!(body_defines_ez_effect(YUV_IDENTITY_BODY));
        assert!(body_defines_ez_effect(
            "fn ez_effect (coord: vec2<f32>) -> vec3<f32> { return vec3<f32>(0.0); }"
        ));
        assert!(body_defines_ez_effect(
            "fn\n  ez_effect\n  (c: vec2<f32>) -> vec3<f32> {}"
        ));
        // U+200E is WGSL blankspace: a legal separator after `fn`.
        assert!(body_defines_ez_effect(
            "fn\u{200E}ez_effect(c: vec2<f32>) -> vec3<f32> {}"
        ));

        // Comments, longer identifiers and call sites must not count.
        assert!(!body_defines_ez_effect("// fn ez_effect(c: vec2<f32>)"));
        assert!(!body_defines_ez_effect(
            "/* fn ez_effect(c) */ fn other() {}"
        ));
        assert!(!body_defines_ez_effect(
            "fn ez_effect_helper(c: vec2<f32>) -> vec3<f32> {}"
        ));
        assert!(!body_defines_ez_effect("fnez_effect(c)"));
        assert!(!body_defines_ez_effect("let x = my_fn; ez_effect(x);"));
    }

    #[test]
    fn group_detection_allows_only_the_literal_params_group() {
        // Group indices are const-expressions: every spelling below names a
        // reserved group (or is unreadable) and must be rejected.
        for body in [
            "@group(0) @binding(9) var<uniform> x: f32;",
            "@ group ( 0 ) @binding(0) var t: f32;",
            "@group(0u) @binding(3) var s: sampler;",
            "@group(0x0) @binding(0) var t: texture_2d<f32>;",
            "@group(1-1) @binding(0) var t: texture_2d<f32>;",
            "@group(u32(0)) @binding(0) var t: texture_2d<f32>;",
            "const ZERO = 0; @group(ZERO) @binding(0) var t: texture_2d<f32>;",
            "@group(2) @binding(0) var<uniform> p: f32;",
            "@group(1u) @binding(0) var<uniform> p: f32;", // literal `1` only
            "@group(1 @binding(0) var<uniform> p: f32;",   // unterminated
            // WGSL blankspace includes U+200E/U+200F, which Rust's
            // trim_start does not — naga still tokenizes across them.
            "@\u{200E}group(0u) @binding(3) var s: sampler;",
            "@group\u{200F}(0) @binding(0) var t: texture_2d<f32>;",
        ] {
            assert!(body_declares_reserved_group(body), "not rejected: {body}");
        }

        for body in [
            "@group(1) @binding(0) var<uniform> params: f32;",
            "@group( 1 ) @binding(0) var<uniform> params: f32;",
            "@group(/* params */ 1) @binding(0) var<uniform> params: f32;",
            "@group\u{200E}(\u{200F}1\u{200E}) @binding(0) var<uniform> params: f32;",
            "// @group(0) commented out",
            "/* nested /* @group(0) */ still comment */ fn f() {}",
        ] {
            assert!(!body_declares_reserved_group(body), "rejected: {body}");
        }
    }

    #[test]
    fn comment_stripping_handles_line_block_and_nesting() {
        assert_eq!(strip_wgsl_comments("a // b\nc"), "a \nc");
        assert_eq!(strip_wgsl_comments("a /* b */ c"), "a   c");
        assert_eq!(strip_wgsl_comments("a /* b /* c */ d */ e"), "a   e");
        // Unterminated block comments swallow the tail.
        assert_eq!(strip_wgsl_comments("a /* b"), "a  ");
    }

    #[test]
    fn comment_stripping_ends_line_comments_at_every_wgsl_line_break() {
        // WGSL also breaks lines at VT, FF, CR, NEL, LS and PS; a `//`
        // comment must not swallow code that naga sees on the next line.
        for br in ['\x0B', '\x0C', '\r', '\u{0085}', '\u{2028}', '\u{2029}'] {
            let hidden = format!("// hide{br}@group(0) @binding(0) var s: sampler;");
            assert!(body_declares_reserved_group(&hidden), "break {br:?}");
            let def = format!("//x{br}fn ez_effect(c: vec2<f32>) -> vec3<f32> {{}}");
            assert!(body_defines_ez_effect(&def), "break {br:?}");
        }
        assert_eq!(strip_wgsl_comments("a // b\rc"), "a \rc");
    }
}
