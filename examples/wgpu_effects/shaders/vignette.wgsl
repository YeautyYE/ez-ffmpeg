// Vignette: darkens toward the corners, aspect-ratio aware.
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let color = textureSample(texture1, sampler1, tex_coord);
    let aspect = ez.width / ez.height;
    let centered = (tex_coord - vec2<f32>(0.5)) * vec2<f32>(aspect, 1.0);
    let falloff = smoothstep(0.9, 0.3, length(centered));
    return vec4<f32>(color.rgb * falloff, 1.0);
}
