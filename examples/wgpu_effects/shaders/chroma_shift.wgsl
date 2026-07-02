// Chromatic aberration: shifts the red and blue channels radially.
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let dir = tex_coord - vec2<f32>(0.5);
    let shift = dir * 0.006;
    let r = textureSample(texture1, sampler1, tex_coord + shift).r;
    let g = textureSample(texture1, sampler1, tex_coord).g;
    let b = textureSample(texture1, sampler1, tex_coord - shift).b;
    return vec4<f32>(r, g, b, 1.0);
}
