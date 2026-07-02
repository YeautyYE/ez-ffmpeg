// Grayscale (Rec.601 luma weights).
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let color = textureSample(texture1, sampler1, tex_coord);
    let gray = dot(color.rgb, vec3<f32>(0.299, 0.587, 0.114));
    return vec4<f32>(vec3<f32>(gray), 1.0);
}
