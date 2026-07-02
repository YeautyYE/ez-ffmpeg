// Brightness/contrast/saturation with live-updatable parameters
// (@group(1) is fed through WgpuParamsHandle from the application).
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

struct AdjustParams {
    brightness: f32, // -1.0 .. 1.0, 0 = unchanged
    contrast: f32,   //  0.0 .. 2.0, 1 = unchanged
    saturation: f32, //  0.0 .. 2.0, 1 = unchanged
    _pad: f32,
};
@group(1) @binding(0) var<uniform> params: AdjustParams;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    var rgb = textureSample(texture1, sampler1, tex_coord).rgb;
    rgb = (rgb - 0.5) * params.contrast + 0.5 + params.brightness;
    let gray = vec3<f32>(dot(rgb, vec3<f32>(0.299, 0.587, 0.114)));
    rgb = mix(gray, rgb, params.saturation);
    return vec4<f32>(clamp(rgb, vec3<f32>(0.0), vec3<f32>(1.0)), 1.0);
}
