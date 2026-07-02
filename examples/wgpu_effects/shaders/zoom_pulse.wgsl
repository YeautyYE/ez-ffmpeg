// Zoom pulse: periodic soft zoom overlay, driven by playback time.
// WGSL port of the fragment shader from the OpenGL video_effects example.
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let duration = 0.9;
    let max_alpha = 0.1;
    let max_scale = 1.5;

    let progress = (ez.play_time % duration) / duration;
    let alpha = max_alpha * (1.0 - progress);
    let scale = 1.0 + (max_scale - 1.0) * progress;

    let weak = vec2<f32>(0.5 + (tex_coord.x - 0.5) / scale,
                         0.5 + (tex_coord.y - 0.5) / scale);
    let base = textureSample(texture1, sampler1, tex_coord);
    let zoomed = textureSample(texture1, sampler1, weak);
    return mix(base, zoomed, alpha);
}
