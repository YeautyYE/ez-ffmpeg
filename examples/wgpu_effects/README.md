# wgpu Effects (WGSL shader templates)

Copy-paste WGSL templates for `WgpuFrameFilter` and a runner that applies them:

```bash
cargo run -- grayscale            # uses ../../test.mp4
cargo run -- adjust input.mp4     # live parameter updates while running
```

| Effect | Demonstrates |
|---|---|
| `grayscale` | minimal shader contract |
| `vignette` | `ez.width`/`ez.height` uniforms (aspect-aware) |
| `zoom_pulse` | `ez.play_time` animation (port of the OpenGL example) |
| `chroma_shift` | multi-tap sampling |
| `adjust` | user params via `@group(1)` + `WgpuParamsHandle` live updates |

## Shader contract

A full WGSL module with this entry point and these bindings:

```wgsl
@group(0) @binding(0) var texture1: texture_2d<f32>;   // converted RGBA input
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;
// optional user params:
// @group(1) @binding(0) var<uniform> params: YourPodStruct;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> { ... }
```

- `tex_coord` origin is the image top-left; `width`/`height` are the OUTPUT size.
- Input frames: YUV420P / YUV422P / YUV444P (plus full-range J variants) and
  NV12 (CPU frames; hardware frames are rejected with guidance). Output
  frames: YUV420P (4:2:2/4:4:4 inputs are chroma-downsampled on the GPU).
- YUV<->RGB runs on the GPU with the correct matrix (BT.601/709 by frame
  metadata, limited/full range) — unlike the OpenGL filter, which used
  swscale's BT.601 default on the CPU.
- By default two frames are in flight (GPU work overlaps the next frame's
  CPU work; output order is preserved). Use
  `builder().frames_in_flight(1)` for strictly synchronous, lowest-latency
  behavior.
- Requires any Vulkan/Metal/DX12 device; works headless (no display needed).
