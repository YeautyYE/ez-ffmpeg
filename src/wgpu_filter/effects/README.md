# wgpu_filter::effects

A catalog of named, built-in GPU effects: each effect is a typed constructor
returning an [`Effect<P>`](mod.rs) bound to its parameter type, ready to drop
into a frame pipeline, with live parameter updates through a typed handle.

## Purpose

Turn the common live-streaming / short-video effects into library API: callers
write no WGSL, never touch uniform layouts, express parameters as plain Rust
structs, and get a pipeline-ready `FrameFilter` from a one-line constructor.

### Non-goals

- No multi-effect fusion into a single instance (each `Effect` owns a full GPU
  pipeline; see "Caveats")
- No face-detection / segmentation-grade beautification (`beauty_lite` is the
  heuristic-skin-mask tier — the name states the limit)
- No runtime kernel-size changes (`BeautyQuality` is baked into the shader at
  build time)

## Quick start

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{adjust, AdjustParams};
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::AVMediaType;

let effect = adjust(AdjustParams {
    saturation: 1.2,
    temperature: 0.15,
    ..AdjustParams::default()
})
.build()?;

let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
let pipeline = pipeline.filter("adjust", Box::new(effect));
// output.add_frame_pipeline(pipeline);
```

Every parameter struct's `Default` is a neutral or gentle preset: a
default-constructed effect does not visibly change the image.

## Examples

**Live-stream beautification (Fast tier for integrated GPUs)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{beauty_lite, BeautyParams, BeautyQuality};

let effect = beauty_lite(BeautyParams::default())
    .quality(BeautyQuality::Fast)   // 9 taps; integrated-GPU 1080p60
    .frames_in_flight(1)            // low latency for live streaming
    .build()?;
```

Or use the fused preset `portrait()` (smoothing + whitening + brightening in
one step).

**Live parameter updates** (from any thread):

```rust,ignore
let effect = adjust(AdjustParams::default()).build()?;
let params = effect.params_handle(); // type already bound, no turbofish
// ...effect installed in a pipeline and running...
params.update(|p| p.saturation = 0.0); // atomic read-modify-write, next frame
```

**Privacy blur + downscaled output**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{soft_blur, SoftBlurParams};

let effect = soft_blur(SoftBlurParams::privacy())
    .output_size(640, 360)
    .build()?;
```

**Mirror (selfie flip)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{transform, TransformParams};

let effect = transform(TransformParams::mirrored()).build()?;
```

**Green-screen extraction (chroma key)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{chroma_key, ChromaKeyParams};

// FFmpeg `chromakey` semantics: similarity/blend on FFmpeg's chroma
// distance scale; despill clamp on the surviving foreground. The
// pipeline outputs alpha-less YUV, so the foreground composites over
// a solid background color (default black).
let effect = chroma_key(ChromaKeyParams::green_screen()).build()?;
// or: ChromaKeyParams::blue_screen(), .with_background(r, g, b)
```

**Fun lens looks (soul / magnifier / fisheye)**:

```rust,ignore
use ez_ffmpeg::wgpu_filter::effects::{soul, SoulParams, magnifier, MagnifierParams};

let ghost = soul(SoulParams::default()).build()?;        // astral projection
let lens = magnifier(MagnifierParams::default()).build()?; // move it live via params_handle()
```

Catalog overview: `adjust` (brightness/contrast/saturation/exposure/gamma/
vibrance/white-balance, 8 controls), `beauty_lite`/`portrait` (skin smoothing
+ whitening + brightening), `sharpen` (luma unsharp mask), `transform`
(mirror/flip/rotate/scale/translate), `pixelate` (mosaic), `soft_blur`
(soft-focus / privacy blur), `chroma_key` (green/blue-screen extraction with
despill), and the lens/motion family — `soul` (fading ghost copy), `sway`
(handheld / breathing camera), `wave` (travelling sine ripple), `swirl`
(vortex, optionally oscillating), `magnifier` (circular lens zoom), `fisheye`
(barrel distortion). The animated ones (`soul`, `sway`, `wave`,
animated `swirl`) run off frame timestamps (`pts` × `time_base`); frames
without timestamps render their `t = 0` phase.

## Dependencies

- **Upstream**: `wgpu_filter::wgpu_frame_filter` (the underlying GPU pipeline
  and builder), `wgpu_filter::params` (`WgpuParamsHandle` live parameters),
  `wgpu_filter::error` (`WgpuFilterError`), `core::filter::frame_filter`
  (the `FrameFilter` trait, implemented by delegation), `bytemuck` (Pod
  derives for the parameter structs)
- **Downstream**: no in-crate callers (public API for crate users)
- **External**: enabled by the `wgpu` feature; needs a wgpu-capable GPU
  adapter at runtime

## Caveats

- **One effect = one full GPU pipeline** (upload/convert/effect/pack/readback).
  Chaining two `Effect`s doubles that cost. Prefer a single effect with wide
  parameter coverage (`adjust` has eight controls, `portrait` is fused) over
  stacking instances.
- **Out-of-range parameters do not error**: every parameter is clamped inside
  the shader to its documented range; `build()` does no value validation —
  passing `saturation: 99.0` behaves as the clamped 3.0.
- **`_pad` fields must stay 0**: the parameter structs' padding fields are
  `#[doc(hidden)] pub` (to keep `..Default::default()` working); do not write
  to them.
- **Never call the same effect's handle methods inside
  `params_handle().update(|p| ...)`** — the lock is held across the closure;
  re-entry deadlocks.
- **`BeautyQuality` is build-time only**: Fast = 9 taps (integrated-GPU
  1080p60), Balanced = 13 taps (discrete GPU or 720p). A runtime tap count
  would still schedule the GPU wavefront for the full loop, so it is not
  offered.
- **No GPU present**: `build()` is pure CPU work (shader assembly +
  validation) and never touches the GPU; device creation happens at pipeline
  `init`, which is where a missing adapter surfaces.
