# wgpu_filter::effects — design

This module is an adapter layer over `WgpuFrameFilter`.

## Architecture

`effects` adapts one fixed WGSL shader contract into six named effects: every
effect shares the same `@group(0)` header (input texture / sampler / the `ez`
uniforms — the standard `shader_wgsl` contract) and the same underlying
pipeline (`WgpuFrameFilter`); they differ only in their `@group(1)` parameter
struct and fragment body. Each effect exposes one constructor
(`adjust(params)`, ...) returning a typed builder; the built `Effect<P>`
implements `FrameFilter` by delegation and fixes the parameter type `P` into
the handle-acquisition path.

### Static layering

```text
┌─────────────────────────────────────────────────────────┐
│  Caller (frame pipeline)                                │
│      ↓ constructors: adjust / beauty_lite / sharpen ... │
├─────────────────────────────────────────────────────────┤
│  Typed builder layer                                    │
│    EffectBuilder<P> (shared passthrough knobs)          │
│    BeautyBuilder (adds the build-time quality choice)   │
│      ↓ shader assembly (EFFECT_HEADER + effect body)    │
├─────────────────────────────────────────────────────────┤
│  Effect<P> (FrameFilter delegation + typed handle)      │
│      ↓ forwards everything                              │
├─────────────────────────────────────────────────────────┤
│  wgpu_frame_filter (GPU pipeline; untouched here)       │
└─────────────────────────────────────────────────────────┘
```

### Entity relations

```text
constructors (6) ──1:1──▶ parameter structs (#[repr(C)] Pod)
      │                        │ positional mirror (field order = layout)
      ▼                        ▼
EffectBuilder<P> ──build──▶ Effect<P> ──1:1──▶ WgpuFrameFilter (embedded)
                               │
                               └──N──▶ WgpuParamsHandle<P> (live updates, any thread)
```

- **Constructor** — the single entry point of an effect; binds the preset
  parameters and the matching WGSL body into the builder
- **Parameter struct** — all-scalar (f32/u32), a multiple of 16 bytes,
  mirroring the WGSL uniform struct position by position
- **Effect<P>** — the parameter type is fixed at build; acquiring the handle
  needs no turbofish and cannot be mis-matched with a same-sized foreign
  struct

The type parameter `P` is what carries the constructor-to-handle mapping: it
moves the "parameter struct ↔ shader uniform" correspondence from a runtime
check (byte-count comparison) to a compile-time one (type binding).

## Contract matrix

| Effect | Params (bytes) | Neighborhood sampling | Kernel | Notes |
|---|---|---|---|---|
| `adjust` | AdjustParams (32) | none (per-pixel) | — | 8 controls in one pass |
| `beauty_lite` / `portrait` | BeautyParams (16) | yes | Fast=9 / Balanced=13 taps (baked at build) | skin-mask gated; `portrait` is a stronger preset |
| `sharpen` | SharpenParams (16) | yes | 5 taps | soft-threshold noise gate |
| `transform` | TransformParams (32) | none (inverse mapping) | — | out-of-frame renders black; positive angle = counterclockwise for the viewer |
| `pixelate` | PixelateParams (16) | none (block-center sample) | — | block clipped to the frame, then its center sampled; `block_size <= 1` degrades to passthrough |
| `soft_blur` | SoftBlurParams (16) | yes | 13-tap two-ring disc | `privacy()` strong preset |

Contracts shared across the matrix:

- Neighborhood step sizes always come from `textureDimensions(texture1)` (the
  input size); `ez.width/height` is the OUTPUT size — when resizing the two
  differ, and using the wrong one makes kernel spacing drift with the output
  scale.
- Parameters are clamped inside the shader to their documented ranges; the
  host side neither validates nor errors.
- The WGSL struct mirrors the host struct BY FIELD POSITION (all scalars, no
  implicit padding on either side; the bytemuck derive proves at compile time
  the host side has no padding bytes). Field names may differ — `smooth` is a
  WGSL reserved word, so the shader side renames it `smoothing`.

## Key decisions and trade-offs

1. **Typed builders, not one big enum.** The alternative was an `EffectKind`
   enum plus a unified `EffectConfig`, but that forces heterogeneous
   parameters into one type, couples version evolution across effects, and
   type-erases the live handle. The cost is one constructor and one parameter
   struct per effect — accepted.

2. **`BeautyQuality` is baked into the shader at build; no runtime tap
   count.** GPU wavefronts schedule a dynamic loop bound at its maximum
   anyway, so a runtime parameter saves no compute; two quality tiers are
   simply two shader source texts.

3. **Out-of-range parameters clamp rather than error.** In live-tuning, one
   `update` with an out-of-range value failing the pipeline costs far more
   than a briefly clamped image; `build()` therefore does zero value
   validation (size/alignment is still validated by the underlying builder).

4. **Padding fields are `#[doc(hidden)] pub`.** Private padding breaks the
   `..Default::default()` functional-update syntax (E0451); `#[non_exhaustive]`
   breaks it too. Public-but-hidden is the only option that keeps the syntax;
   the cost is that callers can theoretically write to padding (the shader
   never reads it — harmless).

5. **`Effect::params_handle` uses `expect` instead of returning `Result`.**
   Invariant: the only construction path for `Effect<P>` is
   `EffectBuilder::build`, which always initializes the underlying parameters
   with `size_of::<P>()` bytes; `SharedParams.len` never changes after
   construction. The size-mismatch branch is unreachable, and infecting the
   caller's signature with an unreachable error is not worth it.

## Failure contract

- **`build()`** → `WgpuFilterError::InvalidOption`. Through this module's API
  the only reachable failures are the underlying builder's generic checks
  (e.g. `frames_in_flight` out of range); the shader and parameter size are
  constructed by this module and always valid.
- **Pipeline `init`** → no GPU adapter / device-creation failure bubbles up
  as `FrameFilterError` (identical to a hand-written-shader
  `WgpuFrameFilter`; this module adds zero new failure modes).
- **Runtime** → over-limit frame sizes, unsupported pixel formats, etc. are
  reported by the underlying pipeline, as above.
- **`params_handle` / `update`** → cannot fail; on lock poisoning the byte
  buffer is adopted as-is (it is always in a valid state). The re-entry
  deadlock is a documented prohibition, not a runtime check.

## Concurrency model

The parameter channel is `Arc<Mutex<Vec<u8>>>` plus an `AtomicBool` dirty
flag (defined in the upstream `params` module; this module only consumes it).
`set` replaces the whole struct; `update` does its read-modify-write inside
the lock, so concurrent `set`/`update` serialize without losing writes.
`Effect<P>`'s `FrameFilter` implementation requires `P: Send` (the handle may
be moved to other threads). This module spawns no threads and holds no
long-lived locks.

## Test strategy

- **Offline shader validation**: naga (same major version as wgpu) parses and
  validates all 7 assembled shaders (beauty_lite × 2 quality tiers + the
  other 5), so shader-text errors explode in `cargo test` without a real GPU.
- **Layout sentinels**: each parameter struct's `size_of` is pinned to a
  multiple of 16 (WGSL rounds uniform struct sizes up to 16; the host must
  match).
- **GPU runtime oracles** (skipped without an adapter): neutral parameters vs
  the identity pipeline within 1–2 code values per plane; mirror swaps edges;
  positive rotation lands the bright edge on top (direction contract);
  pixelate flattens blocks and an over-sized block samples the frame center;
  `block_size = 1` plus resize matches an identity resize; sharpen overshoots
  at edges; soft_blur shrinks edge contrast; beauty_lite keeps constant
  frames uniform and cuts skin-region luma noise; `update` desaturates live.
