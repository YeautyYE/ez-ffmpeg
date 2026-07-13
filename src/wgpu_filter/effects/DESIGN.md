# wgpu_filter::effects — design

This module is an adapter layer over `WgpuFrameFilter`.

## Architecture

`effects` adapts one fixed WGSL shader contract into a catalog of named
effects: every effect shares the same `@group(0)` header (input texture /
sampler / the `ez` uniforms — the standard `shader_wgsl` contract) and the
same underlying pipeline (`WgpuFrameFilter`); they differ only in their
`@group(1)` parameter struct and fragment body. Each effect exposes one
constructor (`adjust(params)`, ...) returning a typed builder; the built
`Effect<P>` implements `FrameFilter` by delegation and fixes the parameter
type `P` into the handle-acquisition path.

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
constructors (13) ──1:1──▶ parameter structs (#[repr(C)] Pod)
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
| `soul` | SoulParams (16) | none (second sample point) | — | ghost blend; cycle start = identity; timestamp-driven |
| `sway` | SwayParams (16) | none (inverse mapping) | — | derived overscan keeps sampling inside the frame at every phase; timestamp-driven |
| `wave` | WaveParams (16) | none (inverse mapping) | — | out-of-frame clamps (edge-extends); timestamp-driven phase |
| `swirl` | SwirlParams (32) | none (inverse mapping) | — | quadratic falloff to zero at the rim; `speed > 0` oscillates through identity at `t = 0`; same rotation convention as `transform` |
| `magnifier` | MagnifierParams (16) | none (inverse mapping) | — | smoothstep-eased zoom, seamless rim; identity outside the radius |
| `fisheye` | FisheyeParams (16) | none (inverse mapping) | — | corner-normalized barrel map (strength ≥ 0 only): corners pinned, factor ≤ 1 keeps every sample in frame |
| `chroma_key` | ChromaKeyParams (48) | yes | 9-tap box on the chroma DISTANCE | FFmpeg `chromakey` ramp + `despill` clamp; composites over a solid background (alpha-less output) |

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

6. **Animated effects read `ez.play_time`, not a frame counter.** The
   pipeline derives `play_time` from `pts × time_base`, so animation speed
   is stable across frame rates and seeks; a counter would tie the look to
   the frame rate. Cost: frames without timestamps (`AV_NOPTS_VALUE` maps
   to `play_time = 0`) render the `t = 0` phase — every animated effect is
   designed so that phase is the identity or a mild pose, never a violent
   one.

7. **`chroma_key` follows FFmpeg/OBS reference math, not novelty.** The
   pixel and the key color are both projected to CbCr with FFmpeg
   `chromakey`'s own RGB→UV coefficients, so distances — and therefore
   `similarity`/`blend` values — land on FFmpeg's scale; the distance, not
   the color, is box-filtered over a 3×3 neighborhood (both references do
   this against 4:2:0 chroma blocking); the matte is FFmpeg's linear
   `(d - similarity)/blend` ramp; spill suppression is FFmpeg `despill`'s
   self-gating clamp aimed at the key's dominant channel. One structural
   difference: FFmpeg compares the source's RAW chroma plane codes against
   a BT.601-converted key, while this shader decodes the pixel to RGB and
   projects both sides through the one BT.601 conversion — the two agree
   (within quantization) only on full-range BT.601 input, and every other
   range/matrix combination carries a small bias on FFmpeg's side, so
   ported values are a close starting point, not bit-identical. Keying on
   chroma (not RGB distance) is what makes the
   matte survive shadows and uneven lighting on a physical screen. Because
   the pipeline's output is alpha-less YUV, the matte composites over a
   solid `background` color here; a future alpha-carrying pipeline could
   expose it directly.

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
  validates all 14 assembled shaders (beauty_lite × 2 quality tiers + the
  other 12), so shader-text errors explode in `cargo test` without a real GPU.
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
- **Lens/motion/keying oracles** (same skip rule): every neutral phase is
  pinned to the identity (soul at cycle start, sway with zero amplitudes,
  wave at zero amplitude, animated swirl at `t = 0`, fisheye at zero
  strength); soul's mid-cycle ghost band blends toward the enlarged copy;
  sway's zoom peak moves the step edge outward, its trough passes the
  frame through, and a max-amplitude translation lands the edge where
  only overscan + translation together put it (dropping either term
  fails); wave bends the edge in opposite directions on opposite-phase
  rows, flips both directions half a period later (the travelling time
  term), and displaces a horizontal edge vertically (the second axis);
  swirl rotates inside its radius only, and an animated swirl at its
  oscillation peak matches the static twist; the magnified square covers
  ground outside its input footprint, the outside stays identity, and a
  near-rim probe pins the smoothstep ease-off (constant zoom fails it);
  fisheye pushes an off-center edge outward while both corners keep their
  content (the corner-pinned mapping); chroma_key keys the pure-green
  band to the background, keys a near-green band through the similarity
  tolerance, keeps a skin band untouched, honors a non-black background
  color, blends a mid-ramp teal partway (the linear ramp, not a hard
  threshold), keys a boundary column only its 3×3 box-averaged distance
  reaches (center-only sampling fails it), and its despill measurably
  pulls a green-spilled foreground toward neutral vs `spill = 0`.
- **Timestamp phases** are chosen on the harness's 1/30 time base so each
  oracle drives a known `play_time` (e.g. pts 15 → 0.5 s).
