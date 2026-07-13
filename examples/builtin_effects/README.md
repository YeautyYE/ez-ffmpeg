# Built-in effects (typed, no WGSL)

Applies a named effect from `ez_ffmpeg::wgpu_filter::effects` to a video:

```bash
cargo run -- adjust               # uses ../../test.mp4
cargo run -- chroma_key green.mp4 # green-screen extraction
cargo run -- magnifier input.mp4  # lens sweeps live while processing
```

| Effect | Demonstrates |
|---|---|
| `adjust` | color grading (8 controls in one pass) |
| `portrait` | fused beautify preset (smooth + whiten + brighten) |
| `sharpen` | luma unsharp mask |
| `soft_blur` | privacy blur preset |
| `pixelate` | mosaic blocks |
| `mirror` | `transform` selfie flip |
| `soul` | ghost copy drifting outward while fading (timestamp-driven) |
| `sway` | handheld camera preset (no black borders at any phase) |
| `wave` | travelling sine ripple |
| `swirl` | `tornado()` oscillating vortex |
| `magnifier` | circular lens + live `params_handle()` sweeping it |
| `fisheye` | barrel distortion (corners stay pinned) |
| `chroma_key` | green-screen keying + despill over a solid background |

Compared to the `wgpu_effects` example (hand-written WGSL against the raw
`WgpuFrameFilter` contract), this one stays entirely in safe typed Rust:
constructors take `#[repr(C)]` parameter structs, out-of-range values clamp
in the shader, and live updates go through the typed handle — no uniform
layouts to keep in sync.

Notes:

- Every parameter struct's `Default` is a neutral-or-mild preset; the values
  in `main.rs` are deliberately visible ones.
- Animated effects (`soul`, `sway`, `wave`, `swirl`) run off frame
  timestamps, so their speed is stable regardless of frame rate.
- `chroma_key` follows FFmpeg `chromakey` parameter semantics
  (`similarity`/`blend` on FFmpeg's chroma-distance scale) plus FFmpeg
  `despill`-style spill suppression; the output pipeline carries no alpha,
  so the foreground is composited over a configurable solid background.
- Requires any Vulkan/Metal/DX12 device; works headless (no display needed).
