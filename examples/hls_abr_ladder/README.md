# hls_abr_ladder

VOD adaptive-bitrate HLS via `recipes::HlsLadder`. One input decode is split to
N scaled renditions (each its own `hls` output mapped from a filtergraph pad),
keyframe-aligned with a fixed GOP (`g`/`keyint_min` + closed GOP) so segments
are switchable, and stitched by a generated `master.m3u8`.

```bash
cargo run --example hls_abr_ladder
```

Expects a **constant-frame-rate** `test.mp4`; writes `hls_out/`. MVP scope: VOD,
single video + single audio, uniform segment duration. See the module docs for
the full scope and limitations.
