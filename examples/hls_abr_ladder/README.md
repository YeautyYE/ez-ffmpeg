# hls_abr_ladder

VOD adaptive-bitrate HLS via `recipes::HlsLadder`. One input decode is split to
N scaled renditions (each its own `hls` output mapped from a filtergraph pad),
keyframe-aligned with a fixed GOP (`g`/`keyint_min` + closed GOP) so segments
are switchable, and stitched by a generated `master.m3u8`.

```bash
cargo run --example hls_abr_ladder        # MPEG-TS segments (default)
cargo run --example hls_abr_ladder fmp4   # fragmented-MP4 segments
```

The `fmp4` variant switches every rendition to fragmented-MP4 HLS: each
rendition directory gets its own `init.mp4` initialization segment (announced
via `EXT-X-MAP`) plus `.m4s` media segments, and the master playlist declares
`#EXT-X-VERSION:7`. One init segment per rendition is the expected layout —
each rendition is its own `hls` muxer.

Expects a **constant-frame-rate** `test.mp4`; writes `hls_out/`. MVP scope: VOD,
single video + single audio, uniform segment duration. See the module docs for
the full scope and limitations (including the HEVC `hev1`/`hvc1` note for
Apple targets).
