# frame_sampling

The `FrameExtractor` sampling strategies side by side: `EveryNth(10)` exports
every 10th decoded frame (index-based), then `EverySec(1.0)` samples one frame
per second (time-based, from presentation timestamps), resized to 160 px wide
and capped with `max_frames(8)`. The sampled thumbnails are pasted side by
side into a single strip image — tightly packed buffers concatenate row by
row with no per-pixel work.

```bash
cargo run --example frame_sampling [input-file]
```

Runs on a synthetic `testsrc2` pattern when no file is given; writes
`thumbnail_strip.ppm`.
