# keyframe_thumbnails

Fast thumbnails with `Sampling::KeyframesOnly`: the extractor pins the decoder
option `skip_frame=nokey`, so non-key frames are skipped at decode time instead
of being decoded and discarded — on long-GOP content that is one decoded frame
per GOP. The same fast path as the encoder-based recipe in
`examples/thumbnail_extraction` (Example 3), but each keyframe arrives as an
in-memory RGB frame with its timestamp instead of a JPEG file, ready for a
scrub bar, a contact sheet, or a vision model.

```bash
cargo run --example keyframe_thumbnails [input-file]
```

When no file is given, generates `keyframe_source.mp4` (12 s, GOP capped so a
keyframe lands at least once per second) first; writes one `keyframe_NNN.ppm`
per keyframe.
