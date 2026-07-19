# uniform_thumbnails

Exactly N frames, evenly spread by presentation time, with
`Sampling::UniformN` — the fixed frame budget VLM/CLIP-style pipelines expect.
Selection happens pre-filtergraph: frames that lose the grid race skip the
scaler entirely (after the grid completes, the remaining tail still traverses
the graph to drive termination). The example pulls exactly 12 thumbnails from
a 10-second clip into a 4x3 contact sheet, then trims the same clip to 0.2 s
and still requests 8: when the window holds fewer distinct frames than
requested, nearby frames repeat (each repeat keeps its source frame's
`pts_us`), so downstream batch shapes never change. When the duration cannot
be probed (common for live or piped inputs), supply it with
`duration_hint_us()`.

```bash
cargo run --example uniform_thumbnails [input-file]
```

When no file is given, generates `uniform_source.mp4` first; writes
`uniform_grid.ppm` and prints each selected frame's timestamp (the sampling
targets are evenly spaced; each target returns the frame displayed at that
time — the latest source frame at or before it).
