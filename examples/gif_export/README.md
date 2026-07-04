# gif_export

High-quality animated GIF export via `recipes::animated_gif`. Wraps the
`fps,scale,split[a][b];[a]palettegen[p];[b][p]paletteuse` filtergraph and the
`gif` muxer behind a small `GifOptions` struct (fps, width, trim, dither,
`max_colors`, loop count).

```bash
cargo run --example gif_export
```

Expects a `test.mp4`; writes `output.gif`.
