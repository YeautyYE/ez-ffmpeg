# thumbnail_recipe

Single-frame thumbnail extraction via the `recipes::thumbnail` helper (vs. the
lower-level `thumbnail_extraction` example that drives the builder directly).
`recipes::sprite_sheet` produces a tiled storyboard from the same options style.

```bash
cargo run --example thumbnail_recipe
```

Expects a `test.mp4`; writes `thumb.jpg`. `At::Sec`/`Frame`/`Percent` choose the
frame; `SeekMode::InputSeek` (fast) vs `FilterScan` (accurate) trade speed for
precision.
