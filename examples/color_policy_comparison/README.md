# color_policy_comparison

Why `ColorPolicy::Tagged` is the `FrameExtractor` default. The same first
frame of a BT.709-tagged YUV test pattern is converted to RGB three times —
`Tagged` (honor the frame's colorspace tags), `Force(BT.709/limited)`, and
`Force(BT.601/limited)` — and the byte-level differences are printed.

`Tagged` and `Force(BT.709)` produce identical buffers, proving the tag was
honored. `Force(BT.601)` — the matrix many decode-to-RGB pipelines silently
assume for all content — shifts colors across the whole frame, by up to
dozens of code values on saturated regions. That is the error that quietly
skews colors in ML training data and
thumbnails built from HD sources; untagged sources fall back to swscale's
documented BT.601/limited default under `Tagged`, and `Force` pins one
interpretation when you know better than the tags.

```bash
cargo run --example color_policy_comparison
```

Self-contained (uses a deterministic lavfi source); prints max/mean per-byte
diffs and the share of changed pixels for each pairing.
