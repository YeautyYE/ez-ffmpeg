# extract_rgb_frames

Decode a video into owned, tightly packed RGB24 frames with `FrameExtractor`
and save the first five as PPM images. `VideoFrame::as_bytes()` has no row
padding (`width * height * 3` bytes, top-down), so a binary PPM is just a text
header plus the buffer as-is — no image crate needed. The same buffer feeds an
ndarray view or a tensor directly.

```bash
cargo run --example extract_rgb_frames [input-file]
```

Runs on a synthetic `testsrc2` pattern when no file is given; writes
`frame_000.ppm` … `frame_004.ppm` and prints each frame's size, layout and
presentation timestamp.
