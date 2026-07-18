# ez-ffmpeg Example: Frames to Video (VideoWriter)

## Functionality

Renders a procedural "plasma" animation in Rust and pushes each RGBA frame
straight into an encoded MP4 with [`VideoWriter`], without any intermediate
files or an `ffmpeg` subprocess.

## Equivalent FFmpeg Command

```bash
ffmpeg -f rawvideo -pix_fmt rgba -s 640x360 -r 30 -i - -c:v mpeg4 -q:v 5 plasma.mp4
```

## How to Run

1. Navigate to the `examples/frames_to_video` directory.
2. Run `cargo run` to execute the code.
3. `plasma.mp4` is written to the current directory.

## Code Explanation

- `VideoWriter::builder(width, height).fps(...).open(output)` starts a pipeline
  whose destination is an ordinary `Output`, so any codec, container, or filter
  works.
- `writer.frame_size()` returns the exact number of bytes one tightly-packed
  frame must contain.
- `write(&frame)` copies a borrowed buffer (so it can be reused); `write_owned`
  moves a `Vec` in, skipping that borrow-copy.
- `finish()` drains the encoder, finalizes the container, and returns the first
  pipeline error.
