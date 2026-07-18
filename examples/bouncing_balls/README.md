# ez-ffmpeg Example: Bouncing Balls (VideoWriter)

## Functionality

Runs a small gravity + wall-collision physics simulation and renders each frame
in Rust, pushing them straight into an MP4 with `VideoWriter`. Shows a real
render loop with mutable per-frame state driving the encoder.

## How to Run

1. Navigate to the `examples/bouncing_balls` directory.
2. Run `cargo run`.
3. `bouncing_balls.mp4` is written to the current directory.
