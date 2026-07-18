# ez-ffmpeg Example: Bouncing Balls (VideoWriter)

## Functionality

Runs a small gravity + wall-collision physics simulation and renders each frame
in Rust, pushing them straight into an MP4 with `VideoWriter`. Shows a real
render loop with mutable per-frame state driving the encoder.

## How to Run

1. From the repository root, run `cargo run --example bouncing_balls`.
2. `bouncing_balls.mp4` is written to the current directory.
