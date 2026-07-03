# ez-ffmpeg Example: Applying Video Effects using OpenGL Shaders

This example demonstrates how to apply custom OpenGL-based video effects using a fragment shader on video frames in the FFmpeg pipeline. It simplifies applying OpenGL shaders to video frames without manually handling the OpenGL context.

> **Deprecation note**: the OpenGL filter path is deprecated (but still fully
> functional) in favor of the wgpu path. This example is kept for users who
> still rely on the `opengl` feature. The same effect implemented with
> `WgpuFrameFilter` lives in the `wgpu_effects` example (`zoom_pulse`), and the
> `ez_ffmpeg::opengl` module docs contain a GLSL → WGSL migration mapping.

## Key Concepts

The main objective of this example is to show how to use the OpenGL filter to apply video effects during video processing. This is done using:

- **OpenGL Fragment Shaders**: These shaders allow you to define custom effects that will be applied to each video frame.
- **Frame Pipeline**: The `FramePipelineBuilder` is used to define the processing steps that video frames will go through before encoding.

### Prerequisites

To use the `OpenGLFrameFilter`, you must enable the `opengl` feature in your `Cargo.toml` file.

```toml
[dependencies.ez-ffmpeg]
version = "x.y.z"
features = ["opengl"]
```

The OpenGL path also needs a display connection (it is not headless); on Linux
that means a running X11/Wayland session.

## Run

```bash
cargo run
```

Reads `../../test.mp4` and writes `output.mp4` with a periodic "zoom pulse"
effect (see `resource/fragment.glsl`) applied to every frame.
