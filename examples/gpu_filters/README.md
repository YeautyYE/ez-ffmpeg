# GPU Filters (FFmpeg native hardware filters)

Demonstrates the recommended pattern for GPU-accelerated filtering:
probe what the **linked** FFmpeg build and **this machine** support, pick the
best chain, degrade gracefully to CPU.

```bash
cargo run                 # uses ../../test.mp4
cargo run /path/to/in.mp4
```

## What it does

1. `ez_ffmpeg::hwaccel::get_gpu_filter_backends()` — per backend (cuda/vaapi/
   qsv/vulkan/opencl): can a device be created here, and which GPU filters are
   compiled into the linked build. Note: the `ffmpeg` binary in `PATH` and the
   `libav*` your program links are often **different builds** — always probe,
   never trust `ffmpeg -filters` output.
2. Picks the first fully usable chain (device + filter + encoder):

   | Chain | Input | filter_desc | Encoder | Frames |
   |---|---|---|---|---|
   | CUDA | `hwaccel=cuda` + `hwaccel_output_format=cuda` | `scale_cuda=640:360` | `h264_nvenc` | stay in VRAM |
   | VAAPI | `hwaccel=vaapi` + `hwaccel_output_format=vaapi` | `scale_vaapi=w=640:h=360` | `h264_vaapi` | stay in VRAM |
   | QSV | `hwaccel=qsv` + `hwaccel_output_format=qsv` | `vpp_qsv=w=640:h=360` | `h264_qsv` | stay in VRAM |
   | libplacebo | software decode | `libplacebo=w=640:h=360:format=yuv420p` | `libx264` | filter uploads internally (Vulkan) |
   | CPU fallback | software decode | `scale=640:360` | `libx264` | system memory |

3. Runs it and writes `output_gpu_filters.mp4`.

## Requirements per chain

- **CUDA**: NVIDIA driver; FFmpeg built with `--enable-ffnvcodec` (or
  `--enable-cuda-llvm` for the `*_cuda` filters); `h264_nvenc` encoder.
- **VAAPI** (Linux Intel/AMD): `mesa-va-drivers` (AMD) or `intel-media-driver`;
  FFmpeg with `--enable-vaapi`.
- **QSV** (Intel): `libvpl`; FFmpeg with `--enable-libvpl`.
- **libplacebo**: FFmpeg with `--enable-libplacebo --enable-vulkan`; a real
  Vulkan device (software rasterizers like llvmpipe are rejected by libplacebo).
  This chain was verified for availability-probing only — end-to-end behavior
  needs a machine with a working Vulkan driver.
- **Custom shaders on GPU**: `libplacebo` also accepts
  `custom_shader_path=<file>` in mpv `.hook` GLSL syntax — see the
  [FFmpeg libplacebo filter docs](https://ffmpeg.org/ffmpeg-filters.html#libplacebo).

If none of the above are available the example still works via the CPU chain —
the point is that *your* code can degrade the same way instead of failing.
