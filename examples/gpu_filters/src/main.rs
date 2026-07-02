//! GPU-accelerated filtering via FFmpeg's native hardware filters.
//!
//! This example demonstrates the recommended "Track A" pattern:
//! 1. Probe what the *linked* FFmpeg build and *this* machine actually support.
//! 2. Pick the best available chain (CUDA → VAAPI → QSV → libplacebo → CPU).
//! 3. Run a scale filter through it, keeping frames in GPU memory when the
//!    decoder/encoder are hardware-backed.
//!
//! Run with: `cargo run [input.mp4]` (defaults to ../../test.mp4)

use ez_ffmpeg::codec::get_encoders;
use ez_ffmpeg::hwaccel::{get_gpu_filter_backends, GpuFilterBackend};
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let input_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "../../test.mp4".to_string());

    // Step 1: probe. The ffmpeg binary in PATH and the libav* your program
    // links are often DIFFERENT builds with different capabilities — always
    // probe the linked build instead of trusting `ffmpeg -filters` output.
    let backends = get_gpu_filter_backends();
    print_capability_report(&backends);

    // Step 2: pick a chain that is fully usable: device creatable on this
    // machine AND filter compiled into the build AND encoder present.
    let chain = pick_chain(&backends);
    println!("\nSelected chain: {}\n", chain.describe());

    // Step 3: run it.
    let (input, filter_desc, video_codec) = chain.build(&input_path);
    let output = Output::from("output_gpu_filters.mp4")
        .set_video_codec(video_codec)
        .set_audio_codec("aac");

    FfmpegContext::builder()
        .input(input)
        .filter_desc(filter_desc)
        .output(output)
        .build()?
        .start()?
        .wait()?;

    println!("Done: output_gpu_filters.mp4 (via {})", chain.describe());
    Ok(())
}

/// The GPU (or CPU-fallback) filter chain this example knows how to build.
enum Chain {
    /// NVIDIA: NVDEC decode → scale_cuda → NVENC encode, frames stay in VRAM.
    Cuda,
    /// Intel/AMD on Linux: VAAPI decode → scale_vaapi → VAAPI encode.
    Vaapi,
    /// Intel Quick Sync: QSV decode → vpp_qsv → QSV encode.
    Qsv,
    /// libplacebo (Vulkan): software decode, filter uploads internally.
    /// Requires a real (non-software) Vulkan device at runtime; the probe
    /// cannot distinguish software Vulkan (llvmpipe/lavapipe) from real GPUs.
    Libplacebo,
    /// No usable GPU backend: plain CPU scale with a probed encoder.
    CpuFallback { encoder: &'static str },
}

impl Chain {
    fn describe(&self) -> String {
        match self {
            Chain::Cuda => "CUDA (NVDEC → scale_cuda → h264_nvenc, zero-copy)".to_string(),
            Chain::Vaapi => "VAAPI (decode → scale_vaapi → h264_vaapi, zero-copy)".to_string(),
            Chain::Qsv => "QSV (decode → vpp_qsv → h264_qsv, zero-copy)".to_string(),
            Chain::Libplacebo => "libplacebo (sw decode → Vulkan filter → libx264)".to_string(),
            Chain::CpuFallback { encoder } => format!("CPU fallback (scale → {encoder})"),
        }
    }

    /// Returns (input, filter_desc, video_codec) for this chain.
    fn build(&self, input_path: &str) -> (Input, &'static str, &'static str) {
        match self {
            Chain::Cuda => (
                Input::from(input_path)
                    .set_hwaccel("cuda")
                    // Keep decoded frames on the GPU so the filter and encoder
                    // consume them without a system-memory round trip.
                    .set_hwaccel_output_format("cuda"),
                "scale_cuda=640:360",
                "h264_nvenc",
            ),
            Chain::Vaapi => (
                Input::from(input_path)
                    .set_hwaccel("vaapi")
                    .set_hwaccel_output_format("vaapi"),
                "scale_vaapi=w=640:h=360",
                "h264_vaapi",
            ),
            Chain::Qsv => (
                Input::from(input_path)
                    .set_hwaccel("qsv")
                    .set_hwaccel_output_format("qsv"),
                "vpp_qsv=w=640:h=360",
                "h264_qsv",
            ),
            Chain::Libplacebo => (
                // Software decode: libplacebo accepts software frames and
                // uploads them to its Vulkan device internally.
                Input::from(input_path),
                "libplacebo=w=640:h=360:format=yuv420p",
                "libx264",
            ),
            Chain::CpuFallback { encoder } => (Input::from(input_path), "scale=640:360", *encoder),
        }
    }
}

fn pick_chain(backends: &[GpuFilterBackend]) -> Chain {
    let usable = |backend_name: &str, filter: &str, encoder: &str| {
        backends
            .iter()
            .any(|b| {
                b.name == backend_name
                    && b.device_available
                    && b.filters.iter().any(|f| f.name == filter && f.present_in_build)
            })
            && encoder_available(encoder)
    };

    // Note: the probe covers device + filter + encoder. The decode side is
    // still runtime-dependent (e.g. NVDEC must support the input codec), so a
    // selected hardware chain can still fail on exotic inputs.
    if usable("cuda", "scale_cuda", "h264_nvenc") {
        Chain::Cuda
    } else if usable("vaapi", "scale_vaapi", "h264_vaapi") {
        Chain::Vaapi
    } else if usable("qsv", "vpp_qsv", "h264_qsv") {
        Chain::Qsv
    } else if usable("vulkan", "libplacebo", "libx264") {
        Chain::Libplacebo
    } else {
        // Even the fallback encoder is probed: LGPL/minimal FFmpeg builds have
        // no libx264. The native mpeg4 encoder is always compiled in.
        let encoder = if encoder_available("libx264") {
            "libx264"
        } else {
            "mpeg4"
        };
        Chain::CpuFallback { encoder }
    }
}

fn encoder_available(name: &str) -> bool {
    get_encoders().iter().any(|c| c.codec_name == name)
}

fn print_capability_report(backends: &[GpuFilterBackend]) {
    println!("GPU filter backends of the LINKED FFmpeg build on THIS machine:");
    if backends.is_empty() {
        println!(
            "  (none — the linked FFmpeg was built without any hardware device support;\n   \
             rebuild FFmpeg with e.g. --enable-vaapi/--enable-vulkan/--enable-libplacebo,\n   \
             or point pkg-config at a full-featured build)"
        );
        return;
    }
    for b in backends {
        let filters_in_build: Vec<&str> = b
            .filters
            .iter()
            .filter(|f| f.present_in_build)
            .map(|f| f.name)
            .collect();
        match (&b.device_error, filters_in_build.is_empty()) {
            (None, false) => println!(
                "  [ok] {:12} filters: {}",
                b.name,
                filters_in_build.join(", ")
            ),
            (None, true) => println!(
                "  [--] {:12} device works, but no GPU filters compiled into this build",
                b.name
            ),
            (Some(err), _) => println!("  [--] {:12} device unavailable: {}", b.name, err),
        }
    }
}
