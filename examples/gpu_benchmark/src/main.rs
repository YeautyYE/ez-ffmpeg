//! Phase 0 benchmark harness for the GPU filter roadmap (ADR-0001).
//!
//! Measures wall time, throughput (fps) and process CPU utilisation of the
//! same transcode job through different filter paths, skipping legs whose
//! prerequisites are missing on this machine/build:
//!
//! - cpu_null           decode → null filter → libx264       (pipeline floor)
//! - cpu_hue            decode → hue=s=0 → libx264           (CPU filter cost)
//! - opengl_identity    decode → OpenGLFrameFilter(identity) → libx264
//! - opengl_effect      decode → OpenGLFrameFilter(zoom-pulse) → libx264
//! - libplacebo_noop    decode → libplacebo(fmt only) → libx264  (GPU round trip)
//! - libplacebo_scale   decode → libplacebo scale 720p → libx264
//! - vaapi_scale        VAAPI decode → scale_vaapi 720p → h264_vaapi
//!                      (full zero-copy HW pipeline — NOTE: different encoder,
//!                       so compare it against whole-job wall time, not filter cost)
//!
//! Usage: `cargo run --release -- <input.mp4> [runs_per_leg] [x264|null]`
//! - `x264` (default): every leg re-encodes with libx264 preset=ultrafast
//!   (unless stated otherwise) — measures whole-job throughput.
//! - `null`: discards frames via the null muxer + wrapped_avframe — removes
//!   encoder cost so leg differences isolate decode+filter cost.

use ez_ffmpeg::codec::get_encoders;
use ez_ffmpeg::hwaccel::{get_gpu_filter_backends, GpuFilterBackend};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::time::Instant;

const SHADER_IDENTITY: &str = r##"
    #version 330 core
    in vec2 TexCoord;
    out vec4 FragColor;
    uniform sampler2D texture1;
    void main() { FragColor = texture(texture1, TexCoord); }
"##;

const SHADER_EFFECT: &str = r##"
    #version 330 core
    in vec2 TexCoord;
    out vec4 FragColor;
    uniform sampler2D texture1;
    uniform float playTime;
    void main() {
        float progress = mod(playTime, 0.9) / 0.9;
        float scale = 1.0 + 0.5 * progress;
        vec2 weak = vec2(0.5 + (TexCoord.x - 0.5) / scale,
                         0.5 + (TexCoord.y - 0.5) / scale);
        FragColor = mix(texture(texture1, TexCoord), texture(texture1, weak), 0.1 * (1.0 - progress));
    }
"##;

struct LegResult {
    name: &'static str,
    wall_secs: Vec<f64>,
    cpu_secs: Vec<f64>,
    error: Option<String>,
}

fn main() {
    env_logger::init();
    let input = std::env::args()
        .nth(1)
        .expect("usage: gpu_benchmark <input.mp4> [runs_per_leg]");
    let runs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let null_out = std::env::args().nth(3).as_deref() == Some("null");

    let frames = probe_frame_count(&input);
    println!(
        "input: {input} ({frames} video frames), {runs} runs per leg + 1 warmup, output={}\n",
        if null_out { "null (no encode)" } else { "libx264 ultrafast" }
    );

    let backends = get_gpu_filter_backends();
    let has = |backend: &str, filter: &str| {
        backends.iter().any(|b: &GpuFilterBackend| {
            b.name == backend
                && b.device_available
                && b.filters.iter().any(|f| f.name == filter && f.present_in_build)
        })
    };
    let enc = |name: &str| get_encoders().iter().any(|c| c.codec_name == name);

    let mut results: Vec<LegResult> = Vec::new();

    results.push(run_leg("cpu_null", runs, || filter_desc_job(&input, "null", null_out)));
    results.push(run_leg("cpu_hue", runs, || filter_desc_job(&input, "hue=s=0", null_out)));

    for (name, shader) in [("opengl_identity", SHADER_IDENTITY), ("opengl_effect", SHADER_EFFECT)] {
        results.push(run_leg(name, runs, || opengl_job(&input, shader, null_out)));
    }

    for (name, shader) in [
        ("wgpu_identity", None),
        ("wgpu_effect", Some(WGSL_EFFECT)),
    ] {
        results.push(run_leg(name, runs, || wgpu_job(&input, shader, null_out)));
    }

    if has("vulkan", "libplacebo") && (null_out || enc("libx264")) {
        results.push(run_leg("libplacebo_noop", runs, || {
            filter_desc_job(&input, "libplacebo=format=yuv420p", null_out)
        }));
        results.push(run_leg("libplacebo_scale", runs, || {
            filter_desc_job(&input, "libplacebo=w=1280:h=720:format=yuv420p", null_out)
        }));
    } else {
        results.push(skipped("libplacebo_noop", "vulkan device or libplacebo filter unavailable"));
        results.push(skipped("libplacebo_scale", "vulkan device or libplacebo filter unavailable"));
    }

    if has("vaapi", "scale_vaapi") && (null_out || enc("h264_vaapi")) {
        results.push(run_leg("vaapi_scale", runs, || vaapi_job(&input, null_out)));
    } else {
        results.push(skipped("vaapi_scale", "vaapi device/filter/encoder unavailable"));
    }

    print_report(&results, frames);
}

/// x264 mode: encode to a discarded mp4. null mode: wrapped_avframe into the
/// null muxer — no encoding, isolating decode+filter cost.
fn make_output(null_out: bool) -> Output {
    if null_out {
        ez_ffmpeg::core::context::null_output::create_null_output()
            .set_format("null")
            .set_video_codec("wrapped_avframe")
    } else {
        Output::from("/tmp/gpu_benchmark_out.mp4")
            .set_video_codec("libx264")
            .set_video_codec_opt("preset", "ultrafast")
            .set_audio_codec("aac")
    }
}

fn filter_desc_job(input: &str, filter: &'static str, null_out: bool) -> Result<(), String> {
    FfmpegContext::builder()
        .input(Input::from(input))
        .filter_desc(filter)
        .output(make_output(null_out))
        .build()
        .map_err(|e| format!("build: {e}"))?
        .start()
        .map_err(|e| format!("start: {e}"))?
        .wait()
        .map_err(|e| format!("wait: {e}"))
}

/// Minimal WGSL identity shader matching the library's user-shader contract.
const IDENTITY_WGSL: &str = r#"
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms {
    play_time: f32,
    width: f32,
    height: f32,
    _pad: f32,
};
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    return textureSample(texture1, sampler1, tex_coord);
}
"#;

/// WGSL port of the zoom-pulse effect used by the OpenGL legs.
const WGSL_EFFECT: &str = r#"
@group(0) @binding(0) var texture1: texture_2d<f32>;
@group(0) @binding(1) var sampler1: sampler;
struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
@group(0) @binding(2) var<uniform> ez: EzUniforms;

@fragment
fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
    let progress = (ez.play_time % 0.9) / 0.9;
    let scale = 1.0 + 0.5 * progress;
    let weak = vec2<f32>(0.5 + (tex_coord.x - 0.5) / scale,
                         0.5 + (tex_coord.y - 0.5) / scale);
    let base = textureSample(texture1, sampler1, tex_coord);
    let zoomed = textureSample(texture1, sampler1, weak);
    return mix(base, zoomed, 0.1 * (1.0 - progress));
}
"#;

fn wgpu_job(input: &str, shader: Option<&str>, null_out: bool) -> Result<(), String> {
    use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
    use ez_ffmpeg::wgpu_filter::WgpuFrameFilter;
    use ez_ffmpeg::AVMediaType;

    // EZ_BENCH_WGPU_FIF=1 forces synchronous readback for A/B comparison
    // against the default overlapped mode (2 frames in flight).
    let frames_in_flight: usize = std::env::var("EZ_BENCH_WGPU_FIF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let filter = WgpuFrameFilter::builder()
        .shader_wgsl(shader.unwrap_or(IDENTITY_WGSL))
        .frames_in_flight(frames_in_flight)
        .build()
        .map_err(|e| format!("wgpu init: {e}"))?;
    let stats = filter.stats_handle();
    let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
    let pipeline = pipeline.filter("bench", Box::new(filter));

    let output = make_output(null_out).add_frame_pipeline(pipeline);

    let result = FfmpegContext::builder()
        .input(Input::from(input))
        .output(output)
        .build()
        .map_err(|e| format!("build: {e}"))?
        .start()
        .map_err(|e| format!("start: {e}"))?
        .wait()
        .map_err(|e| format!("wait: {e}"));

    // Per-stage attribution measured inside the filter: `gpu_wait` is the
    // time the pipeline thread was blocked on GPU results, the quantity the
    // overlapped mode is designed to hide.
    if let Ok(s) = stats.lock() {
        if s.frames > 0 {
            let per = |v: f64| v * 1e3 / s.frames as f64;
            println!(
                "  wgpu-stats[fif={}]: frames={} upload={:.2}ms gpu_wait={:.2}ms download={:.2}ms per frame",
                frames_in_flight,
                s.frames,
                per(s.upload_secs),
                per(s.gpu_secs),
                per(s.download_secs),
            );
        }
    }
    result
}

// The benchmark intentionally measures the deprecated OpenGL path as the
// comparison baseline for the wgpu legs.
#[allow(deprecated)]
fn opengl_job(input: &str, shader: &str, null_out: bool) -> Result<(), String> {
    use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
    use ez_ffmpeg::opengl::opengl_frame_filter::OpenGLFrameFilter;
    use ez_ffmpeg::AVMediaType;

    let filter = OpenGLFrameFilter::new_simple(shader.to_string())
        .map_err(|e| format!("opengl init: {e}"))?;
    let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
    let pipeline = pipeline.filter("bench", Box::new(filter));

    let output = make_output(null_out).add_frame_pipeline(pipeline);

    FfmpegContext::builder()
        .input(Input::from(input))
        .output(output)
        .build()
        .map_err(|e| format!("build: {e}"))?
        .start()
        .map_err(|e| format!("start: {e}"))?
        .wait()
        .map_err(|e| format!("wait: {e}"))
}

fn vaapi_job(input: &str, null_out: bool) -> Result<(), String> {
    let output = if null_out {
        ez_ffmpeg::core::context::null_output::create_null_output()
            .set_format("null")
            .set_video_codec("wrapped_avframe")
    } else {
        Output::from("/tmp/gpu_benchmark_out.mp4")
            .set_video_codec("h264_vaapi")
            .set_audio_codec("aac")
    };
    FfmpegContext::builder()
        .input(
            Input::from(input)
                .set_hwaccel("vaapi")
                .set_hwaccel_output_format("vaapi"),
        )
        .filter_desc("scale_vaapi=w=1280:h=720")
        .output(output)
        .build()
        .map_err(|e| format!("build: {e}"))?
        .start()
        .map_err(|e| format!("start: {e}"))?
        .wait()
        .map_err(|e| format!("wait: {e}"))
}

fn run_leg(
    name: &'static str,
    runs: usize,
    job: impl Fn() -> Result<(), String>,
) -> LegResult {
    // Warmup run: page caches, shader compilation, device init paths.
    if let Err(e) = job() {
        return LegResult { name, wall_secs: vec![], cpu_secs: vec![], error: Some(e) };
    }

    let mut wall_secs = Vec::with_capacity(runs);
    let mut cpu_secs = Vec::with_capacity(runs);
    for _ in 0..runs {
        let cpu_before = process_cpu_seconds();
        let t = Instant::now();
        if let Err(e) = job() {
            return LegResult { name, wall_secs, cpu_secs, error: Some(e) };
        }
        wall_secs.push(t.elapsed().as_secs_f64());
        cpu_secs.push(process_cpu_seconds() - cpu_before);
    }
    LegResult { name, wall_secs, cpu_secs, error: None }
}

fn skipped(name: &'static str, reason: &str) -> LegResult {
    LegResult { name, wall_secs: vec![], cpu_secs: vec![], error: Some(format!("skipped: {reason}")) }
}

/// Total user+system CPU time consumed by this process so far.
fn process_cpu_seconds() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to a zeroed rusage; RUSAGE_SELF is always allowed.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

fn probe_frame_count(input: &str) -> u64 {
    ez_ffmpeg::stream_info::find_video_stream_info(input)
        .ok()
        .flatten()
        .and_then(|info| match info {
            ez_ffmpeg::stream_info::StreamInfo::Video { nb_frames, .. } if nb_frames > 0 => {
                Some(nb_frames as u64)
            }
            _ => None,
        })
        .unwrap_or(0)
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    sorted[sorted.len() / 2]
}

fn print_report(results: &[LegResult], frames: u64) {
    println!("\n| leg | median wall (s) | fps | CPU time (s) | CPU util | note |");
    println!("|---|---|---|---|---|---|");
    for r in results {
        match (&r.error, r.wall_secs.is_empty()) {
            (Some(e), _) => println!("| {} | — | — | — | — | {} |", r.name, e),
            (None, true) => println!("| {} | — | — | — | — | no data |", r.name),
            (None, false) => {
                let wall = median(&r.wall_secs);
                let cpu = median(&r.cpu_secs);
                let fps = if frames > 0 { frames as f64 / wall } else { 0.0 };
                println!(
                    "| {} | {:.2} | {:.1} | {:.2} | {:.0}% | {} runs |",
                    r.name,
                    wall,
                    fps,
                    cpu,
                    cpu / wall * 100.0,
                    r.wall_secs.len()
                );
            }
        }
    }
}
