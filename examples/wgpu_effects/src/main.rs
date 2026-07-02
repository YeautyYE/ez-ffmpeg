//! Applies a WGSL shader effect to a video via WgpuFrameFilter.
//!
//! Usage: `cargo run -- <effect> [input.mp4]`
//! Effects: grayscale | vignette | zoom_pulse | chroma_shift | adjust
//!
//! The `adjust` effect also demonstrates live parameter updates through
//! `WgpuParamsHandle`: a background thread animates saturation while the
//! video is being processed.

use bytemuck::{Pod, Zeroable};
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::wgpu_filter::WgpuFrameFilter;
use ez_ffmpeg::{AVMediaType, FfmpegContext, Input, Output};

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct AdjustParams {
    brightness: f32,
    contrast: f32,
    saturation: f32,
    _pad: f32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let effect = std::env::args().nth(1).unwrap_or_else(|| "grayscale".into());
    let input = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "../../test.mp4".into());

    let shader = match effect.as_str() {
        "grayscale" => include_str!("../shaders/grayscale.wgsl"),
        "vignette" => include_str!("../shaders/vignette.wgsl"),
        "zoom_pulse" => include_str!("../shaders/zoom_pulse.wgsl"),
        "chroma_shift" => include_str!("../shaders/chroma_shift.wgsl"),
        "adjust" => include_str!("../shaders/adjust.wgsl"),
        other => return Err(format!("unknown effect: {other}").into()),
    };

    let mut builder = WgpuFrameFilter::builder().shader_wgsl(shader);
    if effect == "adjust" {
        builder = builder.params(AdjustParams {
            brightness: 0.0,
            contrast: 1.1,
            saturation: 1.0,
            _pad: 0.0,
        });
    }
    let filter = builder.build()?;
    let stats = filter.stats_handle();

    // Live parameter updates while the pipeline runs.
    if effect == "adjust" {
        let handle = filter.params_handle::<AdjustParams>()?;
        std::thread::spawn(move || {
            for i in 0..100 {
                handle.set(AdjustParams {
                    brightness: 0.0,
                    contrast: 1.1,
                    saturation: (i as f32 / 50.0 - 1.0).abs() * 2.0,
                    _pad: 0.0,
                });
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        });
    }

    let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
    let pipeline = pipeline.filter("wgpu_effect", Box::new(filter));

    let output = Output::from(format!("output_{effect}.mp4"))
        .set_video_codec("libx264")
        .set_audio_codec("aac")
        .add_frame_pipeline(pipeline);

    FfmpegContext::builder()
        .input(Input::from(input))
        // WgpuFrameFilter accepts YUV420P/422P/444P (and J variants) or NV12
        // directly; only other formats (e.g. 10-bit) still need a
        // `format=yuv420p` filter_desc before the frame pipeline.
        .output(output)
        .build()?
        .start()?
        .wait()?;

    let s = *stats.lock().unwrap();
    println!(
        "done: output_{effect}.mp4 | {} frames | per-frame avg: upload {:.2}ms, gpu wait {:.2}ms, download {:.2}ms",
        s.frames,
        s.upload_secs / s.frames.max(1) as f64 * 1000.0,
        s.gpu_secs / s.frames.max(1) as f64 * 1000.0,
        s.download_secs / s.frames.max(1) as f64 * 1000.0,
    );
    Ok(())
}
