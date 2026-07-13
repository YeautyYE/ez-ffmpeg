//! Applies a built-in named effect from `wgpu_filter::effects` to a video —
//! no WGSL, just a typed constructor and parameter struct.
//!
//! Usage: `cargo run -- <effect> [input.mp4]`
//! Effects: adjust | portrait | sharpen | soft_blur | pixelate | mirror |
//!          soul | sway | wave | swirl | magnifier | fisheye | chroma_key
//!
//! `magnifier` also demonstrates live parameter updates: a background
//! thread sweeps the lens across the frame while the video is processed.

use ez_ffmpeg::filter::frame_filter::FrameFilter;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::wgpu_filter::effects::{
    adjust, chroma_key, fisheye, magnifier, pixelate, portrait, sharpen, soft_blur, soul, sway,
    swirl, transform, wave, AdjustParams, ChromaKeyParams, FisheyeParams, MagnifierParams,
    PixelateParams, SharpenParams, SoftBlurParams, SoulParams, SwayParams, SwirlParams,
    TransformParams, WaveParams,
};
use ez_ffmpeg::{AVMediaType, FfmpegContext, Input, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let effect = std::env::args().nth(1).unwrap_or_else(|| "adjust".into());
    let input = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "../../test.mp4".into());

    // Every constructor returns a typed builder; `build()` is CPU-only
    // (shader assembly + validation), the GPU is touched at pipeline init.
    let filter: Box<dyn FrameFilter> = match effect.as_str() {
        "adjust" => Box::new(
            adjust(AdjustParams {
                saturation: 1.2,
                temperature: 0.15,
                ..AdjustParams::default()
            })
            .build()?,
        ),
        "portrait" => Box::new(portrait().build()?),
        "sharpen" => Box::new(sharpen(SharpenParams::with_amount(1.5)).build()?),
        "soft_blur" => Box::new(soft_blur(SoftBlurParams::privacy()).build()?),
        "pixelate" => Box::new(pixelate(PixelateParams::with_block_size(24.0)).build()?),
        "mirror" => Box::new(transform(TransformParams::mirrored()).build()?),
        "soul" => Box::new(soul(SoulParams::default()).build()?),
        "sway" => Box::new(sway(SwayParams::handheld()).build()?),
        "wave" => Box::new(wave(WaveParams::default()).build()?),
        "swirl" => Box::new(swirl(SwirlParams::tornado()).build()?),
        "fisheye" => Box::new(fisheye(FisheyeParams::default()).build()?),
        // Green-screen extraction: keys pure green with FFmpeg `chromakey`
        // semantics and composites the foreground over a solid background
        // (the pipeline outputs alpha-less YUV). Swap in `blue_screen()`
        // or `.with_background(r, g, b)` for other setups.
        "chroma_key" => Box::new(chroma_key(ChromaKeyParams::green_screen()).build()?),
        "magnifier" => {
            let built = magnifier(MagnifierParams::default()).build()?;
            // Live updates: the typed handle works from any thread.
            let params = built.params_handle();
            std::thread::spawn(move || {
                for i in 0..200 {
                    let t = i as f32 / 200.0 * std::f32::consts::TAU;
                    params.update(|p| {
                        p.center_x = 0.5 + 0.3 * t.cos();
                        p.center_y = 0.5 + 0.3 * t.sin();
                    });
                    std::thread::sleep(std::time::Duration::from_millis(40));
                }
            });
            Box::new(built)
        }
        other => return Err(format!("unknown effect: {other}").into()),
    };

    let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
    let pipeline = pipeline.filter(&effect, filter);

    let output = Output::from(format!("output_{effect}.mp4"))
        .set_video_codec("libx264")
        .set_audio_codec("aac")
        .add_frame_pipeline(pipeline);

    FfmpegContext::builder()
        .input(Input::from(input))
        .output(output)
        .build()?
        .start()?
        .wait()?;

    println!("done: output_{effect}.mp4");
    Ok(())
}
