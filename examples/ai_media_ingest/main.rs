use ez_ffmpeg::frame_export::{FrameExtractor, SampleExtractor, Sampling};
use ez_ffmpeg::{FfmpegContext, Input, Output};

/// One media file -> both model inputs: sampled RGB frames for a vision model
/// and 16 kHz mono f32 PCM for an ASR model.
///
/// This is the AI-ingest shape: no intermediate JPEG/WAV files, no raw-byte
/// plumbing — each extractor runs its own single decode pass over the same
/// file and hands back owned buffers ready for a tensor library.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or synthesize a 6-second clip
    // (test pattern + 440 Hz tone) so the example runs without any input file.
    let path = match std::env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("usage: ai_media_ingest [input-file]  (generating ai_ingest_input.mp4)");
            generate_source("ai_ingest_input.mp4")?;
            "ai_ingest_input.mp4".to_string()
        }
    };

    // --- Pass 1: video -> one 224-wide RGB frame per second ------------------
    // `width(224)` resizes with the aspect ratio kept (height is derived).
    // Setting `.height(224)` as well would stretch to exactly 224x224 with no
    // cropping — only for models that tolerate the distortion.
    let mut frames = 0u64;
    let mut frame_bytes = 0usize;
    for frame in FrameExtractor::new(path.as_str())
        .sampling(Sampling::EverySec(1.0))
        .width(224)
        .frames()?
    {
        let frame = frame?;
        println!(
            "frame {:>2}: {}x{} rgb24, pts={:?}us",
            frame.index(),
            frame.width(),
            frame.height(),
            frame.pts_us()
        );
        frame_bytes += frame.as_bytes().len();
        frames += 1;
        // frame.as_bytes() / frame.into_vec() feeds the vision model here.
    }

    // --- Pass 2: audio -> 16 kHz mono f32 PCM --------------------------------
    let pcm: Vec<f32> = SampleExtractor::for_whisper(path.as_str()).collect_samples()?;
    let audio_s = pcm.len() as f64 / 16_000.0;
    // `pcm` feeds the ASR model here.

    println!("\ningest summary for {path}:");
    println!("  video: {frames} frame(s), {frame_bytes} packed RGB bytes, sampled at 1 fps");
    println!(
        "  audio: {} samples ({audio_s:.2} s at 16 kHz mono)",
        pcm.len()
    );
    Ok(())
}

/// Encodes a 6-second A/V test clip: `testsrc2` video plus a `sine` tone,
/// muxed into one MP4 (two lavfi inputs, default stream mapping). The
/// built-in `mpeg4` encoder keeps the fallback working on FFmpeg builds
/// without an external H.264 encoder.
fn generate_source(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=6:size=640x360:rate=30").set_format("lavfi"))
        .input(Input::from("sine=frequency=440:sample_rate=44100:duration=6").set_format("lavfi"))
        .output(Output::from(path).set_video_codec("mpeg4"))
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
