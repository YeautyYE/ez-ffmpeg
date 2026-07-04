use ez_ffmpeg::analysis::{Analysis, AudioDetector, VideoDetector};

/// Detect black frames, scene cuts, silence and EBU R128 loudness in a single
/// decode pass, and get the results back as typed Rust data (not FFmpeg logs).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = Analysis::new("test.mp4")
        .video_detector(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.10,
            picture_th: 0.98,
        })
        .video_detector(VideoDetector::Scene { threshold_pct: 10.0 })
        .audio_detector(AudioDetector::Silence {
            noise_db: -30.0,
            min_duration_s: 0.5,
            mono: false,
        })
        .audio_detector(AudioDetector::Ebur128 { true_peak: true })
        .run()?;

    println!("black regions : {:?}", report.black);
    println!("scene changes : {}", report.scenes.len());
    println!("silence regions: {:?}", report.silence);
    if let Some(loudness) = report.loudness {
        println!("integrated    : {:?} LUFS", loudness.integrated);
        println!("true peak     : {:?} dBTP", loudness.true_peak);
    }
    Ok(())
}
