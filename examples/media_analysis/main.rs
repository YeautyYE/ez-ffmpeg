use ez_ffmpeg::analysis::{Analysis, AudioDetector, VideoDetector};

/// Detect black frames, scene cuts, crop/letterbox bounds, silence and EBU
/// R128 loudness in a single decode pass, and get the results back as typed
/// Rust data (not FFmpeg logs).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = Analysis::new("test.mp4")
        .video_detector(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.10,
            picture_th: 0.98,
        })
        .video_detector(VideoDetector::Scene {
            threshold_pct: 10.0,
        })
        // Native Rust crop/letterbox detection — no GPL `cropdetect` filter
        // required. Interlaced or hardware frames fail the job with
        // `Error::AnalysisFrame` (deinterlace / hwdownload first).
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 2,
            reset: 0,
        })
        .audio_detector(AudioDetector::Silence {
            noise_db: -30.0,
            min_duration_s: 0.5,
            mono: false,
        })
        .audio_detector(AudioDetector::Ebur128 { true_peak: true })
        .run()?;

    println!("black regions : {:?}", report.black);
    println!("scene changes : {}", report.scenes.len());
    println!("crop suggestion: {:?}", report.crop);
    println!("silence regions: {:?}", report.silence);
    if let Some(loudness) = report.loudness {
        println!("integrated    : {:?} LUFS", loudness.integrated);
        println!("true peak     : {:?} dBTP", loudness.true_peak);
    }
    Ok(())
}
