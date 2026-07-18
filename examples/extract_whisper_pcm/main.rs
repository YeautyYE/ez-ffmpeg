use ez_ffmpeg::frame_export::SampleExtractor;
use ez_ffmpeg::Input;

/// Audio -> 16 kHz mono f32 PCM, the shape whisper-style ASR models consume.
///
/// `SampleExtractor::for_whisper` presets `sample_rate(16000)` and
/// `channels(Mono)`; `collect_samples()` flattens the run into one interleaved
/// `Vec<f32>` ready to hand to whisper-rs / candle / ort. The second pass shows
/// the streaming alternative (`samples()`) that yields chunks as they decode,
/// for long inputs where one flat buffer is too much memory.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a media file from the command line, or fall back to a synthetic
    // 3-second 440 Hz tone so the example runs without any input file.
    let arg = std::env::args().nth(1);
    if arg.is_none() {
        println!("usage: extract_whisper_pcm [input-file]  (using lavfi sine fallback)");
    }
    let input = || match &arg {
        Some(path) => Input::from(path.clone()),
        None => Input::from("sine=frequency=440:sample_rate=44100:duration=3").set_format("lavfi"),
    };

    // --- One flat buffer -----------------------------------------------------
    let pcm: Vec<f32> = SampleExtractor::for_whisper(input()).collect_samples()?;

    let duration_s = pcm.len() as f64 / 16_000.0; // mono: samples == frames
    let peak = pcm.iter().fold(0.0f32, |m, s| m.max(s.abs()));
    let rms = (pcm
        .iter()
        .map(|s| f64::from(*s) * f64::from(*s))
        .sum::<f64>()
        / pcm.len().max(1) as f64)
        .sqrt();
    println!("samples : {}", pcm.len());
    println!("duration: {duration_s:.3} s at 16 kHz mono");
    println!("peak    : {peak:.4}");
    println!("RMS     : {rms:.4}");
    // For a pure tone peak/RMS is sqrt(2) ~= 1.414 — a quick sanity check that
    // the resample to 16 kHz preserved the waveform.
    if rms > 0.0 {
        println!("peak/RMS: {:.3}", f64::from(peak) / rms);
    }

    // --- Streaming, chunk by chunk -------------------------------------------
    // A second, independent run. Each chunk is one filtered frame's worth of
    // samples (size not contractual); metadata rides along on every chunk.
    // On long inputs run only this loop — it keeps a small bounded number of
    // chunks in flight (channel capacity, default 4), where collect_samples()
    // above holds the whole file.
    let mut chunks = 0u64;
    let mut streamed = 0usize;
    for chunk in SampleExtractor::for_whisper(input()).samples()? {
        let chunk = chunk?;
        if chunks == 0 {
            println!(
                "\nfirst chunk: rate={} Hz, channels={}, pts={:?}us",
                chunk.sample_rate(),
                chunk.channels(),
                chunk.pts_us()
            );
        }
        streamed += chunk.as_slice().len();
        chunks += 1;
    }
    println!("streamed {streamed} samples in {chunks} chunk(s)");
    Ok(())
}
