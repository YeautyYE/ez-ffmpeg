//! End-to-end PCM sample-export tests over synthetic lavfi sources (no fixtures).
//!
//! `sine` is a real decode path, so these exercise the whole scheduler pipeline
//! (demux → decode → aformat/swr → sample sink), not just option plumbing.

use ez_ffmpeg::frame_export::SampleExtractor;
use ez_ffmpeg::{FfmpegContext, Input, Output};

/// A finite synthetic audio source.
fn lavfi(spec: &str) -> Input {
    Input::from(spec).set_format("lavfi")
}

/// Root-mean-square amplitude — a non-silent decode is well above zero, while
/// integer samples reinterpreted as `f32` (the failure mode if the pcm_f32le
/// pin were dropped) would be denormal garbage far outside `[-1, 1]`.
fn rms(samples: &[f32]) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum_sq: f64 = samples.iter().map(|&s| (s as f64) * (s as f64)).sum();
    (sum_sq / samples.len() as f64).sqrt() as f32
}

fn assert_clean_pcm(samples: &[f32]) {
    assert!(!samples.is_empty(), "expected decoded samples");
    assert!(
        samples.iter().all(|s| s.is_finite() && s.abs() <= 1.001),
        "packed f32 PCM must be finite and within [-1, 1]"
    );
    assert!(
        rms(samples) > 0.01,
        "a 440 Hz sine must not decode to silence"
    );
}

#[test]
fn extracts_interleaved_f32_from_sine() {
    let chunks = SampleExtractor::new(lavfi("sine=frequency=440:duration=1:sample_rate=44100"))
        .samples()
        .expect("start failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("extraction failed");

    assert!(!chunks.is_empty(), "sine must yield at least one chunk");
    for (i, c) in chunks.iter().enumerate() {
        assert_eq!(c.sample_rate(), 44100, "source rate preserved by default");
        assert_eq!(c.channels(), 1, "sine is mono; source layout preserved");
        assert_eq!(c.index() as usize, i, "chunk indices are dense and 0-based");
        assert_eq!(
            c.as_slice().len() % c.channels() as usize,
            0,
            "interleaved: len is a whole multiple of channels"
        );
    }
    let all: Vec<f32> = chunks.iter().flat_map(|c| c.as_slice().to_vec()).collect();
    assert_clean_pcm(&all);
    // ~1s @ 44.1 kHz mono; allow slack for encoder/resampler edge handling.
    assert!(
        (40_000..=48_000).contains(&all.len()),
        "expected ~44100 mono samples, got {}",
        all.len()
    );
}

#[test]
fn s16_source_still_yields_f32_packed() {
    // THE pin regression (audio twin of the video color pin). A genuinely s16
    // source is decoded, resampled through `aformat=sample_fmts=flt`, and — only
    // because the null output pins `pcm_f32le` — reaches the tap as packed f32.
    // Drop the `.set_audio_codec("pcm_f32le")` pin and the null muxer's default
    // `pcm_s16le` re-`aformat`s the graph output back to s16; the sink then
    // rejects the frame and this test fails at `expect`.
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_frame_export_audio_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let wav = dir.join("sine_s16.wav");
    let wav = wav.to_str().expect("utf-8 path");

    // Dogfood a real pcm_s16le WAV (native sample format = s16).
    let ctx = FfmpegContext::builder()
        .input(lavfi("sine=frequency=440:duration=1:sample_rate=44100"))
        .output(Output::from(wav).set_audio_codec("pcm_s16le"))
        .build()
        .expect("build s16 wav");
    ctx.start()
        .expect("start s16 wav encode")
        .wait()
        .expect("s16 wav encode failed");

    let samples = SampleExtractor::new(wav)
        .collect_samples()
        .expect("s16 source must still yield f32 PCM (pcm_f32le pin)");
    assert_clean_pcm(&samples);

    let _ = std::fs::remove_file(wav);
}

#[test]
fn duration_window_and_resample_bound_the_sample_count() {
    // 0.25 s of a 44.1 kHz source, resampled to 16 kHz mono, is ~4000 samples
    // (0.25 * 16000). Proves the duration window (`-t` on the input) and the swr
    // resample negotiated by `aformat` both take effect.
    let samples =
        SampleExtractor::for_whisper(lavfi("sine=frequency=440:duration=2:sample_rate=44100"))
            .duration_us(250_000)
            .collect_samples()
            .expect("extraction failed");

    assert_clean_pcm(&samples);
    assert!(
        (3_600..=4_400).contains(&samples.len()),
        "expected ~4000 samples (0.25s @ 16kHz mono), got {}",
        samples.len()
    );
}

#[test]
fn resampled_chunk_pts_track_output_samples() {
    // After 44.1 kHz -> 16 kHz resampling, each chunk's pts_us must be in the
    // OUTPUT time base (the post-filter `frame.pts`), not the stale decoder
    // `best_effort_timestamp`. The latter, rescaled by the rewritten 1/16000
    // time base, would drift ~2.76x (off by seconds); this asserts pts tracks the
    // cumulative output-sample position and stays monotonic.
    let chunks =
        SampleExtractor::for_whisper(lavfi("sine=frequency=440:duration=2:sample_rate=44100"))
            .samples()
            .expect("start failed")
            .collect::<Result<Vec<_>, _>>()
            .expect("extraction failed");

    assert!(chunks.len() >= 2, "need several chunks to check pts drift");
    let rate = 16_000i64;
    let mut cumulative: i64 = 0; // output (mono) samples before the current chunk
    let mut last_pts = i64::MIN;
    let mut checked = 0;
    for c in &chunks {
        assert_eq!(c.sample_rate(), 16_000);
        assert_eq!(c.channels(), 1);
        if let Some(pts) = c.pts_us() {
            assert!(
                pts >= last_pts,
                "chunk pts must be monotonic non-decreasing"
            );
            last_pts = pts;
            let expected_us = cumulative * 1_000_000 / rate;
            // Generous slack for resampler priming/rounding; the bug drifts by
            // ~1.7 s at 1 s in, far outside this band.
            assert!(
                (pts - expected_us).abs() <= 50_000,
                "chunk pts {pts}us drifts from expected {expected_us}us \
                 (cumulative {cumulative} samples)"
            );
            checked += 1;
        }
        cumulative += (c.as_slice().len() / c.channels() as usize) as i64;
    }
    assert!(checked >= 2, "expected timestamped chunks to verify");
}

#[test]
fn drop_mid_stream_does_not_deadlock() {
    // A long source with capacity 1: after one chunk the sink is almost certainly
    // parked in a blocking send(). Dropping must release it (S6: receiver first,
    // then abort) rather than hang. Reaching the end of this test IS the assertion.
    let mut it = SampleExtractor::new(lavfi("sine=frequency=440:duration=30:sample_rate=44100"))
        .channel_capacity(1)
        .samples()
        .expect("start failed");
    let first = it.next().expect("at least one chunk").expect("chunk ok");
    assert_eq!(first.index(), 0);
    drop(it);
}

#[test]
fn iterator_is_fused_after_completion() {
    let mut it = SampleExtractor::new(lavfi("sine=frequency=440:duration=1:sample_rate=8000"))
        .samples()
        .expect("start failed");
    let mut count = 0usize;
    while let Some(item) = it.next() {
        item.expect("chunk ok");
        count += 1;
    }
    assert!(count > 0, "expected at least one chunk");
    // Fused: further calls keep returning None, no panic.
    assert!(it.next().is_none());
    assert!(it.next().is_none());
}

#[test]
fn no_audio_stream_is_typed_error() {
    // A video-only lavfi source has no audio stream.
    let err = SampleExtractor::new(lavfi("testsrc2=s=32x32:r=10:d=1"))
        .samples()
        .err()
        .expect("should fail with no audio stream");
    match err {
        ez_ffmpeg::error::Error::FrameExport(
            ez_ffmpeg::frame_export::FrameExportError::NoAudioStream,
        ) => {}
        other => panic!("expected NoAudioStream, got {other:?}"),
    }
}
