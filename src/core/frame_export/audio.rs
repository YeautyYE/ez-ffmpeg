//! The [`SampleExtractor`] builder and its `samples()` / `collect_samples()` runs.

use super::audio_iter::SampleIter;
use super::audio_options::Channels;
use super::audio_resolve::{resolve_and_build_desc, AudioResolvePlan};
use super::audio_sink::SampleSink;
use super::error::FrameExportError;
use crate::core::context::demuxer::Demuxer;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::filter_complex::FilterComplex;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::core::filter::frame_pipeline_builder::FramePipelineBuilder;
use crate::core::scheduler::ffmpeg_scheduler::FfmpegScheduler;
use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_AUDIO;

/// Whisper's canonical input sample rate (16 kHz).
const WHISPER_SAMPLE_RATE: u32 = 16_000;

/// Decodes an input's audio into owned, interleaved `f32` PCM in one pass, for
/// ASR / audio-ML pipelines.
///
/// Build one with [`SampleExtractor::new`], chain options, then call
/// [`samples`](SampleExtractor::samples) for a streaming iterator or
/// [`collect_samples`](SampleExtractor::collect_samples) for one flat buffer.
///
/// Defaults **preserve the source**: the source sample rate and channel layout
/// pass through untouched, and only the sample *format* is pinned (packed `f32`).
/// Normalization is opt-in — [`sample_rate`](Self::sample_rate) and
/// [`channels`](Self::channels) insert a resample / downmix — so music and
/// analysis users get no silent rate surprise. [`for_whisper`](Self::for_whisper)
/// is a thin preset over those two setters.
///
/// ```no_run
/// use ez_ffmpeg::frame_export::SampleExtractor;
///
/// # fn main() -> Result<(), ez_ffmpeg::error::Error> {
/// // 16 kHz mono f32 — the whisper-rs / candle handoff shape.
/// let pcm: Vec<f32> = SampleExtractor::for_whisper("input.mp4").collect_samples()?;
/// # let _ = pcm;
/// # Ok(())
/// # }
/// ```
///
/// # Threading & teardown
///
/// A run drives the normal scheduler with the sample sink mounted on the audio
/// output pipeline. The returned [`SampleIter`] is `Send` and fused (exactly one
/// terminal error, then `None` forever); dropping it early aborts the run
/// cleanly (see [`SampleIter`] for the teardown ordering).
pub struct SampleExtractor {
    input: Input,
    audio_stream_index: Option<usize>,
    sample_rate: Option<u32>,
    channels: Option<Channels>,
    start_time_us: Option<i64>,
    duration_us: Option<i64>,
    channel_capacity: usize,
}

impl SampleExtractor {
    /// Creates an extractor over `input` (a path, URL, or anything convertible
    /// into an [`Input`]). Defaults: best audio stream, source rate, source
    /// layout, packed `f32`, channel capacity 4.
    pub fn new(input: impl Into<Input>) -> Self {
        Self {
            input: input.into(),
            audio_stream_index: None,
            sample_rate: None,
            channels: None,
            start_time_us: None,
            duration_us: None,
            channel_capacity: 4,
        }
    }

    /// Creates an extractor preset for whisper-style ASR: 16 kHz, mono, `f32`.
    ///
    /// Thin convenience over [`new`](Self::new) followed by
    /// `.sample_rate(16000).channels(Channels::Mono)`; every other option still
    /// applies and can be overridden afterwards.
    pub fn for_whisper(input: impl Into<Input>) -> Self {
        Self::new(input)
            .sample_rate(WHISPER_SAMPLE_RATE)
            .channels(Channels::Mono)
    }

    /// Selects an explicit audio stream by absolute index (default: best audio
    /// stream).
    pub fn audio_stream_index(mut self, index: usize) -> Self {
        self.audio_stream_index = Some(index);
        self
    }

    /// Resamples to this output rate in Hz (default: source rate). `0` is
    /// rejected at [`samples`](Self::samples) time.
    pub fn sample_rate(mut self, hz: u32) -> Self {
        self.sample_rate = Some(hz);
        self
    }

    /// Downmixes to this channel layout (default: source layout).
    pub fn channels(mut self, channels: Channels) -> Self {
        self.channels = Some(channels);
        self
    }

    /// Seeks to this start time (microseconds) before extracting.
    pub fn start_time_us(mut self, us: i64) -> Self {
        self.start_time_us = Some(us);
        self
    }

    /// Limits extraction to this many microseconds of content past the start.
    pub fn duration_us(mut self, us: i64) -> Self {
        self.duration_us = Some(us);
        self
    }

    /// Sets the prefetch channel capacity (default 4, minimum 1). Chunks are
    /// small (a few KiB each), so a slightly deeper queue is nearly free.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Starts the run and returns a streaming iterator over the exported chunks.
    ///
    /// Option and stream-resolution errors surface here, before any chunk is
    /// produced; runtime failures surface as the iterator's terminal `Err`.
    pub fn samples(self) -> crate::error::Result<SampleIter> {
        self.validate()?;

        let capacity = self.channel_capacity.max(1);
        let (tx, rx) = crossbeam_channel::bounded(capacity);

        // Output-side sink on the single graph output stream ([export] => 0).
        let sink = SampleSink::new(tx);
        let sink_pipeline = FramePipelineBuilder::new(AVMEDIA_TYPE_AUDIO)
            .filter("frame_export_sample_sink", Box::new(sink))
            .set_stream_index(0)
            .build();

        // Input time window (no decoder fast path applies to audio).
        let mut input = self.input;
        if let Some(start) = self.start_time_us {
            input = input.set_start_time_us(start);
        }
        if let Some(dur) = self.duration_us {
            input = input.set_recording_time_us(dur);
        }

        // Null output. The audio codec pin is load-bearing: `pcm_f32le` advertises
        // `sample_fmts={flt}`, so the encoder-negotiated `aformat` appended to the
        // graph output agrees with our own `sample_fmts=flt` and the tap sees
        // packed f32. The null muxer's default `pcm_s16le` would instead force
        // `s16` at the output, which the sink rejects.
        let output = Output::from("-")
            .set_format("null")
            .set_audio_codec("pcm_f32le")
            .add_stream_map("[export]")
            .add_frame_pipeline(sink_pipeline);

        // Deferred resolver: runs against the opened demuxer (S8/S9).
        let resolve_plan = AudioResolvePlan {
            stream_index: self.audio_stream_index,
            sample_rate: self.sample_rate,
            channels: self.channels,
        };
        let resolver = move |demuxs: &[Demuxer]| -> crate::error::Result<FilterComplex> {
            let demux = demuxs.first().ok_or(FrameExportError::NoAudioStream)?;
            // SAFETY: the demuxer is opened for the lifetime of this call.
            let desc = unsafe { resolve_and_build_desc(demux.in_fmt_ctx_ptr(), &resolve_plan)? };
            Ok(desc.into())
        };

        let context = FfmpegContext::builder()
            .input(input)
            .output(output)
            .add_deferred_filter_desc(Box::new(resolver))
            .build()?;
        let scheduler = FfmpegScheduler::new(context).start()?;
        Ok(SampleIter::new(rx, scheduler))
    }

    /// Runs to completion and flattens every exported chunk into one interleaved
    /// `f32` buffer — the whisper-rs / candle handoff shape.
    ///
    /// Memory is `duration_s × rate × channels × 4` bytes (1 h @ 16 kHz mono ≈
    /// 230 MB); use [`samples`](Self::samples) to stream when that is too large.
    /// On a terminal error the partial samples are dropped and the error is
    /// returned.
    pub fn collect_samples(self) -> crate::error::Result<Vec<f32>> {
        let mut out: Vec<f32> = Vec::new();
        for chunk in self.samples()? {
            let chunk = chunk?;
            if out.is_empty() {
                // First non-empty chunk: move its buffer in with no copy.
                out = chunk.into_vec();
            } else {
                out.extend_from_slice(chunk.as_slice());
            }
        }
        Ok(out)
    }

    fn validate(&self) -> crate::error::Result<()> {
        if self.channel_capacity == 0 {
            return Err(invalid("channel_capacity must be >= 1"));
        }
        if self.sample_rate == Some(0) {
            return Err(invalid("sample_rate must be > 0"));
        }
        Ok(())
    }
}

fn invalid(msg: &str) -> crate::error::Error {
    FrameExportError::InvalidOption(msg.to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_capacity_is_rejected() {
        let r = SampleExtractor::new("x.mp4").channel_capacity(0).validate();
        assert!(matches!(
            r,
            Err(crate::error::Error::FrameExport(
                FrameExportError::InvalidOption(_)
            ))
        ));
    }

    #[test]
    fn zero_sample_rate_is_rejected() {
        assert!(SampleExtractor::new("x.mp4")
            .sample_rate(0)
            .validate()
            .is_err());
    }

    #[test]
    fn defaults_validate() {
        assert!(SampleExtractor::new("x.mp4").validate().is_ok());
        assert!(SampleExtractor::new("x.mp4")
            .sample_rate(16000)
            .channels(Channels::Mono)
            .duration_us(250_000)
            .validate()
            .is_ok());
    }

    #[test]
    fn whisper_preset_sets_rate_and_mono() {
        let e = SampleExtractor::for_whisper("x.mp4");
        assert_eq!(e.sample_rate, Some(WHISPER_SAMPLE_RATE));
        assert_eq!(e.channels, Some(Channels::Mono));
        assert!(e.validate().is_ok());
    }
}
