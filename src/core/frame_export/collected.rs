//! The self-describing [`CollectedAudio`] output type.

/// One whole audio extraction, collected: owned, interleaved 32-bit float
/// samples plus the sample rate, channel count, and channel layout that
/// describe them.
///
/// Returned by [`collect_audio`](super::SampleExtractor::collect_audio). The
/// buffer is exactly what
/// [`collect_samples`](super::SampleExtractor::collect_samples) returns for
/// the same run — packed native-endian `f32`, channels interleaved
/// (`[L0, R0, L1, R1, …]` for stereo) — with the shape attached, so a WAV
/// writer, resampler, or model can consume the buffer without out-of-band
/// knowledge of the source.
pub struct CollectedAudio {
    samples: Vec<f32>,
    sample_rate: u32,
    channels: u16,
    channel_layout: String,
}

impl CollectedAudio {
    /// Builds a collected result from an already-flattened sample buffer.
    /// Crate-internal: `collect_audio` guarantees the buffer holds whole
    /// interleaved frames, with zeroed metadata only for an empty run.
    pub(crate) fn new(
        samples: Vec<f32>,
        sample_rate: u32,
        channels: u16,
        channel_layout: String,
    ) -> Self {
        // `%` (not `is_multiple_of`) keeps this MSRV-1.80 safe, matching the
        // sibling chunk type; `usize::is_multiple_of` is only stable since 1.87.
        #[allow(clippy::manual_is_multiple_of)]
        {
            debug_assert!(
                if channels == 0 {
                    samples.is_empty()
                } else {
                    samples.len() % channels as usize == 0
                },
                "CollectedAudio buffer must hold whole interleaved frames"
            );
        }
        Self {
            samples,
            sample_rate,
            channels,
            channel_layout,
        }
    }

    /// Samples per second. `0` only when the run delivered no samples at all.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Number of interleaved channels (1 for mono, 2 for stereo). `0` only
    /// when the run delivered no samples at all.
    pub fn channels(&self) -> u16 {
        self.channels
    }

    /// FFmpeg's textual channel-layout description (e.g. `"mono"`,
    /// `"stereo"`, `"5.1"`) — the same vocabulary as
    /// [`AudioChunk::channel_layout`](super::AudioChunk::channel_layout).
    /// Above two channels this distinguishes layouts a bare count cannot
    /// (6 channels may be `"5.1"` or `"6.0"`). Empty when the run delivered
    /// no samples, or when FFmpeg cannot describe the layout.
    pub fn channel_layout(&self) -> &str {
        &self.channel_layout
    }

    /// The interleaved `f32` samples. Length is `frames * channels`.
    pub fn as_slice(&self) -> &[f32] {
        &self.samples
    }

    /// Consumes the collected audio and returns the owned interleaved buffer
    /// (no copy), discarding the metadata.
    pub fn into_vec(self) -> Vec<f32> {
        self.samples
    }
}

impl std::fmt::Debug for CollectedAudio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectedAudio")
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("channel_layout", &self.channel_layout)
            .field("samples", &self.samples.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_round_trip_the_shape() {
        let a = CollectedAudio::new(vec![0.25; 12], 48_000, 6, "5.1".to_string());
        assert_eq!(a.sample_rate(), 48_000);
        assert_eq!(a.channels(), 6);
        assert_eq!(a.channel_layout(), "5.1");
        assert_eq!(a.as_slice().len(), 12);
        assert_eq!(a.into_vec(), vec![0.25; 12]);
    }

    #[test]
    fn empty_run_is_fully_zeroed() {
        let a = CollectedAudio::new(Vec::new(), 0, 0, String::new());
        assert!(a.as_slice().is_empty());
        assert_eq!(a.sample_rate(), 0);
        assert_eq!(a.channels(), 0);
        assert_eq!(a.channel_layout(), "");
    }
}
