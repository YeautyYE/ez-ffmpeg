//! The owned, interleaved [`AudioChunk`] output type.

/// One exported block of decoded audio: owned, interleaved 32-bit float samples
/// plus metadata.
///
/// Samples are packed native-endian `f32`, interleaved:
/// [`as_slice`](AudioChunk::as_slice)`.len() == frames * channels`, channels
/// interleaved (`[L0, R0, L1, R1, …]` for stereo) — the buffer layout
/// whisper / candle / ort pipelines consume, at whatever sample rate and
/// channel shape this chunk reports (normalize via the extractor's
/// `sample_rate`/`channels` options when a model needs a fixed shape).
/// One chunk corresponds to one filtered `AVFrame`; the number of frames per
/// chunk is not contractual (typically ~1024) and must not be relied upon.
pub struct AudioChunk {
    pts_us: Option<i64>,
    index: u64,
    sample_rate: u32,
    channels: u16,
    channel_layout: String,
    data: Vec<f32>,
}

impl AudioChunk {
    /// Builds a chunk from an already-interleaved sample buffer. Crate-internal:
    /// the sink guarantees `data.len()` is a whole multiple of `channels` and
    /// that `channel_layout` describes the exported frame's layout.
    pub(crate) fn new(
        pts_us: Option<i64>,
        index: u64,
        sample_rate: u32,
        channels: u16,
        channel_layout: String,
        data: Vec<f32>,
    ) -> Self {
        // `%` (not `is_multiple_of`) keeps this MSRV-1.80 safe, matching the
        // sibling video sink; `usize::is_multiple_of` is only stable since 1.87.
        #[allow(clippy::manual_is_multiple_of)]
        {
            debug_assert!(
                channels != 0 && data.len() % channels as usize == 0,
                "AudioChunk buffer must hold whole interleaved frames"
            );
        }
        Self {
            pts_us,
            index,
            sample_rate,
            channels,
            channel_layout,
            data,
        }
    }

    /// Presentation time in microseconds, passed through from the source
    /// frame and normalized to the start of the extraction window (the stream
    /// start when no `start_time_us` was set). `None` when the frame carried
    /// no usable timestamp.
    pub fn pts_us(&self) -> Option<i64> {
        self.pts_us
    }

    /// 0-based export index (counts delivered chunks in order).
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Samples per second.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Number of interleaved channels (1 for mono, 2 for stereo).
    pub fn channels(&self) -> u16 {
        self.channels
    }

    /// FFmpeg's textual channel-layout description of this chunk (e.g.
    /// `"mono"`, `"stereo"`, `"5.1"`), read from the exported frame: the
    /// source layout under the default passthrough, or the converted layout
    /// when [`channels`](super::SampleExtractor::channels) requested one.
    /// Above two channels this distinguishes layouts a bare count cannot
    /// (6 channels may be `"5.1"` or `"6.0"`). Empty when FFmpeg cannot
    /// describe the layout.
    pub fn channel_layout(&self) -> &str {
        &self.channel_layout
    }

    /// The interleaved `f32` samples. Length is `frames * channels`.
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }

    /// Consumes the chunk and returns the owned interleaved buffer (no copy).
    pub fn into_vec(self) -> Vec<f32> {
        self.data
    }
}

impl std::fmt::Debug for AudioChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioChunk")
            .field("pts_us", &self.pts_us)
            .field("index", &self.index)
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("channel_layout", &self.channel_layout)
            .field("samples", &self.data.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_accessors_round_trip() {
        let c = AudioChunk::new(Some(40_000), 2, 48_000, 6, "5.1".to_string(), vec![0.0; 12]);
        assert_eq!(c.pts_us(), Some(40_000));
        assert_eq!(c.index(), 2);
        assert_eq!(c.sample_rate(), 48_000);
        assert_eq!(c.channels(), 6);
        assert_eq!(c.channel_layout(), "5.1");
        assert_eq!(c.as_slice().len(), 12);
        assert_eq!(c.into_vec().len(), 12);
    }
}
