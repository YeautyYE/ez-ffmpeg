//! The owned, interleaved [`AudioChunk`] output type.

/// One exported block of decoded audio: owned, interleaved 32-bit float samples
/// plus metadata.
///
/// Samples are packed native-endian `f32` in the exact layout every whisper /
/// candle / ort pipeline expects: [`as_slice`](AudioChunk::as_slice)`.len() ==
/// frames * channels`, channels interleaved (`[L0, R0, L1, R1, …]` for stereo).
/// One chunk corresponds to one filtered `AVFrame`; the number of frames per
/// chunk is not contractual (typically ~1024) and must not be relied upon.
pub struct AudioChunk {
    pts_us: Option<i64>,
    index: u64,
    sample_rate: u32,
    channels: u16,
    data: Vec<f32>,
}

impl AudioChunk {
    /// Builds a chunk from an already-interleaved sample buffer. Crate-internal:
    /// the sink guarantees `data.len()` is a whole multiple of `channels`.
    pub(crate) fn new(
        pts_us: Option<i64>,
        index: u64,
        sample_rate: u32,
        channels: u16,
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
            data,
        }
    }

    /// Presentation time in microseconds from the stream start, passed through
    /// from the source frame. `None` when the frame carried no usable timestamp.
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
            .field("samples", &self.data.len())
            .finish()
    }
}
