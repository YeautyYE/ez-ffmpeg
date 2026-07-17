//! Public option enum for audio sample export: the channel layout to negotiate.

/// The output channel layout for [`SampleExtractor`](super::SampleExtractor).
///
/// Selecting a value inserts a downmix into the resample stage
/// (`aformat=channel_layouts=…`, backed by swr); leaving it unset preserves the
/// source layout. ASR models such as whisper want [`Mono`](Channels::Mono).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Channels {
    /// One channel. Stereo (and higher) sources are downmixed to mono.
    Mono,
    /// Two channels, interleaved as `L R`.
    Stereo,
}

impl Channels {
    /// The FFmpeg `channel_layouts` token this layout maps to.
    pub(crate) fn layout_name(self) -> &'static str {
        match self {
            Channels::Mono => "mono",
            Channels::Stereo => "stereo",
        }
    }
}
