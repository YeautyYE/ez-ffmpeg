//! Where subtitle data comes from (captured by the builder, not public API).

use std::path::PathBuf;

pub(crate) enum SubtitleSource {
    /// A complete ASS/SSA script held in memory (must be UTF-8).
    AssContent(String),
    /// A subtitle file — .srt/.ass/.vtt or any container holding a text
    /// subtitle stream — loaded through lavformat/lavcodec.
    File(PathBuf),
    /// SRT (or any probeable text subtitle format) held in memory, demuxed
    /// through a custom in-memory AVIOContext.
    SrtContent(String),
}
