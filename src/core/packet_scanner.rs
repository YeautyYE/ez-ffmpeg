use ffmpeg_sys_next::{
    av_packet_alloc, av_packet_free, av_packet_unref, av_read_frame,
    avformat_seek_file, AVPacket, AVERROR, EAGAIN,
    AV_PKT_FLAG_CORRUPT, AV_PKT_FLAG_KEY,
};

use std::iter::FusedIterator;

use crate::core::context::AVFormatContextBox;
use crate::error::{DemuxingError, OpenInputError, PacketScannerError, Result};

/// Read-only metadata extracted from a single demuxed packet.
///
/// `PacketInfo` contains scalar values copied out of an `AVPacket`, so it has no
/// lifetime ties to the scanner. It is cheap to clone and store.
#[derive(Debug, Clone)]
pub struct PacketInfo {
    stream_index: usize,
    pts: Option<i64>,
    dts: Option<i64>,
    duration: i64,
    size: usize,
    pos: i64,
    is_keyframe: bool,
    is_corrupt: bool,
}

impl PacketInfo {
    /// The index of the stream this packet belongs to.
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// Presentation timestamp in stream time-base units, if available.
    pub fn pts(&self) -> Option<i64> {
        self.pts
    }

    /// Decompression timestamp in stream time-base units, if available.
    pub fn dts(&self) -> Option<i64> {
        self.dts
    }

    /// Duration of this packet in stream time-base units.
    pub fn duration(&self) -> i64 {
        self.duration
    }

    /// Size of the packet data in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Byte position of this packet in the input file, or -1 if unknown.
    pub fn pos(&self) -> i64 {
        self.pos
    }

    /// Whether this packet contains a keyframe.
    pub fn is_keyframe(&self) -> bool {
        self.is_keyframe
    }

    /// Whether this packet is flagged as corrupt.
    pub fn is_corrupt(&self) -> bool {
        self.is_corrupt
    }
}

/// A stateful packet-level scanner for media files.
///
/// `PacketScanner` opens a media file (or URL) and iterates over demuxed packets
/// without decoding. This is useful for inspecting packet metadata such as
/// timestamps, keyframe flags, sizes, and stream indices.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::packet_scanner::PacketScanner;
///
/// let mut scanner = PacketScanner::open("test.mp4")?;
/// for packet in scanner.packets() {
///     let packet = packet?;
///     println!(
///         "stream={} pts={:?} size={} keyframe={}",
///         packet.stream_index(),
///         packet.pts(),
///         packet.size(),
///         packet.is_keyframe(),
///     );
/// }
/// ```
pub struct PacketScanner {
    fmt_ctx_box: AVFormatContextBox,
    pkt: *mut AVPacket,
}

// SAFETY: PacketScanner owns its AVFormatContext and AVPacket exclusively.
// It is moved between threads, never shared. No thread-affine callbacks are registered.
// This matches the safety reasoning of AVFormatContextBox's own `unsafe impl Send`.
unsafe impl Send for PacketScanner {}

impl PacketScanner {
    /// Open a media file or URL for packet scanning.
    pub fn open(url: impl Into<String>) -> Result<Self> {
        let fmt_ctx_box = crate::core::stream_info::init_format_context(url)?;

        unsafe {
            let pkt = av_packet_alloc();
            if pkt.is_null() {
                return Err(OpenInputError::OutOfMemory.into());
            }

            Ok(Self { fmt_ctx_box, pkt })
        }
    }

    /// Seek to a timestamp in microseconds.
    ///
    /// Seeks to the nearest keyframe before the given timestamp.
    /// Can be called repeatedly for jump-reading patterns.
    ///
    /// On failure you may continue reading or attempt another seek, though
    /// the exact read position is not guaranteed to be unchanged.
    pub fn seek(&mut self, timestamp_us: i64) -> Result<()> {
        unsafe {
            let ret = avformat_seek_file(
                self.fmt_ctx_box.fmt_ctx,
                -1,
                i64::MIN,
                timestamp_us,
                timestamp_us,
                0,
            );
            if ret < 0 {
                return Err(
                    PacketScannerError::SeekError(DemuxingError::from(ret)).into()
                );
            }
        }
        Ok(())
    }

    /// Read the next packet's info. Returns `None` at EOF.
    ///
    /// If the underlying demuxer returns `EAGAIN` (common with network streams),
    /// this method retries with a 10 ms sleep up to 500 times (~5 seconds).
    /// After exhausting retries it returns an error.
    pub fn next_packet(&mut self) -> Result<Option<PacketInfo>> {
        const MAX_EAGAIN_RETRIES: u32 = 500;

        unsafe {
            av_packet_unref(self.pkt);

            let mut eagain_retries: u32 = 0;
            loop {
                let ret = av_read_frame(self.fmt_ctx_box.fmt_ctx, self.pkt);
                if ret == AVERROR(EAGAIN) {
                    eagain_retries += 1;
                    if eagain_retries > MAX_EAGAIN_RETRIES {
                        return Err(
                            PacketScannerError::ReadError(DemuxingError::from(ret)).into()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                if ret < 0 {
                    if ret == ffmpeg_sys_next::AVERROR_EOF {
                        return Ok(None);
                    }
                    return Err(
                        PacketScannerError::ReadError(DemuxingError::from(ret)).into()
                    );
                }
                break;
            }

            let pkt = &*self.pkt;
            let pts = if pkt.pts == ffmpeg_sys_next::AV_NOPTS_VALUE {
                None
            } else {
                Some(pkt.pts)
            };
            let dts = if pkt.dts == ffmpeg_sys_next::AV_NOPTS_VALUE {
                None
            } else {
                Some(pkt.dts)
            };

            Ok(Some(PacketInfo {
                stream_index: pkt.stream_index.max(0) as usize,
                pts,
                dts,
                duration: pkt.duration,
                size: pkt.size.max(0) as usize,
                pos: pkt.pos,
                is_keyframe: (pkt.flags & AV_PKT_FLAG_KEY) != 0,
                is_corrupt: (pkt.flags & AV_PKT_FLAG_CORRUPT) != 0,
            }))
        }
    }

    /// Returns an iterator for convenient `for packet in scanner.packets()` usage.
    ///
    /// Each call creates a fresh iterator, so you can `seek()` and then call
    /// `packets()` again to iterate from the new position.
    ///
    /// The iterator is fused: once it yields `None` (EOF) or an `Err`, all
    /// subsequent calls to `next()` return `None`.
    pub fn packets(&mut self) -> PacketIter<'_> {
        PacketIter { scanner: self, done: false }
    }
}

impl Drop for PacketScanner {
    fn drop(&mut self) {
        unsafe {
            if !self.pkt.is_null() {
                av_packet_free(&mut self.pkt);
            }
        }
        // AVFormatContextBox handles closing the format context
    }
}

/// Iterator wrapper for [`PacketScanner`].
///
/// Yields `Result<PacketInfo>` for each packet until EOF or an error occurs.
/// The iterator is fused: after returning `None` or `Err`, it always returns `None`.
pub struct PacketIter<'a> {
    scanner: &'a mut PacketScanner,
    done: bool,
}

impl<'a> Iterator for PacketIter<'a> {
    type Item = Result<PacketInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.scanner.next_packet() {
            Ok(Some(info)) => Some(Ok(info)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

impl<'a> FusedIterator for PacketIter<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_not_found() {
        let result = PacketScanner::open("not_found.mp4");
        assert!(result.is_err());
    }

    #[test]
    fn test_scan_packets() {
        let mut scanner = PacketScanner::open("test.mp4").unwrap();
        let mut count = 0;
        let mut keyframes = 0;
        for packet in scanner.packets() {
            let info = packet.unwrap();
            count += 1;
            if info.is_keyframe() {
                keyframes += 1;
            }
        }
        assert!(count > 0, "expected at least one packet");
        assert!(keyframes > 0, "expected at least one keyframe");
        println!("total packets: {}, keyframes: {}", count, keyframes);
    }

    #[test]
    fn test_seek_and_read() {
        let mut scanner = PacketScanner::open("test.mp4").unwrap();
        // Seek to 1 second (1_000_000 microseconds)
        scanner.seek(1_000_000).unwrap();
        let packet = scanner.next_packet().unwrap();
        assert!(packet.is_some(), "expected a packet after seeking");
    }
}
