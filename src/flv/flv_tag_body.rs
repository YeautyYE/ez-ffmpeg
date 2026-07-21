//! Byte-level classification of FLV tag bodies.
//!
//! Single owner of the codec-specific predicates over an FLV tag's payload
//! (the bytes after the 11-byte tag header), shared by
//! [`FlvTag`](crate::flv::flv_tag::FlvTag)'s convenience methods and the RTMP
//! fanout path so the two cannot drift apart.

/// Whether a video tag body is an AVC sequence header
/// (an AVCDecoderConfigurationRecord).
///
/// Assumes h264: `0x17` = frame type 1 (keyframe) in the high nibble plus
/// codec id 7 (AVC) in the low nibble; AVCPacketType `0x00` marks the
/// sequence header.
pub fn is_video_sequence_header(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x17 && data[1] == 0x00
}

/// Whether an audio tag body is an AAC sequence header
/// (an AudioSpecificConfig).
///
/// Assumes aac: `0xaf` = sound format 10 (AAC) in the high nibble plus the
/// AAC-mandated rate/size/type bits in the low nibble; AACPacketType `0x00`
/// marks the sequence header.
pub fn is_audio_sequence_header(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0xaf && data[1] == 0x00
}

/// Whether a video tag body is a decodable IDR keyframe.
///
/// Assumes h264: `0x17` = keyframe frame-type + AVC codec; AVCPacketType
/// `0x01` is a NALU (the actual IDR). Require `== 0x01` rather than
/// `!= 0x00` so the sequence header (`0x00`) AND the AVC end-of-sequence
/// marker (`0x02`) are both excluded — only a decodable IDR frame may flip
/// a watcher's keyframe gate.
pub fn is_video_keyframe(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x17 && data[1] == 0x01
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_sequence_header_requires_avc_keyframe_and_packet_type_zero() {
        assert!(is_video_sequence_header(&[0x17, 0x00, 0x01, 0x64]));
        assert!(!is_video_sequence_header(&[0x17, 0x01])); // NALU, not config
        assert!(!is_video_sequence_header(&[0x27, 0x00])); // inter frame type
        assert!(!is_video_sequence_header(&[0x17])); // truncated
    }

    #[test]
    fn audio_sequence_header_requires_aac_and_packet_type_zero() {
        assert!(is_audio_sequence_header(&[0xaf, 0x00, 0x12, 0x10]));
        assert!(!is_audio_sequence_header(&[0xaf, 0x01])); // raw AAC frame
        assert!(!is_audio_sequence_header(&[0x2f, 0x00])); // MP3, not AAC
        assert!(!is_audio_sequence_header(&[0xaf])); // truncated
    }

    #[test]
    fn video_keyframe_is_idr_nalu_only() {
        assert!(is_video_keyframe(&[0x17, 0x01, 0xAA]));
        assert!(!is_video_keyframe(&[0x17, 0x00])); // sequence header
        assert!(!is_video_keyframe(&[0x17, 0x02])); // end-of-sequence marker
        assert!(!is_video_keyframe(&[0x27, 0x01])); // inter frame
        assert!(!is_video_keyframe(&[0x17])); // truncated
    }
}
