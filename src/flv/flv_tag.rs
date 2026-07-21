use crate::flv::flv_tag_body;
use crate::flv::flv_tag_header::FlvTagHeader;
use crate::flv::{FLV_TAG_HEADER_LENGTH, PREVIOUS_TAG_SIZE_LENGTH};

#[derive(Debug, Clone)]
pub struct FlvTag {
    pub header: FlvTagHeader,
    pub data: bytes::Bytes,     // Tag data
    pub previous_tag_size: u32, // PreviousTagSize
}

impl FlvTag {
    // Convert the FLV Tag to a byte array [u8], including the header, data, and PreviousTagSize
    pub fn as_bytes_with_previous_tag_size(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(FLV_TAG_HEADER_LENGTH + self.data.len() + PREVIOUS_TAG_SIZE_LENGTH);

        // Convert header to bytes
        bytes.extend_from_slice(&self.header.as_bytes());

        // Add data
        bytes.extend_from_slice(&self.data);

        // Add PreviousTagSize (4 bytes)
        bytes.extend_from_slice(&self.previous_tag_size.to_be_bytes());

        bytes
    }

    pub fn is_audio(&self) -> bool {
        self.header.is_audio()
    }

    pub fn is_video(&self) -> bool {
        self.header.is_video()
    }

    pub fn is_script_data(&self) -> bool {
        self.header.is_script_data()
    }

    pub fn is_video_sequence_header(&self) -> bool {
        self.is_video() && flv_tag_body::is_video_sequence_header(&self.data)
    }

    pub fn is_audio_sequence_header(&self) -> bool {
        self.is_audio() && flv_tag_body::is_audio_sequence_header(&self.data)
    }

    /// Whether this tag carries a decodable video keyframe: an AVC (h264)
    /// NALU packet (`AVCPacketType` `0x01`) whose FLV frame type is keyframe,
    /// i.e. an IDR picture a decoder can start from.
    ///
    /// This is deliberately narrower than the FLV keyframe frame-type nibble
    /// alone: AVC sequence headers (`AVCPacketType` `0x00`) and end-of-sequence
    /// markers (`AVCPacketType` `0x02`) also arrive with the keyframe frame
    /// type, but neither is a decodable picture, so both return `false`.
    pub fn is_video_keyframe(&self) -> bool {
        self.is_video() && flv_tag_body::is_video_keyframe(&self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(tag_type: u8, data: &'static [u8]) -> FlvTag {
        FlvTag {
            header: FlvTagHeader {
                tag_type,
                data_size: data.len() as u32,
                timestamp: 0,
                timestamp_ext: 0,
                stream_id: 0,
            },
            data: bytes::Bytes::from_static(data),
            previous_tag_size: 0,
        }
    }

    #[test]
    fn classification_is_gated_on_tag_type() {
        // Payload bytes alone are not enough: the tag type must match.
        assert!(tag(0x09, &[0x17, 0x00]).is_video_sequence_header());
        assert!(!tag(0x08, &[0x17, 0x00]).is_video_sequence_header());
        assert!(tag(0x08, &[0xaf, 0x00]).is_audio_sequence_header());
        assert!(!tag(0x09, &[0xaf, 0x00]).is_audio_sequence_header());
    }

    #[test]
    fn video_keyframe_excludes_config_and_end_of_sequence() {
        assert!(tag(0x09, &[0x17, 0x01]).is_video_keyframe());
        // AVCPacketType 0x00 (sequence header) and 0x02 (end-of-sequence)
        // are keyframe frame-types but not decodable IDR frames.
        assert!(!tag(0x09, &[0x17, 0x00]).is_video_keyframe());
        assert!(!tag(0x09, &[0x17, 0x02]).is_video_keyframe());
    }
}
