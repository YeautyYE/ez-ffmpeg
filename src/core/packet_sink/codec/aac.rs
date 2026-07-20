//! AAC strict-tier codec runtime: AudioSpecificConfig handling.
//!
//! AAC packets are raw frames passed through unchanged (every frame is a
//! random access point); the runtime owns the ASC baseline for S8 comparison
//! and derives the RFC 6381 codec string from the audio object type.

use crate::error::PacketSinkError;

/// Per-stream AAC runtime state.
pub(crate) struct AacRuntime {
    /// The AudioSpecificConfig bytes (S8 baseline and delivered config).
    asc: Vec<u8>,
    /// Parsed audio object type (validated at construction).
    audio_object_type: u32,
}

impl AacRuntime {
    /// Builds the runtime from finalized encoder extradata, validating the
    /// AudioSpecificConfig up front — the strict tier promises malformed
    /// configuration fails BEFORE any callback. A minimal ASC is two bytes
    /// (5-bit AOT + frequency index + channel configuration; the escape AOT
    /// additionally needs its 6 extension bits, which span into byte 1), so
    /// truncation can never silently substitute zero bits.
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<Self, PacketSinkError> {
        if extradata.len() < 2 {
            return Err(PacketSinkError::InvalidExtradata {
                stream_index,
                reason: format!(
                    "AudioSpecificConfig too short ({} bytes; at least 2 required)",
                    extradata.len()
                ),
            });
        }
        let audio_object_type = audio_object_type(extradata);
        if audio_object_type == 0 {
            return Err(PacketSinkError::InvalidExtradata {
                stream_index,
                reason: "AudioSpecificConfig declares the null audio object type".to_string(),
            });
        }
        Ok(Self {
            asc: extradata.to_vec(),
            audio_object_type,
        })
    }

    /// S8: a `NEW_EXTRADATA` announcement — byte-equal ASC is redundant and
    /// passes; anything else is a mid-stream configuration change.
    pub(crate) fn check_new_extradata(
        &self,
        bytes: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        if bytes != self.asc.as_slice() {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "NEW_EXTRADATA differs from the stream configuration".to_string(),
            });
        }
        Ok(())
    }

    /// The RFC 6381 codec string, `mp4a.40.X`, where X is the ASC's
    /// audioObjectType (5-bit field, escape value 31 extends by 6 bits).
    pub(crate) fn codec_string(&self) -> String {
        format!("mp4a.40.{}", self.audio_object_type)
    }
}

/// Audio object type from an AudioSpecificConfig (ISO 14496-3
/// GetAudioObjectType). Callers validated `asc.len() >= 2`, so the escape
/// extension bits are always genuinely present.
fn audio_object_type(asc: &[u8]) -> u32 {
    let first = asc[0];
    let aot = (first >> 3) as u32;
    if aot != 31 {
        return aot;
    }
    // Escape: 6 more bits follow the 5-bit escape marker.
    let ext = ((first as u32 & 0x07) << 3) | (asc[1] >> 5) as u32;
    32 + ext
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_string_reads_the_audio_object_type() {
        // AAC-LC: AOT 2, 44.1 kHz, stereo -> 0x12 0x10.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // HE-AAC (SBR): AOT 5 -> first byte 5 << 3 = 0x28.
        let runtime = AacRuntime::from_extradata(&[0x28, 0x10], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // Escape AOT: 31 escape + extension 2 => AOT 34 (byte0 = 0xF8,
        // extension bits 000|010 across the byte boundary).
        let runtime = AacRuntime::from_extradata(&[0xF8, 0x40], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.34");
    }

    #[test]
    fn malformed_asc_is_rejected_typed() {
        // Empty and one-byte configs are truncation, including the escape
        // AOT whose extension bits would otherwise be fabricated as zero.
        for bad in [&[][..], &[0x12][..], &[0xF8][..]] {
            assert!(matches!(
                AacRuntime::from_extradata(bad, 3),
                Err(PacketSinkError::InvalidExtradata {
                    stream_index: 3,
                    ..
                })
            ));
        }
        // The null audio object type is not a usable configuration.
        assert!(matches!(
            AacRuntime::from_extradata(&[0x00, 0x10], 3),
            Err(PacketSinkError::InvalidExtradata { .. })
        ));
    }

    #[test]
    fn redundant_asc_passes_and_change_errors() {
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10], 1).unwrap();
        assert!(runtime.check_new_extradata(&[0x12, 0x10], 1).is_ok());
        assert!(matches!(
            runtime.check_new_extradata(&[0x11, 0x90], 1),
            Err(PacketSinkError::ConfigChange { stream_index: 1, .. })
        ));
    }
}
