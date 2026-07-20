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
}

impl AacRuntime {
    pub(crate) fn from_extradata(extradata: &[u8]) -> Self {
        Self {
            asc: extradata.to_vec(),
        }
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
        format!("mp4a.40.{}", audio_object_type(&self.asc))
    }
}

/// Audio object type from an AudioSpecificConfig (ISO 14496-3 GetAudioObjectType).
fn audio_object_type(asc: &[u8]) -> u32 {
    let Some(&first) = asc.first() else { return 0 };
    let aot = (first >> 3) as u32;
    if aot != 31 {
        return aot;
    }
    // Escape: 6 more bits follow the 5-bit escape marker.
    let ext = ((first as u32 & 0x07) << 3) | asc.get(1).map_or(0, |&b| (b >> 5) as u32);
    32 + ext
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_string_reads_the_audio_object_type() {
        // AAC-LC: AOT 2, 44.1 kHz, stereo -> 0x12 0x10.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10]);
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // HE-AAC (SBR): AOT 5 -> first byte 0x2B-ish (5 << 3 = 0x28).
        let runtime = AacRuntime::from_extradata(&[0x28, 0x10]);
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
    }

    #[test]
    fn redundant_asc_passes_and_change_errors() {
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10]);
        assert!(runtime.check_new_extradata(&[0x12, 0x10], 1).is_ok());
        assert!(matches!(
            runtime.check_new_extradata(&[0x11, 0x90], 1),
            Err(PacketSinkError::ConfigChange { stream_index: 1, .. })
        ));
    }
}
