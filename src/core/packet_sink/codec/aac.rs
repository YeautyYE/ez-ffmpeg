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
    /// configuration fails BEFORE any callback. The required ASC prefix
    /// (ISO/IEC 14496-3 §1.6.2.1) is parsed with a bounds-checked bit
    /// reader: audioObjectType (5 bits, escape value 31 followed by 6
    /// extension bits), samplingFrequencyIndex (4 bits, index 15 followed
    /// by a 24-bit explicit frequency), channelConfiguration (4 bits), and
    /// for the SBR/PS signaled types (AOT 5 and 29) the
    /// extensionSamplingFrequencyIndex with its own index-15 case.
    /// Truncation inside any field is a typed error — zero bits are never
    /// silently substituted.
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<Self, PacketSinkError> {
        let audio_object_type = parse_required_asc_prefix(extradata).map_err(|reason| {
            PacketSinkError::InvalidExtradata {
                stream_index,
                reason,
            }
        })?;
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

/// Bounds-checked MSB-first bit reader over an AudioSpecificConfig.
struct AscBits<'a> {
    data: &'a [u8],
    /// Next unread bit position.
    pos: usize,
}

impl<'a> AscBits<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Reads `count` bits (<= 32) or reports which `field` was truncated.
    fn read(&mut self, count: usize, field: &str) -> Result<u32, String> {
        debug_assert!(count <= 32);
        if self.pos + count > self.data.len() * 8 {
            return Err(format!(
                "AudioSpecificConfig truncated inside {field} ({} bits present, {} required)",
                self.data.len() * 8,
                self.pos + count
            ));
        }
        let mut value = 0u32;
        for _ in 0..count {
            let byte = self.data[self.pos / 8];
            let bit = (byte >> (7 - self.pos % 8)) & 1;
            value = (value << 1) | bit as u32;
            self.pos += 1;
        }
        Ok(value)
    }
}

/// Parses the required AudioSpecificConfig prefix (through the channel
/// configuration, plus the extension sampling-frequency index for AOT 5/29)
/// and returns the audio object type. ISO/IEC 14496-3 GetAudioObjectType()
/// plus the leading AudioSpecificConfig fields.
fn parse_required_asc_prefix(asc: &[u8]) -> Result<u32, String> {
    let mut bits = AscBits::new(asc);
    let mut aot = bits.read(5, "audioObjectType")?;
    if aot == 31 {
        aot = 32 + bits.read(6, "audioObjectTypeExt")?;
    }
    if aot == 0 {
        return Err("AudioSpecificConfig declares the null audio object type".to_string());
    }
    let freq_index = bits.read(4, "samplingFrequencyIndex")?;
    if freq_index == 15 {
        bits.read(24, "explicit samplingFrequency")?;
    }
    bits.read(4, "channelConfiguration")?;
    if aot == 5 || aot == 29 {
        let ext_index = bits.read(4, "extensionSamplingFrequencyIndex")?;
        if ext_index == 15 {
            bits.read(24, "explicit extensionSamplingFrequency")?;
        }
    }
    Ok(aot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_string_reads_the_audio_object_type() {
        // AAC-LC: AOT 2, 44.1 kHz (index 4), stereo -> 0x12 0x10 (13 required
        // bits fit in two bytes).
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // HE-AAC (SBR): AOT 5, 48 kHz (index 3), stereo, extension index 8 —
        // 17 required bits, three bytes.
        let runtime = AacRuntime::from_extradata(&[0x29, 0x94, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // Escape AOT: 31 escape + 6-bit extension 2 => AOT 34; with the
        // frequency index (4) and channel configuration (1) that is 19
        // required bits, three bytes.
        let runtime = AacRuntime::from_extradata(&[0xF8, 0x48, 0x20], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.34");
    }

    #[test]
    fn per_field_truncation_is_rejected_typed() {
        // (fixture, the field the truncation lands in)
        let cases: [(&[u8], &str); 6] = [
            (&[], "audioObjectType"),
            // AOT 2 + 3 bits of the frequency index.
            (&[0x12], "samplingFrequencyIndex"),
            // Escape marker + only 3 of the 6 extension bits.
            (&[0xF8], "audioObjectTypeExt"),
            // Escape AOT 34: 16 bits present, the 19-bit prefix cuts inside
            // the channel configuration.
            (&[0xF8, 0x40], "channelConfiguration"),
            // AOT 2 with frequency index 15: the 24-bit explicit frequency
            // needs 33 bits before the channel configuration.
            (&[0x17, 0x80], "explicit samplingFrequency"),
            // AOT 5 needs the extension sampling-frequency index: 17 bits
            // required, 16 present.
            (&[0x28, 0x10], "extensionSamplingFrequencyIndex"),
        ];
        for (bad, field) in cases {
            match AacRuntime::from_extradata(bad, 3) {
                Err(PacketSinkError::InvalidExtradata {
                    stream_index: 3,
                    reason,
                }) => {
                    assert!(
                        reason.contains(field),
                        "{bad:02X?}: expected truncation inside {field:?}, got {reason:?}"
                    );
                }
                Err(other) => panic!("{bad:02X?}: expected truncation, got {other:?}"),
                Ok(_) => panic!("{bad:02X?}: expected InvalidExtradata, got a valid runtime"),
            }
        }
        // AOT 5 with extension index 15 additionally needs the 24-bit
        // explicit extension frequency.
        match AacRuntime::from_extradata(&[0x28, 0x17, 0x80], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("explicit extensionSamplingFrequency"));
            }
            Err(other) => panic!("expected truncation, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
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
