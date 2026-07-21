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
    /// by a 24-bit explicit frequency), channelConfiguration (4 bits), for
    /// the SBR/PS signaled types (AOT 5 and 29) the full direct extension
    /// block — extensionSamplingFrequencyIndex (with its own index-15
    /// case), the second GetAudioObjectType, and, when that secondary type
    /// is ER BSAC (22), the extensionChannelConfiguration — and, when the
    /// object type left standing is a General Audio type, the fixed
    /// GASpecificConfig head: frameLengthFlag, dependsOnCoreCoder (with
    /// its 14-bit coreCoderDelay when set), and extensionFlag. Reserved
    /// sampling-frequency indexes (13/14) are rejected, and so are channel
    /// configurations 0 (the layout then lives in a program_config_element
    /// this parser does not read — consumers need explicit channels) and
    /// 15 (outside the channel table; the extension field likewise).
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

    /// Peeks `count` bits (<= 32) without consuming, zero-padded past the
    /// end of the config — mirroring FFmpeg's `show_bits` semantics for the
    /// W6132 Annex look-ahead, where the guard must not itself demand bits.
    fn peek(&self, count: usize) -> u32 {
        let mut value = 0u32;
        for offset in 0..count {
            let pos = self.pos + offset;
            let bit = if pos < self.data.len() * 8 {
                (self.data[pos / 8] >> (7 - pos % 8)) & 1
            } else {
                0
            };
            value = (value << 1) | bit as u32;
        }
        value
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

/// One ISO/IEC 14496-3 GetAudioObjectType(): 5 bits, escape value 31
/// followed by 6 extension bits.
fn read_object_type(bits: &mut AscBits<'_>, field: &str, ext_field: &str) -> Result<u32, String> {
    let aot = bits.read(5, field)?;
    if aot == 31 {
        return Ok(32 + bits.read(6, ext_field)?);
    }
    Ok(aot)
}

/// One sampling-frequency field: a 4-bit index whose value 15 is followed
/// by a 24-bit explicit frequency. Indexes 13 and 14 are reserved
/// (FFmpeg's decoder rejects them via its `sampling_index > 12` check).
fn read_sampling_frequency(
    bits: &mut AscBits<'_>,
    field: &str,
    explicit_field: &str,
) -> Result<(), String> {
    let index = bits.read(4, field)?;
    if index == 13 || index == 14 {
        return Err(format!("reserved {field} {index}"));
    }
    if index == 15 {
        bits.read(24, explicit_field)?;
    }
    Ok(())
}

/// The object types whose configuration payload is GASpecificConfig
/// (the General Audio branch of the ISO/IEC 14496-3 §1.6.2.1 switch):
/// AAC main/LC/SSR/LTP, Scalable, TwinVQ, and the ER variants ER AAC
/// LC/LTP/Scalable, ER TwinVQ, ER BSAC, and ER AAC LD.
fn is_ga_object_type(aot: u32) -> bool {
    matches!(aot, 1..=4 | 6 | 7 | 17 | 19..=23)
}

/// Validates the fixed head of GASpecificConfig (ISO/IEC 14496-3 §4.4.1):
/// frameLengthFlag (1 bit), dependsOnCoreCoder (1 bit) — when set, the
/// 14-bit coreCoderDelay — and extensionFlag (1 bit). Fields beyond
/// extensionFlag (layerNr, the extension payloads) are not validated.
fn read_ga_specific_config_prefix(bits: &mut AscBits<'_>) -> Result<(), String> {
    bits.read(1, "frameLengthFlag")?;
    if bits.read(1, "dependsOnCoreCoder")? == 1 {
        bits.read(14, "coreCoderDelay")?;
    }
    bits.read(1, "extensionFlag")?;
    Ok(())
}

/// Parses the required AudioSpecificConfig prefix and returns the audio
/// object type — mirroring `ff_mpeg4audio_get_config_gb` (FFmpeg
/// libavcodec/mpeg4audio.c) through the direct-SBR/PS extension block:
/// audioObjectType, samplingFrequencyIndex (with the index-15 explicit
/// case), channelConfiguration, and for AOT 5/29 the
/// extensionSamplingFrequencyIndex plus a SECOND GetAudioObjectType,
/// whose value 22 (ER BSAC) is followed by the
/// extensionChannelConfiguration. Channel configuration 0 is rejected:
/// the layout then lives in a program_config_element inside the codec
/// payload, which this parser does not read, and downstream consumers
/// need explicit channels. Channel configuration 15 is outside the ISO
/// channel table and rejected for the primary and extension fields
/// alike. When the object type left standing — the secondary one when
/// the extension block was parsed — is a General Audio type, the
/// GASpecificConfig head is validated too. AOT 29 honors the W6132 Annex
/// MP3onMP4 look-ahead: when the next 3 bits have a low bit set and the
/// following 6 are zero, the extension block is absent.
fn parse_required_asc_prefix(asc: &[u8]) -> Result<u32, String> {
    let mut bits = AscBits::new(asc);
    let aot = read_object_type(&mut bits, "audioObjectType", "audioObjectTypeExt")?;
    if aot == 0 {
        return Err("AudioSpecificConfig declares the null audio object type".to_string());
    }
    read_sampling_frequency(&mut bits, "samplingFrequencyIndex", "explicit samplingFrequency")?;
    let channel_config = bits.read(4, "channelConfiguration")?;
    if channel_config == 0 {
        return Err(
            "channelConfiguration 0 defers the layout to a program_config_element, \
             which this parser does not read"
                .to_string(),
        );
    }
    if channel_config == 15 {
        return Err("channelConfiguration 15 is outside the channel table".to_string());
    }
    let parse_extension = match aot {
        5 => true,
        // W6132 Annex YYYY draft MP3onMP4 (mirrors the FFmpeg guard).
        29 => !(bits.peek(3) & 0x03 != 0 && bits.peek(9) & 0x3F == 0),
        _ => false,
    };
    let mut base_object_type = aot;
    if parse_extension {
        read_sampling_frequency(
            &mut bits,
            "extensionSamplingFrequencyIndex",
            "explicit extensionSamplingFrequency",
        )?;
        let secondary = read_object_type(
            &mut bits,
            "extension audioObjectType",
            "extension audioObjectTypeExt",
        )?;
        if secondary == 0 {
            return Err("extension audioObjectType is the null object type".to_string());
        }
        if secondary == 22 {
            let extension_channel_config = bits.read(4, "extensionChannelConfiguration")?;
            if extension_channel_config == 15 {
                return Err(
                    "extensionChannelConfiguration 15 is outside the channel table".to_string(),
                );
            }
        }
        base_object_type = secondary;
    }
    if is_ga_object_type(base_object_type) {
        read_ga_specific_config_prefix(&mut bits)?;
    }
    Ok(aot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_string_reads_the_audio_object_type() {
        // AAC-LC: AOT 2, 44.1 kHz (index 4), stereo -> 0x12 0x10. The
        // all-zero GASpecificConfig head (frameLengthFlag,
        // dependsOnCoreCoder, extensionFlag) lands the 16 required bits
        // exactly on the two-byte boundary.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // The same configuration with dependsOnCoreCoder set: the 14-bit
        // coreCoderDelay pushes extensionFlag to bit 29, four bytes.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x12, 0x00, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // HE-AAC (SBR): AOT 5, 48 kHz (index 3), stereo, extension index 8,
        // secondary object type AAC-LC (2) plus its GASpecificConfig head
        // — 25 required bits, four bytes.
        let runtime = AacRuntime::from_extradata(&[0x29, 0x94, 0x08, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // Direct SBR with secondary ER BSAC (22): the
        // extensionChannelConfiguration (1) and the BSAC GASpecificConfig
        // head complete the prefix — 29 required bits, four bytes.
        let runtime = AacRuntime::from_extradata(&[0x29, 0x94, 0x58, 0x40], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // AOT 29 tripping the W6132 MP3onMP4 look-ahead: no extension block
        // follows (and 29 is not a General Audio type, so no
        // GASpecificConfig either) — 13 bits suffice.
        let runtime = AacRuntime::from_extradata(&[0xEA, 0x0A, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.29");
        // The MINIMAL two-byte form pins the look-ahead's zero-padding at
        // the end of the buffer: only 3 of the peeked 9 bits physically
        // exist (the guard, like FFmpeg's show_bits, must not itself
        // demand bits).
        let runtime = AacRuntime::from_extradata(&[0xEA, 0x0A], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.29");
        // Escape AOT: 31 escape + 6-bit extension 2 => AOT 34; with the
        // frequency index (4) and channel configuration (1) that is 19
        // required bits, three bytes (escaped types are 32+, never in the
        // General Audio branch).
        let runtime = AacRuntime::from_extradata(&[0xF8, 0x48, 0x20], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.34");
    }

    #[test]
    fn per_field_truncation_is_rejected_typed() {
        // (fixture, the field the truncation lands in)
        let cases: [(&[u8], &str); 11] = [
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
            // A byte-aligned cut INSIDE a non-escape secondary object type
            // is geometrically unreachable (it spans bits 17-21 of a
            // 24-bit third byte), so the secondary field's truncation
            // coverage is the escape path: secondary escape marker 11111
            // with only 3 of its 6 extension bits present.
            (&[0x29, 0x94, 0x7C], "extension audioObjectTypeExt"),
            // Secondary ER BSAC (22) demands the extension channel
            // configuration: 24 bits present, 26 required.
            (&[0x29, 0x94, 0x58], "extensionChannelConfiguration"),
            // The GASpecificConfig head starts at a bit offset that is 5,
            // 6, or 2 (mod 8) — 13/37 for a plain GA type, 22/46/70 behind
            // an SBR secondary, 26/50/74 behind an ER BSAC secondary — so
            // a byte-aligned buffer can never end exactly AT
            // frameLengthFlag or dependsOnCoreCoder; the head's truncation
            // coverage lives in coreCoderDelay and extensionFlag.
            //
            // AAC-LC with dependsOnCoreCoder set: the 14-bit coreCoderDelay
            // needs bits 15-28, only bit 15 exists.
            (&[0x12, 0x12], "coreCoderDelay"),
            // SBR secondary AAC-LC (offset 22, the mod-8 = 6 path): the
            // head's first two flags are bits 22/23 and dependsOnCoreCoder
            // is clear, so extensionFlag is bit 24 — one past three bytes.
            (&[0x29, 0x94, 0x08], "extensionFlag"),
            // The same secondary with dependsOnCoreCoder SET: the delay
            // spans bits 24-37, eight of its fourteen bits exist.
            (&[0x29, 0x94, 0x09, 0x00], "coreCoderDelay"),
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
        // The null audio object type is not a usable configuration — for
        // the primary AND the secondary (direct-SBR) object type.
        assert!(matches!(
            AacRuntime::from_extradata(&[0x00, 0x10], 3),
            Err(PacketSinkError::InvalidExtradata { .. })
        ));
        match AacRuntime::from_extradata(&[0x29, 0x94, 0x00], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("null object type"), "got {reason:?}");
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
    }

    #[test]
    fn reserved_and_out_of_table_field_values_are_rejected() {
        // Reserved sampling-frequency index 13 (and 14): AOT 2, index 13.
        match AacRuntime::from_extradata(&[0x16, 0x90], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(
                    reason.contains("reserved samplingFrequencyIndex 13"),
                    "got {reason:?}"
                );
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // channelConfiguration 15 is outside the ISO channel table.
        match AacRuntime::from_extradata(&[0x12, 0x78], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("channelConfiguration 15"), "got {reason:?}");
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // channelConfiguration 0 parks the layout in a
        // program_config_element this parser cannot reach.
        match AacRuntime::from_extradata(&[0x12, 0x00], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(
                    reason.contains("channelConfiguration 0")
                        && reason.contains("program_config_element"),
                    "got {reason:?}"
                );
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // The out-of-table rule applies to the ER BSAC extension channel
        // configuration too: AOT 5, secondary 22, extension field 15.
        match AacRuntime::from_extradata(&[0x29, 0x94, 0x5B, 0xC0], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(
                    reason.contains("extensionChannelConfiguration 15"),
                    "got {reason:?}"
                );
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // The reserved rule applies to the extension index too: AOT 5,
        // main index 3, stereo, extension index 13.
        match AacRuntime::from_extradata(&[0x29, 0x96, 0x88], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(
                    reason.contains("reserved extensionSamplingFrequencyIndex 13"),
                    "got {reason:?}"
                );
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
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
