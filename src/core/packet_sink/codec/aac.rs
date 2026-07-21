//! AAC strict-tier codec runtime: AudioSpecificConfig handling.
//!
//! AAC packets are raw frames passed through unchanged (every frame is a
//! random access point); the runtime owns the ASC baseline for S8 comparison,
//! derives the RFC 6381 codec string from the audio object type, and surfaces
//! the program_config_element channel count for the channelConfiguration-0
//! metadata cross-check.

use crate::error::PacketSinkError;

/// Per-stream AAC runtime state.
pub(crate) struct AacRuntime {
    /// The AudioSpecificConfig bytes (S8 baseline and delivered config).
    asc: Vec<u8>,
    /// Parsed audio object type (validated at construction).
    audio_object_type: u32,
    /// Channel count declared by the program_config_element when
    /// channelConfiguration is 0 (`None` for table-signaled layouts).
    pce_channels: Option<u32>,
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
    /// object type left standing is a General Audio type, the complete
    /// GASpecificConfig (see [`read_ga_specific_config`]), including the
    /// program_config_element that carries the layout when
    /// channelConfiguration is 0, followed by the 2-bit epConfig the ER
    /// object types append at the AudioSpecificConfig level. Reserved
    /// sampling-frequency indexes (13/14) are rejected, and so are the
    /// reserved channel configurations 8-10 and the out-of-table value 15
    /// (for the primary and extension fields alike). Truncation inside any
    /// field is a typed error — zero bits are never silently substituted.
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<Self, PacketSinkError> {
        let prefix = parse_required_asc_prefix(extradata).map_err(|reason| {
            PacketSinkError::InvalidExtradata {
                stream_index,
                reason,
            }
        })?;
        Ok(Self {
            asc: extradata.to_vec(),
            audio_object_type: prefix.audio_object_type,
            pce_channels: prefix.pce_channels,
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

    /// Channel count declared by the ASC's program_config_element; `None`
    /// when channelConfiguration signals the layout by table instead. The
    /// strict path compares this against the advertised stream metadata —
    /// with channelConfiguration 0 the PCE is the configuration's only
    /// channel declaration, so a disagreeing `channels` field would hand
    /// consumers contradictory metadata.
    pub(crate) fn pce_channel_count(&self) -> Option<u32> {
        self.pce_channels
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

    /// Total bits in the config, computed in u64: `len * 8` in usize would
    /// wrap on 32-bit targets for buffers past 512 MiB, silently turning an
    /// out-of-bounds demand into an in-bounds one.
    fn bit_len(&self) -> u64 {
        self.data.len() as u64 * 8
    }

    /// Peeks `count` bits (<= 32) without consuming, zero-padded past the
    /// end of the config — mirroring FFmpeg's `show_bits` semantics for the
    /// W6132 Annex look-ahead, where the guard must not itself demand bits.
    fn peek(&self, count: usize) -> u32 {
        let mut value = 0u32;
        for offset in 0..count {
            let pos = self.pos as u64 + offset as u64;
            let bit = if pos < self.bit_len() {
                // In bounds, so pos/8 < data.len() and the casts are exact.
                (self.data[(pos / 8) as usize] >> (7 - pos % 8)) & 1
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
        if self.pos as u64 + count as u64 > self.bit_len() {
            return Err(format!(
                "AudioSpecificConfig truncated inside {field} ({} bits present, {} required)",
                self.bit_len(),
                self.pos as u64 + count as u64
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

/// Validates a complete GASpecificConfig (ISO/IEC 14496-3 §4.4.1, Table
/// 4.1): frameLengthFlag (1 bit), dependsOnCoreCoder (1 bit) — when set,
/// the 14-bit coreCoderDelay — extensionFlag (1 bit), the 3-bit layerNr
/// for the scalable object types (6 and 20), the program_config_element
/// when `channel_config` is 0 (preceded by its 4-bit
/// element_instance_tag), and, when extensionFlag is set, the
/// AOT-dependent extension fields — numOfSubFrame (5 bits) and
/// layer_length (11 bits) for ER BSAC (22), the three data-resilience
/// flags for ER AAC LC/LTP/Scalable/LD (17/19/20/23) — closed by
/// extensionFlag3 (1 bit, contents reserved for version 3).
///
/// Two deliberate departures from a literal Table 4.1 reading, both
/// mirroring FFmpeg's decode_ga_specific_config
/// (libavcodec/aac/aacdec.c):
/// - layerNr is read BEFORE the program_config_element; Table 4.1 places
///   it after. The orders only diverge for a scalable object type with
///   channelConfiguration 0, which no known encoder emits, so the decoder
///   that would consume the stream is the arbiter.
/// - Table 4.1 says extensionFlag "shall be 1" for the ER object types;
///   FFmpeg branches on the actual flag value instead of enforcing the
///   shall, and so does this parser.
///
/// Returns the channel count declared by the program_config_element, or
/// `None` when `channel_config` signals the layout by table.
fn read_ga_specific_config(
    bits: &mut AscBits<'_>,
    aot: u32,
    channel_config: u32,
) -> Result<Option<u32>, String> {
    bits.read(1, "frameLengthFlag")?;
    if bits.read(1, "dependsOnCoreCoder")? == 1 {
        bits.read(14, "coreCoderDelay")?;
    }
    let extension_flag = bits.read(1, "extensionFlag")?;
    // AAC Scalable (6) and ER AAC Scalable (20).
    if aot == 6 || aot == 20 {
        bits.read(3, "layerNr")?;
    }
    let pce_channels = if channel_config == 0 {
        bits.read(4, "element_instance_tag")?;
        Some(read_program_config_element(bits)?)
    } else {
        None
    };
    if extension_flag == 1 {
        if aot == 22 {
            bits.read(5, "numOfSubFrame")?;
            bits.read(11, "layer_length")?;
        }
        if matches!(aot, 17 | 19 | 20 | 23) {
            bits.read(1, "aacSectionDataResilienceFlag")?;
            bits.read(1, "aacScalefactorDataResilienceFlag")?;
            bits.read(1, "aacSpectralDataResilienceFlag")?;
        }
        bits.read(1, "extensionFlag3")?;
    }
    Ok(pce_channels)
}

/// Validates one program_config_element (ISO/IEC 14496-3 §4.4.1.1, Table
/// 4.2) and returns the output channel count it declares: front, side and
/// back elements contribute two channels when their is_cpe bit is set and
/// one otherwise, LFE elements one each; coupling-channel elements are
/// not output channels (FFmpeg's count_channels in
/// libavcodec/aac/aacdec.c likewise excludes them). Field-for-field this
/// mirrors FFmpeg's decode_pce. The byte alignment before the comment
/// field is relative to the START of the AudioSpecificConfig — this
/// reader's bit 0 — matching FFmpeg's decode_audio_specific_config, which
/// passes reference alignment 0 for an extradata ASC (an in-band LATM
/// config would need the config's own start offset instead). The comment
/// field is skipped as opaque bytes: the height extension FFmpeg reads
/// from inside it rearranges the layout without changing the channel
/// count.
fn read_program_config_element(bits: &mut AscBits<'_>) -> Result<u32, String> {
    bits.read(2, "PCE object_type")?;
    bits.read(4, "PCE sampling_frequency_index")?;
    let num_front = bits.read(4, "num_front_channel_elements")?;
    let num_side = bits.read(4, "num_side_channel_elements")?;
    let num_back = bits.read(4, "num_back_channel_elements")?;
    let num_lfe = bits.read(2, "num_lfe_channel_elements")?;
    let num_assoc_data = bits.read(3, "num_assoc_data_elements")?;
    let num_cc = bits.read(4, "num_valid_cc_elements")?;
    if bits.read(1, "mono_mixdown_present")? == 1 {
        bits.read(4, "mono_mixdown_element_number")?;
    }
    if bits.read(1, "stereo_mixdown_present")? == 1 {
        bits.read(4, "stereo_mixdown_element_number")?;
    }
    if bits.read(1, "matrix_mixdown_idx_present")? == 1 {
        bits.read(2, "matrix_mixdown_idx")?;
        bits.read(1, "pseudo_surround_enable")?;
    }
    let mut channels = 0u32;
    for _ in 0..num_front {
        channels += 1 + bits.read(1, "front_element_is_cpe")?;
        bits.read(4, "front_element_tag_select")?;
    }
    for _ in 0..num_side {
        channels += 1 + bits.read(1, "side_element_is_cpe")?;
        bits.read(4, "side_element_tag_select")?;
    }
    for _ in 0..num_back {
        channels += 1 + bits.read(1, "back_element_is_cpe")?;
        bits.read(4, "back_element_tag_select")?;
    }
    for _ in 0..num_lfe {
        channels += 1;
        bits.read(4, "lfe_element_tag_select")?;
    }
    for _ in 0..num_assoc_data {
        bits.read(4, "assoc_data_element_tag_select")?;
    }
    for _ in 0..num_cc {
        bits.read(1, "cc_element_is_ind_sw")?;
        bits.read(4, "valid_cc_element_tag_select")?;
    }
    let misalignment = bits.pos % 8;
    if misalignment != 0 {
        bits.read(8 - misalignment, "PCE byte alignment")?;
    }
    let comment_bytes = bits.read(8, "comment_field_bytes")?;
    for _ in 0..comment_bytes {
        bits.read(8, "comment_field_data")?;
    }
    if channels == 0 {
        return Err("program_config_element declares zero output channels".to_string());
    }
    Ok(channels)
}

/// The validated facts the runtime keeps from the ASC prefix.
struct AscPrefix {
    audio_object_type: u32,
    /// Channel count from the program_config_element (channelConfiguration
    /// 0 only).
    pce_channels: Option<u32>,
}

/// Rejects the channel-configuration values no layout table row backs:
/// 8-10 are reserved (FFmpeg's ff_mpeg4audio_channels in
/// libavcodec/mpeg4audio.c maps them to zero channels, and
/// ff_aac_set_default_channel_config in libavcodec/aac/aacdec.c accepts
/// only 1-7 and 11-14 as defaults) and 15 is outside the 4-bit table
/// entirely. Value 0 passes here: the layout then lives in the
/// program_config_element the General Audio parse reads.
fn check_channel_config(value: u32, field: &str) -> Result<(), String> {
    match value {
        8..=10 => Err(format!("reserved {field} {value}")),
        15 => Err(format!("{field} 15 is outside the channel table")),
        _ => Ok(()),
    }
}

/// Parses the required AudioSpecificConfig prefix and returns the audio
/// object type plus the PCE-declared channel count — mirroring
/// `ff_mpeg4audio_get_config_gb` (FFmpeg libavcodec/mpeg4audio.c) through
/// the direct-SBR/PS extension block: audioObjectType,
/// samplingFrequencyIndex (with the index-15 explicit case),
/// channelConfiguration, and for AOT 5/29 the
/// extensionSamplingFrequencyIndex plus a SECOND GetAudioObjectType,
/// whose value 22 (ER BSAC) is followed by the
/// extensionChannelConfiguration. Reserved channel configurations (8-10)
/// and the out-of-table value 15 are rejected for the primary and
/// extension fields alike. When the object type left standing — the
/// secondary one when the extension block was parsed — is a General
/// Audio type, the complete GASpecificConfig is validated (including the
/// program_config_element that carries the layout when
/// channelConfiguration is 0), followed by the 2-bit epConfig the ER
/// object types (17, 19-23) append at the AudioSpecificConfig level
/// (ISO/IEC 14496-3 §1.6.2.1); epConfig 2 and 3 are rejected because an
/// ErrorProtectionSpecificConfig follows that this parser cannot bound
/// (FFmpeg's decoder refuses every nonzero epConfig as unimplemented,
/// libavcodec/aac/aacdec.c). channelConfiguration 0 outside a General
/// Audio configuration is rejected: no reachable program_config_element
/// can supply the layout, and downstream consumers need explicit
/// channels. AOT 29 honors the W6132 Annex MP3onMP4 look-ahead: when the
/// next 3 bits have a low bit set and the following 6 are zero, the
/// extension block is absent.
fn parse_required_asc_prefix(asc: &[u8]) -> Result<AscPrefix, String> {
    let mut bits = AscBits::new(asc);
    let aot = read_object_type(&mut bits, "audioObjectType", "audioObjectTypeExt")?;
    if aot == 0 {
        return Err("AudioSpecificConfig declares the null audio object type".to_string());
    }
    read_sampling_frequency(&mut bits, "samplingFrequencyIndex", "explicit samplingFrequency")?;
    let channel_config = bits.read(4, "channelConfiguration")?;
    check_channel_config(channel_config, "channelConfiguration")?;
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
            check_channel_config(extension_channel_config, "extensionChannelConfiguration")?;
        }
        base_object_type = secondary;
    }
    let pce_channels = if is_ga_object_type(base_object_type) {
        let pce_channels = read_ga_specific_config(&mut bits, base_object_type, channel_config)?;
        // ER object types with a GASpecificConfig: ER AAC LC/LTP (17/19),
        // ER AAC Scalable (20), ER TwinVQ (21), ER BSAC (22), ER AAC LD
        // (23).
        if matches!(base_object_type, 17 | 19..=23) {
            let ep_config = bits.read(2, "epConfig")?;
            if ep_config >= 2 {
                return Err(format!(
                    "epConfig {ep_config} is followed by an \
                     ErrorProtectionSpecificConfig this parser cannot bound"
                ));
            }
        }
        pce_channels
    } else if channel_config == 0 {
        return Err(
            "channelConfiguration 0 defers the layout to a program_config_element, \
             which only a General Audio configuration carries"
                .to_string(),
        );
    } else {
        None
    };
    Ok(AscPrefix {
        audio_object_type: aot,
        pce_channels,
    })
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
        // extensionChannelConfiguration (1), the GASpecificConfig with a
        // clear extensionFlag, and the 2-bit ER epConfig (0) complete the
        // prefix — 31 required bits, four bytes.
        let runtime = AacRuntime::from_extradata(&[0x29, 0x94, 0x58, 0x40], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // The same configuration with extensionFlag SET: numOfSubFrame (5),
        // layer_length (11) and extensionFlag3 push the epConfig to bits
        // 46-47 — 48 required bits, six bytes.
        let runtime =
            AacRuntime::from_extradata(&[0x29, 0x94, 0x58, 0x48, 0x00, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.5");
        // Channel configuration 11 (6.1) sits past the reserved 8-10 gap
        // and is a legal table row.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x58], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        // AAC Scalable (AOT 6) carries a 3-bit layerNr after the
        // GASpecificConfig head — 19 required bits, three bytes.
        let runtime = AacRuntime::from_extradata(&[0x32, 0x10, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.6");
        // ER AAC LC (AOT 17) with extensionFlag set: the three resilience
        // flags, extensionFlag3, and the ASC-level epConfig (0) — 22
        // required bits, three bytes.
        let runtime = AacRuntime::from_extradata(&[0x8A, 0x09, 0x00], 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.17");
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
        let cases: [(&[u8], &str); 16] = [
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
            // AAC-LC with extensionFlag SET: extensionFlag3 is bit 16 —
            // one past two bytes.
            (&[0x12, 0x11], "extensionFlag3"),
            // AAC Scalable (AOT 6): the 3-bit layerNr needs bits 16-18.
            (&[0x32, 0x10], "layerNr"),
            // ER AAC LC (AOT 17) with extensionFlag set: the resilience
            // flags start at bit 16.
            (&[0x8A, 0x09], "aacSectionDataResilienceFlag"),
            // ER AAC LC with extensionFlag clear: the ASC-level epConfig
            // needs bits 16-17.
            (&[0x8A, 0x08], "epConfig"),
            // SBR secondary ER BSAC with extensionFlag set: numOfSubFrame
            // spans bits 29-33, three of its five bits exist.
            (&[0x29, 0x94, 0x58, 0x48], "numOfSubFrame"),
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
        // channelConfigurations 8-10 are reserved table rows: no layout
        // backs them, so a stream declaring one is unusable.
        for (bad, value) in [
            (&[0x12u8, 0x40][..], 8),
            (&[0x12, 0x48][..], 9),
            (&[0x12, 0x50][..], 10),
        ] {
            match AacRuntime::from_extradata(bad, 3) {
                Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                    assert!(
                        reason.contains(&format!("reserved channelConfiguration {value}")),
                        "{bad:02X?}: got {reason:?}"
                    );
                }
                Err(other) => panic!("{bad:02X?}: expected InvalidExtradata, got {other:?}"),
                Ok(_) => panic!("{bad:02X?}: expected InvalidExtradata, got a valid runtime"),
            }
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
        // ... and the reserved rows 8-10: the same fixture with extension
        // field 8.
        match AacRuntime::from_extradata(&[0x29, 0x94, 0x5A, 0x00], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(
                    reason.contains("reserved extensionChannelConfiguration 8"),
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
        // epConfig 2 (and 3) appends an ErrorProtectionSpecificConfig this
        // parser cannot bound: ER AAC LC with epConfig 2.
        match AacRuntime::from_extradata(&[0x8A, 0x08, 0x80], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("epConfig 2"), "got {reason:?}");
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
    }

    #[test]
    fn channel_configuration_zero_takes_the_layout_from_the_pce() {
        // FFmpeg's native AAC encoder forced into PCE signaling:
        //   ffmpeg -f lavfi -i anullsrc=channel_layout=quad:sample_rate=48000 \
        //          -c:a aac -aac_pce 1 -bitexact -frames:a 3 out.mp4
        // AOT 2, 48 kHz, channelConfiguration 0; the PCE declares one front
        // CPE and one back CPE (4 channels), byte-aligns relative to the
        // ASC start, and carries the 4-byte "Lavc" comment; the trailing
        // 0x2b7 sync extension is outside the required prefix.
        let ffmpeg_quad: [u8; 16] = [
            0x11, 0x80, 0x04, 0xC4, 0x04, 0x00, 0x21, 0x10, 0x04, 0x4C, 0x61, 0x76, 0x63, 0x56,
            0xE5, 0x00,
        ];
        let runtime = AacRuntime::from_extradata(&ffmpeg_quad, 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        assert_eq!(runtime.pce_channel_count(), Some(4));
        // Minimal hand-built form: one front CPE (stereo), no mixdowns,
        // empty comment; the element map ends at bit 55, so the byte
        // alignment consumes exactly one bit.
        let minimal_stereo: [u8; 8] = [0x12, 0x00, 0x05, 0x04, 0x00, 0x00, 0x20, 0x00];
        let runtime = AacRuntime::from_extradata(&minimal_stereo, 0).unwrap();
        assert_eq!(runtime.codec_string(), "mp4a.40.2");
        assert_eq!(runtime.pce_channel_count(), Some(2));
        // A table-signaled layout reports no PCE channel count.
        let runtime = AacRuntime::from_extradata(&[0x12, 0x10], 0).unwrap();
        assert_eq!(runtime.pce_channel_count(), None);
        // channelConfiguration 0 whose config ends before the PCE is a
        // truncation, not a pass: the old two-byte form now demands the
        // element_instance_tag.
        match AacRuntime::from_extradata(&[0x12, 0x00], 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("element_instance_tag"), "got {reason:?}");
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // A PCE declaring no front/side/back/LFE elements describes zero
        // output channels — no real stream can match it.
        let zero_channels: [u8; 8] = [0x12, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00];
        match AacRuntime::from_extradata(&zero_channels, 3) {
            Err(PacketSinkError::InvalidExtradata { reason, .. }) => {
                assert!(reason.contains("zero output channels"), "got {reason:?}");
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("expected InvalidExtradata, got a valid runtime"),
        }
        // Outside a General Audio configuration no program_config_element
        // is reachable, so channelConfiguration 0 stays rejected (escape
        // AOT 34).
        match AacRuntime::from_extradata(&[0xF8, 0x48, 0x00], 3) {
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
