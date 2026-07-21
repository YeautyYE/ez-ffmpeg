//! H.264/AVC strict-tier codec runtime: configuration (avcC) handling and
//! per-packet access-unit normalization.
//!
//! Splits into two layers:
//! * record functions — parameter-set extraction from Annex-B or avcC
//!   wrappers, avcC synthesis mirroring `ff_isom_write_avcc` (including the
//!   chroma/bit-depth extension), the SPS bit reader;
//! * [`AvcRuntime`] — the per-stream state machine the orchestrator drives:
//!   payload normalization via the streaming NAL walkers (a census walk
//!   then a write walk for Annex-B input), IDR classification, S8
//!   parameter-set fingerprinting and the in-band policy.

use super::super::nal_framing::{
    push_length_prefixed, walk_annexb, walk_length_prefixed, NAL_LENGTH_SIZE, NAL_PPS, NAL_SPS,
};
use crate::error::PacketSinkError;

/// Parsed parameter sets of one H.264 configuration, in original order.
/// avcC synthesis consumes this form (movenc preserves wrapper order); the S8
/// fingerprint uses [`Self::into_canonical`], which is wrapper- AND
/// order-independent (the same sets can arrive as Annex-B, avcC or
/// `NEW_EXTRADATA` side data, in any order — neither wrapper bytes nor
/// ordering constitutes a configuration change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParameterSets {
    pub(crate) sps: Vec<Vec<u8>>,
    pub(crate) pps: Vec<Vec<u8>>,
}

impl ParameterSets {
    /// The canonical S8 fingerprint form: parameter sets sorted by identity
    /// (byte content) with exact duplicates removed. Duplicate policy: a
    /// repeated identical set is redundant, never a change; two sets that
    /// differ in any byte are distinct identities.
    pub(crate) fn into_canonical(mut self) -> Self {
        self.sps.sort_unstable();
        self.sps.dedup();
        self.pps.sort_unstable();
        self.pps.dedup();
        self
    }
}

/// Extracts parameter sets from extradata in either Annex-B or avcC form.
pub(crate) fn parse_parameter_sets(extradata: &[u8]) -> Result<ParameterSets, String> {
    if extradata.first() == Some(&1) {
        parse_avcc_parameter_sets(extradata)
    } else {
        let mut sets = ParameterSets {
            sps: Vec::new(),
            pps: Vec::new(),
        };
        let mut bad_type: Option<u8> = None;
        walk_annexb(extradata, |nal| match nal[0] & 0x1F {
            NAL_SPS => sets.sps.push(nal.to_vec()),
            NAL_PPS => sets.pps.push(nal.to_vec()),
            other => bad_type = bad_type.or(Some(other)),
        })?;
        if let Some(other) = bad_type {
            return Err(format!(
                "unexpected NAL type {other} in configuration data (expected SPS/PPS)"
            ));
        }
        if sets.sps.is_empty() || sets.pps.is_empty() {
            return Err("configuration data lacks an SPS or a PPS".to_string());
        }
        Ok(sets)
    }
}

/// Parses an AVCDecoderConfigurationRecord, enforcing the strict-tier checks:
/// `configurationVersion == 1`, at least one SPS and one PPS, every array
/// entry carrying the NAL type its array declares (7 for SPS, 8 for PPS), and
/// `lengthSizeMinusOne == 3` (FFmpeg passes pre-existing avcC through
/// unchanged, so a non-4-byte configuration is possible in principle and the
/// strict tier rejects it rather than rewriting every packet's prefixes).
pub(crate) fn parse_avcc_parameter_sets(avcc: &[u8]) -> Result<ParameterSets, String> {
    if avcc.len() < 7 {
        return Err(format!("avcC too short ({} bytes)", avcc.len()));
    }
    if avcc[0] != 1 {
        return Err(format!("avcC configurationVersion is {} (expected 1)", avcc[0]));
    }
    let length_size = (avcc[4] & 0x03) as usize + 1;
    if length_size != NAL_LENGTH_SIZE {
        return Err(format!(
            "avcC NAL length size is {length_size} (the strict tier requires 4)"
        ));
    }
    let mut pos = 5usize;
    let sps_count = (avcc[pos] & 0x1F) as usize;
    pos += 1;
    let mut sets = ParameterSets {
        sps: Vec::with_capacity(sps_count),
        pps: Vec::new(),
    };
    for _ in 0..sps_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("SPS entry: {e}"))?;
        let nal_type = ps[0] & 0x1F;
        if nal_type != NAL_SPS {
            return Err(format!(
                "avcC SPS array entry carries NAL type {nal_type} (expected {NAL_SPS})"
            ));
        }
        sets.sps.push(ps);
    }
    if pos >= avcc.len() {
        return Err("avcC truncated before the PPS count".to_string());
    }
    let pps_count = avcc[pos] as usize;
    pos += 1;
    for _ in 0..pps_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("PPS entry: {e}"))?;
        let nal_type = ps[0] & 0x1F;
        if nal_type != NAL_PPS {
            return Err(format!(
                "avcC PPS array entry carries NAL type {nal_type} (expected {NAL_PPS})"
            ));
        }
        sets.pps.push(ps);
    }
    // Trailing bytes (the profile extension) are legal and ignored here.
    if sets.sps.is_empty() || sets.pps.is_empty() {
        return Err("avcC lacks an SPS or a PPS".to_string());
    }
    Ok(sets)
}

fn read_u16_prefixed(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    if data.len() - *pos < 2 {
        return Err("truncated length".to_string());
    }
    let len = u16::from_be_bytes([data[*pos], data[*pos + 1]]) as usize;
    *pos += 2;
    if len == 0 {
        return Err("zero-length parameter set".to_string());
    }
    if data.len() - *pos < len {
        return Err("length overruns the record".to_string());
    }
    let out = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(out)
}

/// Builds an AVCDecoderConfigurationRecord from parsed parameter sets,
/// mirroring `ff_isom_write_avcc`: `lengthSizeMinusOne = 3`, and the
/// chroma-format/bit-depth extension appended for profiles other than
/// Baseline (66), Main (77) and Extended (88).
pub(crate) fn build_avcc(sets: &ParameterSets) -> Result<Vec<u8>, String> {
    let first_sps = sets.sps.first().ok_or("no SPS")?;
    if first_sps.len() < 4 {
        return Err(format!("SPS too short ({} bytes)", first_sps.len()));
    }
    if sets.sps.len() > 0x1F || sets.pps.len() > 0xFF {
        return Err("too many parameter sets for avcC".to_string());
    }
    let mut out = Vec::with_capacity(16 + first_sps.len());
    out.push(1); // configurationVersion
    out.push(first_sps[1]); // AVCProfileIndication
    out.push(first_sps[2]); // profile_compatibility
    out.push(first_sps[3]); // AVCLevelIndication
    out.push(0xFC | (NAL_LENGTH_SIZE as u8 - 1)); // lengthSizeMinusOne = 3
    out.push(0xE0 | sets.sps.len() as u8);
    for sps in &sets.sps {
        if sps.len() > u16::MAX as usize {
            return Err("SPS exceeds the 16-bit avcC length field".to_string());
        }
        out.extend_from_slice(&(sps.len() as u16).to_be_bytes());
        out.extend_from_slice(sps);
    }
    out.push(sets.pps.len() as u8);
    for pps in &sets.pps {
        if pps.len() > u16::MAX as usize {
            return Err("PPS exceeds the 16-bit avcC length field".to_string());
        }
        out.extend_from_slice(&(pps.len() as u16).to_be_bytes());
        out.extend_from_slice(pps);
    }
    let profile = first_sps[1];
    if profile != 66 && profile != 77 && profile != 88 {
        let (chroma_format_idc, bit_depth_luma, bit_depth_chroma) = sps_chroma_info(first_sps)?;
        out.push(0xFC | (chroma_format_idc & 0x03));
        out.push(0xF8 | ((bit_depth_luma - 8) & 0x07));
        out.push(0xF8 | ((bit_depth_chroma - 8) & 0x07));
        out.push(0); // numOfSequenceParameterSetExt
    }
    Ok(out)
}

/// The derived codec projection of one H.264 configuration — the
/// profile / compatibility / level bytes the stream configuration exposes
/// (as fields and inside `avc1.PPCCLL`). Part of the composite S8 baseline:
/// the SAME derivation, from the SAME source (the ordered first SPS that
/// builds the delivered avcC), is compared against every `NEW_EXTRADATA`
/// announcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodecProjection {
    pub(crate) profile: u8,
    pub(crate) compatibility: u8,
    pub(crate) level: u8,
}

impl CodecProjection {
    /// Derives the projection from parsed sets (ordered form; the first SPS
    /// is the one avcC synthesis and `codec_string` use).
    fn from_ordered_sets(sets: &ParameterSets) -> Result<Self, String> {
        let first_sps = sets.sps.first().ok_or("no SPS")?;
        if first_sps.len() < 4 {
            return Err(format!("SPS too short ({} bytes)", first_sps.len()));
        }
        Ok(Self {
            profile: first_sps[1],
            compatibility: first_sps[2],
            level: first_sps[3],
        })
    }

    /// The RFC 6381 codec string, `avc1.PPCCLL`.
    pub(crate) fn codec_string(&self) -> String {
        format!(
            "avc1.{:02X}{:02X}{:02X}",
            self.profile, self.compatibility, self.level
        )
    }
}

/// Reads `chroma_format_idc` / bit depths from an SPS NAL (header byte
/// included), defaulting to 4:2:0 / 8-bit when the profile does not carry the
/// fields — the same subset `ff_avc_decode_sps` extracts for the avcC
/// extension.
fn sps_chroma_info(sps: &[u8]) -> Result<(u8, u8, u8), String> {
    // Strip emulation prevention bytes (00 00 03 -> 00 00) from the RBSP.
    let payload = &sps[1..];
    let mut rbsp = Vec::with_capacity(payload.len());
    let mut zeros = 0u32;
    for &b in payload {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        if b == 0 {
            zeros += 1;
        } else {
            zeros = 0;
        }
        rbsp.push(b);
    }
    let mut r = BitReader::new(&rbsp);
    let profile_idc = r.bits(8)? as u8;
    r.bits(8)?; // constraint flags + reserved
    r.bits(8)?; // level_idc
    r.ue()?; // seq_parameter_set_id
    match profile_idc {
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135 => {
            let chroma_format_idc = r.ue()?;
            if chroma_format_idc > 3 {
                return Err(format!("invalid chroma_format_idc {chroma_format_idc}"));
            }
            if chroma_format_idc == 3 {
                r.bits(1)?; // separate_colour_plane_flag
            }
            // Bound the raw ue(v) values before the +8 arithmetic: H.264
            // 7.4.2.1.1 limits bit_depth_luma_minus8 / bit_depth_chroma_minus8
            // to 0..=6, and an unbounded Exp-Golomb result (up to 2^32 - 2)
            // would overflow the u32 addition.
            let bit_depth_luma_minus8 = r.ue()?;
            let bit_depth_chroma_minus8 = r.ue()?;
            if bit_depth_luma_minus8 > 6 || bit_depth_chroma_minus8 > 6 {
                return Err(format!(
                    "invalid SPS bit depth (bit_depth_luma_minus8 {bit_depth_luma_minus8}, \
                     bit_depth_chroma_minus8 {bit_depth_chroma_minus8}; both must be <= 6)"
                ));
            }
            Ok((
                chroma_format_idc as u8,
                bit_depth_luma_minus8 as u8 + 8,
                bit_depth_chroma_minus8 as u8 + 8,
            ))
        }
        _ => Ok((1, 8, 8)),
    }
}

/// MSB-first bit reader over an RBSP with Exp-Golomb support.
struct BitReader<'a> {
    data: &'a [u8],
    pos: usize, // bit position
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn bits(&mut self, n: u32) -> Result<u32, String> {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = self
                .data
                .get(self.pos / 8)
                .ok_or_else(|| "SPS truncated".to_string())?;
            let bit = (byte >> (7 - (self.pos % 8))) & 1;
            v = (v << 1) | bit as u32;
            self.pos += 1;
        }
        Ok(v)
    }

    /// Unsigned Exp-Golomb.
    fn ue(&mut self) -> Result<u32, String> {
        let mut zeros = 0u32;
        while self.bits(1)? == 0 {
            zeros += 1;
            if zeros > 31 {
                return Err("invalid Exp-Golomb code".to_string());
            }
        }
        if zeros == 0 {
            return Ok(0);
        }
        let rest = self.bits(zeros)?;
        Ok((1u32 << zeros) - 1 + rest)
    }
}

/// Per-stream H.264 runtime state.
pub(crate) struct AvcRuntime {
    /// Decided once, from the extradata form: Annex-B extradata means
    /// Annex-B packets (rewritten per packet), avcC extradata means already
    /// length-prefixed packets (validated in place). The forms never mix
    /// within one encoder.
    annexb_packets: bool,
    /// Composite S8 baseline, part 1: canonical parameter-set fingerprint
    /// (wrapper- and order-independent set identities).
    baseline: ParameterSets,
    /// Composite S8 baseline, part 2: the derived codec projection the
    /// stream configuration exposes — same source as `on_stream_info`, so a
    /// reordering that changes what consumers were told (profile /
    /// compatibility / level) is a configuration change even when the set
    /// identities are unchanged.
    projection: CodecProjection,
}

impl AvcRuntime {
    /// Builds the runtime from finalized encoder extradata; returns the
    /// runtime plus the avcC to deliver in the stream configuration (a
    /// pre-existing avcC passes through after validation; Annex-B parameter
    /// sets are synthesized into one, preserving wrapper order like movenc).
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<(Self, Vec<u8>, CodecProjection), PacketSinkError> {
        let invalid = |reason: String| PacketSinkError::InvalidExtradata {
            stream_index,
            reason,
        };
        let annexb_packets = extradata.first() != Some(&1);
        let ordered = parse_parameter_sets(extradata).map_err(invalid)?;
        let projection = CodecProjection::from_ordered_sets(&ordered)
            .map_err(|reason| PacketSinkError::InvalidExtradata {
                stream_index,
                reason,
            })?;
        let delivered = if annexb_packets {
            build_avcc(&ordered).map_err(|reason| PacketSinkError::InvalidExtradata {
                stream_index,
                reason,
            })?
        } else {
            extradata.to_vec()
        };
        // The avcC header (bytes 1..4) and the accessor projection must
        // describe the same profile/compatibility/level: a synthesized avcC
        // copies the first SPS's bytes, but a passed-through record keeps its
        // original header, which can disagree with the SPS the projection
        // (and the codec string) is derived from. Delivering such a record
        // would hand consumers two conflicting descriptions of one stream.
        if delivered[1] != projection.profile
            || delivered[2] != projection.compatibility
            || delivered[3] != projection.level
        {
            return Err(PacketSinkError::InvalidExtradata {
                stream_index,
                reason: format!(
                    "avcC header declares profile/compatibility/level \
                     {:02X}{:02X}{:02X} but the first SPS carries {:02X}{:02X}{:02X}",
                    delivered[1],
                    delivered[2],
                    delivered[3],
                    projection.profile,
                    projection.compatibility,
                    projection.level
                ),
            });
        }
        Ok((
            Self {
                annexb_packets,
                baseline: ordered.into_canonical(),
                projection,
            },
            delivered,
            projection,
        ))
    }

    /// S8: a `NEW_EXTRADATA` announcement — value-equal (canonicalized)
    /// parameter sets are redundant and pass; anything else is a mid-stream
    /// configuration change.
    pub(crate) fn check_new_extradata(
        &self,
        bytes: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let ordered = parse_parameter_sets(bytes).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("unparseable NEW_EXTRADATA ({reason})"),
            }
        })?;
        // Composite baseline, part 2: the derived projection consumers were
        // told (profile/compatibility/level, from the announcement's own
        // ordered first SPS — the same derivation on_stream_info used). A
        // reorder that changes it IS a configuration change even when the
        // set identities below are unchanged.
        let projection = CodecProjection::from_ordered_sets(&ordered).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("unparseable NEW_EXTRADATA SPS ({reason})"),
            }
        })?;
        if projection != self.projection {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "derived codec projection changed ({} -> {})",
                    self.projection.codec_string(),
                    projection.codec_string()
                ),
            });
        }
        // Composite baseline, part 1: canonical set identities.
        if ordered.into_canonical() != self.baseline {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "NEW_EXTRADATA carries different parameter sets".to_string(),
            });
        }
        Ok(())
    }

    /// Payload normalization: validates NAL boundaries, classifies types
    /// and (for Annex-B input) rewrites into `scratch` as a 4-byte
    /// length-prefixed access unit. Annex-B input takes TWO linear walks —
    /// an allocation-free census that reserves the exact output size, then
    /// the write walk, which therefore never reallocates. The
    /// already-length-prefixed path is a single validate-only pass,
    /// zero-copy.
    ///
    /// Returns `(is_key, payload)` where `is_key` is IDR presence and
    /// `payload` borrows either `scratch` or the input.
    pub(crate) fn normalize_au<'a>(
        &self,
        payload: &'a [u8],
        scratch: &'a mut Vec<u8>,
        stream_index: usize,
    ) -> Result<(bool, &'a [u8]), PacketSinkError> {
        let malformed = |reason: String| PacketSinkError::MalformedPacket {
            stream_index,
            reason,
        };
        let (scan, data): (_, &'a [u8]) = if self.annexb_packets {
            scratch.clear();
            // Reserve the EXACT output size from a start-code census: a
            // first, allocation-free walk sums `4 + trimmed_nal_len` per
            // NAL, so the write walk below can never reallocate — not even
            // for an AU with arbitrarily many 3-byte start codes. Both walks
            // are linear scans of one AU; the census also front-loads the
            // boundary validation.
            let mut exact = 0usize;
            walk_annexb(payload, |nal| exact += NAL_LENGTH_SIZE + nal.len())
                .map_err(&malformed)?;
            scratch.reserve(exact);
            let scan = walk_annexb(payload, |nal| push_length_prefixed(nal, scratch))
                .map_err(&malformed)?;
            (scan, scratch.as_slice())
        } else {
            let scan = walk_length_prefixed(payload, |_| {}).map_err(malformed)?;
            (scan, payload)
        };
        if scan.has_parameter_set {
            // Cold path: collect the in-band sets for the S8 comparison
            // (differing sets are a config change; value-equal sets are still
            // rejected — strict-tier configuration stays out-of-band).
            self.check_inband_parameter_sets(data, stream_index)?;
            return Err(PacketSinkError::InBandParameterSets { stream_index });
        }
        Ok((scan.has_idr, data))
    }

    /// In-band SPS/PPS: sets differing from the baseline are a configuration
    /// change; value-equal sets fall through to the strict-tier in-band
    /// rejection at the caller. `data` is length-prefixed (post-normalization).
    fn check_inband_parameter_sets(
        &self,
        data: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let mut mismatch = false;
        let _ = walk_length_prefixed(data, |nal| {
            let matches_baseline = match nal[0] & 0x1F {
                NAL_SPS => self.baseline.sps.iter().any(|s| s.as_slice() == nal),
                NAL_PPS => self.baseline.pps.iter().any(|p| p.as_slice() == nal),
                _ => return,
            };
            if !matches_baseline {
                mismatch = true;
            }
        });
        if mismatch {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "in-band SPS/PPS differ from the stream configuration".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::nal_framing::collect_annexb;
    use super::*;

    // Minimal but structurally valid SPS/PPS payloads (header byte included).
    // The SPS declares Baseline (66) so no avcC extension applies.
    const SPS: &[u8] = &[0x67, 66, 0xC0, 0x1E, 0xAC, 0xD9, 0x40];
    const PPS: &[u8] = &[0x68, 0xCE, 0x3C, 0x80];

    fn annexb_config() -> Vec<u8> {
        let mut v = vec![0, 0, 0, 1];
        v.extend_from_slice(SPS);
        v.extend_from_slice(&[0, 0, 1]);
        v.extend_from_slice(PPS);
        v
    }

    #[test]
    fn builds_and_reparses_avcc() {
        let sets = parse_parameter_sets(&annexb_config()).unwrap();
        assert_eq!(sets.sps, vec![SPS.to_vec()]);
        assert_eq!(sets.pps, vec![PPS.to_vec()]);
        let avcc = build_avcc(&sets).unwrap();
        assert_eq!(avcc[0], 1);
        assert_eq!(avcc[1], 66);
        assert_eq!(avcc[4] & 0x03, 3, "lengthSizeMinusOne must be 3");
        assert_eq!(avcc[5] & 0x1F, 1);
        let reparsed = parse_avcc_parameter_sets(&avcc).unwrap();
        assert_eq!(reparsed, sets);
        // Baseline profile: no extension bytes.
        let sps_len = SPS.len();
        let pps_len = PPS.len();
        assert_eq!(avcc.len(), 6 + 2 + sps_len + 1 + 2 + pps_len);
        let projection = CodecProjection::from_ordered_sets(&sets).unwrap();
        assert_eq!(projection.codec_string(), "avc1.42C01E");
        assert_eq!(
            (projection.profile, projection.compatibility, projection.level),
            (66, 0xC0, 0x1E)
        );
    }

    #[test]
    fn high_profile_avcc_carries_the_extension() {
        // High profile (100), chroma_format_idc=1, 8-bit depths. RBSP bits:
        // profile 100, flags 0, level 30, sps_id ue(0)=1, chroma ue(1)=010,
        // bit_depth_luma ue(0)=1, bit_depth_chroma ue(0)=1.
        let mut sps = vec![0x67, 100, 0x00, 30];
        // bits: 1 010 1 1 ... pad with stop bit pattern.
        sps.push(0b1010_1110);
        let sets = ParameterSets {
            sps: vec![sps],
            pps: vec![PPS.to_vec()],
        };
        let avcc = build_avcc(&sets).unwrap();
        let tail = &avcc[avcc.len() - 4..];
        assert_eq!(tail[0], 0xFC | 1, "chroma_format_idc");
        assert_eq!(tail[1], 0xF8, "bit_depth_luma_minus8");
        assert_eq!(tail[2], 0xF8, "bit_depth_chroma_minus8");
        assert_eq!(tail[3], 0, "numOfSequenceParameterSetExt");
        let projection = CodecProjection::from_ordered_sets(&sets).unwrap();
        assert_eq!(projection.codec_string(), "avc1.64001E");
    }

    #[test]
    fn rejects_non_four_byte_avcc() {
        let sets = parse_parameter_sets(&annexb_config()).unwrap();
        let mut avcc = build_avcc(&sets).unwrap();
        avcc[4] = 0xFC | 1; // lengthSizeMinusOne = 1 (2-byte prefixes)
        assert!(parse_avcc_parameter_sets(&avcc).is_err());
    }

    #[test]
    fn fingerprint_is_wrapper_independent() {
        let from_annexb = parse_parameter_sets(&annexb_config()).unwrap();
        let avcc = build_avcc(&from_annexb).unwrap();
        let from_avcc = parse_parameter_sets(&avcc).unwrap();
        assert_eq!(from_annexb, from_avcc);
    }

    #[test]
    fn canonical_form_is_order_insensitive_and_deduplicated() {
        let a = ParameterSets {
            sps: vec![SPS.to_vec(), vec![0x67, 1, 2, 3]],
            pps: vec![PPS.to_vec(), PPS.to_vec()],
        };
        let b = ParameterSets {
            sps: vec![vec![0x67, 1, 2, 3], SPS.to_vec()],
            pps: vec![PPS.to_vec()],
        };
        assert_ne!(a, b, "ordered forms differ");
        assert_eq!(
            a.clone().into_canonical(),
            b.clone().into_canonical(),
            "canonical forms are identical"
        );
    }

    #[test]
    fn runtime_normalizes_annexb_and_passes_through_avcc() {
        let (runtime, delivered, projection) =
            AvcRuntime::from_extradata(&annexb_config(), 0).unwrap();
        assert_eq!(projection.codec_string(), "avc1.42C01E");
        assert_eq!(delivered[0], 1);
        let mut scratch = Vec::new();
        let au = vec![0, 0, 0, 1, 0x65, 0x88, 0x80];
        let (is_key, data) = runtime.normalize_au(&au, &mut scratch, 0).unwrap();
        assert!(is_key);
        assert_eq!(data, &[0, 0, 0, 3, 0x65, 0x88, 0x80]);

        // avcC-configured stream: packets are already length-prefixed and
        // pass through unchanged (zero copy).
        let avcc = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap()).unwrap();
        let (runtime, _, _) = AvcRuntime::from_extradata(&avcc, 0).unwrap();
        let lp = vec![0, 0, 0, 2, 0x41, 0x9A];
        let mut scratch = Vec::new();
        let (is_key, data) = runtime.normalize_au(&lp, &mut scratch, 0).unwrap();
        assert!(!is_key);
        assert_eq!(data.as_ptr(), lp.as_ptr(), "pass-through must not copy");
    }

    /// A4 golden fixture (trailing zeros): the FULL production path
    /// (`AvcRuntime::normalize_au` over an Annex-B AU that legally pads its
    /// NAL units with trailing_zero_8bits) must produce the byte-exact AVCC
    /// sample the mp4 muxer's `nal_parse_units` would write — trims applied,
    /// 4-byte length prefixes, no padding bytes carried into the payload.
    /// (Placed at the runtime layer because the integration harness cannot
    /// inject a synthetic AU through a real encoder; this IS the delivery
    /// code path.)
    #[test]
    fn a4_trailing_zero_fixture_matches_movenc_output() {
        let (runtime, _, _) = AvcRuntime::from_extradata(&annexb_config(), 0).unwrap();
        // SEI [06 05 FF] padded with two zeros, 3-byte start code, IDR slice
        // [65 88 84] padded with one zero at stream end.
        let au = vec![
            0, 0, 0, 1, 0x06, 0x05, 0xFF, 0, 0, // SEI + trailing_zero_8bits
            0, 0, 1, 0x65, 0x88, 0x84, 0, // IDR + trailing zero at end
        ];
        let mut scratch = Vec::new();
        let (is_key, data) = runtime.normalize_au(&au, &mut scratch, 0).unwrap();
        assert!(is_key);
        // movenc-equivalent golden bytes: wb32(3) SEI, wb32(3) IDR — the
        // padding is start-code framing, never sample payload.
        assert_eq!(
            data,
            &[0, 0, 0, 3, 0x06, 0x05, 0xFF, 0, 0, 0, 3, 0x65, 0x88, 0x84]
        );
        // Census-based reservation: the exact final size was reserved before
        // writing, so the conversion never reallocated mid-AU.
        assert_eq!(data.len(), 14);
        assert!(scratch.capacity() >= 14);
    }

    #[test]
    fn annexb_config_split_reuses_the_walker() {
        let config = annexb_config();
        let nals = collect_annexb(&config).unwrap();
        assert_eq!(nals, vec![SPS, PPS]);
    }

    /// Builds a raw avcC (lengthSizeMinusOne = 3) with the given header bytes
    /// and exactly one entry per parameter-set array.
    fn raw_avcc(profile: u8, compat: u8, level: u8, sps: &[u8], pps: &[u8]) -> Vec<u8> {
        let mut v = vec![1, profile, compat, level, 0xFF, 0xE1];
        v.extend_from_slice(&(sps.len() as u16).to_be_bytes());
        v.extend_from_slice(sps);
        v.push(1);
        v.extend_from_slice(&(pps.len() as u16).to_be_bytes());
        v.extend_from_slice(pps);
        v
    }

    #[test]
    fn rejects_avcc_with_swapped_sps_pps_nal_types() {
        // SPS array carrying the PPS NAL (type 8) and vice versa: each array
        // entry must actually be the parameter-set type its array declares.
        let swapped = raw_avcc(66, 0xC0, 0x1E, PPS, SPS);
        let err = parse_avcc_parameter_sets(&swapped).unwrap_err();
        assert!(err.contains("NAL type"), "unexpected error: {err}");
        assert!(matches!(
            AvcRuntime::from_extradata(&swapped, 3),
            Err(PacketSinkError::InvalidExtradata { stream_index: 3, .. })
        ));
        // An SPS smuggled into the PPS array alone must also be caught.
        let bad_pps = raw_avcc(66, 0xC0, 0x1E, SPS, SPS);
        let err = parse_avcc_parameter_sets(&bad_pps).unwrap_err();
        assert!(err.contains("NAL type"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_sps_bit_depth_ue_overflow() {
        // High-profile SPS whose bit_depth_luma_minus8 is ue(0xFFFF_FFFE):
        // 31 leading zeros, the stop bit, then 31 one bits. The raw value
        // must be bounded to <= 6 (H.264 7.4.2.1.1) before the +8
        // conversion, which it would otherwise overflow.
        let sps = [
            0x67, 100, 0x00, 30, 0xA0, 0x00, 0x00, 0x00, 0x1F, 0xFF, 0xFF, 0xFF, 0xF0,
        ];
        let sets = ParameterSets {
            sps: vec![sps.to_vec()],
            pps: vec![PPS.to_vec()],
        };
        let err = build_avcc(&sets).unwrap_err();
        assert!(err.contains("bit depth"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_chroma_format_idc_above_three() {
        // chroma_format_idc is two bits wide in the avcC extension; an
        // out-of-range SPS value (4..=7 and beyond) must be rejected, not
        // silently masked into range. ue(7) = '0001000', ue(4) = '00101'.
        for (payload, idc) in [(0x88u8, 7u32), (0x94, 4)] {
            let sps = [0x67, 100, 0x00, 30, payload];
            let sets = ParameterSets {
                sps: vec![sps.to_vec()],
                pps: vec![PPS.to_vec()],
            };
            let err = build_avcc(&sets).unwrap_err();
            assert!(
                err.contains(&format!("chroma_format_idc {idc}")),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn rejects_truncated_configuration_data() {
        // Every strict prefix of a valid record is invalid: parsing must
        // fail cleanly (no panic, no partial acceptance) at every cut.
        let avcc = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap()).unwrap();
        for cut in 0..avcc.len() {
            assert!(
                parse_avcc_parameter_sets(&avcc[..cut]).is_err(),
                "a {cut}-byte prefix must be rejected"
            );
        }
        // A High-profile SPS that ends before the chroma fields must error
        // out of the bit reader, not read past the payload.
        let sets = ParameterSets {
            sps: vec![vec![0x67, 100, 0x00, 30]],
            pps: vec![PPS.to_vec()],
        };
        let err = build_avcc(&sets).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_avcc_header_disagreeing_with_first_sps() {
        // A passed-through record keeps its original header bytes while the
        // profile/compatibility/level accessors derive from the first SPS.
        // A record whose header disagrees would hand consumers two
        // conflicting stream descriptions and must be rejected.
        let good = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap()).unwrap();
        assert!(AvcRuntime::from_extradata(&good, 0).is_ok());
        for byte in 1..4 {
            let mut bad = good.clone();
            bad[byte] ^= 0x01;
            assert!(
                matches!(
                    AvcRuntime::from_extradata(&bad, 5),
                    Err(PacketSinkError::InvalidExtradata { stream_index: 5, .. })
                ),
                "tampered header byte {byte} must be rejected"
            );
        }
    }
}
