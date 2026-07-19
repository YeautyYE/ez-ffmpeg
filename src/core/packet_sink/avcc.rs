//! H.264 Annex-B / AVCC normalization for the strict packet-sink tier.
//!
//! The mp4 muxer performs this conversion at its write site (`ff_nal_parse_units`
//! + `ff_isom_write_avcc` in libavformat); a packet sink bypasses the muxer, so
//! the same responsibility lives here. Two invariants matter:
//!
//! * NAL boundary detection follows FFmpeg's convention exactly (a NAL ends at
//!   the next `00 00 01` triple; both 3- and 4-byte start codes are accepted),
//!   so a packet-sink AU is byte-identical to the sample the mp4 muxer would
//!   have written for the same input.
//! * avcC synthesis mirrors `ff_isom_write_avcc`, including the
//!   chroma/bit-depth extension for profiles other than Baseline/Main/Extended,
//!   with `lengthSizeMinusOne` fixed to 3 (4-byte length prefixes).
//!
//! Everything here is pure byte manipulation; errors are reported as plain
//! `String` reasons and wrapped into typed [`crate::error::PacketSinkError`]
//! variants by the caller.

/// NAL unit types the strict tier cares about (H.264 table 7-1).
pub(crate) const NAL_IDR: u8 = 5;
pub(crate) const NAL_SPS: u8 = 7;
pub(crate) const NAL_PPS: u8 = 8;

/// Byte length of the AVCC length prefix the strict tier emits and accepts
/// (`lengthSizeMinusOne == 3`). Both FFmpeg construction paths (libx264
/// `set_avcc_extradata`, `ff_isom_write_avcc` for Annex-B input) hardcode it.
pub(crate) const NAL_LENGTH_SIZE: usize = 4;

/// What a NAL walk over one access unit observed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuScan {
    pub(crate) nal_count: usize,
    pub(crate) has_idr: bool,
    pub(crate) has_parameter_set: bool,
}

/// Splits an Annex-B byte stream into NAL unit slices.
///
/// Boundary convention is FFmpeg's (`ff_nal_find_startcode`): a NAL ends at the
/// first following `00 00 01` triple. Leading bytes must form a start code
/// (`00 00 01` with any number of leading zero bytes); anything else is
/// malformed. Empty NAL units are rejected.
pub(crate) fn split_annexb(data: &[u8]) -> Result<Vec<&[u8]>, String> {
    if data.len() < 4 {
        return Err(format!("Annex-B payload too short ({} bytes)", data.len()));
    }
    // Leading start code: (00)+ 01.
    let mut pos = 0;
    while pos < data.len() && data[pos] == 0 {
        pos += 1;
    }
    if pos < 2 || pos >= data.len() || data[pos] != 1 {
        return Err("payload does not begin with an Annex-B start code".to_string());
    }
    pos += 1;

    let mut nals = Vec::new();
    loop {
        let end = find_startcode(data, pos).unwrap_or(data.len());
        if end == pos {
            return Err("empty NAL unit".to_string());
        }
        nals.push(&data[pos..end]);
        if end == data.len() {
            break;
        }
        // Skip the separator start code ((00)+ 01 — the triple search already
        // guarantees at least two zeros before the 1).
        let mut next = end;
        while next < data.len() && data[next] == 0 {
            next += 1;
        }
        if next >= data.len() || data[next] != 1 {
            return Err("malformed start code between NAL units".to_string());
        }
        pos = next + 1;
        if pos >= data.len() {
            return Err("trailing start code without a NAL unit".to_string());
        }
    }
    Ok(nals)
}

/// First boundary at or after `from` where a start code begins. Mirrors
/// `ff_nal_find_startcode`: locate the next `00 00 01` triple, then back up by
/// exactly one byte when the preceding byte is zero (the leading zero of a
/// 4-byte start code belongs to the start code, not to the previous NAL).
fn find_startcode(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < 3 {
        return None;
    }
    let i = (from..data.len() - 2)
        .find(|&i| data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1)?;
    if i > from && data[i - 1] == 0 {
        Some(i - 1)
    } else {
        Some(i)
    }
}

/// Splits an already length-prefixed (4-byte) AVCC access unit into NAL
/// slices. Every NAL must be fully contained; zero-length NAL units and
/// trailing garbage are rejected.
pub(crate) fn length_prefixed_nals(data: &[u8]) -> Result<Vec<&[u8]>, String> {
    let mut nals = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        if data.len() - pos < NAL_LENGTH_SIZE {
            return Err("truncated NAL length prefix".to_string());
        }
        let len = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            as usize;
        pos += NAL_LENGTH_SIZE;
        if len == 0 {
            return Err("zero-length NAL unit".to_string());
        }
        if data.len() - pos < len {
            return Err(format!(
                "NAL length {len} overruns the packet ({} bytes remain)",
                data.len() - pos
            ));
        }
        nals.push(&data[pos..pos + len]);
        pos += len;
    }
    if nals.is_empty() {
        return Err("packet contains no NAL units".to_string());
    }
    Ok(nals)
}

/// Serializes NAL slices as a 4-byte length-prefixed AVCC access unit into
/// `out` (cleared first).
pub(crate) fn write_length_prefixed(nals: &[&[u8]], out: &mut Vec<u8>) -> Result<(), String> {
    out.clear();
    for nal in nals {
        if nal.len() > u32::MAX as usize {
            return Err("NAL unit exceeds the 4-byte length prefix range".to_string());
        }
        out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        out.extend_from_slice(nal);
    }
    Ok(())
}

/// Summarizes the NAL composition of one access unit.
pub(crate) fn au_scan(nals: &[&[u8]]) -> AuScan {
    let mut scan = AuScan::default();
    for nal in nals {
        let nal_type = nal[0] & 0x1F;
        scan.nal_count += 1;
        if nal_type == NAL_IDR {
            scan.has_idr = true;
        }
        if nal_type == NAL_SPS || nal_type == NAL_PPS {
            scan.has_parameter_set = true;
        }
    }
    scan
}

/// Parsed parameter sets of one H.264 configuration, in original order — the
/// canonical form used both to synthesize avcC and as the S8 fingerprint (the
/// same sets can arrive wrapped as Annex-B extradata, avcC extradata or
/// `NEW_EXTRADATA` side data; comparing wrapper bytes would misreport an
/// unchanged configuration as changed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParameterSets {
    pub(crate) sps: Vec<Vec<u8>>,
    pub(crate) pps: Vec<Vec<u8>>,
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
        for nal in split_annexb(extradata)? {
            match nal[0] & 0x1F {
                NAL_SPS => sets.sps.push(nal.to_vec()),
                NAL_PPS => sets.pps.push(nal.to_vec()),
                other => {
                    return Err(format!(
                        "unexpected NAL type {other} in configuration data (expected SPS/PPS)"
                    ))
                }
            }
        }
        if sets.sps.is_empty() || sets.pps.is_empty() {
            return Err("configuration data lacks an SPS or a PPS".to_string());
        }
        Ok(sets)
    }
}

/// Parses an AVCDecoderConfigurationRecord, enforcing the strict-tier checks:
/// `configurationVersion == 1`, at least one SPS and one PPS, and
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
        sets.sps.push(ps);
    }
    if pos >= avcc.len() {
        return Err("avcC truncated before the PPS count".to_string());
    }
    let pps_count = avcc[pos] as usize;
    pos += 1;
    for _ in 0..pps_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("PPS entry: {e}"))?;
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
            let bit_depth_luma = r.ue()? + 8;
            let bit_depth_chroma = r.ue()? + 8;
            if bit_depth_luma > 15 || bit_depth_chroma > 15 {
                return Err("invalid SPS bit depth".to_string());
            }
            Ok((chroma_format_idc as u8, bit_depth_luma as u8, bit_depth_chroma as u8))
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

#[cfg(test)]
mod tests {
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
    fn splits_three_and_four_byte_start_codes() {
        let nals = split_annexb(&annexb_config()).unwrap();
        assert_eq!(nals, vec![SPS, PPS]);
    }

    #[test]
    fn rejects_garbage_prefix_and_empty_nals() {
        assert!(split_annexb(&[0x12, 0, 0, 1, 0x67]).is_err());
        assert!(split_annexb(&[0, 0, 1]).is_err());
        // Two adjacent start codes -> empty NAL.
        assert!(split_annexb(&[0, 0, 1, 0, 0, 1, 0x41, 0x9A]).is_err());
    }

    #[test]
    fn converts_annexb_au_to_length_prefixed() {
        let mut au = vec![0, 0, 0, 1, 0x65, 0x88, 0x80];
        au.extend_from_slice(&[0, 0, 1, 0x06, 0x05, 0xFF]);
        let nals = split_annexb(&au).unwrap();
        let scan = au_scan(&nals);
        assert!(scan.has_idr);
        assert!(!scan.has_parameter_set);
        assert_eq!(scan.nal_count, 2);
        let mut out = Vec::new();
        write_length_prefixed(&nals, &mut out).unwrap();
        assert_eq!(
            out,
            vec![0, 0, 0, 3, 0x65, 0x88, 0x80, 0, 0, 0, 3, 0x06, 0x05, 0xFF]
        );
        // Round-trip: the produced AU splits back into the same NALs.
        let back = length_prefixed_nals(&out).unwrap();
        assert_eq!(back, nals);
    }

    #[test]
    fn length_prefixed_split_rejects_overruns() {
        assert!(length_prefixed_nals(&[0, 0, 0, 9, 0x65]).is_err());
        assert!(length_prefixed_nals(&[0, 0, 0, 0]).is_err());
        assert!(length_prefixed_nals(&[]).is_err());
        // Trailing partial prefix after a valid NAL.
        assert!(length_prefixed_nals(&[0, 0, 0, 1, 0x65, 0xFF]).is_err());
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
}
