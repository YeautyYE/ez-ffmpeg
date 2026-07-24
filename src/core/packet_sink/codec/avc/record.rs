//! Record functions: parameter-set extraction from Annex-B or avcC wrappers
//! (full structural validation of the record) and avcC synthesis mirroring
//! `ff_isom_write_avcc` (including the chroma/bit-depth extension).

use super::pps::parse_pps;
use super::sps::{parse_sps, SpsSummary};
use super::{AvcConfig, CodecProjection, ConfigFingerprint, ParameterSets};
use crate::core::packet_sink::nal_framing::{walk_annexb, NAL_LENGTH_SIZE, NAL_PPS, NAL_SPS};

/// Sequence parameter set extension NAL (H.264 Table 7-1, type 13) — the
/// only type `ff_isom_write_avcc` (libavformat/avc.c) stores in the avcC
/// `sequenceParameterSetExtNALUnit` array.
const NAL_SPS_EXT: u8 = 13;

/// Extracts parameter sets from extradata in either Annex-B or avcC form.
/// Both forms parse every SPS body through [`parse_sps`] and every PPS
/// body through [`parse_pps`] (the avcC branch inside
/// [`parse_avcc_record`]), so the syntax guarantees are
/// wrapper-independent.
pub(crate) fn parse_parameter_sets(extradata: &[u8]) -> Result<AvcConfig, String> {
    if extradata.first() == Some(&1) {
        parse_avcc_parameter_sets(extradata)
    } else {
        let mut sets = ParameterSets {
            sps: Vec::new(),
            pps: Vec::new(),
        };
        let mut fingerprint = ConfigFingerprint::default();
        let mut summaries: Vec<SpsSummary> = Vec::new();
        let mut error: Option<String> = None;
        // Sequential, in stream order, mirroring decode_extradata_ps
        // (libavcodec/h264_parse.c): an SPS enters the resolution context
        // as it is encountered, and a PPS resolves its
        // seq_parameter_set_id against the SPS seen SO FAR — parameter-set
        // activation cannot reach forward, so a PPS ahead of every SPS
        // carrying its id is the same dangling reference
        // ff_h264_decode_picture_parameter_set fails ("sps_id ... out of
        // range"). Every body must parse to its rbsp_trailing_bits, not
        // just the first SPS avcC synthesis reads fields from; the first
        // offense in stream order wins.
        walk_annexb(extradata, |nal| {
            if error.is_some() {
                return;
            }
            // Full NAL header validation, not a type mask: FFmpeg's NAL
            // reader rejects a set forbidden_zero_bit the same way
            // (`h264_parse_nal_header`, libavcodec/h2645_parse.c) while
            // accepting any nal_ref_idc.
            if nal[0] & 0x80 != 0 {
                error = Some(format!(
                    "configuration NAL header 0x{:02X} has forbidden_zero_bit set",
                    nal[0]
                ));
                return;
            }
            match nal[0] & 0x1F {
                NAL_SPS => match parse_sps(nal) {
                    Ok(summary) => {
                        fingerprint.put_sps(summary.sps_id, nal);
                        summaries.push(summary);
                        sets.sps.push(nal.to_vec());
                    }
                    Err(e) => error = Some(format!("SPS: {e}")),
                },
                NAL_PPS => match parse_pps(nal, &summaries) {
                    Ok((pps_id, bound)) => {
                        // The binding is fingerprint state: `bound` indexes
                        // (in configuration order) the SPS generation the
                        // reference resolved.
                        fingerprint.put_pps(pps_id, nal, &sets.sps[bound]);
                        sets.pps.push(nal.to_vec());
                    }
                    Err(e) => error = Some(format!("PPS: {e}")),
                },
                other => {
                    error = Some(format!(
                        "unexpected NAL type {other} in configuration data (expected SPS/PPS)"
                    ));
                }
            }
        })?;
        if let Some(reason) = error {
            return Err(reason);
        }
        if sets.sps.is_empty() || sets.pps.is_empty() {
            return Err("configuration data lacks an SPS or a PPS".to_string());
        }
        // Annex-B input is delivered as a synthesized record, so the
        // fingerprint carries the extension triple synthesis will write —
        // derived from the first SPS in announcement order, the one source
        // `ff_isom_write_avcc` reads (libavformat/avc.c).
        fingerprint.extension = derived_extension(&sets.sps[0], &summaries[0]);
        Ok(AvcConfig { sets, fingerprint })
    }
}

/// Parses an AVCDecoderConfigurationRecord, enforcing the strict-tier
/// checks; the thin wrapper over [`parse_avcc_record`] that drops the
/// structural header fields once validation passed.
pub(crate) fn parse_avcc_parameter_sets(avcc: &[u8]) -> Result<AvcConfig, String> {
    parse_avcc_record(avcc).map(|record| AvcConfig {
        sets: record.sets,
        fingerprint: record.fingerprint,
    })
}

/// One fully validated AVCDecoderConfigurationRecord: the header's declared
/// projection, the optional profile-extension fields, the parameter sets
/// and their identity fingerprint (which includes any SPS-EXT bodies).
#[derive(Debug)]
pub(super) struct AvccRecord {
    pub(super) header: CodecProjection,
    /// `(chroma_format_idc, bit_depth_luma, bit_depth_chroma)` declared by
    /// the profile extension, when the record carries one.
    pub(super) extension: Option<(u8, u8, u8)>,
    pub(super) sets: ParameterSets,
    pub(super) fingerprint: ConfigFingerprint,
}

/// Parses an AVCDecoderConfigurationRecord, enforcing the strict-tier
/// checks: `configurationVersion == 1`, all-ones reserved bits in bytes 4
/// and 5, `lengthSizeMinusOne == 3` (FFmpeg passes pre-existing avcC
/// through unchanged, so a non-4-byte configuration is possible in
/// principle and the strict tier rejects it rather than rewriting every
/// packet's prefixes), at least one SPS and one PPS, every array entry
/// carrying a valid NAL header of the type its array declares, a
/// complete-or-absent profile extension with nothing after it
/// ([`parse_avcc_extension`]), every SPS and PPS body parsing through
/// `rbsp_trailing_bits` ([`parse_sps`], [`parse_pps`]), and a header that
/// agrees with the record's own first SPS ([`check_avcc_consistency`]).
pub(super) fn parse_avcc_record(avcc: &[u8]) -> Result<AvccRecord, String> {
    if avcc.len() < 7 {
        return Err(format!("avcC too short ({} bytes)", avcc.len()));
    }
    if avcc[0] != 1 {
        return Err(format!("avcC configurationVersion is {} (expected 1)", avcc[0]));
    }
    let header = CodecProjection {
        profile: avcc[1],
        compatibility: avcc[2],
        level: avcc[3],
    };
    // Byte 4 is reserved '111111' + lengthSizeMinusOne and byte 5 reserved
    // '111' + numOfSequenceParameterSets (ISO/IEC 14496-15, 5.3.3.1.2);
    // `ff_isom_write_avcc` (libavformat/avc.c) emits 0xFF and 0xE0 | count.
    // The strict tier requires the ones instead of masking them away:
    // cleared reserved bits mean the bytes are not an avcC field layout
    // any conforming writer produces.
    if avcc[4] & 0xFC != 0xFC {
        return Err(format!(
            "avcC byte 4 reserved bits are cleared (0x{:02X}, expected 0xFC | lengthSizeMinusOne)",
            avcc[4]
        ));
    }
    let length_size = (avcc[4] & 0x03) as usize + 1;
    if length_size != NAL_LENGTH_SIZE {
        return Err(format!(
            "avcC NAL length size is {length_size} (the strict tier requires 4)"
        ));
    }
    if avcc[5] & 0xE0 != 0xE0 {
        return Err(format!(
            "avcC byte 5 reserved bits are cleared (0x{:02X}, expected 0xE0 | numOfSequenceParameterSets)",
            avcc[5]
        ));
    }
    let sps_count = (avcc[5] & 0x1F) as usize;
    let mut pos = 6usize;
    let mut sets = ParameterSets {
        sps: Vec::with_capacity(sps_count),
        pps: Vec::new(),
    };
    for _ in 0..sps_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("SPS entry: {e}"))?;
        check_ps_nal_header(ps[0], NAL_SPS, "SPS")?;
        sets.sps.push(ps);
    }
    if pos >= avcc.len() {
        return Err("avcC truncated before the PPS count".to_string());
    }
    let pps_count = avcc[pos] as usize;
    pos += 1;
    for _ in 0..pps_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("PPS entry: {e}"))?;
        check_ps_nal_header(ps[0], NAL_PPS, "PPS")?;
        sets.pps.push(ps);
    }
    if sets.sps.is_empty() || sets.pps.is_empty() {
        return Err("avcC lacks an SPS or a PPS".to_string());
    }
    // Every parameter-set body must parse to its rbsp_trailing_bits — the
    // array framing above only proves the entries have the right NAL type.
    // SPS first: the record's arrays put every SPS ahead of every PPS by
    // construction, so the full SPS array IS the "seen so far" context a
    // sequential read (decode_extradata_ps, libavcodec/h264_parse.c) would
    // hand each PPS. The same loops feed the identity fingerprint.
    let mut fingerprint = ConfigFingerprint::default();
    let mut summaries = Vec::with_capacity(sets.sps.len());
    for sps in &sets.sps {
        let summary = parse_sps(sps).map_err(|e| format!("avcC SPS: {e}"))?;
        fingerprint.put_sps(summary.sps_id, sps);
        summaries.push(summary);
    }
    for pps in &sets.pps {
        let (pps_id, bound) = parse_pps(pps, &summaries).map_err(|e| format!("avcC PPS: {e}"))?;
        fingerprint.put_pps(pps_id, pps, &sets.sps[bound]);
    }
    let (extension, sps_ext) = parse_avcc_extension(avcc, pos, header.profile)?;
    // The identity canonicalizes the extension: a record may legally carry
    // either the writer-derived triple (`ff_isom_write_avcc` synthesizing
    // from Annex-B) or the raw SPS-coded triple (the same function copies
    // an existing record VERBATIM when the input is not Annex-B, so a
    // remux preserves that shape). Both describe one stream, so the
    // fingerprint stores the writer-canonical form and a shape switch
    // between announcements is not a configuration change. The literal
    // bytes still face `check_avcc_consistency` below.
    fingerprint.extension = match extension {
        Some(_) => derived_extension(&sets.sps[0], &summaries[0]),
        None => None,
    };
    fingerprint.sps_ext = sps_ext;
    let record = AvccRecord {
        header,
        extension,
        sets,
        fingerprint,
    };
    check_avcc_consistency(&record)?;
    Ok(record)
}

/// Full NAL header validation for configuration parameter sets:
/// `forbidden_zero_bit` must be 0 and `nal_unit_type` must be the one the
/// array declares. FFmpeg's NAL reader rejects a set forbidden bit the
/// same way (`h264_parse_nal_header`, libavcodec/h2645_parse.c) while
/// accepting any `nal_ref_idc`, so the two middle bits stay unconstrained.
fn check_ps_nal_header(header: u8, expected_type: u8, what: &str) -> Result<(), String> {
    if header & 0x80 != 0 {
        return Err(format!(
            "avcC {what} entry NAL header 0x{header:02X} has forbidden_zero_bit set"
        ));
    }
    let nal_type = header & 0x1F;
    if nal_type != expected_type {
        return Err(format!(
            "avcC {what} array entry carries NAL type {nal_type} (expected {expected_type})"
        ));
    }
    Ok(())
}

/// Parsed record tail past the PPS array: the optional profile-extension
/// triple (`chroma_format_idc`, `bit_depth_luma`, `bit_depth_chroma`) and
/// the post-header payloads of the SPS-EXT entries, in record order.
type AvccExtensionTail = (Option<(u8, u8, u8)>, Vec<Vec<u8>>);

/// Parses the trailing profile extension at `pos`, or verifies its legal
/// absence — the record-level trailing-data policy.
///
/// `ff_isom_write_avcc` (libavformat/avc.c) appends the chroma-format /
/// bit-depth block plus the SPS-EXT array only for profiles other than
/// Baseline (66), Main (77) and Extended (88), so for those profiles any
/// trailing byte is foreign data. For all other profiles ISO/IEC 14496-15
/// requires the block, but real muxers predating its introduction end the
/// record at the PPS array, and FFmpeg's own reader
/// (`ff_h264_decode_extradata`, libavcodec/h264_parse.c) stops there
/// without ever requiring it — a record that ends there is accepted. When
/// the block IS present it must be complete, carry all-ones reserved bits,
/// hold SPS-EXT NALs only, and end the record. The SPS-EXT bodies are
/// RETAINED as post-header payloads, not just header-checked: they are
/// part of the delivered configuration, so the S8 fingerprint must see
/// them — an announcement that adds, drops or edits one is a
/// configuration change, while one that only rewrites an entry's
/// `nal_ref_idc` is not. Keying by the payload behind the header byte is
/// THIS crate's identity policy — the rule the SPS and PPS maps already
/// apply — not a mirrored FFmpeg behavior: the decoder's own extradata
/// readers never parse SPS-EXT (`ff_h264_decode_extradata`,
/// libavcodec/h264_parse.c, walks the avcC SPS and PPS arrays and stops,
/// and its Annex-B path ignores NAL type 13 in `decode_extradata_ps`),
/// and CBS's avcC split (`cbs_h264_split_fragment`) extracts only the
/// SPS and PPS arrays, warning off the trailing bytes — it decomposes a
/// type-13 unit into syntax fields only on the Annex-B/NAL-stream path,
/// keeping the raw bytes alongside the parsed content even then.
fn parse_avcc_extension(
    avcc: &[u8],
    mut pos: usize,
    profile: u8,
) -> Result<AvccExtensionTail, String> {
    if pos == avcc.len() {
        return Ok((None, Vec::new()));
    }
    let trailing = avcc.len() - pos;
    if profile == 66 || profile == 77 || profile == 88 {
        return Err(format!(
            "avcC for profile {profile} carries {trailing} trailing byte(s) \
             (no extension is defined)"
        ));
    }
    if trailing < 4 {
        return Err(format!(
            "avcC profile extension truncated ({trailing} byte(s); need the chroma \
             format, two bit depths and the SPS-EXT count)"
        ));
    }
    if avcc[pos] & 0xFC != 0xFC {
        return Err(format!(
            "avcC extension chroma byte reserved bits are cleared (0x{:02X})",
            avcc[pos]
        ));
    }
    let chroma_format_idc = avcc[pos] & 0x03;
    if avcc[pos + 1] & 0xF8 != 0xF8 || avcc[pos + 2] & 0xF8 != 0xF8 {
        return Err(format!(
            "avcC extension bit-depth reserved bits are cleared (0x{:02X} 0x{:02X})",
            avcc[pos + 1],
            avcc[pos + 2]
        ));
    }
    let bit_depth_luma = (avcc[pos + 1] & 0x07) + 8;
    let bit_depth_chroma = (avcc[pos + 2] & 0x07) + 8;
    let sps_ext_count = avcc[pos + 3] as usize;
    pos += 4;
    let mut sps_ext = Vec::with_capacity(sps_ext_count);
    for _ in 0..sps_ext_count {
        let ps = read_u16_prefixed(avcc, &mut pos).map_err(|e| format!("SPS-EXT entry: {e}"))?;
        check_ps_nal_header(ps[0], NAL_SPS_EXT, "SPS-EXT")?;
        sps_ext.push(ps[1..].to_vec());
    }
    if pos != avcc.len() {
        return Err(format!(
            "avcC carries {} trailing byte(s) after the profile extension",
            avcc.len() - pos
        ));
    }
    Ok((
        Some((chroma_format_idc, bit_depth_luma, bit_depth_chroma)),
        sps_ext,
    ))
}

/// The avcC header and the record's own first SPS must describe one
/// stream: `ff_isom_write_avcc` derives bytes 1..4 (profile /
/// compatibility / level) from the first SPS, and the profile-extension
/// triple may carry either of the two derivations that reach real files —
/// the writer's own reader dispatch ([`writer_extension_triple`], what
/// FFmpeg synthesizes from Annex-B input) or the raw SPS syntax (the same
/// function writes a non-Annex-B extradata verbatim, so a remux preserves
/// a syntax-derived tail). A record matching neither hands consumers two
/// conflicting descriptions and is rejected — at initial construction
/// and, via the shared parse, for every `NEW_EXTRADATA` announcement.
fn check_avcc_consistency(record: &AvccRecord) -> Result<(), String> {
    let derived = CodecProjection::from_ordered_sets(&record.sets)?;
    if record.header != derived {
        return Err(format!(
            "avcC header declares profile/compatibility/level \
             {:02X}{:02X}{:02X} but the first SPS carries {:02X}{:02X}{:02X}",
            record.header.profile,
            record.header.compatibility,
            record.header.level,
            derived.profile,
            derived.compatibility,
            derived.level
        ));
    }
    if let Some((chroma, luma, chroma_depth)) = record.extension {
        let first_sps = record.sets.sps.first().ok_or("no SPS")?;
        let summary = parse_sps(first_sps)?;
        let writer_fields = writer_extension_triple(first_sps[1], &summary);
        let syntax_fields = summary.chroma_info();
        if (chroma, luma, chroma_depth) != writer_fields
            && (chroma, luma, chroma_depth) != syntax_fields
        {
            let (want_chroma, want_luma, want_chroma_depth) = writer_fields;
            let mut accepted = format!("{want_chroma} and {want_luma}/{want_chroma_depth}");
            if syntax_fields != writer_fields {
                let (syn_chroma, syn_luma, syn_chroma_depth) = syntax_fields;
                accepted = format!(
                    "{accepted} (writer default) or {syn_chroma} and \
                     {syn_luma}/{syn_chroma_depth} (SPS syntax)"
                );
            }
            return Err(format!(
                "avcC extension declares chroma_format_idc {chroma} and bit depths \
                 {luma}/{chroma_depth} but the first SPS derives {accepted}"
            ));
        }
    }
    Ok(())
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

/// The extension triple `ff_isom_write_avcc` derives from one SPS. The
/// writer's own SPS reader (`ff_avc_decode_sps`, libavformat/avc.c)
/// parses the chroma-format/bit-depth block only for profile_idc 100,
/// 110, 122, 244, 44, 83, 86, 118, 128, 138, 139 and 134, and its else
/// branch pins every other profile to 4:2:0 / 8-bit. That dispatch is
/// narrower than BOTH syntax lists this file tracks: [`parse_sps`] must
/// still read the chroma block for 135 and 144 (H.264 7.3.2.1.1 puts the
/// bits in the payload, so skipping them would misalign everything
/// behind), and the decoder's list (`ff_h264_decode_seq_parameter_set`,
/// libavcodec/h264_ps.c) carries 144 but not 139/134. For a profile
/// outside the writer's list the triple is its (1, 8, 8) default no
/// matter what the SPS codes — an avcC FFmpeg synthesizes from Annex-B
/// for a profile-144 4:4:4 stream says (1, 8, 8) — so synthesis emits
/// this triple and a present record extension canonicalizes to it. The
/// consistency check additionally admits the raw-syntax triple, which
/// survives FFmpeg's verbatim extradata copy on remux.
fn writer_extension_triple(profile_idc: u8, summary: &SpsSummary) -> (u8, u8, u8) {
    match profile_idc {
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 => {
            summary.chroma_info()
        }
        _ => (1, 8, 8),
    }
}

/// The avcC profile-extension triple these parameter sets are delivered
/// with, `None` when no block is defined for them. Mirrors
/// `ff_isom_write_avcc` (libavformat/avc.c): the chroma-format/bit-depth
/// block is appended only when the FIRST SPS's profile_idc is none of
/// Baseline (66), Main (77) or Extended (88) — its
/// `sps[3] != 66 && sps[3] != 77 && sps[3] != 88` gate reads the first
/// entry of the SPS array it just wrote — and the three values are what
/// its own SPS reader decodes from that same first SPS
/// ([`writer_extension_triple`]). Synthesis ([`build_avcc`]), the
/// Annex-B fingerprint and a record-side fingerprint whose extension is
/// present all canonicalize to this derivation — a record that legally
/// ends at the PPS array keeps `None` — so the synthesize-reparse gate
/// compares like with like; the record consistency check is wider — it also admits the raw
/// SPS-coded triple, which survives the writer's verbatim copy of a
/// non-Annex-B extradata.
pub(super) fn derived_extension(first_sps: &[u8], first_summary: &SpsSummary) -> Option<(u8, u8, u8)> {
    match first_sps[1] {
        66 | 77 | 88 => None,
        profile => Some(writer_extension_triple(profile, first_summary)),
    }
}

/// Builds an AVCDecoderConfigurationRecord from parsed parameter sets,
/// mirroring `ff_isom_write_avcc`: `lengthSizeMinusOne = 3`, and the
/// chroma-format/bit-depth extension appended under the
/// [`derived_extension`] first-SPS profile rule.
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
    if let Some((chroma_format_idc, bit_depth_luma, bit_depth_chroma)) =
        derived_extension(first_sps, &parse_sps(first_sps)?)
    {
        out.push(0xFC | (chroma_format_idc & 0x03));
        out.push(0xF8 | ((bit_depth_luma - 8) & 0x07));
        out.push(0xF8 | ((bit_depth_chroma - 8) & 0x07));
        out.push(0); // numOfSequenceParameterSetExt
    }
    Ok(out)
}
