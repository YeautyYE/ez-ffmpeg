//! H.264/AVC strict-tier codec runtime: configuration (avcC) handling and
//! per-packet access-unit normalization.
//!
//! Splits into two layers:
//! * record functions — parameter-set extraction from Annex-B or avcC
//!   wrappers (full structural validation of the record: reserved bits, NAL
//!   headers, the profile extension, trailing data and header/SPS
//!   consistency), avcC synthesis mirroring `ff_isom_write_avcc` (including
//!   the chroma/bit-depth extension), and the full SPS and PPS parses
//!   every extradata path routes each parameter-set body through;
//! * [`AvcRuntime`] — the per-stream state machine the orchestrator drives:
//!   payload normalization via the streaming NAL walkers (a census walk
//!   then a write walk for Annex-B input), IDR classification, S8
//!   parameter-set fingerprinting and the in-band policy.

use super::super::nal_framing::{
    push_length_prefixed, walk_annexb, walk_length_prefixed, NAL_LENGTH_SIZE, NAL_PPS, NAL_SPS,
};
use crate::error::PacketSinkError;

/// Sequence parameter set extension NAL (H.264 Table 7-1, type 13) — the
/// only type `ff_isom_write_avcc` (libavformat/avc.c) stores in the avcC
/// `sequenceParameterSetExtNALUnit` array.
const NAL_SPS_EXT: u8 = 13;

/// Parsed parameter sets of one H.264 configuration, in original order.
/// avcC synthesis consumes this form (movenc preserves wrapper order); the
/// S8 comparison instead uses the [`ConfigFingerprint`] built during the
/// same parse, which keys the sets by parameter-set identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParameterSets {
    pub(crate) sps: Vec<Vec<u8>>,
    pub(crate) pps: Vec<Vec<u8>>,
}

/// The S8 fingerprint of one configuration: the EFFECTIVE parameter sets,
/// keyed by the identity a decoder stores them under. H.264 parameter sets
/// are addressed by id (`seq_parameter_set_id` / `pic_parameter_set_id`),
/// and a re-sent id REPLACES its predecessor — the sps_list/pps_list slot
/// overwrite in libavcodec/h264_ps.c — so what consumers can activate is
/// the last-wins id map, not the byte multiset. Two refinements make the
/// map track decoder state rather than delivery framing:
/// * values are the payloads BEHIND the one-byte NAL header: the header is
///   framing the reader consumes before the body is parsed and stored
///   (`h264_parse_nal_header`, libavcodec/h2645_parse.c), so two sets
///   differing only in `nal_ref_idc` land in identical stored state;
/// * every PPS slot also records the payload of the SPS generation the
///   PPS resolved: `ff_h264_decode_picture_parameter_set`
///   (libavcodec/h264_ps.c) binds the arriving PPS to the same-id SPS
///   live at that moment (the `PPS::sps` reference, libavcodec/h264_ps.h)
///   and derives PPS state from it, and a later same-id SPS replaces the
///   list slot, not that binding — so two configurations with equal id
///   maps still differ when a PPS bound a different SPS body on the way
///   there.
///
/// Two configurations are equal exactly when every id maps to the same
/// payload, every PPS bound the same SPS generation AND the record tail —
/// the profile-extension triple and the SPS-EXT list — agrees: the order
/// of DISTINCT ids never matters (wrapper bytes and array position
/// address nothing), a repeated payload-identical set collapses into its
/// slot and stays redundant, while reordering two same-id sets with
/// different payloads swaps which one is active and IS a configuration
/// change even though the byte set is unchanged. SPS-EXT bodies (the avcC
/// `sequenceParameterSetExtNALUnit` array) are part of the delivered
/// configuration and are fingerprinted in array order as post-header
/// payloads; their ids are not parsed, so adding, dropping or editing one
/// changes the fingerprint, while a header-only `nal_ref_idc` variant
/// stores identically and does not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConfigFingerprint {
    /// `seq_parameter_set_id` -> post-header payload of the last SPS
    /// carrying it, ascending by id.
    sps: Vec<(u32, Vec<u8>)>,
    /// `pic_parameter_set_id` -> slot of the last PPS carrying it,
    /// ascending by id.
    pps: Vec<(u32, PpsSlot)>,
    /// The profile-extension triple (`chroma_format_idc`,
    /// `bit_depth_luma`, `bit_depth_chroma`) in its writer-canonical form
    /// — the [`derived_extension`] triple — whichever accepted shape the
    /// validated bytes carried: a record may legally declare the writer
    /// default or the raw SPS syntax (both reach real files), and both
    /// canonicalize here so a shape switch between announcements is not a
    /// configuration change (`None` when the record legally ends at the
    /// PPS array). Two first SPS can share all three projection bytes yet
    /// differ here (chroma format and bit depth are not header bytes), so
    /// the id maps alone would miss the change.
    extension: Option<(u8, u8, u8)>,
    /// Post-header payloads of the SPS-EXT entries in record order (an
    /// Annex-B configuration carries none — that path admits only SPS
    /// and PPS).
    sps_ext: Vec<Vec<u8>>,
}

/// One PPS slot: the post-header payload of the PPS itself, then the
/// post-header payload of the SPS generation its `seq_parameter_set_id`
/// resolved when the PPS was parsed.
type PpsSlot = (Vec<u8>, Vec<u8>);

impl ConfigFingerprint {
    /// Last-wins insertion: the slot for `id` is created at its
    /// ascending-id position or overwritten when already present.
    fn put<V>(slots: &mut Vec<(u32, V)>, id: u32, value: V) {
        match slots.binary_search_by_key(&id, |(slot, _)| *slot) {
            Ok(i) => slots[i].1 = value,
            Err(i) => slots.insert(i, (id, value)),
        }
    }

    fn put_sps(&mut self, id: u32, nal: &[u8]) {
        Self::put(&mut self.sps, id, nal[1..].to_vec());
    }

    /// `bound_sps` is the full NAL of the SPS generation the PPS resolved.
    fn put_pps(&mut self, id: u32, nal: &[u8], bound_sps: &[u8]) {
        Self::put(
            &mut self.pps,
            id,
            (nal[1..].to_vec(), bound_sps[1..].to_vec()),
        );
    }

    /// Whether `nal` is payload-identical to an ACTIVE set of the
    /// configuration (a replaced same-id predecessor is not active; the
    /// header byte is framing, so a `nal_ref_idc` difference alone does
    /// not make a set foreign).
    fn has_active_sps(&self, nal: &[u8]) -> bool {
        self.sps
            .iter()
            .any(|(_, payload)| payload.as_slice() == &nal[1..])
    }

    fn has_active_pps(&self, nal: &[u8]) -> bool {
        self.pps
            .iter()
            .any(|(_, (payload, _))| payload.as_slice() == &nal[1..])
    }
}

/// One parsed configuration: the ordered sets avcC synthesis and the codec
/// projection consume, plus the identity-keyed S8 fingerprint built during
/// the same parse.
#[derive(Debug)]
pub(crate) struct AvcConfig {
    pub(crate) sets: ParameterSets,
    pub(crate) fingerprint: ConfigFingerprint,
}

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
struct AvccRecord {
    header: CodecProjection,
    /// `(chroma_format_idc, bit_depth_luma, bit_depth_chroma)` declared by
    /// the profile extension, when the record carries one.
    extension: Option<(u8, u8, u8)>,
    sets: ParameterSets,
    fingerprint: ConfigFingerprint,
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
fn parse_avcc_record(avcc: &[u8]) -> Result<AvccRecord, String> {
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
/// and CBS (`ff_cbs_read_extradata`) parses it as a full syntax tree, not
/// a stored-bytes identity.
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
/// this triple and record identities canonicalize to it. The consistency
/// check additionally admits the raw-syntax triple, which survives
/// FFmpeg's verbatim extradata copy on remux.
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
/// Annex-B fingerprint and the record-side fingerprint all canonicalize
/// to this derivation, so the synthesize-reparse gate compares like with
/// like; the record consistency check is wider — it also admits the raw
/// SPS-coded triple, which survives the writer's verbatim copy of a
/// non-Annex-B extradata.
fn derived_extension(first_sps: &[u8], first_summary: &SpsSummary) -> Option<(u8, u8, u8)> {
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

/// Strips emulation prevention bytes (`00 00 03` -> `00 00`) from a NAL
/// payload (header byte excluded), yielding the RBSP (H.264 7.4.1.1).
fn unescape_rbsp(payload: &[u8]) -> Vec<u8> {
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
    rbsp
}

/// The SPS fields later validation stages consume: the identity a PPS
/// resolves its `seq_parameter_set_id` reference against, the chroma /
/// bit-depth block the avcC profile extension, the PPS scaling-matrix
/// size and the `pic_init_qp_minus26` bound derive from, and the coded
/// dimensions the PPS slice-group map is held against.
#[derive(Debug, Clone, Copy)]
struct SpsSummary {
    sps_id: u32,
    chroma_format_idc: u8,
    bit_depth_luma: u8,
    bit_depth_chroma: u8,
    /// `PicWidthInMbs` (H.264 7-13): `pic_width_in_mbs_minus1 + 1`.
    pic_width_in_mbs: u32,
    /// `PicHeightInMapUnits` (H.264 7-16):
    /// `pic_height_in_map_units_minus1 + 1` — map units, NOT frame
    /// macroblock rows (a field-coded picture holds two map units per
    /// frame MB column). `PicSizeInMapUnits`, the budget every FMO
    /// slice-group shape is bounded by (7.4.2.2), is the product of
    /// these two fields (7-17).
    pic_height_in_map_units: u32,
}

impl SpsSummary {
    /// The `(chroma_format_idc, bit_depth_luma, bit_depth_chroma)` triple
    /// the avcC profile extension mirrors.
    fn chroma_info(&self) -> (u8, u8, u8) {
        (self.chroma_format_idc, self.bit_depth_luma, self.bit_depth_chroma)
    }
}

/// Parses one complete SPS NAL (header byte included) through
/// `rbsp_trailing_bits` (H.264 7.3.2.1.1), returning the [`SpsSummary`] —
/// 4:2:0 / 8-bit defaults when the profile does not carry the chroma
/// fields. Every extradata path routes each SPS body through this parse,
/// so a configuration whose SPS cannot be read to its end (truncated
/// syntax, out-of-range fields, a corrupt stop bit) is rejected up front
/// instead of being handed to consumers as valid.
///
/// Field bounds mirror `ff_h264_decode_seq_parameter_set`
/// (libavcodec/h264_ps.c): the high-profile dispatch set (including
/// profile_idc 144), `seq_parameter_set_id <= 31` (MAX_SPS_COUNT),
/// `delta_scale` in [-128, 127] inside scaling lists,
/// `log2_max_frame_num_minus4 <= 12`, `pic_order_cnt_type <= 2`,
/// `log2_max_pic_order_cnt_lsb_minus4 <= 12`,
/// `num_ref_frames_in_pic_order_cnt_cycle < 256`,
/// `max_num_ref_frames <= 16` (H264_MAX_DPB_FRAMES), the coded-dimension
/// sanity gate (its av_image_check_size call), the cropping gate (a crop
/// must leave at least one sample each way) and, inside the VUI HRD,
/// `cpb_cnt_minus1 <= 31`. Deliberate divergences, each toward the spec
/// where h264_ps.c substitutes a decoder recovery policy or omits the
/// check:
/// * for some broken encoders h264_ps.c only WARNS when declared-present
///   VUI data runs out mid-structure, because a decoder can still play
///   the stream; a validator has no decode path to fall back on, so
///   truncation anywhere inside the declared syntax is rejected here;
/// * zeroed `timing_info` (h264_ps.c drops the flag and plays on) and
///   out-of-range `chroma_sample_loc_type` (h2645_vui.c maps values above
///   5 to "unspecified") are E.2.1 violations a validator rejects instead
///   of scrubbing;
/// * the bitstream_restriction denominators and MV-length exponents are
///   held to their E.2.1 ceilings (16), which h264_ps.c reads unchecked,
///   and `max_dec_frame_buffering` to `max_num_ref_frames..=16`. The 16
///   ceiling on both DPB fields is FFmpeg's CBS bound
///   (`ue(..., 0, H264_MAX_DPB_FRAMES)`,
///   cbs_h264_syntax_template.c:196-197); the floor is E.2.1's own rule —
///   max_dec_frame_buffering shall be >= max_num_ref_frames, the declared
///   DPB must hold the reference frames — which no FFmpeg path enforces:
///   h264_ps.c only reads the field, and its reorder-depth check tests
///   `> 16` alone, the max_dec_frame_buffering clauses sitting inside a
///   comment.
fn parse_sps(sps: &[u8]) -> Result<SpsSummary, String> {
    let rbsp = unescape_rbsp(&sps[1..]);
    let mut r = BitReader::new(&rbsp);
    let profile_idc = r.bits(8)? as u8;
    r.bits(6)?; // constraint_set0..5_flag
    // reserved_zero_2bits "shall be equal to 0" (H.264 7.4.2.1.1) and
    // conforming encoders comply. FFmpeg's decoder and its avcC writer's
    // own SPS reader both skip the pair unchecked (`skip_bits(gb, 2)` in
    // libavcodec/h264_ps.c and libavformat/avc.c); the CBS reader pins the
    // field to zero (`u(2, reserved_zero_2bits, 0, 0)`,
    // libavcodec/cbs_h264_syntax_template.c), and a validator has no
    // reason to wave through bits the spec forbids.
    let reserved_zero_2bits = r.bits(2)?;
    if reserved_zero_2bits != 0 {
        return Err(format!(
            "reserved_zero_2bits is {reserved_zero_2bits} (7.4.2.1.1 requires 0)"
        ));
    }
    r.bits(8)?; // level_idc
    let sps_id = r.ue()?;
    if sps_id > 31 {
        return Err(format!("seq_parameter_set_id {sps_id} exceeds 31"));
    }
    let chroma_info = match profile_idc {
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135 | 144 => {
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
            r.bits(1)?; // qpprime_y_zero_transform_bypass_flag
            if r.bits(1)? == 1 {
                // seq_scaling_matrix_present_flag: 8 lists (12 for 4:4:4),
                // the first 6 sized 16 and the rest sized 64.
                let lists = if chroma_format_idc != 3 { 8 } else { 12 };
                for i in 0..lists {
                    if r.bits(1)? == 1 {
                        skip_scaling_list(&mut r, if i < 6 { 16 } else { 64 })?;
                    }
                }
            }
            (
                chroma_format_idc as u8,
                bit_depth_luma_minus8 as u8 + 8,
                bit_depth_chroma_minus8 as u8 + 8,
            )
        }
        // Profiles outside the dispatch set carry no chroma block; the
        // 4:2:0 / 8-bit projection mirrors BOTH FFmpeg parsers this file
        // tracks: the decoder (libavcodec/h264_ps.c) and the avcC writer's
        // own SPS reader (ff_avc_decode_sps, libavformat/avc.c) infer
        // chroma_format_idc 1 unconditionally. FFmpeg's CBS reader instead
        // infers monochrome for profile_idc 183 (`infer(chroma_format_idc,
        // current->profile_idc == 183 ? 0 : 1)`,
        // libavcodec/cbs_h264_syntax_template.c); following it here would
        // make the synthesized avcC extension disagree with what
        // ff_isom_write_avcc itself writes for the same SPS, and the
        // header consistency check would then reject FFmpeg's own records.
        _ => (1, 8, 8),
    };
    let (chroma_format_idc, bit_depth_luma, bit_depth_chroma) = chroma_info;
    let log2_max_frame_num_minus4 = r.ue()?;
    if log2_max_frame_num_minus4 > 12 {
        return Err(format!(
            "log2_max_frame_num_minus4 {log2_max_frame_num_minus4} out of range (0..=12)"
        ));
    }
    let pic_order_cnt_type = r.ue()?;
    match pic_order_cnt_type {
        0 => {
            let log2_max_poc_lsb_minus4 = r.ue()?;
            if log2_max_poc_lsb_minus4 > 12 {
                return Err(format!(
                    "log2_max_pic_order_cnt_lsb_minus4 {log2_max_poc_lsb_minus4} \
                     out of range (0..=12)"
                ));
            }
        }
        1 => {
            r.bits(1)?; // delta_pic_order_always_zero_flag
            r.se()?; // offset_for_non_ref_pic
            r.se()?; // offset_for_top_to_bottom_field
            let cycle_len = r.ue()?;
            if cycle_len >= 256 {
                return Err(format!(
                    "num_ref_frames_in_pic_order_cnt_cycle {cycle_len} exceeds 255"
                ));
            }
            for _ in 0..cycle_len {
                r.se()?; // offset_for_ref_frame[i]
            }
        }
        2 => {}
        other => return Err(format!("pic_order_cnt_type {other} (must be <= 2)")),
    }
    let max_num_ref_frames = r.ue()?;
    // H264_MAX_DPB_FRAMES: no level defines a DPB deeper than 16 frames
    // (A.3.1), and h264_ps.c rejects a larger max_num_ref_frames outright
    // ("too many reference frames").
    if max_num_ref_frames > 16 {
        return Err(format!(
            "max_num_ref_frames {max_num_ref_frames} exceeds 16"
        ));
    }
    r.bits(1)?; // gaps_in_frame_num_value_allowed_flag
    let pic_width_in_mbs = r.ue()? as u64 + 1; // pic_width_in_mbs_minus1
    let pic_height_in_map_units = r.ue()? as u64 + 1; // ..._minus1
    let frame_mbs_only = r.bits(1)? == 1;
    if !frame_mbs_only {
        r.bits(1)?; // mb_adaptive_frame_field_flag
    }
    // Coded-dimension sanity, the ff_h264_decode_seq_parameter_set gate
    // ("mb_width/height overflow", via av_image_check_size): a dimension of
    // zero is unrepresentable here (the syntax codes minus-one values), so
    // the live bounds are the ceilings. 65535 caps both axes — larger
    // sizes overflow the 16-bit VisualSampleEntry width/height fields
    // (ISO/IEC 14496-12, 12.1.3.2) this configuration feeds — and the
    // buffer-size product guard mirrors av_image_check_size2
    // (libavutil/imgutils.c): the 8-bytes-per-sample linesize plus
    // alignment slack, times the padded height, must stay below INT_MAX.
    let width = 16 * pic_width_in_mbs;
    let height = 16 * pic_height_in_map_units * if frame_mbs_only { 1 } else { 2 };
    if width > 65535 || height > 65535 {
        return Err(format!(
            "coded picture size {width}x{height} exceeds 65535 on an axis"
        ));
    }
    if (8 * width + 1024) * (height + 128) >= 1 << 31 {
        return Err(format!(
            "coded picture size {width}x{height} overflows the sample buffer bound"
        ));
    }
    r.bits(1)?; // direct_8x8_inference_flag
    if r.bits(1)? == 1 {
        // frame_cropping_flag: the four offsets count in crop units — one
        // chroma sample horizontally for 4:2:0/4:2:2 and vertically for
        // 4:2:0, with the vertical unit doubled again for field-coded
        // streams (7.4.2.1.1 via SubWidthC/SubHeightC; the same step_x /
        // step_y derivation as h264_ps.c). A total crop reaching either
        // coded dimension removes the whole picture and is rejected there
        // ("crop values invalid").
        let crop_left = r.ue()? as u64;
        let crop_right = r.ue()? as u64;
        let crop_top = r.ue()? as u64;
        let crop_bottom = r.ue()? as u64;
        let step_x: u64 = if chroma_format_idc == 1 || chroma_format_idc == 2 { 2 } else { 1 };
        let step_y: u64 = (if chroma_format_idc == 1 { 2 } else { 1 })
            * (if frame_mbs_only { 1 } else { 2 });
        if (crop_left + crop_right) * step_x >= width
            || (crop_top + crop_bottom) * step_y >= height
        {
            return Err(format!(
                "frame cropping {crop_left}/{crop_right}/{crop_top}/{crop_bottom} \
                 removes the whole {width}x{height} picture"
            ));
        }
    }
    if r.bits(1)? == 1 {
        // vui_parameters_present_flag
        skip_vui(&mut r, max_num_ref_frames)?;
    }
    r.finish_rbsp()?;
    Ok(SpsSummary {
        sps_id,
        chroma_format_idc,
        bit_depth_luma,
        bit_depth_chroma,
        // Both fit u32: the 65535-pixel axis caps above bound them to 4095.
        pic_width_in_mbs: pic_width_in_mbs as u32,
        pic_height_in_map_units: pic_height_in_map_units as u32,
    })
}

/// Consumes one `scaling_list()` (H.264 7.3.2.1.1.1) without storing the
/// coefficients — validation only needs the syntax length. The
/// `delta_scale` recurrence mirrors the decode loop in
/// libavcodec/h264_ps.c: `nextScale = (lastScale + delta_scale + 256) % 256`,
/// with `lastScale` carried forward once a zero `nextScale` switches the
/// remainder of the list to the default matrix (no further `delta_scale`
/// codes are present after that point). Each `delta_scale` must lie in
/// [-128, 127] (7.4.2.1.1.1); `decode_scaling_list` rejects the same range
/// ("delta scale %d is invalid").
fn skip_scaling_list(r: &mut BitReader, size: u32) -> Result<(), String> {
    let mut last_scale: i64 = 8;
    let mut next_scale: i64 = 8;
    for _ in 0..size {
        if next_scale != 0 {
            let delta_scale = r.se()?;
            if !(-128..=127).contains(&delta_scale) {
                return Err(format!("delta_scale {delta_scale} outside [-128, 127]"));
            }
            next_scale = (last_scale + delta_scale + 256).rem_euclid(256);
        }
        if next_scale != 0 {
            last_scale = next_scale;
        }
    }
    Ok(())
}

/// Parses one complete PPS NAL (header byte included) through
/// `rbsp_trailing_bits` (H.264 7.3.2.2), returning its
/// `pic_parameter_set_id` plus the index (into `sps_context`) of the SPS
/// the `seq_parameter_set_id` reference resolved — the binding the
/// fingerprint records. Every extradata path routes each PPS body
/// through this parse; `sps_context` holds one summary per SPS that
/// PRECEDES the PPS in the configuration — the sets a sequential reader
/// (decode_extradata_ps, libavcodec/h264_parse.c) would have decoded when
/// the PPS arrives, so a PPS can only bind backward. The referenced SPS
/// supplies the chroma_format_idc that sizes the pic_scaling_matrix
/// block, the same way `ff_h264_decode_picture_parameter_set`
/// (libavcodec/h264_ps.c) reads it from the `pps->sps` it resolves first.
///
/// Field bounds mirror h264_ps.c: `pic_parameter_set_id <= 255`
/// (MAX_PPS_COUNT = 256), `seq_parameter_set_id <= 31` (MAX_SPS_COUNT =
/// 32) with the reference required to resolve (a PPS naming an absent SPS
/// fails there with the same "sps_id out of range" as an oversized id, and
/// `ff_h264_decode_extradata` fails the whole extradata over it),
/// `num_ref_idx_l0/l1_default_active_minus1 <= 31` ("reference overflow
/// (pps)"), both chroma_qp_index_offset fields in [-12, 12], and
/// `pic_init_qp_minus26` held to the range 7.4.2.2 grants under the
/// REFERENCED SPS's bit depth — h264_ps.c folds the same
/// `6 * bit_depth_luma_minus8` offset into `init_qp` and defers the range
/// check to slice time (the "QP %u out of range" rejection in
/// h264_slice.c); a validator has the resolved SPS in hand and needs no
/// slice to check against.
/// Divergences, each toward the spec where h264_ps.c substitutes a
/// decoder-capability policy:
/// * the multi-group slice_group_map syntax is parsed per 7.3.2.2 with
///   `num_slice_groups_minus1 <= 7` (the Annex A.2.1 FMO limit) and the
///   per-shape semantic bounds held against the referenced SPS
///   ([`check_slice_group_map`]); h264_ps.c instead refuses every
///   multi-group PPS as unsupported FMO — a statement about its decoder,
///   not about the bitstream;
/// * `weighted_bipred_idc <= 2` (7.4.2.2); h264_ps.c reads the two bits
///   unchecked;
/// * the `more_rbsp_data()` tail is decided by the spec definition (7.2,
///   via [`BitReader::more_rbsp_data`]); h264_ps.c substitutes a profile
///   heuristic (`more_rbsp_data_in_pps`) that ignores the tail wholesale
///   on constrained Baseline/Main/Extended streams because some such
///   encoders pad the RBSP and it never verifies rbsp_trailing_bits. The
///   validator verifies the trailing bits instead, which accepts
///   conforming padding and rejects garbage tails on every profile.
fn parse_pps(pps: &[u8], sps_context: &[SpsSummary]) -> Result<(u32, usize), String> {
    let rbsp = unescape_rbsp(&pps[1..]);
    let mut r = BitReader::new(&rbsp);
    let pps_id = r.ue()?;
    if pps_id > 255 {
        return Err(format!("pic_parameter_set_id {pps_id} exceeds 255"));
    }
    let sps_id = r.ue()?;
    if sps_id > 31 {
        return Err(format!("seq_parameter_set_id {sps_id} exceeds 31"));
    }
    // The last preceding summary with the id wins, mirroring the sps_list
    // slot replacement in h264_ps.c (a re-sent id overwrites its
    // predecessor); an SPS that only appears LATER in the configuration is
    // invisible here, exactly as it is to a sequential decode.
    let (bound_index, sps) = sps_context
        .iter()
        .enumerate()
        .rev()
        .find(|(_, s)| s.sps_id == sps_id)
        .ok_or_else(|| {
            format!("references seq_parameter_set_id {sps_id}, which no preceding SPS in the configuration carries")
        })?;
    r.bits(1)?; // entropy_coding_mode_flag
    r.bits(1)?; // bottom_field_pic_order_in_frame_present_flag
    let num_slice_groups_minus1 = r.ue()?;
    if num_slice_groups_minus1 > 7 {
        return Err(format!(
            "num_slice_groups_minus1 {num_slice_groups_minus1} exceeds 7"
        ));
    }
    if num_slice_groups_minus1 > 0 {
        check_slice_group_map(&mut r, num_slice_groups_minus1, sps)?;
    }
    let num_ref_idx_l0 = r.ue()?;
    let num_ref_idx_l1 = r.ue()?;
    if num_ref_idx_l0 > 31 || num_ref_idx_l1 > 31 {
        return Err(format!(
            "num_ref_idx_l0/l1_default_active_minus1 {num_ref_idx_l0}/{num_ref_idx_l1} \
             exceeds 31"
        ));
    }
    r.bits(1)?; // weighted_pred_flag
    let weighted_bipred_idc = r.bits(2)?;
    if weighted_bipred_idc > 2 {
        return Err(format!(
            "weighted_bipred_idc {weighted_bipred_idc} (must be <= 2)"
        ));
    }
    let pic_init_qp_minus26 = r.se()?;
    // 7.4.2.2 bounds pic_init_qp_minus26 to -(26 + QpBdOffsetY) .. +25 with
    // QpBdOffsetY = 6 * bit_depth_luma_minus8 (7.4.2.1.1): [-26, 25] for
    // an 8-bit SPS, the floor deepening by 6 per extra bit down to -62 at
    // the deepest legal (14-bit) depth. The bound follows the SPS this PPS
    // RESOLVED above — the same pairing h264_ps.c uses when it folds
    // qp_bd_offset into init_qp, deferring the range rejection to slice
    // time ("QP %u out of range", h264_slice.c) only because a decoder has
    // no earlier point where both halves meet; a validator does.
    let qp_floor = -(26 + 6 * (sps.bit_depth_luma as i64 - 8));
    if !(qp_floor..=25).contains(&pic_init_qp_minus26) {
        return Err(format!(
            "pic_init_qp_minus26 {pic_init_qp_minus26} outside [{qp_floor}, 25] \
             (the referenced SPS codes {}-bit luma)",
            sps.bit_depth_luma
        ));
    }
    let pic_init_qs_minus26 = r.se()?;
    // SP/SI quantization has no bit-depth offset: -26 .. +25 flat (7.4.2.2).
    if !(-26..=25).contains(&pic_init_qs_minus26) {
        return Err(format!(
            "pic_init_qs_minus26 {pic_init_qs_minus26} outside [-26, 25]"
        ));
    }
    let chroma_qp_index_offset = r.se()?;
    if !(-12..=12).contains(&chroma_qp_index_offset) {
        return Err(format!(
            "chroma_qp_index_offset {chroma_qp_index_offset} outside [-12, 12]"
        ));
    }
    r.bits(1)?; // deblocking_filter_control_present_flag
    r.bits(1)?; // constrained_intra_pred_flag
    r.bits(1)?; // redundant_pic_cnt_present_flag
    if r.more_rbsp_data() {
        let transform_8x8_mode_flag = r.bits(1)? == 1;
        if r.bits(1)? == 1 {
            // pic_scaling_matrix_present_flag: six 4x4 lists, plus the 8x8
            // block only under transform_8x8_mode_flag — two lists, or six
            // when the referenced SPS codes 4:4:4 (7.3.2.2; the set
            // decode_scaling_matrices reads for a PPS).
            let lists = 6
                + if transform_8x8_mode_flag {
                    if sps.chroma_format_idc != 3 {
                        2
                    } else {
                        6
                    }
                } else {
                    0
                };
            for i in 0..lists {
                if r.bits(1)? == 1 {
                    skip_scaling_list(&mut r, if i < 6 { 16 } else { 64 })?;
                }
            }
        }
        let second_chroma_qp_index_offset = r.se()?;
        if !(-12..=12).contains(&second_chroma_qp_index_offset) {
            return Err(format!(
                "second_chroma_qp_index_offset {second_chroma_qp_index_offset} \
                 outside [-12, 12]"
            ));
        }
    }
    r.finish_rbsp()?;
    Ok((pps_id, bound_index))
}

/// Parses the multi-group slice_group_map syntax of 7.3.2.2 (map types
/// 0..=6) and holds each shape to its 7.4.2.2 semantics against the
/// REFERENCED SPS. h264_ps.c has no equivalent to mirror — it refuses
/// every `num_slice_groups_minus1 > 0` PPS as unsupported FMO — so both
/// the shapes and the bounds come straight from the spec, with
/// `PicSizeInMapUnits = PicWidthInMbs * PicHeightInMapUnits` (7-17) as the
/// map-unit budget:
/// * type 0: each `run_length_minus1[i]` <= PicSizeInMapUnits - 1;
/// * type 2: foreground rectangles for all but the last group (the loop
///   runs `num_slice_groups_minus1` times — the final group is the
///   leftover background); `bottom_right[i]` < PicSizeInMapUnits,
///   `top_left[i] <= bottom_right[i]`, and the column order
///   `top_left[i] % PicWidthInMbs <= bottom_right[i] % PicWidthInMbs`;
/// * types 3..=5 (the changing maps): defined only for exactly two slice
///   groups (`num_slice_groups_minus1` shall be 1), with
///   `slice_group_change_rate_minus1 <= PicSizeInMapUnits - 1`;
/// * type 6: the explicit table covers the picture exactly —
///   `pic_size_in_map_units_minus1` shall EQUAL PicSizeInMapUnits - 1
///   (7.4.2.2; cbs_h264_syntax_template.c codes the field with that
///   value as both of its ue bounds) — and every `slice_group_id[i]`
///   <= `num_slice_groups_minus1`.
///
/// Type 1 (dispersed) carries no syntax.
fn check_slice_group_map(
    r: &mut BitReader,
    num_slice_groups_minus1: u32,
    sps: &SpsSummary,
) -> Result<(), String> {
    let pic_size_in_map_units =
        sps.pic_width_in_mbs as u64 * sps.pic_height_in_map_units as u64;
    let map_type = r.ue()?;
    match map_type {
        0 => {
            for _ in 0..=num_slice_groups_minus1 {
                let run_length_minus1 = r.ue()?;
                if run_length_minus1 as u64 >= pic_size_in_map_units {
                    return Err(format!(
                        "slice-group run_length_minus1 {run_length_minus1} reaches past \
                         the {pic_size_in_map_units} map units of the referenced SPS"
                    ));
                }
            }
        }
        1 => {}
        2 => {
            for _ in 0..num_slice_groups_minus1 {
                let top_left = r.ue()?;
                let bottom_right = r.ue()?;
                if bottom_right as u64 >= pic_size_in_map_units {
                    return Err(format!(
                        "slice-group bottom_right {bottom_right} is outside the \
                         {pic_size_in_map_units} map units of the referenced SPS"
                    ));
                }
                let width = sps.pic_width_in_mbs;
                if top_left > bottom_right || top_left % width > bottom_right % width {
                    return Err(format!(
                        "slice-group rectangle {top_left}..{bottom_right} is inverted \
                         (7.4.2.2 orders both corners, by map unit and by column)"
                    ));
                }
            }
        }
        3..=5 => {
            if num_slice_groups_minus1 != 1 {
                return Err(format!(
                    "slice_group_map_type {map_type} requires exactly two slice groups \
                     (num_slice_groups_minus1 is {num_slice_groups_minus1})"
                ));
            }
            r.bits(1)?; // slice_group_change_direction_flag
            let change_rate_minus1 = r.ue()?;
            if change_rate_minus1 as u64 >= pic_size_in_map_units {
                return Err(format!(
                    "slice_group_change_rate_minus1 {change_rate_minus1} reaches past \
                     the {pic_size_in_map_units} map units of the referenced SPS"
                ));
            }
        }
        6 => {
            // 7.4.2.2 fixes the table to the picture:
            // pic_size_in_map_units_minus1 shall be EQUAL to
            // PicSizeInMapUnits - 1 (cbs_h264_syntax_template.c pins the
            // ue range to exactly that value). An undersized table is as
            // broken as an oversized one — the map units past its end
            // belong to no slice group.
            let pic_size_in_map_units_minus1 = r.ue()?;
            if pic_size_in_map_units_minus1 as u64 + 1 != pic_size_in_map_units {
                return Err(format!(
                    "slice-group table declares {} map units but the referenced SPS \
                     codes {pic_size_in_map_units}",
                    pic_size_in_map_units_minus1 as u64 + 1
                ));
            }
            // slice_group_id[i] is u(v) with v = Ceil(Log2(
            // num_slice_groups_minus1 + 1)) (7.4.2.2), which equals the bit
            // length of num_slice_groups_minus1 for every value >= 1 —
            // 1..=3 bits over the 2..=8 groups admitted above.
            let width = 32 - num_slice_groups_minus1.leading_zeros();
            for _ in 0..=pic_size_in_map_units_minus1 {
                let slice_group_id = r.bits(width)?;
                if slice_group_id > num_slice_groups_minus1 {
                    return Err(format!(
                        "slice_group_id {slice_group_id} exceeds \
                         num_slice_groups_minus1 {num_slice_groups_minus1}"
                    ));
                }
            }
        }
        other => return Err(format!("slice_group_map_type {other} (must be <= 6)")),
    }
    Ok(())
}

/// Consumes `vui_parameters()` (H.264 E.1.1), the field set
/// libavcodec/h2645_vui.c and h264_ps.c read. Values are not retained; the
/// pass exists so a declared-present VUI that ends mid-structure fails the
/// parse instead of leaving unread syntax in front of `rbsp_trailing_bits`,
/// and so the E.2.1 value bounds hold: `chroma_sample_loc_type_* <= 5`,
/// nonzero `timing_info` (the h264_ps.c "time_scale/num_units_in_tick
/// invalid" condition — it scrubs the flag, a validator rejects), and the
/// bitstream_restriction set (`max_bytes_per_pic_denom <= 16`,
/// `max_bits_per_mb_denom <= 16`, `log2_max_mv_length_* <= 16`,
/// `max_num_reorder_frames <= max_dec_frame_buffering` and `<= 16`, and
/// `max_num_ref_frames <= max_dec_frame_buffering <= 16`). On the DPB
/// pair, FFmpeg enforces only the 16 ceiling — CBS codes both fields as
/// `ue(..., 0, H264_MAX_DPB_FRAMES)` (cbs_h264_syntax_template.c:196-197),
/// and the h264_ps.c "illegal num_reorder_frames" rejection fires on
/// `> 16` alone, its max_dec_frame_buffering clauses sitting inside a
/// comment. The cross-field bounds are E.2.1's own rules:
/// max_num_reorder_frames shall not exceed max_dec_frame_buffering, and
/// max_dec_frame_buffering shall be >= max_num_ref_frames — a ref-count
/// floor no FFmpeg path checks; the caller passes the SPS's
/// `max_num_ref_frames` in for it.
fn skip_vui(r: &mut BitReader, max_num_ref_frames: u32) -> Result<(), String> {
    if r.bits(1)? == 1 {
        // aspect_ratio_info_present_flag
        if r.bits(8)? == 255 {
            // aspect_ratio_idc == Extended_SAR
            r.bits(16)?; // sar_width
            r.bits(16)?; // sar_height
        }
    }
    if r.bits(1)? == 1 {
        // overscan_info_present_flag
        r.bits(1)?; // overscan_appropriate_flag
    }
    if r.bits(1)? == 1 {
        // video_signal_type_present_flag
        r.bits(3)?; // video_format
        r.bits(1)?; // video_full_range_flag
        if r.bits(1)? == 1 {
            // colour_description_present_flag: colour_primaries,
            // transfer_characteristics, matrix_coefficients
            r.bits(24)?;
        }
    }
    if r.bits(1)? == 1 {
        // chroma_loc_info_present_flag
        let top = r.ue()?; // chroma_sample_loc_type_top_field
        let bottom = r.ue()?; // chroma_sample_loc_type_bottom_field
        if top > 5 || bottom > 5 {
            return Err(format!(
                "chroma_sample_loc_type {top}/{bottom} exceeds 5"
            ));
        }
    }
    if r.bits(1)? == 1 {
        // timing_info_present_flag: E.2.1 requires both fields nonzero (a
        // zero denominator or numerator makes the declared tick undefined).
        let num_units_in_tick = r.bits(32)?;
        let time_scale = r.bits(32)?;
        if num_units_in_tick == 0 || time_scale == 0 {
            return Err(format!(
                "timing_info {num_units_in_tick}/{time_scale} carries a zero field"
            ));
        }
        r.bits(1)?; // fixed_frame_rate_flag
    }
    let nal_hrd = r.bits(1)? == 1; // nal_hrd_parameters_present_flag
    if nal_hrd {
        skip_hrd_parameters(r)?;
    }
    let vcl_hrd = r.bits(1)? == 1; // vcl_hrd_parameters_present_flag
    if vcl_hrd {
        skip_hrd_parameters(r)?;
    }
    if nal_hrd || vcl_hrd {
        r.bits(1)?; // low_delay_hrd_flag
    }
    r.bits(1)?; // pic_struct_present_flag
    if r.bits(1)? == 1 {
        // bitstream_restriction_flag
        r.bits(1)?; // motion_vectors_over_pic_boundaries_flag
        let max_bytes_per_pic_denom = r.ue()?;
        let max_bits_per_mb_denom = r.ue()?;
        let log2_max_mv_length_horizontal = r.ue()?;
        let log2_max_mv_length_vertical = r.ue()?;
        let max_num_reorder_frames = r.ue()?;
        let max_dec_frame_buffering = r.ue()?;
        if max_bytes_per_pic_denom > 16 {
            return Err(format!(
                "max_bytes_per_pic_denom {max_bytes_per_pic_denom} exceeds 16"
            ));
        }
        if max_bits_per_mb_denom > 16 {
            return Err(format!(
                "max_bits_per_mb_denom {max_bits_per_mb_denom} exceeds 16"
            ));
        }
        if log2_max_mv_length_horizontal > 16 || log2_max_mv_length_vertical > 16 {
            return Err(format!(
                "log2_max_mv_length {log2_max_mv_length_horizontal}/\
                 {log2_max_mv_length_vertical} exceeds 16"
            ));
        }
        // E.2.1 bounds max_num_reorder_frames by max_dec_frame_buffering,
        // and no level admits more than 16 held frames. Of the two
        // clauses only the 16 ceiling is FFmpeg practice
        // (H264_MAX_DPB_FRAMES: cbs_h264_syntax_template.c:196, and the
        // h264_ps.c "illegal num_reorder_frames" check, whose
        // max_dec_frame_buffering comparison is commented out); the
        // cross-field bound is E.2.1's own rule.
        if max_num_reorder_frames > 16 || max_num_reorder_frames > max_dec_frame_buffering {
            return Err(format!(
                "max_num_reorder_frames {max_num_reorder_frames} exceeds 16 or \
                 max_dec_frame_buffering {max_dec_frame_buffering}"
            ));
        }
        // max_dec_frame_buffering itself is bounded both ways. The 16
        // ceiling is FFmpeg's CBS bound (`ue(max_dec_frame_buffering, 0,
        // H264_MAX_DPB_FRAMES)`, cbs_h264_syntax_template.c:197). The
        // floor is E.2.1's own rule — max_dec_frame_buffering shall be
        // >= max_num_ref_frames, the declared DPB must hold at least the
        // reference frames the SPS keeps — which no FFmpeg path enforces:
        // h264_ps.c only reads the field.
        if max_dec_frame_buffering > 16 || max_dec_frame_buffering < max_num_ref_frames {
            return Err(format!(
                "max_dec_frame_buffering {max_dec_frame_buffering} is outside \
                 max_num_ref_frames {max_num_ref_frames}..=16"
            ));
        }
    }
    Ok(())
}

/// Consumes `hrd_parameters()` (H.264 E.1.2). `cpb_cnt_minus1` is bounded
/// to 31 as in libavcodec/h264_ps.c (and E.2.2), which also keeps the CPB
/// loop from consuming an attacker-controlled iteration count.
fn skip_hrd_parameters(r: &mut BitReader) -> Result<(), String> {
    let cpb_cnt_minus1 = r.ue()?;
    if cpb_cnt_minus1 > 31 {
        return Err(format!("cpb_cnt_minus1 {cpb_cnt_minus1} exceeds 31"));
    }
    r.bits(4)?; // bit_rate_scale
    r.bits(4)?; // cpb_size_scale
    for _ in 0..=cpb_cnt_minus1 {
        r.ue()?; // bit_rate_value_minus1
        r.ue()?; // cpb_size_value_minus1
        r.bits(1)?; // cbr_flag
    }
    r.bits(5)?; // initial_cpb_removal_delay_length_minus1
    r.bits(5)?; // cpb_removal_delay_length_minus1
    r.bits(5)?; // dpb_output_delay_length_minus1
    r.bits(5)?; // time_offset_length
    Ok(())
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
                .ok_or_else(|| "RBSP truncated".to_string())?;
            let bit = (byte >> (7 - (self.pos % 8))) & 1;
            v = (v << 1) | bit as u32;
            self.pos += 1;
        }
        Ok(v)
    }

    /// `more_rbsp_data()` (H.264 7.2): syntax data remains exactly when the
    /// current position is strictly before the last set bit of the RBSP —
    /// that bit being the rbsp_stop_one_bit. A remainder with no set bit at
    /// all reports false and leaves the malformed trailing bits to
    /// [`Self::finish_rbsp`].
    fn more_rbsp_data(&self) -> bool {
        for (i, &b) in self.data.iter().enumerate().rev() {
            if b != 0 {
                let last_set = i * 8 + 7 - b.trailing_zeros() as usize;
                return self.pos < last_set;
            }
        }
        false
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

    /// Signed Exp-Golomb (H.264 9.1.1): code number k maps to
    /// (-1)^(k+1) * ceil(k / 2). Widened to i64 because k can reach
    /// 2^32 - 2.
    fn se(&mut self) -> Result<i64, String> {
        let k = self.ue()? as i64;
        Ok(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }

    /// `rbsp_trailing_bits()` (H.264 7.3.2.11) plus the end-of-data
    /// requirement: the stop bit must be 1, the alignment bits zero, and no
    /// bytes may remain — an SPS RBSP ends at its trailing bits, so leftover
    /// bytes are foreign data. FFmpeg's decode path does not verify this on
    /// every route (`ff_h264_decode_seq_parameter_set` returns once the
    /// fields it stores are read); a validator confirms the declared syntax
    /// is exactly what the payload carries.
    fn finish_rbsp(&mut self) -> Result<(), String> {
        if self.bits(1)? != 1 {
            return Err("rbsp_trailing_bits stop bit is 0".to_string());
        }
        // `%` (not `is_multiple_of`) keeps this MSRV-1.80 safe;
        // `usize::is_multiple_of` is only stable since 1.87.
        #[allow(clippy::manual_is_multiple_of)]
        while self.pos % 8 != 0 {
            if self.bits(1)? != 0 {
                return Err("nonzero rbsp_trailing_bits alignment bit".to_string());
            }
        }
        let left = self.data.len() - self.pos / 8;
        if left != 0 {
            return Err(format!("{left} byte(s) after rbsp_trailing_bits"));
        }
        Ok(())
    }
}

/// Per-stream H.264 runtime state.
pub(crate) struct AvcRuntime {
    /// Decided once, from the extradata form: Annex-B extradata means
    /// Annex-B packets (rewritten per packet), avcC extradata means already
    /// length-prefixed packets (validated in place). The forms never mix
    /// within one encoder.
    annexb_packets: bool,
    /// Composite S8 baseline, part 1: the identity-keyed fingerprint of
    /// the effective configuration ([`ConfigFingerprint`] — wrapper-
    /// independent, last-wins per parameter-set id).
    baseline: ConfigFingerprint,
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
    /// sets are synthesized into one, preserving wrapper order like movenc,
    /// with the synthesized record reparsed to prove it reproduces the
    /// validated configuration).
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<(Self, Vec<u8>, CodecProjection), PacketSinkError> {
        let invalid = |reason: String| PacketSinkError::InvalidExtradata {
            stream_index,
            reason,
        };
        let annexb_packets = extradata.first() != Some(&1);
        let config = parse_parameter_sets(extradata).map_err(&invalid)?;
        let projection = CodecProjection::from_ordered_sets(&config.sets).map_err(&invalid)?;
        // Header/SPS agreement needs no separate step here: a pass-through
        // record was already held against its own first SPS by
        // `check_avcc_consistency` inside the parse (bytes 1..4 plus the
        // profile-extension fields), and a synthesized record copies those
        // bytes from that same first SPS.
        let delivered = if annexb_packets {
            let record = build_avcc(&config.sets).map_err(&invalid)?;
            // Synthesis-fidelity gate. Annex-B resolves each PPS against
            // the last PRECEDING same-id SPS, while the record's arrays
            // put every SPS ahead of every PPS, so reparsing the record
            // binds each PPS to the last same-id SPS OVERALL. An ordering
            // that re-sends a PPS's SPS generation after the PPS therefore
            // synthesizes a record describing a different configuration —
            // or one whose PPS no longer parses under the rebound SPS at
            // all. The delivered record must mean exactly the
            // configuration validated above, so it is reparsed through the
            // same avcC parser and its identity fingerprint (bindings and
            // the extension triple included) must agree. Single-generation
            // orderings with each
            // PPS after its SPS — every real encoder handoff — bind
            // identically on both reads and pass unchanged.
            let reparsed = parse_avcc_parameter_sets(&record).map_err(|reason| {
                invalid(format!(
                    "the parameter-set interleaving has no faithful avcC \
                     representation (the synthesized record fails to \
                     reparse: {reason})"
                ))
            })?;
            if reparsed.fingerprint != config.fingerprint {
                return Err(invalid(
                    "the parameter-set interleaving has no faithful avcC \
                     representation (reparsing the synthesized record binds \
                     a PPS to a different SPS generation)"
                        .to_string(),
                ));
            }
            record
        } else {
            extradata.to_vec()
        };
        Ok((
            Self {
                annexb_packets,
                baseline: config.fingerprint,
                projection,
            },
            delivered,
            projection,
        ))
    }

    /// S8: a `NEW_EXTRADATA` announcement — one whose EFFECTIVE
    /// configuration (the identity-keyed [`ConfigFingerprint`]) is
    /// unchanged is redundant and passes; anything else is a mid-stream
    /// configuration change.
    ///
    /// An avcC-form announcement passes through the same full record
    /// validation as initial construction (`parse_avcc_record` via
    /// [`parse_parameter_sets`]), including the header-vs-first-SPS
    /// consistency check — so tampered header bytes over unchanged SPS/PPS
    /// fail here rather than slipping past the set-identity comparison.
    pub(crate) fn check_new_extradata(
        &self,
        bytes: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let config = parse_parameter_sets(bytes).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("invalid NEW_EXTRADATA ({reason})"),
            }
        })?;
        // Composite baseline, part 2: the derived projection consumers were
        // told (profile/compatibility/level, from the announcement's own
        // ordered first SPS — the same derivation on_stream_info used). A
        // reorder that changes it IS a configuration change even when the
        // effective identity map below is unchanged.
        let projection = CodecProjection::from_ordered_sets(&config.sets).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("invalid NEW_EXTRADATA SPS ({reason})"),
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
        // Composite baseline, part 1: the effective identity map (and the
        // SPS-EXT list) must be unchanged.
        if config.fingerprint != self.baseline {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "NEW_EXTRADATA changes the effective parameter sets".to_string(),
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

    /// In-band SPS/PPS: sets differing from the ACTIVE baseline (a
    /// replaced same-id predecessor is no longer active) are a
    /// configuration change; value-equal sets fall through to the
    /// strict-tier in-band rejection at the caller. `data` is
    /// length-prefixed (post-normalization).
    fn check_inband_parameter_sets(
        &self,
        data: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let mut mismatch = false;
        let _ = walk_length_prefixed(data, |nal| {
            let matches_baseline = match nal[0] & 0x1F {
                NAL_SPS => self.baseline.has_active_sps(nal),
                NAL_PPS => self.baseline.has_active_pps(nal),
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

    // Encoder-produced parameter sets (x264 via the ffmpeg CLI, header byte
    // included): Constrained Baseline (profile_idc 66, constraint_set0+1 ->
    // compatibility 0xC0), level_idc 30, coding 320x240 yuv420p (20x15
    // macroblocks, frame_mbs_only, no cropping). Profile 66 means no avcC
    // chroma/bit-depth extension applies. The payloads carry real emulation
    // prevention (00 00 03) sequences.
    const SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x01, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
        0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xE4, 0x80,
    ];
    const PPS: &[u8] = &[0x68, 0xCB, 0x83, 0xCB, 0x20];

    // Same encoder, profile and level as `SPS` (identical bytes 1..4, so
    // the derived projection matches) but coding 640x480: same
    // seq_parameter_set_id 0 with a different tail, so it REPLACES `SPS`
    // in the id map when both appear.
    const SAME_PROJ_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x00, 0xA0, 0x3D, 0xB0, 0x11, 0x00, 0x00, 0x03, 0x00,
        0x01, 0x00, 0x00, 0x03, 0x00, 0x32, 0x0F, 0x16, 0x2E, 0x48,
    ];

    // Same encoder and coded shape (320x240 yuv420p 8-bit, level_idc 30) at
    // High profile (profile_idc 100): chroma_format_idc 1 and 8-bit depths,
    // the values the avcC extension must carry.
    const HIGH_SPS: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00,
        0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
    ];
    const HIGH_PPS: &[u8] = &[0x68, 0xEB, 0xE3, 0xCB, 0x22, 0xC0];

    // `HIGH_SPS` with seq_scaling_matrix_present_flag set and eight zero
    // seq_scaling_list_present_flag bits spliced in. The flag is the last
    // bit of RBSP byte 3 (0xAC -> 0xAD) and the insertion point is the byte
    // boundary right after it, so the splice is one flipped bit plus one
    // inserted 0x00 byte; everything downstream is bit-identical.
    const HIGH_SPS_SCALING: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAD, 0x00, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00,
        0x03, 0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
    ];
    // `HIGH_SPS` with profile_idc patched to 144 (a one-byte u(8) field, no
    // bit shifts). 144 is in the chroma-block dispatch set; a parser that
    // omitted it would read the chroma/bit-depth bits as frame_num / POC
    // syntax and end misaligned, failing rbsp_trailing_bits — so this
    // fixture being accepted pins the dispatch membership.
    const PROFILE_144_SPS: &[u8] = &[
        0x67, 0x90, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03,
        0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
    ];
    // `CHROMA3_SCALING_SPS` with profile_idc patched to 144 (a one-byte
    // u(8) field, no bit shifts): the payload still codes chroma_format_idc
    // 3, but 144 is outside the avcC writer's chroma dispatch
    // (ff_avc_decode_sps, libavformat/avc.c), so the record FFmpeg
    // synthesizes from Annex-B carries the writer's (1, 8, 8) default —
    // while a remux copies an existing record's coded 4:4:4 verbatim.
    const PROFILE_144_CHROMA3_SPS: &[u8] = &[
        0x67, 0x90, 0x00, 0x1E, 0x91, 0xB0, 0x88, 0x00, 0xB4, 0x0A, 0x0F, 0xC8,
    ];
    // `TEN_BIT_SPS` with profile_idc patched to 135: the same
    // writer-default class from the depth side — coded 10-bit,
    // synthesized 8-bit.
    const PROFILE_135_TEN_BIT_SPS: &[u8] = &[
        0x67, 0x87, 0x00, 0x1E, 0xA6, 0xCB, 0x40, 0xA0, 0xFC, 0x80,
    ];

    // Hand-assembled 7.3.2.1.1 bit streams (High or Constrained Baseline
    // skeleton: 320x240, frame_mbs_only, no cropping, no VUI unless a field
    // is the point of the fixture).
    //
    // Profile-100 SPS whose scaling list 0 is present with all sixteen
    // delta_scale codes (se(0) each), lists 1..7 absent.
    const SCALING_LIST_SPS: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAD, 0xFF, 0xFF, 0x80, 0xE8, 0x14, 0x1F, 0x90,
    ];
    // Profile-110, chroma_format_idc 3 (separate_colour_plane_flag present,
    // twelve scaling lists): list 0 carries delta_scale -8, driving
    // nextScale to 0 so the remaining fifteen entries have no codes.
    const CHROMA3_SCALING_SPS: &[u8] = &[
        0x67, 0x6E, 0x00, 0x1E, 0x91, 0xB0, 0x88, 0x00, 0xB4, 0x0A, 0x0F, 0xC8,
    ];
    // seq_parameter_set_id 31 (the largest legal value) and 32 (ue one code
    // up): the pair differs in a single bit.
    const SPS_ID_31: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0x04, 0x16, 0x81, 0x41, 0xF9];
    const SPS_ID_32: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0x04, 0x36, 0x81, 0x41, 0xF9];
    // log2_max_frame_num_minus4 at the 12 boundary and one above it.
    const LOG2_FRAME_NUM_12: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0x8D, 0x68, 0x14, 0x1F, 0x90];
    const LOG2_FRAME_NUM_13: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0x8E, 0x68, 0x14, 0x1F, 0x90];
    // pic_order_cnt_type 3 (only 0..=2 exist).
    const POC_TYPE_3: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xC8, 0x81, 0x41, 0xF9];
    // pic_order_cnt_type 1 with num_ref_frames_in_pic_order_cnt_cycle 255
    // (boundary, with all 255 offset_for_ref_frame codes present) and 256
    // (rejected before any offsets are read).
    const POC_CYCLE_255: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD3, 0x00, 0x80, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x40, 0xA0, 0xFC,
        0x80,
    ];
    const POC_CYCLE_256: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD3, 0x00, 0x80, 0xA0, 0x50, 0x7E, 0x40,
    ];
    // frame_cropping_flag set but the payload ends before the four crop
    // offsets complete.
    const MISSING_CROP_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xF8];
    // vui_parameters_present_flag set with the payload ending a few flag
    // bits into the VUI.
    const VUI_TRUNCATED_MIN_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8];
    // `HIGH_SPS` with the rbsp_trailing_bits stop bit flipped to 0 (last
    // byte 0x60 -> 0x40); every field before it is untouched.
    const BAD_STOP_BIT_SPS: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03,
        0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x40,
    ];
    // max_num_ref_frames 16 (the H264_MAX_DPB_FRAMES ceiling) and 17: one
    // ue bit apart on the Constrained Baseline 320x240 skeleton.
    const REF_FRAMES_16_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xD8, 0x44, 0x14, 0x1F, 0x90];
    const REF_FRAMES_17_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xD8, 0x48, 0x14, 0x1F, 0x90];
    // pic_width_in_mbs_minus1 4094 (width 65520, the widest coded picture
    // both 16-bit sample-entry fields and the buffer bound admit at height
    // 16) and 4095 (width 65536, past the 16-bit axis cap).
    const WIDTH_65520_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x00, 0x0F, 0xFF, 0xE4];
    const WIDTH_65536_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x00, 0x04, 0x00, 0x39];
    // 65520x65520: each axis is individually inside the 16-bit cap, the
    // sample-buffer product is not.
    const PIXEL_PRODUCT_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x00, 0x0F, 0xFF, 0x00, 0x1F, 0xFF, 0x90,
    ];
    // Horizontal crop in 4:2:0 chroma units on the 320x240 skeleton:
    // (79 + 80) * 2 = 318 of 320 luma columns survive one column, while
    // (80 + 80) * 2 = 320 removes the whole picture. One ue bit apart.
    const NEAR_FULL_CROP_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xF0, 0x28, 0x01, 0x47, 0x40,
    ];
    const FULL_CROP_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xF0, 0x28, 0x81, 0x47, 0x40,
    ];
    // VUI bitstream_restriction fixtures on the skeleton: every bounded
    // field at its E.2.1 ceiling (both denominators and both MV-length
    // exponents 16, max_num_reorder_frames 16 <= max_dec_frame_buffering
    // 16 — which also pins buffering accepted AT the ceiling), then one
    // field over at a time — reorder 17, reorder 2 against buffering 1,
    // max_bytes_per_pic_denom 17, max_bits_per_mb_denom 17,
    // log2_max_mv_length_horizontal 17.
    const REORDER_16_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x06, 0x11, 0x08, 0x84, 0x42, 0x21,
        0x10, 0x8C,
    ];
    const REORDER_17_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x07, 0xE1, 0x20, 0x94,
    ];
    const REORDER_ABOVE_BUFFERING_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x07, 0xED, 0x40,
    ];
    const BYTES_DENOM_17_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x06, 0x12, 0xFC,
    ];
    const MB_DENOM_17_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x07, 0x09, 0x7C,
    ];
    const MV_LEN_17_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x07, 0x84, 0xBC,
    ];
    // max_dec_frame_buffering boundaries against the skeleton's
    // max_num_ref_frames 1 (reorder 0 in all three, so only the buffering
    // bound decides): 17 breaches the H264_MAX_DPB_FRAMES ceiling, 0
    // undercuts the E.2.1 floor (a DPB that cannot hold the declared
    // reference frame), and the accept pins the floor AT the ceiling —
    // max_num_ref_frames 16 with buffering 16.
    const BUFFERING_17_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x05, 0xF0, 0x94,
    ];
    const BUFFERING_BELOW_REF_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x05, 0xFC,
    ];
    const REF_EQUALS_BUFFERING_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD8, 0x44, 0x14, 0x1F, 0xA0, 0x17, 0xC2, 0x30,
    ];
    // VUI timing_info with time_scale 0, then with num_units_in_tick 0:
    // either zero leaves the declared tick undefined (E.2.1).
    const ZERO_TIME_SCALE_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x40, 0x00, 0x00, 0x03, 0x00, 0x40,
        0x00, 0x00, 0x03, 0x00, 0x01,
    ];
    const ZERO_NUM_UNITS_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x40, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x06, 0x41,
    ];
    // chroma_sample_loc_type 5 on both fields (the E.2.1 ceiling) and 6.
    const CHROMA_LOC_5_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x98, 0xC0, 0x80,
    ];
    const CHROMA_LOC_6_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x9E, 0x08,
    ];
    // High10 (profile_idc 110) 320x240 at 10-bit depth: QpBdOffsetY = 12,
    // deepening the pic_init_qp_minus26 floor of a referencing PPS to -38.
    const TEN_BIT_SPS: &[u8] = &[
        0x67, 0x6E, 0x00, 0x1E, 0xA6, 0xCB, 0x40, 0xA0, 0xFC, 0x80,
    ];
    // Profile-100 scaling list 0 opening with delta_scale -128 (the
    // decode_scaling_list floor; nextScale = (8 - 128 + 256) % 256 = 136,
    // so all sixteen codes are present) and with +128, one past the
    // ceiling — the pair differs inside one se code.
    const DELTA_SCALE_M128_SPS: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAD, 0x80, 0x40, 0x7F, 0xFF, 0x80, 0xB4, 0x0A, 0x0F, 0xC8,
    ];
    const DELTA_SCALE_128_SPS: &[u8] = &[
        0x67, 0x64, 0x00, 0x1E, 0xAD, 0x80, 0x40, 0x3F, 0xFF, 0x80, 0xB4, 0x0A, 0x0F, 0xC8,
    ];
    // 32x32 variant of the skeleton: 2x2 = 4 map units, the
    // hand-manageable PicSizeInMapUnits an explicit type-6 slice-group
    // table must cover exactly.
    const MAP_UNITS_4_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x25, 0x90];

    // Hand-assembled 7.3.2.2 PPS bit streams. The skeleton (`MINIMAL_PPS`)
    // codes both ids 0, CAVLC, one slice group, every ue/se field at its
    // smallest code and no more_rbsp_data tail; each bound fixture below
    // perturbs it at one field, and each reject sits one ue/se code (most
    // one bit) from an accepted sibling.
    const MINIMAL_PPS: &[u8] = &[0x68, 0xCE, 0x38, 0x80];
    // pic_parameter_set_id 255 (the largest legal value) and 256: the pair
    // differs in the final bit of the 17-bit ue code.
    const PPS_ID_255_PPS: &[u8] = &[0x68, 0x00, 0x80, 0x4E, 0x38, 0x80];
    const PPS_ID_256_PPS: &[u8] = &[0x68, 0x00, 0x80, 0xCE, 0x38, 0x80];
    // seq_parameter_set_id 31 (legal, resolvable against `SPS_ID_31`) and
    // 32 (out of range): one bit apart.
    const SPS_ID_31_PPS: &[u8] = &[0x68, 0x82, 0x03, 0x8E, 0x20];
    const SPS_ID_32_PPS: &[u8] = &[0x68, 0x82, 0x13, 0x8E, 0x20];
    // weighted_bipred_idc 2 (the largest legal value) and 3: one bit apart.
    const WEIGHTED_BIPRED_2_PPS: &[u8] = &[0x68, 0xCE, 0xB8, 0x80];
    const WEIGHTED_BIPRED_3_PPS: &[u8] = &[0x68, 0xCE, 0xF8, 0x80];
    // chroma_qp_index_offset +12 and +13: one bit apart inside the se code.
    const CHROMA_QP_12_PPS: &[u8] = &[0x68, 0xCE, 0x30, 0xC0, 0x80];
    const CHROMA_QP_13_PPS: &[u8] = &[0x68, 0xCE, 0x30, 0xD0, 0x80];
    // pic_init_qp_minus26 fixtures: -26 (the 8-bit floor) and -27 (one se
    // bit past it); -32, legal only under a 9-bit-or-deeper SPS
    // (QpBdOffsetY >= 6); -88 and -89, below the floor of every legal
    // depth (the 14-bit floor is -62).
    const INIT_QP_M26_PPS: &[u8] = &[0x68, 0xCE, 0x01, 0xAE, 0x20];
    const INIT_QP_M27_PPS: &[u8] = &[0x68, 0xCE, 0x01, 0xBE, 0x20];
    const INIT_QP_M32_PPS: &[u8] = &[0x68, 0xCE, 0x00, 0x83, 0x88];
    const INIT_QP_M88_PPS: &[u8] = &[0x68, 0xCE, 0x00, 0x58, 0xE2];
    const INIT_QP_M89_PPS: &[u8] = &[0x68, 0xCE, 0x00, 0x59, 0xE2];
    // pic_init_qs_minus26 -27 (one below the flat -26 floor).
    const INIT_QS_M27_PPS: &[u8] = &[0x68, 0xCE, 0x20, 0xDE, 0x20];
    // num_ref_idx_l0_default_active_minus1 32.
    const REF_IDX_L0_32_PPS: &[u8] = &[0x68, 0xC8, 0x21, 0x8E, 0x20];
    // more_rbsp_data tails with transform_8x8_mode_flag and
    // pic_scaling_matrix_present_flag set and every list flag zero: eight
    // flag bits (the 4:2:0 / 4:2:2 count) vs twelve (the 4:4:4 count),
    // each followed by second_chroma_qp_index_offset se(0).
    const SCALING_TAIL_8_PPS: &[u8] = &[0x68, 0xCE, 0x38, 0xC0, 0x30];
    const SCALING_TAIL_12_PPS: &[u8] = &[0x68, 0xCE, 0x38, 0xC0, 0x03];
    // Multi-group slice_group_map fixtures, two slice groups each: type 0
    // (two run lengths), type 2 (ONE top_left/bottom_right pair — the
    // rectangle loop runs num_slice_groups_minus1 times), type 3 (change
    // direction + rate), and type 6 (four one-bit slice_group_id entries
    // — exactly the 4 map units of `MAP_UNITS_4_SPS`; against the
    // 300-map-unit skeleton the same table is the undersize reject, since
    // 7.4.2.2 fixes the type-6 table size to the picture).
    const FMO_TYPE0_PPS: &[u8] = &[0x68, 0xC5, 0xF1, 0xC4];
    const FMO_TYPE2_PPS: &[u8] = &[0x68, 0xC4, 0xFC, 0x71];
    const FMO_TYPE3_PPS: &[u8] = &[0x68, 0xC4, 0x47, 0x1C, 0x40];
    const FMO_TYPE6_PPS: &[u8] = &[0x68, 0xC4, 0x72, 0x2E, 0x38, 0x80];
    // num_slice_groups_minus1 8 (nine groups) and slice_group_map_type 7:
    // both shapes the spec does not define.
    const FMO_GROUPS_8_PPS: &[u8] = &[0x68, 0xC1, 0x30];
    const FMO_TYPE7_PPS: &[u8] = &[0x68, 0xC4, 0x22];
    // Slice-group shapes that overrun the 300 map units of the referenced
    // 320x240 SPS (20x15 map units): a type-0 run_length_minus1 of 300, a
    // type-2 bottom_right of 300, a type-3 change rate of 300, and a
    // type-6 table declaring 301 entries (the oversize arm of the exact
    // size rule; `FMO_TYPE6_PPS` against this SPS is the undersize arm).
    const FMO_TYPE0_RUN_300_PPS: &[u8] = &[0x68, 0xC5, 0x00, 0x96, 0xF1, 0xC4];
    const FMO_TYPE2_BR_300_PPS: &[u8] = &[0x68, 0xC4, 0xE0, 0x12, 0xDC, 0x71];
    const FMO_RATE_300_PPS: &[u8] = &[0x68, 0xC4, 0x40, 0x04, 0xB7, 0x1C, 0x40];
    const FMO_TYPE6_SIZE_300_PPS: &[u8] = &[0x68, 0xC4, 0x70, 0x09, 0x6B, 0x1C, 0x40];
    // Inverted type-2 rectangles: map units 40..20 invert the rows, and
    // 1..20 invert the columns (1 % 20 = 1 > 20 % 20 = 0) on the 20-wide
    // SPS.
    const FMO_TYPE2_INVERTED_PPS: &[u8] = &[0x68, 0xC4, 0xC1, 0x48, 0x57, 0x1C, 0x40];
    const FMO_TYPE2_COLUMN_PPS: &[u8] = &[0x68, 0xC4, 0xD0, 0x57, 0x1C, 0x40];
    // A changing map (type 3) over three slice groups: 7.4.2.2 defines the
    // changing maps for exactly two.
    const FMO_TYPE3_GROUPS_3_PPS: &[u8] = &[0x68, 0xC6, 0x47, 0x1C, 0x40];
    // Type 6 with three groups (two-bit ids) and a full four-entry table
    // for `MAP_UNITS_4_SPS` whose last entry codes group id 3, one past
    // num_slice_groups_minus1 2 — the size check passes so only the id
    // bound can reject it.
    const FMO_TYPE6_ID_3_PPS: &[u8] = &[0x68, 0xC6, 0x72, 0x0D, 0xE3, 0x88];
    // The real `PPS` with only its rbsp_trailing_bits stop bit cleared
    // (last byte 0x20 -> 0x00); every parsed field is untouched.
    const BAD_STOP_BIT_PPS: &[u8] = &[0x68, 0xCB, 0x83, 0xCB, 0x00];

    /// Joins NALs into an Annex-B configuration: a 4-byte start code, then
    /// 3-byte separators.
    fn annexb_concat(nals: &[&[u8]]) -> Vec<u8> {
        let mut v = Vec::new();
        for (i, nal) in nals.iter().enumerate() {
            v.extend_from_slice(if i == 0 { &[0, 0, 0, 1][..] } else { &[0, 0, 1][..] });
            v.extend_from_slice(nal);
        }
        v
    }

    fn annexb_with(sps: &[u8], pps: &[u8]) -> Vec<u8> {
        annexb_concat(&[sps, pps])
    }

    fn annexb_config() -> Vec<u8> {
        annexb_with(SPS, PPS)
    }

    /// SPS summaries for a PPS parse, in configuration order.
    fn sps_ctx(sps_list: &[&[u8]]) -> Vec<SpsSummary> {
        sps_list.iter().map(|s| parse_sps(s).unwrap()).collect()
    }

    #[test]
    fn builds_and_reparses_avcc() {
        let sets = parse_parameter_sets(&annexb_config()).unwrap().sets;
        assert_eq!(sets.sps, vec![SPS.to_vec()]);
        assert_eq!(sets.pps, vec![PPS.to_vec()]);
        let avcc = build_avcc(&sets).unwrap();
        assert_eq!(avcc[0], 1);
        assert_eq!(avcc[1], 66);
        assert_eq!(avcc[4] & 0x03, 3, "lengthSizeMinusOne must be 3");
        assert_eq!(avcc[5] & 0x1F, 1);
        let reparsed = parse_avcc_parameter_sets(&avcc).unwrap().sets;
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
        let sets = ParameterSets {
            sps: vec![HIGH_SPS.to_vec()],
            pps: vec![HIGH_PPS.to_vec()],
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
        let sets = parse_parameter_sets(&annexb_config()).unwrap().sets;
        let mut avcc = build_avcc(&sets).unwrap();
        avcc[4] = 0xFC | 1; // lengthSizeMinusOne = 1 (2-byte prefixes)
        assert!(parse_avcc_parameter_sets(&avcc).is_err());
    }

    #[test]
    fn fingerprint_is_wrapper_independent() {
        let from_annexb = parse_parameter_sets(&annexb_config()).unwrap();
        let avcc = build_avcc(&from_annexb.sets).unwrap();
        let from_avcc = parse_parameter_sets(&avcc).unwrap();
        assert_eq!(from_annexb.sets, from_avcc.sets);
        assert_eq!(from_annexb.fingerprint, from_avcc.fingerprint);
    }

    /// The S8 fingerprint keys parameter sets by identity: reordering
    /// DISTINCT ids or repeating a byte-identical set changes nothing,
    /// while reordering two same-id sets with different bytes swaps which
    /// one is active — the two orders are different configurations even
    /// though their byte SET is identical.
    #[test]
    fn fingerprint_keys_parameter_sets_by_identity() {
        let fp = |config: &[u8]| parse_parameter_sets(config).unwrap().fingerprint;
        // Distinct ids (0 and 31): order is not part of the identity map.
        let a = annexb_concat(&[SPS, SPS_ID_31, PPS]);
        let b = annexb_concat(&[SPS_ID_31, SPS, PPS]);
        assert_eq!(fp(&a), fp(&b), "distinct-id order must not matter");
        // A byte-identical resend collapses into its slot.
        let dup = annexb_concat(&[SPS, SPS, PPS, PPS]);
        let single = annexb_concat(&[SPS, PPS]);
        assert_eq!(fp(&dup), fp(&single), "identical resend is redundant");
        // Two id-0 SPS with different bytes: the last one is active, so
        // the two orders are DIFFERENT effective configurations.
        let ab = annexb_concat(&[SPS, SAME_PROJ_SPS, PPS]);
        let ba = annexb_concat(&[SAME_PROJ_SPS, SPS, PPS]);
        assert_ne!(fp(&ab), fp(&ba), "same-id reorder swaps the active SPS");
        // Same-id PPS pair: the same last-wins rule on the PPS side.
        let pab = annexb_concat(&[SPS, PPS, MINIMAL_PPS]);
        let pba = annexb_concat(&[SPS, MINIMAL_PPS, PPS]);
        assert_ne!(fp(&pab), fp(&pba), "same-id reorder swaps the active PPS");
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
        let avcc = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap().sets).unwrap();
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
    /// sample FFmpeg master's (n8.2+) `nal_parse_units` writes — trims
    /// applied, 4-byte length prefixes, no padding bytes carried into the
    /// payload. FFmpeg 7.1/8.1 instead length-prefix the NAL unchanged,
    /// carrying the padding into the sample; the divergence is fixture-only,
    /// since real encoders emit no trailing_zero_8bits.
    /// (Placed at the runtime layer because the integration harness cannot
    /// inject a synthetic AU through a real encoder; this IS the delivery
    /// code path.)
    #[test]
    fn a4_trailing_zero_fixture_matches_master_movenc_output() {
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
        // Golden bytes in the trimmed form: wb32(3) SEI, wb32(3) IDR — the
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
        let avcc = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap().sets).unwrap();
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
        let good = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap().sets).unwrap();
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

    fn high_sets() -> ParameterSets {
        ParameterSets {
            sps: vec![HIGH_SPS.to_vec()],
            pps: vec![HIGH_PPS.to_vec()],
        }
    }

    #[test]
    fn rejects_avcc_reserved_bits_cleared() {
        let good = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap().sets).unwrap();
        // Byte 4 keeps lengthSizeMinusOne = 3 but clears the six reserved
        // ones a conforming writer emits; masking would accept it.
        let mut bad = good.clone();
        bad[4] = 0x03;
        let err = parse_avcc_parameter_sets(&bad).unwrap_err();
        assert!(err.contains("byte 4 reserved"), "unexpected error: {err}");
        // Byte 5 keeps numOfSequenceParameterSets = 1 but clears the three
        // reserved ones.
        let mut bad = good.clone();
        bad[5] = 0x01;
        let err = parse_avcc_parameter_sets(&bad).unwrap_err();
        assert!(err.contains("byte 5 reserved"), "unexpected error: {err}");
        assert!(matches!(
            AvcRuntime::from_extradata(&bad, 2),
            Err(PacketSinkError::InvalidExtradata { stream_index: 2, .. })
        ));
    }

    #[test]
    fn rejects_parameter_set_with_forbidden_zero_bit() {
        // 0xE7 keeps nal_unit_type 7 (SPS) while setting forbidden_zero_bit;
        // a type-only mask would accept it. The full header must be checked.
        let mut sps = SPS.to_vec();
        sps[0] = 0xE7;
        let err = parse_avcc_parameter_sets(&raw_avcc(66, 0xC0, 0x1E, &sps, PPS)).unwrap_err();
        assert!(err.contains("forbidden_zero_bit"), "unexpected error: {err}");
        // The same header through the Annex-B configuration path.
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(&sps);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(PPS);
        let err = parse_parameter_sets(&config).unwrap_err();
        assert!(err.contains("forbidden_zero_bit"), "unexpected error: {err}");
        // A PPS entry with the bit set must fail identically.
        let mut pps = PPS.to_vec();
        pps[0] = 0xE8;
        let err = parse_avcc_parameter_sets(&raw_avcc(66, 0xC0, 0x1E, SPS, &pps)).unwrap_err();
        assert!(err.contains("forbidden_zero_bit"), "unexpected error: {err}");
    }

    #[test]
    fn accepts_high_profile_avcc_without_the_extension() {
        // Muxers predating the ISO/IEC 14496-15 profile extension end the
        // record at the PPS array even for High profile, and FFmpeg's own
        // reader (ff_h264_decode_extradata, libavcodec/h264_parse.c) never
        // requires the block — such a record must parse.
        let bare = raw_avcc(0x64, 0x00, 0x1E, HIGH_SPS, HIGH_PPS);
        let record = parse_avcc_record(&bare).unwrap();
        assert_eq!(record.extension, None);
        assert_eq!(record.sets, high_sets());
        assert!(AvcRuntime::from_extradata(&bare, 0).is_ok());
    }

    #[test]
    fn round_trips_the_high_profile_extension() {
        let avcc = build_avcc(&high_sets()).unwrap();
        let record = parse_avcc_record(&avcc).unwrap();
        assert_eq!(record.extension, Some((1, 8, 8)));
        assert_eq!(record.sets, high_sets());
        assert!(AvcRuntime::from_extradata(&avcc, 0).is_ok());
    }

    #[test]
    fn rejects_extension_reserved_bits_cleared() {
        let good = build_avcc(&high_sets()).unwrap();
        let ext = good.len() - 4;
        // Each write keeps the field value and clears only the reserved
        // ones: chroma byte 0xFD -> 0x01, bit-depth bytes 0xF8 -> 0x00.
        for (offset, cleared) in [(0usize, 0x01u8), (1, 0x00), (2, 0x00)] {
            let mut bad = good.clone();
            bad[ext + offset] = cleared;
            let err = parse_avcc_parameter_sets(&bad).unwrap_err();
            assert!(err.contains("reserved"), "unexpected error: {err}");
        }
    }

    #[test]
    fn rejects_partial_or_padded_extension() {
        let good = build_avcc(&high_sets()).unwrap();
        // A cut inside the extension leaves the chroma byte without its bit
        // depths: the block must be complete or absent, never partial.
        let err = parse_avcc_parameter_sets(&good[..good.len() - 2]).unwrap_err();
        assert!(err.contains("extension truncated"), "unexpected error: {err}");
        // A byte beyond the complete extension is foreign data.
        let mut padded = good.clone();
        padded.push(0);
        let err = parse_avcc_parameter_sets(&padded).unwrap_err();
        assert!(
            err.contains("after the profile extension"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_trailing_bytes_on_baseline_profile() {
        // No extension is defined for Baseline/Main/Extended
        // (ff_isom_write_avcc appends it only for other profiles), so any
        // trailing byte on a profile-66 record is foreign data.
        let mut avcc = build_avcc(&parse_parameter_sets(&annexb_config()).unwrap().sets).unwrap();
        avcc.push(0);
        let err = parse_avcc_parameter_sets(&avcc).unwrap_err();
        assert!(err.contains("no extension is defined"), "unexpected error: {err}");
    }

    #[test]
    fn validates_sequence_parameter_set_ext_entries() {
        // numOfSequenceParameterSetExt = 1 with one SPS-EXT NAL (type 13,
        // header 0x6D): the entry parses and ends the record.
        let mut avcc = build_avcc(&high_sets()).unwrap();
        let count = avcc.len() - 1;
        avcc[count] = 1;
        avcc.extend_from_slice(&[0, 2, 0x6D, 0x40]);
        let record = parse_avcc_record(&avcc).unwrap();
        assert_eq!(record.extension, Some((1, 8, 8)));
        // The array holds SPS-EXT NALs only: an SPS header there is wrong.
        let header = avcc.len() - 2;
        avcc[header] = 0x67;
        let err = parse_avcc_record(&avcc).unwrap_err();
        assert!(err.contains("SPS-EXT"), "unexpected error: {err}");
        // forbidden_zero_bit applies to SPS-EXT entries too.
        avcc[header] = 0xED;
        let err = parse_avcc_record(&avcc).unwrap_err();
        assert!(err.contains("forbidden_zero_bit"), "unexpected error: {err}");
        // A declared entry with no bytes must fail, not read past the end.
        let mut short = build_avcc(&high_sets()).unwrap();
        let count = short.len() - 1;
        short[count] = 1;
        let err = parse_avcc_parameter_sets(&short).unwrap_err();
        assert!(err.contains("SPS-EXT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_extension_disagreeing_with_the_sps() {
        // bit_depth_luma_minus8 = 2 in the record while the SPS codes 0:
        // the extension fields derive from the first SPS
        // (ff_isom_write_avcc), so a disagreement is two conflicting
        // stream descriptions.
        let mut avcc = build_avcc(&high_sets()).unwrap();
        let ext = avcc.len() - 4;
        avcc[ext + 1] = 0xF8 | 2;
        let err = parse_avcc_parameter_sets(&avcc).unwrap_err();
        assert!(err.contains("bit depths"), "unexpected error: {err}");
        assert!(matches!(
            AvcRuntime::from_extradata(&avcc, 6),
            Err(PacketSinkError::InvalidExtradata { stream_index: 6, .. })
        ));
    }

    #[test]
    fn s8_rejects_tampered_avcc_header_with_unchanged_sets() {
        let (runtime, delivered, _) = AvcRuntime::from_extradata(&annexb_config(), 0).unwrap();
        // The identical record is redundant and passes.
        runtime.check_new_extradata(&delivered, 0).unwrap();
        // Each header byte flipped with SPS/PPS untouched: the announcement
        // contradicts its own SPS and must fail S8 instead of slipping
        // through the set-identity comparison.
        for byte in 1..4 {
            let mut tampered = delivered.clone();
            tampered[byte] ^= 0x01;
            assert!(
                matches!(
                    runtime.check_new_extradata(&tampered, 7),
                    Err(PacketSinkError::ConfigChange { stream_index: 7, .. })
                ),
                "tampered avcC header byte {byte} must be rejected"
            );
        }
    }

    #[test]
    fn s8_rejects_tampered_extension_fields_with_unchanged_sets() {
        let config = build_avcc(&high_sets()).unwrap();
        let (runtime, delivered, _) = AvcRuntime::from_extradata(&config, 0).unwrap();
        runtime.check_new_extradata(&delivered, 0).unwrap();
        // chroma_format_idc 2 in the announcement while the unchanged SPS
        // codes 1: rejected through the same header consistency check.
        let mut tampered = delivered;
        let ext = tampered.len() - 4;
        tampered[ext] = 0xFC | 2;
        assert!(matches!(
            runtime.check_new_extradata(&tampered, 4),
            Err(PacketSinkError::ConfigChange { stream_index: 4, .. })
        ));
    }

    /// The delivered profile-extension triple derives from the FIRST SPS
    /// (`ff_isom_write_avcc`, libavformat/avc.c), so a permutation that
    /// changes which SPS comes first changes what consumers are told the
    /// stream's chroma format and bit depths are. TEN_BIT_SPS and
    /// CHROMA3_SCALING_SPS share id 0 and all three projection bytes
    /// (0x6E 0x00 0x1E) while differing in chroma_info, and duplicating
    /// the 4:4:4 SPS keeps the final id map AND the PPS binding identical
    /// across the permutation — the extension slot is the only
    /// fingerprint entry that can tell the two configurations apart.
    #[test]
    fn s8_sees_the_derived_extension_triple() {
        assert_eq!(parse_sps(TEN_BIT_SPS).unwrap().sps_id, 0);
        assert_eq!(parse_sps(CHROMA3_SCALING_SPS).unwrap().sps_id, 0);
        assert_eq!(TEN_BIT_SPS[1..4], CHROMA3_SCALING_SPS[1..4]);
        assert_eq!(parse_sps(TEN_BIT_SPS).unwrap().chroma_info(), (1, 10, 10));
        assert_eq!(parse_sps(CHROMA3_SCALING_SPS).unwrap().chroma_info(), (3, 8, 8));
        let ten_first = annexb_concat(&[
            TEN_BIT_SPS,
            CHROMA3_SCALING_SPS,
            CHROMA3_SCALING_SPS,
            MINIMAL_PPS,
        ]);
        let chroma3_first = annexb_concat(&[
            CHROMA3_SCALING_SPS,
            TEN_BIT_SPS,
            CHROMA3_SCALING_SPS,
            MINIMAL_PPS,
        ]);
        // Everything but the derived triple is identical: same last-wins
        // id maps, same bound generation, no SPS-EXT.
        let a = parse_parameter_sets(&ten_first).unwrap().fingerprint;
        let b = parse_parameter_sets(&chroma3_first).unwrap().fingerprint;
        assert_eq!(a.sps, b.sps);
        assert_eq!(a.pps, b.pps);
        assert_eq!(a.sps_ext, b.sps_ext);
        assert_ne!(a.extension, b.extension, "the first SPS decides the triple");
        // Both orders are faithful (each PPS behind its bound SPS, one
        // binding on both reads): construction accepts them, and each
        // synthesized record carries its own first SPS's triple.
        let (runtime, delivered, _) = AvcRuntime::from_extradata(&ten_first, 0).unwrap();
        assert_eq!(&delivered[delivered.len() - 4..], &[0xFD, 0xFA, 0xFA, 0x00]);
        let (_, delivered, _) = AvcRuntime::from_extradata(&chroma3_first, 0).unwrap();
        assert_eq!(&delivered[delivered.len() - 4..], &[0xFF, 0xF8, 0xF8, 0x00]);
        // The identical announcement stays redundant; the permutation
        // flips the delivered triple and is a configuration change.
        runtime.check_new_extradata(&ten_first, 0).unwrap();
        assert!(matches!(
            runtime.check_new_extradata(&chroma3_first, 5),
            Err(PacketSinkError::ConfigChange { stream_index: 5, .. })
        ));
    }

    /// S8 over a same-id reorder: both SPS carry seq_parameter_set_id 0
    /// and identical projection bytes, so neither the projection gate nor
    /// a byte-set view can tell the orders apart — but swapping them swaps
    /// which SPS a decoder holds for id 0, so the announcement must fail.
    /// Distinct-id reorders (ids 0 and 31) leave the identity map
    /// untouched and stay redundant.
    #[test]
    fn s8_distinguishes_same_id_reorder_from_distinct_id_reorder() {
        let config = annexb_concat(&[SPS, SAME_PROJ_SPS, PPS]);
        let (runtime, _, _) = AvcRuntime::from_extradata(&config, 0).unwrap();
        // The identical announcement is redundant and passes.
        runtime.check_new_extradata(&config, 0).unwrap();
        let swapped = annexb_concat(&[SAME_PROJ_SPS, SPS, PPS]);
        assert!(matches!(
            runtime.check_new_extradata(&swapped, 5),
            Err(PacketSinkError::ConfigChange { stream_index: 5, .. })
        ));

        let config = annexb_concat(&[SPS, SPS_ID_31, PPS]);
        let (runtime, _, _) = AvcRuntime::from_extradata(&config, 0).unwrap();
        let swapped = annexb_concat(&[SPS_ID_31, SPS, PPS]);
        runtime.check_new_extradata(&swapped, 0).unwrap();
    }

    /// The fingerprint records which SPS generation each PPS bound, not
    /// just the final id map: [A, P] and [B, P, A] both end with active
    /// sps = A and pps = P — equal id maps — but the second ordering's
    /// PPS resolved B when it arrived, and a decoder keeps that binding
    /// (the `PPS::sps` reference ff_h264_decode_picture_parameter_set
    /// takes, libavcodec/h264_ps.c) after A replaces the list slot. A and
    /// B share seq_parameter_set_id AND projection bytes, so only the
    /// binding entry can tell the two configurations apart.
    #[test]
    fn s8_sees_the_sps_generation_each_pps_bound() {
        // Scenario preconditions, pinned byte-by-byte: same id, same
        // projection bytes, different body.
        assert_eq!(parse_sps(SPS).unwrap().sps_id, 0);
        assert_eq!(parse_sps(SAME_PROJ_SPS).unwrap().sps_id, 0);
        assert_eq!(SPS[1..4], SAME_PROJ_SPS[1..4]);
        assert_ne!(SPS[4..], SAME_PROJ_SPS[4..]);
        let baseline = annexb_with(SPS, PPS);
        let announced = annexb_concat(&[SAME_PROJ_SPS, PPS, SPS]);
        let fp = |config: &[u8]| parse_parameter_sets(config).unwrap().fingerprint;
        assert_ne!(
            fp(&baseline),
            fp(&announced),
            "the PPS binding must be part of the fingerprint"
        );
        let (runtime, _, _) = AvcRuntime::from_extradata(&baseline, 0).unwrap();
        runtime.check_new_extradata(&baseline, 0).unwrap();
        assert!(matches!(
            runtime.check_new_extradata(&announced, 5),
            Err(PacketSinkError::ConfigChange { stream_index: 5, .. })
        ));
        // [A, B, P] vs [A, P, B]: SAME SPS list and SAME final map either
        // way (B is the last id-0 SPS in both), so a fingerprint that
        // recorded every seen SPS payload — ordered or not — could not
        // tell them apart; only the generation P bound (B vs A) differs.
        let bound_b = annexb_concat(&[SPS, SAME_PROJ_SPS, PPS]);
        let bound_a = annexb_concat(&[SPS, PPS, SAME_PROJ_SPS]);
        assert_ne!(
            fp(&bound_b),
            fp(&bound_a),
            "equal SPS lists with a different binding must not collapse"
        );
        let (runtime, _, _) = AvcRuntime::from_extradata(&bound_b, 0).unwrap();
        runtime.check_new_extradata(&bound_b, 0).unwrap();
        assert!(matches!(
            runtime.check_new_extradata(&bound_a, 5),
            Err(PacketSinkError::ConfigChange { stream_index: 5, .. })
        ));
        // Two-id case: the PPS names id 0, so it binds across the nearer
        // id-31 SPS. The slot must record the id-RESOLVED generation —
        // whichever configuration position it sits at — never simply the
        // nearest, first or last SPS of the list.
        for config in [
            annexb_concat(&[SPS, SPS_ID_31, PPS]),
            annexb_concat(&[SPS_ID_31, SPS, PPS]),
        ] {
            let prints = fp(&config);
            let (_, (_, bound)) = &prints.pps[0];
            assert_eq!(
                bound.as_slice(),
                &SPS[1..],
                "the PPS slot must record the id-0 generation it bound"
            );
        }
    }

    /// The one-byte NAL header is framing, not configuration: the reader
    /// consumes it before the body is parsed and stored
    /// (h264_parse_nal_header, libavcodec/h2645_parse.c), so a legal
    /// nal_ref_idc change over an identical payload (0x67 -> 0x47, 0x68 ->
    /// 0x48) leaves decoder state untouched. Header-only resends and
    /// reorders must stay redundant — in the SPS map, the PPS map and the
    /// in-band comparison alike — while synthesis keeps delivering the
    /// original header bytes.
    #[test]
    fn header_only_nal_ref_idc_changes_stay_redundant() {
        let mut sps_idc2 = SPS.to_vec();
        sps_idc2[0] = 0x47; // nal_ref_idc 3 -> 2; forbidden bit 0, type 7
        let mut pps_idc2 = PPS.to_vec();
        pps_idc2[0] = 0x48; // nal_ref_idc 3 -> 2; forbidden bit 0, type 8
        let (runtime, delivered, _) = AvcRuntime::from_extradata(&annexb_config(), 0).unwrap();
        // The delivered record still carries the original full NAL bytes
        // (6 header bytes + the 2-byte length ahead of the first SPS).
        assert_eq!(&delivered[8..8 + SPS.len()], SPS);
        runtime
            .check_new_extradata(&annexb_with(&sps_idc2, PPS), 0)
            .unwrap();
        runtime
            .check_new_extradata(&annexb_with(SPS, &pps_idc2), 0)
            .unwrap();
        // In-band, the header-only variant is value-equal to the active
        // set: it falls through to the strict-tier in-band rejection
        // instead of reporting a configuration change.
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(&sps_idc2);
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x80]);
        let mut scratch = Vec::new();
        assert!(matches!(
            runtime.normalize_au(&au, &mut scratch, 1),
            Err(PacketSinkError::InBandParameterSets { stream_index: 1 })
        ));
        // The PPS side of the same rule: the header-only PPS variant is
        // value-equal in the PPS map too, so it must classify as in-band
        // parameter sets, not as a configuration change.
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(&pps_idc2);
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x80]);
        assert!(matches!(
            runtime.normalize_au(&au, &mut scratch, 1),
            Err(PacketSinkError::InBandParameterSets { stream_index: 1 })
        ));
        // Two same-id sets differing only in the header are ONE
        // generation: reordering them swaps nothing a decoder stores.
        let config = annexb_concat(&[SPS, &sps_idc2, PPS]);
        let (runtime, _, _) = AvcRuntime::from_extradata(&config, 0).unwrap();
        runtime
            .check_new_extradata(&annexb_concat(&[&sps_idc2, SPS, PPS]), 0)
            .unwrap();
    }

    /// A PPS ahead of every SPS carrying its id cannot bind: activation
    /// cannot reach forward, and a sequential read (decode_extradata_ps,
    /// libavcodec/h264_parse.c) fails the dangling seq_parameter_set_id
    /// inside ff_h264_decode_picture_parameter_set. The avcC form needs no
    /// counterpart — its arrays put every SPS ahead of every PPS by
    /// construction.
    #[test]
    fn rejects_annexb_pps_ahead_of_its_sps() {
        let config = annexb_concat(&[PPS, SPS]);
        let err = parse_parameter_sets(&config).unwrap_err();
        assert!(err.contains("no preceding SPS"), "unexpected error: {err}");
        assert!(matches!(
            AvcRuntime::from_extradata(&config, 2),
            Err(PacketSinkError::InvalidExtradata { stream_index: 2, .. })
        ));
    }

    /// A PPS binds the last same-id SPS seen SO FAR, never a later one:
    /// the scaling-tail PPS parses only against the chroma-1 SPS (8 list
    /// flags), so [chroma-1 SPS, PPS, chroma-3 SPS] must parse — binding
    /// forward to the chroma-3 SPS would misalign the tail — while the
    /// flipped order makes the chroma-3 set the visible context and must
    /// fail.
    #[test]
    fn pps_binds_the_preceding_sps_not_a_later_one() {
        let forward = annexb_concat(&[HIGH_SPS, SCALING_TAIL_8_PPS, CHROMA3_SCALING_SPS]);
        parse_parameter_sets(&forward).unwrap();
        let flipped = annexb_concat(&[CHROMA3_SCALING_SPS, SCALING_TAIL_8_PPS, HIGH_SPS]);
        let err = parse_parameter_sets(&flipped).unwrap_err();
        assert!(err.starts_with("PPS:"), "unexpected error: {err}");
    }

    /// Annex-B binds each PPS to the last PRECEDING same-id SPS; an avcC
    /// record's arrays bind each PPS to the last same-id SPS OVERALL. An
    /// ordering that re-sends a PPS's SPS generation after the PPS is
    /// therefore sequentially valid yet has no faithful record form, and
    /// runtime construction must reject it instead of delivering a record
    /// that means something else:
    /// * [HIGH, PPS-under-HIGH, CHROMA3]: the synthesized record rebinds
    ///   the PPS to the 4:4:4 SPS, under which its scaling tail no longer
    ///   parses at all;
    /// * [A, P, B] (A and B same-id, P parses under both): the record
    ///   reparses cleanly but binds P to B where the stream bound A.
    /// Orderings with each PPS after its SPS generation — every real
    /// encoder handoff — round-trip and pass unchanged.
    #[test]
    fn from_extradata_rejects_interleavings_with_no_faithful_avcc() {
        let reason = |config: &[u8]| match AvcRuntime::from_extradata(config, 4) {
            Ok(_) => panic!("an unrepresentable interleaving must be rejected"),
            Err(PacketSinkError::InvalidExtradata {
                stream_index: 4,
                reason,
            }) => reason,
            Err(other) => panic!("unexpected error: {other}"),
        };
        let rebind_breaks = annexb_concat(&[HIGH_SPS, SCALING_TAIL_8_PPS, CHROMA3_SCALING_SPS]);
        parse_parameter_sets(&rebind_breaks).unwrap(); // sequentially valid
        let r = reason(&rebind_breaks);
        assert!(
            r.contains("no faithful avcC representation"),
            "unexpected reason: {r}"
        );
        let rebind_shifts = annexb_concat(&[SPS, PPS, SAME_PROJ_SPS]);
        parse_parameter_sets(&rebind_shifts).unwrap(); // sequentially valid
        let r = reason(&rebind_shifts);
        assert!(
            r.contains("no faithful avcC representation"),
            "unexpected reason: {r}"
        );
        // The real-encoder fixtures keep passing: one live generation per
        // id, each PPS behind its SPS.
        assert!(AvcRuntime::from_extradata(&annexb_config(), 0).is_ok());
        assert!(AvcRuntime::from_extradata(&annexb_with(HIGH_SPS, HIGH_PPS), 0).is_ok());
    }

    /// SPS-EXT entries are part of the delivered configuration: an
    /// announcement that edits, drops or adds one changes what consumers
    /// hold, so S8 must see them; the byte-identical announcement stays
    /// redundant.
    #[test]
    fn s8_sees_sps_ext_entries() {
        let mut with_ext = build_avcc(&high_sets()).unwrap();
        let count = with_ext.len() - 1;
        with_ext[count] = 1;
        with_ext.extend_from_slice(&[0, 2, 0x6D, 0x40]);
        let (runtime, delivered, _) = AvcRuntime::from_extradata(&with_ext, 0).unwrap();
        runtime.check_new_extradata(&delivered, 0).unwrap();
        // Mutated SPS-EXT body: same SPS/PPS, different configuration.
        let mut mutated = with_ext.clone();
        let last = mutated.len() - 1;
        mutated[last] = 0x41;
        assert!(matches!(
            runtime.check_new_extradata(&mutated, 3),
            Err(PacketSinkError::ConfigChange { stream_index: 3, .. })
        ));
        // Dropped SPS-EXT array: ditto.
        let without = build_avcc(&high_sets()).unwrap();
        assert!(matches!(
            runtime.check_new_extradata(&without, 3),
            Err(PacketSinkError::ConfigChange { stream_index: 3, .. })
        ));
        // And the mirror: a baseline without SPS-EXT rejects an
        // announcement that adds one.
        let (runtime, _, _) = AvcRuntime::from_extradata(&without, 0).unwrap();
        assert!(matches!(
            runtime.check_new_extradata(&with_ext, 3),
            Err(PacketSinkError::ConfigChange { stream_index: 3, .. })
        ));
    }

    /// The SPS-EXT NAL header is framing like every other parameter
    /// set's: this crate keys the entry by its post-header payload, the
    /// same identity policy the SPS/PPS maps use. (The decoder's own
    /// extradata readers give nothing to mirror — `ff_h264_decode_extradata`
    /// stops after the PPS array and `decode_extradata_ps` ignores type
    /// 13 — and CBS's `ff_cbs_read_extradata` parses SPS-EXT as a full
    /// syntax tree, not a stored-bytes identity.) An entry re-sent with a
    /// different legal `nal_ref_idc` (0x6D -> 0x4D, both type 13) lands
    /// in identical stored state and must stay redundant, while a payload
    /// difference behind the same header is still a configuration change.
    #[test]
    fn s8_keys_sps_ext_by_post_header_payload() {
        let with_ext = |header: u8, body: u8| {
            let mut avcc = build_avcc(&high_sets()).unwrap();
            let count = avcc.len() - 1;
            avcc[count] = 1;
            avcc.extend_from_slice(&[0, 2, header, body]);
            avcc
        };
        let (runtime, delivered, _) =
            AvcRuntime::from_extradata(&with_ext(0x6D, 0x40), 0).unwrap();
        runtime.check_new_extradata(&delivered, 0).unwrap();
        // nal_ref_idc 3 -> 2 over an identical payload: identity-equal.
        runtime.check_new_extradata(&with_ext(0x4D, 0x40), 0).unwrap();
        // Identical header, different payload: a configuration change.
        assert!(matches!(
            runtime.check_new_extradata(&with_ext(0x6D, 0x41), 3),
            Err(PacketSinkError::ConfigChange { stream_index: 3, .. })
        ));
    }

    /// In-band comparison runs against the ACTIVE sets: an id-0 SPS the
    /// configuration later replaced is no longer part of the stream
    /// configuration, so carrying it in-band is a configuration change,
    /// while the ACTIVE id-0 SPS is value-equal and falls through to the
    /// strict-tier in-band rejection.
    #[test]
    fn inband_replaced_predecessor_is_a_config_change() {
        let config = annexb_concat(&[SPS, SAME_PROJ_SPS, PPS]);
        let (runtime, _, _) = AvcRuntime::from_extradata(&config, 0).unwrap();
        let mut scratch = Vec::new();
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(SPS); // the REPLACED id-0 SPS
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x80]);
        assert!(matches!(
            runtime.normalize_au(&au, &mut scratch, 1),
            Err(PacketSinkError::ConfigChange { stream_index: 1, .. })
        ));
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(SAME_PROJ_SPS); // the ACTIVE id-0 SPS
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x80]);
        assert!(matches!(
            runtime.normalize_au(&au, &mut scratch, 1),
            Err(PacketSinkError::InBandParameterSets { stream_index: 1 })
        ));
    }

    /// reserved_zero_2bits after the constraint flags must be zero (H.264
    /// 7.4.2.1.1). SPS byte 2 is exactly the flags-plus-reserved byte, so
    /// setting its low bit perturbs nothing downstream — only the new
    /// check can catch it.
    #[test]
    fn rejects_nonzero_reserved_zero_2bits() {
        let mut sps = SPS.to_vec();
        sps[2] |= 0x01;
        let err = parse_sps(&sps).unwrap_err();
        assert!(err.contains("reserved_zero_2bits"), "unexpected error: {err}");
        let config = annexb_with(&sps, PPS);
        assert!(matches!(
            AvcRuntime::from_extradata(&config, 6),
            Err(PacketSinkError::InvalidExtradata { stream_index: 6, .. })
        ));
    }

    /// An SPS cut right after the chroma / bit-depth block must fail on
    /// BOTH wrapper paths: the body parses through rbsp_trailing_bits
    /// wherever it arrives, not just far enough to fill the avcC extension
    /// fields.
    #[test]
    fn rejects_sps_truncated_after_the_chroma_block_on_both_paths() {
        let cut = &HIGH_SPS[..5]; // ends with the bit-depth fields
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(cut);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(HIGH_PPS);
        let err = parse_parameter_sets(&config).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
        let avcc = raw_avcc(0x64, 0x00, 0x1E, cut, HIGH_PPS);
        let err = parse_avcc_parameter_sets(&avcc).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
    }

    /// A Baseline SPS cut inside pic_width_in_mbs_minus1: non-high profiles
    /// have no chroma block, so their first parsed field past
    /// seq_parameter_set_id is the frame_num / dimension chain.
    #[test]
    fn rejects_sps_truncated_mid_dimensions() {
        let cut = &SPS[..6];
        let err = parse_sps(cut).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(cut);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(PPS);
        let err = parse_parameter_sets(&config).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
    }

    /// seq_parameter_set_id is bounded to 31 (H.264 7.4.2.1.1; h264_ps.c
    /// checks against MAX_SPS_COUNT = 32). The fixtures differ in one bit:
    /// ue(31) parses, ue(32) is rejected by value, not by shape.
    #[test]
    fn rejects_seq_parameter_set_id_above_31() {
        assert_eq!(parse_sps(SPS_ID_31).unwrap().chroma_info(), (1, 8, 8));
        assert_eq!(parse_sps(SPS_ID_31).unwrap().sps_id, 31);
        let err = parse_sps(SPS_ID_32).unwrap_err();
        assert!(err.contains("seq_parameter_set_id 32"), "unexpected error: {err}");
    }

    /// Field bounds in the frame_num / POC chain, each pinned at its
    /// boundary: log2_max_frame_num_minus4 <= 12, pic_order_cnt_type <= 2,
    /// num_ref_frames_in_pic_order_cnt_cycle < 256 (all as in h264_ps.c).
    #[test]
    fn rejects_out_of_range_frame_num_and_poc_fields() {
        assert_eq!(parse_sps(LOG2_FRAME_NUM_12).unwrap().chroma_info(), (1, 8, 8));
        let err = parse_sps(LOG2_FRAME_NUM_13).unwrap_err();
        assert!(
            err.contains("log2_max_frame_num_minus4 13"),
            "unexpected error: {err}"
        );
        let err = parse_sps(POC_TYPE_3).unwrap_err();
        assert!(err.contains("pic_order_cnt_type 3"), "unexpected error: {err}");
        assert_eq!(parse_sps(POC_CYCLE_255).unwrap().chroma_info(), (1, 8, 8));
        let err = parse_sps(POC_CYCLE_256).unwrap_err();
        assert!(
            err.contains("num_ref_frames_in_pic_order_cnt_cycle 256"),
            "unexpected error: {err}"
        );
    }

    /// Declared-present tail structures must be complete. h264_ps.c only
    /// warns when VUI data runs out mid-structure (a decoder can still play
    /// the stream); a validator has nothing to fall back on and rejects.
    #[test]
    fn rejects_declared_but_truncated_tail_structures() {
        // frame_cropping_flag with incomplete crop offsets.
        let err = parse_sps(MISSING_CROP_SPS).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
        // vui_parameters_present_flag with the VUI cut a few flags in.
        let err = parse_sps(VUI_TRUNCATED_MIN_SPS).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
        // The real High fixture cut inside its VUI timing_info block.
        let err = parse_sps(&HIGH_SPS[..12]).unwrap_err();
        assert!(err.contains("truncated"), "unexpected error: {err}");
    }

    /// A single flipped stop bit leaves every parsed field intact, so only
    /// the rbsp_trailing_bits check can catch it — including through the
    /// full runtime construction path.
    #[test]
    fn rejects_corrupt_rbsp_trailing_bits() {
        let err = parse_sps(BAD_STOP_BIT_SPS).unwrap_err();
        assert!(err.contains("stop bit"), "unexpected error: {err}");
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(BAD_STOP_BIT_SPS);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(HIGH_PPS);
        assert!(matches!(
            AvcRuntime::from_extradata(&config, 9),
            Err(PacketSinkError::InvalidExtradata { stream_index: 9, .. })
        ));
    }

    /// seq_scaling_matrix coverage: all list flags zero, a full sixteen-code
    /// list, and a 4:4:4 twelve-list SPS whose present list stops early on
    /// nextScale == 0. The spliced fixture also synthesizes a working avcC.
    #[test]
    fn accepts_scaling_matrix_sps_variants() {
        assert_eq!(parse_sps(HIGH_SPS_SCALING).unwrap().chroma_info(), (1, 8, 8));
        assert_eq!(parse_sps(SCALING_LIST_SPS).unwrap().chroma_info(), (1, 8, 8));
        assert_eq!(parse_sps(CHROMA3_SCALING_SPS).unwrap().chroma_info(), (3, 8, 8));
        let sets = ParameterSets {
            sps: vec![HIGH_SPS_SCALING.to_vec()],
            pps: vec![HIGH_PPS.to_vec()],
        };
        let avcc = build_avcc(&sets).unwrap();
        assert!(AvcRuntime::from_extradata(&avcc, 0).is_ok());
        let tail = &avcc[avcc.len() - 4..];
        assert_eq!(tail[..3], [0xFC | 1, 0xF8, 0xF8]);
    }

    /// profile_idc 144 carries the chroma block (h264_ps.c includes 144 in
    /// its dispatch). Read without the block, this fixture's tail ends
    /// misaligned and fails rbsp_trailing_bits — acceptance pins the
    /// dispatch set.
    #[test]
    fn accepts_profile_144_with_the_chroma_block() {
        assert_eq!(parse_sps(PROFILE_144_SPS).unwrap().chroma_info(), (1, 8, 8));
        let sets = ParameterSets {
            sps: vec![PROFILE_144_SPS.to_vec()],
            pps: vec![HIGH_PPS.to_vec()],
        };
        let avcc = build_avcc(&sets).unwrap();
        assert!(AvcRuntime::from_extradata(&avcc, 0).is_ok());
    }

    /// profile_idc 144 and 135 sit in the SYNTAX dispatch — the chroma
    /// block is present in the bits and must be parsed past — but not in
    /// the avcC writer's: `ff_avc_decode_sps` (libavformat/avc.c) lists
    /// 100/110/122/244/44/83/86/118/128/138/139/134 and defaults every
    /// other profile to (1, 8, 8), so the record FFmpeg SYNTHESIZES from
    /// Annex-B for these streams carries the default triple no matter
    /// what the SPS codes (a remux can still carry a syntax-shaped record
    /// verbatim — the parse side accepts both). Synthesis and the
    /// canonical identity must be the writer's: deriving (3, 8, 8) here
    /// would emit a tail FFmpeg's own synthesis never writes for the same
    /// stream.
    #[test]
    fn synthesizes_the_writer_default_triple_outside_the_writer_dispatch() {
        // The syntax parse still sees the coded values...
        assert_eq!(
            parse_sps(PROFILE_144_CHROMA3_SPS).unwrap().chroma_info(),
            (3, 8, 8)
        );
        assert_eq!(
            parse_sps(PROFILE_135_TEN_BIT_SPS).unwrap().chroma_info(),
            (1, 10, 10)
        );
        // ...while the delivered triple is the writer's default, the
        // synthesized tail is the default block, and the record
        // round-trips through the synthesis-fidelity gate.
        for sps in [PROFILE_144_CHROMA3_SPS, PROFILE_135_TEN_BIT_SPS] {
            assert_eq!(
                derived_extension(sps, &parse_sps(sps).unwrap()),
                Some((1, 8, 8))
            );
            let sets = ParameterSets {
                sps: vec![sps.to_vec()],
                pps: vec![MINIMAL_PPS.to_vec()],
            };
            let avcc = build_avcc(&sets).unwrap();
            assert_eq!(avcc[avcc.len() - 4..], [0xFD, 0xF8, 0xF8, 0x00]);
            let (_, delivered, _) =
                AvcRuntime::from_extradata(&annexb_with(sps, MINIMAL_PPS), 0).unwrap();
            assert_eq!(delivered, avcc);
        }
    }

    /// An avcC shaped like `ff_isom_write_avcc`'s own SYNTHESIS output for
    /// the 4:4:4-syntax profile-144 stream — extension (1, 8, 8) — must be
    /// accepted, initially and as an announcement over the equivalent
    /// Annex-B baseline. So must one carrying the raw SPS-coded values:
    /// the same function writes a non-Annex-B extradata VERBATIM
    /// (`mov_write_avcc_tag` hands the track extradata straight through),
    /// so an FFmpeg remux preserves a syntax-derived tail unchanged. Both
    /// shapes describe one stream and canonicalize to one identity; a
    /// triple matching neither derivation is rejected.
    #[test]
    fn accepts_ffmpeg_shaped_records_outside_the_writer_dispatch() {
        let mut avcc = raw_avcc(144, 0x00, 0x1E, PROFILE_144_CHROMA3_SPS, MINIMAL_PPS);
        avcc.extend_from_slice(&[0xFD, 0xF8, 0xF8, 0x00]);
        let record = parse_avcc_record(&avcc).unwrap();
        assert_eq!(record.extension, Some((1, 8, 8)));
        assert!(AvcRuntime::from_extradata(&avcc, 0).is_ok());
        let annexb = annexb_with(PROFILE_144_CHROMA3_SPS, MINIMAL_PPS);
        let (runtime, _, _) = AvcRuntime::from_extradata(&annexb, 0).unwrap();
        runtime.check_new_extradata(&avcc, 0).unwrap();
        // The verbatim-copy shape: chroma_format_idc 3 — the SPS-coded
        // value — in the extension. Same stream, same canonical identity:
        // accepted initially, over the Annex-B baseline, and over the
        // synthesis-shaped record.
        let ext = avcc.len() - 4;
        let mut passthrough = avcc.clone();
        passthrough[ext] = 0xFC | 3;
        let passthrough_record = parse_avcc_record(&passthrough).unwrap();
        assert_eq!(passthrough_record.extension, Some((3, 8, 8)));
        assert_eq!(passthrough_record.fingerprint, record.fingerprint);
        assert!(AvcRuntime::from_extradata(&passthrough, 0).is_ok());
        runtime.check_new_extradata(&passthrough, 0).unwrap();
        let (from_synthesis, _, _) = AvcRuntime::from_extradata(&avcc, 0).unwrap();
        from_synthesis.check_new_extradata(&passthrough, 0).unwrap();
        // Depth-differing passthrough, profile 135: writer default
        // (1, 8, 8) vs SPS-coded (1, 10, 10) — both shapes, one identity.
        let mut avcc135 = raw_avcc(135, 0x00, 0x1E, PROFILE_135_TEN_BIT_SPS, MINIMAL_PPS);
        avcc135.extend_from_slice(&[0xFD, 0xF8, 0xF8, 0x00]);
        let writer_shaped = parse_avcc_record(&avcc135).unwrap();
        let ext135 = avcc135.len() - 4;
        let mut passthrough135 = avcc135.clone();
        passthrough135[ext135 + 1] = 0xF8 | 2;
        passthrough135[ext135 + 2] = 0xF8 | 2;
        let syntax_shaped = parse_avcc_record(&passthrough135).unwrap();
        assert_eq!(syntax_shaped.extension, Some((1, 10, 10)));
        assert_eq!(syntax_shaped.fingerprint, writer_shaped.fingerprint);
        // A triple matching NEITHER derivation (chroma_format_idc 2) is
        // still two disagreeing stream descriptions.
        let mut neither = avcc.clone();
        neither[ext] = 0xFC | 2;
        let err = parse_avcc_parameter_sets(&neither).unwrap_err();
        assert!(
            err.contains("chroma_format_idc 2")
                && err.contains("(writer default)")
                && err.contains("(SPS syntax)"),
            "unexpected error: {err}"
        );
    }

    /// The encoder-produced PPS bodies parse to their trailing bits against
    /// their own SPS: the Constrained Baseline one (CAVLC, no tail) and the
    /// High one, whose more_rbsp_data tail carries transform_8x8_mode_flag
    /// and second_chroma_qp_index_offset behind weighted_bipred_idc 2. The
    /// minimal hand fixture pins the skeleton the rejection fixtures
    /// perturb.
    #[test]
    fn parses_real_and_minimal_pps_bodies() {
        parse_pps(PPS, &sps_ctx(&[SPS])).unwrap();
        parse_pps(HIGH_PPS, &sps_ctx(&[HIGH_SPS])).unwrap();
        parse_pps(MINIMAL_PPS, &sps_ctx(&[SPS])).unwrap();
    }

    /// A PPS cut mid-syntax (here inside pic_init_qp_minus26) must fail on
    /// BOTH wrapper paths and through runtime construction, exactly like a
    /// truncated SPS.
    #[test]
    fn rejects_truncated_pps_on_both_paths() {
        let cut = &PPS[..3];
        let err = parse_parameter_sets(&annexb_with(SPS, cut)).unwrap_err();
        assert!(
            err.contains("PPS") && err.contains("truncated"),
            "unexpected error: {err}"
        );
        let err = parse_avcc_parameter_sets(&raw_avcc(66, 0xC0, 0x1E, SPS, cut)).unwrap_err();
        assert!(
            err.contains("PPS") && err.contains("truncated"),
            "unexpected error: {err}"
        );
        assert!(matches!(
            AvcRuntime::from_extradata(&annexb_with(SPS, cut), 4),
            Err(PacketSinkError::InvalidExtradata { stream_index: 4, .. })
        ));
    }

    /// A single cleared stop bit leaves every parsed PPS field intact, so
    /// only the rbsp_trailing_bits check can catch it. The avcC entry
    /// framing preserves the now-zero trailing byte, so the rejection is
    /// pinned through the full record path and runtime construction.
    #[test]
    fn rejects_pps_with_corrupt_rbsp_trailing_bits() {
        let err = parse_pps(BAD_STOP_BIT_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(err.contains("stop bit"), "unexpected error: {err}");
        let avcc = raw_avcc(66, 0xC0, 0x1E, SPS, BAD_STOP_BIT_PPS);
        let err = parse_avcc_parameter_sets(&avcc).unwrap_err();
        assert!(err.contains("stop bit"), "unexpected error: {err}");
        assert!(matches!(
            AvcRuntime::from_extradata(&avcc, 8),
            Err(PacketSinkError::InvalidExtradata { stream_index: 8, .. })
        ));
    }

    /// pic_parameter_set_id is bounded to 255 (MAX_PPS_COUNT = 256 in
    /// h264_ps.c); the fixtures differ in the final bit of the ue code, so
    /// 256 is rejected by value, not by shape.
    #[test]
    fn rejects_pic_parameter_set_id_above_255() {
        parse_pps(PPS_ID_255_PPS, &sps_ctx(&[SPS])).unwrap();
        let err = parse_pps(PPS_ID_256_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(
            err.contains("pic_parameter_set_id 256"),
            "unexpected error: {err}"
        );
    }

    /// seq_parameter_set_id 32 is out of range outright (MAX_SPS_COUNT =
    /// 32); 31 is legal but must RESOLVE — against the id-31 SPS it parses,
    /// while against a configuration whose only SPS is id 0 it is the
    /// dangling reference h264_ps.c fails with the same "sps_id out of
    /// range" error.
    #[test]
    fn rejects_pps_sps_reference_out_of_range_or_dangling() {
        let err = parse_pps(SPS_ID_32_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(
            err.contains("seq_parameter_set_id 32"),
            "unexpected error: {err}"
        );
        parse_pps(SPS_ID_31_PPS, &sps_ctx(&[SPS_ID_31])).unwrap();
        let err = parse_pps(SPS_ID_31_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(err.contains("no preceding SPS"), "unexpected error: {err}");
    }

    /// weighted_bipred_idc is a two-bit field whose value 3 does not exist
    /// (7.4.2.2 bounds it to 2); h264_ps.c reads the bits unchecked, so the
    /// boundary pair pins the validator's added bound.
    #[test]
    fn rejects_weighted_bipred_idc_three() {
        parse_pps(WEIGHTED_BIPRED_2_PPS, &sps_ctx(&[SPS])).unwrap();
        let err = parse_pps(WEIGHTED_BIPRED_3_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(
            err.contains("weighted_bipred_idc 3"),
            "unexpected error: {err}"
        );
    }

    /// QP-family bounds, each pinned at its boundary: chroma_qp_index_offset
    /// in [-12, 12] (h264_ps.c rejects 13 identically), pic_init_qs_minus26
    /// in the flat [-26, 25], and pic_init_qp_minus26 in the 7.4.2.2 range
    /// of the REFERENCED SPS — [-26, 25] against the 8-bit fixtures, so
    /// both -27 (one bit past the floor) and the old fixed-envelope floor
    /// -88 are rejected there, while -32 flips per referenced depth: legal
    /// under the 10-bit SPS (floor -38), rejected under the 8-bit one.
    #[test]
    fn rejects_out_of_range_pps_qp_fields() {
        let ctx = sps_ctx(&[SPS]);
        parse_pps(CHROMA_QP_12_PPS, &ctx).unwrap();
        let err = parse_pps(CHROMA_QP_13_PPS, &ctx).unwrap_err();
        assert!(
            err.contains("chroma_qp_index_offset 13"),
            "unexpected error: {err}"
        );
        parse_pps(INIT_QP_M26_PPS, &ctx).unwrap();
        for (fixture, qp) in [
            (INIT_QP_M27_PPS, -27),
            (INIT_QP_M32_PPS, -32),
            (INIT_QP_M88_PPS, -88),
            (INIT_QP_M89_PPS, -89),
        ] {
            let err = parse_pps(fixture, &ctx).unwrap_err();
            assert!(
                err.contains(&format!("pic_init_qp_minus26 {qp}")),
                "unexpected error: {err}"
            );
        }
        // The same -32 PPS binds a 10-bit SPS: QpBdOffsetY 12 deepens the
        // floor to -38 and the parse succeeds.
        parse_pps(INIT_QP_M32_PPS, &sps_ctx(&[TEN_BIT_SPS])).unwrap();
        let err = parse_pps(INIT_QS_M27_PPS, &ctx).unwrap_err();
        assert!(
            err.contains("pic_init_qs_minus26 -27"),
            "unexpected error: {err}"
        );
    }

    /// num_ref_idx_l0_default_active_minus1 at 32 is the "reference
    /// overflow (pps)" rejection of h264_ps.c.
    #[test]
    fn rejects_pps_reference_count_overflow() {
        let err = parse_pps(REF_IDX_L0_32_PPS, &sps_ctx(&[SPS])).unwrap_err();
        assert!(err.contains("num_ref_idx"), "unexpected error: {err}");
    }

    /// The pic_scaling_matrix list count follows the REFERENCED SPS's
    /// chroma_format_idc (6 + 2 lists under 4:2:0/4:2:2, 6 + 6 under 4:4:4,
    /// transform_8x8_mode_flag set in both fixtures): each parses against
    /// the SPS shape it was assembled for and misaligns into rejection
    /// against the other, so the SPS context is load-bearing.
    #[test]
    fn pps_scaling_block_count_follows_the_referenced_sps() {
        let chroma1 = sps_ctx(&[HIGH_SPS]);
        let chroma3 = sps_ctx(&[CHROMA3_SCALING_SPS]);
        parse_pps(SCALING_TAIL_8_PPS, &chroma1).unwrap();
        assert!(parse_pps(SCALING_TAIL_8_PPS, &chroma3).is_err());
        parse_pps(SCALING_TAIL_12_PPS, &chroma3).unwrap();
        assert!(parse_pps(SCALING_TAIL_12_PPS, &chroma1).is_err());
    }

    /// The multi-group slice_group_map shapes of 7.3.2.2, one accept per
    /// family (types 0, 2, 3, 6 — type 2 would overrun into rejection if
    /// the rectangle loop wrongly ran num_slice_groups_minus1 + 1 times;
    /// type 6 pairs with the SPS whose 4 map units its table covers
    /// exactly), plus the two bounds: nine slice groups exceeds the A.2.1
    /// limit and map type 7 does not exist.
    #[test]
    fn parses_the_slice_group_map_types() {
        let ctx = sps_ctx(&[SPS]);
        for (i, fixture) in [FMO_TYPE0_PPS, FMO_TYPE2_PPS, FMO_TYPE3_PPS]
            .iter()
            .enumerate()
        {
            parse_pps(fixture, &ctx).unwrap_or_else(|e| panic!("FMO fixture {i}: {e}"));
        }
        parse_pps(FMO_TYPE6_PPS, &sps_ctx(&[MAP_UNITS_4_SPS]))
            .unwrap_or_else(|e| panic!("FMO type-6 fixture: {e}"));
        let err = parse_pps(FMO_GROUPS_8_PPS, &ctx).unwrap_err();
        assert!(
            err.contains("num_slice_groups_minus1 8"),
            "unexpected error: {err}"
        );
        let err = parse_pps(FMO_TYPE7_PPS, &ctx).unwrap_err();
        assert!(
            err.contains("slice_group_map_type 7"),
            "unexpected error: {err}"
        );
    }

    /// The slice-group shapes are held to the referenced SPS (7.4.2.2):
    /// every map-unit index fits the SPS's PicSizeInMapUnits, the type-6
    /// table covers it EXACTLY — an oversized table indexes past the
    /// picture and an undersized one leaves map units with no slice
    /// group, so both directions reject — type-2 rectangles are
    /// corner-ordered, changing maps require exactly two groups and
    /// type-6 ids stay within the declared group count. The in-budget
    /// accepts live in `parses_the_slice_group_map_types`.
    #[test]
    fn bounds_the_slice_group_map_against_the_referenced_sps() {
        let ctx = sps_ctx(&[SPS]);
        for (fixture, needle) in [
            (FMO_TYPE0_RUN_300_PPS, "run_length_minus1 300"),
            (FMO_TYPE2_BR_300_PPS, "bottom_right 300"),
            (FMO_RATE_300_PPS, "slice_group_change_rate_minus1 300"),
            (FMO_TYPE6_SIZE_300_PPS, "declares 301 map units"),
            (FMO_TYPE6_PPS, "declares 4 map units"),
            (FMO_TYPE2_INVERTED_PPS, "rectangle 40..20 is inverted"),
            (FMO_TYPE2_COLUMN_PPS, "rectangle 1..20 is inverted"),
            (FMO_TYPE3_GROUPS_3_PPS, "requires exactly two slice groups"),
        ] {
            let err = parse_pps(fixture, &ctx).unwrap_err();
            assert!(err.contains(needle), "expected {needle:?}, got: {err}");
        }
        // The id bound needs a table that already covers its picture: the
        // four-entry table against the 4-map-unit SPS passes the size
        // check and fails only on its final id.
        let err = parse_pps(FMO_TYPE6_ID_3_PPS, &sps_ctx(&[MAP_UNITS_4_SPS])).unwrap_err();
        assert!(err.contains("slice_group_id 3"), "unexpected error: {err}");
    }

    /// max_num_ref_frames is bounded to 16 (H264_MAX_DPB_FRAMES; the
    /// h264_ps.c "too many reference frames" rejection): the fixtures sit
    /// one ue bit apart across the boundary.
    #[test]
    fn rejects_max_num_ref_frames_above_16() {
        assert_eq!(parse_sps(REF_FRAMES_16_SPS).unwrap().chroma_info(), (1, 8, 8));
        let err = parse_sps(REF_FRAMES_17_SPS).unwrap_err();
        assert!(err.contains("max_num_ref_frames 17"), "unexpected error: {err}");
    }

    /// Coded-dimension sanity (the av_image_check_size gate of h264_ps.c):
    /// 65520x16 is the widest accepted shape, one more macroblock column
    /// overflows the 16-bit axis cap, and 65520x65520 — each axis legal —
    /// fails the sample-buffer product bound.
    #[test]
    fn rejects_oversized_coded_dimensions() {
        let summary = parse_sps(WIDTH_65520_SPS).unwrap();
        assert_eq!(
            (summary.pic_width_in_mbs, summary.pic_height_in_map_units),
            (4095, 1)
        );
        let err = parse_sps(WIDTH_65536_SPS).unwrap_err();
        assert!(err.contains("65536x16"), "unexpected error: {err}");
        let err = parse_sps(PIXEL_PRODUCT_SPS).unwrap_err();
        assert!(
            err.contains("65520x65520") && err.contains("buffer"),
            "unexpected error: {err}"
        );
    }

    /// Cropping must leave at least one sample per axis (the h264_ps.c
    /// "crop values invalid" gate): a 4:2:0 horizontal crop of 318 of 320
    /// columns passes, 320 of 320 removes the whole picture. One ue bit
    /// apart.
    #[test]
    fn rejects_cropping_that_removes_the_whole_picture() {
        assert_eq!(parse_sps(NEAR_FULL_CROP_SPS).unwrap().chroma_info(), (1, 8, 8));
        let err = parse_sps(FULL_CROP_SPS).unwrap_err();
        assert!(
            err.contains("removes the whole"),
            "unexpected error: {err}"
        );
    }

    /// VUI value bounds (E.2.1): the all-ceilings bitstream_restriction
    /// fixture parses, then each field one past its bound is rejected —
    /// reorder depth over 16 and over max_dec_frame_buffering, buffering
    /// over 16 and under the SPS's max_num_ref_frames (the DPB must hold
    /// at least the declared reference frames), both denominators, the
    /// MV-length exponent, zeroed timing_info fields and
    /// chroma_sample_loc_type 6.
    #[test]
    fn rejects_out_of_range_vui_fields() {
        assert_eq!(parse_sps(REORDER_16_SPS).unwrap().chroma_info(), (1, 8, 8));
        assert_eq!(parse_sps(CHROMA_LOC_5_SPS).unwrap().chroma_info(), (1, 8, 8));
        // The E.2.1 floor meeting the ceiling stays legal:
        // max_num_ref_frames 16 == max_dec_frame_buffering 16.
        assert_eq!(
            parse_sps(REF_EQUALS_BUFFERING_SPS).unwrap().chroma_info(),
            (1, 8, 8)
        );
        for (fixture, needle) in [
            (REORDER_17_SPS, "max_num_reorder_frames 17"),
            (REORDER_ABOVE_BUFFERING_SPS, "max_num_reorder_frames 2"),
            (BUFFERING_17_SPS, "max_dec_frame_buffering 17"),
            (BUFFERING_BELOW_REF_SPS, "max_dec_frame_buffering 0"),
            (BYTES_DENOM_17_SPS, "max_bytes_per_pic_denom 17"),
            (MB_DENOM_17_SPS, "max_bits_per_mb_denom 17"),
            (MV_LEN_17_SPS, "log2_max_mv_length 17/0"),
            (ZERO_TIME_SCALE_SPS, "timing_info 1/0"),
            (ZERO_NUM_UNITS_SPS, "timing_info 0/25"),
            (CHROMA_LOC_6_SPS, "chroma_sample_loc_type 6/0"),
        ] {
            let err = parse_sps(fixture).unwrap_err();
            assert!(err.contains(needle), "expected {needle:?}, got: {err}");
        }
    }

    /// delta_scale is bounded to [-128, 127] (7.4.2.1.1.1; the
    /// decode_scaling_list rejection): the floor value parses through all
    /// sixteen codes, +128 differs inside one se code and is rejected.
    #[test]
    fn rejects_delta_scale_outside_range() {
        assert_eq!(
            parse_sps(DELTA_SCALE_M128_SPS).unwrap().chroma_info(),
            (1, 8, 8)
        );
        let err = parse_sps(DELTA_SCALE_128_SPS).unwrap_err();
        assert!(err.contains("delta_scale 128"), "unexpected error: {err}");
    }
}
