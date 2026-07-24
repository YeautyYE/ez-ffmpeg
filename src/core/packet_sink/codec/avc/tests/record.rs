//! Record parsing, avcC synthesis, extension and header-consistency tests.

use super::super::record::{derived_extension, parse_avcc_record};
use super::super::{
    build_avcc, parse_avcc_parameter_sets, parse_parameter_sets, AvcRuntime, CodecProjection,
};
use super::*;
use crate::error::PacketSinkError;

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
