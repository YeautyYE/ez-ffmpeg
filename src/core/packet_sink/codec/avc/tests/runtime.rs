//! Runtime construction, payload normalization, S8 announcement and
//! in-band parameter-set tests.

use super::super::{build_avcc, parse_parameter_sets, AvcRuntime};
use super::*;
use crate::core::packet_sink::nal_framing::collect_annexb;
use crate::error::PacketSinkError;

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
/// stops after the PPS array, `decode_extradata_ps` ignores type 13,
/// and CBS's avcC split warns off the bytes after the PPS array,
/// decomposing SPS-EXT only when one arrives on the Annex-B/NAL
/// path.) An entry re-sent with a
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
