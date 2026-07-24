//! PPS parse tests: field bounds, SPS binding and the slice-group map.

use super::super::pps::parse_pps;
use super::super::{parse_avcc_parameter_sets, parse_parameter_sets, AvcRuntime};
use super::*;
use crate::error::PacketSinkError;

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
