//! SPS parse tests: field bounds, truncation and acceptance fixtures.

use super::super::{build_avcc, parse_avcc_parameter_sets, parse_parameter_sets, AvcRuntime};
use super::*;
use crate::error::PacketSinkError;

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
