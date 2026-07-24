//! The full PPS parse every extradata path routes each PPS body through,
//! plus the multi-group slice-group map validation it drives.

use super::rbsp::{unescape_rbsp, BitReader};
use super::sps::{skip_scaling_list, SpsSummary};

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
pub(super) fn parse_pps(pps: &[u8], sps_context: &[SpsSummary]) -> Result<(u32, usize), String> {
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
