//! The full SPS parse every extradata path routes each SPS body through,
//! plus the syntax passes it drives: scaling lists, VUI and HRD.

use super::rbsp::{unescape_rbsp, BitReader};

/// The SPS fields later validation stages consume: the identity a PPS
/// resolves its `seq_parameter_set_id` reference against, the chroma /
/// bit-depth block the avcC profile extension, the PPS scaling-matrix
/// size and the `pic_init_qp_minus26` bound derive from, and the coded
/// dimensions the PPS slice-group map is held against.
#[derive(Debug, Clone, Copy)]
pub(super) struct SpsSummary {
    pub(super) sps_id: u32,
    pub(super) chroma_format_idc: u8,
    pub(super) bit_depth_luma: u8,
    pub(super) bit_depth_chroma: u8,
    /// `PicWidthInMbs` (H.264 7-13): `pic_width_in_mbs_minus1 + 1`.
    pub(super) pic_width_in_mbs: u32,
    /// `PicHeightInMapUnits` (H.264 7-16):
    /// `pic_height_in_map_units_minus1 + 1` — map units, NOT frame
    /// macroblock rows (a field-coded picture holds two map units per
    /// frame MB column). `PicSizeInMapUnits`, the budget every FMO
    /// slice-group shape is bounded by (7.4.2.2), is the product of
    /// these two fields (7-17).
    pub(super) pic_height_in_map_units: u32,
}

impl SpsSummary {
    /// The `(chroma_format_idc, bit_depth_luma, bit_depth_chroma)` triple
    /// the avcC profile extension mirrors.
    pub(super) fn chroma_info(&self) -> (u8, u8, u8) {
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
pub(super) fn parse_sps(sps: &[u8]) -> Result<SpsSummary, String> {
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
pub(super) fn skip_scaling_list(r: &mut BitReader, size: u32) -> Result<(), String> {
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
