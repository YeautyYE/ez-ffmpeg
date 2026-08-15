//! Shared fixtures and helpers for the AVC unit tests; the tests themselves
//! live in the child modules, split by subject.

mod emission;
mod pps;
mod record;
mod runtime;
mod sps;

use super::sps::{parse_sps, SpsSummary};
use super::ParameterSets;

// Encoder-produced parameter sets (x264 via the ffmpeg CLI, header byte
// included): Constrained Baseline (profile_idc 66, constraint_set0+1 ->
// compatibility 0xC0), level_idc 30, coding 320x240 yuv420p (20x15
// macroblocks, frame_mbs_only, no cropping). Profile 66 means no avcC
// chroma/bit-depth extension applies. The payloads carry real emulation
// prevention (00 00 03) sequences.
const SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x01, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00,
    0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xE4, 0x80,
];
const PPS: &[u8] = &[0x68, 0xCB, 0x83, 0xCB, 0x20];

// Same encoder, profile and level as `SPS` (identical bytes 1..4, so
// the derived projection matches) but coding 640x480: same
// seq_parameter_set_id 0 with a different tail, so it REPLACES `SPS`
// in the id map when both appear.
const SAME_PROJ_SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x00, 0xA0, 0x3D, 0xB0, 0x11, 0x00, 0x00, 0x03, 0x00, 0x01, 0x00,
    0x00, 0x03, 0x00, 0x32, 0x0F, 0x16, 0x2E, 0x48,
];

// Same encoder and coded shape (320x240 yuv420p 8-bit, level_idc 30) at
// High profile (profile_idc 100): chroma_format_idc 1 and 8-bit depths,
// the values the avcC extension must carry.
const HIGH_SPS: &[u8] = &[
    0x67, 0x64, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
    0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
];
const HIGH_PPS: &[u8] = &[0x68, 0xEB, 0xE3, 0xCB, 0x22, 0xC0];

// `HIGH_SPS` with seq_scaling_matrix_present_flag set and eight zero
// seq_scaling_list_present_flag bits spliced in. The flag is the last
// bit of RBSP byte 3 (0xAC -> 0xAD) and the insertion point is the byte
// boundary right after it, so the splice is one flipped bit plus one
// inserted 0x00 byte; everything downstream is bit-identical.
const HIGH_SPS_SCALING: &[u8] = &[
    0x67, 0x64, 0x00, 0x1E, 0xAD, 0x00, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00,
    0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
];
// `HIGH_SPS` with profile_idc patched to 144 (a one-byte u(8) field, no
// bit shifts). 144 is in the chroma-block dispatch set; a parser that
// omitted it would read the chroma/bit-depth bits as frame_num / POC
// syntax and end misaligned, failing rbsp_trailing_bits — so this
// fixture being accepted pins the dispatch membership.
const PROFILE_144_SPS: &[u8] = &[
    0x67, 0x90, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
    0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x60,
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
const PROFILE_135_TEN_BIT_SPS: &[u8] =
    &[0x67, 0x87, 0x00, 0x1E, 0xA6, 0xCB, 0x40, 0xA0, 0xFC, 0x80];

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
    0x67, 0x42, 0xC0, 0x1E, 0xD3, 0x00, 0x80, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x40, 0xA0, 0xFC, 0x80,
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
    0x67, 0x64, 0x00, 0x1E, 0xAC, 0xD9, 0x41, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
    0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xD9, 0x40,
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
    0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x06, 0x11, 0x08, 0x84, 0x42, 0x21, 0x10, 0x8C,
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
const BUFFERING_BELOW_REF_SPS: &[u8] =
    &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x05, 0xFC];
const REF_EQUALS_BUFFERING_SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xD8, 0x44, 0x14, 0x1F, 0xA0, 0x17, 0xC2, 0x30,
];
// VUI timing_info with time_scale 0, then with num_units_in_tick 0:
// either zero leaves the declared tick undefined (E.2.1).
const ZERO_TIME_SCALE_SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x40, 0x00, 0x00, 0x03, 0x00, 0x40, 0x00, 0x00,
    0x03, 0x00, 0x01,
];
const ZERO_NUM_UNITS_SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x40, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x06, 0x41,
];
// chroma_sample_loc_type 5 on both fields (the E.2.1 ceiling) and 6.
const CHROMA_LOC_5_SPS: &[u8] = &[
    0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x98, 0xC0, 0x80,
];
const CHROMA_LOC_6_SPS: &[u8] = &[0x67, 0x42, 0xC0, 0x1E, 0xDA, 0x05, 0x07, 0xE8, 0x9E, 0x08];
// High10 (profile_idc 110) 320x240 at 10-bit depth: QpBdOffsetY = 12,
// deepening the pic_init_qp_minus26 floor of a referencing PPS to -38.
const TEN_BIT_SPS: &[u8] = &[0x67, 0x6E, 0x00, 0x1E, 0xA6, 0xCB, 0x40, 0xA0, 0xFC, 0x80];
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

/// MSB-first bit writer used to assemble synthetic slice NAL payloads.
struct BitWriter {
    bytes: Vec<u8>,
    acc: u8,
    n: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            acc: 0,
            n: 0,
        }
    }

    fn bit(&mut self, value: u8) {
        self.acc = (self.acc << 1) | (value & 1);
        self.n += 1;
        if self.n == 8 {
            self.bytes.push(self.acc);
            self.acc = 0;
            self.n = 0;
        }
    }

    /// Unsigned Exp-Golomb of `value`.
    fn ue(&mut self, value: u32) {
        let x = u64::from(value) + 1;
        let zeros = 63 - x.leading_zeros();
        for _ in 0..zeros {
            self.bit(0);
        }
        for i in (0..=zeros).rev() {
            self.bit(((x >> i) & 1) as u8);
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.n > 0 {
            self.acc <<= 8 - self.n;
            self.bytes.push(self.acc);
        }
        if self.bytes.is_empty() {
            self.bytes.push(0x80);
        }
        self.bytes
    }
}

/// Slice NAL whose first syntax element is `first_mb_in_slice`.
fn slice_nal(nal_type: u8, first_mb: u32) -> Vec<u8> {
    let nal_ref_idc = if nal_type == 5 { 3 } else { 2 };
    let mut bits = BitWriter::new();
    bits.ue(first_mb);
    // One extra 1-bit so the NAL is more than a lone ue(0) and still
    // starts the RBSP with the requested first_mb code.
    bits.bit(1);
    let mut nal = vec![(nal_ref_idc << 5) | nal_type];
    nal.extend(bits.finish());
    nal
}

/// Joins NALs into an Annex-B configuration: a 4-byte start code, then
/// 3-byte separators.
fn annexb_concat(nals: &[&[u8]]) -> Vec<u8> {
    let mut v = Vec::new();
    for (i, nal) in nals.iter().enumerate() {
        v.extend_from_slice(if i == 0 {
            &[0, 0, 0, 1][..]
        } else {
            &[0, 0, 1][..]
        });
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

fn high_sets() -> ParameterSets {
    ParameterSets {
        sps: vec![HIGH_SPS.to_vec()],
        pps: vec![HIGH_PPS.to_vec()],
    }
}
