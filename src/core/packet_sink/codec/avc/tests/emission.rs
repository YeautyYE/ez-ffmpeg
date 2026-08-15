//! Encoder-specific emission-shape fixtures for the newly admitted wrappers.
//!
//! Payloads are the minimum legal bytes that express each wrapper's audited
//! NAL histogram (FFmpeg 7.1 and 8.1 share these shapes). Tests assert
//! normalization, parameter-set separation, IDR detection, and access-unit
//! completeness — not bitrate, QP, or a full payload hash. Hardware user-data
//! SEI text / UUIDs are not stored.
//!
//! The n7.1 and n8.1 fixtures share the same **synthetic** bytes — they are
//! hand-built shapes, not captured VideoToolbox / OpenH264 output. Split
//! them into per-ref fixtures only when a real shape divergence between the
//! two FFmpeg refs is observed.

use super::super::{au_boundary, AvcRuntime};
use super::*;
use crate::core::packet_sink::nal_framing::{collect_annexb, collect_length_prefixed};

struct H264EmissionFixture<'a> {
    encoder: &'static str,
    ffmpeg_refs: &'static [&'static str],
    extradata: &'static [u8],
    packets: &'a [&'a [u8]],
    expected_nal_types: &'a [&'a [u8]],
    /// Which packet indices must be classified as IDR.
    idr_packets: &'a [usize],
}

fn nal_types(length_prefixed: &[u8]) -> Vec<u8> {
    collect_length_prefixed(length_prefixed)
        .expect("normalized AU must parse as length-prefixed")
        .iter()
        .map(|nal| nal[0] & 0x1F)
        .collect()
}

fn assert_fixture(fx: &H264EmissionFixture) {
    assert!(
        !fx.ffmpeg_refs.is_empty(),
        "{} fixture must record the audited FFmpeg refs",
        fx.encoder
    );
    let (runtime, avcc, _) = AvcRuntime::from_extradata(fx.extradata, 0).unwrap();
    assert_eq!(
        avcc[0], 1,
        "{} extradata must normalize to avcC",
        fx.encoder
    );
    assert_eq!(fx.packets.len(), fx.expected_nal_types.len());

    let mut scratch = Vec::new();
    for (index, packet) in fx.packets.iter().enumerate() {
        let (is_key, data) = runtime
            .normalize_au(packet, &mut scratch, 0)
            .unwrap_or_else(|e| panic!("{} packet {index} rejected: {e}", fx.encoder));
        assert_eq!(
            is_key,
            fx.idr_packets.contains(&index),
            "{} packet {index} IDR flag",
            fx.encoder
        );
        let types = nal_types(data);
        assert_eq!(
            types, fx.expected_nal_types[index],
            "{} packet {index} NAL types",
            fx.encoder
        );
        assert!(
            !types.iter().any(|t| matches!(t, 7 | 8)),
            "{} packet {index} must not carry in-band SPS/PPS",
            fx.encoder
        );

        // Shadow the Phase 2 boundary check on the normalized NAL sequence.
        // Trusted production `normalize_au` does not call this.
        let nals = collect_length_prefixed(data).unwrap();
        au_boundary::validate_nals(nals).unwrap_or_else(|e| {
            panic!(
                "{} packet {index} failed AU-boundary shadow: {e:?}",
                fx.encoder
            )
        });
    }
}

// Shared Annex-B SPS/PPS (Constrained Baseline 320x240). Both wrappers
// emit Annex-B extradata under GLOBAL_HEADER.
const VT_OPENH264_EXTRADATA: &[u8] = &[
    0, 0, 0, 1, //
    0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x01, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00,
    0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xE4, 0x80, //
    0, 0, 1, //
    0x68, 0xCB, 0x83, 0xCB, 0x20,
];

// VideoToolbox (bf=0, GLOBAL_HEADER): first key AU is user-data SEI + IDR;
// subsequent AUs are a single non-IDR slice. Filler is allowed in-AU.
const VT_KEY_SEI_IDR: &[u8] = &[
    0, 0, 0, 1, 0x06, 0x05, 0xFF, 0x80, // SEI (type 6)
    0, 0, 1, 0x65, 0x88, 0x84, 0x21, 0xFF, // IDR, first_mb=0
];
const VT_NONIDR: &[u8] = &[0, 0, 0, 1, 0x41, 0x9A, 0x21, 0x03];
// max_slice_bytes / multi-slice sample: one first_mb=0 slice plus two
// non-zero slices, still one access unit. Built so the Phase 2 shadow
// count of first_mb==0 is exactly one.
fn vt_multi_slice_packet() -> Vec<u8> {
    let s0 = slice_nal(5, 0);
    let s1 = slice_nal(1, 20);
    let s2 = slice_nal(1, 40);
    annexb_concat(&[&s0, &s1, &s2])
}

const VT_FILLER_NONIDR: &[u8] = &[
    0, 0, 0, 1, 0x41, 0x9A, 0x22, 0x03, //
    0, 0, 1, 0x0C, 0xFF, 0xFF, 0x80,
];

fn videotoolbox_fixture<'a>(
    packets: &'a [&'a [u8]],
    expected_nal_types: &'a [&'a [u8]],
    idr_packets: &'a [usize],
) -> H264EmissionFixture<'a> {
    H264EmissionFixture {
        encoder: "h264_videotoolbox",
        ffmpeg_refs: &["n7.1", "n8.1"],
        extradata: VT_OPENH264_EXTRADATA,
        packets,
        expected_nal_types,
        idr_packets,
    }
}

// OpenH264 (GLOBAL_HEADER): IDR image layer has no parameter-set layer;
// multi-slice stays in one packet; non-IDR may also be multi-slice.
fn openh264_idr_multi() -> Vec<u8> {
    let s0 = slice_nal(5, 0);
    let s1 = slice_nal(5, 50);
    annexb_concat(&[&s0, &s1])
}

fn openh264_p_multi() -> Vec<u8> {
    let s0 = slice_nal(1, 0);
    let s1 = slice_nal(1, 50);
    let s2 = slice_nal(1, 100);
    annexb_concat(&[&s0, &s1, &s2])
}

const OPENH264_IDR_SINGLE: &[u8] = &[0, 0, 0, 1, 0x65, 0x88, 0x84, 0x21, 0xFF];

fn run_videotoolbox_shapes() {
    let multi = vt_multi_slice_packet();
    let packets: [&[u8]; 4] = [
        VT_KEY_SEI_IDR,
        VT_NONIDR,
        multi.as_slice(),
        VT_FILLER_NONIDR,
    ];
    let types: [&[u8]; 4] = [&[6, 5], &[1], &[5, 1, 1], &[1, 12]];
    assert_fixture(&videotoolbox_fixture(&packets, &types, &[0, 2]));
}

fn run_openh264_shapes() {
    let idr_multi = openh264_idr_multi();
    let p_multi = openh264_p_multi();
    let packets: [&[u8]; 3] = [
        OPENH264_IDR_SINGLE,
        idr_multi.as_slice(),
        p_multi.as_slice(),
    ];
    let types: [&[u8]; 3] = [&[5], &[5, 5], &[1, 1, 1]];
    let fx = H264EmissionFixture {
        encoder: "libopenh264",
        ffmpeg_refs: &["n7.1", "n8.1"],
        extradata: VT_OPENH264_EXTRADATA,
        packets: &packets,
        expected_nal_types: &types,
        idr_packets: &[0, 1],
    };
    assert_fixture(&fx);
}

#[test]
fn videotoolbox_n7_1_emission_shape() {
    run_videotoolbox_shapes();
}

#[test]
fn videotoolbox_n8_1_emission_shape() {
    run_videotoolbox_shapes();
}

#[test]
fn openh264_n7_1_emission_shape() {
    run_openh264_shapes();
}

#[test]
fn openh264_n8_1_emission_shape() {
    run_openh264_shapes();
}

/// Wrapper output is Annex-B; the runtime must emit 4-byte length prefixes
/// and leave every NAL in the access unit (SEI is not dropped).
#[test]
fn videotoolbox_annexb_rewrites_to_length_prefixed_without_dropping_sei() {
    let (runtime, _, _) = AvcRuntime::from_extradata(VT_OPENH264_EXTRADATA, 0).unwrap();
    let mut scratch = Vec::new();
    let (is_key, data) = runtime
        .normalize_au(VT_KEY_SEI_IDR, &mut scratch, 0)
        .unwrap();
    assert!(is_key);
    assert_eq!(
        data,
        [0, 0, 0, 4, 0x06, 0x05, 0xFF, 0x80, 0, 0, 0, 5, 0x65, 0x88, 0x84, 0x21, 0xFF]
    );
    // Input was Annex-B; confirm the walker saw start codes, not avcC.
    assert_eq!(collect_annexb(VT_KEY_SEI_IDR).unwrap().len(), 2);
}

#[test]
fn openh264_multi_slice_stays_one_length_prefixed_packet() {
    let packet = openh264_p_multi();
    let (runtime, _, _) = AvcRuntime::from_extradata(VT_OPENH264_EXTRADATA, 0).unwrap();
    let mut scratch = Vec::new();
    let (is_key, data) = runtime.normalize_au(&packet, &mut scratch, 0).unwrap();
    assert!(!is_key);
    let nals = collect_length_prefixed(data).unwrap();
    assert_eq!(nals.len(), 3, "all slices remain in the same packet");
    assert!(nals.iter().all(|nal| nal[0] & 0x1F == 1));
}
