//! Simplified H.264 access-unit boundary check for unaudited wrappers.
//!
//! This module is a Phase 2 prototype. The Trusted packet-sink path
//! (`AvcRuntime::normalize_au`) must **not** call it: audited encoders keep
//! zero extra per-NAL work. Enforcement on a `ValidateBoundaries` policy
//! is a later change and needs encoder identity from the muxer, which this
//! crate does not recover from `AVCodecParameters`.
//!
//! # Algorithm (H.264 7.4.1.2.3 / 7.4.1.2.4)
//!
//! Counting `first_mb_in_slice == 0` is **order-independent**. ASO may
//! deliver the macroblock-0 slice after other slices of the same primary
//! picture; a "second VCL with `first_mb == 0`" detector would reject that
//! legal ordering and is not implemented here.
//!
//! 1. For VCL types 1, 2, and 5, read the first `ue(v)` (first_mb_in_slice).
//! 2. The packet must contain exactly one slice with that value equal to 0.
//! 3. Types 3/4 (data partitions B/C) do not count as picture starts and
//!    are allowed only after a type-2 partition A in the same packet.
//! 4. At most one AUD, and only before any VCL.
//! 5. AUD, SEI, SPS, or PPS after VCL starts the next access unit — fail.
//! 6. Filler (type 12) and end-of-sequence (type 10) are allowed only
//!    **after** VCL; either before the first VCL fails closed. End of
//!    stream (type 11) is allowed only after VCL and must be the **last**
//!    NAL in the packet — any NAL after it fails.
//! 7. Types 13..=23, 0, and 24..=31 fail closed, including VCL extension
//!    types 20/21. Redundant coded pictures and separate colour planes
//!    produce extra `first_mb == 0` values and therefore fail closed.
//!
//! The reader skips `00 00 03` emulation-prevention bytes in place and
//! never allocates an RBSP. The byte after a skipped `03` must be
//! `0x00..=0x03` (H.264 7.4.1); anything else fails closed as an invalid
//! emulation-prevention sequence.

use crate::error::PacketSinkError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AuBoundaryError {
    InvalidNalHeader,
    TruncatedSliceHeader,
    MissingPrimaryPictureStart,
    MultiplePrimaryPictures,
    PrefixNalAfterVcl,
    UnexpectedDataPartition(u8),
    UnsupportedNalType(u8),
    UnsupportedVclExtension(u8),
    MisplacedNonVcl(u8),
    InvalidEmulationPrevention,
}

impl AuBoundaryError {
    pub(super) fn packet_reason(self) -> String {
        match self {
            Self::InvalidNalHeader => "H.264 NAL unit header is invalid".to_string(),
            Self::TruncatedSliceHeader => {
                "H.264 slice header has a truncated first_mb_in_slice".to_string()
            }
            Self::MissingPrimaryPictureStart => {
                "H.264 packet does not contain a primary coded picture start".to_string()
            }
            Self::MultiplePrimaryPictures => {
                "H.264 packet contains more than one primary coded picture".to_string()
            }
            Self::PrefixNalAfterVcl => {
                "H.264 packet starts a new access unit after VCL data".to_string()
            }
            Self::UnexpectedDataPartition(t) => {
                format!("H.264 data partition NAL type {t} without a coded slice data partition A")
            }
            Self::UnsupportedNalType(t) => {
                format!("H.264 NAL type {t} is not supported by boundary validation")
            }
            Self::UnsupportedVclExtension(t) => {
                format!("H.264 VCL extension NAL type {t} is not supported by boundary validation")
            }
            Self::MisplacedNonVcl(t) => {
                format!("H.264 non-VCL NAL type {t} is misplaced within the access unit")
            }
            Self::InvalidEmulationPrevention => {
                "H.264 emulation-prevention byte is followed by an invalid byte".to_string()
            }
        }
    }

    pub(super) fn into_malformed(self, stream_index: usize) -> PacketSinkError {
        PacketSinkError::MalformedPacket {
            stream_index,
            reason: self.packet_reason(),
        }
    }
}

pub(super) struct H264AuBoundaryValidator {
    zero_first_mb_count: u8,
    saw_vcl: bool,
    saw_aud: bool,
    saw_partition_a: bool,
    saw_end_of_stream: bool,
    saw_end_of_sequence: bool,
}

impl H264AuBoundaryValidator {
    pub(super) fn new() -> Self {
        Self {
            zero_first_mb_count: 0,
            saw_vcl: false,
            saw_aud: false,
            saw_partition_a: false,
            saw_end_of_stream: false,
            saw_end_of_sequence: false,
        }
    }

    pub(super) fn observe(&mut self, nal: &[u8]) -> Result<(), AuBoundaryError> {
        if nal.is_empty() {
            return Err(AuBoundaryError::InvalidNalHeader);
        }
        let header = nal[0];
        if header & 0x80 != 0 {
            return Err(AuBoundaryError::InvalidNalHeader);
        }
        let nal_type = header & 0x1F;
        if self.saw_end_of_stream {
            // End of stream (type 11) must be the last NAL in the packet.
            return Err(AuBoundaryError::MisplacedNonVcl(11));
        }
        if self.saw_end_of_sequence && nal_type != 11 {
            // End of sequence (type 10) closes this AU; only type 11 may follow.
            return Err(AuBoundaryError::MisplacedNonVcl(10));
        }
        match nal_type {
            1 | 5 => {
                if self.observe_coded_slice(nal)? {
                    // A new primary picture invalidates any earlier
                    // partition A; later type 3/4 partitions must follow
                    // their own partition A.
                    self.saw_partition_a = false;
                }
            }
            2 => {
                self.saw_partition_a = true;
                self.observe_coded_slice(nal)?;
            }
            3 | 4 => {
                if !self.saw_partition_a {
                    return Err(AuBoundaryError::UnexpectedDataPartition(nal_type));
                }
                self.saw_vcl = true;
            }
            6..=8 => {
                if self.saw_vcl {
                    return Err(AuBoundaryError::PrefixNalAfterVcl);
                }
            }
            9 => {
                if self.saw_vcl || self.saw_aud {
                    // A second AUD, or an AUD after VCL, starts another AU.
                    return Err(AuBoundaryError::PrefixNalAfterVcl);
                }
                self.saw_aud = true;
            }
            10 => {
                // End of sequence is a trailing NAL: before the first VCL
                // it cannot belong to this access unit. After VCL it
                // closes the AU for later VCL / data-partition NALs.
                if !self.saw_vcl {
                    return Err(AuBoundaryError::MisplacedNonVcl(10));
                }
                self.saw_end_of_sequence = true;
            }
            12 => {
                // Filler is a trailing NAL: before the first VCL it cannot
                // belong to this access unit.
                if !self.saw_vcl {
                    return Err(AuBoundaryError::MisplacedNonVcl(12));
                }
            }
            11 => {
                if !self.saw_vcl {
                    return Err(AuBoundaryError::MisplacedNonVcl(11));
                }
                self.saw_end_of_stream = true;
            }
            20 | 21 => return Err(AuBoundaryError::UnsupportedVclExtension(nal_type)),
            0 | 13..=19 | 22..=31 => {
                return Err(AuBoundaryError::UnsupportedNalType(nal_type));
            }
            _ => unreachable!("nal_unit_type is five bits"),
        }
        Ok(())
    }

    /// Returns `true` when the slice is a primary picture start
    /// (`first_mb_in_slice == 0`).
    fn observe_coded_slice(&mut self, nal: &[u8]) -> Result<bool, AuBoundaryError> {
        self.saw_vcl = true;
        let first_mb = first_mb_in_slice(&nal[1..])?;
        if first_mb == 0 {
            self.zero_first_mb_count = self.zero_first_mb_count.saturating_add(1);
            if self.zero_first_mb_count > 1 {
                return Err(AuBoundaryError::MultiplePrimaryPictures);
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn finish(self) -> Result<(), AuBoundaryError> {
        match self.zero_first_mb_count {
            0 => Err(AuBoundaryError::MissingPrimaryPictureStart),
            1 => Ok(()),
            _ => Err(AuBoundaryError::MultiplePrimaryPictures),
        }
    }
}

pub(super) fn validate_nals<'a, I>(nals: I) -> Result<(), AuBoundaryError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut validator = H264AuBoundaryValidator::new();
    for nal in nals {
        validator.observe(nal)?;
    }
    validator.finish()
}

fn first_mb_in_slice(payload: &[u8]) -> Result<u32, AuBoundaryError> {
    if payload.is_empty() {
        return Err(AuBoundaryError::TruncatedSliceHeader);
    }
    // ue(0) is a single `1` bit. A first payload byte with the high bit set
    // cannot be an emulation-prevention sequence (those start with 0x00).
    if payload[0] & 0x80 != 0 {
        return Ok(0);
    }
    let mut reader = EbspBitReader::new(payload);
    reader.ue()
}

/// Streaming EBSP bit reader: skips `00 00 03` in place, no allocation.
struct EbspBitReader<'a> {
    data: &'a [u8],
    next: usize,
    current: u8,
    bits_left: u8,
    epb_zeros: u8,
    after_epb: bool,
}

impl<'a> EbspBitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            next: 0,
            current: 0,
            bits_left: 0,
            epb_zeros: 0,
            after_epb: false,
        }
    }

    fn fill(&mut self) -> Result<(), AuBoundaryError> {
        loop {
            if self.next >= self.data.len() {
                return Err(AuBoundaryError::TruncatedSliceHeader);
            }
            let byte = self.data[self.next];
            self.next += 1;
            if self.epb_zeros >= 2 && byte == 0x03 {
                self.epb_zeros = 0;
                self.after_epb = true;
                continue;
            }
            if self.after_epb {
                self.after_epb = false;
                // H.264 7.4.1: the byte after an emulation-prevention `03`
                // must be 0x00..=0x03; `00 00 03 80` is not a legal EBSP.
                if byte > 0x03 {
                    return Err(AuBoundaryError::InvalidEmulationPrevention);
                }
            }
            if byte == 0 {
                self.epb_zeros = self.epb_zeros.saturating_add(1);
            } else {
                self.epb_zeros = 0;
            }
            self.current = byte;
            self.bits_left = 8;
            return Ok(());
        }
    }

    fn bit(&mut self) -> Result<u8, AuBoundaryError> {
        if self.bits_left == 0 {
            self.fill()?;
        }
        self.bits_left -= 1;
        Ok((self.current >> self.bits_left) & 1)
    }

    fn ue(&mut self) -> Result<u32, AuBoundaryError> {
        let mut zeros = 0u32;
        while self.bit()? == 0 {
            zeros += 1;
            if zeros > 31 {
                return Err(AuBoundaryError::TruncatedSliceHeader);
            }
        }
        if zeros == 0 {
            return Ok(0);
        }
        let mut rest = 0u32;
        for _ in 0..zeros {
            rest = (rest << 1) | u32::from(self.bit()?);
        }
        Ok(((1u32 << zeros) - 1) + rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::packet_sink::nal_framing::{
        collect_annexb, collect_length_prefixed, push_length_prefixed,
    };

    fn header(nal_ref_idc: u8, nal_type: u8) -> u8 {
        (nal_ref_idc << 5) | nal_type
    }

    fn annexb(nals: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, nal) in nals.iter().enumerate() {
            out.extend_from_slice(if i == 0 {
                &[0, 0, 0, 1][..]
            } else {
                &[0, 0, 1][..]
            });
            out.extend_from_slice(nal);
        }
        out
    }

    fn avcc(nals: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for nal in nals {
            push_length_prefixed(nal, &mut out);
        }
        out
    }

    fn write_ue(value: u32) -> Vec<u8> {
        let x = u64::from(value) + 1;
        let zeros = 63 - x.leading_zeros();
        let mut bits = vec![0u8; zeros as usize];
        for i in (0..=zeros).rev() {
            bits.push(((x >> i) & 1) as u8);
        }
        bits.push(1);
        let mut bytes = Vec::new();
        let mut acc = 0u8;
        let mut n = 0u8;
        for bit in bits {
            acc = (acc << 1) | bit;
            n += 1;
            if n == 8 {
                bytes.push(acc);
                acc = 0;
                n = 0;
            }
        }
        if n > 0 {
            acc <<= 8 - n;
            bytes.push(acc);
        }
        bytes
    }

    fn slice(nal_type: u8, first_mb: u32) -> Vec<u8> {
        let nal_ref_idc = if nal_type == 5 { 3 } else { 2 };
        let mut nal = vec![header(nal_ref_idc, nal_type)];
        nal.extend(write_ue(first_mb));
        nal
    }

    fn assert_ok(nals: &[&[u8]]) {
        validate_nals(nals.iter().copied()).expect("expected a single primary picture");
        let annexb_bytes = annexb(nals);
        let avcc_bytes = avcc(nals);
        let annexb_nals = collect_annexb(&annexb_bytes).unwrap();
        let avcc_nals = collect_length_prefixed(&avcc_bytes).unwrap();
        assert_eq!(
            validate_nals(annexb_nals.iter().copied()),
            validate_nals(avcc_nals.iter().copied()),
            "Annex-B and AVCC must agree"
        );
        validate_nals(annexb_nals).unwrap();
        validate_nals(avcc_nals).unwrap();
    }

    fn assert_err(nals: &[&[u8]], expected: AuBoundaryError) {
        assert_eq!(validate_nals(nals.iter().copied()), Err(expected));
        let annexb_bytes = annexb(nals);
        if let Ok(parsed) = collect_annexb(&annexb_bytes) {
            assert_eq!(validate_nals(parsed), Err(expected));
        }
        let avcc_bytes = avcc(nals);
        if let Ok(parsed) = collect_length_prefixed(&avcc_bytes) {
            assert_eq!(validate_nals(parsed), Err(expected));
        }
    }

    #[test]
    fn single_slice_idr_and_non_idr_pass() {
        assert_ok(&[&slice(5, 0)]);
        assert_ok(&[&slice(1, 0)]);
    }

    #[test]
    fn multi_slice_zero_then_nonzero_passes() {
        let a = slice(1, 0);
        let b = slice(1, 20);
        let c = slice(1, 40);
        assert_ok(&[&a, &b, &c]);
    }

    #[test]
    fn aso_nonzero_then_zero_still_one_picture() {
        // ASO: the macroblock-0 slice may arrive after other slices.
        let a = slice(1, 40);
        let b = slice(1, 0);
        let c = slice(1, 20);
        assert_ok(&[&a, &b, &c]);
    }

    #[test]
    fn mbaff_zero_first_mb_is_still_the_picture_start() {
        // MBAFF addresses macroblock *pairs*; the first pair is still 0.
        assert_ok(&[&slice(1, 0), &slice(1, 1)]);
    }

    #[test]
    fn sei_and_aud_before_vcl_pass() {
        let sei = [0x06, 0x01, 0x02, 0x80];
        let aud = [0x09, 0xF0];
        let idr = slice(5, 0);
        assert_ok(&[&aud, &sei, &idr]);
    }

    #[test]
    fn recovery_point_sei_before_vcl_passes() {
        // Payload is not parsed; a type-6 NAL before VCL is in-AU.
        let sei = [0x06, 0x06, 0x01, 0x80];
        let idr = slice(5, 0);
        assert_ok(&[&sei, &idr]);
    }

    #[test]
    fn filler_after_vcl_passes() {
        let p = slice(1, 0);
        let filler = [0x0C, 0xFF, 0x80];
        assert_ok(&[&p, &filler]);
    }

    #[test]
    fn two_zero_first_mb_slices_are_two_pictures() {
        let a = slice(1, 0);
        let b = slice(1, 0);
        assert_err(&[&a, &b], AuBoundaryError::MultiplePrimaryPictures);
    }

    #[test]
    fn two_field_pictures_in_one_packet_fail() {
        // Each field has its own macroblock 0.
        let top = slice(1, 0);
        let bot = slice(1, 0);
        assert_err(&[&top, &bot], AuBoundaryError::MultiplePrimaryPictures);
    }

    #[test]
    fn sei_after_vcl_starts_the_next_au() {
        let p = slice(1, 0);
        let sei = [0x06, 0x01, 0x80];
        assert_err(&[&p, &sei], AuBoundaryError::PrefixNalAfterVcl);
    }

    #[test]
    fn aud_after_vcl_starts_the_next_au() {
        let p = slice(1, 0);
        let aud = [0x09, 0xF0];
        assert_err(&[&p, &aud], AuBoundaryError::PrefixNalAfterVcl);
    }

    #[test]
    fn sps_after_vcl_starts_the_next_au() {
        let p = slice(1, 0);
        let sps = [0x67, 0x42, 0xC0, 0x1E];
        assert_err(&[&p, &sps], AuBoundaryError::PrefixNalAfterVcl);
    }

    #[test]
    fn packet_with_no_vcl_fails() {
        let sei = [0x06, 0x01, 0x80];
        assert_err(&[&sei], AuBoundaryError::MissingPrimaryPictureStart);
    }

    #[test]
    fn only_nonzero_first_mb_fails() {
        let a = slice(1, 20);
        let b = slice(1, 40);
        assert_err(&[&a, &b], AuBoundaryError::MissingPrimaryPictureStart);
    }

    #[test]
    fn header_only_slice_is_truncated() {
        let nal = [header(2, 1)];
        assert_err(&[&nal], AuBoundaryError::TruncatedSliceHeader);
    }

    #[test]
    fn overlong_exp_golomb_fails_closed() {
        // 32 leading zeros cannot be a legal first_mb_in_slice.
        let mut nal = vec![header(2, 1)];
        nal.extend(std::iter::repeat(0u8).take(5));
        assert_eq!(
            validate_nals([&nal[..]]),
            Err(AuBoundaryError::TruncatedSliceHeader)
        );
    }

    #[test]
    fn type_20_and_21_fail_closed() {
        let ext20 = [header(2, 20), 0x80];
        let ext21 = [header(2, 21), 0x80];
        assert_err(&[&ext20], AuBoundaryError::UnsupportedVclExtension(20));
        assert_err(&[&ext21], AuBoundaryError::UnsupportedVclExtension(21));
    }

    #[test]
    fn type_13_fails_closed() {
        let sps_ext = [header(3, 13), 0x80];
        assert_err(&[&sps_ext], AuBoundaryError::UnsupportedNalType(13));
    }

    #[test]
    fn partition_b_without_a_fails() {
        let b = [header(2, 3), 0x80];
        assert_err(&[&b], AuBoundaryError::UnexpectedDataPartition(3));
    }

    #[test]
    fn partition_a_counts_the_picture_start_and_bc_do_not() {
        let a = slice(2, 0);
        let b = [header(2, 3), 0x80];
        let c = [header(2, 4), 0x80];
        assert_ok(&[&a, &b, &c]);
    }

    #[test]
    fn forbidden_zero_bit_rejects_the_nal() {
        let nal = [0x80 | header(2, 1), 0x80];
        assert_err(&[&nal], AuBoundaryError::InvalidNalHeader);
    }

    #[test]
    fn emulation_prevention_nonzero_ue_is_read_in_place() {
        // Legal EBSP: the byte after the `00 00 03` escape is 0x00.
        // Encoded 00 00 03 00 80 00 80 80 unescapes to 00 00 00 80 00 80 80:
        // 24 leading zeros, then 1, then 24 rest bits (2^8 + 2^0 = 257) →
        // first_mb = (2^24 - 1) + 257 = 16777472. The final 0x80 is a
        // non-zero tail so Annex-B trailing_zero trim cannot drop the rest
        // bits the ue() still needs.
        let nal = vec![header(2, 1), 0x00, 0x00, 0x03, 0x00, 0x80, 0x00, 0x80, 0x80];
        let zero = slice(1, 0);
        assert_ok(&[&nal, &zero]);
        let mut reader = EbspBitReader::new(&nal[1..]);
        assert_eq!(reader.ue().unwrap(), 16_777_472);
    }

    #[test]
    fn emulation_prevention_followed_by_invalid_byte_fails() {
        // 00 00 03 80 is not a legal EBSP: the byte after the escape `03`
        // must be 0x00..=0x03 (H.264 7.4.1). Fail closed.
        let nal = vec![header(2, 1), 0x00, 0x00, 0x03, 0x80, 0x00, 0x80];
        assert_err(&[&nal], AuBoundaryError::InvalidEmulationPrevention);
    }

    #[test]
    fn end_of_sequence_before_vcl_fails() {
        let eos = [header(0, 10)];
        let idr = slice(5, 0);
        assert_err(&[&eos, &idr], AuBoundaryError::MisplacedNonVcl(10));
    }

    #[test]
    fn end_of_stream_before_vcl_fails() {
        let eostream = [header(0, 11)];
        let idr = slice(5, 0);
        assert_err(&[&eostream, &idr], AuBoundaryError::MisplacedNonVcl(11));
    }

    #[test]
    fn nal_after_end_of_stream_fails() {
        let p = slice(1, 0);
        let eostream = [header(0, 11)];
        let filler = [0x0C, 0xFF, 0x80];
        assert_err(
            &[&p, &eostream, &filler],
            AuBoundaryError::MisplacedNonVcl(11),
        );
    }

    #[test]
    fn filler_before_vcl_fails() {
        let filler = [0x0C, 0xFF, 0x80];
        let idr = slice(5, 0);
        assert_err(&[&filler, &idr], AuBoundaryError::MisplacedNonVcl(12));
    }

    #[test]
    fn eos_and_filler_after_vcl_pass() {
        let p = slice(1, 0);
        let filler = [0x0C, 0xFF, 0x80];
        let eos = [header(0, 10)];
        let eostream = [header(0, 11)];
        assert_ok(&[&p, &filler, &eos]);
        // End of stream is allowed only as the very last NAL.
        assert_ok(&[&p, &eostream]);
        assert_ok(&[&p, &filler, &eos, &eostream]);
    }

    #[test]
    fn vcl_after_end_of_sequence_fails() {
        let p = slice(1, 0);
        let eos = [header(0, 10)];
        let extra = slice(1, 1);
        assert_err(&[&p, &eos, &extra], AuBoundaryError::MisplacedNonVcl(10));
    }

    #[test]
    fn malformed_packet_reason_strings_are_stable() {
        assert_eq!(
            AuBoundaryError::MultiplePrimaryPictures.packet_reason(),
            "H.264 packet contains more than one primary coded picture"
        );
        assert_eq!(
            AuBoundaryError::MissingPrimaryPictureStart.packet_reason(),
            "H.264 packet does not contain a primary coded picture start"
        );
        assert_eq!(
            AuBoundaryError::TruncatedSliceHeader.packet_reason(),
            "H.264 slice header has a truncated first_mb_in_slice"
        );
        assert_eq!(
            AuBoundaryError::PrefixNalAfterVcl.packet_reason(),
            "H.264 packet starts a new access unit after VCL data"
        );
        assert_eq!(
            AuBoundaryError::UnsupportedVclExtension(20).packet_reason(),
            "H.264 VCL extension NAL type 20 is not supported by boundary validation"
        );
        assert_eq!(
            AuBoundaryError::MisplacedNonVcl(11).packet_reason(),
            "H.264 non-VCL NAL type 11 is misplaced within the access unit"
        );
        assert_eq!(
            AuBoundaryError::InvalidEmulationPrevention.packet_reason(),
            "H.264 emulation-prevention byte is followed by an invalid byte"
        );
        let err = AuBoundaryError::MultiplePrimaryPictures.into_malformed(3);
        assert!(
            matches!(
                err,
                PacketSinkError::MalformedPacket {
                    stream_index: 3,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn byte_cuts_of_a_valid_au_do_not_panic() {
        let idr = slice(5, 0);
        let sei = [0x06, 0x05, 0xFF, 0x80];
        let bytes = annexb(&[&sei, &idr]);
        for end in 0..=bytes.len() {
            let cut = &bytes[..end];
            let parsed = collect_annexb(cut);
            if let Ok(nals) = parsed {
                let _ = validate_nals(nals);
            }
        }
    }

    #[test]
    fn second_aud_before_vcl_is_another_access_unit() {
        let aud = [0x09, 0xF0];
        let idr = slice(5, 0);
        assert_err(&[&aud, &aud, &idr], AuBoundaryError::PrefixNalAfterVcl);
    }
}
