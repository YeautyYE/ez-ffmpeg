//! Codec-neutral NAL unit framing: streaming walkers over Annex-B and 4-byte
//! length-prefixed (AVCC) access units.
//!
//! Both walkers validate boundaries, classify NAL types and hand each NAL
//! slice to a caller closure in ONE traversal, allocating nothing themselves —
//! the packet hot path stays allocation-free (the Annex-B rewrite writes into
//! a caller-reserved scratch buffer; the already-length-prefixed and AAC paths
//! do no byte work at all).
//!
//! Boundary conventions mirror libavformat exactly (`nal.c`):
//! * a NAL ends at the next `00 00 01` triple, backing up one byte when the
//!   preceding byte is zero (4-byte start-code attribution);
//! * `trailing_zero_8bits` are trimmed from every NAL before it is emitted.

/// NAL unit types the strict tier cares about (H.264 table 7-1).
pub(crate) const NAL_IDR: u8 = 5;
pub(crate) const NAL_SPS: u8 = 7;
pub(crate) const NAL_PPS: u8 = 8;

/// Byte length of the AVCC length prefix the strict tier emits and accepts
/// (`lengthSizeMinusOne == 3`). Both FFmpeg construction paths (libx264
/// `set_avcc_extradata`, `ff_isom_write_avcc` for Annex-B input) hardcode it.
pub(crate) const NAL_LENGTH_SIZE: usize = 4;

/// What a NAL walk over one access unit observed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuScan {
    pub(crate) nal_count: usize,
    pub(crate) has_idr: bool,
    pub(crate) has_parameter_set: bool,
}

impl AuScan {
    fn note(&mut self, header: u8) {
        let nal_type = header & 0x1F;
        self.nal_count += 1;
        if nal_type == NAL_IDR {
            self.has_idr = true;
        }
        if nal_type == NAL_SPS || nal_type == NAL_PPS {
            self.has_parameter_set = true;
        }
    }
}

/// Walks an Annex-B byte stream NAL by NAL, invoking `on_nal` with each
/// trimmed NAL slice, and returns the accumulated scan.
///
/// Validation (identical to the previous materializing splitter): the payload
/// must begin with a start code (`(00)+ 01`), separators must be well-formed,
/// and a NAL that is empty (including empty after the trailing-zero trim) is
/// rejected.
pub(crate) fn walk_annexb<'a>(
    data: &'a [u8],
    mut on_nal: impl FnMut(&'a [u8]),
) -> Result<AuScan, String> {
    if data.len() < 4 {
        return Err(format!("Annex-B payload too short ({} bytes)", data.len()));
    }
    // Leading start code: (00)+ 01.
    let mut pos = 0;
    while pos < data.len() && data[pos] == 0 {
        pos += 1;
    }
    if pos < 2 || pos >= data.len() || data[pos] != 1 {
        return Err("payload does not begin with an Annex-B start code".to_string());
    }
    pos += 1;

    let mut scan = AuScan::default();
    loop {
        let boundary = find_startcode(data, pos).unwrap_or(data.len());
        // FFmpeg master's (n8.2+) nal_parse_units (libavformat/nal.c) trims
        // trailing_zero_8bits from every NAL before length-prefixing; this
        // trim matches it. FFmpeg 7.1/8.1 length-prefix the NAL unchanged,
        // carrying the padding into the sample. The divergence is
        // fixture-only: real encoders emit no trailing_zero_8bits.
        let mut end = boundary;
        while end > pos && data[end - 1] == 0 {
            end -= 1;
        }
        if end == pos {
            return Err("empty NAL unit".to_string());
        }
        scan.note(data[pos]);
        on_nal(&data[pos..end]);
        if !data[boundary..].iter().any(|&b| b != 0) {
            // Only trailing zeros remain: the stream ends after this NAL.
            break;
        }
        // Skip the separator start code ((00)+ 01; the trim above may have
        // folded this NAL's trailing zeros into the run before the 1).
        let mut next = boundary;
        while next < data.len() && data[next] == 0 {
            next += 1;
        }
        if next >= data.len() || data[next] != 1 {
            return Err("malformed start code between NAL units".to_string());
        }
        pos = next + 1;
        if pos >= data.len() {
            return Err("trailing start code without a NAL unit".to_string());
        }
    }
    Ok(scan)
}

/// First boundary at or after `from` where a start code begins. Mirrors
/// `ff_nal_find_startcode`: locate the next `00 00 01` triple, then back up by
/// exactly one byte when the preceding byte is zero (the leading zero of a
/// 4-byte start code belongs to the start code, not to the previous NAL).
fn find_startcode(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < 3 {
        return None;
    }
    let i = (from..data.len() - 2)
        .find(|&i| data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1)?;
    if i > from && data[i - 1] == 0 {
        Some(i - 1)
    } else {
        Some(i)
    }
}

/// Walks an already 4-byte length-prefixed AVCC access unit NAL by NAL,
/// invoking `on_nal` with each NAL slice, and returns the accumulated scan.
/// Every NAL must be fully contained; zero-length NAL units and trailing
/// garbage are rejected.
pub(crate) fn walk_length_prefixed<'a>(
    data: &'a [u8],
    mut on_nal: impl FnMut(&'a [u8]),
) -> Result<AuScan, String> {
    let mut scan = AuScan::default();
    let mut pos = 0usize;
    while pos < data.len() {
        if data.len() - pos < NAL_LENGTH_SIZE {
            return Err("truncated NAL length prefix".to_string());
        }
        let len = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            as usize;
        pos += NAL_LENGTH_SIZE;
        if len == 0 {
            return Err("zero-length NAL unit".to_string());
        }
        if data.len() - pos < len {
            return Err(format!(
                "NAL length {len} overruns the packet ({} bytes remain)",
                data.len() - pos
            ));
        }
        scan.note(data[pos]);
        on_nal(&data[pos..pos + len]);
        pos += len;
    }
    if scan.nal_count == 0 {
        return Err("packet contains no NAL units".to_string());
    }
    Ok(scan)
}

/// Appends one NAL as `u32 big-endian length + payload` to `out` — the write
/// half of the Annex-B rewrite, fed from [`walk_annexb`].
pub(crate) fn push_length_prefixed(nal: &[u8], out: &mut Vec<u8>) {
    debug_assert!(nal.len() <= u32::MAX as usize);
    out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
    out.extend_from_slice(nal);
}

#[cfg(test)]
pub(crate) fn collect_annexb(data: &[u8]) -> Result<Vec<&[u8]>, String> {
    let mut nals = Vec::new();
    walk_annexb(data, |n| nals.push(n))?;
    Ok(nals)
}

#[cfg(test)]
pub(crate) fn collect_length_prefixed(data: &[u8]) -> Result<Vec<&[u8]>, String> {
    let mut nals = Vec::new();
    walk_length_prefixed(data, |n| nals.push(n))?;
    Ok(nals)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPS: &[u8] = &[0x67, 66, 0xC0, 0x1E, 0xAC, 0xD9, 0x40];
    const PPS: &[u8] = &[0x68, 0xCE, 0x3C, 0x80];

    fn annexb_config() -> Vec<u8> {
        let mut v = vec![0, 0, 0, 1];
        v.extend_from_slice(SPS);
        v.extend_from_slice(&[0, 0, 1]);
        v.extend_from_slice(PPS);
        v
    }

    #[test]
    fn splits_three_and_four_byte_start_codes() {
        let config = annexb_config();
        let nals = collect_annexb(&config).unwrap();
        assert_eq!(nals, vec![SPS, PPS]);
    }

    #[test]
    fn rejects_garbage_prefix_and_empty_nals() {
        assert!(collect_annexb(&[0x12, 0, 0, 1, 0x67]).is_err());
        assert!(collect_annexb(&[0, 0, 1]).is_err());
        // Two adjacent start codes -> empty NAL.
        assert!(collect_annexb(&[0, 0, 1, 0, 0, 1, 0x41, 0x9A]).is_err());
    }

    #[test]
    fn converts_annexb_au_to_length_prefixed_in_one_walk() {
        let mut au = vec![0, 0, 0, 1, 0x65, 0x88, 0x80];
        au.extend_from_slice(&[0, 0, 1, 0x06, 0x05, 0xFF]);
        let mut out = Vec::new();
        let scan = walk_annexb(&au, |nal| push_length_prefixed(nal, &mut out)).unwrap();
        assert!(scan.has_idr);
        assert!(!scan.has_parameter_set);
        assert_eq!(scan.nal_count, 2);
        assert_eq!(
            out,
            vec![0, 0, 0, 3, 0x65, 0x88, 0x80, 0, 0, 0, 3, 0x06, 0x05, 0xFF]
        );
        // Round-trip: the produced AU walks back into the same NALs.
        let back = collect_length_prefixed(&out).unwrap();
        assert_eq!(back, collect_annexb(&au).unwrap());
    }

    /// FFmpeg-equivalence fixture: trailing_zero_8bits after a NAL (before a
    /// following start code and at stream end) are trimmed exactly like
    /// libavformat/nal.c before length-prefixing.
    #[test]
    fn trailing_zero_bytes_are_trimmed_like_ffmpeg() {
        // NAL [65 AA] + two trailing zeros + 3-byte startcode + NAL [06 05].
        let au = vec![0, 0, 0, 1, 0x65, 0xAA, 0, 0, 0, 0, 1, 0x06, 0x05];
        let nals = collect_annexb(&au).unwrap();
        assert_eq!(nals, vec![&[0x65u8, 0xAA][..], &[0x06u8, 0x05][..]]);
        let mut out = Vec::new();
        walk_annexb(&au, |nal| push_length_prefixed(nal, &mut out)).unwrap();
        assert_eq!(out, vec![0, 0, 0, 2, 0x65, 0xAA, 0, 0, 0, 2, 0x06, 0x05]);

        // Trailing zeros at stream end are trimmed from the final NAL too.
        let au = vec![0, 0, 0, 1, 0x65, 0xBB, 0, 0];
        let nals = collect_annexb(&au).unwrap();
        assert_eq!(nals, vec![&[0x65u8, 0xBB][..]]);

        // A NAL reduced to nothing by the trim is still an empty NAL.
        assert!(collect_annexb(&[0, 0, 0, 1, 0, 0]).is_err());
    }

    #[test]
    fn length_prefixed_walk_rejects_overruns() {
        assert!(collect_length_prefixed(&[0, 0, 0, 9, 0x65]).is_err());
        assert!(collect_length_prefixed(&[0, 0, 0, 0]).is_err());
        assert!(collect_length_prefixed(&[]).is_err());
        // Trailing partial prefix after a valid NAL.
        assert!(collect_length_prefixed(&[0, 0, 0, 1, 0x65, 0xFF]).is_err());
    }

    /// The hot-path walkers must not allocate: exercised here by walking with
    /// a counting closure only (no Vec materialization anywhere in the call).
    #[test]
    fn walkers_scan_without_materializing() {
        let mut au = vec![0, 0, 0, 1, 0x65, 0x88, 0x80];
        au.extend_from_slice(&[0, 0, 1, 0x06, 0x05, 0xFF]);
        let mut count = 0usize;
        let scan = walk_annexb(&au, |_| count += 1).unwrap();
        assert_eq!(count, scan.nal_count);
        let lp = vec![0, 0, 0, 1, 0x41];
        let mut count = 0usize;
        let scan = walk_length_prefixed(&lp, |_| count += 1).unwrap();
        assert_eq!((count, scan.nal_count), (1, 1));
    }
}
