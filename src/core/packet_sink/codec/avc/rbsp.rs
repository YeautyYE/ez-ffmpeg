//! RBSP primitives shared by the SPS and PPS parses: emulation-prevention
//! stripping and the MSB-first bit reader with Exp-Golomb support.

/// Strips emulation prevention bytes (`00 00 03` -> `00 00`) from a NAL
/// payload (header byte excluded), yielding the RBSP (H.264 7.4.1.1).
pub(super) fn unescape_rbsp(payload: &[u8]) -> Vec<u8> {
    let mut rbsp = Vec::with_capacity(payload.len());
    let mut zeros = 0u32;
    for &b in payload {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        if b == 0 {
            zeros += 1;
        } else {
            zeros = 0;
        }
        rbsp.push(b);
    }
    rbsp
}

/// MSB-first bit reader over an RBSP with Exp-Golomb support.
pub(super) struct BitReader<'a> {
    data: &'a [u8],
    pos: usize, // bit position
}

impl<'a> BitReader<'a> {
    pub(super) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(super) fn bits(&mut self, n: u32) -> Result<u32, String> {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = self
                .data
                .get(self.pos / 8)
                .ok_or_else(|| "RBSP truncated".to_string())?;
            let bit = (byte >> (7 - (self.pos % 8))) & 1;
            v = (v << 1) | bit as u32;
            self.pos += 1;
        }
        Ok(v)
    }

    /// `more_rbsp_data()` (H.264 7.2): syntax data remains exactly when the
    /// current position is strictly before the last set bit of the RBSP —
    /// that bit being the rbsp_stop_one_bit. A remainder with no set bit at
    /// all reports false and leaves the malformed trailing bits to
    /// [`Self::finish_rbsp`].
    pub(super) fn more_rbsp_data(&self) -> bool {
        for (i, &b) in self.data.iter().enumerate().rev() {
            if b != 0 {
                let last_set = i * 8 + 7 - b.trailing_zeros() as usize;
                return self.pos < last_set;
            }
        }
        false
    }

    /// Unsigned Exp-Golomb.
    pub(super) fn ue(&mut self) -> Result<u32, String> {
        let mut zeros = 0u32;
        while self.bits(1)? == 0 {
            zeros += 1;
            if zeros > 31 {
                return Err("invalid Exp-Golomb code".to_string());
            }
        }
        if zeros == 0 {
            return Ok(0);
        }
        let rest = self.bits(zeros)?;
        Ok((1u32 << zeros) - 1 + rest)
    }

    /// Signed Exp-Golomb (H.264 9.1.1): code number k maps to
    /// (-1)^(k+1) * ceil(k / 2). Widened to i64 because k can reach
    /// 2^32 - 2.
    pub(super) fn se(&mut self) -> Result<i64, String> {
        let k = self.ue()? as i64;
        Ok(if k % 2 == 1 { (k + 1) / 2 } else { -(k / 2) })
    }

    /// `rbsp_trailing_bits()` (H.264 7.3.2.11) plus the end-of-data
    /// requirement: the stop bit must be 1, the alignment bits zero, and no
    /// bytes may remain — an SPS RBSP ends at its trailing bits, so leftover
    /// bytes are foreign data. FFmpeg's decode path does not verify this on
    /// every route (`ff_h264_decode_seq_parameter_set` returns once the
    /// fields it stores are read); a validator confirms the declared syntax
    /// is exactly what the payload carries.
    pub(super) fn finish_rbsp(&mut self) -> Result<(), String> {
        if self.bits(1)? != 1 {
            return Err("rbsp_trailing_bits stop bit is 0".to_string());
        }
        // `%` (not `is_multiple_of`) keeps this MSRV-1.80 safe;
        // `usize::is_multiple_of` is only stable since 1.87.
        #[allow(clippy::manual_is_multiple_of)]
        while self.pos % 8 != 0 {
            if self.bits(1)? != 0 {
                return Err("nonzero rbsp_trailing_bits alignment bit".to_string());
            }
        }
        let left = self.data.len() - self.pos / 8;
        if left != 0 {
            return Err(format!("{left} byte(s) after rbsp_trailing_bits"));
        }
        Ok(())
    }
}
