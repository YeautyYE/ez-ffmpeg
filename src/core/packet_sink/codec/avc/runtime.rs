//! [`AvcRuntime`] — the per-stream state machine the orchestrator drives.

use super::{
    build_avcc, parse_avcc_parameter_sets, parse_parameter_sets, CodecProjection,
    ConfigFingerprint,
};
use crate::core::packet_sink::nal_framing::{
    push_length_prefixed, walk_annexb, walk_length_prefixed, NAL_LENGTH_SIZE, NAL_PPS, NAL_SPS,
};
use crate::error::PacketSinkError;

/// Per-stream H.264 runtime state.
pub(crate) struct AvcRuntime {
    /// Decided once, from the extradata form: Annex-B extradata means
    /// Annex-B packets (rewritten per packet), avcC extradata means already
    /// length-prefixed packets (validated in place). The forms never mix
    /// within one encoder.
    annexb_packets: bool,
    /// Composite S8 baseline, part 1: the identity-keyed fingerprint of
    /// the effective configuration ([`ConfigFingerprint`] — wrapper-
    /// independent, last-wins per parameter-set id).
    baseline: ConfigFingerprint,
    /// Composite S8 baseline, part 2: the derived codec projection the
    /// stream configuration exposes — same source as `on_stream_info`, so a
    /// reordering that changes what consumers were told (profile /
    /// compatibility / level) is a configuration change even when the set
    /// identities are unchanged.
    projection: CodecProjection,
}

impl AvcRuntime {
    /// Builds the runtime from finalized encoder extradata; returns the
    /// runtime plus the avcC to deliver in the stream configuration (a
    /// pre-existing avcC passes through after validation; Annex-B parameter
    /// sets are synthesized into one, preserving wrapper order like movenc,
    /// with the synthesized record reparsed to prove it reproduces the
    /// validated configuration).
    pub(crate) fn from_extradata(
        extradata: &[u8],
        stream_index: usize,
    ) -> Result<(Self, Vec<u8>, CodecProjection), PacketSinkError> {
        let invalid = |reason: String| PacketSinkError::InvalidExtradata {
            stream_index,
            reason,
        };
        let annexb_packets = extradata.first() != Some(&1);
        let config = parse_parameter_sets(extradata).map_err(&invalid)?;
        let projection = CodecProjection::from_ordered_sets(&config.sets).map_err(&invalid)?;
        // Header/SPS agreement needs no separate step here: a pass-through
        // record was already held against its own first SPS by
        // `check_avcc_consistency` inside the parse (bytes 1..4 plus the
        // profile-extension fields), and a synthesized record copies those
        // bytes from that same first SPS.
        let delivered = if annexb_packets {
            let record = build_avcc(&config.sets).map_err(&invalid)?;
            // Synthesis-fidelity gate. Annex-B resolves each PPS against
            // the last PRECEDING same-id SPS, while the record's arrays
            // put every SPS ahead of every PPS, so reparsing the record
            // binds each PPS to the last same-id SPS OVERALL. An ordering
            // that re-sends a PPS's SPS generation after the PPS therefore
            // synthesizes a record describing a different configuration —
            // or one whose PPS no longer parses under the rebound SPS at
            // all. The delivered record must mean exactly the
            // configuration validated above, so it is reparsed through the
            // same avcC parser and its identity fingerprint (bindings and
            // the extension triple included) must agree. Single-generation
            // orderings with each
            // PPS after its SPS — every real encoder handoff — bind
            // identically on both reads and pass unchanged.
            let reparsed = parse_avcc_parameter_sets(&record).map_err(|reason| {
                invalid(format!(
                    "the parameter-set interleaving has no faithful avcC \
                     representation (the synthesized record fails to \
                     reparse: {reason})"
                ))
            })?;
            if reparsed.fingerprint != config.fingerprint {
                return Err(invalid(
                    "the parameter-set interleaving has no faithful avcC \
                     representation (reparsing the synthesized record binds \
                     a PPS to a different SPS generation)"
                        .to_string(),
                ));
            }
            record
        } else {
            extradata.to_vec()
        };
        Ok((
            Self {
                annexb_packets,
                baseline: config.fingerprint,
                projection,
            },
            delivered,
            projection,
        ))
    }

    /// S8: a `NEW_EXTRADATA` announcement — one whose EFFECTIVE
    /// configuration (the identity-keyed [`ConfigFingerprint`]) is
    /// unchanged is redundant and passes; anything else is a mid-stream
    /// configuration change.
    ///
    /// An avcC-form announcement passes through the same full record
    /// validation as initial construction (`parse_avcc_record` via
    /// [`parse_parameter_sets`]), including the header-vs-first-SPS
    /// consistency check — so tampered header bytes over unchanged SPS/PPS
    /// fail here rather than slipping past the set-identity comparison.
    pub(crate) fn check_new_extradata(
        &self,
        bytes: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let config = parse_parameter_sets(bytes).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("invalid NEW_EXTRADATA ({reason})"),
            }
        })?;
        // Composite baseline, part 2: the derived projection consumers were
        // told (profile/compatibility/level, from the announcement's own
        // ordered first SPS — the same derivation on_stream_info used). A
        // reorder that changes it IS a configuration change even when the
        // effective identity map below is unchanged.
        let projection = CodecProjection::from_ordered_sets(&config.sets).map_err(|reason| {
            PacketSinkError::ConfigChange {
                stream_index,
                what: format!("invalid NEW_EXTRADATA SPS ({reason})"),
            }
        })?;
        if projection != self.projection {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "derived codec projection changed ({} -> {})",
                    self.projection.codec_string(),
                    projection.codec_string()
                ),
            });
        }
        // Composite baseline, part 1: the effective identity map (and the
        // SPS-EXT list) must be unchanged.
        if config.fingerprint != self.baseline {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "NEW_EXTRADATA changes the effective parameter sets".to_string(),
            });
        }
        Ok(())
    }

    /// Payload normalization: validates NAL boundaries, classifies types
    /// and (for Annex-B input) rewrites into `scratch` as a 4-byte
    /// length-prefixed access unit in ONE linear walk — a strict arithmetic
    /// bound reserves the output up front, so the walk never reallocates.
    /// The already-length-prefixed path is a single validate-only pass,
    /// zero-copy.
    ///
    /// Returns `(is_key, payload)` where `is_key` is IDR presence and
    /// `payload` borrows either `scratch` or the input.
    pub(crate) fn normalize_au<'a>(
        &self,
        payload: &'a [u8],
        scratch: &'a mut Vec<u8>,
        stream_index: usize,
    ) -> Result<(bool, &'a [u8]), PacketSinkError> {
        let malformed = |reason: String| PacketSinkError::MalformedPacket {
            stream_index,
            reason,
        };
        let (scan, data): (_, &'a [u8]) = if self.annexb_packets {
            scratch.clear();
            // Strict output bound, no census walk needed: the output is
            // sum(4 + nal.len()) over the AU's NALs. Every NAL costs at
            // least 4 input bytes (a >= 3-byte start code plus >= 1 payload
            // byte), so nal_count <= payload.len() / 4, and the NAL bytes
            // themselves are a subset of the input; the +NAL_LENGTH_SIZE
            // absorbs the division truncation. Reserving this bound keeps
            // the write walk reallocation-free — the property the old
            // exact-size census walk existed to provide — at the price of
            // up to 25% over-reserve on the reused scratch buffer.
            scratch.reserve(payload.len() + payload.len() / 4 + NAL_LENGTH_SIZE);
            let scan = walk_annexb(payload, |nal| push_length_prefixed(nal, scratch))
                .map_err(&malformed)?;
            (scan, scratch.as_slice())
        } else {
            let scan = walk_length_prefixed(payload, |_| {}).map_err(malformed)?;
            (scan, payload)
        };
        if scan.has_parameter_set {
            // Cold path: collect the in-band sets for the S8 comparison
            // (differing sets are a config change; value-equal sets are still
            // rejected — strict-tier configuration stays out-of-band).
            self.check_inband_parameter_sets(data, stream_index)?;
            return Err(PacketSinkError::InBandParameterSets { stream_index });
        }
        Ok((scan.has_idr, data))
    }

    /// In-band SPS/PPS: sets differing from the ACTIVE baseline (a
    /// replaced same-id predecessor is no longer active) are a
    /// configuration change; value-equal sets fall through to the
    /// strict-tier in-band rejection at the caller. `data` is
    /// length-prefixed (post-normalization).
    fn check_inband_parameter_sets(
        &self,
        data: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        let mut mismatch = false;
        let _ = walk_length_prefixed(data, |nal| {
            let matches_baseline = match nal[0] & 0x1F {
                NAL_SPS => self.baseline.has_active_sps(nal),
                NAL_PPS => self.baseline.has_active_pps(nal),
                _ => return,
            };
            if !matches_baseline {
                mismatch = true;
            }
        });
        if mismatch {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "in-band SPS/PPS differ from the stream configuration".to_string(),
            });
        }
        Ok(())
    }
}
