//! Identity types of one H.264 configuration: the ordered parameter sets,
//! the identity-keyed S8 fingerprint and the derived codec projection.

/// Parsed parameter sets of one H.264 configuration, in original order.
/// avcC synthesis consumes this form (movenc preserves wrapper order); the
/// S8 comparison instead uses the [`ConfigFingerprint`] built during the
/// same parse, which keys the sets by parameter-set identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParameterSets {
    pub(crate) sps: Vec<Vec<u8>>,
    pub(crate) pps: Vec<Vec<u8>>,
}

/// The S8 fingerprint of one configuration: the EFFECTIVE parameter sets,
/// keyed by the identity a decoder stores them under. H.264 parameter sets
/// are addressed by id (`seq_parameter_set_id` / `pic_parameter_set_id`),
/// and a re-sent id REPLACES its predecessor — the sps_list/pps_list slot
/// overwrite in libavcodec/h264_ps.c — so what consumers can activate is
/// the last-wins id map, not the byte multiset. Two refinements make the
/// map track decoder state rather than delivery framing:
/// * values are the payloads BEHIND the one-byte NAL header: the header is
///   framing the reader consumes before the body is parsed and stored
///   (`h264_parse_nal_header`, libavcodec/h2645_parse.c), so two sets
///   differing only in `nal_ref_idc` land in identical stored state;
/// * every PPS slot also records the payload of the SPS generation the
///   PPS resolved: `ff_h264_decode_picture_parameter_set`
///   (libavcodec/h264_ps.c) binds the arriving PPS to the same-id SPS
///   live at that moment (the `PPS::sps` reference, libavcodec/h264_ps.h)
///   and derives PPS state from it, and a later same-id SPS replaces the
///   list slot, not that binding — so two configurations with equal id
///   maps still differ when a PPS bound a different SPS body on the way
///   there.
///
/// Two configurations are equal exactly when every id maps to the same
/// payload, every PPS bound the same SPS generation AND the record tail —
/// the profile-extension triple and the SPS-EXT list — agrees: the order
/// of DISTINCT ids never matters (wrapper bytes and array position
/// address nothing), a repeated payload-identical set collapses into its
/// slot and stays redundant, while reordering two same-id sets with
/// different payloads swaps which one is active and IS a configuration
/// change even though the byte set is unchanged. SPS-EXT bodies (the avcC
/// `sequenceParameterSetExtNALUnit` array) are part of the delivered
/// configuration and are fingerprinted in array order as post-header
/// payloads; their ids are not parsed, so adding, dropping or editing one
/// changes the fingerprint, while a header-only `nal_ref_idc` variant
/// stores identically and does not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConfigFingerprint {
    /// `seq_parameter_set_id` -> post-header payload of the last SPS
    /// carrying it, ascending by id.
    pub(super) sps: Vec<(u32, Vec<u8>)>,
    /// `pic_parameter_set_id` -> slot of the last PPS carrying it,
    /// ascending by id.
    pub(super) pps: Vec<(u32, PpsSlot)>,
    /// The profile-extension triple (`chroma_format_idc`,
    /// `bit_depth_luma`, `bit_depth_chroma`) in its writer-canonical form
    /// — the [`derived_extension`] triple — whichever accepted shape the
    /// validated bytes carried: a record may legally declare the writer
    /// default or the raw SPS syntax (both reach real files), and both
    /// canonicalize here so a shape switch between announcements is not a
    /// configuration change (`None` when the record legally ends at the
    /// PPS array). Two first SPS can share all three projection bytes yet
    /// differ here (chroma format and bit depth are not header bytes), so
    /// the id maps alone would miss the change.
    pub(super) extension: Option<(u8, u8, u8)>,
    /// Post-header payloads of the SPS-EXT entries in record order (an
    /// Annex-B configuration carries none — that path admits only SPS
    /// and PPS).
    pub(super) sps_ext: Vec<Vec<u8>>,
}

/// One PPS slot: the post-header payload of the PPS itself, then the
/// post-header payload of the SPS generation its `seq_parameter_set_id`
/// resolved when the PPS was parsed.
pub(super) type PpsSlot = (Vec<u8>, Vec<u8>);

impl ConfigFingerprint {
    /// Last-wins insertion: the slot for `id` is created at its
    /// ascending-id position or overwritten when already present.
    fn put<V>(slots: &mut Vec<(u32, V)>, id: u32, value: V) {
        match slots.binary_search_by_key(&id, |(slot, _)| *slot) {
            Ok(i) => slots[i].1 = value,
            Err(i) => slots.insert(i, (id, value)),
        }
    }

    pub(super) fn put_sps(&mut self, id: u32, nal: &[u8]) {
        Self::put(&mut self.sps, id, nal[1..].to_vec());
    }

    /// `bound_sps` is the full NAL of the SPS generation the PPS resolved.
    pub(super) fn put_pps(&mut self, id: u32, nal: &[u8], bound_sps: &[u8]) {
        Self::put(
            &mut self.pps,
            id,
            (nal[1..].to_vec(), bound_sps[1..].to_vec()),
        );
    }

    /// Whether `nal` is payload-identical to an ACTIVE set of the
    /// configuration (a replaced same-id predecessor is not active; the
    /// header byte is framing, so a `nal_ref_idc` difference alone does
    /// not make a set foreign).
    pub(super) fn has_active_sps(&self, nal: &[u8]) -> bool {
        self.sps
            .iter()
            .any(|(_, payload)| payload.as_slice() == &nal[1..])
    }

    pub(super) fn has_active_pps(&self, nal: &[u8]) -> bool {
        self.pps
            .iter()
            .any(|(_, (payload, _))| payload.as_slice() == &nal[1..])
    }
}

/// One parsed configuration: the ordered sets avcC synthesis and the codec
/// projection consume, plus the identity-keyed S8 fingerprint built during
/// the same parse.
#[derive(Debug)]
pub(crate) struct AvcConfig {
    pub(crate) sets: ParameterSets,
    pub(crate) fingerprint: ConfigFingerprint,
}

/// The derived codec projection of one H.264 configuration — the
/// profile / compatibility / level bytes the stream configuration exposes
/// (as fields and inside `avc1.PPCCLL`). Part of the composite S8 baseline:
/// the SAME derivation, from the SAME source (the ordered first SPS that
/// builds the delivered avcC), is compared against every `NEW_EXTRADATA`
/// announcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodecProjection {
    pub(crate) profile: u8,
    pub(crate) compatibility: u8,
    pub(crate) level: u8,
}

impl CodecProjection {
    /// Derives the projection from parsed sets (ordered form; the first SPS
    /// is the one avcC synthesis and `codec_string` use).
    pub(super) fn from_ordered_sets(sets: &ParameterSets) -> Result<Self, String> {
        let first_sps = sets.sps.first().ok_or("no SPS")?;
        if first_sps.len() < 4 {
            return Err(format!("SPS too short ({} bytes)", first_sps.len()));
        }
        Ok(Self {
            profile: first_sps[1],
            compatibility: first_sps[2],
            level: first_sps[3],
        })
    }

    /// The RFC 6381 codec string, `avc1.PPCCLL`.
    pub(crate) fn codec_string(&self) -> String {
        format!(
            "avc1.{:02X}{:02X}{:02X}",
            self.profile, self.compatibility, self.level
        )
    }
}
