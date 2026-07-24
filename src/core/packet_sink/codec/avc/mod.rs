//! H.264/AVC strict-tier codec runtime: configuration (avcC) handling and
//! per-packet access-unit normalization.
//!
//! Splits into two layers:
//! * record functions — parameter-set extraction from Annex-B or avcC
//!   wrappers (full structural validation of the record: reserved bits, NAL
//!   headers, the profile extension, trailing data and header/SPS
//!   consistency), avcC synthesis mirroring `ff_isom_write_avcc` (including
//!   the chroma/bit-depth extension), and the full SPS and PPS parses
//!   every extradata path routes each parameter-set body through;
//! * [`AvcRuntime`] — the per-stream state machine the orchestrator drives:
//!   payload normalization via the streaming NAL walkers (a census walk
//!   then a write walk for Annex-B input), IDR classification, S8
//!   parameter-set fingerprinting and the in-band policy.

mod fingerprint;
mod pps;
mod rbsp;
mod record;
mod runtime;
mod sps;

#[cfg(test)]
mod tests;

pub(crate) use fingerprint::{AvcConfig, CodecProjection, ConfigFingerprint, ParameterSets};
pub(crate) use record::{build_avcc, parse_avcc_parameter_sets, parse_parameter_sets};
pub(crate) use runtime::AvcRuntime;
