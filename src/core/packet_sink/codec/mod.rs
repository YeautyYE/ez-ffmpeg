//! Strict-tier codec runtimes. One variant per supported codec; a future
//! HEVC entry (hvcC configuration, IRAP classification) adds a file and an
//! arm here without touching the orchestrator's control flow.

pub(crate) mod aac;
pub(crate) mod avc;

use crate::error::PacketSinkError;

/// Codec-specific handling of one output stream.
pub(crate) enum CodecRuntime {
    Avc(avc::AvcRuntime),
    Aac(aac::AacRuntime),
}

impl CodecRuntime {
    /// S8 `NEW_EXTRADATA` comparison against this stream's baseline.
    pub(crate) fn check_new_extradata(
        &self,
        bytes: &[u8],
        stream_index: usize,
    ) -> Result<(), PacketSinkError> {
        match self {
            CodecRuntime::Avc(avc) => avc.check_new_extradata(bytes, stream_index),
            CodecRuntime::Aac(aac) => aac.check_new_extradata(bytes, stream_index),
        }
    }
}
