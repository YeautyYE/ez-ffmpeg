//! Checked iteration over `AVPacket` side data.
//!
//! Replaces raw pointer arithmetic at the use sites with one audited
//! boundary: entries are yielded as `(type, byte slice)` pairs, and an entry
//! whose `data`/`size` pair is inconsistent (null data with a nonzero size)
//! surfaces as a typed reason instead of a wild slice.

use ffmpeg_sys_next::{AVPacket, AVPacketSideDataType};

/// Iterator over a packet's side data entries.
pub(crate) struct SideDataIter<'a> {
    pkt: &'a AVPacket,
    index: i32,
}

/// Iterates `pkt`'s side data.
///
/// # Safety
/// - `pkt` must be a valid, non-null `AVPacket` whose
///   `side_data`/`side_data_elems` array is consistent and outlives the
///   returned iterator (each entry's `data`/`size` pair is read).
pub(crate) unsafe fn entries<'a>(pkt: *const AVPacket) -> SideDataIter<'a> {
    SideDataIter {
        pkt: &*pkt,
        index: 0,
    }
}

impl<'a> Iterator for SideDataIter<'a> {
    type Item = Result<(AVPacketSideDataType, &'a [u8]), String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.pkt.side_data_elems {
            return None;
        }
        // SAFETY: index < side_data_elems, upheld by the constructor contract.
        let entry = unsafe { &*self.pkt.side_data.add(self.index as usize) };
        self.index += 1;
        if entry.size == 0 {
            return Some(Ok((entry.type_, &[][..])));
        }
        if entry.data.is_null() {
            return Some(Err(format!(
                "side data entry {:?} has a null payload with size {}",
                entry.type_, entry.size
            )));
        }
        // SAFETY: non-null data of `size` bytes per the AVPacket contract.
        let bytes = unsafe { std::slice::from_raw_parts(entry.data, entry.size) };
        Some(Ok((entry.type_, bytes)))
    }
}
