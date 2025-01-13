use std::cmp::Ordering;
use std::sync::Arc;

use crate::sorted_table::Key;

use super::{DataEntry, SearchResult};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[cfg(feature = "bloom-filters")]
use bloomfilter::Bloom;

#[cfg(feature = "wisckey")]
use crate::values::{ValueBatchId, ValueOffset};

#[cfg(feature = "bloom-filters")]
//TODO change the size of this depending on max_key_block_length
pub(super) const BLOOM_LENGTH: usize = 1024;

#[cfg(feature = "bloom-filters")]
pub(super) const BLOOM_ITEM_COUNT: usize = 1024;

#[cfg(feature = "bloom-filters")]
/// Taken from https://github.com/jedisct1/rust-bloom-filter/blob/6b93b922be474998514b696dc84333d6c04ed991/src/bitmap.rs#L5
pub(super) const BLOOM_HEADER_SIZE: usize = 1 + 8 + 4 + 32;

/**
 * Layout of a data block on disk
 *
 * 1. 4 bytes marking where the restart list starts
 * 2. 4 bytes indicating the number of entries in this block
 * 3. 1024+32 bytes for the bloom filter (if enabled)
 * 4. Sequence of variable-length entries
 * 5. Variable length restart list (each entry is 4bytes; so we don't need length information)
 */
#[derive(IntoBytes, Immutable, KnownLayout, FromBytes)]
#[repr(C, packed)]
pub(super) struct DataBlockHeader {
    pub(super) restart_list_start: u32,
    pub(super) number_of_entries: u32,
    #[cfg(feature = "bloom-filters")]
    pub(super) bloom_filter: [u8; BLOOM_LENGTH + BLOOM_HEADER_SIZE],
}

/**
 * For WiscKey an entry contains:
 *
 * Header:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Seq_number (8 bytes)
 *  - Entry type (1 byte)
 *  - Value reference (batch id and offset)
 *
 * Content (not part of the header):
 *  - Variable length key suffix
 *
 * When not using WiscKey an entry is variable length and contains the following
 *
 * Header:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Value length (8 bytes)
 *  - Entry Type (1 byte)
 *  - Sequence number (8 bytes)
 *
 * Content (not part of the header):
 *  - Variable length key suffix
 *  - Variable length value
 */
#[derive(IntoBytes, Immutable, FromBytes, KnownLayout)]
#[repr(C, packed)]
pub(super) struct EntryHeader {
    pub(super) prefix_len: u32,
    pub(super) suffix_len: u32,
    pub(super) entry_type: u8,
    pub(super) seq_number: u64,
    #[cfg(feature = "wisckey")]
    pub(super) value_batch: ValueBatchId,
    #[cfg(feature = "wisckey")]
    pub(super) value_offset: ValueOffset,
    #[cfg(not(feature = "wisckey"))]
    pub(super) value_length: u64,
}

//TODO support data block layouts without prefixed keys
pub struct DataBlock {
    pub(super) restart_list_start: usize,
    pub(super) num_entries: u32,
    pub(super) restart_interval: u32,
    pub(super) data: Vec<u8>,
    #[cfg(feature = "bloom-filters")]
    pub(super) bloom_filter: Bloom<[u8]>,
}

impl DataBlock {
    pub fn new_from_data(data: Vec<u8>, restart_interval: u32) -> Self {
        assert!(!data.is_empty(), "No data?");

        let header = DataBlockHeader::ref_from_bytes(&data[..Self::header_length()]).unwrap();

        #[cfg(feature = "bloom-filters")]
        let bloom_filter = Bloom::from_bytes(header.bloom_filter.as_slice().to_vec())
            .expect("Failed to load bloom filter");

        log::trace!("Created new data block from existing data");

        Self {
            num_entries: header.number_of_entries,
            restart_list_start: header.restart_list_start as usize,
            data,
            restart_interval,
            #[cfg(feature = "bloom-filters")]
            bloom_filter,
        }
    }

    fn header_length() -> usize {
        std::mem::size_of::<DataBlockHeader>()
    }

    /// Get the key and entry at the specified offset in bytes (must be valid!)
    /// The third entry in this result is the new offset after the entry
    #[tracing::instrument(skip(self_ptr, previous_key))]
    pub fn get_entry_at_offset(
        self_ptr: Arc<DataBlock>,
        offset: u32,
        previous_key: &[u8],
    ) -> (Key, DataEntry) {
        let mut offset = (offset as usize) + Self::header_length();

        let header_len = std::mem::size_of::<EntryHeader>();

        if offset + header_len > self_ptr.restart_list_start {
            panic!("Invalid offset {offset}");
        }

        let header = EntryHeader::ref_from_bytes(&self_ptr.data[offset..offset + header_len])
            .expect("Failed to read entry header");
        let entry_offset = offset;

        offset += std::mem::size_of::<EntryHeader>();

        let kdata = [
            &previous_key[..(header.prefix_len as usize)],
            &self_ptr.data[offset..offset + (header.suffix_len as usize)],
        ]
        .concat();
        offset += header.suffix_len as usize;

        // Move offset to after the entry
        #[cfg(not(feature = "wisckey"))]
        {
            offset += header.value_length as usize;
        }

        let next_offset = offset - Self::header_length();

        let entry = DataEntry {
            block: self_ptr,
            offset: entry_offset,
            next_offset: next_offset as u32,
        };

        (kdata, entry)
    }

    /// How many entries are in this data block?
    pub fn get_num_entries(&self) -> u32 {
        self.num_entries
    }

    /// Get they entry at the specified index
    /// (the index is in entries not bytes)
    #[tracing::instrument(skip(self_ptr))]
    pub fn get_entry_at_index(self_ptr: &Arc<Self>, index: u32) -> (Key, DataEntry) {
        // First, get the closest restart offset
        let restart_pos = index / self_ptr.restart_interval;

        let restart_offset = self_ptr.get_restart_offset(restart_pos);
        let (mut key, mut entry) = Self::get_entry_at_offset(self_ptr.clone(), restart_offset, &[]);

        let mut current_idx = restart_pos * self_ptr.restart_interval;

        while current_idx < index {
            (key, entry) =
                Self::get_entry_at_offset(self_ptr.clone(), entry.get_next_offset(), &key);
            current_idx += 1;
        }

        (key, entry)
    }

    /// Length of this block in bytes without the header and restart list
    pub fn byte_len(&self) -> u32 {
        // "Cut-off" the beginning and end
        let rl_len = self.data.len() - self.restart_list_start;
        (self.data.len() - Self::header_length() - rl_len) as u32
    }

    #[inline(always)]
    fn restart_list_len(&self) -> usize {
        let offset_len = std::mem::size_of::<u32>();
        let rl_len = self.data.len() - self.restart_list_start;

        assert!(rl_len % offset_len == 0);
        rl_len / offset_len
    }

    /// Get get byte offset of a restart entry
    #[inline(always)]
    fn get_restart_offset(&self, pos: u32) -> u32 {
        let offset_len = std::mem::size_of::<u32>();
        let pos = self.restart_list_start + (pos as usize) * offset_len;

        u32::from_le_bytes(self.data[pos..pos + offset_len].try_into().unwrap())
            - Self::header_length() as u32
    }

    #[tracing::instrument(skip(self_ptr, key))]
    fn binary_search(self_ptr: &Arc<Self>, key: &[u8]) -> SearchResult {
        let rl_len = self_ptr.restart_list_len();

        let mut start: u32 = 0;
        let mut end = (rl_len as u32) - 1;

        // binary search
        while end - start > 1 {
            let mid = start + (end - start) / 2;

            // We always perform the search at the restart positions for efficiency
            let offset = self_ptr.get_restart_offset(mid);
            let (this_key, entry) = Self::get_entry_at_offset(self_ptr.clone(), offset, &[]);

            match this_key.as_slice().cmp(key) {
                Ordering::Equal => {
                    // Exact match
                    return SearchResult::ExactMatch(entry);
                }
                Ordering::Less => {
                    // continue with right half
                    start = mid;
                }
                Ordering::Greater => {
                    // continue with left half
                    end = mid;
                }
            }
        }

        // There is no reset at the very end so we need to include
        // that part in the sequential search
        let end = if end + 1 == rl_len as u32 {
            self_ptr.byte_len()
        } else {
            self_ptr.get_restart_offset(end)
        };

        SearchResult::Range(start, end)
    }

    /// Get the entry for the specified key
    /// Will return None if no such entry exists
    #[tracing::instrument(skip(self_ptr, key))]
    pub fn get_by_key(self_ptr: &Arc<Self>, key: &[u8]) -> Option<DataEntry> {
        #[cfg(feature = "bloom-filters")]
        if !self_ptr.bloom_filter.check(key) {
            return None;
        }

        let (start, end) = match Self::binary_search(self_ptr, key) {
            SearchResult::ExactMatch(entry) => {
                return Some(entry);
            }
            SearchResult::Range(start, end) => (start, end),
        };

        let mut pos = self_ptr.get_restart_offset(start);

        let mut last_key = vec![];
        while pos < end {
            let (this_key, entry) = Self::get_entry_at_offset(self_ptr.clone(), pos, &last_key);

            if key == this_key {
                return Some(entry);
            }

            pos = entry.get_next_offset();
            last_key = this_key;
        }

        // Not found
        None
    }
}
