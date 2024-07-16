use std::cmp::Ordering;
use std::sync::Arc;

use cfg_if::cfg_if;

use rkyv::{Archive, Deserialize, Serialize};

use crate::sorted_table::Key;

use super::{DataEntry, SearchResult};

#[cfg(feature = "bloom-filters")]
use bloomfilter::Bloom;

#[cfg(feature = "wisckey")]
use crate::data_blocks::ValueId;

#[cfg(feature = "bloom-filters")]
//TODO change the size of this depending on max_key_block_length
pub(super) const BLOOM_LENGTH: usize = 1024;

#[cfg(feature = "bloom-filters")]
pub(super) const BLOOM_KEY_NUM: usize = 1024;

#[cfg(feature = "bloom-filters")]
pub(super) const SIP_KEYS_LENGTH: usize = 4 * std::mem::size_of::<u64>();

/**
 * Layout of a data block on disk
 *
 * 1. 4 bytes marking where the restart list starts
 * 2. 4 bytes indicating the number of entries in this block
 * 3. 1024+32 bytes for the bloom filter (if enabled)
 * 4. Sequence of variable-length entries
 * 5. Variable length restart list (each entry is 4bytes; so we don't need length information)
 */
#[derive(Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub(super) struct DataBlockHeader {
    pub(super) restart_list_start: u32,
    pub(super) number_of_entries: u32,
    #[cfg(feature = "bloom-filters")]
    pub(super) bloom_filter: [u8; 1024],
    #[cfg(feature = "bloom-filters")]
    pub(super) bloom_filter_keys: [(u64, u64); 2],
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
#[derive(Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
pub(super) struct EntryHeader {
    pub(super) prefix_len: u32,
    pub(super) suffix_len: u32,
    pub(super) entry_type: u8,
    pub(super) seq_number: u64,
    #[cfg(feature = "wisckey")]
    pub(super) value_ref: ValueId,
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

        let header_slice = &data[..std::mem::size_of::<ArchivedDataBlockHeader>()];
        let header =
            rkyv::access::<ArchivedDataBlockHeader, rkyv::rancor::Error>(header_slice).unwrap();

        #[cfg(feature = "bloom-filters")]
        let bloom_filter = Bloom::from_existing(
            header.bloom_filter.as_slice(),
            (BLOOM_LENGTH * 8) as u64,
            BLOOM_KEY_NUM as u32,
            rkyv::deserialize::<_, _, rkyv::rancor::Error>(&header.bloom_filter_keys, &mut ())
                .unwrap(),
        );

        log::trace!("Created new data block from existing data");

        Self {
            num_entries: header.number_of_entries.to_native(),
            restart_list_start: header.restart_list_start.to_native() as usize,
            data,
            restart_interval,
            #[cfg(feature = "bloom-filters")]
            bloom_filter,
        }
    }

    fn header_length() -> usize {
        let rl_len_len = std::mem::size_of::<u32>();
        let len_len = std::mem::size_of::<u32>();

        cfg_if! {
            if #[cfg(feature="bloom-filters")] {
                len_len + rl_len_len + BLOOM_LENGTH + SIP_KEYS_LENGTH
            } else {
                len_len + rl_len_len
            }
        }
    }

    /// Get the key and entry at the specified offset in bytes (must be valid!)
    /// The third entry in this result is the new offset after the entry
    #[tracing::instrument(skip(self_ptr, previous_key))]
    pub fn get_entry_at_offset(
        self_ptr: Arc<DataBlock>,
        offset: u32,
        previous_key: &[u8],
    ) -> (Key, DataEntry, u32) {
        println!("PRE {offset}");
        let mut offset = crate::align_offset((offset as usize) + Self::header_length());
        if offset >= self_ptr.restart_list_start {
            panic!("Invalid offset {offset}");
        }
        println!("POST {offset}");
 
        let header_slice =
            &self_ptr.data[offset..offset + std::mem::size_of::<ArchivedEntryHeader>()];
        let archived_header =
            rkyv::access::<ArchivedEntryHeader, rkyv::rancor::Error>(header_slice)
                .expect("Failed to read entry");
        let header: EntryHeader =
            rkyv::deserialize::<_, _, rkyv::rancor::Error>(archived_header, &mut ())
                .expect("Failed to deserialize header");

        offset += std::mem::size_of::<ArchivedEntryHeader>();

        let kdata = [
            &previous_key[..(header.prefix_len as usize)],
            &self_ptr.data[offset..offset + (header.suffix_len as usize)],
        ]
        .concat();
        offset += header.suffix_len as usize;

        let entry = DataEntry {
            header,

            #[cfg(not(feature = "wisckey"))]
            offset,
            #[cfg(not(feature = "wisckey"))]
            block: self_ptr,
        };

        // Move offset to after the entry
        #[cfg(not(feature="wisckey"))] {
            offset += entry.header.value_length as usize;
        }

        offset -= Self::header_length();

        (kdata, entry, offset as u32)
    }

    /// How many entries are in this data block?
    pub fn get_num_entries(&self) -> u32 {
        self.num_entries
    }

    /// Get they entry at the specified index
    /// (the index is in entries not bytes)
    #[tracing::instrument(skip(self_ptr))]
    pub fn get_entry_at_index(self_ptr: Arc<DataBlock>, index: u32) -> (Key, DataEntry) {
        // First, get the closest restart offset
        let restart_pos = index / self_ptr.restart_interval;

        let restart_offset = self_ptr.get_restart_offset(restart_pos);
        let (mut key, mut entry, mut next_offset) =
            Self::get_entry_at_offset(self_ptr.clone(), restart_offset, &[]);

        let mut current_idx = restart_pos * self_ptr.restart_interval;

        while current_idx < index {
            (key, entry, next_offset) =
                Self::get_entry_at_offset(self_ptr.clone(), next_offset, &key);
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
    fn binary_search(self_ptr: &Arc<DataBlock>, key: &[u8]) -> SearchResult {
        let rl_len = self_ptr.restart_list_len();

        let mut start: u32 = 0;
        let mut end = (rl_len as u32) - 1;

        // binary search
        while end - start > 1 {
            let mid = start + (end - start) / 2;

            // We always perform the search at the restart positions for efficiency
            let offset = self_ptr.get_restart_offset(mid);
            let (this_key, entry, _) = Self::get_entry_at_offset(self_ptr.clone(), offset, &[]);

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
    pub fn get(self_ptr: &Arc<DataBlock>, key: &[u8]) -> Option<DataEntry> {
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
            let (this_key, entry, new_pos) =
                Self::get_entry_at_offset(self_ptr.clone(), pos, &last_key);

            if key == this_key {
                return Some(entry);
            }

            pos = new_pos;
            last_key = this_key;
        }

        // Not found
        None
    }
}
