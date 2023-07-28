use std::cmp::Ordering;
use std::convert::TryInto;
use std::sync::Arc;

use cfg_if::cfg_if;

use crate::sorted_table::Key;

use super::{DataEntry, SearchResult};

#[cfg(feature = "bloom-filters")]
use bloomfilter::Bloom;

#[cfg(feature = "wisckey")]
use super::ENTRY_LENGTH;

#[cfg(not(feature = "wisckey"))]
use super::DataLen;

#[cfg(feature = "bloom-filters")]
//TODO change the size of this depending on max_key_block_length
pub(super) const BLOOM_LENGTH: usize = 1024;

#[cfg(feature = "bloom-filters")]
pub(super) const BLOOM_KEY_NUM: usize = 1024;

#[cfg(feature = "bloom-filters")]
pub(super) const SIP_KEYS_LENGTH: usize = 4 * std::mem::size_of::<u64>();

/**
 * Data Layout
 * 1. 4 bytes marking where the restart list starts
 * 2. 4 bytes indicating the number of entries in this block
 * 3. 1024+32 bytes for the bloom filter (if enabled)
 * 4. Sequence of variable-length entries, where each entry is structured as listed below
 * 5. Variable length restart list (each entry is 4bytes; so we don't need length information)
 *
 * For WiscKey the layout of an entry is the following:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Variable length key suffix
 *  - Fixed size value reference, with
 *    - seq_number (8 bytes)
 *    - entry type (1 byte)
 *    - value block id
 *    - offset
 * Otherwise:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Variable length key suffix
 *  - Value length (8 bytes)
 *  - Entry Type (1 byte)
 *  - Sequence number (8 bytes)
 *  - Variable length value
 */
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

        let rls_len = std::mem::size_of::<u32>();
        let len_len = std::mem::size_of::<u32>();
        let restart_list_start = u32::from_le_bytes(data[..rls_len].try_into().unwrap()) as usize;
        let num_entries = u32::from_le_bytes(data[rls_len..rls_len + len_len].try_into().unwrap());

        #[cfg(feature = "bloom-filters")]
        let bloom_filter = {
            let filter = &data[rls_len + len_len..rls_len + len_len + BLOOM_LENGTH];
            let keys: [(u64, u64); 2] = bincode::deserialize(
                &data[rls_len + len_len + BLOOM_LENGTH..Self::header_length()],
            )
            .unwrap();

            Bloom::from_existing(
                filter,
                (BLOOM_LENGTH * 8) as u64,
                BLOOM_KEY_NUM as u32,
                keys,
            )
        };

        assert!(restart_list_start <= data.len(), "Data corrupted?");

        Self {
            data,
            num_entries,
            restart_list_start,
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
    /// The third entry in this result is the new offset after (or before) the entry
    #[tracing::instrument(skip(self_ptr, previous_key))]
    pub fn get_entry_at_offset(
        self_ptr: Arc<DataBlock>,
        offset: u32,
        previous_key: &[u8],
    ) -> (Key, DataEntry, u32) {
        let mut offset = (offset as usize) + Self::header_length();
        let len_len = std::mem::size_of::<u32>();

        assert!(offset < self_ptr.restart_list_start);

        let pkey_len =
            u32::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
        offset += len_len;

        let skey_len =
            u32::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
        offset += len_len;

        let kdata = [
            &previous_key[..pkey_len as usize],
            &self_ptr.data[offset..offset + (skey_len as usize)],
        ]
        .concat();
        offset += skey_len as usize;

        #[cfg(not(feature = "wisckey"))]
        let entry_len = {
            let len_len = std::mem::size_of::<DataLen>();
            let elen =
                DataLen::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
            offset += len_len;

            elen as usize
        };

        // WiscKey has constant-size entries
        #[cfg(feature = "wisckey")]
        let entry_len = ENTRY_LENGTH;

        let entry = DataEntry {
            block: self_ptr,
            offset,
            #[cfg(not(feature = "wisckey"))]
            length: entry_len,
        };

        offset += entry_len;
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
