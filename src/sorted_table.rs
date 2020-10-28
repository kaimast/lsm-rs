use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use crate::entry::Entry;
use crate::values::ValueId;
use crate::data_blocks::{PrefixedKey, DataBlockId, DataBlocks};

pub trait Key = Ord+Serialize+DeserializeOwned+Send+Sync+Clone;

const BLOCK_SIZE: usize = 32*1024;

pub struct SortedTable<K: Key> {
    min: K,
    max: K,
    size: usize,
    //TODO add index,
    block_ids: Vec<DataBlockId>,
    data_blocks: Arc<DataBlocks>
}

pub struct TableIterator<'a, K: Key> {
    block_pos: usize,
    block_offset: usize,
    key: Vec<u8>,
    entry: Entry,
    table: &'a SortedTable<K>
}

impl<'a, K: Key> TableIterator<'a, K> {
    fn new(table: &'a SortedTable<K>) -> Self {
        let last_key = vec![];
        let block_id = table.block_ids[0];
        let first_block = table.data_blocks.get_block(&block_id);
        let (key, entry) = first_block.get_offset(0, &last_key);

        // Are we already at the end of the first block?
        let (block_pos, block_offset) = if first_block.len() == 1 {
            (1, 0)
        } else {
            (0, 1)
        };

        Self{ key, entry, block_pos, block_offset, table }
    }

    pub fn at_end(&self) -> bool {
        self.block_pos >= self.table.block_ids.len()
    }

    pub fn get_key(&self) -> K {
        bincode::deserialize(&self.key).unwrap()
    }

    pub fn get_entry(&self) -> Entry {
        self.entry.clone()
    }

    pub fn step(&mut self) {
        let block_id = self.table.block_ids[self.block_pos];
        let block = self.table.data_blocks.get_block(&block_id);

        let (key, entry) = block.get_offset(self.block_offset, &self.key);
        self.key = key;
        self.entry = entry;

        self.block_offset += 1;

        // At the end of the block?
        if self.block_offset >= block.len() {
            self.block_pos += 1;
            self.block_offset = 0;
        }
    }
}

impl<K: Key> SortedTable<K> {
    pub fn new(mut entries: Vec<(K, Entry)>, data_blocks: Arc<DataBlocks>) -> Self {
        if entries.is_empty() {
            panic!("Cannot create empty table");
        }

        entries.sort_by(|elem1, elem2| -> std::cmp::Ordering {
            if elem1.0 == elem2.0 {
                // If keys are equal, sort by sequence number
                elem1.1.seq_number.cmp(&elem2.1.seq_number)
            } else {
                elem1.0.cmp(&elem2.0)
            }
        });

        let min = entries[0].0.clone();
        let max = entries[entries.len()-1].0.clone();

        Self::new_from_sorted(entries, min, max, data_blocks)
    }

    /// Create a table from an already sorted set of entries
    pub fn new_from_sorted(mut entries: Vec<(K, Entry)>, min: K, max: K, data_blocks: Arc<DataBlocks>) -> Self {
        let mut block_ids = Vec::new();
        let mut prefixed_entries = Vec::new();
        let mut last_kdata = vec![];
        let mut block_size = 0;
        let mut size = 0;

        for (key, entry) in entries.drain(..) {
            let kdata = bincode::serialize(&key).expect("Failed to serialize key");
            let mut prefix_len = 0;

            while prefix_len < kdata.len() && prefix_len < last_kdata.len()
                && kdata[prefix_len] == last_kdata[prefix_len] {
                prefix_len += 1;
            }

            let suffix = kdata[prefix_len..].to_vec();
            let this_size = std::mem::size_of::<PrefixedKey>() + std::mem::size_of::<Entry>();
            block_size += this_size;
            size += this_size;

            let pkey = PrefixedKey::new(prefix_len, suffix);
            prefixed_entries.push((pkey, entry));

            last_kdata = kdata;

            if block_size >= BLOCK_SIZE {
                let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries));
                block_ids.push(id);

                block_size = 0;
                last_kdata.clear();
            }
        }

        if block_size > 0 {
            let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries));
            block_ids.push(id);
        }

        Self{ size, block_ids, data_blocks, min, max }
    }

    #[inline]
    pub fn iter(&self) -> TableIterator<K> {
        TableIterator::new(&self)
    }

    // Get the size of this table (in bytes)
    #[inline]
    pub fn get_size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn get_min(&self) -> &K {
        &self.min
    }

    #[inline]
    pub fn get_max(&self) -> &K {
        &self.max
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        if key < self.get_min() || key > self.get_max() {
            return None;
        }

        for block_id in self.block_ids.iter() {
            let block = self.data_blocks.get_block(&block_id);

            if let Some(vid) = block.get(key) {
                return Some(vid);
            }
        }

        None
    }

    #[inline]
    pub fn overlaps(&self, other: &SortedTable<K>) -> bool {
        self.get_max() >= other.get_min() && self.get_min() <= other.get_max()
    }
}
