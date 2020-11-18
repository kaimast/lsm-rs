use std::sync::Arc;

use crate::entry::Entry;
use crate::values::ValueId;
use crate::data_blocks::{PrefixedKey, DataBlockId, DataBlocks};

pub type Key = Vec<u8>;

const BLOCK_SIZE: usize = 32*1024;

pub struct SortedTable {
    min: Key,
    max: Key,
    size: usize,
    block_index: Vec<Key>,
    block_ids: Vec<DataBlockId>,
    data_blocks: Arc<DataBlocks>
}

pub trait InternalIterator {
    fn at_end(&self) -> bool;
    fn step(&mut self);
    fn get_key(&self) -> &Key;
    fn get_entry(&self) -> &Entry;
}

pub struct TableIterator {
    block_pos: usize,
    block_offset: usize,
    key: Key,
    entry: Entry,
    table: Arc<SortedTable>
}


impl TableIterator {
    pub fn new(table: Arc<SortedTable>) -> Self {
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
}

impl InternalIterator for TableIterator {
    fn at_end(&self) -> bool {
        self.block_pos >= self.table.block_ids.len()
    }

    fn get_key(&self) -> &Key {
        &self.key
    }

    fn get_entry(&self) -> &Entry {
        &self.entry
    }

    fn step(&mut self) {
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

impl SortedTable {
    pub fn new(mut entries: Vec<(Key, Entry)>, data_blocks: Arc<DataBlocks>) -> Self {
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

        assert!(min < max);

        Self::new_from_sorted(entries, min, max, data_blocks)
    }

    /// Create a table from an already sorted set of entries
    pub fn new_from_sorted(mut entries: Vec<(Key, Entry)>, min: Key, max: Key, data_blocks: Arc<DataBlocks>) -> Self {
        let mut block_ids = Vec::new();
        let mut block_index = Vec::new();
        let mut prefixed_entries = Vec::new();
        let mut last_key= vec![];
        let mut block_size = 0;
        let mut size = 0;

        for (key, entry) in entries.drain(..) {
            if block_size == 0 {
                block_index.push(key.clone());
            }

            let mut prefix_len = 0;

            while prefix_len < key.len() && prefix_len < last_key.len()
                && key[prefix_len] == last_key[prefix_len] {
                prefix_len += 1;
            }

            let suffix = key[prefix_len..].to_vec();
            let this_size = std::mem::size_of::<PrefixedKey>() + std::mem::size_of::<Entry>();
            block_size += this_size;
            size += this_size;

            let pkey = PrefixedKey::new(prefix_len, suffix);
            prefixed_entries.push((pkey, entry));

            last_key = key;

            if block_size >= BLOCK_SIZE {
                let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries));
                block_ids.push(id);

                block_size = 0;
                last_key.clear();
            }
        }

        if block_size > 0 {
            let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries));
            block_ids.push(id);
        }

        Self{ block_index, size, block_ids, data_blocks, min, max }
    }

    // Get the size of this table (in bytes)
    #[inline]
    pub fn get_size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn get_min(&self) -> &Key {
        &self.min
    }

    #[inline]
    pub fn get_max(&self) -> &Key {
        &self.max
    }

    pub fn get(&self, key: &[u8]) -> Option<ValueId> {
        if key < self.get_min().as_slice() || key > self.get_max().as_slice() {
            return None;
        }

        //FIXME don't copy the key here
        let block_offset = match self.block_index.binary_search(&key.to_vec()) {
            Ok(pos) => pos,
            Err(pos) => pos-1
        };

        let block_id = self.block_ids[block_offset];
        let block = self.data_blocks.get_block(&block_id);

        if let Some(vid) = block.get(key) {
            return Some(vid);
        }

        None
    }

    #[inline]
    pub fn overlaps(&self, other: &SortedTable) -> bool {
        self.get_max() >= other.get_min() && self.get_min() <= other.get_max()
    }
}
