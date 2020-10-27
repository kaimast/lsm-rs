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
    //TODO add index,
    block_ids: Vec<DataBlockId>,
    data_blocks: Arc<DataBlocks>
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

        let mut block_ids = Vec::new();
        let mut prefixed_entries = Vec::new();
        let mut last_kdata = vec![];
        let mut block_size = 0;

        for (key, entry) in entries.drain(..) {
            let kdata = bincode::serialize(&key).expect("Failed to serialize key");
            let mut prefix_len = 0;

            while prefix_len < kdata.len() && prefix_len < last_kdata.len()
                && kdata[prefix_len] == last_kdata[prefix_len] {
                prefix_len += 1;
            }

            let suffix = kdata[prefix_len..].to_vec();
            block_size += std::mem::size_of::<PrefixedKey>() + std::mem::size_of::<Entry>();

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

        Self{ block_ids, data_blocks, min, max }
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
}
