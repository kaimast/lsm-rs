use crate::Key;
use crate::data_blocks::DataBlockId;

use serde::{Serialize, Deserialize};

use std::cmp::Ordering;

#[ derive(Serialize, Deserialize) ]
pub struct IndexBlock {
    min: Key,
    max: Key,
    size: usize,
    index: Vec<(Key, DataBlockId)>,
}

impl IndexBlock {
    pub fn new( index: Vec<(Key, DataBlockId)>, size: usize, min: Key, max: Key) -> Self {
        Self{ index, size, min, max }
    }

    pub fn get_block_id(&self, pos: usize) -> DataBlockId {
        self.index[pos].1
    }

    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_min(&self) -> &[u8] {
        &self.min
    }

    pub fn get_max(&self) -> &[u8] {
        &self.max
    }

    pub fn binary_search(&self, key: &[u8]) -> Option<DataBlockId> {
        if key < self.get_min() || key > self.get_max() {
            return None;
        }

        let idx = &self.index;

        let mut start = 0;
        let mut end = idx.len() - 1;

        while end - start > 1 {
            let mid = (end - start) / 2 + start;
            let mid_key = idx[mid].0.as_slice();

            match mid_key.cmp(key) {
                Ordering::Equal => {
                    return Some(idx[mid].1);
                }
                Ordering::Greater => {
                    end = mid;
                }
                Ordering::Less => {
                    start = mid;
                }
            }
        }

        assert!(key >= idx[start].0.as_slice());

        if key >= idx[end].0.as_slice() {
            Some(idx[end].1)
        } else {
            Some(idx[start].1)
        }
    }
}
