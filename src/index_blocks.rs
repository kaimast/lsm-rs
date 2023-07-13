use crate::data_blocks::DataBlockId;
use crate::sorted_table::TableId;
use crate::{disk, Error};
use crate::{Key, Params};

use serde::{Deserialize, Serialize};

use std::cmp::Ordering;
use std::path::Path;

use bincode::Options;

/// Index blocks hold metadata about a sorted table
/// Each table has exactly one index block
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexBlock {
    index: Vec<(Key, DataBlockId)>,
    size: u64,
    min: Key,
    max: Key,
}

impl IndexBlock {
    pub async fn new(
        params: &Params,
        id: TableId,
        index: Vec<(Key, DataBlockId)>,
        size: u64,
        min: Key,
        max: Key,
    ) -> Result<Self, Error> {
        let block = Self {
            index,
            size,
            min,
            max,
        };

        // Store on disk before grabbing the lock
        let block_data = crate::get_encoder().serialize(&block)?;
        let fpath = Self::get_file_path(params, &id);
        disk::write(&fpath, &block_data, 0).await?;

        Ok(block)
    }

    pub async fn load(params: &Params, id: TableId) -> Result<Self, Error> {
        log::trace!("Loading index block from disk");
        let fpath = Self::get_file_path(params, &id);
        let data = disk::read(&fpath, 0).await?;

        Ok(crate::get_encoder().deserialize(&data)?)
    }

    /// where is this index block located on disk?
    #[inline]
    fn get_file_path(params: &Params, block_id: &TableId) -> std::path::PathBuf {
        let fname = format!("idx{:08}.data", block_id);
        params.db_path.join(Path::new(&fname))
    }

    /// Get the unique id for the data block at the specified index
    pub fn get_block_id(&self, pos: usize) -> DataBlockId {
        self.index[pos].1
    }

    /// How many data blocks does this table have?
    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }

    /// The size of this table in bytes
    /// (for WiscKey this just counts the references, not the values themselves)
    pub fn get_size(&self) -> usize {
        self.size as usize
    }

    /// Whats the minimum key in this table?
    pub fn get_min(&self) -> &[u8] {
        &self.min
    }

    /// What is the maximum key in this table?
    pub fn get_max(&self) -> &[u8] {
        &self.max
    }

    /// Search for a specific key
    /// This will a return a data block id that *might* hold this entry or None
    #[tracing::instrument(skip(self))]
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
