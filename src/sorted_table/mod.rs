use std::sync::atomic::{AtomicBool, AtomicI32, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::data_blocks::{DataBlock, DataBlocks, DataEntry};
use crate::index_blocks::IndexBlock;
use crate::{Error, Params};

#[cfg(feature = "wisckey")]
use crate::values::ValueId;

mod iterator;
pub use iterator::{InternalIterator, TableIterator};

mod builder;
pub use builder::TableBuilder;

#[cfg(test)]
mod tests;

pub type Key = Vec<u8>;
pub type TableId = u64;
pub type Value = Vec<u8>;

/// Entries ach level are grouped into sorted tables
/// These tables contain an ordered set of key/value-pairs
///
/// Except for level 0, sorted tables do not overlap others on the same level
#[derive(Debug)]
pub struct SortedTable {
    identifier: TableId,
    /// The index of the table; it holds all relevant metadata
    index: IndexBlock,
    data_blocks: Arc<DataBlocks>,
    /// Is this table currently being compacted
    compaction_flag: AtomicBool,
    /// The number of seek operations on this table before compaction is triggered
    /// This improves read performance for heavily queried keys
    allowed_seeks: AtomicI32,
}

#[cfg(feature = "wisckey")]
#[derive(Debug, PartialEq, Eq)]
pub enum ValueResult<'a> {
    Reference(ValueId),
    Value(&'a [u8]),
    NoValue,
}

impl SortedTable {
    pub async fn load(
        identifier: TableId,
        data_blocks: Arc<DataBlocks>,
        params: &Params,
    ) -> Result<Self, Error> {
        let index = IndexBlock::load(params, identifier).await?;

        let allowed_seeks = if let Some(count) = params.seek_based_compaction {
            ((index.get_size() / 1024).max(1) as i32) * (count as i32)
        } else {
            0
        };

        Ok(Self {
            identifier,
            index,
            data_blocks,
            allowed_seeks: AtomicI32::new(allowed_seeks),
            compaction_flag: AtomicBool::new(false),
        })
    }

    pub fn has_maximum_seeks(&self) -> bool {
        self.allowed_seeks.load(AtomicOrdering::SeqCst) <= 0
    }

    /// Returns false if another task is already compacting this table
    pub fn maybe_start_compaction(&self) -> bool {
        let order = AtomicOrdering::SeqCst;
        let result = self
            .compaction_flag
            .compare_exchange(false, true, order, order);

        result.is_ok()
    }

    pub fn finish_compaction(&self) {
        let prev = self.compaction_flag.swap(false, AtomicOrdering::SeqCst);
        assert!(prev, "Compaction flag was not set!");
    }

    #[inline]
    pub fn get_id(&self) -> TableId {
        self.identifier
    }

    // Get the size of this table (in bytes)
    #[inline]
    pub fn get_size(&self) -> usize {
        self.index.get_size()
    }

    #[inline]
    pub fn get_min(&self) -> &[u8] {
        self.index.get_min()
    }

    #[inline]
    pub fn get_max(&self) -> &[u8] {
        self.index.get_max()
    }

    /// Gets an entry for particular key in this table
    /// Returns None if no entry for the key exists
    #[tracing::instrument(skip(self, key))]
    pub async fn get(&self, key: &[u8]) -> Option<DataEntry> {
        self.allowed_seeks.fetch_sub(1, AtomicOrdering::Relaxed);

        let block_id = self.index.binary_search(key)?;
        let block = self.data_blocks.get_block(&block_id).await;

        DataBlock::get(&block, key)
    }

    #[inline]
    pub fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.get_max() >= min && self.get_min() <= max
    }
}
