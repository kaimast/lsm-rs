use std::sync::atomic::{AtomicBool, AtomicI32, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::data_blocks::{
    DataBlock, DataBlocks, DataEntry,
};
use crate::index_blocks::IndexBlock;
use crate::{Error, Params};

#[cfg(feature = "wisckey")]
use crate::values::ValueId;

mod iterator;
pub use iterator::{InternalIterator, TableIterator};

mod builder;
pub use builder::TableBuilder;

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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::manifest::Manifest;
    use crate::Params;

    use tempfile::tempdir;

    #[cfg(feature = "async-io")]
    use tokio_uring::test as async_test;

    #[cfg(not(feature = "async-io"))]
    use tokio::test as async_test;

    #[cfg(feature = "wisckey")]
    #[async_test]
    async fn iterate() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

        let key1 = vec![5];
        let key2 = vec![15];

        let vref1 = (4, 2);
        let vref2 = (4, 50);

        let id = 124234;
        let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

        builder.add_value(&key1, 1, vref1).await.unwrap();

        builder.add_value(&key2, 4, vref2).await.unwrap();

        let table = builder.finish().await.unwrap();

        let mut iter = TableIterator::new(Arc::new(table), false).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_value(), ValueResult::Reference(vref1));

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_value(), ValueResult::Reference(vref2));

        iter.step().await;

        assert_eq!(iter.at_end(), true);
    }

    #[cfg(not(feature = "wisckey"))]
    #[async_test]
    async fn iterate() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

        let key1 = vec![5];
        let key2 = vec![15];

        let value1 = vec![4, 2];
        let value2 = vec![4, 50];

        let id = 124234;
        let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

        builder.add_value(&key1, 1, &value1).await.unwrap();

        builder.add_value(&key2, 4, &value2).await.unwrap();

        let table = Arc::new(builder.finish().await.unwrap());

        let mut iter = TableIterator::new(table, false).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_value(), Some(&value1 as &[u8]));

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_value(), Some(&value2 as &[u8]));

        iter.step().await;

        assert_eq!(iter.at_end(), true);
    }

    #[cfg(not(feature = "wisckey"))]
    #[async_test]
    async fn reverse_iterate() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

        let key1 = vec![5];
        let key2 = vec![15];

        let value1 = vec![4, 2];
        let value2 = vec![4, 50];

        let id = 124234;
        let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

        builder.add_value(&key1, 1, &value1).await.unwrap();

        builder.add_value(&key2, 4, &value2).await.unwrap();

        let table = Arc::new(builder.finish().await.unwrap());

        let mut iter = TableIterator::new(table, true).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_value(), Some(&value2 as &[u8]));

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_value(), Some(&value1 as &[u8]));

        iter.step().await;

        assert_eq!(iter.at_end(), true);
    }

    #[cfg(feature = "wisckey")]
    #[async_test]
    async fn iterate_many() {
        const COUNT: u32 = 5_000;

        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

        let min_key = (0u32).to_le_bytes().to_vec();
        let max_key = (COUNT as u32).to_le_bytes().to_vec();

        let id = 1;
        let mut builder = TableBuilder::new(id, &*params, data_blocks, min_key, max_key);

        for pos in 0..COUNT {
            let key = (pos as u32).to_le_bytes().to_vec();
            let seq_num = (500 + pos) as u64;

            builder.add_value(&key, seq_num, (100, pos)).await.unwrap();
        }

        let table = Arc::new(builder.finish().await.unwrap());

        let mut iter = TableIterator::new(table, false).await;

        for pos in 0..COUNT {
            assert_eq!(iter.at_end(), false);

            assert_eq!(iter.get_key(), &(pos as u32).to_le_bytes().to_vec());
            assert_eq!(iter.get_value(), ValueResult::Reference((100, pos)));
            assert_eq!(iter.get_seq_number(), 500 + pos as u64);

            iter.step().await;
        }

        assert_eq!(iter.at_end(), true);
    }
}
