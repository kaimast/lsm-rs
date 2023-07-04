use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::data_blocks::{
    DataBlock, DataBlockBuilder, DataBlockId, DataBlocks, DataEntry, DataEntryType, PrefixedKey,
};
use crate::index_blocks::IndexBlock;
use crate::manifest::SeqNumber;
use crate::{Error, KvTrait, Params, WriteOp};

use cfg_if::cfg_if;

#[cfg(feature = "wisckey")]
use crate::data_blocks::ENTRY_LENGTH;
#[cfg(feature = "wisckey")]
use crate::values::ValueId;

pub type Key = Vec<u8>;
pub type TableId = u64;
pub type Value = Vec<u8>;

#[derive(Debug)]
pub struct SortedTable {
    identifier: TableId,
    index: IndexBlock,
    data_blocks: Arc<DataBlocks>,
    compaction_flag: AtomicBool,
    allowed_seeks: AtomicI32,
}

#[cfg(feature = "wisckey")]
#[derive(Debug, PartialEq, Eq)]
pub enum ValueResult<'a> {
    Reference(ValueId),
    Value(&'a [u8]),
    NoValue,
}

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
pub trait InternalIterator: Send {
    fn at_end(&self) -> bool;
    async fn step(&mut self);
    fn get_key(&self) -> &Key;
    fn get_seq_number(&self) -> SeqNumber;
    fn get_entry_type(&self) -> DataEntryType;

    #[cfg(feature = "wisckey")]
    fn get_value(&self) -> ValueResult;

    #[cfg(not(feature = "wisckey"))]
    fn get_value(&self) -> Option<&[u8]>;
}

#[derive(Debug)]
pub struct TableIterator {
    block_pos: usize,
    block_offset: u32,
    key: Key,
    entry: DataEntry,
    table: Arc<SortedTable>,
}

pub struct TableBuilder<'a> {
    identifier: TableId,
    params: &'a Params,
    data_blocks: Arc<DataBlocks>,
    min_key: Key,
    max_key: Key,

    data_block: DataBlockBuilder,
    block_index: Vec<(Key, DataBlockId)>,
    last_key: Key,
    block_entry_count: usize,
    size: u64,
    restart_count: usize,
    index_key: Option<Key>,
}

impl<'a> TableBuilder<'a> {
    pub fn new(
        identifier: TableId,
        params: &'a Params,
        data_blocks: Arc<DataBlocks>,
        min_key: Key,
        max_key: Key,
    ) -> TableBuilder<'a> {
        let block_index = vec![];
        let last_key = vec![];
        let block_entry_count = 0;
        let size = 0;
        let restart_count = 0;
        let index_key = None;
        let data_block = DataBlocks::build_block(data_blocks.clone());

        Self {
            identifier,
            params,
            data_blocks,
            block_index,
            data_block,
            last_key,
            block_entry_count,
            size,
            restart_count,
            index_key,
            min_key,
            max_key,
        }
    }

    #[cfg(feature = "wisckey")]
    pub async fn add_value(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        value_ref: ValueId,
    ) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value_ref)
            .await
    }

    #[cfg(feature = "wisckey")]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, ValueId::default())
            .await
    }

    #[cfg(not(feature = "wisckey"))]
    pub async fn add_value(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        value: &[u8],
    ) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value)
            .await
    }

    #[cfg(not(feature = "wisckey"))]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, &[])
            .await
    }

    async fn add_entry(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        op_type: u8,
        #[cfg(feature = "wisckey")] value: ValueId,
        #[cfg(not(feature = "wisckey"))] value: &[u8],
    ) -> Result<(), Error> {
        if self.index_key.is_none() {
            self.index_key = Some(key.to_vec());
        }
        let mut prefix_len = 0;

        // After a certain interval we reset the prefixed keys
        // So that it is possible to binary search blocks
        if self.restart_count == self.params.block_restart_interval {
            self.restart_count = 0;
        } else {
            // Calculate key prefix length
            while prefix_len < key.len()
                && prefix_len < self.last_key.len()
                && key[prefix_len] == self.last_key[prefix_len]
            {
                prefix_len += 1;
            }
        }

        let suffix = key[prefix_len..].to_vec();

        cfg_if! {
            if #[ cfg(feature="wisckey") ] {
                let this_size = std::mem::size_of::<PrefixedKey>() + ENTRY_LENGTH + prefix_len;
            } else {
                let this_size = std::mem::size_of::<PrefixedKey>() + value.len() + prefix_len;
            }
        }

        self.size += this_size as u64;
        self.block_entry_count += 1;
        self.restart_count += 1;

        let pkey = PrefixedKey::new(prefix_len, suffix);

        self.last_key = key.to_vec();

        self.data_block.add_entry(pkey, seq_number, op_type, value);

        if self.block_entry_count >= self.params.max_key_block_size {
            let mut next_block = DataBlocks::build_block(self.data_blocks.clone());
            std::mem::swap(&mut next_block, &mut self.data_block);

            let id = next_block.finish().await?.unwrap();
            self.block_index.push((self.index_key.take().unwrap(), id));

            self.block_entry_count = 0;
            self.restart_count = 0;
            self.last_key.clear();
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<SortedTable, Error> {
        if let Some(id) = self.data_block.finish().await? {
            self.block_index.push((self.index_key.take().unwrap(), id));
        }

        log::debug!("Created new table with {} blocks", self.block_index.len());

        let index = IndexBlock::new(
            self.params,
            self.identifier,
            self.block_index,
            self.size,
            self.min_key,
            self.max_key,
        )
        .await?;

        let allowed_seeks = if let Some(count) = self.params.seek_based_compaction {
            ((index.get_size() / 1024).max(1) as i32) * (count as i32)
        } else {
            0
        };

        Ok(SortedTable {
            index,
            allowed_seeks: AtomicI32::new(allowed_seeks),
            identifier: self.identifier,
            compaction_flag: AtomicBool::new(false),
            data_blocks: self.data_blocks,
        })
    }
}

impl TableIterator {
    pub async fn new(table: Arc<SortedTable>) -> Self {
        let last_key = vec![];
        let block_id = table.index.get_block_id(0);
        let first_block = table.data_blocks.get_block(&block_id).await;
        let byte_len = first_block.byte_len();
        let (key, entry, entry_len) = DataBlock::get_offset(first_block, 0, &last_key);

        // Are we already at the end of the first block?
        let (block_pos, block_offset) = if byte_len == entry_len {
            (1, 0)
        } else {
            (0, entry_len)
        };

        Self {
            block_pos,
            block_offset,
            key,
            entry,
            table,
        }
    }
}

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
impl InternalIterator for TableIterator {
    fn at_end(&self) -> bool {
        self.block_pos > self.table.index.num_data_blocks()
    }

    fn get_key(&self) -> &Key {
        &self.key
    }

    fn get_seq_number(&self) -> SeqNumber {
        self.entry.get_sequence_number()
    }

    fn get_entry_type(&self) -> DataEntryType {
        self.entry.get_type()
    }

    #[cfg(feature = "wisckey")]
    fn get_value(&self) -> ValueResult {
        if let Some(value_ref) = self.entry.get_value_ref() {
            ValueResult::Reference(value_ref)
        } else {
            ValueResult::NoValue
        }
    }

    #[cfg(not(feature = "wisckey"))]
    fn get_value(&self) -> Option<&[u8]> {
        if let Some(value) = &self.entry.get_value() {
            Some(value)
        } else {
            None
        }
    }

    #[tracing::instrument]
    async fn step(&mut self) {
        #[allow(clippy::comparison_chain)]
        if self.block_pos == self.table.index.num_data_blocks() {
            self.block_pos += 1;
            return;
        } else if self.block_pos > self.table.index.num_data_blocks() {
            panic!("Cannot step(); already at end");
        }

        let block_id = self.table.index.get_block_id(self.block_pos);
        let block = self.table.data_blocks.get_block(&block_id).await;
        let byte_len = block.byte_len();

        let (key, entry, new_offset) = DataBlock::get_offset(block, self.block_offset, &self.key);
        self.key = key;
        self.entry = entry;

        // At the end of the block?
        if new_offset >= byte_len {
            self.block_pos += 1;
            self.block_offset = 0;
        } else {
            self.block_offset = new_offset;
        }
    }
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
        self.allowed_seeks.load(Ordering::SeqCst) <= 0
    }

    pub fn maybe_start_compaction(&self) -> bool {
        let order = Ordering::SeqCst;
        let result = self
            .compaction_flag
            .compare_exchange(false, true, order, order);

        result.is_ok()
    }

    pub fn finish_compaction(&self) {
        let prev = self.compaction_flag.swap(false, Ordering::SeqCst);
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

    #[tracing::instrument]
    pub async fn get(&self, key: &[u8]) -> Option<DataEntry> {
        self.allowed_seeks.fetch_sub(1, Ordering::Relaxed);

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

    #[cfg(feature = "wisckey")]
    #[tokio::test]
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

        let mut iter = TableIterator::new(Arc::new(table)).await;

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
    #[tokio::test]
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

        let mut iter = TableIterator::new(table).await;

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

    #[cfg(feature = "wisckey")]
    #[tokio::test]
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

        let mut iter = TableIterator::new(table).await;

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
