use std::sync::Arc;

use crate::{Error, KvTrait, Params, WriteOp};
use crate::data_blocks::{PrefixedKey, DataBlockId, DataBlock, DataBlocks, DataBlockBuilder, DataEntry, DataEntryType};
use crate::index_blocks::IndexBlock;
use crate::manifest::SeqNumber;

use cfg_if::cfg_if;

#[ cfg(feature="wisckey") ]
use crate::values::ValueId;
#[ cfg(feature="wisckey") ]
use crate::data_blocks::ENTRY_LENGTH;

pub type Key = Vec<u8>;
pub type TableId = u64;
pub type Value = Vec<u8>;

#[ derive(Debug) ]
pub struct SortedTable {
    identifier: TableId,
    index: IndexBlock,
    data_blocks: Arc<DataBlocks>
}

#[ cfg(feature="wisckey") ]
#[ derive(Debug, PartialEq) ]
pub enum ValueResult<'a> {
    Reference(ValueId),
    Value(&'a [u8]),
    NoValue,
}

#[async_trait::async_trait]
pub trait InternalIterator: Send  {
    fn at_end(&self) -> bool;
    async fn step(&mut self);
    fn get_key(&self) -> &Key;
    fn get_seq_number(&self) -> SeqNumber;
    fn get_entry_type(&self) -> DataEntryType;

    #[ cfg(feature="wisckey") ]
    fn get_value(&self) -> ValueResult;

    #[ cfg(not(feature="wisckey")) ]
    fn get_value(&self) -> Option<&[u8]>;
}

#[ derive(Debug) ]
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
    pub fn new(identifier: TableId, params: &'a Params, data_blocks: Arc<DataBlocks>, min_key: Key, max_key: Key) -> TableBuilder<'a> {
        let block_index = vec![];
        let last_key = vec![];
        let block_entry_count = 0;
        let size = 0;
        let restart_count = 0;
        let index_key = None;
        let data_block = DataBlocks::build_block(data_blocks.clone());

        Self{
            identifier, params, data_blocks, block_index, data_block, last_key,
            block_entry_count, size, restart_count, index_key, min_key, max_key,
        }
    }

    #[cfg(feature="wisckey") ]
    pub async fn add_value(&mut self, key: &[u8], seq_number: SeqNumber, value_ref: ValueId) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value_ref).await
    }

    #[cfg(feature="wisckey") ]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, ValueId::default()).await
    }

    #[cfg(not(feature="wisckey")) ]
    pub async fn add_value(&mut self, key: &[u8], seq_number: SeqNumber, value: &[u8]) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value).await
    }

    #[cfg(not(feature="wisckey")) ]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, &[]).await
    }

    async fn add_entry(&mut self, key: &[u8], seq_number: SeqNumber, op_type: u8,
                       #[ cfg(feature="wisckey") ] value: ValueId,
                       #[ cfg(not(feature="wisckey")) ] value: &[u8],
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
                    && key[prefix_len] == self.last_key[prefix_len] {
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

        let index = IndexBlock::new(self.params, self.identifier, self.block_index,
                                    self.size, self.min_key, self.max_key).await?;

        Ok(SortedTable{ identifier: self.identifier, index, data_blocks: self.data_blocks })
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

        Self{
            block_pos, block_offset, key, entry, table,
        }
    }
}

#[async_trait::async_trait]
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

    #[ cfg(feature="wisckey") ]
    fn get_value(&self) -> ValueResult {
        if let Some(value_ref) = self.entry.get_value_ref() {
            ValueResult::Reference(value_ref)
        } else {
            ValueResult::NoValue
        }
    }

    #[ cfg(not(feature="wisckey")) ]
    fn get_value(&self) -> Option<&[u8]> {
        if let Some(value) = &self.entry.get_value() {
            Some(value)
        } else {
            None
        }
    }

    #[ tracing::instrument ]
    async fn step(&mut self) {
        #[ allow(clippy::comparison_chain) ]
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
    pub async fn load(identifier: TableId, data_blocks: Arc<DataBlocks>, params: &Params)
            -> Result<Self, Error> {
        let index = IndexBlock::load(params, identifier).await?;
        Ok( Self{ identifier, index, data_blocks } )
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

    #[ tracing::instrument ]
    pub async fn get(&self, key: &[u8]) -> Option<DataEntry> {
        log::trace!("Checking table #{} for value", self.identifier);
        let block_id = self.index.binary_search(key)?;
        let block = self.data_blocks.get_block(&block_id).await;

        DataBlock::get(&block, key)
    }

    #[inline]
    pub fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.get_max() >= min && self.get_min() <= max
    }
}

/*
#[ cfg(test) ]
mod tests {
    use super::*;

    use crate::manifest::Manifest;
    use crate::Params;
    use crate::entry::Entry;

    use tempfile::tempdir;

    #[ cfg(feature="wisckey") ]
    #[tokio::test]
    async fn iterate() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let data_blocks = Arc::new( DataBlocks::new(params.clone(), manifest) );

        let key1 = vec![5];
        let entry1 = Entry::Value{ seq_number: 1, value_ref: (4,2) };

        let key2 = vec![15];
        let entry2 = Entry::Value{ seq_number: 4, value_ref: (4,50) };

        let id = 124234;
        let entries = vec![(key1.clone(), entry1.clone()), (key2.clone(), entry2.clone())];
        let table = Arc::new( SortedTable::new(id, entries, key1.clone(), key2.clone(), data_blocks, &*params).await.unwrap() );

        let mut iter = TableIterator::new(table).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_value(), ValueResult::Reference(*entry1.get_value_ref().unwrap()));

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_value(), ValueResult::Reference(*entry2.get_value_ref().unwrap()));

        iter.step().await;

        assert_eq!(iter.at_end(), true);
    }

    #[ cfg(not(feature="wisckey")) ]
    #[tokio::test]
    async fn iterate() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let data_blocks = Arc::new( DataBlocks::new(params.clone(), manifest) );

        let key1 = vec![5];
        let entry1 = Entry::Value{ seq_number: 1, value: vec![4,2] };

        let key2 = vec![15];
        let entry2 = Entry::Value{ seq_number: 4, value: vec![4, 50] };

        let id = 124234;
        let entries = vec![(key1.clone(), entry1.clone()), (key2.clone(), entry2.clone())];
        let table = Arc::new( SortedTable::new(id, entries, key1.clone(), key2.clone(), data_blocks, &*params).await.unwrap() );

        let mut iter = TableIterator::new(table).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_value(), entry1.get_value());

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_value(), entry2.get_value());

        iter.step().await;

        assert_eq!(iter.at_end(), true);
    }

    #[ cfg(feature="wisckey") ]
    #[tokio::test]
    async fn iterate_many() {
        const COUNT: u32 = 5_000;

        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let data_blocks = Arc::new( DataBlocks::new(params.clone(), manifest) );

        let mut entries = vec![];

        let min_key = (0u32).to_le_bytes().to_vec();
        let max_key = (COUNT as u32).to_le_bytes().to_vec();

        for pos in 0..COUNT {
            let key = (pos as u32).to_le_bytes().to_vec();
            let entry = Entry::Value{ seq_number: 500+pos as u64, value_ref: (100, pos) };

            entries.push((key, entry));
        }

        let id = 1;
        let table = Arc::new( SortedTable::new(id, entries, min_key, max_key, data_blocks, &*params).await.unwrap() );

        let mut iter = TableIterator::new(table).await;

        for pos in 0..COUNT {
            assert_eq!(iter.at_end(), false);

            assert_eq!(iter.get_key(), &(pos as u32).to_le_bytes().to_vec());
            assert_eq!(iter.get_value(), ValueResult::Reference((100, pos)) );
            assert_eq!(iter.get_seq_number(), 500+pos as u64);

            iter.step().await;
        }

        assert_eq!(iter.at_end(), true);
    }
}*/
