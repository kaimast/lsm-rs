use std::sync::Arc;

use crate::Params;
use crate::entry::Entry;
use crate::data_blocks::{PrefixedKey, DataBlocks};
use crate::index_blocks::IndexBlock;

pub type Key = Vec<u8>;
pub type TableId = u64;
pub type Value = Vec<u8>;

pub struct SortedTable {
    identifier: TableId,
    index: Box<IndexBlock>,
    data_blocks: Arc<DataBlocks>
}

#[async_trait::async_trait]
pub trait InternalIterator: Send {
    fn at_end(&self) -> bool;
    async fn step(&mut self);
    fn get_key(&self) -> &Key;
    fn get_entry(&self) -> &Entry;
}

pub struct TableIterator {
    block_pos: usize,
    block_offset: u32,
    key: Key,
    entry: Entry,
    table: Arc<SortedTable>
}

impl TableIterator {
    pub async fn new(table: Arc<SortedTable>) -> Self {
        let last_key = vec![];
        let block_id = table.index.get_block_id(0);
        let first_block = table.data_blocks.get_block(&block_id).await;
        let (key, entry, entry_len) = first_block.get_offset(0, &last_key);

        // Are we already at the end of the first block?
        let (block_pos, block_offset) = if first_block.byte_len() == entry_len {
            (1, 0)
        } else {
            (0, entry_len)
        };

        Self{ block_pos, block_offset, key, entry, table }
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

    fn get_entry(&self) -> &Entry {
        &self.entry
    }

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

        let (key, entry, new_offset) = block.get_offset(self.block_offset, &self.key);
        self.key = key;
        self.entry = entry;

        // At the end of the block?
        if new_offset >= block.byte_len() {
            self.block_pos += 1;
            self.block_offset = 0;
        } else {
            self.block_offset = new_offset;
        }
    }
}

impl SortedTable {
    pub async fn new(identifier: TableId, mut entries: Vec<(Key, Entry)>, min: Key, max: Key, data_blocks: Arc<DataBlocks>, params: &Params)
            -> Self {
        let mut block_index = Vec::new();
        let mut prefixed_entries = Vec::new();
        let mut last_key= vec![];
        let mut block_entry_count = 0;
        let mut size = 0;
        let mut restart_count = 0;
        let mut index_key = None;

        for (key, entry) in entries.drain(..) {
            if index_key.is_none() {
                index_key = Some(key.clone());
            }
            let mut prefix_len = 0;

            // After a certain interval we reset the prefixed keys
            // So that it is possible to binary search blocks
            if restart_count == params.block_restart_interval {
                restart_count = 0;
            } else {
                // Calculate key prefix length
                while prefix_len < key.len()
                        && prefix_len < last_key.len()
                        && key[prefix_len] == last_key[prefix_len] {
                    prefix_len += 1;
                }
            }

            let suffix = key[prefix_len..].to_vec();
            let this_size = std::mem::size_of::<PrefixedKey>() + std::mem::size_of::<Entry>() + prefix_len;
            size += this_size;
            block_entry_count += 1;
            restart_count += 1;

            let pkey = PrefixedKey::new(prefix_len, suffix);
            prefixed_entries.push((pkey, entry));

            last_key = key;

            if block_entry_count >= params.max_key_block_size {
                let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries), params)
                            .await;

                block_index.push((index_key.take().unwrap(), id));

                block_entry_count = 0;
                restart_count = 0;
                last_key.clear();
            }
        }

        if block_entry_count > 0 {
            let id = data_blocks.make_block(std::mem::take(&mut prefixed_entries), params).await;
            block_index.push((index_key.take().unwrap(), id));
        }

        log::debug!("Created new table with {} blocks", block_index.len());

        let index = Box::new(IndexBlock::new(block_index, size, min, max));
        Self{ identifier, index, data_blocks }
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

    pub async fn get(&self, key: &[u8]) -> Option<Entry> {
        let block_id = self.index.binary_search(key)?;
        let block = self.data_blocks.get_block(&block_id).await;

        block.get(key)
    }

    #[inline]
    pub fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.get_max() >= min && self.get_min() <= max
    }
}

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
        let table = Arc::new( SortedTable::new(id, entries, key1.clone(), key2.clone(), data_blocks, &*params).await );

        let mut iter = TableIterator::new(table).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_entry(), &entry1);

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_entry(), &entry2);

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
        let table = Arc::new( SortedTable::new(id, entries, key1.clone(), key2.clone(), data_blocks, &*params).await );

        let mut iter = TableIterator::new(table).await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_entry(), &entry1);

        iter.step().await;

        assert_eq!(iter.at_end(), false);
        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_entry(), &entry2);

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
        let table = Arc::new( SortedTable::new(id, entries, min_key, max_key, data_blocks, &*params).await );

        let mut iter = TableIterator::new(table).await;

        for pos in 0..COUNT {
            assert_eq!(iter.at_end(), false);

            assert_eq!(iter.get_key(), &(pos as u32).to_le_bytes().to_vec());
            assert_eq!(iter.get_entry(),
                    &Entry::Value{ seq_number: 500+pos as u64, value_ref: (100, pos) });

            iter.step().await;
        }

        assert_eq!(iter.at_end(), true);
    }
}
