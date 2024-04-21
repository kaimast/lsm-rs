use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use lru::LruCache;

use crate::manifest::{Manifest, SeqNumber};
use crate::Params;
use crate::{disk, WriteOp};

mod builder;
pub use builder::DataBlockBuilder;

mod block;
pub use block::DataBlock;

#[cfg(feature = "wisckey")]
use crate::values::{ValueBatchId, ValueId, ValueOffset};

pub type DataBlockId = u64;
const NUM_SHARDS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

#[derive(Debug)]
pub struct PrefixedKey {
    prefix_len: u32,
    suffix: Vec<u8>,
}

impl PrefixedKey {
    pub fn new(prefix_len: usize, suffix: Vec<u8>) -> Self {
        Self {
            prefix_len: prefix_len as u32,
            suffix,
        }
    }
}

type BlockShard = LruCache<DataBlockId, Arc<DataBlock>>;

#[cfg(not(feature = "wisckey"))]
type DataLen = u64;

pub enum DataEntryType {
    Put,
    Delete,
}

#[derive(Clone)]
pub struct DataEntry {
    block: Arc<DataBlock>,
    offset: usize,

    #[cfg(not(feature = "wisckey"))]
    length: usize,
}

enum SearchResult {
    ExactMatch(DataEntry),
    Range(u32, u32),
}

#[cfg(feature = "wisckey")]
pub const ENTRY_LENGTH: usize =
    size_of::<SeqNumber>() + size_of::<u8>() + size_of::<ValueBatchId>() + size_of::<ValueOffset>();

impl DataEntry {
    #[inline(always)]
    fn get_length(&self) -> usize {
        cfg_if::cfg_if! {
            if #[cfg(feature="wisckey")] {
                ENTRY_LENGTH
            } else {
                self.length
            }
        }
    }

    #[inline(always)]
    fn data(&self) -> &[u8] {
        &self.block.data[self.offset..self.offset + self.get_length()]
    }

    pub fn get_sequence_number(&self) -> u64 {
        let seq_len = size_of::<SeqNumber>();
        u64::from_le_bytes(self.data()[0..seq_len].try_into().unwrap())
    }

    pub fn get_type(&self) -> DataEntryType {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let type_data = self.data()[seq_len];

        if type_data == WriteOp::PUT_OP {
            DataEntryType::Put
        } else if type_data == WriteOp::DELETE_OP {
            DataEntryType::Delete
        } else {
            panic!("Unknown data entry type");
        }
    }

    #[cfg(not(feature = "wisckey"))]
    pub fn get_value(&self) -> Option<&[u8]> {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let type_data = self.data()[seq_len];

        let header_len = seq_len + 1;

        if type_data == WriteOp::PUT_OP {
            Some(&self.data()[header_len..])
        } else if type_data == WriteOp::DELETE_OP {
            None
        } else {
            panic!("Unknown write op");
        }
    }

    #[cfg(feature = "wisckey")]
    pub fn get_value_ref(&self) -> Option<ValueId> {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let batch_id_len = std::mem::size_of::<ValueBatchId>();
        let offset_len = std::mem::size_of::<ValueOffset>();

        let type_data = self.data()[seq_len];

        if type_data == WriteOp::PUT_OP {
            let offset = seq_len + 1;
            let batch_id = ValueBatchId::from_le_bytes(
                self.data()[offset..offset + batch_id_len]
                    .try_into()
                    .unwrap(),
            );
            let offset = offset + batch_id_len;
            let value_offset = ValueOffset::from_le_bytes(
                self.data()[offset..offset + offset_len].try_into().unwrap(),
            );

            Some((batch_id, value_offset))
        } else if type_data == WriteOp::DELETE_OP {
            None
        } else {
            panic!("Unknown write op");
        }
    }
}

/// Keeps track of all in-memory data blocks
#[derive(Debug)]
pub struct DataBlocks {
    params: Arc<Params>,
    block_caches: Vec<Mutex<BlockShard>>,
    manifest: Arc<Manifest>,
}

impl DataBlocks {
    pub fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let max_data_files = NonZeroUsize::new(params.max_open_files / 2)
            .expect("Max open files needs to be greater than 2");

        let shard_size = NonZeroUsize::new(max_data_files.get() / NUM_SHARDS)
            .expect("Not enough open files to support the number of shards");

        let mut block_caches = Vec::new();
        for _ in 0..NUM_SHARDS.get() {
            block_caches.push(Mutex::new(BlockShard::new(shard_size)));
        }

        Self {
            params,
            block_caches,
            manifest,
        }
    }

    #[inline]
    fn block_to_shard_id(block_id: DataBlockId) -> usize {
        (block_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, block_id: &DataBlockId) -> std::path::PathBuf {
        let fname = format!("key{block_id:08}.data");
        self.params.db_path.join(Path::new(&fname))
    }

    #[tracing::instrument]
    pub fn build_block(self_ptr: Arc<DataBlocks>) -> DataBlockBuilder {
        DataBlockBuilder::new(self_ptr)
    }

    /// Get a block by its id
    /// Will either return the block from cache or load it from disk
    #[tracing::instrument(skip(self))]
    pub async fn get_block(&self, id: &DataBlockId) -> Arc<DataBlock> {
        let shard_id = Self::block_to_shard_id(*id);
        let cache = &self.block_caches[shard_id];

        if let Some(block) = cache.lock().get(id) {
            return block.clone();
        }

        // Do not hold the lock while loading form disk for better concurrency
        // Worst case this means we load the same block multiple times...
        let fpath = self.get_file_path(id);
        log::trace!("Loading data block from disk at {fpath:?}");
        let data = disk::read(&fpath, 0)
            .await
            .expect("Failed to load data block from disk at {fpath:?}");
        let block = Arc::new(DataBlock::new_from_data(
            data,
            self.params.block_restart_interval,
        ));

        cache.lock().put(*id, block.clone());
        log::trace!("Stored new block in cache");
        block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[cfg(feature = "async-io")]
    use tokio_uring::test as async_test;

    #[cfg(not(feature = "async-io"))]
    use tokio::test as async_test;

    #[cfg(feature = "wisckey")]
    #[async_test]
    async fn store_and_load() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));
        let mut builder = DataBlocks::build_block(data_blocks.clone());

        let key1 = PrefixedKey {
            prefix_len: 0,
            suffix: vec![5],
        };
        let seq1 = 14234524;
        let val1 = (4, 2);
        builder.add_entry(key1, &[5], seq1, WriteOp::PUT_OP, val1);

        let key2 = PrefixedKey {
            prefix_len: 1,
            suffix: vec![2],
        };
        let seq2 = 424234;
        let val2 = (4, 5);
        builder.add_entry(key2, &[5, 2], seq2, WriteOp::PUT_OP, val2);

        let id = builder.finish().await.unwrap().unwrap();
        let data_block1 = data_blocks.get_block(&id).await;
        let data_block2 = Arc::new(DataBlock::new_from_data(
            data_block1.data.clone(),
            params.block_restart_interval,
        ));

        let prev_key = vec![];
        let (key, entry, pos) = DataBlock::get_entry_at_offset(data_block2.clone(), 0, &prev_key);

        assert_eq!(key, vec![5]);
        assert_eq!(entry.get_value_ref(), Some(val1));

        let (key, entry, pos) = DataBlock::get_entry_at_offset(data_block2.clone(), pos, &key);

        assert_eq!(key, vec![5, 2]);
        assert_eq!(entry.get_value_ref(), Some(val2));
        assert_eq!(pos, data_block2.byte_len());
    }

    #[cfg(not(feature = "wisckey"))]
    #[async_test]
    async fn store_and_load() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));
        let mut builder = DataBlocks::build_block(data_blocks.clone());

        let key1 = PrefixedKey {
            prefix_len: 0,
            suffix: vec![5],
        };
        let seq1 = 14234524;
        let val1 = vec![4, 2];
        builder.add_entry(key1, &[5u8], seq1, WriteOp::PUT_OP, &val1);

        let key2 = PrefixedKey {
            prefix_len: 1,
            suffix: vec![2],
        };
        let seq2 = 424234;
        let val2 = vec![24, 50];
        builder.add_entry(key2, &[5u8, 2u8], seq2, WriteOp::PUT_OP, &val2);

        let id = builder.finish().await.unwrap().unwrap();
        let data_block1 = data_blocks.get_block(&id).await;
        let data_block2 = Arc::new(DataBlock::new_from_data(
            data_block1.data.clone(),
            params.block_restart_interval as u32,
        ));

        let prev_key = vec![];
        let (key, entry, pos) = DataBlock::get_entry_at_offset(data_block2.clone(), 0, &prev_key);

        assert_eq!(key, vec![5]);
        assert_eq!(entry.get_value(), Some(&val1[..]));

        let (key, entry, pos) = DataBlock::get_entry_at_offset(data_block2.clone(), pos, &key);

        assert_eq!(key, vec![5, 2]);
        assert_eq!(entry.get_value(), Some(&val2[..]));
        assert_eq!(pos, data_block2.byte_len());
    }
}
