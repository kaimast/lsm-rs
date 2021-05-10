use crate::Key;
use crate::data_blocks::DataBlockId;
use crate::Params;
use crate::disk;
use crate::sorted_table::TableId;

use serde::{Serialize, Deserialize};

use lru::LruCache;

use std::cmp::Ordering;
use std::sync::Arc;
use std::path::Path;

use tokio::sync::Mutex;

use bincode::Options;

const NUM_SHARDS: usize = 8;

#[ derive(Serialize, Deserialize) ]
pub struct IndexBlock {
    index: Vec<(Key, DataBlockId)>,
    size: u64,
    min: Key,
    max: Key,
}

type BlockShard = LruCache<TableId, Arc<IndexBlock>>;

pub struct IndexBlocks {
    params: Arc<Params>,
    block_caches: Vec<Mutex<BlockShard>>,
}

impl IndexBlocks {
    pub fn new(params: Arc<Params>) -> Self {
        let max_index_files = params.max_open_files / 2;
        let shard_size = max_index_files / NUM_SHARDS;
        assert!(shard_size > 0);

        let mut block_caches = Vec::new();
        for _ in 0..NUM_SHARDS {
            block_caches.push(Mutex::new(BlockShard::new(shard_size)));
        }

        Self{ params, block_caches }
    }

    #[inline]
    fn block_to_shard_id(block_id: DataBlockId) -> usize {
        (block_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, block_id: &DataBlockId) -> std::path::PathBuf {
        let fname = format!("idx{:08}.data", block_id);
        self.params.db_path.join(Path::new(&fname))
    }

    pub async fn make_block(&self, id: TableId, index: Vec<(Key, DataBlockId)>, size: u64, min: Key, max: Key) -> Arc<IndexBlock> {
        let block = Arc::new( IndexBlock::new(index, size, min, max) );
        let shard_id = Self::block_to_shard_id(id);

        // Store on disk before grabbing the lock
        let block_data = crate::get_encoder().serialize(&*block).unwrap();
        let fpath = self.get_file_path(&id);
        disk::write(&fpath, &block_data).await;

        let mut cache = self.block_caches[shard_id].lock().await;
        cache.put(id, block.clone());

        block
    }

    pub async fn get_block(&self, id: &TableId) -> Arc<IndexBlock> {
        let shard_id = Self::block_to_shard_id(*id);

        let mut cache = self.block_caches[shard_id].lock().await;
        if let Some(block) = cache.get(id) {
            block.clone()
        } else {
            log::trace!("Loading data block from disk");
            let fpath = self.get_file_path(&id);
            let data = disk::read(&fpath).await;

            let block: IndexBlock = crate::get_encoder().deserialize(&data).unwrap();
            let block = Arc::new(block);

            cache.put(*id, block.clone());
            block
        }
    }
}


impl IndexBlock {
    fn new( index: Vec<(Key, DataBlockId)>, size: u64, min: Key, max: Key) -> Self {
        Self{ index, size, min, max }
    }

    pub fn get_block_id(&self, pos: usize) -> DataBlockId {
        self.index[pos].1
    }

    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }

    pub fn get_size(&self) -> usize {
        self.size as usize
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
