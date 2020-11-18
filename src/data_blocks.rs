use std::sync::{atomic, Arc, Mutex};
use std::path::Path;
use std::io::Write;

use serde::{Serialize, Deserialize};

use lru::LruCache;

use bincode::Options;

use crate::Params;
use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;

pub type DataBlockId = u64;

const NUM_SHARDS: usize = 16;
const SHARD_SIZE: usize = 100;

#[ derive(Serialize, Deserialize) ]
pub struct PrefixedKey {
    prefix_len: usize,
    suffix: Vec<u8>
}

impl PrefixedKey {
    pub fn new(prefix_len: usize, suffix: Vec<u8>) -> Self {
        Self{ prefix_len, suffix }
    }
}

type BlockShard = LruCache<DataBlockId, Arc<DataBlock>>;

pub struct DataBlocks {
    params: Arc<Params>,
    block_caches: Vec<Mutex<BlockShard>>,
    next_block_id: atomic::AtomicU64
}

impl DataBlocks {
    pub fn new(params: Arc<Params>) -> Self {
        let mut block_caches = Vec::new();
        for _ in 0..NUM_SHARDS {
            block_caches.push(Mutex::new(BlockShard::new(SHARD_SIZE)));
        }

        let next_block_id = atomic::AtomicU64::new(1);

        Self{ params, next_block_id, block_caches }
    }

    #[inline]
    fn block_to_shard_id(block_id: DataBlockId) -> usize {
        (block_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, block_id: &DataBlockId) -> std::path::PathBuf {
        let fname = format!("keys{:08}.lld", block_id);
        self.params.db_path.join(Path::new(&fname))
    }

    pub fn make_block(&self, entries: Vec<(PrefixedKey, Entry)>) -> DataBlockId {
        let id = self.next_block_id.fetch_add(1, atomic::Ordering::SeqCst);
        let block = Arc::new( DataBlock::new(entries) );
        let shard_id = Self::block_to_shard_id(id);

        // Store on disk before grabbing the lock
        let block_data = super::get_encoder().serialize(&*block).unwrap();
        let fpath = self.get_file_path(&id);

        let mut file = std::fs::File::create(fpath).unwrap();
        file.write_all(block_data.as_slice()).expect("Failed to store data block on disk");
        file.sync_all().unwrap();
        log::trace!("Created new data block on disk");

        let mut cache = self.block_caches[shard_id].lock().unwrap();
        cache.put(id, block);

        id
    }

    pub fn get_block(&self, id: &DataBlockId) -> Arc<DataBlock> {
        let shard_id = Self::block_to_shard_id(*id);

        let mut cache = self.block_caches[shard_id].lock().unwrap();
        if let Some(block) = cache.get(id) {
            block.clone()
        } else {
            log::trace!("Loading key block from disk");
            let fpath = self.get_file_path(&id);
            let data = std::fs::read(fpath).expect("Cannot read data block from disk");

            let block = super::get_encoder().deserialize(&data).unwrap();
            let block: Arc<DataBlock>= Arc::new(block);

            cache.put(*id, block.clone());
            block
        }
    }
}

#[ derive(Serialize, Deserialize) ]
pub struct DataBlock {
    entries: Vec<(PrefixedKey, Entry)>
}

impl DataBlock {
    pub fn new(entries: Vec<(PrefixedKey, Entry)>) -> Self {
        Self{ entries }
    }

    pub fn get_offset(&self, offset: usize, previous_key: &[u8]) -> (Key, Entry) {
        let (pkey, entry) = &self.entries[offset];
        let kdata = [&previous_key[..pkey.prefix_len], &pkey.suffix[..]].concat();

        (kdata, entry.clone())
    }

    pub fn get(&self, key: &[u8]) -> Option<(u64, ValueId)> {
        let mut last_kdata = vec![];

        for (pkey, entry) in self.entries.iter() {
            let this_key = [&last_kdata[..pkey.prefix_len], &pkey.suffix[..]].concat();

            if this_key == key {
                return Some((entry.seq_number, entry.value_ref));
            }

            last_kdata = this_key;
        }

        None
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
