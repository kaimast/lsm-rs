use std::sync::{atomic, Arc, Mutex};
use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::entry::Entry;
use crate::sorted_table::Key;
use crate::values::ValueId;

pub type DataBlockId = u64;

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

pub struct DataBlocks {
    //TODO use a lockfree datastructure here
    block_cache: Mutex<HashMap<DataBlockId, Arc<DataBlock>>>,
    next_block_id: atomic::AtomicU64
}

impl DataBlocks {
    pub fn new() -> Self {
        Self{
            next_block_id: atomic::AtomicU64::new(1),
            block_cache: Mutex::new( HashMap::new() )
        }
    }

    pub fn make_block(&self, entries: Vec<(PrefixedKey, Entry)>) -> DataBlockId {
        let id = self.next_block_id.fetch_add(1, atomic::Ordering::SeqCst);
        let block = Arc::new( DataBlock::new(entries) );

        let mut cache = self.block_cache.lock().unwrap();
        cache.insert(id, block);

        //TODO store on disk

        id
    }

    pub fn get_block(&self, id: &DataBlockId) -> Arc<DataBlock> {
        //TODO load from disk

        let cache = self.block_cache.lock().unwrap();
        cache.get(id).unwrap().clone()
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

    pub fn get<K: Key>(&self, key: &K) -> Option<ValueId> {
        let mut last_kdata = vec![];

        for (pkey, entry) in self.entries.iter() {
            let kdata = [&last_kdata[..pkey.prefix_len], &pkey.suffix[..]].concat();
            let this_key: K = bincode::deserialize(&kdata).expect("Failed to deserialize key");

            if &this_key == key {
                return Some(entry.value_ref);
            }

            last_kdata = kdata;
        }

        None
    }
}
