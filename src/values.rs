use std::sync::{Arc, RwLock, Mutex};
use std::path::Path;
use std::io::Write;

use serde::{Serialize, Deserialize};

use lru::LruCache;

use crate::Params;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

pub type Value = Vec<u8>;
pub type ValueRef = [u8];

const NUM_SHARDS: usize = 16;
const SHARD_SIZE: usize = 100;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

pub struct ValueLog {
    params: Arc<Params>,
    pending_values: RwLock<(ValueBatchId, Vec<Value>)>,
    batch_caches: Vec<Mutex<BatchShard>>
}

#[ derive(Serialize, Deserialize) ]
pub struct ValueBatch {
    values: Vec<Value>
}

impl ValueLog {
    pub fn new(params: Arc<Params>) -> Self {
        let pending_values = RwLock::new((1,Vec::new()));
        let mut batch_caches = Vec::new();

        for _ in 0..NUM_SHARDS {
            let cache = Mutex::new( BatchShard::new(SHARD_SIZE) );
            batch_caches.push(cache);
        }

        Self{ pending_values, batch_caches, params }
    }

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        let fname = format!("values{:08}.lld", batch_id);
        self.params.db_path.join(Path::new(&fname))
    }

    pub fn flush_pending(&self) {
        let (id, values) = {
            let mut lock = self.pending_values.write().unwrap();
            let (next_id, values) = &mut *lock;

            let id = *next_id;
            *next_id += 1;

            (id, std::mem::take(values))
        };

        let batch = ValueBatch{ values };

        // Store on disk before grabbing the lock
        let block_data = bincode::serialize(&batch).unwrap();
        let fpath = self.get_file_path(&id);

        let mut file = std::fs::File::create(fpath).unwrap();
        file.write_all(block_data.as_slice()).expect("Failed to store value batch on disk");
        file.sync_all().unwrap();
        log::debug!("Created new value batch on disk");

        let shard_id = Self::batch_to_shard_id(id);
        let mut cache = self.batch_caches[shard_id].lock().unwrap();
        cache.put(id, Arc::new(batch));
    }

    pub fn add_value(&self, val: Value) -> (ValueId, usize) {
        let mut lock = self.pending_values.write().unwrap();
        let (next_id, values) = &mut *lock;

        let data = bincode::serialize(&val).expect("Failed to serialize value");
        values.push(val);

        let val_len = data.len();

        let pos = (values.len()-1) as ValueOffset;
        let id = (*next_id, pos);

        (id, val_len)
    }

    pub fn get<V: serde::de::DeserializeOwned>(&self, value_ref: &ValueId) -> V {
        let (id, offset) = value_ref;
        let shard_id = Self::batch_to_shard_id(*id);

        let batch = {
            let mut cache = self.batch_caches[shard_id].lock().unwrap();

            if let Some(batch) = cache.get(id) {
                batch.clone()
            } else {
                log::debug!("Loading value batch from disk");
                let fpath = self.get_file_path(&id);
                let data = std::fs::read(fpath).expect("Cannot value batch block from disk");
                let batch: Arc<ValueBatch> = Arc::new( bincode::deserialize(&data).unwrap() );

                cache.put(*id, batch.clone());
                batch
            }
        };

        let val = batch.get_value(*offset);
        bincode::deserialize(val).expect("Failed to serialize value")
    }

    pub fn get_pending<V: serde::de::DeserializeOwned>(&self, id: &ValueId) -> V {
        let lock = self.pending_values.read().unwrap();
        let (_, values) = &*lock;

        let vdata = values.get(id.1 as usize).expect("out of pending values bounds");
        bincode::deserialize(vdata).expect("Failed to deserialize value")
    }
}

impl ValueBatch {
    pub fn get_value(&self, pos: ValueOffset) -> &Value {
        self.values.get(pos as usize).expect("out of batch bounds")
    }
}
