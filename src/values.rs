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

pub trait ToBytes {
    fn encode(&self) -> Vec<u8>;
    fn encode_ref(&self) -> Option<&[u8]>;
    fn decode(v: &[u8]) -> Self;
}

impl ToBytes for String {
    fn encode(&self) -> Vec<u8> {
        let mut val = Vec::new();
        val.extend_from_slice(self.as_bytes());
        val
    }

    fn encode_ref(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }

    fn decode(v: &[u8]) -> Self {
        Self::from_utf8(v.to_vec()).unwrap()
    }
}

impl ToBytes for u64 {
    fn encode(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn encode_ref(&self) -> Option<&[u8]> {
        None
    }

    fn decode(v: &[u8]) -> Self {
        let mut data = [0; 8];
        data.clone_from_slice(v);
        Self::from_le_bytes(data)
    }

}

const NUM_SHARDS: usize = 16;
const SHARD_SIZE: usize = 100;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

pub struct ValueLog {
    params: Arc<Params>,
    pending_values: RwLock<(ValueBatchId, Vec<Value>)>,
    imm_values: Mutex<Option<(ValueBatchId, Vec<Value>)>>,
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
        let imm_values = Mutex::new(None);

        for _ in 0..NUM_SHARDS {
            let cache = Mutex::new( BatchShard::new(SHARD_SIZE) );
            batch_caches.push(cache);
        }

        Self{ pending_values, batch_caches, params, imm_values }
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

    pub fn next_pending(&self) {
        let mut lock = self.pending_values.write().unwrap();
        let mut imm_values = self.imm_values.lock().unwrap();

        assert!(imm_values.is_none());

        let (next_id, values) = &mut *lock;

        let id = *next_id;
        *next_id += 1;

        *imm_values = Some((id, std::mem::take(values)));
    }

    pub fn flush_pending(&self) {
       let mut imm_values = self.imm_values.lock().unwrap();
       let (id, values) = imm_values.take().unwrap();

       let batch = ValueBatch{ values };

        // Store on disk before grabbing the lock
        let block_data = bincode::serialize(&batch).unwrap();
        let fpath = self.get_file_path(&id);

        let mut file = std::fs::File::create(fpath).unwrap();
        file.write_all(block_data.as_slice()).expect("Failed to store value batch on disk");
        file.sync_all().unwrap();
        log::trace!("Created new value batch on disk");

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

    pub fn get<V: ToBytes>(&self, value_ref: &ValueId) -> V {
        let (id, offset) = value_ref;
        let shard_id = Self::batch_to_shard_id(*id);

        let batch = {
            let mut cache = self.batch_caches[shard_id].lock().unwrap();

            if let Some(batch) = cache.get(id) {
                batch.clone()
            } else {
                log::trace!("Loading value batch from disk");
                let fpath = self.get_file_path(&id);
                let data = std::fs::read(fpath).expect("Cannot value batch block from disk");
                let batch: Arc<ValueBatch> = Arc::new( bincode::deserialize(&data).unwrap() );

                cache.put(*id, batch.clone());
                batch
            }
        };

        let val = batch.get_value(*offset);
        V::decode(val)
    }

    pub fn get_pending<V: ToBytes>(&self, id: &ValueId) -> V {
        let lock = self.pending_values.read().unwrap();
        let (_, values) = &*lock;

        let vdata = values.get(id.1 as usize).expect("out of pending values bounds");
        V::decode(vdata)
    }
}

impl ValueBatch {
    pub fn get_value(&self, pos: ValueOffset) -> &Value {
        self.values.get(pos as usize).expect("out of batch bounds")
    }
}
