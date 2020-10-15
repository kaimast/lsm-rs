use std::sync::RwLock;
use std::collections::HashMap;

use serde::{Serialize, de::DeserializeOwned};

const FS_PREFIX: &str = "data";

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

pub trait Value = Serialize+DeserializeOwned+Clone+Send+Sync;

pub struct ValueLog<V: Value> {
    pending_values: RwLock<(ValueBatchId, Vec<V>)>,
    cache: RwLock<HashMap<ValueBatchId, ValueBatch<V>>>
}

pub struct ValueBatch<V: Value> {
    values: Vec<V>
}

impl<V: Value> ValueLog<V> {
    pub fn new() -> Self {
        let pending_values = RwLock::new( (1, Vec::new()) );
        let cache =  RwLock::new( HashMap::default() );

        Self{ pending_values, cache }
    }

    pub fn flush_pending(&self) {
        let (id, values) = {
            let mut lock = self.pending_values.write().unwrap();
            let (next_id, pending_vals) = &mut *lock;
            let id = *next_id;
            *next_id += 1;

            (id, std::mem::take(pending_vals))
        };

        let mut cache = self.cache.write().unwrap();
        cache.insert(id, ValueBatch{ values });
    }

    pub fn add_value(&self, val: V) -> (ValueId, usize) {
        let mut lock = self.pending_values.write().unwrap();
        let (next_id, values) = &mut *lock;

        let data = bincode::serialize(&val).expect("Failed to serialize value");
        values.push(val);

        let val_len = data.len();

        let pos = (values.len()-1) as ValueOffset;
        let id = (*next_id, pos);

        (id, val_len)
    }

    pub fn get(&self, value_ref: &ValueId) -> V {
        let cache = self.cache.read().unwrap();
        let batch = cache.get(&value_ref.0).unwrap();
        batch.get_value(value_ref.1).clone()
    }

    /*
    pub fn make_batch(&self, values: Vec<V>) -> Arc<ValueBatch<V>> {
        let identifier = self.next_id.fetch_add(1, atomic::Ordering::SeqCst);
        let registry = self.registry.clone();

        let batch = Arc::new( ValueBatch{ identifier, values, registry } );

        let mut registry = self.registry.batches.lock().unwrap();
        registry.insert(identifier);

        batch
    }*/

    pub fn get_pending(&self, id: &ValueId) -> V {
        let lock = self.pending_values.read().unwrap();
        let (_, values) = &*lock;

        values.get(id.1 as usize).expect("out of pending values bounds").clone()
    }
}

impl<V: Value> ValueBatch<V> {
    pub fn get_value(&self, pos: ValueOffset) -> &V {
        self.values.get(pos as usize).expect("out of batch bounds")
    }
}
