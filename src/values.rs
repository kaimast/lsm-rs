use std::fs;
use std::sync::{Arc, RwLock};
use std::collections::HashSet;

use serde::{Serialize, de::DeserializeOwned};

const FS_PREFIX: &str = "data";

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

pub trait Value = Serialize+DeserializeOwned+Clone+Send+Sync;

#[ derive(Default) ]
struct BatchRegistry {
    batches: RwLock<HashSet<ValueBatchId>>
}

pub struct ValueLog<V: Value> {
    pending_values: RwLock<(ValueBatchId, Vec<V>)>,
    registry: Arc<BatchRegistry>
}

pub struct ValueBatch<V: Value> {
    registry: Arc<BatchRegistry>,
    identifier: usize,
    values: Vec<V>
}

impl<V: Value> ValueLog<V> {
    pub fn new() -> Self {
        let pending_values = RwLock::new( (1, Vec::new()) );
        let registry = Arc::new( BatchRegistry::default() );

        Self{ pending_values, registry }
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
    #[inline]
    pub fn get_id(&self) -> usize {
        self.identifier
    }

    pub fn get_value(&self, pos: usize) -> &V {
        self.values.get(pos).expect("out of batch bounds")
    }
}


impl<V: Value> Drop for ValueBatch<V> {
    fn drop(&mut self) {
        log::trace!("Dropping ValueBatch");
        fs::remove_file(format!("{}/{}", FS_PREFIX, self.identifier)).unwrap();
    }
}
