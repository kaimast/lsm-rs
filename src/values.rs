use std::fs;
use std::sync::{Arc, Mutex, atomic};
use std::collections::HashSet;

use serde::{Serialize, de::DeserializeOwned};

const FS_PREFIX: &str = "data";

pub trait Value = Serialize+DeserializeOwned+Clone+Send+Sync;

#[ derive(Default) ]
struct BatchRegistry {
    batches: Mutex<HashSet<usize>>
}

pub struct ValueLog<V: Value> {
    next_id: atomic::AtomicUsize,
    pending_values: Mutex<Vec<V>>,
    registry: Arc<BatchRegistry>
}

pub struct ValueBatch<V: Value> {
    registry: Arc<BatchRegistry>,
    identifier: usize,
    values: Vec<V>
}

impl<V: Value> ValueLog<V> {
    pub fn new() -> Self {
        let next_id = atomic::AtomicUsize::new(1);
        let pending_values = Mutex::new( Vec::new() );
        let registry = Arc::new( BatchRegistry::default() );

        Self{ next_id, pending_values, registry }
    }

    pub fn add_value(&self, val: V) -> (usize, usize) {
        let mut values = self.pending_values.lock().unwrap();

        let data = bincode::serialize(&val).expect("Failed to serialize value");
        values.push(val);

        let pos = values.len()-1;
        let val_len = data.len();

        (pos, val_len)
    }

    pub fn make_batch(&self, values: Vec<V>) -> Arc<ValueBatch<V>> {
        let identifier = self.next_id.fetch_add(1, atomic::Ordering::SeqCst);
        let registry = self.registry.clone();

        let batch = Arc::new( ValueBatch{ identifier, values, registry } );

        let mut registry = self.registry.batches.lock().unwrap();
        registry.insert(identifier);

        batch
    }

    pub fn get_pending(&self, pos: usize) -> V {
        let values = self.pending_values.lock().unwrap();
        values.get(pos).expect("out of pending values bounds").clone()
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
