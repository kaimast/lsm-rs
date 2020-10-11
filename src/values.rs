use std::fs;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

use serde::{Serialize, de::DeserializeOwned};

const FS_PREFIX: &str = "data";

pub trait Value = Serialize+DeserializeOwned+Clone;

#[ derive(Default) ]
struct BatchRegistry {
    batches: Mutex<HashSet<usize>>
}

pub struct ValueLog<V: Value> {
    next_id: usize,
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
        let next_id = 1;
        let pending_values = Mutex::new( Vec::new() );
        let registry = Arc::new( BatchRegistry::default() );

        Self{ next_id, pending_values, registry }
    }

    pub fn add_value(&self, val: V) -> usize {
        let mut values = self.pending_values.lock().unwrap();
        values.push(val);

        //Return position of new value
        values.len()-1
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
