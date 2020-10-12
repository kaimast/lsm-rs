use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

use crate::values::{Value, ValueBatch};

pub trait Key = Ord+Serialize+DeserializeOwned+Send+Sync;

pub struct Entry<K: Key, V: Value> {
    key: K,
    value_batch: Arc<ValueBatch<V>>,
    value_pos: usize
}

pub struct SSTable<K: Key, V: Value> {
    entries: Vec<Entry<K, V>>
}
