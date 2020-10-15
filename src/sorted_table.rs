use serde::{Serialize, de::DeserializeOwned};

use crate::entry::Entry;
use crate::values::ValueId;

pub trait Key = Ord+Serialize+DeserializeOwned+Send+Sync+Clone;

pub struct SortedTable<K: Key> {
    entries: Vec<Entry<K>>
}

impl<K: Key> SortedTable<K> {
    pub fn new(mut entries: Vec<Entry<K>>) -> Self {
        if entries.is_empty() {
            panic!("Cannot create empty table");
        }

        entries.sort();

        Self{ entries }
    }

    #[inline]
    pub fn min(&self) -> &K {
        &self.entries.get(0).unwrap().key
    }

    #[inline]
    pub fn max(&self) -> &K {
        let len = self.entries.len();
        &self.entries.get(len-1).unwrap().key
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        if key < self.min() || key > self.max() {
            return None;
        }

        for e in self.entries.iter() {
            if &e.key == key {
                return Some(e.value_ref);
            }
        }

        None
    }
}
