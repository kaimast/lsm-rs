use crate::sstable::Key;

use std::sync::RwLock;

struct PendingEntry<K: Key> {
    key: K,
    value_pos: usize
}

struct Inner<K: Key> {
    entries: Vec<PendingEntry<K>>,
    size: usize
}

pub struct Memtable<K: Key> {
    inner: RwLock<Inner<K>>
}

impl<K: Key> Memtable<K> {
    pub fn new() -> Self {
        let entries = Vec::new();
        let size = 0;

        let inner = RwLock::new( Inner{entries, size} );

        Self{ inner }
    }

    pub fn get(&self, key: &K) -> Option<usize> {
        let inner = self.inner.read().unwrap();

        for e in inner.entries.iter() {
            if &e.key == key {
                return Some(e.value_pos);
            }
        }

        None
    }

    pub fn put(&self, key: K, value_pos: usize, value_len: usize) {
        let mut inner = self.inner.write().unwrap();

        inner.size += value_len;
        inner.entries.push(PendingEntry{ key, value_pos });
    }

    pub fn size(&self) -> usize {
        let inner = self.inner.read().unwrap();

        inner.size
    }
}
