#![ feature(trait_alias) ]

mod values;
use values::{Value, ValueLog};

mod sstable;
use sstable::Key;

use std::sync::Mutex;

struct PendingEntry<K: Key> {
    key: K,
    value_pos: usize
}

struct Memtable<K: Key> {
    entries: Mutex<Vec<PendingEntry<K>>>
}

pub struct Datastore<K: Key, V: Value> {
    memtable: Memtable<K>,
    value_log: ValueLog<V>
}

impl<K: Key> Memtable<K> {
    pub fn new() -> Self {
        let entries = Mutex::new( Vec::new() );
        Self{entries}
    }

    pub fn get(&self, key: &K) -> Option<usize> {
        let entries = self.entries.lock().unwrap();
        for e in entries.iter() {
            if &e.key == key {
                return Some(e.value_pos);
            }
        }

        None
    }

    pub fn put(&self, key: K, value_pos: usize) {
        let mut entries = self.entries.lock().unwrap();
        entries.push(PendingEntry{ key, value_pos });
    }
}

impl<K: Key, V: Value> Datastore<K, V> {
    pub fn new() -> Self {
        let memtable = Memtable::new();
        let value_log = ValueLog::new();

        Self{ memtable, value_log }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(pos) = self.memtable.get(key) {
            Some(self.value_log.get_pending(pos))
        } else {
            None
        }
    }

    pub fn put(&self, key: K, value: V) {
        let value_pos =  self.value_log.add_value(value);
        self.memtable.put(key, value_pos);
    }
}

#[cfg(test)]
mod tests {
    use super::Datastore;

    #[test]
    fn get_put() {
        let ds = Datastore::<String, String>::new();
        
        let key1 = String::from("Foo");
        let key2 = String::from("Foz");
        let value = String::from("Bar");

        assert_eq!(ds.get(&key1), None);

        ds.put(key1.clone(), value.clone());

        assert_eq!(ds.get(&key1), Some(value.clone()));
        assert_eq!(ds.get(&key2), None);
    }
}
