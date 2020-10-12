#![ feature(trait_alias) ]

mod values;
use values::{Value, ValueLog};

mod sstable;
use sstable::Key;

mod tasks;
use tasks::TaskManager;

mod memtable;
use memtable::Memtable;

use std::sync::{Arc, Mutex};

pub enum StartMode {
    CreateOrOpen,
    Open,
    CreateOrOverride
}

pub struct Params {
    max_memtable_size: usize
}

impl Default for Params {
    fn default() -> Self {
        Self {
            max_memtable_size: 8*1024*1024
        }
    }
}

pub struct Datastore<K: Key, V: Value> {
    memtable: Memtable<K>,
    imm_memtables: Mutex<Vec<Memtable<K>>>,
    value_log: ValueLog<V>,
    params: Arc<Params>
}


impl<K: Key, V: Value> Datastore<K, V> {
    pub fn new(mode: StartMode) -> Self {
        let params = Params::default();

        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Self {
        let memtable = Memtable::new();
        let imm_memtables = Mutex::new( Vec::new() );
        let value_log = ValueLog::new();
        let params = Arc::new(params);

        Self{ memtable, value_log, imm_memtables, params }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(pos) = self.memtable.get(key) {
            Some(self.value_log.get_pending(pos))
        } else {
            None
        }
    }

    pub fn put(&self, key: K, value: V) {
        let (value_pos, value_len) = self.value_log.add_value(value);

        self.memtable.put(key, value_pos, value_len);
    }

    pub fn needs_compaction(&self) -> bool {
        {
            let imm_mems = self.imm_memtables.lock().unwrap();

            if imm_mems.len() > 0 {
                return true;
            }
        }

        //TODO check levels

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SM: StartMode = StartMode::CreateOrOverride;

    #[test]
    fn get_put() {
        let ds = Datastore::<String, String>::new(SM);
        
        let key1 = String::from("Foo");
        let key2 = String::from("Foz");
        let value = String::from("Bar");

        assert_eq!(ds.get(&key1), None);

        ds.put(key1.clone(), value.clone());

        assert_eq!(ds.get(&key1), Some(value.clone()));
        assert_eq!(ds.get(&key2), None);
    }
}
