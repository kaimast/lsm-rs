#![ feature(trait_alias) ]

use std::thread;
use std::sync::{Arc, Mutex, RwLock, atomic};
use std::collections::VecDeque;

mod entry;

mod values;
use values::{Value, ValueLog};

mod sorted_table;
use sorted_table::Key;

mod tasks;
use tasks::TaskManager;

mod memtable;
use memtable::Memtable;

mod level;
use level::Level;

mod operation;

mod wal;
use wal::WriteAheadLog;

pub enum StartMode {
    CreateOrOpen,
    Open,
    CreateOrOverride
}

pub struct Params {
    max_memtable_size: usize,
    num_levels: usize,
    data_prefix: String
}

impl Default for Params {
    fn default() -> Self {
        Self {
            max_memtable_size: 8*1024*1024,
            num_levels: 5,
            data_prefix: String::from("./data/")
        }
    }
}

pub struct Datastore<K: Key, V: Value> {
    inner: Arc<DbLogic<K, V>>,
    tasks: Arc<TaskManager<K, V>>
}

pub struct DbLogic<K: Key, V: Value> {
    memtable: RwLock<Memtable<K>>,
    imm_memtables: Mutex<VecDeque<Memtable<K>>>,
    value_log: ValueLog<V>,
    wal: Mutex<WriteAheadLog>,
    params: Arc<Params>,
    levels: Vec<Level<K>>,
    next_table_id: atomic::AtomicUsize,
    running: atomic::AtomicBool
}

impl<K: 'static+Key, V: 'static+Value> Datastore<K, V> {
    pub fn new(mode: StartMode) -> Self {
        let params = Params::default();
        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Self {
        let inner = Arc::new( DbLogic::new(mode, params) );
        let tasks = Arc::new( TaskManager::new(inner.clone()) );
        let tasks_cpy = tasks.clone();

        thread::spawn(move ||{
            TaskManager::work_loop(tasks_cpy);
        });

        Self{ inner, tasks }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key)
    }

    pub fn put(&self, key: K, value: V) {
        let needs_compaction = self.inner.put(key, value);

        if needs_compaction {
            self.tasks.wake_up();
        }
    }
}

impl<K: Key, V: Value> Drop for Datastore<K, V> {
    fn drop(&mut self) {
        self.inner.stop();
    }
}

impl<K: Key, V: Value> DbLogic<K, V> {
    pub fn new(_sm: StartMode, params: Params) -> Self {
        let memtable = RwLock::new( Memtable::new() );
        let imm_memtables = Mutex::new( VecDeque::new() );
        let value_log = ValueLog::new();
        let params = Arc::new(params);
        let next_table_id = atomic::AtomicUsize::new(1);
        let running = atomic::AtomicBool::new(true);
        let wal = Mutex::new(WriteAheadLog::new());

        if params.num_levels == 0 {
            panic!("Need at least one level!");
        }

        let mut levels = Vec::new();
        for _ in 0..params.num_levels {
            levels.push(Level::new());
        }

        Self{ memtable, value_log, wal, imm_memtables, params, running, levels, next_table_id }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let memtable = self.memtable.read().unwrap();

        if let Some(val_ref) = memtable.get(key) {
            return Some(self.value_log.get_pending(&val_ref));
        }

        for level in self.levels.iter() {
            if let Some(val_ref) = level.get(key) {
               return Some(self.value_log.get(&val_ref));
            }
        }

        None
    }

    pub fn put(&self, key: K, value: V) -> bool {
        let mut memtable = self.memtable.write().unwrap();
        self.wal.lock().unwrap().store(&key, &value);

        let (value_pos, value_len) = self.value_log.add_value(value);
        memtable.put(key, value_pos, value_len);

        if let Some(imm) = memtable.maybe_seal(&self.params) {
            let mut imm_mems = self.imm_memtables.lock().unwrap();
            imm_mems.push_back(imm);

            true
        } else {
            false
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(atomic::Ordering::SeqCst)
    }

    pub fn do_compaction(&self) -> bool {
        let imm_mems = self.imm_memtables.lock().unwrap();

        if imm_mems.len() > 0 {
            true
        } else {
            false
        }
    }

    pub fn needs_compaction(&self) -> bool {
        {
            let mut imm_mems = self.imm_memtables.lock().unwrap();

            if let Some(mem) = imm_mems.pop_front() {
                self.value_log.flush_pending();

                let table_id = self.next_table_id.fetch_add(1, atomic::Ordering::SeqCst);

                let l0 = self.levels.get(0).unwrap();
                l0.create_table(table_id, &self.params.data_prefix, mem.take());
                return true;
            }
        }

        {
            for level in self.levels.iter() {
                if level.needs_compaction() {
                    return true;
                }
            }
        }

        false
    }

    pub fn stop(&self) {
        self.running.store(false, atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SM: StartMode = StartMode::CreateOrOverride;

    fn test_init() {
         let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn get_put() {
        test_init();
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
