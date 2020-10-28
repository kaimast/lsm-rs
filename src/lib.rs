// For the Key and Value traits
#![ feature(trait_alias) ]

// For writing to the log
#![ feature(write_all_vectored) ]
#![ feature(array_methods) ]

use std::thread;
use std::sync::{Arc, Mutex, RwLock, atomic};
use std::collections::VecDeque;
use std::path::Path;

mod entry;

mod data_blocks;
use data_blocks::{DataBlocks};

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

mod wal;
use wal::WriteAheadLog;

mod manifest;
use manifest::Manifest;

pub enum StartMode {
    CreateOrOpen,
    Open,
    CreateOrOverride
}

pub struct Params {
    db_path: &'static Path,
    max_memtable_size: usize,
    num_levels: usize
}

impl Default for Params {
    fn default() -> Self {
        Self {
            db_path: Path::new("./storage.lsm"),
            max_memtable_size: 64*1024,
            num_levels: 5,
        }
    }
}

pub struct Datastore<K: Key> {
    inner: Arc<DbLogic<K>>,
    tasks: Arc<TaskManager<K>>
}

pub struct DbLogic<K: Key> {
    #[allow(dead_code)]
    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<Memtable<K>>,
    imm_memtables: Mutex<VecDeque<Memtable<K>>>,
    value_log: ValueLog,
    wal: Mutex<WriteAheadLog>,
    levels: Vec<Level<K>>,
    next_table_id: atomic::AtomicUsize,
    running: atomic::AtomicBool
}

impl<K: 'static+Key> Datastore<K> {
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

    /// Will deserialize V from the raw data (avoids an additional copy)
    pub fn get<V: serde::de::DeserializeOwned>(&self, key: &K) -> Option<V> {
        self.inner.get::<V>(key)
    }

    pub fn put<V: serde::Serialize>(&self, key: K, value: &V) {
        let vdata = bincode::serialize(&value).expect("Failed to serialize value");
        let needs_compaction = self.inner.put(key, vdata);

        if needs_compaction {
            self.tasks.wake_up();
        }
    }
}

impl<K: Key> Drop for Datastore<K> {
    fn drop(&mut self) {
        self.inner.stop();
    }
}

impl<K: Key> DbLogic<K> {
    pub fn new(start_mode: StartMode, params: Params) -> Self {
        let create;

        if params.db_path.components().next().is_none() {
            panic!("DB path must not be empty!");
        }

        if params.db_path.exists() && !params.db_path.is_dir() {
            panic!("DB path must be a folder!");
        }

        match start_mode {
            StartMode::CreateOrOpen => {
                create = !params.db_path.exists();
            },
            StartMode::Open => {
                if !params.db_path.exists() {
                    panic!("DB does not exist");
                }
                create = false;
            },
            StartMode::CreateOrOverride => {
                if params.db_path.exists() {
                    log::info!("Removing old data at \"{}\"", params.db_path.to_str().unwrap());
                    std::fs::remove_dir_all(&params.db_path).expect("Failed to remove existing database");
                }

                create = true;
            }
        }

        if create {
            match std::fs::create_dir(&params.db_path) {
                Ok(()) => log::info!("Created database folder at \"{}\"", params.db_path.to_str().unwrap()),
                Err(e) => panic!("Failed to create DB folder: {}", e)
            }
        }

        let params = Arc::new(params);

        let manifest = Arc::new(Manifest::new(params.clone()));
        manifest.store();

        let memtable = RwLock::new( Memtable::new() );
        let imm_memtables = Mutex::new( VecDeque::new() );
        let value_log = ValueLog::new();
        let next_table_id = atomic::AtomicUsize::new(1);
        let running = atomic::AtomicBool::new(true);
        let wal = Mutex::new(WriteAheadLog::new(params.clone()));
        let data_blocks = Arc::new( DataBlocks::new(params.clone()) );

        if params.num_levels == 0 {
            panic!("Need at least one level!");
        }

        let mut levels = Vec::new();
        for _ in 0..params.num_levels {
            levels.push(Level::new(data_blocks.clone()));
        }

        Self {
            manifest, memtable, value_log, wal, imm_memtables,
            params, running, levels, next_table_id
        }
    }

    pub fn get<V: serde::de::DeserializeOwned>(&self, key: &K) -> Option<V> {
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

    pub fn put(&self, key: K, value: Value) -> bool {
        let mut memtable = self.memtable.write().unwrap();
        let mut wal = self.wal.lock().unwrap();
        wal.store(&key, &value);
        wal.sync(); //TODO add a flag for this
        drop(wal);

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
        {
            let mut imm_mems = self.imm_memtables.lock().unwrap();

            if let Some(mem) = imm_mems.pop_front() {
                self.value_log.flush_pending();

                let table_id = self.next_table_id.fetch_add(1, atomic::Ordering::SeqCst);

                let l0 = self.levels.get(0).unwrap();
                l0.create_table(table_id, mem.take());

                log::debug!("Created new L0 table");
                return true;
            }
        }

        //TODO level-to-level compaction

        true
    }

    pub fn needs_compaction(&self) -> bool {
        {
            let imm_mems = self.imm_memtables.lock().unwrap();

            if !imm_mems.is_empty() {
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
        let ds = Datastore::<String>::new(SM);
        
        let key1 = String::from("Foo");
        let key2 = String::from("Foz");
        let value = String::from("Bar");

        assert_eq!(ds.get::<String>(&key1), None);
        assert_eq!(ds.get::<String>(&key2), None);

        ds.put(key1.clone(), &value);

        assert_eq!(ds.get::<String>(&key1), Some(value.clone()));
        assert_eq!(ds.get::<String>(&key2), None);
    }

    #[test]
    fn get_put_many() {
        const COUNT: usize = 10_000;

        test_init();
        let ds = Datastore::<String>::new(SM);

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            ds.put(key, &pos);
        }

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            assert_eq!(ds.get::<usize>(&key), Some(pos));
        }
    }
}
