// For the Key and Value traits
#![ feature(trait_alias) ]

// For writing to the log
#![ feature(write_all_vectored) ]
#![ feature(array_methods) ]

use std::sync::{Arc, Condvar, Mutex, RwLock, atomic};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

mod entry;

mod data_blocks;
use data_blocks::{DataBlocks};

mod values;
use values::{Value, ValueLog};

mod sorted_table;
use sorted_table::{SortedTable, Key};

#[cfg(feature="compaction")]
mod tasks;
#[cfg(feature="compaction")]
use tasks::TaskManager;

mod memtable;
use memtable::Memtable;

mod level;
use level::Level;

mod wal;
use wal::WriteAheadLog;

mod manifest;
use manifest::Manifest;

pub struct WriteBatch<K: Key> {
    writes: Vec<(K, Value)>
}

impl<K: Key> WriteBatch<K> {
    pub fn new() -> Self {
        Self{ writes: Vec::new() }
    }

    pub fn put<V: serde::Serialize>(&mut self, key: K, value: &V) {
        let vdata = bincode::serialize(value).expect("Failed to serialize value");
        self.writes.push((key, vdata));
    }
}

impl<K: Key> Default for WriteBatch<K> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WriteOptions {
    pub sync: bool
}

impl WriteOptions {
    pub const fn new() -> Self {
        Self{ sync: true }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub enum StartMode {
    CreateOrOpen,
    Open,
    CreateOrOverride
}

pub struct Params {
    pub db_path: PathBuf,
    pub max_memtable_size: usize,
    pub num_levels: usize
}

impl Default for Params {
    fn default() -> Self {
        Self {
            db_path: Path::new("./storage.lsm").to_path_buf(),
            max_memtable_size: 64*1024,
            num_levels: 5,
        }
    }
}

pub struct Datastore<K: Key> {
    inner: Arc<DbLogic<K>>,
    #[ cfg(feature="compaction") ]
    tasks: Arc<TaskManager<K>>
}

pub struct DbLogic<K: Key> {
    #[allow(dead_code)]
    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<Memtable<K>>,
    imm_memtables: Mutex<VecDeque<Memtable<K>>>,
    imm_cond: Condvar,
    value_log: ValueLog,
    wal: Mutex<WriteAheadLog>,
    levels: Vec<Level<K>>,
    next_table_id: atomic::AtomicUsize,
    running: atomic::AtomicBool,
    data_blocks: Arc<DataBlocks>
}

impl<K: 'static+Key> Datastore<K> {
    pub fn new(mode: StartMode) -> Self {
        let params = Params::default();
        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Self {
        let inner = Arc::new( DbLogic::new(mode, params) );

        #[ cfg(feature="compaction") ]
        let tasks = Arc::new( TaskManager::new(inner.clone()) );

        #[ cfg(feature="compaction") ]
        let tasks_cpy = tasks.clone();

        #[ cfg(feature="compaction") ]
        std::thread::spawn(move ||{
            TaskManager::work_loop(tasks_cpy);
        });

        #[ cfg(feature="compaction") ]
        return Self{ inner, tasks };
        #[ cfg(not(feature="compaction")) ]
        return Self{ inner };
    }

    /// Will deserialize V from the raw data (avoids an additional copy)
    pub fn get<V: serde::de::DeserializeOwned>(&self, key: &K) -> Option<V> {
        self.inner.get::<V>(key)
    }

    pub fn put<V: serde::Serialize>(&self, key: K, value: &V) {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS);
    }

    pub fn put_opts<V: serde::Serialize>(&self, key: K, value: &V, opts: &WriteOptions) {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts);
    }

    pub fn write(&self, write_batch: WriteBatch<K>) {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS);
    }

    pub fn write_opts(&self, write_batch: WriteBatch<K>, opts: &WriteOptions) {
        let needs_compaction = self.inner.write_opts(write_batch, opts);

        if needs_compaction {
            #[ cfg(feature="compaction") ]
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
        let imm_cond = Condvar::new();
        let value_log = ValueLog::new(params.clone());
        let next_table_id = atomic::AtomicUsize::new(1);
        let running = atomic::AtomicBool::new(true);
        let wal = Mutex::new(WriteAheadLog::new(params.clone()));
        let data_blocks = Arc::new( DataBlocks::new(params.clone()) );

        if params.num_levels == 0 {
            panic!("Need at least one level!");
        }

        let mut levels = Vec::new();
        for index in 0..params.num_levels {
            levels.push(Level::new(index, data_blocks.clone()));
        }

        Self {
            manifest, memtable, value_log, wal, imm_memtables, imm_cond,
            params, running, levels, next_table_id, data_blocks
        }
    }

    pub fn get<V: serde::de::DeserializeOwned>(&self, key: &K) -> Option<V> {
        let memtable = self.memtable.read().unwrap();

        if let Some(val_ref) = memtable.get(key) {
            return Some(self.value_log.get_pending(&val_ref));
        }

        {
            let imm_mems = self.imm_memtables.lock().unwrap();
            for imm in imm_mems.iter().rev() {
                if let Some(val_ref) = imm.get(key) {
                    return Some(self.value_log.get_pending(&val_ref));
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(val_ref) = level.get(key) {
               return Some(self.value_log.get(&val_ref));
            }
        }

        None
    }

    pub fn write_opts(&self, mut write_batch: WriteBatch<K>, opt: &WriteOptions) -> bool {
        let mut memtable = self.memtable.write().unwrap();
        let mut wal = self.wal.lock().unwrap();

        for (key, value) in write_batch.writes.iter() {
            wal.store(key, value);
        }
        if opt.sync {
            wal.sync();
        }
        drop(wal);

        for (key, value) in write_batch.writes.drain(..) {
            let (value_pos, value_len) = self.value_log.add_value(value);
            memtable.put(key, value_pos, value_len);
        }

        if let Some(imm) = memtable.maybe_seal(&self.params) {
            let mut imm_mems = self.imm_memtables.lock().unwrap();

            // Currently only one immutable memtable is supported
            #[ cfg(feature="compaction") ]
            while !imm_mems.is_empty() {
                imm_mems = self.imm_cond.wait(imm_mems).unwrap();
            }

            self.value_log.next_pending();
            imm_mems.push_back(imm);

            true
        } else {
            false
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(atomic::Ordering::SeqCst)
    }

    /// Do compaction if necessary
    /// Returns true if any work was done
    pub fn do_compaction(&self) -> bool {
        {
            let mut imm_mems = self.imm_memtables.lock().unwrap();

            if let Some(mem) = imm_mems.pop_front() {
                self.value_log.flush_pending();

                let table_id = self.next_table_id.fetch_add(1, atomic::Ordering::SeqCst);

                let l0 = self.levels.get(0).unwrap();
                l0.create_l0_table(table_id, mem.take());

                log::debug!("Created new L0 table");
                self.imm_cond.notify_all();
                return true;
            }
        }

        // level-to-level compaction
        for (level_pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if level_pos < self.params.num_levels-1 && level.needs_compaction() {
                self.compact(level_pos, level);
                return true;
            }
        }

        false
    }

    fn compact(&self, level_pos: usize, level: &Level<K>) {
        let (offset, table) = level.start_compaction();
        let overlaps = self.levels[level_pos+1].get_overlaps(&*table);

        // Fast path
        if overlaps.is_empty() {
            let mut parent_tables = level.get_tables();
            let mut child_tables = self.levels[level_pos+1].get_tables();

            log::debug!("Moving table from level {} to level {}", level_pos, level_pos+1);

            let mut new_pos = 0;
            for (pos, other_table) in child_tables.iter().enumerate() {
                if other_table.get_min() > table.get_min() {
                    new_pos = pos;
                    break;
                }
            }

            parent_tables.remove(offset);
            child_tables.insert(new_pos, table);
            return
        }

        log::debug!("Compacting one table in level {} with {} table(s) in level {}", level_pos, overlaps.len(), level_pos+1);

        //Merge
        let mut entries = Vec::new();
        let mut min = table.get_min();
        let mut max = table.get_max();

        for (_, table) in overlaps.iter() {
            min = std::cmp::min(min, table.get_min());
            max = std::cmp::max(max, table.get_max());
        }

        assert!(min < max);

        let min = min.clone();
        let max = max.clone();

        let mut parent_iter = table.iter();
        let mut child_iters = Vec::new();
        let mut pos = min.clone();

        for (_, child) in overlaps.iter() {
            child_iters.push(child.iter());
        }

        let mut child_iters_iter = child_iters.iter_mut();
        let mut child_iter = child_iters_iter.next();

        let mut next_parent_key = Some(parent_iter.get_key());

        //TODO None means there are no overlapping tables at the lower level
        // add a fast path for this case
        let mut next_child_key = if let Some(child) = child_iter.as_ref() {
            Some(child.get_key())
        } else { 
            None
        };

        loop {
            let mut entry = None;

            if let Some(key) = &next_parent_key {
                if key == &pos {
                    entry = Some(parent_iter.get_entry());

                    if parent_iter.at_end() {
                        next_parent_key = None;
                    } else {
                        parent_iter.step();
                        next_parent_key = Some(parent_iter.get_key());
                    }
                }
            }

            // Tables in the next level are guaranteed to not overlap
            // So we only need to look at one table at a time
            if let Some(iter) = &mut child_iter {
                if let Some(key) = &next_child_key {
                    if key == &pos {
                        let child_entry = iter.get_entry();

                        if let Some(e) = &entry {
                            if e.seq_number < child_entry.seq_number {
                                entry = Some(child_entry);
                            }
                        } else {
                            entry = Some(child_entry);
                        }

                        if iter.at_end() {
                            child_iter = child_iters_iter.next();
                        } else {
                            iter.step();
                        }

                        if let Some(next_iter) = &mut child_iter {
                            next_child_key = Some(next_iter.get_key());
                        } else {
                            next_child_key = None;
                        }
                    }
                }
            }

            let at_end = pos == max;

            entries.push((pos, entry.unwrap()));

            if at_end {
                //Done
                break;
            } else {
                if next_child_key.is_some() && next_parent_key.is_some() {
                    pos = std::cmp::min(next_child_key.as_ref().unwrap(), next_parent_key.as_ref().unwrap()).clone();
                } else if next_child_key.is_some() {
                    pos = next_child_key.as_ref().unwrap().clone()
                } else if next_parent_key.is_some() {
                    pos = next_parent_key.as_ref().unwrap().clone()
                } else {
                    panic!("Invalid state");
                }
            }
        }

        let new_table = SortedTable::new_from_sorted(entries, min, max, self.data_blocks.clone());

        // Install new tables atomically
        let mut parent_tables = level.get_tables();
        let mut child_tables = self.levels[level_pos+1].get_tables();

        // iterate backwards to ensure 
        for (offset, _) in overlaps.iter().rev() {
            child_tables.remove(*offset);
        }

        let mut new_pos = 0;
        for (pos, other_table) in child_tables.iter().enumerate() {
            if other_table.get_min() > new_table.get_min() {
                new_pos = pos;
                break;
            }
        }

        child_tables.insert(new_pos, Arc::new(new_table));
        parent_tables.remove(offset);
    }

    pub fn needs_compaction(&self) -> bool {
        {
            let imm_mems = self.imm_memtables.lock().unwrap();

            if !imm_mems.is_empty() {
                return true;
            }
        }

        for (pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if pos < self.params.num_levels-1 && level.needs_compaction() {
                return true;
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
        let value2 = String::from("Baz");

        assert_eq!(ds.get::<String>(&key1), None);
        assert_eq!(ds.get::<String>(&key2), None);

        ds.put(key1.clone(), &value);

        assert_eq!(ds.get::<String>(&key1), Some(value.clone()));
        assert_eq!(ds.get::<String>(&key2), None);

        ds.put(key1.clone(), &value2);
        assert_eq!(ds.get::<String>(&key1), Some(value2.clone()));
    }

    #[test]
    fn get_put_many() {
        const COUNT: usize = 100_000;

        test_init();
        let ds = Datastore::<usize>::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(key, &value, &options);
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get::<String>(&pos), Some(format!("some_string_{}", pos)));
        }
    }

    #[test]
    fn override_many() {
        const COUNT: usize = 100_000;

        test_init();
        let ds = Datastore::<usize>::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(key, &value, &options);
        }

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_other_string_{}", pos);
            ds.put_opts(key, &value, &options);
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get::<String>(&pos), Some(format!("some_other_string_{}", pos)));
        }
    }


    #[test]
    fn batched_write() {
        const COUNT: usize = 1000;

        test_init();
        let ds = Datastore::<String>::new(SM);
        let mut batch = WriteBatch::new();

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            batch.put(key, &pos);
        }

        ds.write(batch);

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            assert_eq!(ds.get::<usize>(&key), Some(pos));
        }
    }

}
