use crate::sorted_table::{SortedTable, Key, TableIterator, InternalIterator};
use crate::entry::Entry;
use crate::{Params, StartMode, KV_Trait, WriteBatch, WriteError, WriteOptions};
use crate::data_blocks::DataBlocks;
use crate::memtable::{Memtable, MemtableRef, ImmMemtableRef};
use crate::level::Level;
use crate::wal::WriteAheadLog;
use crate::manifest::Manifest;
use crate::iterate::DbIterator;

#[ cfg(feature="wisckey") ]
use crate::values::ValueLog;

use std::marker::PhantomData;
use std::collections::VecDeque;
use std::sync::{Arc, atomic};

use tokio::fs;
use tokio::sync::{RwLock, Mutex};

use crate::cond_var::Condvar;

#[ cfg(not(feature="wisckey")) ]
use bincode::Options;

#[ cfg(feature="wisckey") ]
use std::collections::HashSet;

pub struct DbLogic<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    #[allow(dead_code)]
    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<MemtableRef>,
    imm_memtables: Mutex<VecDeque<ImmMemtableRef>>,
    imm_cond: Condvar,
    wal: Mutex<WriteAheadLog>,
    levels: Vec<Level>,
    next_table_id: atomic::AtomicUsize,
    running: atomic::AtomicBool,
    data_blocks: Arc<DataBlocks>,

    #[ cfg(feature="wisckey") ]
    value_log: Arc<ValueLog>,

    #[ cfg(feature="wisckey") ]
    watched_keys: Mutex<HashSet<Key>>,

    #[ cfg(feature="wisckey") ]
    watched_keys_cond: Condvar,
}

impl<K: KV_Trait, V: KV_Trait>  DbLogic<K, V> {
    pub async fn new(start_mode: StartMode, params: Params) -> Self {
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
                    fs::remove_dir_all(&params.db_path).await.expect("Failed to remove existing database");
                }

                create = true;
            }
        }

        if create {
            match fs::create_dir(&params.db_path).await {
                Ok(()) => log::info!("Created database folder at \"{}\"", params.db_path.to_str().unwrap()),
                Err(e) => panic!("Failed to create DB folder: {}", e)
            }
        }

        let params = Arc::new(params);

        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let memtable = RwLock::new( MemtableRef::wrap( Memtable::new(1) ) );
        let imm_memtables = Mutex::new( VecDeque::new() );
        let imm_cond = Condvar::new();
        let next_table_id = atomic::AtomicUsize::new(1);
        let running = atomic::AtomicBool::new(true);
        let wal = Mutex::new(WriteAheadLog::new(params.clone()).await);
        let data_blocks = Arc::new( DataBlocks::new(params.clone(), manifest.clone()) );

        #[cfg(feature="wisckey") ]
        let value_log = Arc::new( ValueLog::new(params.clone(), manifest.clone()).await );

        #[cfg(feature="wisckey")]
        let watched_keys = Mutex::new( HashSet::new() );

        #[cfg(feature="wisckey") ]
        let watched_keys_cond = Condvar::new();

        if params.num_levels == 0 {
            panic!("Need at least one level!");
        }

        let mut levels = Vec::new();
        for index in 0..params.num_levels {
            let level = Level::new(index, data_blocks.clone(), params.clone(), manifest.clone());
            levels.push(level);
        }

        let _marker = PhantomData;

        Self {
            _marker, manifest, params, memtable, imm_memtables, imm_cond,
            wal, levels, next_table_id, running, data_blocks,
            #[ cfg(feature="wisckey") ] value_log,
            #[ cfg(feature="wisckey") ] watched_keys,
            #[ cfg(feature="wisckey") ] watched_keys_cond,
        }
    }

    #[cfg(feature="wisckey")]
    pub async fn garbage_collect(&self) {
        loop {
            let batch_id = self.value_log.get_oldest_batch_id().await;
            log::trace!("Checking if we can garbage collection batch #{}", batch_id);

            let mut watch_lock = self.watched_keys.lock().await;
            assert!(watch_lock.is_empty());

            loop {
                let mut keys = self.value_log.get_keys(batch_id).await;
                watch_lock.clear();

                //TODO only re-check keys that have changed
                for key in keys.drain() {
                    let mut found = false;

                    // We count all references because clients might
                    // still iterate over outdated values
                    for vref in self.get_value_refs(&key).await {
                        let (value_batch, _) = vref;

                        if batch_id == value_batch {
                            found = true;
                            break;
                        }
                    }

                    if found {
                        watch_lock.insert(key);
                    }
                }

                if watch_lock.is_empty() {
                    break;
                } else {
                    watch_lock = self.watched_keys_cond.wait(watch_lock, &self.watched_keys).await;
                }
            }

            // Batch is not needed anymore
            self.value_log.delete_batch(batch_id).await;
        }
    }

    #[cfg(feature="sync")]
    pub async fn iter(&self, tokio_rt: Arc<tokio::runtime::Runtime>) -> DbIterator<K, V> {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        {
            let memtable = self.memtable.read().await;
            let iter = memtable.clone_immutable().into_iter().await;
            mem_iters.push(iter);
        }

        for imm in self.imm_memtables.lock().await.iter() {
            let iter = imm.clone().into_iter().await;
            mem_iters.push(iter);
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                let iter = TableIterator::new(table.clone()).await;
                table_iters.push(iter);
            }
        }

        DbIterator::new(tokio_rt, mem_iters, table_iters, self.value_log.clone())
    }

    #[cfg(not(feature="sync"))]
    pub async fn iter(&self) -> DbIterator<K, V> {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        {
            let memtable = self.memtable.read().await;
            let imm_mems = self.imm_memtables.lock().await;

            mem_iters.push(
               memtable.clone_immutable().into_iter().await
            );

            for imm in imm_mems.iter() {
                let iter = imm.clone().into_iter().await;
                mem_iters.push(iter);
            }
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                let iter = TableIterator::new(table.clone()).await;
                table_iters.push(iter);
            }
        }

        #[ cfg(feature="wisckey") ]
        return DbIterator::new(mem_iters, table_iters, self.value_log.clone());

        #[ cfg(not(feature="wisckey")) ]
        return DbIterator::new(mem_iters, table_iters);
    }

    #[ cfg(feature="wisckey") ]
    pub async fn get(&self, key: &[u8]) -> Option<V> {
        log::trace!("Starting to seek for key `{:?}`", key);

        // special case for memtable to avoid race conditions
        {
            let memtable = self.memtable.read().await;

            if let Some(entry) = memtable.get().get(key) {
                match entry {
                    Entry::Value{value_ref, ..} => {
                        let val = self.value_log.get_pending(value_ref).await;
                        return Some(val);
                    }
                    Entry::Deletion{..} => { return None; }
                }
            };
        }

        if let Some(value_ref) = self.get_value_ref(key).await {
            Some( self.value_log.get(value_ref).await )
        } else {
            None
        }
    }

    /// Get the most recent value reference for a specific key
    /// - This does *not* include pending values
    #[ cfg(feature="wisckey") ]
    pub async fn get_value_ref(&self, key: &[u8]) -> Option<crate::values::ValueId> {
        {
            let imm_mems = self.imm_memtables.lock().await;

            for imm in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry {
                        Entry::Value{ value_ref, ..} => {
                            return Some(value_ref);
                        }
                        Entry::Deletion{..} => {
                            return None;
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(entry) = level.get(key).await {
                match entry {
                    Entry::Value{value_ref, ..} => {
                        return Some(value_ref);
                    }
                    Entry::Deletion{..} => {
                        return None;
                    }
                }
            }
        }

        None
    }

    /// Get all values associated for a specific key
    /// - This does *not* include pending values
    #[ cfg(feature="wisckey") ]
    pub async fn get_value_refs(&self, key: &[u8]) -> Vec<crate::values::ValueId> {
        let mut result = vec![];

        {
            let imm_mems = self.imm_memtables.lock().await;

            for imm in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry {
                        Entry::Value{ value_ref, ..} => {
                            result.push(value_ref);
                        }
                        Entry::Deletion{..} => {}
                    }
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(entry) = level.get(key).await {
                match entry {
                    Entry::Value{value_ref, ..} => {
                        result.push(value_ref);
                    }
                    Entry::Deletion{..} => {}
                }
            }
        }

        result
    }

    #[ cfg(not(feature="wisckey")) ]
    pub async fn get(&self, key: &[u8]) -> Option<V> {
        log::trace!("Starting to seek for key `{:?}`", key);

        let memtable = self.memtable.read().await;
        let encoder = crate::get_encoder();

        if let Some(entry) = memtable.get().get(key) {
            match entry {
                Entry::Value{value, ..} => {
                    let value = encoder.deserialize(&value).unwrap();
                    return Some(value);
                }
                Entry::Deletion{..} => { return None; }
            }
        };

        {
            let imm_mems = self.imm_memtables.lock().await;

            for imm in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry {
                        Entry::Value{ value, ..} => {
                            let value = encoder.deserialize(&value).unwrap();
                            return Some(value);
                        }
                        Entry::Deletion{..} => {
                            return None;
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(entry) = level.get(key).await {
                match entry {
                    Entry::Value{value, ..} => {
                        let value = encoder.deserialize(&value).unwrap();
                        return Some(value);
                    }
                    Entry::Deletion{..} => {
                        return None;
                    }
                }
            }

        }

        None
    }

    pub async fn write_opts(&self, mut write_batch: WriteBatch<K, V>, opt: &WriteOptions) -> Result<bool, WriteError> {
        let mut memtable = self.memtable.write().await;
        let mem_inner = unsafe{ memtable.get_mut() };

        let mut wal = self.wal.lock().await;

        #[cfg(feature="wisckey")]
        let mut vlog_info = Vec::new();

        #[cfg(feature="wisckey")]
        {
            let watched_keys = self.watched_keys.lock().await;
            let mut watch_triggered = false;

            for op in write_batch.writes.drain(..) {
                wal.store(&op).await;

                if watched_keys.contains(op.get_key()) {
                    watch_triggered = true;
                }

                match op {
                    crate::WriteOp::Put(key, value) => {
                        let vinfo = self.value_log.add_value(&key, value).await;
                        vlog_info.push((key, Some(vinfo)));
                    }
                    crate::WriteOp::Delete(key) => {
                        vlog_info.push((key, None));
                    }
                }
            }

            if watch_triggered {
                self.watched_keys_cond.notify_all();
            }
        }

        #[ cfg(not(feature="wisckey")) ]
        for op in write_batch.writes.iter() {
            wal.store(op).await;
        }

        if opt.sync {
            wal.sync().await;
            #[cfg(feature="wisckey")]
            self.value_log.sync().await;
        }
        drop(wal);

        #[ cfg(feature="wisckey") ]
        for (key, vref) in vlog_info.drain(..) {
            if let Some(vinfo) = vref {
                log::trace!("Storing new value for key `{:?}`", key);
                let (value_pos, value_len) = vinfo;
                mem_inner.put(key, value_pos, value_len);
            } else {
                log::trace!("Storing deletion for key `{:?}`", key);
                mem_inner.delete(key);
            }
        }

        #[ cfg(not(feature="wisckey")) ]
        for op in write_batch.writes.drain(..) {
            match op {
                crate::WriteOp::Put(key, value) => {
                    log::trace!("Storing new value for key `{:?}`", key);
                    mem_inner.put(key, value);
                }
                crate::WriteOp::Delete(key) => {
                    log::trace!("Storing deletion for key `{:?}`", key);
                    mem_inner.delete(key);
                }
            }
        }

        if mem_inner.is_full(&*self.params) {
            let mut imm_mems = self.imm_memtables.lock().await;

            let next_seq_num = mem_inner.get_next_seq_number();
            let imm = memtable.take(next_seq_num);

            #[ cfg(feature="wisckey") ]
            self.value_log.next_pending().await;

            drop(memtable);

            // Currently only one immutable memtable is supported
            while !imm_mems.is_empty() {
                imm_mems = self.imm_cond.wait(imm_mems, &self.imm_memtables).await;
            }

            imm_mems.push_back(imm);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(atomic::Ordering::SeqCst)
    }

    /// Do compaction if necessary
    /// Returns true if any work was done
    pub async fn do_compaction(&self) -> bool {
        // memtable to immutable memtable compaction
        {
            let mut imm_mems = self.imm_memtables.lock().await;

            if let Some(mem) = imm_mems.pop_front() {
                let table_id = self.next_table_id.fetch_add(1, atomic::Ordering::SeqCst);

                let l0 = self.levels.get(0).unwrap();
                l0.create_l0_table(table_id, mem.get().get_entries()).await;

                log::debug!("Created new L0 table");
                self.imm_cond.notify_all();
                return true;
            }
        }

        // level-to-level compaction
        for (level_pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if level_pos < self.params.num_levels-1 && level.needs_compaction().await {
                self.compact(level_pos, level).await;
                return true;
            }
        }

        false
    }

    async fn compact(&self, level_pos: usize, level: &Level) {
        let (offsets, mut tables) = level.start_compaction().await;
        assert!(!tables.is_empty());

        let mut min = tables[0].get_min();
        let mut max = tables[0].get_max();

        if tables.len() > 1 {
            for table in tables[1..].iter() {
                min = std::cmp::min(min, table.get_min());
                max = std::cmp::max(max, table.get_max());
            }
        }

        let overlaps = self.levels[level_pos+1].get_overlaps(&min, &max).await;

        // Fast path
        if tables.len() == 1 && overlaps.is_empty() {
            let mut parent_tables = level.get_tables().await;
            let mut child_tables = self.levels[level_pos+1].get_tables().await;

            log::debug!("Moving table from level {} to level {}", level_pos, level_pos+1);
            let table = tables.remove(0);

            let mut new_pos = 0;
            for (pos, other_table) in child_tables.iter().enumerate() {
                if other_table.get_min() > table.get_min() {
                    new_pos = pos;
                    break;
                }
            }

            child_tables.insert(new_pos, table);
            parent_tables.remove(offsets[0]);

            log::trace!("Done moving table");
            return
        }

        log::debug!("Compacting {} table(s) in level {} with {} table(s) in level {}", tables.len(), level_pos, overlaps.len(), level_pos+1);

        //Merge
        let mut entries = Vec::new();

        for (_, table) in overlaps.iter() {
            min = std::cmp::min(min, table.get_min());
            max = std::cmp::max(max, table.get_max());
        }

        // Table can potentially contain a single entry
        assert!(min <= max);

        let min = min.to_vec();
        let max = max.to_vec();

        let mut table_iters = Vec::new();
        for table in tables.drain(..) {
            table_iters.push(TableIterator::new(table.clone()).await);
        }

        for (_, child) in overlaps.iter() {
            table_iters.push(TableIterator::new(child.clone()).await);
        }

        let mut last_key: Option<Key> = None;

        #[ cfg(feature="wisckey") ]
        let watched_keys = self.watched_keys.lock().await;

        #[ cfg(feature="wisckey") ]
        let mut watch_triggered = false;

        loop {
            log::trace!("Starting compaction for next key");
            let mut min_key = None;

            for table_iter in table_iters.iter_mut() {
                // Advance the iterator, if needed
                if let Some(last_key) = &last_key {
                    while !table_iter.at_end() && table_iter.get_key() <= last_key {
                        table_iter.step().await;
                    }
                }

                if !table_iter.at_end() {
                    if let Some(key) = min_key {
                        min_key = Some(std::cmp::min(table_iter.get_key(), key));
                    } else {
                        min_key = Some(table_iter.get_key());
                    }
                }
            }

            if min_key.is_none() {
                break;
            }

            let mut min_entry: Option<&Entry> = None;
            let min_key = min_key.unwrap().clone();

            for table_iter in table_iters.iter_mut() {
                if table_iter.at_end() {
                    continue;
                }

                // Figure out if this table's entry is more recent
                let key = table_iter.get_key();
                let entry = table_iter.get_entry();

                if key != &min_key {
                    continue;
                }

                if let Some(other_entry) = min_entry {
                    if entry.get_sequence_number() > other_entry.get_sequence_number() {
                        log::trace!("Overriding key {:?}: new seq #{}, old seq #{}", key, entry.get_sequence_number(), other_entry.get_sequence_number());

                        min_entry = Some(entry);

                        #[ cfg(feature="wisckey") ]
                        if watched_keys.contains(key) {
                            watch_triggered = true;
                        }
                    }
                } else {
                    log::trace!("Found new key {:?}", key);
                    min_entry = Some(entry);
                }
            }

            entries.push((min_key.clone(), min_entry.unwrap().clone()));
            last_key = Some(min_key);
        }

        let id = self.manifest.next_table_id().await;

        let new_table = SortedTable::new(id, entries, min, max, self.data_blocks.clone(), &*self.params).await;

        let add_set = vec![(level_pos+1, id)];
        let mut remove_set = vec![];

        // Install new tables atomically
        let mut parent_tables = level.get_tables().await;
        let mut child_tables = self.levels[level_pos+1].get_tables().await;

        // iterate backwards to ensure oldest entries are removed first
        for (offset, _) in overlaps.iter().rev() {
            let id = child_tables.remove(*offset).get_id();
            remove_set.push((level_pos+1, id));
        }

        let mut new_pos = child_tables.len(); // insert at the end by default
        for (pos, other_table) in child_tables.iter().enumerate() {
            if other_table.get_min() > new_table.get_min() {
                new_pos = pos;
                break;
            }
        }

        child_tables.insert(new_pos, Arc::new(new_table));

        for offset in offsets.iter().rev() {
            let id = parent_tables.remove(*offset).get_id();
            remove_set.push((level_pos, id));
        }

        self.manifest.update_table_set(add_set, remove_set).await;

        #[ cfg(feature="wisckey") ]
        if watch_triggered {
            self.watched_keys_cond.notify_all();
        }

        log::trace!("Done compacting tables");
    }

    #[ allow(dead_code) ]
    pub async fn needs_compaction(&self) -> bool {
        {
            let imm_mems = self.imm_memtables.lock().await;

            if !imm_mems.is_empty() {
                return true;
            }
        }

        for (pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if pos < self.params.num_levels-1 && level.needs_compaction().await {
                return true;
            }
        }

        false
    }

    pub fn stop(&self) {
        self.running.store(false, atomic::Ordering::SeqCst);
    }
}

