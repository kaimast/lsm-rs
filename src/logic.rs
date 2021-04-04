use crate::sorted_table::{SortedTable, Key, TableIterator, InternalIterator};
use crate::values::ValueLog;
use crate::entry::Entry;
use crate::{Params, StartMode, KV_Trait, WriteBatch, WriteError, WriteOptions};
use crate::data_blocks::DataBlocks;
use crate::memtable::{Memtable, MemtableRef, ImmMemtableRef};
use crate::level::Level;
use crate::wal::WriteAheadLog;
use crate::manifest::Manifest;
use crate::iterate::DbIterator;

use std::marker::PhantomData;
use std::collections::VecDeque;
use std::sync::{Arc, atomic};

use tokio::fs;
use tokio::sync::{RwLock, Mutex};

use crate::cond_var::Condvar;

pub struct DbLogic<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    #[allow(dead_code)]
    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<MemtableRef>,
    imm_memtables: Mutex<VecDeque<ImmMemtableRef>>,
    imm_cond: Condvar,
    value_log: Arc<ValueLog>,
    wal: Mutex<WriteAheadLog>,
    levels: Vec<Level>,
    next_table_id: atomic::AtomicUsize,
    running: atomic::AtomicBool,
    data_blocks: Arc<DataBlocks>
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
        let value_log = Arc::new( ValueLog::new(params.clone(), manifest.clone()).await );
        let next_table_id = atomic::AtomicUsize::new(1);
        let running = atomic::AtomicBool::new(true);
        let wal = Mutex::new(WriteAheadLog::new(params.clone()));
        let data_blocks = Arc::new( DataBlocks::new(params.clone(), manifest.clone()) );

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
            value_log, wal, levels, next_table_id, running, data_blocks
        }
    }

    #[cfg(feature="sync")]
    pub async fn iter(&self, tokio_rt: Arc<tokio::runtime::Runtime>) -> DbIterator<K, V> {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        mem_iters.push(self.memtable.read().await.clone_immutable().into_iter());

        for imm in self.imm_memtables.lock().await.iter() {
            mem_iters.push(imm.clone().into_iter());
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                table_iters.push(TableIterator::new(table.clone()));
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
               memtable.clone_immutable().into_iter()
            );

            for imm in imm_mems.iter() {
                mem_iters.push(imm.clone().into_iter());
            }
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                table_iters.push(TableIterator::new(table.clone()));
            }
        }

        DbIterator::new(mem_iters, table_iters, self.value_log.clone())
    }

    pub async fn get(&self, key: &[u8]) -> Option<V> {
        log::trace!("Starting to seek for key `{:?}`", key);

        let mut vref = None;
        let memtable = self.memtable.read().await;

        if let Some(e) = memtable.get().get(key) {
            vref = Some(e.value_ref);
        };

        if let Some(vref) = vref {
            return Some(self.value_log.get_pending(vref).await);
        }

        {
            let imm_mems = self.imm_memtables.lock().await;

            for imm in imm_mems.iter().rev() {
                if let Some(e) = imm.get().get(key) {
                    vref = Some(e.value_ref);
                    break;
                }
            }
        }

        if let Some(vref) = vref {
            return Some(self.value_log.get_pending(vref).await);
        }

        for level in self.levels.iter() {
            if let Some((_, val_ref)) = level.get(key).await {
                let value = self.value_log.get(val_ref).await;
                return Some(value);
            }
        }

        None
    }

    pub async fn write_opts(&self, mut write_batch: WriteBatch<K, V>, opt: &WriteOptions) -> Result<bool, WriteError> {
        let mut memtable = self.memtable.write().await;
        let mem_inner = unsafe{ memtable.get_mut() };

        let mut wal = self.wal.lock().await;

        for op in write_batch.writes.iter() {
            wal.store(op);
        }
        if opt.sync {
            wal.sync();
        }
        drop(wal);

        for op in write_batch.writes.drain(..) {
            match op {
                crate::WriteOp::Put(key, value) => {
                    log::trace!("Storing new value for key `{:?}`", key);
                    let (value_pos, value_len) = self.value_log.add_value(value).await;
                    mem_inner.put(key, value_pos, value_len);
                }
                crate::WriteOp::Delete(_) => {
                    todo!();
                }
            }
        }

        if mem_inner.is_full(&*self.params) {
            let mut imm_mems = self.imm_memtables.lock().await;

            let next_seq_num = mem_inner.get_next_seq_number();
            let imm = memtable.take(next_seq_num);
            drop(memtable);

            // Currently only one immutable memtable is supported
            while !imm_mems.is_empty() {
                imm_mems = self.imm_cond.wait(imm_mems, &self.imm_memtables).await;
            }

            self.value_log.next_pending().await;
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
                self.value_log.flush_pending().await;

                let table_id = self.next_table_id.fetch_add(1, atomic::Ordering::SeqCst);

                let l0 = self.levels.get(0).unwrap();
                l0.create_l0_table(table_id, mem.get().get_entries()).await;

                log::debug!("Created new L0 table");
                self.imm_cond.notify_all(imm_mems);
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

        assert!(min < max);

        let min = min.to_vec();
        let max = max.to_vec();

        let mut table_iters = Vec::new();
        for table in tables.drain(..) {
            table_iters.push(TableIterator::new(table.clone()));
        }

        for (_, child) in overlaps.iter() {
            table_iters.push(TableIterator::new(child.clone()));
        }

        let mut last_key: Option<Key> = None;

        loop {
            log::trace!("Starting compaction for next key");
            let mut min_key = None;

            for table_iter in table_iters.iter_mut() {
                // Advance the iterator, if needed
                if let Some(last_key) = &last_key {
                    while !table_iter.at_end() && table_iter.get_key() <= last_key {
                        table_iter.step();
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
                    if entry.seq_number > other_entry.seq_number {
                        log::trace!("Overriding key {:?}: new seq #{}, old seq #{}", key, entry.seq_number, other_entry.seq_number);

                        min_entry = Some(entry);
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

