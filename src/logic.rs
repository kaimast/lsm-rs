use crate::data_blocks::{DataBlocks, DataEntryType};
use crate::iterate::DbIterator;
use crate::level::Level;
use crate::manifest::{LevelId, Manifest};
use crate::memtable::{ImmMemtableRef, Memtable, MemtableEntry, MemtableRef};
use crate::sorted_table::{InternalIterator, Key, TableIterator};
use crate::wal::WriteAheadLog;
use crate::{Error, KvTrait, Params, StartMode, WriteBatch, WriteOp, WriteOptions};

#[cfg(feature = "wisckey")]
use crate::values::ValueLog;

#[cfg(feature = "wisckey")]
use crate::sorted_table::ValueResult;

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "async-io")]
use tokio::fs;

#[cfg(not(feature = "async-io"))]
use std::fs;

use tokio::sync::{Mutex, Notify, RwLock};

use bincode::Options;
use cfg_if::cfg_if;

/// The main database logic
#[derive(Debug)]
pub struct DbLogic<K: KvTrait, V: KvTrait> {
    _marker: PhantomData<fn(K, V)>,

    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<MemtableRef>,
    imm_memtables: Mutex<VecDeque<(u64, ImmMemtableRef)>>,
    imm_cond: Notify,
    levels: Vec<Level>,
    wal: Mutex<WriteAheadLog>,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

impl<K: KvTrait, V: KvTrait> DbLogic<K, V> {
    pub async fn new(start_mode: StartMode, params: Params) -> Result<Self, Error> {
        if params.db_path.components().next().is_none() {
            return Err(Error::InvalidParams(
                "DB path must not be empty!".to_string(),
            ));
        }

        if params.db_path.exists() && !params.db_path.is_dir() {
            return Err(Error::InvalidParams(
                "DB path must be a folder!".to_string(),
            ));
        }

        let create = match start_mode {
            StartMode::CreateOrOpen => !params.db_path.exists(),
            StartMode::Open => {
                if !params.db_path.exists() {
                    return Err(Error::InvalidParams("DB does not exist".to_string()));
                }

                false
            }
            StartMode::CreateOrOverride => {
                if params.db_path.exists() {
                    log::info!(
                        "Removing old data at \"{}\"",
                        params.db_path.to_str().unwrap()
                    );

                    cfg_if! {
                        if #[ cfg(feature="async-io") ] {
                            fs::remove_dir_all(&params.db_path)
                                .await.expect("Failed to remove existing database");
                        } else {
                            fs::remove_dir_all(&params.db_path)
                                .expect("Failed to remove existing database");
                        }
                    }
                }

                true
            }
        };

        let params = Arc::new(params);
        let manifest;
        let memtable;
        let wal;

        if create {
            cfg_if! {
                if #[ cfg(feature="async-io") ] {
                    match fs::create_dir(&params.db_path).await {
                        Ok(()) => {
                            log::info!("Created database folder at \"{}\"", params.db_path.to_str().unwrap())
                        }
                        Err(err) => {
                            return Err(Error::Io(format!("Failed to create DB folder: {err}")));
                        }
                    }
                } else {
                    #[ cfg(not(feature="async-io")) ]
                    match fs::create_dir(&params.db_path) {
                        Ok(()) => {
                            log::info!("Created database folder at \"{}\"", params.db_path.to_str().unwrap())
                        }
                        Err(err) => {
                            return Err(Error::Io(format!("Failed to create DB folder: {err}")));
                        }
                    }
                }
            }

            manifest = Arc::new(Manifest::new(params.clone()).await);
            memtable = RwLock::new(MemtableRef::wrap(Memtable::new(1)));
            wal = Mutex::new(WriteAheadLog::new(params.clone()).await?);
        } else {
            log::info!(
                "Opening database folder at \"{}\"",
                params.db_path.to_str().unwrap()
            );

            manifest = Arc::new(Manifest::open(params.clone()).await?);

            let mut mtable = Memtable::new(manifest.get_seq_number_offset().await);
            wal = Mutex::new(
                WriteAheadLog::open(params.clone(), manifest.get_log_offset().await, &mut mtable)
                    .await?,
            );

            memtable = RwLock::new(MemtableRef::wrap(mtable));
        }

        #[cfg(feature = "wisckey")]
        let value_log = Arc::new(ValueLog::new(params.clone(), manifest.clone()).await);

        let imm_memtables = Mutex::new(VecDeque::new());
        let imm_cond = Notify::new();
        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest.clone()));

        if params.num_levels == 0 {
            panic!("Need at least one level!");
        }

        let mut levels = Vec::new();
        for index in 0..params.num_levels {
            let index = index as LevelId;
            let level = Level::new(index, data_blocks.clone(), params.clone(), manifest.clone());
            levels.push(level);
        }

        if !create {
            for (level_id, tables) in manifest.get_table_set().await.iter().enumerate() {
                for table_id in tables {
                    levels[level_id].load_table(*table_id).await?;
                }
            }
        }

        let _marker = PhantomData;

        Ok(Self {
            _marker,
            manifest,
            params,
            memtable,
            imm_memtables,
            imm_cond,
            levels,
            wal,
            #[cfg(feature = "wisckey")]
            value_log,
        })
    }

    #[cfg(feature = "sync")]
    pub async fn iter(&self, tokio_rt: Arc<tokio::runtime::Runtime>) -> DbIterator<K, V> {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        {
            let memtable = self.memtable.read().await;
            let iter = memtable.clone_immutable().into_iter().await;
            mem_iters.push(iter);
        }

        for (_, imm) in self.imm_memtables.lock().await.iter() {
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

        cfg_if! {
            if #[ cfg(feature="wisckey") ] {
                DbIterator::new(tokio_rt, mem_iters, table_iters, self.value_log.clone())
            } else {
                DbIterator::new(tokio_rt, mem_iters, table_iters)
            }
        }
    }

    #[cfg(not(feature = "sync"))]
    pub async fn iter(&self) -> DbIterator<K, V> {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        {
            let memtable = self.memtable.read().await;
            let imm_mems = self.imm_memtables.lock().await;

            mem_iters.push(memtable.clone_immutable().into_iter().await);

            for (_, imm) in imm_mems.iter() {
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

        cfg_if! {
            if #[ cfg(feature="wisckey") ] {
                DbIterator::new(mem_iters, table_iters, self.value_log.clone())
            } else {
                DbIterator::new(mem_iters, table_iters)
            }
        }
    }

    #[cfg(feature = "wisckey")]
    #[tracing::instrument(skip(self))]
    pub async fn get(&self, key: &[u8]) -> Result<Option<V>, Error> {
        log::trace!("Starting to seek for key `{key:?}`");
        let encoder = crate::get_encoder();

        {
            let memtable = self.memtable.read().await;

            if let Some(entry) = memtable.get().get(key) {
                match entry {
                    MemtableEntry::Value { value, .. } => {
                        let val = encoder.deserialize(&value)?;
                        return Ok(Some(val));
                    }
                    MemtableEntry::Deletion { .. } => {
                        return Ok(None);
                    }
                }
            };
        }

        {
            let imm_mems = self.imm_memtables.lock().await;

            for (_, imm) in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry {
                        MemtableEntry::Value { value, .. } => {
                            let val = encoder.deserialize(&value)?;
                            return Ok(Some(val));
                        }
                        MemtableEntry::Deletion { .. } => {
                            return Ok(None);
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(entry) = level.get(key).await {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let value_ref = entry.get_value_ref().unwrap();
                        return Ok(Some(self.value_log.get(value_ref).await?));
                    }
                    DataEntryType::Delete => {
                        return Ok(None);
                    }
                }
            }
        }

        // Does not exist
        Ok(None)
    }

    #[cfg(not(feature = "wisckey"))]
    #[tracing::instrument(skip(self))]
    pub async fn get(&self, key: &[u8]) -> Result<Option<V>, Error> {
        log::trace!("Starting to seek for key `{key:?}`");
        let encoder = crate::get_encoder();

        {
            let memtable = self.memtable.read().await;

            if let Some(entry) = memtable.get().get(key) {
                match entry {
                    MemtableEntry::Value { value, .. } => {
                        let value = encoder.deserialize(&value)?;
                        return Ok(Some(value));
                    }
                    MemtableEntry::Deletion { .. } => {
                        return Ok(None);
                    }
                }
            };
        }

        {
            let imm_mems = self.imm_memtables.lock().await;

            for (_, imm) in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry {
                        MemtableEntry::Value { value, .. } => {
                            let value = encoder.deserialize(&value)?;
                            return Ok(Some(value));
                        }
                        MemtableEntry::Deletion { .. } => {
                            return Ok(None);
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            if let Some(entry) = level.get(key).await {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let data = entry.get_value().unwrap();
                        let value = encoder.deserialize(data)?;
                        return Ok(Some(value));
                    }
                    DataEntryType::Delete => {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn synchronize(&self) -> Result<(), Error> {
        let mut wal = self.wal.lock().await;
        wal.sync().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn write_opts(
        &self,
        mut write_batch: WriteBatch<K, V>,
        opt: &WriteOptions,
    ) -> Result<bool, Error> {
        let mut memtable = self.memtable.write().await;
        let mem_inner = unsafe { memtable.get_mut() };

        let mut wal = self.wal.lock().await;

        for op in write_batch.writes.iter() {
            wal.store(op).await?;
        }

        if opt.sync {
            wal.sync().await?;
        }

        for op in write_batch.writes.drain(..) {
            match op {
                WriteOp::Put(key, value) => {
                    log::trace!("Storing new value for key `{key:?}`");
                    mem_inner.put(key, value);
                }
                WriteOp::Delete(key) => {
                    log::trace!("Storing deletion for key `{key:?}`");
                    mem_inner.delete(key);
                }
            }
        }

        if mem_inner.is_full(&*self.params) {
            let mut imm_mems = self.imm_memtables.lock().await;

            let next_seq_num = mem_inner.get_next_seq_number();
            let imm = memtable.take(next_seq_num);

            let wal_offset = wal.get_log_position();

            // FIXME this might cause inconsistencies in the order the memtables are flushed
            drop(wal);
            drop(memtable);

            // Currently only one immutable memtable is supported
            // Wait for it to be flushed...
            loop {
                if imm_mems.is_empty() {
                    break;
                }
                drop(imm_mems);
                self.imm_cond.notified().await;
                imm_mems = self.imm_memtables.lock().await;
            }

            imm_mems.push_back((wal_offset, imm));

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Do compaction if necessary
    /// Returns true if any work was done
    #[tracing::instrument(skip(self))]
    pub async fn do_compaction(&self) -> Result<bool, Error> {
        // (Immutable) memtable to level compaction
        {
            let mut wal = self.wal.lock().await;
            let mut imm_mems = self.imm_memtables.lock().await;

            if let Some((log_offset, mem)) = imm_mems.pop_front() {
                // First create table
                let (min_key, max_key) = mem.get().get_min_max_key();
                let l0 = self.levels.get(0).unwrap();
                let mut table_builder = l0.build_table(min_key, max_key).await;

                let memtable_entries = mem.get().get_entries();

                cfg_if! {
                    if #[cfg(feature="wisckey")] {
                        let mut vbuilder = self.value_log.make_batch().await;

                        for (key, mem_entry) in memtable_entries.into_iter() {
                            match mem_entry {
                                MemtableEntry::Value{seq_number, value} => {
                                    let value_ref = vbuilder.add_value(value).await;
                                    table_builder.add_value(&key, seq_number, value_ref).await?;
                                }
                                MemtableEntry::Deletion{seq_number} => {
                                    table_builder.add_deletion(&key, seq_number).await?;
                                }
                            }
                        }

                        vbuilder.finish().await?;
                    } else {
                        for (key, mem_entry) in memtable_entries.into_iter() {
                            match mem_entry {
                                MemtableEntry::Value{seq_number, value} => {
                                    table_builder.add_value(&key, seq_number, &value).await?;
                                }
                                MemtableEntry::Deletion{seq_number} => {
                                    table_builder.add_deletion(&key, seq_number).await?;
                                }
                            }
                        }
                    }
                }

                let table = table_builder.finish().await?;
                let table_id = table.get_id();
                l0.add_l0_table(table).await;

                // Then update manifest and flush WAL
                let seq_offset = mem.get().get_next_seq_number();
                self.manifest.set_seq_number_offset(seq_offset).await;
                self.manifest
                    .update_table_set(vec![(0, table_id)], vec![])
                    .await;

                wal.set_offset(log_offset).await;
                self.manifest.set_log_offset(log_offset).await;

                log::debug!("Created new L0 table");
                self.imm_cond.notify_waiters();
                return Ok(true);
            }
        }

        // level-to-level compaction
        for (level_pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if level_pos < self.params.num_levels - 1 && level.needs_compaction().await {
                self.compact(level_pos as LevelId, level).await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    #[tracing::instrument(skip(self, level))]
    async fn compact(&self, level_pos: LevelId, level: &Level) -> Result<(), Error> {
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

        let parent_level = &self.levels[level_pos as usize];
        let child_level = &self.levels[(level_pos + 1) as usize];

        let overlaps = child_level.get_overlaps(min, max).await;

        // Fast path
        if tables.len() == 1 && overlaps.is_empty() {
            let mut parent_tables = level.get_tables().await;
            let mut child_tables = child_level.get_tables().await;

            log::debug!(
                "Moving table from level {} to level {}",
                level_pos,
                level_pos + 1
            );
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
            return Ok(());
        }

        log::debug!(
            "Compacting {} table(s) in level {} with {} table(s) in level {}",
            tables.len(),
            level_pos,
            overlaps.len(),
            level_pos + 1
        );

        for (_, table) in overlaps.iter() {
            min = std::cmp::min(min, table.get_min());
            max = std::cmp::max(max, table.get_max());
        }

        // Table can potentially contain a single entry
        assert!(min <= max);

        let min = min.to_vec();
        let max = max.to_vec();

        let mut table_iters = Vec::new();
        for table in tables.into_iter() {
            table_iters.push(TableIterator::new(table.clone()).await);
        }

        for (_, child) in overlaps.iter() {
            table_iters.push(TableIterator::new(child.clone()).await);
        }

        let mut last_key: Option<Key> = None;

        #[cfg(feature = "wisckey")]
        let mut deleted_values = vec![];

        let mut table_builder = child_level.build_table(min, max).await;

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

            let mut min_iter: Option<&TableIterator> = None;
            let min_key = min_key.unwrap().clone();

            for table_iter in table_iters.iter_mut() {
                if table_iter.at_end() {
                    continue;
                }

                // Figure out if this table's entry is more recent
                let key = table_iter.get_key();

                if key != &min_key {
                    continue;
                }

                if let Some(other_iter) = min_iter {
                    if table_iter.get_seq_number() > other_iter.get_seq_number() {
                        log::trace!(
                            "Overriding key {:?}: new seq #{}, old seq #{}",
                            key,
                            table_iter.get_seq_number(),
                            other_iter.get_seq_number()
                        );

                        // Check whether we overwrote a key that is about to
                        // be garbage collected
                        #[cfg(feature = "wisckey")]
                        if let ValueResult::Reference(vid) = other_iter.get_value() {
                            deleted_values.push(vid);
                        } else {
                            panic!("Invalid state");
                        }

                        min_iter = Some(table_iter);
                    }
                } else {
                    log::trace!("Found new key {:?}", key);
                    min_iter = Some(table_iter);
                }
            }

            let min_iter = min_iter.unwrap();
            match min_iter.get_entry_type() {
                DataEntryType::Put => {
                    cfg_if! {
                        if #[cfg(feature="wisckey")] {
                            if let ValueResult::Reference(value_ref) = min_iter.get_value() {
                                table_builder.add_value(&min_key, min_iter.get_seq_number(), value_ref).await?;
                            } else {
                                panic!("Invalid state");
                            }
                        } else {
                            let value = min_iter.get_value().unwrap();
                            table_builder.add_value(&min_key, min_iter.get_seq_number(), value).await?;
                        }
                    }
                }
                DataEntryType::Delete => {
                    table_builder
                        .add_deletion(&min_key, min_iter.get_seq_number())
                        .await?;
                }
            }

            last_key = Some(min_key);
        }

        let new_table = table_builder.finish().await?;

        let add_set = vec![(level_pos + 1, new_table.get_id())];
        let mut remove_set = vec![];

        // Install new tables atomically
        let mut parent_tables = parent_level.get_tables().await;
        let mut child_tables = child_level.get_tables().await;

        // iterate backwards to ensure oldest entries are removed first
        for (offset, _) in overlaps.iter().rev() {
            let id = child_tables.remove(*offset).get_id();
            remove_set.push((level_pos + 1_u32, id));
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

        #[cfg(feature = "wisckey")]
        for vid in deleted_values.into_iter() {
            self.value_log.mark_value_deleted(vid).await?;
        }

        self.manifest.update_table_set(add_set, remove_set).await;

        log::trace!("Done compacting tables");
        Ok(())
    }
}
