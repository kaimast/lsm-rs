use std::collections::VecDeque;
use std::sync::Arc;

#[cfg(not(feature = "async-io"))]
use std::fs;

use tokio::sync::RwLock;
use tokio_condvar::Condvar;

use cfg_if::cfg_if;

use crate::data_blocks::{DataBlocks, DataEntryType};
use crate::level::Level;
use crate::level_logger::LevelLogger;
use crate::manifest::{LevelId, Manifest};
use crate::memtable::{
    ImmMemtableRef, Memtable, MemtableEntry, MemtableEntryRef, MemtableIterator, MemtableRef,
};
use crate::sorted_table::{InternalIterator, Key, TableIterator};
use crate::wal::WriteAheadLog;
use crate::{Error, Params, StartMode, WriteBatch, WriteOp, WriteOptions};

#[cfg(feature = "wisckey")]
use crate::values::{ValueLog, ValueRef};

use crate::data_blocks::DataEntry;

enum CompactResult {
    NothingToDo,
    DidWork,
    Locked,
}

/// Refers to an entry in the key-value store without copying it
pub enum EntryRef {
    SortedTable {
        entry: DataEntry,
        #[cfg(feature = "wisckey")]
        value_ref: ValueRef,
    },
    Memtable {
        entry: MemtableEntryRef,
    },
}

impl EntryRef {
    pub fn get_value(&self) -> &[u8] {
        match self {
            #[cfg(feature = "wisckey")]
            Self::SortedTable { value_ref, .. } => value_ref.get_value(),
            #[cfg(not(feature = "wisckey"))]
            Self::SortedTable { entry } => entry.get_value().unwrap(),
            Self::Memtable { entry } => entry.get_value().unwrap(),
        }
    }
}

/// The main database logic
///
/// Generally, you will not interact with this directly but use
/// Database instead.
/// This is mainly kept public so that we can implement the sync
/// API in a separate crate.
pub struct DbLogic {
    manifest: Arc<Manifest>,
    params: Arc<Params>,
    memtable: RwLock<MemtableRef>,
    /// Immutable memtables are about to be compacted
    imm_memtables: RwLock<VecDeque<(u64, ImmMemtableRef)>>,
    imm_cond: Condvar,
    levels: Vec<Level>,
    wal: WriteAheadLog,
    level_logger: Option<LevelLogger>,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

impl DbLogic {
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

        let level_logger = if let Some(path) = &params.log_level_stats {
            Some(LevelLogger::new(path, params.num_levels))
        } else {
            None
        };

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
                            // Not yet supported in tokio_uring
                            std::fs::remove_dir_all(&params.db_path)
                                .expect("Failed to remove existing database");
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
                    // Not yet supported in tokio_uring
                    match std::fs::create_dir(&params.db_path) {
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
            wal = WriteAheadLog::new(params.clone()).await?;
        } else {
            log::info!(
                "Opening database folder at \"{}\"",
                params.db_path.to_str().unwrap()
            );

            manifest = Arc::new(Manifest::open(params.clone()).await?);

            let mut mtable = Memtable::new(manifest.get_seq_number_offset().await);
            wal = WriteAheadLog::open(params.clone(), manifest.get_log_offset().await, &mut mtable)
                .await?;

            memtable = RwLock::new(MemtableRef::wrap(mtable));
        }

        #[cfg(feature = "wisckey")]
        let value_log = Arc::new(ValueLog::new(params.clone(), manifest.clone()).await);

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
            for (level_id, tables) in manifest.get_tables().await.iter().enumerate() {
                for table_id in tables {
                    levels[level_id].load_table(*table_id).await?;
                }
            }
        }

        Ok(Self {
            manifest,
            params,
            memtable,
            imm_memtables: Default::default(),
            imm_cond: Default::default(),
            levels,
            wal,
            level_logger,
            #[cfg(feature = "wisckey")]
            value_log,
        })
    }

    #[cfg(feature = "wisckey")]
    pub fn get_value_log(&self) -> Arc<ValueLog> {
        self.value_log.clone()
    }

    pub async fn prepare_iter(
        &self,
        min_key: Option<&[u8]>,
        max_key: Option<&[u8]>,
    ) -> (
        Vec<MemtableIterator>,
        Vec<TableIterator>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    ) {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        if let Some(min_key) = &min_key
            && let Some(max_key) = &max_key
        {
            assert!(min_key < max_key);
        }

        {
            let memtable = self.memtable.read().await;
            let imm_mems = self.imm_memtables.read().await;

            mem_iters.push(memtable.clone_immutable().into_iter(false).await);

            for (_, imm) in imm_mems.iter() {
                let iter = imm.clone().into_iter(false).await;
                mem_iters.push(iter);
            }
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                let mut skip = false;

                if let Some(min_key) = min_key {
                    if table.get_max() < min_key {
                        skip = true;
                    }
                }

                if let Some(max_key) = max_key {
                    if table.get_min() > max_key {
                        skip = true;
                    }
                }

                if !skip {
                    let iter = TableIterator::new(table.clone(), false).await;
                    table_iters.push(iter);
                }
            }
        }

        (
            mem_iters,
            table_iters,
            min_key.map(|k| k.to_vec()),
            max_key.map(|k| k.to_vec()),
        )
    }

    /// Iterate over the specified range in reverse
    pub async fn prepare_reverse_iter(
        &self,
        max_key: Option<&[u8]>,
        min_key: Option<&[u8]>,
    ) -> (
        Vec<MemtableIterator>,
        Vec<TableIterator>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    ) {
        let mut table_iters = Vec::new();
        let mut mem_iters = Vec::new();

        if let Some(min_key) = &min_key
            && let Some(max_key) = &max_key
        {
            assert!(min_key < max_key);
        };

        {
            let memtable = self.memtable.read().await;
            let imm_mems = self.imm_memtables.read().await;

            mem_iters.push(memtable.clone_immutable().into_iter(true).await);

            for (_, imm) in imm_mems.iter() {
                let iter = imm.clone().into_iter(true).await;
                mem_iters.push(iter);
            }
        }

        for level in self.levels.iter() {
            let tables = level.get_tables_ro().await;

            for table in tables.iter() {
                let mut skip = false;

                if let Some(min_key) = min_key {
                    if table.get_max() < min_key {
                        skip = true;
                    }
                }

                if let Some(max_key) = max_key {
                    if table.get_min() > max_key {
                        skip = true;
                    }
                }

                if !skip {
                    let iter = TableIterator::new(table.clone(), true).await;
                    table_iters.push(iter);
                }
            }
        }

        (
            mem_iters,
            table_iters,
            min_key.map(|k| k.to_vec()),
            max_key.map(|k| k.to_vec()),
        )
    }

    #[cfg(feature = "wisckey")]
    #[tracing::instrument(skip(self, key))]
    pub async fn get(&self, key: &[u8]) -> Result<(bool, Option<EntryRef>), Error> {
        let mut compaction_triggered = false;

        {
            let memtable = self.memtable.read().await;

            if let Some(entry) = memtable.get().get(key) {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let entry = EntryRef::Memtable { entry };
                        return Ok((compaction_triggered, Some(entry)));
                    }
                    DataEntryType::Delete => {
                        return Ok((compaction_triggered, None));
                    }
                }
            }
        }

        {
            let imm_mems = self.imm_memtables.read().await;

            for (_, imm) in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry.get_type() {
                        DataEntryType::Put => {
                            let entry = EntryRef::Memtable { entry };
                            return Ok((compaction_triggered, Some(entry)));
                        }
                        DataEntryType::Delete => {
                            return Ok((compaction_triggered, None));
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            let (level_compact_triggered, result) = level.get(key).await;
            if level_compact_triggered {
                compaction_triggered = true;
            }

            if let Some(entry) = result {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let value_ref = self
                            .value_log
                            .get_ref(entry.get_value_id().unwrap())
                            .await
                            .unwrap();
                        let entry = EntryRef::SortedTable { entry, value_ref };
                        return Ok((compaction_triggered, Some(entry)));
                    }
                    DataEntryType::Delete => {
                        return Ok((compaction_triggered, None));
                    }
                }
            }
        }

        // Does not exist
        Ok((compaction_triggered, None))
    }

    #[cfg(not(feature = "wisckey"))]
    #[tracing::instrument(skip(self, key))]
    pub async fn get(&self, key: &[u8]) -> Result<(bool, Option<EntryRef>), Error> {
        let mut compaction_triggered = false;

        {
            let memtable = self.memtable.read().await;

            if let Some(entry) = memtable.get().get(key) {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let entry = EntryRef::Memtable { entry };
                        return Ok((compaction_triggered, Some(entry)));
                    }
                    DataEntryType::Delete => {
                        return Ok((compaction_triggered, None));
                    }
                }
            }
        }

        {
            let imm_mems = self.imm_memtables.read().await;

            for (_, imm) in imm_mems.iter().rev() {
                if let Some(entry) = imm.get().get(key) {
                    match entry.get_type() {
                        DataEntryType::Put => {
                            let entry = EntryRef::Memtable { entry };
                            return Ok((compaction_triggered, Some(entry)));
                        }
                        DataEntryType::Delete => {
                            return Ok((compaction_triggered, None));
                        }
                    }
                }
            }
        }

        for level in self.levels.iter() {
            let (level_compact_triggered, result) = level.get(key).await;
            if level_compact_triggered {
                compaction_triggered = true;
            }

            if let Some(entry) = result {
                match entry.get_type() {
                    DataEntryType::Put => {
                        let entry = EntryRef::SortedTable { entry };
                        return Ok((compaction_triggered, Some(entry)));
                    }
                    DataEntryType::Delete => {
                        return Ok((compaction_triggered, None));
                    }
                }
            }
        }

        Ok((compaction_triggered, None))
    }

    pub async fn synchronize(&self) -> Result<(), Error> {
        self.wal.sync().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self, write_batch, opt))]
    pub async fn write_opts(
        &self,
        mut write_batch: WriteBatch,
        opt: &WriteOptions,
    ) -> Result<bool, Error> {
        let mut memtable = self.memtable.write().await;
        let mem_inner = unsafe { memtable.get_mut() };

        let wal_offset = {
            let log_pos = self.wal.store(&write_batch.writes).await?;

            if opt.sync {
                self.wal.sync().await?;
            }

            for op in write_batch.writes.drain(..) {
                match op {
                    WriteOp::Put(key, value) => mem_inner.put(key, value),
                    WriteOp::Delete(key) => mem_inner.delete(key),
                }
            }

            log_pos
        };

        // If the current memtable is full, mark it as immutable, so it can be flushed to L0
        if mem_inner.is_full(&self.params) {
            let next_seq_num = mem_inner.get_next_seq_number();
            let imm = memtable.take(next_seq_num);
            let mut imm_mems = self.imm_memtables.write().await;

            while !imm_mems.is_empty() {
                imm_mems = self
                    .imm_cond
                    .rw_write_wait(&self.imm_memtables, imm_mems)
                    .await;
            }

            imm_mems.push_back((wal_offset, imm));

            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn do_memtable_compaction(&self) -> Result<bool, Error> {
        log::trace!("Attempting memtable compaction");

        // SAFETY
        // Only one task will do the memtable compaction, so it is
        // fine to not hold the lock the entire time

        let to_compact = self.imm_memtables.read().await.front().cloned();

        if let Some((log_offset, mem)) = to_compact {
            log::trace!("Found memtable to compact");

            // First create table
            let (min_key, max_key) = mem.get().get_min_max_key();
            let l0 = self.levels.first().unwrap();
            let table_id = self.manifest.next_table_id().await;
            let mut table_builder = l0.build_table(table_id, min_key.to_vec(), max_key.to_vec());

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

            if let Some(logger) = &self.level_logger {
                logger.l0_table_added();
            }

            // Then update manifest and flush WAL
            let seq_offset = mem.get().get_next_seq_number();
            self.manifest.set_seq_number_offset(seq_offset).await;
            self.manifest
                .update_table_set(vec![(0, table_id)], vec![])
                .await;

            self.wal.set_offset(log_offset).await;
            self.manifest.set_log_offset(log_offset).await;

            // Finally, remove immutable memtable
            {
                let mut imm_mems = self.imm_memtables.write().await;
                let entry = imm_mems.pop_front();
                assert!(entry.is_some());
            }
            log::debug!("Created new L0 table");
            self.imm_cond.notify_all();

            Ok(true)
        } else {
            log::trace!("Found no memtable to compact");
            Ok(false)
        }
    }

    /// Do compaction if necessary
    /// Returns true if any work was done
    #[tracing::instrument(skip(self))]
    pub async fn do_level_compaction(&self) -> Result<bool, Error> {
        let mut was_locked = false;
        log::trace!("Attempting level compaction");

        // level-to-level compaction
        for (level_pos, level) in self.levels.iter().enumerate() {
            // Last level cannot be compacted
            if level_pos < self.params.num_levels - 1 {
                match self.compact_level(level_pos as LevelId, level).await? {
                    CompactResult::DidWork => {
                        log::trace!("Compacted level {level_pos}");
                        return Ok(true);
                    }
                    CompactResult::Locked => {
                        log::trace!("Cannot compact level {level_pos} right now; lock was held");
                        was_locked = true;
                    }
                    CompactResult::NothingToDo => {
                        log::trace!("Nothing to do for level {level_pos}");
                    }
                }
            }
        }

        // We'll try again if it was locked
        Ok(was_locked)
    }

    #[tracing::instrument(skip(self, level))]
    async fn compact_level(
        &self,
        level_pos: LevelId,
        level: &Level,
    ) -> Result<CompactResult, Error> {
        let mut parent_tables = match level.maybe_start_compaction().await {
            Ok(Some(result)) => result,
            Ok(None) => return Ok(CompactResult::NothingToDo),
            Err(()) => return Ok(CompactResult::Locked),
        };
        assert!(!parent_tables.is_empty());

        log::trace!("Starting compaction on level {level_pos}");

        let mut min = parent_tables[0].get_min();
        let mut max = parent_tables[0].get_max();

        if parent_tables.len() > 1 {
            for table in parent_tables[1..].iter() {
                min = std::cmp::min(min, table.get_min());
                max = std::cmp::max(max, table.get_max());
            }
        }

        let parent_level = &self.levels[level_pos as usize];
        let child_level = &self.levels[(level_pos + 1) as usize];

        let overlap_result = if parent_tables.len() == 1 {
            child_level
                .get_overlaps(min, max, Some(parent_tables[0].get_id()))
                .await
        } else {
            child_level.get_overlaps(min, max, None).await
        };

        // Abort due to concurrency?
        let (table_id, child_tables) = match overlap_result {
            Some(res) => res,
            None => {
                log::trace!("Aborting compaction due to concurrency");
                return Ok(CompactResult::NothingToDo);
            }
        };

        // Fast path
        if parent_tables.len() == 1 && child_tables.is_empty() {
            let mut all_parent_tables = level.get_tables().await;
            let mut all_child_tables = child_level.get_tables().await;

            log::debug!(
                "Moving table from level {} to level {}",
                level_pos,
                level_pos + 1
            );
            let table = parent_tables.remove(0);

            let mut new_pos = 0;
            for (pos, other_table) in all_child_tables.iter().enumerate() {
                if other_table.get_min() > table.get_min() {
                    new_pos = pos;
                    break;
                }
            }

            let add_set = vec![(level_pos + 1, table.get_id())];
            let remove_set = vec![(level_pos, table.get_id())];

            all_child_tables.insert(new_pos, table.clone());
            child_level.remove_table_placeholder(table_id).await;

            for (pos, other_table) in all_parent_tables.iter().enumerate() {
                if table.get_id() == other_table.get_id() {
                    all_parent_tables.remove(pos);
                    break;
                }
            }

            if let Some(logger) = &self.level_logger {
                logger.compaction(level_pos, 1, 1);
            }

            self.manifest.update_table_set(add_set, remove_set).await;
            table.finish_compaction();

            log::trace!("Done moving table");
            return Ok(CompactResult::DidWork);
        }

        log::debug!(
            "Compacting {} table(s) in level {} with {} table(s) in level {}",
            parent_tables.len(),
            level_pos,
            child_tables.len(),
            level_pos + 1
        );

        for table in child_tables.iter() {
            min = std::cmp::min(min, table.get_min());
            max = std::cmp::max(max, table.get_max());
        }

        // Table can potentially contain a single entry
        assert!(min <= max);

        let min = min.to_vec();
        let max = max.to_vec();

        let mut table_iters = Vec::new();
        for table in parent_tables.iter() {
            table_iters.push(TableIterator::new(table.clone(), false).await);
        }

        for child in child_tables.iter() {
            table_iters.push(TableIterator::new(child.clone(), false).await);
        }

        let mut last_key: Option<Key> = None;

        #[cfg(feature = "wisckey")]
        let mut deleted_values = vec![];

        let mut table_builder = child_level.build_table(table_id, min, max);

        loop {
            log::trace!("Starting compaction for next key");
            let mut min_key: Option<Vec<u8>> = None;

            for table_iter in table_iters.iter_mut() {
                // Advance the iterator, if needed
                if let Some(last_key) = &last_key {
                    while !table_iter.at_end() && table_iter.get_key() <= last_key.as_slice() {
                        table_iter.step().await;
                    }
                }

                if !table_iter.at_end() {
                    if let Some(key) = &min_key {
                        if table_iter.get_key() < key.as_slice() {
                            min_key = Some(table_iter.get_key().to_vec());
                        }
                    } else {
                        min_key = Some(table_iter.get_key().to_vec());
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

                if key != min_key {
                    continue;
                }

                if let Some(other_iter) = min_iter {
                    if table_iter.get_seq_number() > other_iter.get_seq_number() {
                        log::trace!(
                            "Overriding key {key:?}: new seq #{}, old seq #{}",
                            table_iter.get_seq_number(),
                            other_iter.get_seq_number()
                        );

                        // Check whether we overwrote a key that is about to
                        // be garbage collected
                        #[cfg(feature = "wisckey")]
                        deleted_values.push(other_iter.get_value_id().unwrap());

                        min_iter = Some(table_iter);
                    }
                } else {
                    log::trace!("Found new key {key:?}");
                    min_iter = Some(table_iter);
                }
            }

            let min_iter = min_iter.unwrap();
            match min_iter.get_entry_type() {
                DataEntryType::Put => {
                    table_builder
                        .add_value(
                            &min_key,
                            min_iter.get_seq_number(),
                            #[cfg(feature = "wisckey")]
                            min_iter.get_value_id().unwrap(),
                            #[cfg(not(feature = "wisckey"))]
                            min_iter.get_entry().unwrap().get_value(),
                        )
                        .await?;
                }
                DataEntryType::Delete => {
                    table_builder
                        .add_deletion(&min_key, min_iter.get_seq_number())
                        .await?;
                }
            }

            last_key = Some(min_key.to_vec());
        }

        let new_table = table_builder.finish().await?;

        let add_set = vec![(level_pos + 1, new_table.get_id())];
        let mut remove_set = vec![];

        // Install new tables atomically
        let mut all_parent_tables = parent_level.get_tables().await;
        let mut all_child_tables = child_level.get_tables().await;

        // iterate backwards to ensure oldest entries are removed first
        for table in child_tables.iter() {
            let mut found = false;
            for (pos, other_table) in all_child_tables.iter().enumerate() {
                if other_table.get_id() == table.get_id() {
                    remove_set.push((level_pos + 1_u32, table.get_id()));
                    all_child_tables.remove(pos);
                    found = true;
                    break;
                }
            }
            assert!(found);
        }

        let mut new_pos = all_child_tables.len(); // insert at the end by default
        for (pos, other_table) in all_child_tables.iter().enumerate() {
            if other_table.get_min() > new_table.get_min() {
                new_pos = pos;
                break;
            }
        }

        all_child_tables.insert(new_pos, Arc::new(new_table));
        child_level.remove_table_placeholder(table_id).await;

        for table in parent_tables.iter() {
            let mut found = false;
            for (pos, other_table) in all_parent_tables.iter().enumerate() {
                if other_table.get_id() == table.get_id() {
                    remove_set.push((level_pos + 1_u32, table.get_id()));
                    all_parent_tables.remove(pos);
                    found = true;
                    break;
                }
            }
            assert!(found);
        }

        #[cfg(feature = "wisckey")]
        for vid in deleted_values.into_iter() {
            self.value_log.mark_value_deleted(vid).await?;
        }

        if let Some(logger) = &self.level_logger {
            logger.compaction(level_pos, add_set.len(), remove_set.len());
        }

        self.manifest.update_table_set(add_set, remove_set).await;

        log::trace!("Done compacting tables");
        Ok(CompactResult::DidWork)
    }
}
