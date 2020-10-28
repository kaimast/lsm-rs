use crate::sorted_table::{SortedTable, Key};
use crate::entry::Entry;
use crate::values::ValueId;
use crate::data_blocks::DataBlocks;

use std::sync::{Arc, Mutex, RwLock};

const MAX_L0_FILES: usize = 8;

pub type TableVec<K> = Vec<Arc<SortedTable<K>>>;

pub struct Level<K: Key> {
    index: usize,
    next_compaction: Mutex<usize>,
    data_blocks: Arc<DataBlocks>,
    tables: RwLock<TableVec<K>>
}

impl<K: Key> Level<K> {
    pub fn new(index: usize, data_blocks: Arc<DataBlocks>) -> Self {
        Self {
            index,
            next_compaction: Mutex::new(0),
            data_blocks,
            tables: RwLock::new(Vec::new())
        }
    }

    pub fn create_l0_table(&self, _id: usize, entries: Vec<(K, Entry)>) {
        let table = SortedTable::new(entries, self.data_blocks.clone());

        //TODO update manifest
        let mut tables = self.tables.write().unwrap();
        tables.push(Arc::new(table));
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        let tables = self.tables.read().unwrap();

        // Iterate from back to front (newest to oldest)
        // as L0 may have overlapping entries
        for table in tables.iter().rev() {
            if let Some(val_ref) = table.get(key) {
                return Some(val_ref);
            }
        }

        None
    }

    #[inline]
    pub fn get_total_size(&self) -> usize {
        let tables = self.tables.read().unwrap();
        let mut total_size = 0;

        for t in tables.iter() {
            total_size += t.get_size();
        }

        total_size
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        let mut result: usize = 10 * 1048576;
        let mut level = self.index;
        while level > 1 {
            result *= 10;
            level -= 1;
        }

        result
    }

    #[inline]
    pub fn num_tables(&self) -> usize {
        let tables = self.tables.read().unwrap();
        tables.len()
    }

    pub fn needs_compaction(&self) -> bool {
        if self.index == 0 {
            self.num_tables() > MAX_L0_FILES
        } else {
            self.get_total_size() > self.max_size()
        }
    }

    pub fn start_compaction(&self) -> (usize, Arc<SortedTable<K>>) {
        let mut next_compaction = self.next_compaction.lock().unwrap();
        let tables = self.tables.read().unwrap();

        if tables.is_empty() {
            panic!("Cannot start compaction; level {} is empty", self.index);
        }

        if *next_compaction >= tables.len() {
            *next_compaction = 0;
        }

        let offset = *next_compaction;
        let table = tables[offset].clone();
        *next_compaction += 1;

        (offset, table)
    }

    pub fn get_overlaps(&self, parent_table: &SortedTable<K>) -> Vec<(usize, Arc<SortedTable<K>>)> {
        let mut overlaps = Vec::new();
        let tables = self.tables.read().unwrap();

        for (pos, table) in tables.iter().enumerate() {
            if table.overlaps(parent_table) {
                overlaps.push((pos, table.clone()));
            }
        }

        overlaps
    }

    pub fn get_tables(&self) -> std::sync::RwLockWriteGuard<'_, TableVec<K>> {
        self.tables.write().unwrap()
    }
}
