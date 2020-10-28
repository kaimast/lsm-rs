use crate::sorted_table::{SortedTable, Key};
use crate::entry::Entry;
use crate::values::ValueId;
use crate::data_blocks::DataBlocks;

use std::sync::{Arc, RwLock};

pub struct Level<K: Key> {
    data_blocks: Arc<DataBlocks>,
    tables: RwLock<Vec<SortedTable<K>>>
}

impl<K: Key> Level<K> {
    pub fn new(data_blocks: Arc<DataBlocks>) -> Self {
        Self {
            data_blocks,
            tables: RwLock::new(Vec::new())
        }
    }

    pub fn create_table(&self, _id: usize, entries: Vec<(K, Entry)>) {
        let table = SortedTable::new(entries, self.data_blocks.clone());

        //TODO update manifest
        let mut tables = self.tables.write().unwrap();
        tables.push(table);
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        let tables = self.tables.read().unwrap();

        // Iterate from back to front (newest to oldest)
        // as L0 may have overlapping
        for table in tables.iter().rev() {
            if let Some(val_ref) = table.get(key) {
                return Some(val_ref);
            }
        }

        None
    }

    pub fn needs_compaction(&self) -> bool {
        let _tables = self.tables.read().unwrap();

        //TODO

        false
    }
}
