use crate::sorted_table::{SortedTable, Key};
use crate::entry::Entry;
use crate::values::ValueId;

use std::sync::RwLock;

pub struct Level<K: Key> {
    tables: RwLock<Vec<SortedTable<K>>>
}

impl<K: Key> Level<K> {
    pub fn new() -> Self {
        Self{ tables: RwLock::new(Vec::new()) }
    }

    pub fn create_table(&self, id: usize, data_prefix: &str, entries: Vec<(K, Entry)>) {
        let tdata = bincode::serialize(&entries).unwrap();

        let table = SortedTable::new(entries);
        let path = format!("{}{}.table", data_prefix,id);

        std::fs::write(path, tdata).expect("Failed to write table to disk");

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
