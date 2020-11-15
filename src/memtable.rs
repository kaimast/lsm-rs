use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;
use crate::Params;

use std::collections::BTreeMap;

pub struct Memtable {
    // Sorted data
    table: BTreeMap<Key, ValueId>,

    // Sequential updates
    entries: Vec<(Key, Entry)>,
    size: usize,

    //TODO move this somewhere else
    next_seq_number: u64
}

pub struct MemtableIterator {

}

impl MemtableIterator {

}

impl Memtable {
    pub fn new() -> Self {
        let entries = Vec::new();
        let size = 0;
        let table = BTreeMap::new();
        let next_seq_number = 0;

        Self{entries, size, next_seq_number, table}
    }

    pub fn get(&self, key: &[u8]) -> Option<ValueId> {
        match self.table.get(key) {
            Some(id) => Some(id.clone()),
            None => None
        }
    }

    pub fn put(&mut self, key: Key, value_ref: ValueId, value_len: usize) {
        self.size += value_len;
        self.entries.push((key.clone(), Entry{
            value_ref, seq_number: self.next_seq_number
        }));

        self.next_seq_number += 1;
        self.table.insert(key, value_ref);
    }

    pub fn maybe_seal(&mut self, params: &Params) -> Option<Memtable> {
        if self.size < params.max_memtable_size {
            return None;
        }

        let entries = std::mem::take(&mut self.entries);
        let table = std::mem::take(&mut self.table);

        let size = self.size;
        self.size = 0;
        let next_seq_number = 0; // not used

        Some(Memtable{ entries, size, next_seq_number, table })
    }

    pub fn take(self) -> Vec<(Key, Entry)> {
        self.entries
    }

    pub fn iter(&self) -> MemtableIterator {
        todo!();
    }
}
