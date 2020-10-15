use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;
use crate::Params;

pub struct Memtable<K: Key> {
    entries: Vec<Entry<K>>,
    size: usize,

    //TODO move this somewhere else
    next_seq_number: u64
}

impl<K: Key> Memtable<K> {
    pub fn new() -> Self {
        let entries = Vec::new();
        let size = 0;
        let next_seq_number = 0;

        Self{entries, size, next_seq_number}
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        for e in self.entries.iter() {
            if &e.key == key {
                return Some(e.value_ref);
            }
        }

        None
    }

    pub fn put(&mut self, key: K, value_ref: ValueId, value_len: usize) {
        self.size += value_len;
        self.entries.push(Entry{
            key, value_ref, seq_number: self.next_seq_number
        });

        self.next_seq_number += 1;
    }

    pub fn maybe_seal(&mut self, params: &Params) -> Option<Memtable<K>> {
        if self.size < params.max_memtable_size {
            return None;
        }

        let entries = std::mem::take(&mut self.entries);
        let size = self.size;
        self.size = 0;
        let next_seq_number = 0; // not used

        Some(Memtable{ entries, size, next_seq_number })
    }

    pub fn take(self) -> Vec<Entry<K>> {
        self.entries
    }
}
