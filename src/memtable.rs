use crate::data_blocks::DataEntryType;
use crate::manifest::SeqNumber;
use crate::sorted_table::{InternalIterator, Key};
use crate::{KvTrait, Params};

#[cfg(feature = "wisckey")]
use crate::sorted_table::ValueResult;

use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MemtableRef {
    inner: Arc<Memtable>,
}

#[derive(Debug, Clone)]
pub struct ImmMemtableRef {
    inner: Arc<Memtable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemtableEntry {
    Value { seq_number: u64, value: Vec<u8> },
    Deletion { seq_number: u64 },
}

impl MemtableEntry {
    pub fn get_value(&self) -> Option<&[u8]> {
        match self {
            MemtableEntry::Value { value, .. } => Some(value),
            MemtableEntry::Deletion { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct MemtableIterator {
    inner: Arc<Memtable>,
    next_index: usize,
    key: Option<Key>,
    entry: Option<MemtableEntry>,
}

impl MemtableIterator {
    pub async fn new(inner: Arc<Memtable>) -> Self {
        let mut obj = Self {
            inner,
            key: None,
            entry: None,
            next_index: 0,
        };

        obj.step().await;

        obj
    }
}

#[async_trait::async_trait]
impl InternalIterator for MemtableIterator {
    #[tracing::instrument]
    async fn step(&mut self) {
        let entries = &self.inner.entries;

        match self.next_index.cmp(&entries.len()) {
            Ordering::Greater => {
                panic!("Cannot step(); already at end");
            }
            Ordering::Equal => {
                self.next_index += 1;
            }
            Ordering::Less => {
                let key = &entries[self.next_index].0;

                while self.next_index + 1 < entries.len() && &entries[self.next_index + 1].0 == key
                {
                    self.next_index += 1;
                }

                let (key, entry) = entries[self.next_index].clone();
                self.key = Some(key);
                self.entry = Some(entry);
                self.next_index += 1;
            }
        }
    }

    fn at_end(&self) -> bool {
        let len = self.inner.entries.len();
        self.next_index > len
    }

    fn get_key(&self) -> &Key {
        self.key.as_ref().expect("Not a valid iterator")
    }

    fn get_seq_number(&self) -> SeqNumber {
        match self.entry.as_ref().unwrap() {
            MemtableEntry::Value { seq_number, .. } | MemtableEntry::Deletion { seq_number } => {
                *seq_number
            }
        }
    }

    fn get_entry_type(&self) -> DataEntryType {
        match self.entry.as_ref().unwrap() {
            MemtableEntry::Value { .. } => DataEntryType::Put,
            MemtableEntry::Deletion { .. } => DataEntryType::Delete,
        }
    }

    #[cfg(feature = "wisckey")]
    fn get_value(&self) -> ValueResult {
        let entry = self.entry.as_ref().unwrap();

        if let Some(value) = entry.get_value() {
            ValueResult::Value(value)
        } else {
            ValueResult::NoValue
        }
    }

    #[cfg(not(feature = "wisckey"))]
    fn get_value(&self) -> Option<&[u8]> {
        self.entry.as_ref().unwrap().get_value()
    }
}

impl ImmMemtableRef {
    pub fn get(&self) -> &Memtable {
        &*self.inner
    }

    pub async fn into_iter(self) -> MemtableIterator {
        MemtableIterator::new(self.inner).await
    }
}

impl MemtableRef {
    pub fn wrap(inner: Memtable) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn clone_immutable(&self) -> ImmMemtableRef {
        ImmMemtableRef {
            inner: self.inner.clone(),
        }
    }

    /// Make the current contents into an immutable memtable
    /// And create a new mutable one
    pub fn take(&mut self, next_seq_number: u64) -> ImmMemtableRef {
        let mut inner = Arc::new(Memtable::new(next_seq_number));
        std::mem::swap(&mut inner, &mut self.inner);

        ImmMemtableRef { inner }
    }

    pub fn get(&self) -> &Memtable {
        &*self.inner
    }

    /// This is only safe to call from the DbLogic while holding the memtable lock
    pub unsafe fn get_mut(&mut self) -> &mut Memtable {
        Arc::get_mut_unchecked(&mut self.inner)
    }
}

/// In-memory representation of state that has not been written to level 0 yet.
/// This data structure does not exist on disk, but can be recreated from the write-ahead log
#[derive(Debug)]
pub struct Memtable {
    /// Sorted updates
    entries: Vec<(Key, MemtableEntry)>,
    size: usize,
    next_seq_number: SeqNumber,
}

impl Memtable {
    pub fn new(next_seq_number: SeqNumber) -> Self {
        let entries = Vec::new();
        let size = 0;

        Self {
            entries,
            size,
            next_seq_number,
        }
    }

    #[inline]
    pub fn get_next_seq_number(&self) -> u64 {
        self.next_seq_number
    }

    pub fn get_min_max_key(&self) -> (Key, Key) {
        let len = self.entries.len();

        if len == 0 {
            panic!("Memtable is empty");
        }

        (self.entries[0].0.clone(), self.entries[len - 1].0.clone())
    }

    pub fn get(&self, key: &[u8]) -> Option<MemtableEntry> {
        match self.entries.binary_search_by_key(&key, |t| t.0.as_slice()) {
            Ok(pos) => Some(self.entries[pos].1.clone()),
            Err(_) => None,
        }
    }

    /// Get position were to insert the key
    /// Will remove existing entries with the same key
    fn get_key_pos(&mut self, key: &[u8]) -> usize {
        match self.entries.binary_search_by_key(&key, |t| t.0.as_slice()) {
            Ok(pos) => {
                // remove old entry
                let entry_len = {
                    let (_, entry) = self.entries.remove(pos);
                    match entry {
                        MemtableEntry::Value { value, .. } => key.len() + value.len(),
                        MemtableEntry::Deletion { .. } => key.len(),
                    }
                };

                self.size -= entry_len;
                pos
            }
            Err(pos) => pos,
        }
    }

    pub fn put(&mut self, key: Key, value: Vec<u8>) {
        let pos = self.get_key_pos(key.as_slice());
        let entry_len = key.len() + value.len();

        self.entries.insert(
            pos,
            (
                key,
                MemtableEntry::Value {
                    value,
                    seq_number: self.next_seq_number,
                },
            ),
        );

        self.size += entry_len;
        self.next_seq_number += 1;
    }

    pub fn delete(&mut self, key: Key) {
        let pos = self.get_key_pos(key.as_slice());
        let entry_len = key.len();

        self.entries.insert(
            pos,
            (
                key,
                MemtableEntry::Deletion {
                    seq_number: self.next_seq_number,
                },
            ),
        );

        self.size += entry_len;
        self.next_seq_number += 1;
    }

    #[inline]
    pub fn is_full(&self, params: &Params) -> bool {
        self.size >= params.max_memtable_size
    }

    //FIXME avoid this copy somehow without breaking seek consistency
    pub fn get_entries(&self) -> Vec<(Key, MemtableEntry)> {
        self.entries.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_put() {
        let mut mem = Memtable::new(1);

        let key1 = vec![5, 2, 4];
        let key2 = vec![3, 8, 1];

        let val1 = vec![5, 1];
        let val2 = vec![1, 8];

        mem.put(key1.clone(), val1.clone());
        mem.put(key2.clone(), val2.clone());

        assert_eq!(mem.get(&key1).unwrap().get_value().unwrap(), &val1);
        assert_eq!(mem.get(&key2).unwrap().get_value().unwrap(), &val2);
    }

    #[test]
    fn delete() {
        let mut mem = Memtable::new(1);

        assert_eq!(mem.entries.len(), 0);

        let key = vec![5, 2, 4];
        let val = vec![5, 1];

        mem.put(key.clone(), val.clone());
        mem.delete(key.clone());

        assert_eq!(mem.entries.len(), 1);
        assert_eq!(mem.get(&key).unwrap().get_value(), None);
    }

    #[test]
    fn override_entry() {
        let mut mem = Memtable::new(1);

        let key1 = vec![5, 2, 4];

        let val1 = vec![5, 1];
        let val2 = vec![1, 8];

        mem.put(key1.clone(), val1.clone());
        mem.put(key1.clone(), val2.clone());

        assert_eq!(mem.get(&key1).unwrap().get_value().unwrap(), &val2);
    }
}
