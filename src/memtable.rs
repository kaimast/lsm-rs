use crate::sorted_table::{InternalIterator, Key};
use crate::entry::Entry;
use crate::values::ValueId;
use crate::Params;

use std::sync::Arc;

#[ derive(Clone) ]
pub struct MemtableRef {
    //TODO this rw lock is not really needed because there is another lock in DbInner
    // Not sure how to remove the lock logic without using unsafe code though
    inner: Arc<Memtable>
}

#[ derive(Clone) ]
pub struct ImmMemtableRef {
    inner: Arc<Memtable>
}

pub struct MemtableIterator {
    inner: Arc<Memtable>,
    next_index: usize,
    key: Option<Key>,
    entry: Option<Entry>
}

impl MemtableIterator {
    pub fn new(inner: Arc<Memtable>) -> Self {
        let mut obj = Self{ inner, key: None, entry: None, next_index: 0 };
        obj.step();

        obj
    }
}

impl InternalIterator for MemtableIterator {
    fn step(&mut self) {
        let entries = &self.inner.entries;

        #[ allow(clippy::comparison_chain) ]
        if self.next_index > entries.len() {
            panic!("Cannot step(); already at end");
        } else if self.next_index == entries.len() {
            self.next_index += 1;
            return;
        }

        let key = &entries[self.next_index].0;

        while self.next_index+1 < entries.len()
            && &entries[self.next_index+1].0 == key {
            self.next_index += 1;
        }

        let (key, entry) = entries[self.next_index].clone();
        self.key = Some(key);
        self.entry = Some(entry);
        self.next_index += 1;
    }

    fn at_end(&self) -> bool {
        let len = self.inner.entries.len();
        self.next_index > len
    }

    fn get_key(&self) -> &Key {
        self.key.as_ref().expect("Not a valid iterator")
    }

    fn get_entry(&self) -> &Entry {
        self.entry.as_ref().expect("Not a valid iterator")
    }
}


impl ImmMemtableRef {
    pub fn get(&self) -> &Memtable {
        &*self.inner
    }

    pub fn into_iter(self) -> MemtableIterator {
        MemtableIterator::new( self.inner )
    }
}

impl MemtableRef {
    pub fn wrap(inner: Memtable) -> Self {
        Self{ inner: Arc::new(inner) }
    }

    pub fn clone_immutable(&self) -> ImmMemtableRef {
        ImmMemtableRef{ inner: self.inner.clone() }
    }

    /// Make the current contents into an immutable memtable
    /// And create a new mutable one
    pub fn take(&mut self, next_seq_number: u64)  -> ImmMemtableRef {
        let mut inner =  Arc::new( Memtable::new(next_seq_number) );
        std::mem::swap(&mut inner, &mut self.inner);

        ImmMemtableRef{ inner }
    }

    pub fn get(&self) -> &Memtable {
        &*self.inner
    }

    /// This is only safe to call from the DbLogic while holding the memtable lock
    pub unsafe fn get_mut(&mut self) -> &mut Memtable {
        Arc::get_mut_unchecked(&mut self.inner)
    }
}

pub struct Memtable {
    // Sorted upadtes
    entries: Vec<(Key, Entry)>,
    size: usize,

    //TODO move this somewhere else
    next_seq_number: u64
}

impl Memtable {
    pub fn new(next_seq_number: u64) -> Self {
        let entries = Vec::new();
        let size = 0;

        Self{entries, size, next_seq_number}
    }

    #[inline]
    pub fn get_next_seq_number(&self) -> u64 {
        self.next_seq_number
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        match self.entries.binary_search_by_key(&key, |t| t.0.as_slice()) {
            Ok(mut pos) => {
                //Find most recent update
                while self.entries.len() > pos+1
                        && self.entries[pos+1].0 == key {
                    pos += 1;
                }
                Some(self.entries[pos].1.clone())
            }
            Err(_) => None
        }
    }

    pub fn put(&mut self, key: Key, value_ref: ValueId, value_len: usize) {
        let pos = match self.entries.binary_search_by_key(&key.as_slice(), |t| t.0.as_slice()) {
            Ok(mut pos) => {
                //Find most recent update
                while pos+1 < self.entries.len()
                    && self.entries[pos+1].0 == key {
                    pos += 1;
                }
                pos+1
            },
            Err(pos) => pos,
        };

        self.entries.insert(pos,
            (key, Entry::Value{
                value_ref, seq_number: self.next_seq_number
            })
        );

        self.size += value_len;
        self.next_seq_number += 1;
    }


    pub fn delete(&mut self, key: Key) {
        let pos = match self.entries.binary_search_by_key(&key.as_slice(), |t| t.0.as_slice()) {
            Ok(mut pos) => {
                //Find most recent update
                while pos+1 < self.entries.len()
                    && self.entries[pos+1].0 == key {
                    pos += 1;
                }
                pos+1
            },
            Err(pos) => pos,
        };

        self.entries.insert(pos,
            (key, Entry::Deletion{
                _value_ref: (0,0), seq_number: self.next_seq_number
            })
        );

        self.next_seq_number += 1;
    }

    #[inline]
    pub fn is_full(&self, params: &Params) -> bool {
        self.size >= params.max_memtable_size
    }

    //FIXME avoid this copy somehow without breaking seek consistency
    pub fn get_entries(&self) -> Vec<(Key, Entry)> {
        self.entries.clone()
    }
}

#[ cfg(test) ]
mod tests {
    use super::*;

    #[test]
    fn iterate() {
        let mut reference = MemtableRef::wrap( Memtable::new(1) );

        let iter = reference.clone_immutable().into_iter();
        assert_eq!(iter.at_end(), true);

        let key1 = bincode::serialize(&(5u64)).unwrap();
        let val1 = (5, 141);

        let key2 = bincode::serialize(&(10u64)).unwrap();
        let val2 = (92, 76);

        unsafe{ reference.get_mut().put(key1.clone(), val1.clone(), 1024) };
        unsafe{ reference.get_mut().put(key2.clone(), val2.clone(), 1024) };

        let mut iter = reference.clone_immutable().into_iter();

        assert_eq!(iter.get_key(), &key1);
        assert_eq!(iter.get_entry().get_value_ref().unwrap(), &val1);

        iter.step();

        assert_eq!(iter.get_key(), &key2);
        assert_eq!(iter.get_entry().get_value_ref().unwrap(), &val2);

        assert_eq!(iter.at_end(), false);

        iter.step();

        assert_eq!(iter.at_end(), true);
    }

    #[test]
    fn get_put() {
        let mut mem = Memtable::new(1);

        let key1 = vec![5, 2, 4];
        let key2 = vec![3, 8, 1];

        let val1 = (5, 1);
        let val2 = (1, 8);

        mem.put(key1.clone(), val1.clone(), 50);
        mem.put(key2.clone(), val2.clone(), 41);

        assert_eq!(mem.get(&key1).unwrap().get_value_ref().unwrap(), &val1);
        assert_eq!(mem.get(&key2).unwrap().get_value_ref().unwrap(), &val2);
    }

    #[test]
    fn override_entry() {
        let mut mem = Memtable::new(1);

        let key1 = vec![5, 2, 4];

        let val1 = (5, 1);
        let val2 = (1, 8);

        mem.put(key1.clone(), val1.clone(), 50);
        mem.put(key1.clone(), val2.clone(), 42);

        assert_eq!(mem.get(&key1).unwrap().get_value_ref().unwrap(), &val2);
    }
}
