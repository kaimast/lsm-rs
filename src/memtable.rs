use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;
use crate::Params;

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::BTreeMap;

#[ derive(Clone) ]
pub(crate) struct MemtableRef {
    inner: Arc<RwLock<Memtable>>
}

#[ derive(Clone) ]
pub(crate) struct ImmMemtableRef {
    inner: Arc<RwLock<Memtable>>
}

impl ImmMemtableRef {
    pub fn get<'a>(&'a self) -> RwLockReadGuard<Memtable> {
        self.inner.read().unwrap()
    }
}

impl MemtableRef {
    pub fn wrap(inner: Memtable) -> Self {
        Self{ inner: Arc::new( RwLock::new( inner )) }
    }

    /// Make the current contents into an immutable memtable
    /// And create a new mutable one
    pub fn take(&mut self) -> ImmMemtableRef {
        let mut inner =  Arc::new( RwLock::new( Memtable::new() ));
        std::mem::swap(&mut inner, &mut self.inner);

        ImmMemtableRef{ inner }
    }

    pub fn get<'a>(&'a self) -> RwLockReadGuard<Memtable> {
        self.inner.read().unwrap()
    }

    pub fn get_mut<'a>(&'a self) -> RwLockWriteGuard<Memtable> {
        self.inner.write().unwrap()
    }
}

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

    #[inline]
    pub fn is_full(&self, params: &Params) -> bool {
        self.size >= params.max_memtable_size
    }

    //FIXME avoid this copy somehow without breaking seek consistency
    pub fn get_entries(&self) -> Vec<(Key, Entry)> {
        self.entries.clone()
    }

    pub fn iter(&self) -> MemtableIterator {
        todo!();
    }
}
