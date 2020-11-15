use crate::sorted_table::Key;
use crate::entry::Entry;
use crate::values::ValueId;
use crate::Params;

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[ derive(Clone) ]
pub struct MemtableRef {
    //TODO this rw lock is not really needed because there is another lock in DbInner
    // Not sure how to remove the lock logic without using unsafe code though
    inner: Arc<RwLock<Memtable>>
}

#[ derive(Clone) ]
pub struct ImmMemtableRef {
    //TODO see comment above
    inner: Arc<RwLock<Memtable>>
}

pub struct MemtableIterator {
    //TODO see comment above
    inner: Arc<RwLock<Memtable>>,
    next_index: usize,
    key: Option<Key>,
    value: Option<ValueId>
}

impl MemtableIterator {
    pub fn new(inner: Arc<RwLock<Memtable>>) -> Self {
        let ilock = inner.read().unwrap();

        if let Some((key, value)) = ilock.table.get(0) {
            let key = Some(key.clone());
            let value = Some(value.clone());
            drop(ilock);

            Self{ inner, key, value, next_index: 1}
        } else {
            drop(ilock);
            Self{ inner, key: None, value: None, next_index: 1 }
        }
    }

    pub fn at_end(&self) -> bool {
        let len = self.inner.read().unwrap().table.len();
        self.next_index >= len
    }

    pub fn get_key(&self) -> &Key {
        &(self.key.as_ref().expect("Not a valid iterator"))
    }

    pub fn get_value_ref(&self) -> &ValueId {
        &(self.value.as_ref().expect("Not a valid iterator"))
    }
}


impl ImmMemtableRef {
    pub fn get<'a>(&'a self) -> RwLockReadGuard<Memtable> {
        self.inner.read().unwrap()
    }

    pub fn into_iter(self) -> MemtableIterator {
        MemtableIterator::new( self.inner )
    }
}

impl MemtableRef {
    pub fn wrap(inner: Memtable) -> Self {
        Self{ inner: Arc::new( RwLock::new( inner )) }
    }

    pub fn clone_immutable(&self) -> ImmMemtableRef {
        ImmMemtableRef{ inner: self.inner.clone() }
    }

    /// Make the current contents into an immutable memtable
    /// And create a new mutable one
    pub fn take(&mut self) -> ImmMemtableRef {
        let mut inner =  Arc::new( RwLock::new( Memtable::new() ));
        std::mem::swap(&mut inner, &mut self.inner);

        ImmMemtableRef{ inner }
    }

    pub fn get(& self) -> RwLockReadGuard<Memtable> {
        self.inner.read().unwrap()
    }

    pub fn get_mut<'a>(&'a self) -> RwLockWriteGuard<Memtable> {
        self.inner.write().unwrap()
    }
}

pub struct Memtable {
    // Sorted data
    table: Vec<(Key, ValueId)>,

    // Sequential updates
    entries: Vec<(Key, Entry)>,
    size: usize,

    //TODO move this somewhere else
    next_seq_number: u64
}

impl Memtable {
    pub fn new() -> Self {
        let entries = Vec::new();
        let size = 0;
        let table = Vec::new();
        let next_seq_number = 0;

        Self{entries, size, next_seq_number, table}
    }

    pub fn get(&self, key: &[u8]) -> Option<ValueId> {
        match self.table.binary_search_by_key(&key, |t| t.0.as_slice()) {
            Ok(pos) => Some(self.table[pos].1.clone()),
            Err(_) => None
        }
    }

    pub fn put(&mut self, key: Key, value_ref: ValueId, value_len: usize) {
        self.size += value_len;
        self.entries.push((key.clone(), Entry{
            value_ref, seq_number: self.next_seq_number
        }));

        self.next_seq_number += 1;

        match self.table.binary_search_by_key(&&key, |t| &t.0) {
            Ok(pos) => {
                // Override
                self.table[pos] = (key, value_ref);
            }
            Err(pos) => {
                // Does not exist yet; insert after
                self.table.insert(pos, (key, value_ref));
            }
        }
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

#[ cfg(test) ]
mod tests {
    use super::*;

    #[test]
    fn iterate() {
        let reference = MemtableRef::wrap( Memtable::new() );

        let iter = reference.clone_immutable().into_iter();
        assert_eq!(iter.at_end(), true);

        let key = vec![5,1,2,3];
        let val_id = (5, 141);

        reference.get_mut().put(key, val_id.clone(), 1024);

        let iter = reference.clone_immutable().into_iter();
        assert_eq!(iter.get_value_ref(), &val_id);
    }

}
