use crate::values::ValueLog;
use crate::entry::Entry;
use crate::sorted_table::{Key, TableIterator, InternalIterator};
use crate::memtable::MemtableIterator;

use std::marker::PhantomData;
use std::sync::Arc;
use serde::de::DeserializeOwned;

pub struct DbIterator<K: DeserializeOwned, V: DeserializeOwned> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,

    last_key: Option<Vec<u8>>,
    mem_iters: Vec<MemtableIterator>,
    table_iters: Vec<TableIterator>,
    value_log: Arc<ValueLog>,
}

impl<K: DeserializeOwned, V: DeserializeOwned> DbIterator<K,V> {
    pub(crate) fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log,
            last_key: None, phantom_key: PhantomData, phantom_value: PhantomData
        }
    }

    #[inline]
    fn parse_iter<'a>(last_key: &Option<Key>, iter: &'a mut dyn InternalIterator,
        min_key: &mut Option<&'a Key>, min_entry: &mut Option<&'a Entry>) {
           if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= &last_key {
                iter.step();
            }
        }

        let key = iter.get_key();

        if let Some(other_key) = min_key {
            if key < other_key {
                *min_key = Some(key);
                *min_entry = Some(iter.get_entry());
            } else if &key == other_key {
                let entry = iter.get_entry();

                if entry.seq_number > min_entry.unwrap().seq_number {
                    *min_entry = Some(entry);
                }
            }
        } else {
            *min_key = Some(key);
            *min_entry = Some(iter.get_entry());
        }
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut min_key = None;
        let mut min_entry = None;

        for iter in self.mem_iters.iter_mut() {
            Self::parse_iter(&self.last_key, iter, &mut min_key, &mut min_entry);
        }

        for iter in self.table_iters.iter_mut() {
              Self::parse_iter(&self.last_key, iter, &mut min_key, &mut min_entry);
        }

        if let Some(key) = min_key {
            let res_key = bincode::deserialize(&key).unwrap();
            let value = self.value_log.get(&min_entry.unwrap().value_ref);

            self.last_key = Some(key.clone());

            Some((res_key, value))
        } else {
            None
        }
    }
}
