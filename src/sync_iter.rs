use crate::values::ValueLog;
use crate::entry::Entry;
use crate::sorted_table::{Key, TableIterator, InternalIterator};
use crate::memtable::MemtableIterator;
use crate::KV_Trait;

use bincode::Options;

use std::marker::PhantomData;
use std::sync::Arc;
use std::cmp::Ordering;

/// Allows iterating over a consistent snapshot of the database
pub struct DbIterator<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    last_key: Option<Vec<u8>>,
    mem_iters: Vec<MemtableIterator>,
    table_iters: Vec<TableIterator>,
    value_log: Arc<ValueLog>,
    tokio_rt: Arc<tokio::runtime::Runtime>
}

impl<K: KV_Trait, V: KV_Trait> DbIterator<K,V> {
    #[ cfg(feature="sync") ]
    pub(crate) fn new(tokio_rt: Arc<tokio::runtime::Runtime>,
                      mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log, tokio_rt,
            last_key: None, _marker: PhantomData
        }
    }

    #[inline]
    pub(crate) fn parse_iter<'a>(last_key: &Option<Key>, iter: &'a mut dyn InternalIterator,
            min_kv: &mut Option<(&'a Key, &'a Entry)>) -> bool {
        if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step();
            }
        }

        if iter.at_end() {
            return false;
        }

        let key = iter.get_key();
        let entry = iter.get_entry();

        if let Some((min_key, min_entry)) = min_kv {
            match key.cmp(min_key) {
                Ordering::Less => {
                    *min_kv = Some((key, entry));
                    true
                }
                Ordering::Equal => {
                    if entry.get_sequence_number() > min_entry.get_sequence_number() {
                        *min_kv = Some((key, entry));
                        true
                    } else {
                        false
                    }
                }
                Ordering::Greater => false
            }
        } else {
            *min_kv = Some((key, entry));
            true
        }
    }

    fn next_entry(&mut self) -> Option<(bool, K, Entry)> {
        let mut min_kv = None;
        let mut is_pending = true;

        for iter in self.mem_iters.iter_mut() {
            Self::parse_iter(&self.last_key, iter, &mut min_kv);
        }

        for iter in self.table_iters.iter_mut() {
            if Self::parse_iter(&self.last_key, iter, &mut min_kv) {
                is_pending = false;
            }
        }
        if let Some((key, entry)) = min_kv {
            let res_key = super::get_encoder().deserialize(&key).unwrap();
            let entry = entry.clone();

            self.last_key = Some(key.clone());
            Some((is_pending, res_key, entry))//TODO can we avoid cloning here?
        } else {
            None
        }
    }
}

impl<K: KV_Trait, V: KV_Trait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let result = self.next_entry();
            let value_log = &*self.value_log;

            if let Some((is_pending, key, entry)) = result {
                match entry {
                    Entry::Value{value_ref, ..} => {
                        return self.tokio_rt.block_on(async move {
                            let res_val = if is_pending {
                                value_log.get_pending(value_ref).await
                            } else {
                                value_log.get(value_ref).await
                            };

                            Some((key, res_val))
                        });
                    }
                    Entry::Deletion{..} => {
                        // this is a deletion... skip
                    }
                }
            } else {
                return None;
            }
        }
    }
}

