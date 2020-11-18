use crate::values::ValueLog;
use crate::entry::Entry;
use crate::sorted_table::{Key, TableIterator, InternalIterator};
use crate::memtable::MemtableIterator;
use crate::KV_Trait;

use bincode::Options;

use std::marker::PhantomData;
use std::sync::Arc;

pub struct DbIterator<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    last_key: Option<Vec<u8>>,
    mem_iters: Vec<MemtableIterator>,
    table_iters: Vec<TableIterator>,
    value_log: Arc<ValueLog>,
}

impl<K: KV_Trait, V: KV_Trait> DbIterator<K,V> {
    pub(crate) fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log,
            last_key: None, _marker: PhantomData
        }
    }

    #[inline]
    fn parse_iter<'a>(last_key: &Option<Key>, iter: &'a mut dyn InternalIterator,
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
        println!("{:?}", key);

        if let Some((min_key, min_entry)) = min_kv {
            if key < min_key {
                *min_kv = Some((key, entry));
            } else if &key == min_key {
                if entry.seq_number > min_entry.seq_number {
                    *min_kv = Some((key, entry));
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            *min_kv = Some((key, entry));
        }

        true
    }
}

impl<K: KV_Trait, V: KV_Trait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
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
            let val_ref = &entry.value_ref;

            let res_val = if is_pending {
                self.value_log.get_pending(val_ref)
            } else {
                self.value_log.get(val_ref)
            };

            self.last_key = Some(key.clone());

            Some((res_key, res_val))
        } else {
            None
        }
    }
}
