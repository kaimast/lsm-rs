#[ cfg(feature="wisckey") ]
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
    tokio_rt: Arc<tokio::runtime::Runtime>,

    #[ cfg(feature="wisckey") ]
    value_log: Arc<ValueLog>,
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
    pub(crate) async fn parse_iter<'a>(last_key: &Option<Key>, iter: &'a mut dyn InternalIterator,
            min_kv: Option<(&'a Key, &'a Entry)>) -> (bool, Option<(&'a Key, &'a Entry)>) {
        if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step().await;
            }
        }

        if iter.at_end() {
            return (false, min_kv);
        }

        let key = iter.get_key();
        let entry = iter.get_entry();

        if let Some((min_key, min_entry)) = min_kv {
            match key.cmp(min_key) {
                Ordering::Less => {
                    (true, Some((key, entry)))
                }
                Ordering::Equal => {
                    if entry.get_sequence_number() > min_entry.get_sequence_number() {
                        (true, Some((key, entry)))
                    } else {
                        (false, min_kv)
                    }
                }
                Ordering::Greater => (false, min_kv)
            }
        } else {
            (true, Some((key, entry)))
        }
    }

    fn next_entry(&mut self) -> Option<(bool, K, Entry)> {
        let (result, last_key, mem_iters, table_iters) = {
            let mut mem_iters = std::mem::take(&mut self.mem_iters);
            let mut table_iters = std::mem::take(&mut self.table_iters);
            let mut last_key = self.last_key.clone();

            self.tokio_rt.block_on(async move {
                let mut is_pending = true;
                let mut min_kv = None;

                for iter in mem_iters.iter_mut() {
                    let (change, kv) = Self::parse_iter(&last_key, iter, min_kv).await;

                    if change {
                        min_kv = kv;
                    }
                }

                for iter in table_iters.iter_mut() {
                    let (change, kv) = Self::parse_iter(&last_key, iter, min_kv).await;

                    if change {
                        is_pending = false;
                        min_kv = kv;
                    }
                }

                let result = if let Some((key, entry)) = min_kv.take() {
                    let res_key = super::get_encoder().deserialize(&key).unwrap();
                    let entry = entry.clone();

                    last_key = Some(key.clone());
                    Some((is_pending, res_key, entry))//TODO can we avoid cloning here?
                } else {
                    None
                };

                (result, last_key, mem_iters, table_iters)
            })
        };

        self.last_key = last_key;
        self.mem_iters = mem_iters;
        self.table_iters = table_iters;

        result
    }
}

impl<K: KV_Trait, V: KV_Trait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    #[ cfg(feature="wisckey") ]
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

    #[ cfg(not(feature="wisckey")) ]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let result = self.next_entry();

            if let Some((_, key, entry)) = result {
                match entry {
                    Entry::Value{value, ..} => {
                        let encoder = crate::get_encoder();
                        let res_val = encoder.deserialize(&value).unwrap();
                        return Some((key, res_val));
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

