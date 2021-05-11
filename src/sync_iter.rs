#[ cfg(feature="wisckey") ]
use crate::values::ValueLog;

#[ cfg(feature="wisckey") ]
use crate::sorted_table::ValueResult;

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
    #[ cfg(all(feature="sync", feature="wisckey")) ]
    pub(crate) fn new(tokio_rt: Arc<tokio::runtime::Runtime>, mem_iters: Vec<MemtableIterator>,
            table_iters: Vec<TableIterator>, value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log, tokio_rt,
            last_key: None, _marker: PhantomData
        }
    }

    #[ cfg(all(feature="sync", not(feature="wisckey"))) ]
    pub(crate) fn new(tokio_rt: Arc<tokio::runtime::Runtime>, mem_iters: Vec<MemtableIterator>,
            table_iters: Vec<TableIterator>) -> Self {
        Self{
            mem_iters, table_iters, tokio_rt,
            last_key: None, _marker: PhantomData
        }
    }

    #[inline]
    pub(crate) async fn parse_iter<'a>(last_key: &Option<Key>, iter: &'a mut dyn InternalIterator,
            min_kv: Option<(&'a Key, &'a dyn InternalIterator)>)
             -> (bool, Option<(&'a Key, &'a dyn InternalIterator)>) {
        if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step().await;
            }
        }

        if iter.at_end() {
            return (false, min_kv);
        }

        let key = iter.get_key();

        if let Some((min_key, min_iter)) = min_kv {
            match key.cmp(min_key) {
                Ordering::Less => {
                    (true, Some((key, iter)))
                }
                Ordering::Equal => {
                    if iter.get_seq_number() > min_iter.get_seq_number() {
                        (true, Some((key, iter)))
                    } else {
                        (false, min_kv)
                    }
                }
                Ordering::Greater => (false, min_kv)
            }
        } else {
            (true, Some((key, iter)))
        }
    }

}

impl<K: KV_Trait, V: KV_Trait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut mem_iters = std::mem::take(&mut self.mem_iters);
        let mut table_iters = std::mem::take(&mut self.table_iters);
        let mut last_key = self.last_key.clone();
        let mut result = None;

        while result.is_none() {
            #[cfg(feature="wisckey")]
            let value_log = self.value_log.clone();

            let (out_result, out_last_key, out_mem_iters, out_table_iters) = self.tokio_rt.block_on(async move {
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
                        min_kv = kv;
                    }
                }

                let result = if let Some((key, iter)) = min_kv.take() {
                    let res_key = super::get_encoder().deserialize(&key).unwrap();
                    last_key = Some(key.clone());

                    #[ cfg(feature="wisckey") ]
                    match iter.get_value() {
                        ValueResult::Value(value) => {
                            let encoder = crate::get_encoder();
                            Some(Some((res_key, encoder.deserialize(value).unwrap())))
                        }
                        ValueResult::Reference(value_ref) => {
                            let res_val = value_log.get(value_ref).await;
                            Some(Some((res_key, res_val)))
                        }
                        ValueResult::NoValue => {
                            // this is a deletion... skip
                            None
                        }
                    }
                    #[ cfg(not(feature="wisckey")) ]
                    match iter.get_value() {
                        Some(value) => {
                            let encoder = crate::get_encoder();
                            let res_val = encoder.deserialize(value).unwrap();
                            Some(Some((res_key, res_val)))
                        }
                        None => {
                            // this is a deletion... skip
                            None
                        }
                    }
                } else {
                    Some(None)
                };

                (result, last_key, mem_iters, table_iters)
            });

            result = out_result;
            last_key = out_last_key;
            mem_iters = out_mem_iters;
            table_iters = out_table_iters;
        }

        self.last_key = last_key;
        self.mem_iters = mem_iters;
        self.table_iters = table_iters;

        result.unwrap()
    }
}

