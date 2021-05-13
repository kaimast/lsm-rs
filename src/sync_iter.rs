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

use cfg_if::cfg_if;

/// Allows iterating over a consistent snapshot of the database
pub struct DbIterator<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    tokio_rt: Arc<tokio::runtime::Runtime>,

    #[ cfg(feature="wisckey") ]
    value_log: Arc<ValueLog>,
}

type MinKV = Option<(crate::manifest::SeqNumber, usize)>;

impl<K: KV_Trait, V: KV_Trait> DbIterator<K,V> {
    #[ cfg(all(feature="sync", feature="wisckey")) ]
    pub(crate) fn new(tokio_rt: Arc<tokio::runtime::Runtime>, mut mem_iters: Vec<MemtableIterator>,
            mut table_iters: Vec<TableIterator>, value_log: Arc<ValueLog>) -> Self {
        let mut iterators: Vec<Box<dyn InternalIterator>>= vec![];
        for iter in mem_iters.drain(..) {
            iterators.push(Box::new(iter));
        }
        for iter in table_iters.drain(..) {
            iterators.push(Box::new(iter));
        }
 
        Self{
            iterators, value_log, tokio_rt,
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
    async fn parse_iter(offset: usize, last_key: &Option<Key>, min_iter: Option<&dyn InternalIterator>, iter: &mut dyn InternalIterator, min_kv: MinKV) -> (bool, MinKV) {
        if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step().await;
            }
        }

        if iter.at_end() {
            return (false, min_kv);
        }

        let key = iter.get_key();
        let seq_number = iter.get_seq_number();

        if let Some((min_seq_number, _)) = min_kv {
            let min_key = min_iter.unwrap().get_key();

            match key.cmp(min_key) {
                Ordering::Less => {
                    (true, Some((seq_number, offset)))
                }
                Ordering::Equal => {
                    if seq_number > min_seq_number {
                        (true, Some((seq_number, offset)))
                    } else {
                        (false, min_kv)
                    }
                }
                Ordering::Greater => (false, min_kv)
            }
        } else {
            (true, Some((seq_number, offset)))
        }
    }
}

impl<K: KV_Trait, V: KV_Trait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut iterators = std::mem::take(&mut self.iterators);
        let mut last_key = self.last_key.clone();
        let mut result = None;

        while result.is_none() {
            #[cfg(feature="wisckey")]
            let value_log = self.value_log.clone();

            let (out_result, out_last_key, out_iterators) = self.tokio_rt.block_on(async move {
                let mut min_kv = None;
                let num_iterators = iterators.len();
                
                for offset in 0..num_iterators {
                    // Split slices to make the borrow checker happy
                    let (prev, cur) = iterators[..].split_at_mut(offset);

                    let min_iter = if let Some((_,offset)) = min_kv {
                        #[ allow(clippy::borrowed_box) ]
                        let iter: &Box<dyn InternalIterator> = &prev[offset];
                        Some(&**iter)
                    } else {
                        None
                    };

                    let current_iter = &mut *cur[0];
                    let (change, kv) = Self::parse_iter(offset, &last_key, min_iter,
                                                        current_iter, min_kv).await;

                    if change {
                        min_kv = kv;
                    }
                }

                let result = if let Some((_, offset)) = min_kv.take() {
                    let encoder = crate::get_encoder();
                    let iter: &dyn InternalIterator = &*iterators[offset];

                    let res_key = encoder.deserialize(iter.get_key()).unwrap();
                    last_key = Some(iter.get_key().clone());

                    cfg_if! {
                        if #[ cfg(feature="wisckey") ] {
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
                        } else {
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
                        }
                    }
                } else {
                    Some(None)
                };

                (result, last_key, iterators)
            });

            result = out_result;
            last_key = out_last_key;
            iterators = out_iterators;
        }

        self.last_key = last_key;
        self.iterators = iterators;

        result.unwrap()
    }
}
