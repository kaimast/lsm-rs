#[cfg(feature = "wisckey")]
use crate::values::ValueLog;

#[cfg(feature = "wisckey")]
use crate::sorted_table::ValueResult;

use crate::memtable::MemtableIterator;
use crate::sorted_table::{InternalIterator, Key, TableIterator};
use crate::KvTrait;

use bincode::Options;

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;

use cfg_if::cfg_if;

/// Allows iterating over a consistent snapshot of the database
pub struct DbIterator<K: KvTrait, V: KvTrait> {
    _marker: PhantomData<fn(K, V)>,

    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,

    tokio_rt: Arc<tokio::runtime::Runtime>,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

type MinKV = Option<(crate::manifest::SeqNumber, usize)>;

impl<K: KvTrait, V: KvTrait> DbIterator<K, V> {
    pub(crate) fn new(
        mem_iters: Vec<MemtableIterator>,
        table_iters: Vec<TableIterator>,
        min_key: Option<Vec<u8>>,
        max_key: Option<Vec<u8>>,
        #[cfg(feature = "wisckey")] value_log: Arc<ValueLog>,
        tokio_rt: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let mut iterators: Vec<Box<dyn InternalIterator>> = vec![];

        for iter in mem_iters.into_iter() {
            iterators.push(Box::new(iter));
        }

        for iter in table_iters.into_iter() {
            iterators.push(Box::new(iter));
        }

        Self {
            _marker: PhantomData,
            last_key: None,
            iterators,
            tokio_rt,
            min_key,
            max_key,
            #[cfg(feature = "wisckey")]
            value_log,
        }
    }

    async fn parse_iter(
        &self,
        pos: usize,
        last_key: &Option<Key>,
        min_iter: Option<&dyn InternalIterator>,
        iter: &mut dyn InternalIterator,
        min_kv: MinKV,
    ) -> (bool, MinKV) {
        if let Some(last_key) = last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step().await;
            }
        }

        // Don't pick a key that is smaller than the minimum
        if let Some(min_key) = &self.min_key {
            while !iter.at_end() && iter.get_key() < min_key {
                iter.step().await;
            }

            // There might be no key in this iterator that is >=min_key
            if iter.at_end() || iter.get_key() < min_key {
                return (false, min_kv);
            }
        }

        if iter.at_end() {
            return (false, min_kv);
        }

        // Don't pick a key that is greater than the maximum
        if let Some(max_key) = &self.max_key {
            if iter.get_key().as_slice() >= max_key.as_slice() {
                return (false, min_kv);
            }
        }

        let key = iter.get_key();
        let seq_number = iter.get_seq_number();

        if let Some((min_seq_number, _)) = min_kv {
            let min_key = min_iter.unwrap().get_key();

            match key.cmp(min_key) {
                Ordering::Less => (true, Some((seq_number, pos))),
                Ordering::Equal => {
                    if seq_number > min_seq_number {
                        (true, Some((seq_number, pos)))
                    } else {
                        (false, min_kv)
                    }
                }
                Ordering::Greater => (false, min_kv),
            }
        } else {
            (true, Some((seq_number, pos)))
        }
    }
}

impl<K: KvTrait, V: KvTrait> Iterator for DbIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut iterators = std::mem::take(&mut self.iterators);
        let mut last_key = self.last_key.clone();
        let mut result = None;

        while result.is_none() {
            let (out_result, out_last_key, out_iterators) = self.tokio_rt.block_on(async {
                let mut min_kv = None;
                let num_iterators = iterators.len();

                for pos in 0..num_iterators {
                    // Split slices to make the borrow checker happy
                    let (prev, cur) = iterators[..].split_at_mut(pos);

                    let min_iter = if let Some((_, pos)) = min_kv {
                        #[allow(clippy::borrowed_box)]
                        let iter: &Box<dyn InternalIterator> = &prev[pos];
                        Some(&**iter)
                    } else {
                        None
                    };

                    let current_iter = &mut *cur[0];
                    let (change, kv) = self
                        .parse_iter(pos, &last_key, min_iter, current_iter, min_kv)
                        .await;

                    if change {
                        min_kv = kv;
                    }
                }

                let result = if let Some((_, pos)) = min_kv.take() {
                    let encoder = crate::get_encoder();
                    #[ allow(clippy::explicit_auto_deref)]
                    let iter: &dyn InternalIterator = &*iterators[pos];

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
                                    let res_val = self.value_log.get(value_ref).await.unwrap();
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
