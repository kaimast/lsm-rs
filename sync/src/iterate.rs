#[cfg(feature = "wisckey")]
use lsm::values::ValueLog;

use lsm::EntryRef;
use lsm::memtable::MemtableIterator;
use lsm::sorted_table::{InternalIterator, TableIterator};

use std::cmp::Ordering;
use std::sync::Arc;

use cfg_if::cfg_if;

/// Allows iterating over a consistent snapshot of the database
pub struct DbIterator {
    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,

    tokio_rt: Arc<tokio::runtime::Runtime>,

    reverse: bool,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

type NextKV = Option<(lsm::manifest::SeqNumber, usize)>;

impl DbIterator {
    pub(crate) fn new(
        mem_iters: Vec<MemtableIterator>,
        table_iters: Vec<TableIterator>,
        min_key: Option<Vec<u8>>,
        max_key: Option<Vec<u8>>,
        reverse: bool,
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
            last_key: None,
            iterators,
            tokio_rt,
            min_key,
            max_key,
            reverse,
            #[cfg(feature = "wisckey")]
            value_log,
        }
    }

    async fn parse_iter(
        &self,
        pos: usize,
        last_key: &Option<Vec<u8>>,
        next_iter: Option<&dyn InternalIterator>,
        iter: &mut dyn InternalIterator,
        next_kv: NextKV,
    ) -> (bool, NextKV) {
        if self.reverse {
            // This iterator might be "behind" other iterators
            if let Some(last_key) = last_key {
                while !iter.at_end() && iter.get_key() >= last_key.as_slice() {
                    iter.step().await;
                }
            }

            // Don't pick a key that is greater than the maximum
            if let Some(max_key) = &self.max_key {
                while !iter.at_end() && iter.get_key() > max_key.as_slice() {
                    iter.step().await;
                }

                // There might be no key in this iterator that is <=max_key
                if iter.at_end() || iter.get_key() > max_key.as_slice() {
                    return (false, next_kv);
                }
            }

            if iter.at_end() {
                return (false, next_kv);
            }

            let key = iter.get_key();

            // Don't pick a key that is less or equal to the minimum
            if let Some(min_key) = &self.min_key
                && iter.get_key() <= min_key.as_slice()
            {
                return (false, next_kv);
            }

            let seq_number = iter.get_seq_number();

            if let Some((max_seq_number, _)) = next_kv {
                let max_key = next_iter.unwrap().get_key();

                match key.cmp(max_key) {
                    Ordering::Greater => (true, Some((seq_number, pos))),
                    Ordering::Equal => {
                        if seq_number > max_seq_number {
                            (true, Some((seq_number, pos)))
                        } else {
                            (false, next_kv)
                        }
                    }
                    Ordering::Less => (false, next_kv),
                }
            } else {
                (true, Some((seq_number, pos)))
            }
        } else {
            // This iterator might be "behind" other iterators
            if let Some(last_key) = last_key {
                while !iter.at_end() && iter.get_key() <= last_key.as_slice() {
                    iter.step().await;
                }
            }

            // Don't pick a key that is smaller than the minimum
            if let Some(min_key) = &self.min_key {
                while !iter.at_end() && iter.get_key() < min_key.as_slice() {
                    iter.step().await;
                }

                // There might be no key in this iterator that is >=min_key
                if iter.at_end() || iter.get_key() < min_key.as_slice() {
                    return (false, next_kv);
                }
            }

            if iter.at_end() {
                return (false, next_kv);
            }

            let key = iter.get_key();

            // Don't pick a key that is greater or equal to the maximum
            if let Some(max_key) = &self.max_key
                && iter.get_key() >= max_key.as_slice()
            {
                return (false, next_kv);
            }

            let seq_number = iter.get_seq_number();

            if let Some((min_seq_number, _)) = next_kv {
                let min_key = next_iter.unwrap().get_key();

                match key.cmp(min_key) {
                    Ordering::Less => (true, Some((seq_number, pos))),
                    Ordering::Equal => {
                        if seq_number > min_seq_number {
                            (true, Some((seq_number, pos)))
                        } else {
                            (false, next_kv)
                        }
                    }
                    Ordering::Greater => (false, next_kv),
                }
            } else {
                (true, Some((seq_number, pos)))
            }
        }
    }
}

impl Iterator for DbIterator {
    type Item = (Vec<u8>, EntryRef);

    fn next(&mut self) -> Option<Self::Item> {
        let mut iterators = std::mem::take(&mut self.iterators);
        let mut last_key = self.last_key.clone();
        let mut result = None;

        while result.is_none() {
            let (out_result, out_last_key, out_iterators) = self.tokio_rt.block_on(async {
                let mut next_kv = None;
                let num_iterators = iterators.len();

                for pos in 0..num_iterators {
                    // Split slices to make the borrow checker happy
                    let (prev, cur) = iterators[..].split_at_mut(pos);

                    let next_iter = if let Some((_, pos)) = next_kv {
                        // see https://github.com/rust-lang/rust-clippy/issues/9309
                        #[allow(clippy::borrowed_box)]
                        let iter: &Box<dyn InternalIterator> = &prev[pos];
                        Some(&**iter)
                    } else {
                        None
                    };

                    let current_iter = &mut *cur[0];
                    let (change, kv) = self
                        .parse_iter(pos, &last_key, next_iter, current_iter, next_kv)
                        .await;

                    if change {
                        next_kv = kv;
                    }
                }

                let result = if let Some((_, pos)) = next_kv.take() {
                    #[allow(clippy::explicit_auto_deref)]
                    let iter: &dyn InternalIterator = &*iterators[pos];

                    let res_key = iter.get_key().to_vec();
                    last_key = Some(res_key.clone());

                    cfg_if! {
                        if #[ cfg(feature="wisckey") ] {
                            iter.get_entry(&self.value_log).await
                                .map(|entry| Some((res_key, entry)))
                        } else {
                            iter.get_entry().map(|entry|Some((res_key, entry)))
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
