use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "wisckey")]
use std::sync::Arc;

#[cfg(feature = "wisckey")]
use crate::values::ValueLog;

use crate::logic::EntryRef;
use crate::memtable::MemtableIterator;
use crate::sorted_table::{InternalIterator, TableIterator};
use crate::{Error, Key};

use futures::stream::Stream;

#[cfg(feature = "_async-io")]
type IterFuture = dyn Future<Output = Result<(DbIteratorInner, Option<(Key, EntryRef)>), Error>>;

#[cfg(not(feature = "_async-io"))]
type IterFuture =
    dyn Future<Output = Result<(DbIteratorInner, Option<(Key, EntryRef)>), Error>> + Send;

pub struct DbIterator {
    state: Option<Pin<Box<IterFuture>>>,
}

impl DbIterator {
    pub(crate) fn new(
        mem_iters: Vec<MemtableIterator>,
        table_iters: Vec<TableIterator>,
        min_key: Option<Vec<u8>>,
        max_key: Option<Vec<u8>>,
        reverse: bool,
        #[cfg(feature = "wisckey")] value_log: Arc<ValueLog>,
    ) -> Self {
        let inner = DbIteratorInner::new(
            mem_iters,
            table_iters,
            min_key,
            max_key,
            reverse,
            #[cfg(feature = "wisckey")]
            value_log,
        );
        let state = Box::pin(DbIteratorInner::next(inner));

        Self { state: Some(state) }
    }
}

impl Stream for DbIterator {
    type Item = (Key, EntryRef);

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let (inner, res) = if let Some(mut fut) = self.state.take() {
            match Future::poll(fut.as_mut(), ctx) {
                // return and keep waiting for result
                Poll::Pending => {
                    self.state = Some(fut);
                    return Poll::Pending;
                }
                // item computation complete
                Poll::Ready(result) => {
                    let (inner, res) = result.expect("iteration failed");
                    (inner, res)
                }
            }
        } else {
            // no items left
            return Poll::Ready(None);
        };

        // Prepare next state?
        if res.is_some() {
            self.state = Some(Box::pin(DbIteratorInner::next(inner)));
        } else {
            self.state = None;
        }

        // return item
        Poll::Ready(res)
    }
}

struct DbIteratorInner {
    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    reverse: bool,

    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

type NextKV = Option<(crate::manifest::SeqNumber, usize)>;

impl DbIteratorInner {
    fn new(
        mem_iters: Vec<MemtableIterator>,
        table_iters: Vec<TableIterator>,
        min_key: Option<Vec<u8>>,
        max_key: Option<Vec<u8>>,
        reverse: bool,
        #[cfg(feature = "wisckey")] value_log: Arc<ValueLog>,
    ) -> Self {
        let mut iterators: Vec<Box<dyn InternalIterator>> = vec![];
        for iter in mem_iters.into_iter() {
            iterators.push(Box::new(iter));
        }
        for iter in table_iters.into_iter() {
            iterators.push(Box::new(iter));
        }

        Self {
            iterators,
            last_key: None,
            min_key,
            max_key,
            reverse,
            #[cfg(feature = "wisckey")]
            value_log,
        }
    }

    /// Tries to pick the next value from the specified iterator
    async fn parse_iter(&mut self, pos: usize, next_kv: NextKV) -> (bool, NextKV) {
        // Split slices to make the borrow checker happy
        let (prev, cur) = self.iterators[..].split_at_mut(pos);
        let iter = &mut *cur[0];

        if self.reverse {
            // This iterator might be "behind" other iterators
            if let Some(last_key) = &self.last_key {
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
            if let Some(min_key) = &self.min_key {
                if iter.get_key() <= min_key.as_slice() {
                    return (false, next_kv);
                }
            }

            let seq_number = iter.get_seq_number();

            if let Some((max_seq_number, max_pos)) = next_kv {
                let max_iter = &*prev[max_pos];
                let max_key = max_iter.get_key();

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
            if let Some(last_key) = &self.last_key {
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
            if let Some(max_key) = &self.max_key {
                if iter.get_key() >= max_key.as_slice() {
                    return (false, next_kv);
                }
            }

            let seq_number = iter.get_seq_number();

            if let Some((min_seq_number, min_pos)) = next_kv {
                let min_iter = &*prev[min_pos];
                let min_key = min_iter.get_key();

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

    async fn next(mut self) -> Result<(Self, Option<(Key, EntryRef)>), Error> {
        let mut result = None;

        while result.is_none() {
            let mut next_kv = None;
            let num_iterators = self.iterators.len();

            for pos in 0..num_iterators {
                let (change, kv) = self.parse_iter(pos, next_kv).await;

                if change {
                    next_kv = kv;
                }
            }

            if let Some((_, pos)) = next_kv.take() {
                let iter = &*self.iterators[pos];

                let res_key = iter.get_key();
                self.last_key = Some(iter.get_key().to_vec());

                #[cfg(feature = "wisckey")]
                let entry = iter.get_entry(&self.value_log).await;
                #[cfg(not(feature = "wisckey"))]
                let entry = iter.get_entry();

                if let Some(entry) = entry {
                    result = Some(Some((res_key.to_vec(), entry)));
                } else {
                    // this is a deletion... skip
                }
            } else {
                // at end
                result = Some(None);
            };
        }

        let (key, result) = match result.unwrap() {
            Some(inner) => inner,
            None => {
                return Ok((self, None));
            }
        };

        Ok((self, Some((key, result))))
    }
}
