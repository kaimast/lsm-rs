#[cfg(feature = "wisckey")]
use crate::values::{ValueId, ValueLog};

#[cfg(feature = "wisckey")]
use crate::sorted_table::ValueResult;

use crate::memtable::MemtableIterator;
use crate::sorted_table::{InternalIterator, TableIterator};
use crate::{Error, KvTrait};

use bincode::Options;

use std::cmp::Ordering;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "wisckey")]
use std::sync::Arc;

use cfg_if::cfg_if;

use futures::stream::Stream;

#[cfg(feature = "async-io")]
type IterFuture<K, V> = dyn Future<Output = Result<(DbIteratorInner<K, V>, Option<(K, V)>), Error>>;

#[cfg(not(feature = "async-io"))]
type IterFuture<K, V> =
    dyn Future<Output = Result<(DbIteratorInner<K, V>, Option<(K, V)>), Error>> + Send;

pub struct DbIterator<K: KvTrait, V: KvTrait> {
    state: Option<Pin<Box<IterFuture<K, V>>>>,
}

impl<K: KvTrait, V: KvTrait> DbIterator<K, V> {
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

impl<K: KvTrait, V: KvTrait> Stream for DbIterator<K, V> {
    type Item = (K, V);

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

struct DbIteratorInner<K: KvTrait, V: KvTrait> {
    _marker: PhantomData<(K, V)>,

    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    reverse: bool,

    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,

    #[cfg(feature = "wisckey")]
    value_log: Arc<ValueLog>,
}

#[cfg(feature = "wisckey")]
enum IterResult<V: KvTrait> {
    Value(V),
    ValueRef(ValueId),
}

type NextKV = Option<(crate::manifest::SeqNumber, usize)>;

impl<K: KvTrait, V: KvTrait> DbIteratorInner<K, V> {
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
            _marker: PhantomData,
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
                while !iter.at_end() && iter.get_key() >= last_key {
                    iter.step().await;
                }
            }

            // Don't pick a key that is greater than the maximum
            if let Some(max_key) = &self.max_key {
                while !iter.at_end() && iter.get_key() > max_key {
                    iter.step().await;
                }

                // There might be no key in this iterator that is <=max_key
                if iter.at_end() || iter.get_key() > max_key {
                    return (false, next_kv);
                }
            }

            if iter.at_end() {
                return (false, next_kv);
            }

            let key = iter.get_key();

            // Don't pick a key that is less or equal to the minimum
            if let Some(min_key) = &self.min_key {
                if iter.get_key().as_slice() <= min_key.as_slice() {
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
                    return (false, next_kv);
                }
            }

            if iter.at_end() {
                return (false, next_kv);
            }

            let key = iter.get_key();

            // Don't pick a key that is greater or equal to the maximum
            if let Some(max_key) = &self.max_key {
                if iter.get_key().as_slice() >= max_key.as_slice() {
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

    async fn next(mut self) -> Result<(Self, Option<(K, V)>), Error> {
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
                let encoder = crate::get_encoder();
                let iter = &*self.iterators[pos];

                let res_key = encoder.deserialize(iter.get_key())?;
                self.last_key = Some(iter.get_key().clone());

                cfg_if! {
                    if #[ cfg(feature="wisckey") ] {
                        match iter.get_value() {
                            ValueResult::Value(value) => {
                                let encoder = crate::get_encoder();
                                result = Some(Some((res_key, IterResult::Value(encoder.deserialize(value)?))));
                            }
                            ValueResult::Reference(value_ref) => {
                                result = Some(Some((res_key, IterResult::ValueRef(value_ref))));
                                                    }
                            ValueResult::NoValue => {
                                // this is a deletion... skip
                            }
                        }
                    } else {
                        if let Some(value) = iter.get_value() {
                            let res_val = encoder.deserialize(value)?;
                            result = Some(Some((res_key, res_val)));
                        } else {
                            // this is a deletion... skip
                        }
                    }
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

        cfg_if! {
            if #[ cfg(feature="wisckey") ] {
                match result {
                    IterResult::ValueRef(value_ref) => {
                        let res_val = self.value_log.get(value_ref).await?;
                        Ok((self, Some((key, res_val))))
                    }
                    IterResult::Value(value) => {
                        Ok((self, Some((key, value))))
                    }
                }
            } else {
                Ok((self, Some((key, result))))
            }
        }
    }
}
