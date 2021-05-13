#[ cfg(feature="wisckey") ]
use crate::values::{ValueLog, ValueId};

#[ cfg(feature="wisckey") ]
use crate::sorted_table::ValueResult;

use crate::sorted_table::{InternalIterator, TableIterator};
use crate::memtable::MemtableIterator;
use crate::KV_Trait;

use bincode::Options;

use std::future::Future;
use std::task::{Context, Poll};
use std::marker::PhantomData;
use std::pin::Pin;
use std::cmp::Ordering;

#[ cfg(feature="wisckey") ]
use std::sync::Arc;

use cfg_if::cfg_if;

use futures::stream::Stream;

type IterFuture<K, V> = dyn Future<Output = (DbIteratorInner<K,V>, Option<(K,V)>)>+Send;

pub struct DbIterator<K: KV_Trait, V: KV_Trait> {
    state: Option<Pin<Box<IterFuture<K,V>>>>
}

impl<K: KV_Trait, V: KV_Trait> DbIterator<K, V> {
    #[ cfg(feature="wisckey") ]
    pub(crate) fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>,
                      value_log: Arc<ValueLog>) -> Self {
        let inner = DbIteratorInner::new(mem_iters, table_iters, value_log);
        let state = Box::pin(DbIteratorInner::next(inner));

        Self{ state: Some(state) }
    }

    #[ cfg(not(feature="wisckey")) ]
    pub(crate) fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>) -> Self {
        let inner = DbIteratorInner::new(mem_iters, table_iters);
        let state = Box::pin(DbIteratorInner::next(inner));

        Self{ state: Some(state) }
    }
}

impl<K: KV_Trait, V: KV_Trait> Stream for DbIterator<K, V> {
    type Item = (K, V);

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let (inner, res) = if let Some(mut fut) = self.state.take() {
            match Future::poll(fut.as_mut(), ctx) {
                // return and keep waiting for result
                Poll::Pending => {
                    self.state = Some(fut);
                    return Poll::Pending
                }
                // item computation complete
                Poll::Ready((inner, res)) => (inner, res),
            }
        } else {
            // no items left
            return Poll::Ready(None)
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

struct DbIteratorInner<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,

    last_key: Option<Vec<u8>>,
    iterators: Vec<Box<dyn InternalIterator>>,

    #[ cfg(feature="wisckey") ]
    value_log: Arc<ValueLog>,
}

#[ cfg(feature="wisckey") ]
enum IterResult<V: KV_Trait> {
    Value(V),
    ValueRef(ValueId),
}

type MinKV = Option<(crate::manifest::SeqNumber, usize)>;

impl<K: KV_Trait, V: KV_Trait> DbIteratorInner<K, V> {
    #[ cfg(feature="wisckey") ]
    fn new(mut mem_iters: Vec<MemtableIterator>, mut table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        let mut iterators: Vec<Box<dyn InternalIterator>>= vec![];
        for iter in mem_iters.drain(..) {
            iterators.push(Box::new(iter));
        }
        for iter in table_iters.drain(..) {
            iterators.push(Box::new(iter));
        }

        Self{
            iterators, value_log, last_key: None, _marker: PhantomData
        }
    }

    #[ cfg(not(feature="wisckey")) ]
    fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>) -> Self {
        Self{
            mem_iters, table_iters,
            last_key: None, _marker: PhantomData
        }
    }

    async fn parse_iter(&mut self, offset: usize, min_kv: MinKV) -> (bool, MinKV) {
        // Split slices to make the borrow checker happy
        let (prev, cur) = self.iterators[..].split_at_mut(offset);
        let iter = &mut *cur[0];

        if let Some(last_key) = &self.last_key {
            while !iter.at_end() && iter.get_key() <= last_key {
                iter.step().await;
            }
        }

        if iter.at_end() {
            return (false, min_kv);
        }

        let key = iter.get_key();
        let seq_number = iter.get_seq_number();

        if let Some((min_seq_number, min_offset)) = min_kv {
            let min_iter = &*prev[min_offset];
            let min_key = min_iter.get_key();

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


    async fn next(mut self) -> (Self, Option<(K,V)>) {
        let mut result: Option<Option<(K, IterResult<V>)>> = None;

        while result.is_none() {
            let mut min_kv = None;
            let num_iterators = self.iterators.len();

            for offset in 0..num_iterators {
                let (change, kv) = self.parse_iter(offset, min_kv).await;

                if change {
                    min_kv = kv;
                }
            }

            if let Some((_, offset)) = min_kv.take() {
                let encoder = crate::get_encoder();
                let iter = &*self.iterators[offset];

                let res_key = encoder.deserialize(iter.get_key()).unwrap();
                self.last_key = Some(iter.get_key().clone());

                cfg_if! {
                    if #[ cfg(feature="wisckey") ] {
                        match iter.get_value() {
                            ValueResult::Value(value) => {
                                let encoder = crate::get_encoder();
                                result = Some(Some((res_key, IterResult::Value(encoder.deserialize(value).unwrap()))));
                            }
                            ValueResult::Reference(value_ref) => {
                                let value_ref = value_ref;
                                result = Some(Some((res_key, IterResult::ValueRef(value_ref))));
                                                    }
                            ValueResult::NoValue => {
                                // this is a deletion... skip
                            }
                        }
                    } else {
                        match iter.get_value() {
                            Some(value) => {
                                let res_val = encoder.deserialize(value).unwrap();
                                result = Some(Some((res_key, res_val)));
                            }
                            None => {
                                // this is a deletion... skip
                            }
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
            None => { return (self, None); }
        };

        cfg_if!{
            if #[ cfg(feature="wisckey") ] {
                match result {
                    IterResult::ValueRef(value_ref) => {
                        let res_val = self.value_log.get(value_ref).await;
                        (self, Some((key, res_val)))
                    }
                    IterResult::Value(value) => {
                        (self, Some((key, value)))
                    }
                }
            } else {
                (self, Some((key, value)))
            }
        }
    }
}
