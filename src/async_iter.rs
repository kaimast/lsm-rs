#[ cfg(feature="wisckey") ]
use crate::values::{ValueLog, ValueId};

#[ cfg(feature="wisckey") ]
use crate::sorted_table::ValueResult;

use crate::sorted_table::TableIterator;
use crate::memtable::MemtableIterator;
use crate::KV_Trait;
use crate::sync_iter::DbIterator as SyncIter;

use bincode::Options;

use std::future::Future;
use futures::stream::Stream;
use std::task::{Context, Poll};

use std::marker::PhantomData;
use std::pin::Pin;

#[ cfg(feature="wisckey") ]
use std::sync::Arc;

use cfg_if::cfg_if;

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
    mem_iters: Vec<MemtableIterator>,
    table_iters: Vec<TableIterator>,

    #[ cfg(feature="wisckey") ]
    value_log: Arc<ValueLog>,
}

#[ cfg(feature="wisckey") ]
enum IterResult<V: KV_Trait> {
    Value(V),
    ValueRef(ValueId),
}

impl<K: KV_Trait, V: KV_Trait> DbIteratorInner<K, V> {
    #[ cfg(feature="wisckey") ]
    fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log,
            last_key: None, _marker: PhantomData
        }
    }

    #[ cfg(not(feature="wisckey")) ]
    fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>) -> Self {
        Self{
            mem_iters, table_iters,
            last_key: None, _marker: PhantomData
        }
    }

    async fn next(mut self) -> (Self, Option<(K,V)>) {
        let mut result = None;

        while result.is_none() {
            let mut min_kv = None;

            let mut mem_iters = std::mem::take(&mut self.mem_iters);
            let mut table_iters = std::mem::take(&mut self.table_iters);

            for iter in mem_iters.iter_mut() {
                let (change, kv) = SyncIter::<K,V>::parse_iter(&self.last_key, iter, min_kv).await;

                if change {
                    min_kv = kv;
                }
            }

            for iter in table_iters.iter_mut() {
                let (change, kv) = SyncIter::<K,V>::parse_iter(&self.last_key, iter, min_kv).await;

                if change {
                    min_kv = kv;
                }
            }

            if let Some((key, iter)) = min_kv.take() {
                let res_key = super::get_encoder().deserialize(&key).unwrap();
                self.last_key = Some(key.clone());

                cfg_if! {
                    if #[ cfg(feature="wisckey") ] {
                        match iter.get_value() {
                            ValueResult::Value(value) => {
                                let encoder = crate::get_encoder();
                                result = Some(Some((res_key, IterResult::Value(encoder.deserialize(value).unwrap()))));
                            }
                            ValueResult::Reference(value_ref) => {
                                let value_ref = value_ref.clone();
                                result = Some(Some((res_key, IterResult::ValueRef(value_ref))));
                                                    }
                            ValueResult::NoValue => {
                                // this is a deletion... skip
                            }
                        }
                    } else {
                        match iter.get_value() {
                            Some(value) => {
                                let encoder = crate::get_encoder();
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
                result = None;
            };

            self.mem_iters = mem_iters;
            self.table_iters = table_iters;
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
