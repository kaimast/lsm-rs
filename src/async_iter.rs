use crate::values::ValueLog;
use crate::sorted_table::TableIterator;
use crate::memtable::MemtableIterator;
use crate::KV_Trait;
use crate::entry::Entry;
use crate::sync_iter::DbIterator as SyncIter;

use bincode::Options;

use std::future::Future;
use futures::stream::Stream;
use std::task::{Context, Poll};

use std::marker::PhantomData;
use std::sync::Arc;
use std::pin::Pin;


type IterFuture<K, V> = dyn Future<Output = (DbIteratorInner<K,V>, Option<(K,V)>)>+Send;

pub struct DbIterator<K: KV_Trait, V: KV_Trait> {
    state: Option<Pin<Box<IterFuture<K,V>>>>
}

impl<K: KV_Trait, V: KV_Trait> DbIterator<K, V> {
    pub(crate) fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>,
                      value_log: Arc<ValueLog>) -> Self {
        let inner = DbIteratorInner::new(mem_iters, table_iters, value_log);
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
    value_log: Arc<ValueLog>,
}

impl<K: KV_Trait, V: KV_Trait> DbIteratorInner<K, V> {
    fn new(mem_iters: Vec<MemtableIterator>, table_iters: Vec<TableIterator>
            , value_log: Arc<ValueLog>) -> Self {
        Self{
            mem_iters, table_iters, value_log,
            last_key: None, _marker: PhantomData
        }
    }

    async fn next_entry(mut slf: Self) -> (Self, Option<(bool,K,Entry)>) {
        let mut min_kv = None;
        let mut is_pending = true;

        for iter in slf.mem_iters.iter_mut() {
            SyncIter::<K,V>::parse_iter(&slf.last_key, iter, &mut min_kv);
        }

        for iter in slf.table_iters.iter_mut() {
            if SyncIter::<K,V>::parse_iter(&slf.last_key, iter, &mut min_kv) {
                is_pending = false;
            }
        }
        if let Some((key, entry)) = min_kv {
            let res_key = super::get_encoder().deserialize(&key).unwrap();
            let entry = entry.clone();

            slf.last_key = Some(key.clone());
            (slf, Some((is_pending, res_key, entry))) //TODO can we avoid cloning here?
        } else {
            (slf, None)
        }
    }

    async fn next(mut slf_: Self) -> (Self, Option<(K, V)>) {
        loop {
            let (slf, result) = Self::next_entry(slf_).await;

            if let Some((is_pending, key, entry)) = result {
                match entry {
                    Entry::Value{value_ref, ..} => {
                        let res_val = if is_pending {
                            slf.value_log.get_pending(value_ref).await
                        } else {
                            slf.value_log.get(value_ref).await
                        };

                        return (slf, Some((key, res_val)));
                    },
                    Entry::Deletion{..} => {
                        // This is a deletion. Skip
                        slf_ = slf;
                    }
                }
            } else {
                return (slf, None);
            }
        }
    }
}
