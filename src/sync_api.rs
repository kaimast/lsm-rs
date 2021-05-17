use crate::tasks::{TaskType, TaskManager};
use crate::iterate::DbIterator;
use crate::logic::DbLogic;
use crate::{get_encoder, StartMode, KV_Trait, Params, WriteBatch, Error, WriteOptions};

use std::sync::Arc;

use bincode::Options;

use tokio::runtime::Runtime as TokioRuntime;

pub struct Database<K: KV_Trait, V: KV_Trait> {
    inner: Arc<DbLogic<K, V>>,
    tasks: Arc<TaskManager>,
    tokio_rt: Arc<TokioRuntime>
}

impl<K: 'static+KV_Trait, V: 'static+KV_Trait> Database<K, V> {
    pub fn new(mode: StartMode) -> Result<Self, Error> {
        let params = Params::default();
        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Result<Self, Error> {
        let tokio_rt = Arc::new(TokioRuntime::new().expect("Failed to start tokio"));
        let (inner, tasks) = tokio_rt.block_on(async move {
            match DbLogic::new(mode, params).await {
                Ok(inner) => {
                    let inner = Arc::new(inner);
                    let tasks = Arc::new( TaskManager::new(inner.clone()).await );

                    Ok((inner, tasks))
                }
                Err(err) => {
                    Err(err)
                }
            }
        })?;

        Ok( Self{ inner, tasks, tokio_rt } )
    }

    /// Will deserialize V from the raw data (avoids an additional copy)
    #[inline]
    pub fn get(&self, key: &K)-> Result<Option<V>, Error> {
        let key_data = get_encoder().serialize(key)?;
        let inner = &*self.inner;

        self.tokio_rt.block_on(async move{
            inner.get(&key_data).await
        })
    }

    /// Store 
    #[inline]
    pub fn put(&self, key: &K, value: &V) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS)
    }

    #[inline]
    pub fn put_opts(&self, key: &K, value: &V, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts)
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent (which could be the first one) as deleted
    #[inline]
    pub fn delete(&self, key: &K) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();

        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, &OPTS)
    }

    #[inline]
    pub fn delete_opts(&self, key: &K, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, opts)
    }

    #[inline]
    pub fn iter(&self) -> DbIterator<K, V> {
        let inner = &*self.inner;
        let tokio_rt = self.tokio_rt.clone();

        self.tokio_rt.block_on(async move {
            inner.iter(tokio_rt).await
        })
    }

    /// Write a batch of updates to the database
    ///
    /// If you only want to write to a single key, use `Database::put` instead
    #[inline]
    pub fn write(&self, write_batch: WriteBatch<K, V>) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS)
    }

    pub fn write_opts(&self, write_batch: WriteBatch<K, V>, opts: &WriteOptions) -> Result<(), Error> {
        let inner = &*self.inner;

        self.tokio_rt.block_on(async move {
            let needs_compaction = inner.write_opts(write_batch, opts).await?;
            if needs_compaction {
                self.tasks.wake_up(&TaskType::Compaction).await;
            }

            Ok(())
        })
    }
}

impl<K: KV_Trait, V: KV_Trait> Drop for Database<K,V> {
    fn drop(&mut self) {
        self.tasks.stop_all();
        self.inner.stop();
    }
}

