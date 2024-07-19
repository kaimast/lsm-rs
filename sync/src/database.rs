use std::sync::Arc;

use tokio::runtime::Runtime as TokioRuntime;

use lsm::logic::DbLogic;
use lsm::sorted_table::{Key, Value};
use lsm::tasks::{TaskManager, TaskType};
use lsm::{EntryRef, Error, Params, StartMode, WriteBatch, WriteOptions};

use crate::iterate::DbIterator;

pub struct Database {
    inner: Arc<DbLogic>,
    tasks: Arc<TaskManager>,
    tokio_rt: Arc<TokioRuntime>,
}

impl Database {
    pub fn new(mode: StartMode) -> Result<Self, Error> {
        let params = Params::default();
        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Result<Self, Error> {
        let tokio_rt = Arc::new(TokioRuntime::new().expect("Failed to start tokio"));
        let (inner, tasks) = tokio_rt.block_on(async {
            let compaction_concurrency = params.compaction_concurrency;

            match DbLogic::new(mode, params).await {
                Ok(inner) => {
                    let inner = Arc::new(inner);
                    let tasks =
                        Arc::new(TaskManager::new(inner.clone(), compaction_concurrency).await);

                    Ok((inner, tasks))
                }
                Err(err) => Err(err),
            }
        })?;

        Ok(Self {
            inner,
            tasks,
            tokio_rt,
        })
    }

    /// Will deserialize V from the raw data (avoids an additional copy)
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Option<EntryRef>, Error> {
        let inner = &*self.inner;

        self.tokio_rt.block_on(async {
            let result = inner.get(key).await;

            match result {
                Ok((needs_compaction, data)) => {
                    if needs_compaction {
                        self.tasks.wake_up(&TaskType::LevelCompaction);
                    }

                    Ok(data)
                }
                Err(err) => Err(err),
            }
        })
    }

    /// Ensure all data is written to disk
    /// Only has an effect if there were previous writes with sync=false
    pub async fn synchronize(&self) -> Result<(), Error> {
        let inner = &*self.inner;

        self.tokio_rt
            .block_on(async move { inner.synchronize().await })
    }

    /// Store entry
    #[inline]
    pub fn put(&self, key: Key, value: Value) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS)
    }

    /// Store entry (with options)
    #[inline]
    pub fn put_opts(&self, key: Key, value: Value, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts)
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent (which could be the first one) as deleted
    pub fn delete(&self, key: Key) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();

        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, &OPTS)
    }

    /// Delete an existing entry (with additional options)
    pub fn delete_opts(&self, key: Key, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, opts)
    }

    /// Iterate over all entries in the database
    pub fn iter(&self) -> DbIterator {
        let tokio_rt = self.tokio_rt.clone();

        self.tokio_rt.block_on(async {
            let (mem_iters, table_iters, min_key, max_key) =
                self.inner.prepare_iter(None, None).await;

            DbIterator::new(
                mem_iters,
                table_iters,
                min_key,
                max_key,
                false,
                #[cfg(feature = "wisckey")]
                self.inner.get_value_log(),
                tokio_rt,
            )
        })
    }

    /// Like iter(), but reverse
    pub fn reverse_iter(&self) -> DbIterator {
        let tokio_rt = self.tokio_rt.clone();

        self.tokio_rt.block_on(async {
            let (mem_iters, table_iters, min_key, max_key) =
                self.inner.prepare_reverse_iter(None, None).await;

            DbIterator::new(
                mem_iters,
                table_iters,
                min_key,
                max_key,
                true,
                #[cfg(feature = "wisckey")]
                self.inner.get_value_log(),
                tokio_rt,
            )
        })
    }

    /// Like iter(), but will only include entries with keys in [min_key;max_key)
    pub fn range_iter(&self, min: &[u8], max: &[u8]) -> DbIterator {
        let tokio_rt = self.tokio_rt.clone();

        self.tokio_rt.block_on(async {
            let (mem_iters, table_iters, min_key, max_key) =
                self.inner.prepare_iter(Some(min), Some(max)).await;

            DbIterator::new(
                mem_iters,
                table_iters,
                min_key,
                max_key,
                false,
                #[cfg(feature = "wisckey")]
                self.inner.get_value_log(),
                tokio_rt,
            )
        })
    }

    /// Like range_iter(), but in reverse.
    /// It will only include entries with keys in (min_key;max_key]
    pub fn reverse_range_iter(&self, max_key: &[u8], min_key: &[u8]) -> DbIterator {
        let tokio_rt = self.tokio_rt.clone();

        self.tokio_rt.block_on(async {
            let (mem_iters, table_iters, min_key, max_key) = self
                .inner
                .prepare_reverse_iter(Some(max_key), Some(min_key))
                .await;

            DbIterator::new(
                mem_iters,
                table_iters,
                min_key,
                max_key,
                true,
                #[cfg(feature = "wisckey")]
                self.inner.get_value_log(),
                tokio_rt,
            )
        })
    }

    /// Write a batch of updates to the database
    ///
    /// If you only want to write to a single key, use `Database::put` instead
    pub fn write(&self, write_batch: WriteBatch) -> Result<(), Error> {
        self.write_opts(write_batch, &WriteOptions::default())
    }

    pub fn write_opts(&self, write_batch: WriteBatch, opts: &WriteOptions) -> Result<(), Error> {
        let inner = &*self.inner;

        self.tokio_rt.block_on(async move {
            let needs_compaction = inner.write_opts(write_batch, opts).await?;
            if needs_compaction {
                self.tasks.wake_up(&TaskType::MemtableCompaction);
            }

            Ok(())
        })
    }

    /// Stop all background tasks gracefully
    pub fn stop(&self) -> Result<(), Error> {
        let tasks = self.tasks.clone();

        self.tokio_rt
            .block_on(async move { tasks.stop_all().await })
    }
}

impl Drop for Database {
    /// This might abort some tasks is stop() has not been called
    /// crash consistency should prevent this from being a problem
    fn drop(&mut self) {
        self.tasks.terminate();
    }
}
