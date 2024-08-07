use crate::iterate::DbIterator;
use crate::logic::{DbLogic, EntryRef};
use crate::tasks::{TaskManager, TaskType};
use crate::{Error, Key, Params, StartMode, Value, WriteBatch, WriteOptions};

use std::sync::Arc;

/// The main database structure
/// This struct can be accessed concurrently and you should
/// never instantiate it more than once for the same on-disk files
pub struct Database {
    inner: Arc<DbLogic>,
    tasks: Arc<TaskManager>,
}

impl Database {
    /// Create a new database instance with default parameters
    pub async fn new(mode: StartMode) -> Result<Self, Error> {
        let params = Params::default();
        Self::new_with_params(mode, params).await
    }

    /// Create a new database instance with specific parameters
    pub async fn new_with_params(mode: StartMode, params: Params) -> Result<Self, Error> {
        let compaction_concurrency = params.compaction_concurrency;

        let inner = Arc::new(DbLogic::new(mode, params).await?);
        let tasks = Arc::new(TaskManager::new(inner.clone(), compaction_concurrency).await);

        Ok(Self { inner, tasks })
    }

    /// Will deserialize V from the raw data (avoids an additional data copy)
    #[tracing::instrument(skip(self, key))]
    pub async fn get(&self, key: &[u8]) -> Result<Option<EntryRef>, Error> {
        match self.inner.get(key).await {
            Ok((needs_compaction, data)) => {
                if needs_compaction {
                    self.tasks.wake_up(&TaskType::LevelCompaction);
                }

                Ok(data)
            }
            Err(err) => Err(err),
        }
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent version (which could be the first one) as deleted
    #[tracing::instrument(skip(self, key))]
    pub async fn delete(&self, key: Key) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, &WriteOptions::default()).await
    }

    /// Ensure all data is written to disk
    /// Only has an effect if there were previous writes with sync=false
    pub async fn synchronize(&self) -> Result<(), Error> {
        self.inner.synchronize().await
    }

    /// Delete an existing entry (with additional options)
    pub async fn delete_opts(&self, key: Key, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_opts(batch, opts).await
    }

    /// Insert or update a single entry
    pub async fn put(&self, key: Key, value: Value) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS).await
    }

    /// Insert or update a single entry (with additional options)
    #[tracing::instrument(skip(self))]
    pub async fn put_opts(&self, key: Key, value: Value, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts).await
    }

    /// Iterate over all entries in the database
    pub async fn iter(&self) -> DbIterator {
        let (mem_iters, table_iters, min_key, max_key) = self.inner.prepare_iter(None, None).await;

        DbIterator::new(
            mem_iters,
            table_iters,
            min_key,
            max_key,
            false,
            #[cfg(feature = "wisckey")]
            self.inner.get_value_log(),
        )
    }

    /// Like iter(), but will only include entries with keys in [min_key;max_key)
    pub async fn range_iter(&self, min_key: &[u8], max_key: &[u8]) -> DbIterator {
        let (mem_iters, table_iters, min_key, max_key) =
            self.inner.prepare_iter(Some(min_key), Some(max_key)).await;

        DbIterator::new(
            mem_iters,
            table_iters,
            min_key,
            max_key,
            false,
            #[cfg(feature = "wisckey")]
            self.inner.get_value_log(),
        )
    }

    /// Like range_iter(), but in reverse.
    /// It will only include entries with keys in (min_key;max_key]
    pub async fn reverse_range_iter(&self, max_key: &[u8], min_key: &[u8]) -> DbIterator {
        let (mem_iters, table_iters, min_key, max_key) = self
            .inner
            .prepare_reverse_iter(Some(max_key), Some(min_key))
            .await;

        DbIterator::new(
            mem_iters,
            table_iters,
            min_key.map(|k| k.to_vec()),
            max_key.map(|k| k.to_vec()),
            true,
            #[cfg(feature = "wisckey")]
            self.inner.get_value_log(),
        )
    }

    /// Write a batch of updates to the database
    ///
    /// If you only want to write to a single key, use `Database::put` instead
    pub async fn write(&self, write_batch: WriteBatch) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS).await
    }

    /// Write a batch of updates to the database
    /// This version of write allows you to specify options such as "synchronous"
    #[tracing::instrument(skip(self, write_batch, opts))]
    pub async fn write_opts(
        &self,
        write_batch: WriteBatch,
        opts: &WriteOptions,
    ) -> Result<(), Error> {
        let needs_compaction = self.inner.write_opts(write_batch, opts).await?;

        if needs_compaction {
            self.tasks.wake_up(&TaskType::MemtableCompaction);
        }

        Ok(())
    }

    /// Stop all background tasks gracefully
    pub async fn stop(&self) -> Result<(), Error> {
        self.inner.stop().await?;
        self.tasks.stop_all().await
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.tasks.terminate();
    }
}
