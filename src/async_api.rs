use crate::tasks::{TaskType, TaskManager};
use crate::iterate::DbIterator;
use crate::logic::DbLogic;
use crate::{get_encoder, StartMode, KvTrait, Params, WriteBatch, Error, WriteOptions};

use std::sync::Arc;

use bincode::Options;

/// The main database structure
/// This struct can be accessed concurrently and you should
/// never instantiate it more than once for the same on-disk files
#[ derive(Debug) ]
pub struct Database<K: KvTrait, V: KvTrait> {
    inner: Arc<DbLogic<K, V>>,
    tasks: Arc<TaskManager>
}

impl<K: 'static+KvTrait, V: 'static+KvTrait> Database<K, V> {
    /// Create a new database instance with default parameters
    pub async fn new(mode: StartMode) -> Result<Self, Error> {
        let params = Params::default();
        Self::new_with_params(mode, params).await
    }

    /// Create a new database instance with specific parameters
    pub async fn new_with_params(mode: StartMode, params: Params) -> Result<Self, Error> {
        let inner = Arc::new( DbLogic::new(mode, params).await? );
        let tasks = Arc::new( TaskManager::new(inner.clone()).await );

        Ok( Self{ inner, tasks } )
    }

    /// Will deserialize V from the raw data (avoids an additional data copy)
    #[inline]
    pub async fn get(&self, key: &K)-> Result<Option<V>, Error> {
        let key_data = get_encoder().serialize(key)?;
        self.inner.get(&key_data).await
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent version(which could be the first one) as deleted
    #[inline]
    pub async fn delete(&self, key: &K) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();

        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, &OPTS).await
    }

    /// Delete an existing entry (with additional options)
    #[inline]
    pub async fn delete_opts(&self, key: &K, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_opts(batch, opts).await
    }

    /// Insert or update a single entry
    #[inline]
    pub async fn put(&self, key: &K, value: &V) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS).await
    }

    /// Insert or update a single entry (with additional options)
    #[inline]
    pub async fn put_opts(&self, key: &K, value: &V, opts: &WriteOptions) -> Result<(), Error> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts).await
    }

    /// Iterate over all entries in the database
    #[inline]
    pub async fn iter(&self) -> DbIterator<K, V> {
        self.inner.iter().await
    }

    /// Write a batch of updates to the database
    ///
    /// If you only want to write to a single key, use `Database::put` instead
    #[inline]
    pub async fn write(&self, write_batch: WriteBatch<K, V>) -> Result<(), Error> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS).await
    }

    /// Write a batch of updates to the database
    /// This version of write allows you to specfiy options such as "synchronous"
    pub async fn write_opts(&self, write_batch: WriteBatch<K, V>, opts: &WriteOptions) -> Result<(), Error> {
        let needs_compaction = self.inner.write_opts(write_batch, opts).await?;

        if needs_compaction {
            self.tasks.wake_up(&TaskType::Compaction).await;
        }

        Ok(())
    }

    /// Stop all background tasks gracefully
    pub async fn stop(&self) -> Result<(), Error> {
        self.tasks.stop_all().await
    }
}

impl<K: KvTrait, V: KvTrait> Drop for Database<K,V> {
    fn drop(&mut self) {
        self.tasks.terminate();
    }
}

