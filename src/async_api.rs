use crate::tasks::TaskManager;
use crate::iterate::DbIterator;
use crate::logic::DbLogic;
use crate::{get_encoder, StartMode, KV_Trait, Params, WriteBatch, WriteError, WriteOptions};

use std::sync::Arc;

use bincode::Options;

/// The main database structure
/// This struct can be accessed concurrently and you should
/// never instantiate it more than once for the same on-disk files
pub struct Database<K: KV_Trait, V: KV_Trait> {
    inner: Arc<DbLogic<K, V>>,
    tasks: Arc<TaskManager<K, V>>
}

impl<K: 'static+KV_Trait, V: 'static+KV_Trait> Database<K, V> {
    /// Create a new database instance with default parameters
    pub async fn new(mode: StartMode) -> Self {
        let params = Params::default();
        Self::new_with_params(mode, params).await
    }

    /// Create a new database instance with specific parameters
    pub async fn new_with_params(mode: StartMode, params: Params) -> Self {
        let inner = Arc::new( DbLogic::new(mode, params).await );

        let tasks = Arc::new( TaskManager::new(inner.clone()) );

        let tasks_cpy = tasks.clone();

        tokio::spawn(async move {
            TaskManager::work_loop(tasks_cpy).await;
        });

        Self{ inner, tasks }
    }

    /// Will deserialize V from the raw data (avoids an additional copy)
    #[inline]
    pub async fn get(&self, key: &K)-> Option<V> {
        let key_data = get_encoder().serialize(key).unwrap();
        self.inner.get(&key_data).await
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent (which could be the first one) as deleted
    #[inline]
    pub async fn delete(self, key: &K) {
        const OPTS: WriteOptions = WriteOptions::new();

        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.inner.write_opts(batch, &OPTS).await.unwrap();
    }

    #[inline]
    pub async fn delete_opts(self, key: &K, opts: &WriteOptions) {
        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.inner.write_opts(batch, opts).await.unwrap();
    }

    /// Store
    #[inline]
    pub async fn put(&self, key: &K, value: &V) -> Result<(), WriteError> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS).await
    }

    #[inline]
    pub async fn put_opts(&self, key: &K, value: &V, opts: &WriteOptions) -> Result<(), WriteError> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts).await
    }

    #[inline]
    pub async fn iter(&self) -> DbIterator<K, V> {
        self.inner.iter().await
    }

    /// Write a batch of updates to the database
    ///
    /// If you only want to write to a single key, use `Database::put` instead
    #[inline]
    pub async fn write(&self, write_batch: WriteBatch<K, V>) -> Result<(), WriteError> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS).await
    }

    /// Write a batch of updates to the database
    /// This version of write allows you to specfiy options such as "synchronous"
    pub async fn write_opts(&self, write_batch: WriteBatch<K, V>, opts: &WriteOptions) -> Result<(), WriteError> {
        let result = self.inner.write_opts(write_batch, opts).await;

        match result {
            Ok(needs_compaction) => {
                if needs_compaction {
                    self.tasks.wake_up().await;
                }

                Ok(())
            },
            Err(e) => Err(e)
        }
    }
}

impl<K: KV_Trait, V: KV_Trait> Drop for Database<K,V> {
    fn drop(&mut self) {
        self.inner.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;

    const SM: StartMode = StartMode::CreateOrOverride;

    fn test_init() {
         let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn get_put() {
        test_init();
        let ds = Database::<String, String>::new(SM).await;

        let key1 = String::from("Foo");
        let key2 = String::from("Foz");
        let value = String::from("Bar");
        let value2 = String::from("Baz");

        assert_eq!(ds.get(&key1).await, None);
        assert_eq!(ds.get(&key2).await, None);

        ds.put(&key1, &value).await.unwrap();

        assert_eq!(ds.get(&key1).await, Some(value.clone()));
        assert_eq!(ds.get(&key2).await, None);

        ds.put(&key1, &value2).await.unwrap();
        assert_eq!(ds.get(&key1).await, Some(value2.clone()));
    }

    #[tokio::test]
    async fn iterate() {
        const COUNT: u64 = 25_000;

        test_init();
        let ds = Database::<u64, String>::new(SM).await;

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        let mut pos = 0;
        let mut iter = ds.iter().await;

        while let Some((key, val)) = iter.next().await {
            assert_eq!(pos as u64, key);
            assert_eq!(format!("some_string_{}", pos), val);

            pos += 1;
        }

        assert_eq!(pos, COUNT);
    }

    #[tokio::test]
    async fn get_put_many() {
        const COUNT: u64 = 100_000;

        test_init();
        let ds = Database::new(SM).await;

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos).await, Some(format!("some_string_{}", pos)));
        }
    }

    #[tokio::test]
    async fn override_some() {
        const COUNT: u64 = 10_000;

        test_init();
        let ds = Database::new(SM).await;

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_other_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos).await, Some(format!("some_other_string_{}", pos)));
        }
    }


    #[tokio::test]
    async fn override_many() {
        const NCOUNT: u64 = 200_000;
        const COUNT: u64 = 50_000;

        test_init();
        let ds = Database::new(SM).await;

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..NCOUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_other_string_{}", pos);
            ds.put_opts(&key, &value, &options).await.unwrap();
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos).await, Some(format!("some_other_string_{}", pos)));
        }

        for pos in COUNT..NCOUNT {
            assert_eq!(ds.get(&pos).await, Some(format!("some_string_{}", pos)));
        }
    }

    #[tokio::test]
    async fn batched_write() {
        const COUNT: u64 = 1000;

        test_init();
        let ds = Database::new(SM).await;
        let mut batch = WriteBatch::new();

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            batch.put(&key, &pos);
        }

        ds.write(batch).await.unwrap();

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            assert_eq!(ds.get(&key).await, Some(pos));
        }
    }
}
