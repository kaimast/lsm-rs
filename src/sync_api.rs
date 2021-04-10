use crate::tasks::TaskManager;
use crate::iterate::DbIterator;
use crate::logic::DbLogic;
use crate::{get_encoder, StartMode, KV_Trait, Params, WriteBatch, WriteError, WriteOptions};

use std::sync::Arc;

use bincode::Options;

use tokio::runtime::Runtime as TokioRuntime;

pub struct Database<K: KV_Trait, V: KV_Trait> {
    inner: Arc<DbLogic<K, V>>,
    tasks: Arc<TaskManager<K, V>>,
    tokio_rt: Arc<TokioRuntime>
}

impl<K: 'static+KV_Trait, V: 'static+KV_Trait> Database<K, V> {
    pub fn new(mode: StartMode) -> Self {
        let params = Params::default();
        Self::new_with_params(mode, params)
    }

    pub fn new_with_params(mode: StartMode, params: Params) -> Self {
        let tokio_rt = Arc::new(TokioRuntime::new().expect("Failed to start tokio"));
        let inner = tokio_rt.block_on(async move {
            Arc::new( DbLogic::new(mode, params).await )
        });

        let tasks = Arc::new( TaskManager::new(inner.clone()) );

        {
            let tasks = tasks.clone();
            tokio_rt.spawn(async move {
                TaskManager::work_loop(tasks).await;
            });
        }

        #[ cfg(feature="wisckey") ]
        {
            let inner = inner.clone();
            tokio_rt.spawn(async move {
                inner.garbage_collect().await;
            });
        }

        Self{ inner, tasks, tokio_rt }
    }

    /// Will deserialize V from the raw data (avoids an additional copy)
    #[inline]
    pub fn get(&self, key: &K)-> Option<V> {
        let key_data = get_encoder().serialize(key).unwrap();
        let inner = &*self.inner;

        self.tokio_rt.block_on(async move{
            inner.get(&key_data).await
        })
    }

    /// Store 
    #[inline]
    pub fn put(&self, key: &K, value: &V) -> Result<(), WriteError> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.put_opts(key, value, &OPTS)
    }

    #[inline]
    pub fn put_opts(&self, key: &K, value: &V, opts: &WriteOptions) -> Result<(), WriteError> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_opts(batch, opts)
    }

    /// Delete an existing entry
    /// For efficiency, the datastore does not check whether the key actually existed
    /// Instead, it will just mark the most recent (which could be the first one) as deleted
    #[inline]
    pub fn delete(&self, key: &K) {
        const OPTS: WriteOptions = WriteOptions::new();

        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, &OPTS).unwrap();
    }

    #[inline]
    pub fn delete_opts(&self, key: &K, opts: &WriteOptions) {
        let mut batch = WriteBatch::new();
        batch.delete(key);

        self.write_opts(batch, opts).unwrap();
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
    pub fn write(&self, write_batch: WriteBatch<K, V>) -> Result<(), WriteError> {
        const OPTS: WriteOptions = WriteOptions::new();
        self.write_opts(write_batch, &OPTS)
    }

    pub fn write_opts(&self, write_batch: WriteBatch<K, V>, opts: &WriteOptions) -> Result<(), WriteError> {
        let inner = &*self.inner;

        self.tokio_rt.block_on(async move {
            let result = inner.write_opts(write_batch, opts).await;

            match result {
                Ok(needs_compaction) => {
                    if needs_compaction {
                        self.tasks.wake_up().await;
                    }

                    Ok(())
                },
                Err(e) => Err(e)
            }
        })
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

    const SM: StartMode = StartMode::CreateOrOverride;

    fn test_init() {
         let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn get_put() {
        test_init();
        let ds = Database::<String, String>::new(SM);

        let key1 = String::from("Foo");
        let key2 = String::from("Foz");
        let value = String::from("Bar");
        let value2 = String::from("Baz");

        assert_eq!(ds.get(&key1), None);
        assert_eq!(ds.get(&key2), None);

        ds.put(&key1, &value).unwrap();

        assert_eq!(ds.get(&key1), Some(value.clone()));
        assert_eq!(ds.get(&key2), None);

        ds.put(&key1, &value2).unwrap();
        assert_eq!(ds.get(&key1), Some(value2.clone()));
    }

    #[test]
    fn iterate() {
        const COUNT: u64 = 5_000;

        test_init();
        let ds = Database::<u64, String>::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).unwrap();
        }

        let mut count = 0;

        for (pos, (key, val)) in ds.iter().enumerate() {
            assert_eq!(pos as u64, key);
            assert_eq!(val, format!("some_string_{}", pos));

            count += 1;
        }

        assert_eq!(count, COUNT);
    }

    #[test]
    fn get_put_many() {
        const COUNT: u64 = 100_000;

        test_init();
        let ds = Database::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).unwrap();
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos), Some(format!("some_string_{}", pos)));
        }
    }

    #[test]
    fn get_put_delete_many() {
        const COUNT: u64 = 10_000;

        test_init();
        let ds = Database::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).unwrap();
        }

        for pos in 0..COUNT {
            let key = pos;
            ds.delete(&key);
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos), None);
        }
    }

    #[test]
    fn override_many() {
        const COUNT: u64 = 100_000;

        test_init();
        let ds = Database::new(SM);

        // Write without fsync to speed up tests
        let mut options = WriteOptions::default();
        options.sync = false;

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_string_{}", pos);
            ds.put_opts(&key, &value, &options).unwrap();
        }

        for pos in 0..COUNT {
            let key = pos;
            let value = format!("some_other_string_{}", pos);
            ds.put_opts(&key, &value, &options).unwrap();
        }

        for pos in 0..COUNT {
            assert_eq!(ds.get(&pos), Some(format!("some_other_string_{}", pos)));
        }
    }

    #[test]
    fn batched_write() {
        const COUNT: u64 = 1000;

        test_init();
        let ds = Database::new(SM);
        let mut batch = WriteBatch::new();

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            batch.put(&key, &pos);
        }

        ds.write(batch).unwrap();

        for pos in 0..COUNT {
            let key = format!("key{}", pos);
            assert_eq!(ds.get(&key), Some(pos));
        }
    }
}
