use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::Error;

use lru::LruCache;

use crate::Params;
use crate::disk;
use crate::manifest::Manifest;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub const MIN_VALUE_BATCH_ID: ValueBatchId = 1;

pub type ValueId = (ValueBatchId, ValueOffset);

const NUM_SHARDS: NonZeroUsize = NonZeroUsize::new(16).unwrap();

pub const GARBAGE_COLLECT_THRESHOLD: f64 = 0.2;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

#[cfg(test)]
mod tests;

mod freelist;
pub use freelist::{FreelistPageId, MIN_FREELIST_PAGE_ID, ValueFreelist};

mod batch;
use batch::ValueBatch;
pub use batch::ValueBatchBuilder;

pub struct ValueLog {
    freelist: ValueFreelist,
    params: Arc<Params>,
    manifest: Arc<Manifest>,
    batch_caches: Vec<Mutex<BatchShard>>,
}

pub struct ValueRef {
    batch: Arc<ValueBatch>,
    offset: usize,
    length: usize,
}

impl ValueRef {
    pub fn get_value(&self) -> &[u8] {
        &self.batch.get_value_data()[self.offset..self.offset + self.length]
    }
}

impl ValueLog {
    fn init_caches(params: &Params) -> Vec<Mutex<BatchShard>> {
        let max_value_files = NonZeroUsize::new(params.max_open_files / 2)
            .expect("Max open files needs to be greater than 2");

        let shard_size = NonZeroUsize::new(max_value_files.get() / NUM_SHARDS)
            .expect("Not enough open files to support the number of shards");

        (0..NUM_SHARDS.get())
            .map(|_| Mutex::new(BatchShard::new(shard_size)))
            .collect()
    }

    pub async fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let batch_caches = Self::init_caches(&params);
        let freelist = ValueFreelist::new(params.clone(), manifest.clone());

        Self {
            freelist,
            params,
            manifest,
            batch_caches,
        }
    }

    pub async fn open(params: Arc<Params>, manifest: Arc<Manifest>) -> Result<Self, Error> {
        let batch_caches = Self::init_caches(&params);
        let freelist = ValueFreelist::open(params.clone(), manifest.clone()).await?;

        Ok(Self {
            freelist,
            params,
            manifest,
            batch_caches,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn mark_value_deleted(&self, vid: ValueId) -> Result<(), Error> {
        self.freelist.mark_value_as_deleted(vid).await?;

        // FIXME make sure there aren't any race conditions here
        let most_recent = self.manifest.most_recent_value_batch_id().await;
        for batch_id in vid.0..=most_recent {
            // This will re-read some of the file
            // it's somewhat inefficient but makes the code much more readable
            let deleted = self.cleanup_batch(batch_id).await?;

            if !deleted {
                break;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn cleanup_batch(&self, batch_id: ValueBatchId) -> Result<bool, Error> {
        let num_active = self.freelist.get_active_entries(batch_id).await;
        let vlog_min = self.manifest.get_minimum_value_batch().await;

        if num_active == 0 && batch_id == vlog_min + 1 {
            log::trace!("Deleting batch #{batch_id}");

            // Hold lock so nobody else messes with the file while we do this
            let shard_id = Self::batch_to_shard_id(batch_id);
            let mut cache = self.batch_caches[shard_id].lock().await;

            self.manifest.set_minimum_value_batch(vlog_min + 1).await;

            let fpath = self.get_batch_file_path(&batch_id);
            disk::remove_file(&fpath)
                .await
                .map_err(|err| Error::from_io_error("Failed to remove value log batch", err))?;

            cache.pop(&batch_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_batch_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        self.params.db_path.join(format!("val{batch_id:08}.data"))
    }

    pub async fn make_batch(&self) -> ValueBatchBuilder<'_> {
        let identifier = self.manifest.generate_next_value_batch_id().await;
        ValueBatchBuilder::new(identifier, self)
    }

    #[tracing::instrument(skip(self))]
    async fn get_batch(&self, identifier: ValueBatchId) -> Result<Arc<ValueBatch>, Error> {
        let shard_id = Self::batch_to_shard_id(identifier);
        let mut cache = self.batch_caches[shard_id].lock().await;

        if let Some(batch) = cache.get(&identifier) {
            Ok(batch.clone())
        } else {
            log::trace!("Loading value batch #{identifier} from disk");

            let data = disk::read(&self.get_batch_file_path(&identifier), 0)
                .await
                .map_err(|err| Error::from_io_error("Failed to read value log batch", err))?;

            let obj = Arc::new(ValueBatch::from_existing(data));
            cache.put(identifier, obj.clone());

            Ok(obj)
        }
    }

    /// Return the reference to a value
    pub async fn get_ref(&self, value_ref: ValueId) -> Result<ValueRef, Error> {
        log::trace!("Getting value at {value_ref:?}");

        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await?;

        Ok(ValueBatch::get_ref(batch, offset))
    }
}
