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

use crate::EntryList;
use crate::wal::{LogEntry, WriteAheadLog};

pub struct ValueLog {
    /// The value log uses the WAL to batch updates
    /// especially those to the freelist
    wal: Arc<WriteAheadLog>,

    /// The freelist keeps track of all used entires within
    /// the value log and helps to garbage collect and
    /// defragment
    freelist: ValueFreelist,

    /// Sharded storage of log batches
    batch_caches: Vec<Mutex<BatchShard>>,

    params: Arc<Params>,
    manifest: Arc<Manifest>,
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

    pub async fn new(
        wal: Arc<WriteAheadLog>,
        params: Arc<Params>,
        manifest: Arc<Manifest>,
    ) -> Self {
        let batch_caches = Self::init_caches(&params);
        let freelist = ValueFreelist::new(params.clone(), manifest.clone());

        Self {
            wal,
            freelist,
            params,
            manifest,
            batch_caches,
        }
    }

    pub async fn open(
        wal: Arc<WriteAheadLog>,
        params: Arc<Params>,
        manifest: Arc<Manifest>,
    ) -> Result<Self, Error> {
        let batch_caches = Self::init_caches(&params);
        let freelist = ValueFreelist::open(params.clone(), manifest.clone()).await?;

        Ok(Self {
            wal,
            freelist,
            params,
            manifest,
            batch_caches,
        })
    }

    /// Marks a value as unused and, potentially, removes old value batches
    /// On success, this might return a list of entries to reinsert in order to defragment the log
    #[tracing::instrument(skip(self))]
    pub async fn mark_value_deleted(&self, vid: ValueId) -> Result<EntryList, Error> {
        let (page_id, page_offset) = self.freelist.mark_value_as_deleted(vid).await?;
        self.wal
            .store(&[LogEntry::ValueDeletion(page_id, page_offset)])
            .await?;

        // If this is the oldest value batch, try to remove things
        let start = vid.0;
        let min_batch = self
            .manifest
            .get_minimum_value_batch()
            .await
            .max(MIN_VALUE_BATCH_ID);
        let mut defragment_pos = self.manifest.get_value_defragment_position().await;
        let most_recent = self.manifest.most_recent_value_batch_id().await;

        if min_batch == start {
            for batch_id in vid.0..=most_recent {
                if !self.try_to_remove(batch_id).await? {
                    // Don't defragment what has already been deleted
                    if batch_id > defragment_pos {
                        defragment_pos = batch_id;
                        self.manifest.set_value_defragment_position(batch_id).await;
                    }
                    break;
                }
            }
        }

        let mut reinsert = vec![];

        if defragment_pos == start {
            for batch_id in start..most_recent {
                if let Some(mut entries) = self.try_to_defragment(batch_id).await? {
                    reinsert.append(&mut entries);
                } else {
                    if batch_id > start {
                        self.manifest.set_value_defragment_position(batch_id).await;
                    } else {
                        assert!(reinsert.is_empty());
                    }

                    break;
                }
            }
        }

        Ok(reinsert)
    }

    /// Attempts to delete empty batches
    #[tracing::instrument(skip(self))]
    async fn try_to_remove(&self, batch_id: ValueBatchId) -> Result<bool, Error> {
        log::trace!("Checking if value batch #{batch_id} can be removed");

        let num_active = self.freelist.count_active_entries(batch_id).await;

        // Can only remove if no values in this batch are active
        if num_active > 0 {
            return Ok(false);
        }

        log::trace!("Deleting empty batch #{batch_id}");

        // Hold lock so nobody else messes with the file while we do this
        let shard_id = Self::batch_to_shard_id(batch_id);
        let mut cache = self.batch_caches[shard_id].lock().await;

        self.manifest.set_minimum_value_batch(batch_id + 1).await;

        let fpath = self.get_batch_file_path(&batch_id);
        disk::remove_file(&fpath)
            .await
            .map_err(|err| Error::from_io_error("Failed to remove value log batch", err))?;

        cache.pop(&batch_id);

        Ok(true)
    }

    /// Check if we should reinsert entries from this batch
    #[tracing::instrument(skip(self))]
    async fn try_to_defragment(&self, batch_id: ValueBatchId) -> Result<Option<EntryList>, Error> {
        log::trace!("Checking if value batch #{batch_id} should be defragmented");

        let batch = self.get_batch(batch_id).await?;
        let num_entries = batch.total_num_values() as usize;
        let num_active = self.freelist.count_active_entries(batch_id).await;
        let active_ratio = (num_active * 100) / (num_entries * 100);

        if active_ratio < 25 {
            log::trace!("Re-inserting sparse value batch #{batch_id}");
            let offsets = self.freelist.get_active_entries(batch_id).await;
            Ok(Some(batch.get_entries(&offsets)))
        } else {
            Ok(None)
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
