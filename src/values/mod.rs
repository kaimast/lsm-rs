use std::collections::HashMap;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::Mutex;

use zerocopy::{FromBytes, IntoBytes};

use crate::Error;

use lru::LruCache;

#[cfg(feature = "_async-io")]
use tokio_uring::fs::{OpenOptions, remove_file};

#[cfg(not(feature = "_async-io"))]
use std::fs::{OpenOptions, remove_file};
#[cfg(not(feature = "_async-io"))]
use std::io::{Read, Seek, SeekFrom, Write};

use crate::Params;
use crate::disk;
use crate::manifest::Manifest;

use cfg_if::cfg_if;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

const NUM_SHARDS: NonZeroUsize = NonZeroUsize::new(16).unwrap();

pub const GARBAGE_COLLECT_THRESHOLD: f64 = 0.2;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

#[cfg(test)]
mod tests;

mod batch;
pub use batch::ValueBatchBuilder;
use batch::{ValueBatch, ValueBatchHeader, ValueEntryHeader};

pub struct ValueLog {
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
    pub async fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let mut batch_caches = Vec::new();
        let max_value_files = NonZeroUsize::new(params.max_open_files / 2)
            .expect("Max open files needs to be greater than 2");

        let shard_size = NonZeroUsize::new(max_value_files.get() / NUM_SHARDS)
            .expect("Not enough open files to support the number of shards");

        for _ in 0..NUM_SHARDS.get() {
            let cache = Mutex::new(BatchShard::new(shard_size));
            batch_caches.push(cache);
        }

        Self {
            params,
            manifest,
            batch_caches,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn mark_value_deleted(&self, vid: ValueId) -> Result<(), Error> {
        let (batch_id, value_offset) = vid;
        let meta_path = self.get_meta_file_path(&batch_id);

        log::trace!("Marking value in batch #{batch_id} at offset {value_offset} as deleted");

        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(&meta_path).await?;

                let (res, buf) = file.read_exact_at(header_data, 0).await;
                res?;
                header_data = buf;
            } else {
                let mut file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(&meta_path)?;

                file.read_exact(&mut header_data)?;
            }
        }

        let header = ValueBatchHeader::ref_from_bytes(&header_data).unwrap();

        let mut offset_pos = None;
        let offset_len = size_of::<u32>();

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                // Skip deletion markers
                let pos = header_len + header.delete_markers_len();
                let buf = vec![0u8; header.offsets_len()];
                let (res, data) = file.read_exact_at(buf, pos as u64).await;
                res?;

                if header.is_folded() {
                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let (old_offset, _) = u32::ref_from_prefix(&data[2*upos*offset_len..]).unwrap();

                        if old_offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                } else {
                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let (offset, _) = u32::ref_from_prefix(&data[upos*offset_len..]).unwrap();
                        if offset == &value_offset {
                            offset_pos = Some(pos);
                            break;
                        }
                    }
                }

                let delete_marker_pos = (header_len as u64) + (offset_pos.expect("Not a valid offset") as u64);
                file.write_all_at(vec![1u8], delete_marker_pos).await.0?;
            } else {
                // Skip deletion markers
                file.seek(SeekFrom::Current(header.delete_markers_len() as i64))?;
                let mut data = vec![0u8; header.offsets_len()];
                file.read_exact(&mut data)?;

                if header.is_folded() {
                     for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let (old_offset, _) = u32::ref_from_prefix(&data[2*upos*offset_len..]).unwrap();

                        if old_offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                } else {
                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let (offset, _) = u32::ref_from_prefix(&data[upos*offset_len..]).unwrap();
                        if offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                }

                let offset_pos = offset_pos.unwrap_or_else(|| panic!("Cannot mark as deleted: {value_offset} is not a valid offset"));
                assert!(offset_pos < header.num_values);

                log::trace!("Marking entry #{offset_pos} at offset {value_offset} as deleted");
                file.seek(SeekFrom::Start((header_len as u64) + (offset_pos as u64)))?;
                file.write_all(&[1u8])?;
            }
        }

        // Metadata is (currently) not mmap, so evict outdated version from cache
        let shard_id = Self::batch_to_shard_id(batch_id);
        self.batch_caches[shard_id].lock().await.pop(&batch_id);

        // we might need to delete more than one batch
        let mut batch_id = batch_id;

        // FIXME make sure there aren't any race conditions here
        let most_recent = self.manifest.most_recent_value_batch_id().await;
        while batch_id <= most_recent {
            // This will re-read some of the file
            // it's somewhat inefficient but makes the code much more readable
            let deleted = self.cleanup_batch(batch_id).await?;

            if deleted {
                batch_id += 1;
            } else {
                break;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn cleanup_batch(&self, batch_id: ValueBatchId) -> Result<bool, Error> {
        let fpath = self.get_meta_file_path(&batch_id);

        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(&fpath).await?;

                let (res, buf) = file.read_exact_at(header_data, 0).await;
                res?;
                header_data = buf;
            } else {
                let mut file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(&fpath)?;

                file.read_exact(&mut header_data)?;
            }
        }

        let header = ValueBatchHeader::ref_from_bytes(&header_data).unwrap();

        let olen = std::mem::size_of::<u32>();
        let mut offsets = vec![0u32; header.num_values as usize];
        let mut delete_flags = vec![0u8; header.delete_markers_len()];

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let (res, buf) = file.read_exact_at(delete_flags, header_len as u64).await;
                res?;
                delete_flags = buf;
            } else {
                file.read_exact(&mut delete_flags)?;
            }
        }

        let mut buf = if header.is_folded() {
            vec![0u8; offsets.len() * 2 * olen]
        } else {
            vec![0u8; offsets.len() * olen]
        };

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let pos = header_len + header.delete_markers_len();
                let (res, data) = file.read_exact_at(buf, pos as u64).await;
                res?;
                buf = data;
            } else {
                file.read_exact(&mut buf)?;
           }
        }

        if header.is_folded() {
            for idx in 0..(header.num_values as usize) {
                offsets[idx] = *u32::ref_from_prefix(&buf[idx * 2 * olen..]).unwrap().0;
            }
        } else {
            for idx in 0..(header.num_values as usize) {
                offsets[idx] = *u32::ref_from_prefix(&buf[idx * olen..]).unwrap().0;
            }
        }

        let mut num_active: u32 = 0;
        // Remove padding...
        for flag in &delete_flags[..(header.num_values as usize)] {
            if *flag == 0u8 {
                num_active += 1;
            }
        }

        let active_ratio = (num_active as f64) / (header.num_values as f64);
        let vlog_offset = self.manifest.get_value_log_offset().await;

        if num_active == 0 && batch_id == vlog_offset + 1 {
            log::trace!("Deleting batch #{batch_id}");

            // Hold lock so nobody else messes with the file while we do this
            let shard_id = Self::batch_to_shard_id(batch_id);
            let mut cache = self.batch_caches[shard_id].lock().await;

            self.manifest.set_value_log_offset(vlog_offset + 1).await;

            cfg_if! {
                if #[ cfg(feature="_async-io") ] {
                    remove_file(&fpath).await?;
                } else {
                    remove_file(&fpath)?;
                }
            }

            cache.pop(&batch_id);

            Ok(true)
        } else if !header.is_folded() && active_ratio <= GARBAGE_COLLECT_THRESHOLD {
            self.fold_batch(batch_id, &offsets).await?;
            Ok(false)
        } else {
            Ok(false)
        }
    }

    /// Creates a more compact representation of an existing value batch
    async fn fold_batch(&self, batch_id: ValueBatchId, offsets: &[u32]) -> Result<(), Error> {
        log::debug!("Folding value batch #{batch_id}");
        let batch = self.get_batch(batch_id).await?;

        let mut value_data = vec![];
        let mut new_offsets = vec![];

        // Hold lock so nobody else messes with the file while we do this
        let shard_id = Self::batch_to_shard_id(batch_id);
        let mut cache = self.batch_caches[shard_id].lock().await;

        // Ensure it has not been folded before we grab the lock
        if batch.is_folded() {
            return Ok(());
        }

        // Fetch flags again to prevent any race conditions
        let delete_flags = batch.get_delete_flags();
        let num_active = batch.num_active_values();

        for (pos, flag) in delete_flags.iter().enumerate() {
            let old_offset: u32 = offsets[pos];
            let data_start = old_offset as usize;

            let (entry_header, _) =
                ValueEntryHeader::ref_from_prefix(&batch.get_value_data()[data_start..]).unwrap();

            let data_end =
                data_start + size_of::<ValueEntryHeader>() + (entry_header.length as usize);

            if *flag != 0u8 {
                continue;
            }

            let new_offset = value_data.len() as u32;

            new_offsets.push((old_offset, new_offset));
            value_data.extend_from_slice(&batch.get_value_data()[data_start..data_end]);
        }

        // re-write batch file
        // TODO make this an atomic file operation

        // write file header
        let header = ValueBatchHeader {
            folded: 1,
            num_values: num_active,
        };

        let mut fold_table = HashMap::new();
        assert!(header.num_values as usize == new_offsets.len());

        let header_data = header.as_bytes();
        let mut delete_markers = vec![0u8; header.delete_markers_len()];

        let nsize = std::mem::size_of::<u32>();
        let mut offsets = vec![0u8; header.offsets_len()];

        for (pos, (old_offset, new_offset)) in new_offsets.iter().enumerate() {
            let start = pos * 2 * nsize;
            offsets[start..start + nsize].copy_from_slice(old_offset.as_bytes());
            offsets[start + nsize..start + 2 * nsize].copy_from_slice(new_offset.as_bytes());
            fold_table.insert(*old_offset, *new_offset);
        }

        let mut metadata = header_data.to_vec();
        metadata.append(&mut delete_markers);
        metadata.append(&mut offsets);

        // Write metadata uncompressed so it can be updated easily
        disk::write_uncompressed(&self.get_meta_file_path(&batch_id), metadata.clone()).await?;
        disk::write(&self.get_data_file_path(&batch_id), &value_data).await?;

        // Replace existing data block with folded version
        cache.put(
            batch_id,
            Arc::new(ValueBatch::from_existing(metadata, value_data)),
        );

        Ok(())
    }

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_meta_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        self.params.db_path.join(format!("val{batch_id:08}.value"))
    }

    #[inline]
    fn get_data_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        self.params.db_path.join(format!("val{batch_id:08}.data"))
    }

    pub async fn make_batch(&self) -> ValueBatchBuilder<'_> {
        let identifier = self.manifest.next_value_batch_id().await;
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

            let metadata =
                disk::read_uncompressed(&self.get_meta_file_path(&identifier), 0).await?;
            let value_data = disk::read(&self.get_data_file_path(&identifier), 0).await?;

            let obj = Arc::new(ValueBatch::from_existing(metadata, value_data));
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
