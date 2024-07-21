use std::collections::HashMap;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use zerocopy::{AsBytes, FromBytes};

use crate::Error;

use lru::LruCache;

#[cfg(feature = "async-io")]
use tokio_uring::fs::{remove_file, File, OpenOptions};

#[cfg(not(feature = "async-io"))]
use std::fs::{remove_file, File, OpenOptions};
#[cfg(not(feature = "async-io"))]
use std::io::{Read, Seek, SeekFrom, Write};

use crate::disk;
use crate::manifest::Manifest;
use crate::Params;

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
use batch::{ValueBatch, ValueBatchHeader};

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
        &self.batch.get_data()[self.offset..self.offset + self.length]
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
        let fpath = self.get_file_path(&batch_id);

        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

        let header = ValueBatchHeader::ref_from(&header_data).unwrap();
        let mut offset_pos = None;
        let offset_len = size_of::<u32>();

        cfg_if! {
            if #[cfg(feature="async-io")] {
                // Skip deletion markers
                let pos = header_len + (header.num_values as usize);

                if header.is_folded() {
                    let buf = vec![0u8; (header.num_values as usize)*2*size_of::<u32>()];

                    let (res, data) = file.read_exact_at(buf, pos as u64).await;
                    res?;

                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let old_offset = u32::ref_from_prefix(&data[2*upos*offset_len..]).unwrap();

                        if old_offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                } else {
                    let buf = vec![0u8; (header.num_values as usize)*size_of::<u32>()];

                    let (res, data) = file.read_exact_at(buf, pos as u64).await;
                    res?;

                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let offset = u32::ref_from_prefix(&data[upos*offset_len..]).unwrap();

                        if offset == &value_offset {
                            offset_pos = Some(pos);
                            break;
                        }
                    }
                }

                let offset_pos = offset_pos.expect("Not a valid offset");
                let (res, _buf) = file.write_all_at(vec![1u8], offset_pos as u64).await;
                res?;
            } else {
                // Skip deletion markers
                file.seek(SeekFrom::Current(header.num_values as i64))?;

                if header.is_folded() {
                    let mut data = vec![0u8; (header.num_values as usize)*2*offset_len];
                    file.read_exact(&mut data)?;

                     for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let old_offset = u32::ref_from_prefix(&data[2*upos*offset_len..]).unwrap();

                        if old_offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                } else {
                    let offset_len = size_of::<u32>();
                    let mut data = vec![0u8; (header.num_values as usize)*offset_len];
                    file.read_exact(&mut data)?;

                    for pos in 0..header.num_values {
                        let upos = pos as usize;
                        let offset = u32::ref_from_prefix(&data[upos*offset_len..]).unwrap();

                        if offset == &value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                }

                let offset_pos = offset_pos.unwrap_or_else(|| panic!("Cannot mark as deleted: {value_offset} is not a valid offset"));

                log::trace!("Marking entry #{offset_pos} at {value_offset} as deleted");
                file.seek(SeekFrom::Start((header_len as u64) + (offset_pos as u64)))?;
                file.write_all(&[1u8])?;
            }
        }

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
        let fpath = self.get_file_path(&batch_id);

        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

        let header = ValueBatchHeader::ref_from(&header_data).unwrap();

        let olen = std::mem::size_of::<u32>();
        let mut offsets = vec![0u32; header.num_values as usize];
        let mut delete_flags = vec![0u8; header.num_values as usize];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let (res, buf) = file.read_exact_at(delete_flags, header_len as u64).await;
                res?;
                delete_flags = buf;

                let pos = header_len + header.num_values as usize;

                if header.is_folded() {
                    let buf = vec![0u8; offsets.len() * 2 * olen];
                    let (res, buf) = file.read_exact_at(buf, pos as u64).await;
                    res?;

                    for idx in 0..(header.num_values as usize) {
                        offsets[idx] = *u32::ref_from_prefix(&buf[idx*2*olen..]).unwrap();
                    }
                } else {
                    let buf = vec![0u8; offsets.len() * olen];
                    for idx in 0..(header.num_values as usize) {
                        offsets[idx] = *u32::ref_from_prefix(&buf[idx*olen..]).unwrap();
                    }
                }
            } else {
                file.read_exact(&mut delete_flags)?;

                if header.is_folded() {
                     let mut data = [0u8; 2*size_of::<u32>()];

                     for pos in 0..header.num_values {
                        file.read_exact(&mut data)?;
                        let offset = u32::ref_from(&data[..olen]).unwrap();
                        offsets[pos as usize] = *offset;
                        // do we need the new offset?
                    }
                } else {
                    let mut data = [0u8; size_of::<u32>()];

                    for pos in 0..header.num_values {
                        file.read_exact(&mut data)?;
                        let offset = u32::ref_from(&data).unwrap();
                        offsets[pos as usize] = *offset;
                    }
                }
            }
        }

        let mut num_active: u32 = 0;
        for flag in delete_flags.iter() {
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
                if #[ cfg(feature="async-io") ] {
                    remove_file(&fpath).await?;
                } else {
                    remove_file(&fpath)?;
                }
            }

            cache.pop(&batch_id);

            Ok(true)
        } else if !header.is_folded() && active_ratio <= GARBAGE_COLLECT_THRESHOLD {
            log::debug!("Folding value batch #{batch_id}");

            let batch = self.get_batch(batch_id).await?;

            let mut batch_data = vec![];
            let mut new_offsets = vec![];

            // Hold lock so nobody else messes with the file while we do this
            let shard_id = Self::batch_to_shard_id(batch_id);
            let mut cache = self.batch_caches[shard_id].lock().await;

            for (pos, flag) in delete_flags.iter().enumerate() {
                let old_offset = offsets[pos];
                let len_len = size_of::<u32>();
                let data_start = old_offset as usize;
                let vlen = u32::ref_from_prefix(&batch.get_data()[data_start..]).unwrap();

                let data_end = data_start + len_len + (*vlen as usize);

                if *flag != 0u8 {
                    continue;
                }

                let new_offset = batch_data.len() as u32;

                new_offsets.push((old_offset, new_offset));
                batch_data.extend_from_slice(&batch.get_data()[data_start..data_end]);
            }

            // re-write batch file
            // TODO make this an atomic file operation

            // write file header
            let header = ValueBatchHeader {
                folded: 1u8,
                num_values: num_active,
            };

            let mut fold_table = HashMap::new();
            assert!(header.num_values as usize == new_offsets.len());

            let header_data = header.as_bytes();
            let delete_markers_len = header.num_values as usize;
            let delete_markers = vec![0u8; delete_markers_len];

            let nsize = std::mem::size_of::<u32>();
            let offsets_len = (header.num_values as usize) * 2 * nsize;
            let mut offsets = vec![0u8; offsets_len];

            for (pos, (old_offset, new_offset)) in new_offsets.iter().enumerate() {
                let start = pos * 2 * nsize;
                offsets[start..start + nsize].copy_from_slice(old_offset.as_bytes());
                offsets[start + nsize..start + 2 * nsize].copy_from_slice(new_offset.as_bytes());
                fold_table.insert(*old_offset, *new_offset);
            }

            cfg_if! {
                if #[cfg(feature="async-io")] {
                    let file = File::create(&fpath).await?;
                    file.write_all_at(header_data.to_vec(), 0).await.0?;
                    file.write_all_at(delete_markers, header_len as u64).await.0?;
                    file.write_all_at(offsets, (header_len+delete_markers_len) as u64).await.0?;
                } else {
                    let mut file = File::create(&fpath)?;
                    file.write_all(header_data)?;
                    file.write_all(&delete_markers)?;
                    file.write_all(&offsets)?;
                }
            }

            let offset = header_data.len() + delete_markers_len + offsets_len;

            //TODO use same fd
            disk::write(&fpath, &batch_data, offset as u64).await?;

            cache.put(
                batch_id,
                Arc::new(ValueBatch::from_existing(Some(fold_table), batch_data)),
            );

            Ok(false)
        } else {
            Ok(false)
        }
    }

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        let fname = format!("val{batch_id:08}.data");
        self.params.db_path.join(Path::new(&fname))
    }

    pub async fn make_batch(&self) -> ValueBatchBuilder<'_> {
        let identifier = self.manifest.next_value_batch_id().await;
        ValueBatchBuilder::new(identifier, self)
    }

    #[tracing::instrument(skip(self))]
    async fn get_batch(&self, identifier: ValueBatchId) -> Result<Arc<ValueBatch>, Error> {
        let shard_id = Self::batch_to_shard_id(identifier);
        let mut cache = self.batch_caches[shard_id].lock().await;
        let header_len = std::mem::size_of::<ValueBatchHeader>();

        if let Some(batch) = cache.get(&identifier) {
            Ok(batch.clone())
        } else {
            log::trace!("Loading value batch #{identifier} from disk");

            let fpath = self.get_file_path(&identifier);

            let mut header_data = vec![0u8; header_len];

            cfg_if! {
                if #[cfg(feature="async-io")] {
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

            let header = ValueBatchHeader::ref_from(&header_data).unwrap();

            // The point where the delete flags end and the fold table / offset list begins
            let df_offset = header_len + (header.num_values as usize);

            let (offset, fold_table) = if header.is_folded() {
                log::trace!("Loading fold table for batch #{identifier}");

                let mut fold_table = HashMap::new();
                let mut data = vec![0u8; size_of::<u32>() * (header.num_values as usize)];

                cfg_if! {
                    if #[cfg(feature="async-io")] {
                        let (res, buf) = file.read_exact_at(data, df_offset as u64).await;
                        res?;
                        data = buf;
                    } else {
                        file.seek(SeekFrom::Start(df_offset as u64))?;
                        file.read_exact(&mut data)?;
                    }
                }

                for pos in 0..header.num_values {
                    let offset = (pos as usize) * 2 * size_of::<u32>();

                    let old_offset = u32::ref_from_prefix(&data[offset..]).unwrap();
                    let new_offset =
                        u32::ref_from_prefix(&data[offset + size_of::<u32>()..]).unwrap();
                    fold_table.insert(*old_offset, *new_offset);
                }

                let offset = df_offset + data.len();
                (offset, Some(fold_table))
            } else {
                let offset = df_offset + (header.num_values as usize) * size_of::<u32>();
                (offset, None)
            };

            let data = disk::read(&fpath, offset as u64).await?;
            let batch = Arc::new(ValueBatch::from_existing(fold_table, data));

            cache.put(identifier, batch.clone());
            Ok(batch)
        }
    }

    pub async fn get_ref(&self, value_ref: ValueId) -> Result<ValueRef, Error> {
        log::trace!("Getting value at {value_ref:?}");

        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await?;

        Ok(ValueBatch::get_value(batch, offset))
    }

    #[allow(dead_code)]
    async fn is_batch_folded(&self, identifier: ValueBatchId) -> Result<bool, Error> {
        let fpath = self.get_file_path(&identifier);
        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

        Ok(ValueBatchHeader::ref_from(&header_data)
            .unwrap()
            .is_folded())
    }

    #[allow(dead_code)]
    async fn get_active_values_in_batch(&self, identifier: ValueBatchId) -> Result<u32, Error> {
        let fpath = self.get_file_path(&identifier);
        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

        let header = ValueBatchHeader::ref_from(&header_data).unwrap();
        let mut delete_flags = vec![0u8; header.num_values as usize];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let (res, buf) = file.read_exact_at(delete_flags, header_len as u64).await;
                res?;
                delete_flags = buf;
            } else {
                file.read_exact(&mut delete_flags)?;
            }
        }

        let mut num_active = 0;
        for f in delete_flags.into_iter() {
            if f == 0u8 {
                num_active += 1;
            }
        }

        Ok(num_active)
    }

    #[allow(dead_code)]
    async fn get_total_values_in_batch(&self, identifier: ValueBatchId) -> Result<u32, Error> {
        let fpath = self.get_file_path(&identifier);
        let header_len = std::mem::size_of::<ValueBatchHeader>();
        let mut header_data = vec![0u8; header_len];

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

        Ok(ValueBatchHeader::ref_from(&header_data).unwrap().num_values)
    }
}
