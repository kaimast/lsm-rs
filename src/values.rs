use std::collections::HashMap;
use std::convert::TryInto;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::sorted_table::Value;
use crate::Error;

use bincode::Options;

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

#[derive(Debug)]
pub struct ValueLog {
    params: Arc<Params>,
    manifest: Arc<Manifest>,
    batch_caches: Vec<Mutex<BatchShard>>,
}

#[derive(Debug)]
struct ValueBatch {
    fold_table: Option<HashMap<ValueOffset, ValueOffset>>,
    data: Vec<u8>,
}

pub struct ValueBatchBuilder<'a> {
    vlog: &'a ValueLog,
    identifier: ValueBatchId,
    data: Vec<u8>,
    offsets: Vec<u8>,
}

impl<'a> ValueBatchBuilder<'a> {
    pub async fn finish(self) -> Result<ValueBatchId, Error> {
        let fpath = self.vlog.get_file_path(&self.identifier);
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;

        // The first byte is the fold flag
        let prefix_len = 1 + std::mem::size_of::<u32>() + (num_values as usize);
        let mut prefix = vec![0u8; prefix_len];
        prefix[1..5].copy_from_slice(num_values.to_le_bytes().as_slice());

        // write file header
        cfg_if! {
            if #[cfg(feature="async-io")] {
                let file = File::create(&fpath).await?;
                let (res, _buf) = file.write_all_at(prefix, 0).await;
                res?;
                let (res, _buf) = file.write_all_at(self.offsets, prefix_len as u64).await;
                res?;
            } else {
                let mut file = File::create(&fpath)?;
                file.write_all(&prefix)?;
                file.write_all(self.offsets[..].try_into().unwrap())?;
            }
        }

        let offset = (size_of::<u8>() as u64)
            + (size_of::<u32>() as u64)
            + ((size_of::<u32>() + size_of::<u8>()) as u64) * (num_values as u64);

        //TODO use same fd
        disk::write(&fpath, &self.data, offset).await?;

        let batch = ValueBatch {
            fold_table: None,
            data: self.data,
        };

        // Store in the cache so we don't have to load immediately
        {
            let shard_id = ValueLog::batch_to_shard_id(self.identifier);
            let mut shard = self.vlog.batch_caches[shard_id].lock().await;
            shard.put(self.identifier, Arc::new(batch));
        }

        log::trace!("Created value batch #{}", self.identifier);
        Ok(self.identifier)
    }

    pub async fn add_value(&mut self, mut val: Value) -> ValueId {
        let val_len = (val.len() as u32).to_le_bytes();

        let offset = self.data.len() as u32;
        let mut offset_data = offset.to_le_bytes().to_vec();
        self.offsets.append(&mut offset_data);

        self.data.extend_from_slice(&val_len);
        self.data.append(&mut val);

        (self.identifier, offset)
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

        const HEADER_LEN: u64 = (size_of::<u8>() + size_of::<u32>()) as u64;
        let mut header_data = vec![0u8; HEADER_LEN as usize];

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

        let is_folded = header_data[0] != 0u8;
        let num_values = u32::from_le_bytes(header_data[size_of::<u8>()..].try_into().unwrap());

        let mut offset_pos = None;

        cfg_if! {
            if #[cfg(feature="async-io")] {
                // Skip delete and fold marker
                let pos = 1 + num_values;

                if is_folded {
                    let len = (num_values as usize)*2*size_of::<u32>();
                    let buf = vec![0u8; len];

                    let (res, offset_positions) = file.read_exact_at(buf, pos as u64).await;
                    res?;

                    for idx in 0..num_values {
                        let start = (idx as usize)*2*size_of::<u32>();
                        let offset = u32::from_le_bytes(offset_positions[start..size_of::<u32>()].try_into().unwrap());

                        if offset == value_offset {
                            offset_pos = Some(pos);
                            break;
                        }
                    }
                } else {
                    let len = (num_values as usize)*size_of::<u32>();
                    let buf = vec![0u8; len];

                    let (res, offset_positions) = file.read_exact_at(buf, pos as u64).await;
                    res?;

                    for idx in 0..num_values {
                        let start = (idx as usize)*size_of::<u32>();
                        let offset = u32::from_le_bytes(offset_positions[start..size_of::<u32>()].try_into().unwrap());

                        if offset == value_offset {
                            offset_pos = Some(pos);
                            break;
                        }
                    }
                }

                let offset_pos = offset_pos.expect("Not a valid offset");
                let (res, _buf) = file.write_all_at(vec![1u8], offset_pos as u64).await;
                res?;
            } else {
                file.seek(SeekFrom::Current(num_values as i64))?;

                if is_folded {
                     let mut data = [0u8; 2*size_of::<u32>()];

                     for pos in 0..num_values {
                        file.read_exact(&mut data)?;
                        let old_offset = u32::from_le_bytes(data[..size_of::<u32>()].try_into().unwrap());

                        if old_offset == value_offset {
                            offset_pos = Some(pos);
                        }

                        // do we need the new offset?
                    }
                } else {
                    let mut data = [0u8; size_of::<u32>()];

                    for pos in 0..num_values {
                        file.read_exact(&mut data)?;
                        let offset = u32::from_le_bytes(data);

                        if offset == value_offset {
                            offset_pos = Some(pos);
                        }
                    }
                }

                let offset_pos = offset_pos.expect("Not a valid offset");
                file.seek(SeekFrom::Start(HEADER_LEN + (offset_pos as u64)))?;
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

        const HEADER_LEN: u64 = (size_of::<u8>() + size_of::<u32>()) as u64;
        let mut header_data = vec![0u8; HEADER_LEN as usize];

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

        let is_folded = header_data[0] != 0u8;
        let num_values = u32::from_le_bytes(header_data[size_of::<u8>()..].try_into().unwrap());
        let mut offsets = vec![0u32; num_values as usize];

        let mut delete_flags = vec![0u8; num_values as usize];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let (res, buf) = file.read_exact_at(delete_flags, HEADER_LEN).await;
                res?;
                delete_flags = buf;

                let pos = HEADER_LEN + num_values as u64;

                if is_folded {
                    let buf = vec![0u8; offsets.len() * 2];
                    let (res, buf) = file.read_exact_at(buf, pos).await;
                    res?;
                    for idx in 0..(num_values as usize) {
                        let olen = std::mem::size_of::<u32>();
                        let slice = buf[idx*2*olen..(idx+1)*olen].try_into().unwrap();
                        offsets[idx] = u32::from_le_bytes(slice);
                    }
                } else {
                    let buf = vec![0u8; offsets.len()];
                    for idx in 0..(num_values as usize) {
                        let olen = std::mem::size_of::<u32>();
                        let slice = buf[idx*olen..(idx+1)*olen].try_into().unwrap();
                        offsets[idx] = u32::from_le_bytes(slice);
                    }
                }
            } else {
                file.read_exact(&mut delete_flags)?;

                if is_folded {
                     let mut data = [0u8; 2*size_of::<u32>()];

                     for pos in 0..num_values {
                        file.read_exact(&mut data)?;
                        let offset = u32::from_le_bytes(data[..size_of::<u32>()].try_into().unwrap());
                        offsets[pos as usize] = offset;
                        // do we need the new offset?
                    }
                } else {
                    let mut data = [0u8; size_of::<u32>()];

                    for pos in 0..num_values {
                        file.read_exact(&mut data)?;
                        let offset = u32::from_le_bytes(data);
                        offsets[pos as usize] = offset;
                    }
                }
            }
        }

        let mut num_active: u32 = 0;
        for f in delete_flags.iter() {
            if *f == 0u8 {
                num_active += 1;
            }
        }

        let active_ratio = (num_active as f64) / (num_values as f64);
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
        } else if !is_folded && active_ratio <= GARBAGE_COLLECT_THRESHOLD {
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
                let vlen_data = batch.data[data_start..data_start + len_len]
                    .try_into()
                    .unwrap();
                let vlen = u32::from_le_bytes(vlen_data);

                let data_end = data_start + len_len + (vlen as usize);

                if *flag != 0u8 {
                    continue;
                }

                let new_offset = batch_data.len() as u32;

                new_offsets.push((old_offset, new_offset));
                batch_data.extend_from_slice(&batch.data[data_start..data_end]);
            }

            // re-write batch file
            // TODO make this an atomic file operation

            // write file header
            let fold_flag = 1u8;
            let num_values = num_active;

            let mut fold_table = HashMap::new();
            assert!(num_values as usize == new_offsets.len());

            let fold_flag_len = std::mem::size_of::<u8>();
            let num_vals_len = std::mem::size_of::<u32>();

            let prefix_len = fold_flag_len + num_vals_len + (num_values as usize);
            let mut prefix = vec![0u8; prefix_len];
            prefix[0] = fold_flag;
            prefix[fold_flag_len..fold_flag_len + num_vals_len]
                .copy_from_slice(&num_values.to_le_bytes());

            let nsize = std::mem::size_of::<u32>();
            let mut offsets = vec![0u8; (num_values as usize) * 2 * nsize];

            for (pos, (old_offset, new_offset)) in new_offsets.iter().enumerate() {
                let start = pos * 2 * nsize;
                offsets[start..start + nsize].copy_from_slice(&old_offset.to_le_bytes());
                offsets[start + nsize..start + 2 * nsize]
                    .copy_from_slice(&new_offset.to_le_bytes());
                fold_table.insert(*old_offset, *new_offset);
            }

            cfg_if! {
                if #[cfg(feature="async-io")] {
                    let file = File::create(&fpath).await?;
                    let (res, _buf) = file.write_all_at(prefix, 0).await;
                    res?;

                    let (res, _buf) = file.write_all_at(offsets, prefix_len as u64).await;
                    res?;
                } else {
                    let mut file = File::create(&fpath)?;
                    file.write_all(&prefix)?;
                    file.write_all(&offsets)?;
                    for (old_offset, new_offset) in new_offsets.into_iter() {
                        file.write_all(&old_offset.to_le_bytes())?;
                        file.write_all(&new_offset.to_le_bytes())?;
                    }
                }
            }

            let offset = (size_of::<u8>() as u64)
                + (size_of::<u32>() as u64)
                + ((2 * size_of::<u32>() + size_of::<u8>()) as u64) * (num_values as u64);

            //TODO use same fd
            disk::write(&fpath, &batch_data, offset).await?;

            cache.put(
                batch_id,
                Arc::new(ValueBatch {
                    fold_table: Some(fold_table),
                    data: batch_data,
                }),
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
        ValueBatchBuilder {
            identifier,
            vlog: self,
            data: vec![],
            offsets: vec![],
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_batch(&self, identifier: ValueBatchId) -> Result<Arc<ValueBatch>, Error> {
        let shard_id = Self::batch_to_shard_id(identifier);
        let mut cache = self.batch_caches[shard_id].lock().await;
        const HEADER_LEN: u64 = (size_of::<bool>() + size_of::<u32>()) as u64;

        if let Some(batch) = cache.get(&identifier) {
            Ok(batch.clone())
        } else {
            log::trace!("Loading value batch #{identifier} from disk");

            let fpath = self.get_file_path(&identifier);
            let mut header_data = vec![0u8; HEADER_LEN as usize];

            cfg_if! {
                if #[cfg(feature="async-io")] {
                    let file = File::open(&fpath).await?;
                    let (res, buf) = file.read_exact_at(header_data, 0).await;
                    res?;
                    header_data = buf;
                } else {
                    let mut file = File::open(&fpath)?;
                    file.read_exact(&mut header_data)?;
                }
            }

            let num_values = u32::from_le_bytes(header_data[size_of::<u8>()..].try_into().unwrap());
            let is_folded = header_data[0] != 0u8;

            // The point where the delete flags end and the fold table / offset list begins
            let df_offset = HEADER_LEN + (num_values as u64);

            let (offset, fold_table) = if is_folded {
                log::trace!("Loading fold table for batch #{identifier}");

                let mut fold_table = HashMap::new();
                let mut data = vec![0u8; size_of::<u32>() * (num_values as usize)];

                cfg_if! {
                    if #[cfg(feature="async-io")] {
                        let (res, buf) = file.read_exact_at(data, df_offset).await;
                        res?;
                        data = buf;
                    } else {
                        file.seek(SeekFrom::Start(df_offset))?;
                        file.read_exact(&mut data)?;
                    }
                }

                for pos in 0..num_values {
                    let len = 2 * size_of::<u32>();
                    let offset = (pos as usize) * len;

                    let old_offset =
                        u32::from_le_bytes(data[offset..offset + len].try_into().unwrap());
                    let new_offset =
                        u32::from_le_bytes(data[offset..offset + len].try_into().unwrap());
                    fold_table.insert(old_offset, new_offset);
                }

                let offset = df_offset + data.len() as u64;
                (offset, Some(fold_table))
            } else {
                let offset = df_offset + (num_values as u64) * (size_of::<u32>() as u64);
                (offset, None)
            };

            let data = disk::read(&fpath, offset).await?;
            let batch = Arc::new(ValueBatch { fold_table, data });

            cache.put(identifier, batch.clone());
            Ok(batch)
        }
    }

    pub async fn get<V: serde::de::DeserializeOwned>(
        &self,
        value_ref: ValueId,
    ) -> Result<V, Error> {
        log::trace!("Getting value at {value_ref:?}");

        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await?;

        let val = batch.get_value(offset);
        Ok(super::get_encoder().deserialize(val)?)
    }

    async fn is_batch_folded(&self, identifier: ValueBatchId) -> Result<bool, Error> {
        let fpath = self.get_file_path(&identifier);
        let mut data = vec![0u8; 1];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let file = File::open(fpath).await?;
                let (res, buf) = file.read_exact_at(data, 0).await;
                res?;
                data = buf;
            } else {
                let mut file = File::open(fpath)?;
                file.read_exact(&mut data)?;
            }
        }

        Ok(data[0] != 0u8)
    }

    async fn get_active_values_in_batch(&self, identifier: ValueBatchId) -> Result<u32, Error> {
        let fpath = self.get_file_path(&identifier);
        let mut data = vec![0u8; size_of::<u32>()];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let file = File::open(&fpath).await?;
                let pos = size_of::<u8>() as u64;
                let (res, buf) = file.read_exact_at(data, pos).await;
                res?;
                data = buf;
            } else {
                let mut file = File::open(&fpath)?;
                file.seek(SeekFrom::Start(size_of::<u8>() as u64))?;
                file.read_exact(&mut data)?;
            }
        }

        let num_values = u32::from_le_bytes(data.as_slice().try_into().unwrap());
        let mut delete_flags = vec![0u8; num_values as usize];

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let pos = (size_of::<u8>()+size_of::<u32>()) as u64;
                let (res, buf) = file.read_exact_at(delete_flags, pos).await;
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
        let mut data = vec![0u8; size_of::<u32>()];
        let fpath = self.get_file_path(&identifier);

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let file = File::open(fpath).await?;
                let pos = size_of::<u8>() as u64;
                let (res, buf) = file.read_exact_at(data, pos).await;
                res?;
                data = buf;
            } else {
                let mut file = File::open(fpath)?;
                file.seek(SeekFrom::Start(size_of::<u8>() as u64))?;
                file.read_exact(&mut data)?;
            }
        }

        Ok(u32::from_le_bytes(data.as_slice().try_into().unwrap()))
    }
}

impl ValueBatch {
    fn get_value(&self, pos: ValueOffset) -> &[u8] {
        let pos = if let Some(fold_table) = &self.fold_table {
            *fold_table.get(&pos).expect("No such entry")
        } else {
            pos
        };

        let mut offset = pos as usize;

        let len_len = size_of::<u32>();

        let vlen_data = self.data[offset..offset + len_len].try_into().unwrap();
        let vlen = u32::from_le_bytes(vlen_data);

        offset += len_len;
        &self.data[offset..offset + (vlen as usize)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::{Builder, TempDir};

    async fn test_init() -> (TempDir, ValueLog) {
        let tmp_dir = Builder::new()
            .prefix("lsm-value-log-test-")
            .tempdir()
            .unwrap();
        let _ = env_logger::builder().is_test(true).try_init();

        let mut params = Params::default();
        params.db_path = tmp_dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        (tmp_dir, ValueLog::new(params, manifest).await)
    }

    #[cfg(feature = "wisckey-fold")]
    #[tokio::test]
    async fn delete_value() {
        const SIZE: usize = 1_000;

        let (_tmpdir, values) = test_init().await;

        let mut builder = values.make_batch().await;

        let mut data = vec![];
        data.resize(SIZE, 'a' as u8);

        let value = String::from_utf8(data).unwrap();

        let e = crate::get_encoder();
        let _ = builder.add_value(e.serialize(&value).unwrap()).await;
        let vid2 = builder.add_value(e.serialize(&value).unwrap()).await;

        let batch_id = builder.finish().await.unwrap();

        assert_eq!(
            values.get_active_values_in_batch(batch_id).await.unwrap(),
            2
        );
        assert_eq!(values.get_total_values_in_batch(batch_id).await.unwrap(), 2);

        values.mark_value_deleted(vid2).await.unwrap();

        assert_eq!(
            values.get_active_values_in_batch(batch_id).await.unwrap(),
            1
        );
        assert_eq!(values.get_total_values_in_batch(batch_id).await.unwrap(), 2);
    }

    #[cfg(feature = "wisckey-fold")]
    #[tokio::test]
    async fn delete_batch() {
        const SIZE: usize = 1_000;

        let (_tmpdir, values) = test_init().await;
        let mut builder = values.make_batch().await;

        let mut data = vec![];
        data.resize(SIZE, 'a' as u8);

        let value = String::from_utf8(data).unwrap();

        let e = crate::get_encoder();
        let vid = builder.add_value(e.serialize(&value).unwrap()).await;

        let batch_id = builder.finish().await.unwrap();

        assert_eq!(
            values.get_active_values_in_batch(batch_id).await.unwrap(),
            1
        );
        assert_eq!(values.get_total_values_in_batch(batch_id).await.unwrap(), 1);

        values.mark_value_deleted(vid).await.unwrap();

        let result = values.get_batch(batch_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_put_many() {
        let (_tmpdir, values) = test_init().await;
        let e = crate::get_encoder();

        let mut builder = values.make_batch().await;
        let mut vids = vec![];

        for pos in 0..1000u32 {
            let value = format!("Number {}", pos);

            let vid = builder.add_value(e.serialize(&value).unwrap()).await;
            vids.push(vid);
        }

        builder.finish().await.unwrap();

        for (pos, vid) in vids.iter().enumerate() {
            let value = format!("Number {}", pos);

            let result = values.get::<String>(*vid).await.unwrap();
            assert_eq!(result, value);
        }
    }

    #[tokio::test]
    async fn fold() {
        let (_tmpdir, values) = test_init().await;
        let e = crate::get_encoder();

        let mut vids = vec![];
        let mut builder = values.make_batch().await;

        for pos in 0..20u32 {
            let value = format!("Number {}", pos);

            let vid = builder.add_value(e.serialize(&value).unwrap()).await;
            vids.push(vid);
        }

        let batch_id = builder.finish().await.unwrap();

        for pos in 2..19 {
            values.mark_value_deleted(vids[pos]).await.unwrap();
        }

        assert!(values.is_batch_folded(batch_id).await.unwrap());
        assert_eq!(
            values.get_active_values_in_batch(batch_id).await.unwrap(),
            3
        );

        for pos in [0u32, 1u32, 19u32] {
            let vid = vids[pos as usize];
            let value = format!("Number {}", pos);

            let result = values.get::<String>(vid).await.unwrap();
            assert_eq!(result, value);
        }
    }

    #[tokio::test]
    async fn get_put_large_value() {
        let (_tmpdir, values) = test_init().await;

        const SIZE: usize = 1_000_000;
        let mut builder = values.make_batch().await;

        let mut data = vec![];
        data.resize(SIZE, 'a' as u8);

        let value = String::from_utf8(data).unwrap();

        let e = crate::get_encoder();
        let vid = builder.add_value(e.serialize(&value).unwrap()).await;

        builder.finish().await.unwrap();

        let result = values.get::<String>(vid).await.unwrap();
        assert_eq!(result, value);
    }
}
