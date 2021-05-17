use std::sync::Arc;
use std::path::Path;
use std::convert::TryInto;
use std::mem::size_of;

use tokio::sync::Mutex;

use crate::Error;
use crate::sorted_table::Value;

use bincode::Options;

use lru::LruCache;

#[ cfg(feature="async-io") ]
use tokio::fs::{File, OpenOptions};
#[ cfg(feature="async-io") ]
use futures::io::SeekFrom;
#[ cfg(feature="async-io") ]
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[ cfg(not(feature="async-io")) ]
use std::fs::{File, OpenOptions};
#[ cfg(not(feature="async-io")) ]
use std::io::{Read, Seek, Write, SeekFrom};

use crate::Params;
use crate::disk;
use crate::manifest::Manifest;

use cfg_if::cfg_if;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

const NUM_SHARDS: usize = 16;

#[ allow(dead_code) ]
pub const GARBAGE_COLLECT_WINDOW: usize = 10;
#[ allow(dead_code) ]
pub const GARBATE_COLLECT_THRESHOLD: f64 = 0.1;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

pub struct ValueLog {
    params: Arc<Params>,
    manifest: Arc<Manifest>,
    batch_caches: Vec<Mutex<BatchShard>>,
}

struct ValueBatch {
    is_folded: bool,
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
        let fold_flag = 0u8;
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;
        let delete_markers = vec![0u8; num_values as usize];

        // write file header
        cfg_if! {
            if #[cfg(feature="async-io")] {
                let mut file = File::create(&fpath).await?;
                file.write_all(&fold_flag.to_le_bytes()).await?;
                file.write_all(&num_values.to_le_bytes()).await?;
                file.write_all(&delete_markers).await?;
                file.write_all(self.offsets[..].into()).await?;
            } else {
                let mut file = File::create(&fpath)?;
                file.write_all(&fold_flag.to_le_bytes())?;
                file.write_all(&num_values.to_le_bytes())?;
                file.write_all(&delete_markers)?;
                file.write_all(self.offsets[..].try_into().unwrap())?;
            }
        }

        let offset = (size_of::<u8>() as u64) + (size_of::<u32>() as u64)
            + ((size_of::<u32>() + size_of::<u8>()) as u64) * (num_values as u64);

        //TODO use same fd
        disk::write(&fpath, &self.data, offset).await?;

        let batch = ValueBatch{ is_folded: false, data: self.data };

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
        let max_value_files = params.max_open_files / 2;
        let shard_size = max_value_files / NUM_SHARDS;
        assert!(shard_size > 0);

        for _ in 0..NUM_SHARDS {
            let cache = Mutex::new( BatchShard::new(shard_size) );
            batch_caches.push(cache);
        }

        Self{
            params, manifest, batch_caches,
        }
    }

    pub async fn mark_value_deleted(&self, vid: ValueId) -> Result<(), Error> {
        let fpath = self.get_file_path(&vid.0);
        let mut data = [0u8; 4];
        let header_len = (size_of::<u8>() + size_of::<u32>()) as u64;

        cfg_if!{
            if #[cfg(feature="async-io")] {
                let mut file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(fpath).await?;

                file.seek(SeekFrom::Start(size_of::<u8>() as u64)).await?;
                file.read_exact(&mut data).await?;
            } else {
                let mut file =  OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(fpath)?;


                file.seek(SeekFrom::Start(size_of::<u8>() as u64))?;
                file.read_exact(&mut data)?;
            }
        }

        let num_values = u32::from_le_bytes(data);

        let mut pos = 0;
        cfg_if! {
            if #[cfg(feature="async-io")] {
                file.seek(SeekFrom::Current(num_values as i64)).await?;

                loop {
                    file.read_exact(&mut data).await?;
                    let offset = u32::from_le_bytes(data);

                    if offset == vid.1 {
                        break;
                    } else {
                        pos += 1;
                    }
                }

                file.seek(SeekFrom::Start((header_len as u64) + pos)).await?;
                file.write_all(&[1u8]).await?;
            } else {
                file.seek(SeekFrom::Current(num_values as i64))?;

                loop {
                    file.read_exact(&mut data)?;
                    let offset = u32::from_le_bytes(data);

                    if offset == vid.1 {
                        break;
                    } else {
                        pos += 1;
                    }
                }

                file.seek(SeekFrom::Start((header_len as u64) + pos))?;
                file.write_all(&[1u8])?;
            }
        }

        Ok(())
    }

    /*
    pub async fn maybe_garbage_collect(&self, bid: ValueBatchId) {
        
        log::trace!("Deleting value batch #{}", bid);

        {
            let mut batch_ids = self.batch_ids.lock().await;

            if !batch_ids.remove(&bid) {
                panic!("No such batch");
            }
        }

        let fpath = self.get_file_path(&bid);

        cfg_if! {
            if #[ cfg(feature="async-io") ] {
                tokio::fs::remove_file(&fpath).await.unwrap();
            } else {
                std::fs::remove_file(&fpath).unwrap();
            }
        }
    }*/

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        let fname = format!("val{:08}.data", batch_id);
        self.params.db_path.join(Path::new(&fname))
    }

    #[ allow(clippy::needless_lifetimes) ] //clippy bug
    pub async fn make_batch<'a>(&'a self) -> ValueBatchBuilder<'a> {
        let identifier = self.manifest.next_value_batch_id().await;
        ValueBatchBuilder{ identifier, vlog: &self, data: vec![], offsets: vec![] }
    }

    #[inline]
    async fn get_batch(&self, identifier: ValueBatchId) -> Result<Arc<ValueBatch>, Error> {
        let shard_id = Self::batch_to_shard_id(identifier);
        let mut cache = self.batch_caches[shard_id].lock().await;
        let header_len = (size_of::<bool>() + size_of::<u32>()) as u64;

        if let Some(batch) = cache.get(&identifier) {
            Ok(batch.clone())
        } else {
            log::trace!("Loading value batch from disk");

            let fpath = self.get_file_path(&identifier);
            let is_folded;
            let num_values;

            let offset = {
                let mut header_data = [0u8; 5];

                cfg_if!{
                    if #[cfg(feature="async-io")] {
                        let mut file = File::open(&fpath).await?;
                        file.read_exact(&mut header_data).await?;
                    } else {
                        let mut file = File::open(&fpath)?;
                        file.read_exact(&mut header_data)?;
                    }
                }

                num_values = u32::from_le_bytes(header_data[1..].try_into().unwrap());
                is_folded = header_data[0] != 0;

                header_len + (num_values as u64)
            };

            let data = disk::read(&fpath, offset).await
                .expect("Failed to read value batch from disk");

            let batch = Arc::new( ValueBatch{ is_folded, data } );

            cache.put(identifier, batch.clone());
            Ok(batch)
        }
    }

    pub async fn get<V: serde::de::DeserializeOwned>(&self, value_ref: ValueId)
            -> Result<V, Error> {
        log::trace!("Getting value at {:?}", value_ref);

        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await?;

        let val = batch.get_value(offset);
        Ok(super::get_encoder().deserialize(val)?)
    }

    #[ allow(dead_code) ]
    async fn get_active_values_in_batch(&self, identifier: ValueBatchId) -> Result<u32, Error> {
        let fpath = self.get_file_path(&identifier);
        let mut data = [0u8; 4];

        cfg_if!{
            if #[cfg(feature="async-io")] {
                let mut file = File::open(&fpath).await?;
                file.seek(SeekFrom::Start(size_of::<u8>() as u64)).await?;
                file.read_exact(&mut data).await?;
            } else {
                let mut file = File::open(&fpath)?;
                file.seek(SeekFrom::Start(size_of::<u8>() as u64))?;
                file.read_exact(&mut data)?;
            }
        }

        let num_values = u32::from_le_bytes(data);
        let mut delete_flags = vec![0u8; num_values as usize];
 
        cfg_if!{
            if #[cfg(feature="async-io")] {
                file.read_exact(&mut delete_flags).await?;
            } else {
                file.read_exact(&mut delete_flags)?;
            }
        }

        let mut num_active = 0;
        for f in delete_flags.drain(..) {
            if f == 0u8 {
                num_active += 1;
            }
        }

        Ok(num_active)
    }

    #[ cfg(feature="wisckey-fold") ]
    #[ allow(dead_code) ]
    async fn get_total_values_in_batch(&self, identifier: ValueBatchId) -> Result<u32, Error> {
        let mut data = [0u8; 4];
        let fpath = self.get_file_path(&identifier);

        cfg_if!{
            if #[cfg(feature="async-io")] {
                let mut file = File::open(&fpath).await?;
                file.seek(SeekFrom::Start(1)).await?;
                file.read_exact(&mut data).await?;
            } else {
                let mut file = File::open(&fpath)?;
                file.seek(SeekFrom::Start(1))?;
                file.read_exact(&mut data)?;
            }
        }

        Ok(u32::from_le_bytes(data))
    }
}

impl ValueBatch {
    fn get_value(&self, pos: ValueOffset) -> &[u8] {
        if self.is_folded {
            todo!()
        } else {
            let mut offset = pos as usize;

            let len_len = size_of::<u32>();

            let vlen_data = self.data[offset..offset+len_len].try_into().unwrap();
            let vlen = u32::from_le_bytes(vlen_data);

            offset += len_len;
            &self.data[offset..offset+(vlen as usize)]
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[ cfg(feature="wisckey-fold") ]
    #[tokio::test]
    async fn delete_value() {
        const SIZE: usize = 1_000;

        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let values = ValueLog::new(params, manifest).await;
        let mut builder = values.make_batch().await;

        let mut data = vec![];
        data.resize(SIZE, 'a' as u8);

        let value = String::from_utf8(data).unwrap();

        let e = crate::get_encoder();
        let vid = builder.add_value(e.serialize(&value).unwrap()).await;

        let batch_id = builder.finish().await.unwrap();

        assert_eq!(values.get_active_values_in_batch(batch_id).await.unwrap(), 1);
        assert_eq!(values.get_total_values_in_batch(batch_id).await.unwrap(), 1);

        values.mark_value_deleted(vid).await.unwrap();

        assert_eq!(values.get_active_values_in_batch(batch_id).await.unwrap(), 0);
        assert_eq!(values.get_total_values_in_batch(batch_id).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn get_put_many() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let e = crate::get_encoder();
        let values = ValueLog::new(params, manifest).await;

        let mut vids = vec![];
        let mut builder = values.make_batch().await;

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
    async fn get_put_large_value() {
        const SIZE: usize = 1_000_000;

        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new( Manifest::new(params.clone()).await );

        let values = ValueLog::new(params, manifest).await;
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
