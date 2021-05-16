use std::sync::Arc;
use std::path::Path;
use std::collections::BTreeSet;
use std::convert::TryInto;

use tokio::sync::Mutex;

use crate::sorted_table::Value;

use bincode::Options;

use lru::LruCache;

#[ cfg(feature="async-io") ]
use tokio::fs::File;
#[ cfg(feature="async-io") ]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[ cfg(not(feature="async-io")) ]
use std::fs::File;
#[ cfg(not(feature="async-io")) ]
use std::io::{Read, Write};

use crate::Params;
use crate::disk;
use crate::manifest::Manifest;

use cfg_if::cfg_if;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

const NUM_SHARDS: usize = 16;

pub const GARBAGE_COLLECT_WINDOW: usize = 10;
pub const GARBATE_COLLECT_THRESHOLD: f64 = 0.1;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

pub struct ValueLog {
    params: Arc<Params>,
    manifest: Arc<Manifest>,
    batch_caches: Vec<Mutex<BatchShard>>,
}

pub struct ValueBatch {
    data: Vec<u8>
}

pub struct ValueBatchBuilder<'a> {
    vlog: &'a ValueLog,
    identifier: ValueBatchId,
    data: Vec<u8>,
    num_values: u32,
}

impl<'a> ValueBatchBuilder<'a> {
    pub async fn finish(self) {
        let fpath = self.vlog.get_file_path(&self.identifier);

        // write file header
        cfg_if! {
            if #[cfg(feature="async-io")] {
                let mut file = File::create(&fpath).await.unwrap();
                file.write_all(&self.num_values.to_le_bytes()).await.unwrap();

                let delete_markers = vec![0u8; self.num_values as usize];
                file.write_all(&delete_markers).await.unwrap();
            } else {
                let mut file = File::create(&fpath).unwrap();
                file.write_all(&self.num_values.to_le_bytes()).unwrap();

                let delete_markers = vec![0u8; self.num_values as usize];
                file.write_all(&delete_markers).unwrap();
            }
        }

        let offset = (std::mem::size_of::<u32>() as u64) + (self.num_values as u64);
        disk::write(&fpath, &self.data, offset).await;

        let batch = ValueBatch{ data: self.data };

        // Store in the cache so we don't have to load immediately
        {
            let shard_id = ValueLog::batch_to_shard_id(self.identifier);
            let mut shard = self.vlog.batch_caches[shard_id].lock().await;
            shard.put(self.identifier, Arc::new(batch));
        }

        log::trace!("Created value batch #{}", self.identifier);
    }

    pub async fn add_value(&mut self, mut val: Value) -> ValueId {
        let val_len = (val.len() as u32).to_le_bytes();

        let offset = self.data.len() as u32;

        self.num_values += 1;

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

    pub async fn mark_value_deleted(&self, vid: ValueId) {
        todo!();
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
        ValueBatchBuilder{ identifier, vlog: &self, data: vec![], num_values: 0 }
    }

    #[inline]
    async fn get_batch(&self, id: ValueBatchId) -> Arc<ValueBatch> {
        let shard_id = Self::batch_to_shard_id(id);
        let mut cache = self.batch_caches[shard_id].lock().await;

        if let Some(batch) = cache.get(&id) {
            batch.clone()
        } else {
            log::trace!("Loading value batch from disk");

            let fpath = self.get_file_path(&id);

            let offset = {
                let num_values;

                cfg_if!{
                    if #[cfg(feature="async-io")] {
                        let mut file = File::open(&fpath).await.unwrap();
                        let mut len_data = [0u8; 4];

                        file.read_exact(&mut len_data).await.unwrap();
                        num_values = u32::from_le_bytes(len_data);
                    } else {
                        let mut file = File::open(&fpath).unwrap();
                        let mut len_data = [0u8; 4];

                        file.read_exact(&mut len_data).unwrap();
                        num_values = u32::from_le_bytes(len_data);
                    }
                }

                (num_values as u64) + (std::mem::size_of::<u32>() as u64)
            };

            let data = disk::read(&fpath, offset).await;

            let batch = Arc::new( ValueBatch{ data } );

            cache.put(id, batch.clone());
            batch
        }
    }

    pub async fn get<V: serde::de::DeserializeOwned>(&self, value_ref: ValueId) -> V {
        log::trace!("Getting value at {:?}", value_ref);

        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await;

        let val = batch.get_value(offset);
        super::get_encoder().deserialize(val)
            .expect("Failed to decode value")
    }
}

impl ValueBatch {
    pub fn get_value(&self, pos: ValueOffset) -> &[u8] {
        let mut offset = pos as usize;

        let len_len = std::mem::size_of::<u32>();

        let vlen = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        &self.data[offset..offset+(vlen as usize)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

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

        builder.finish().await;

        for (pos, vid) in vids.iter().enumerate() {
            let value = format!("Number {}", pos);

            let result = values.get::<String>(*vid).await;
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

        builder.finish().await;

        let result = values.get::<String>(vid).await;

        assert_eq!(result, value);
    }

}
