use std::sync::Arc;
use std::path::Path;
use std::collections::BTreeSet;
use std::convert::TryInto;

use tokio::sync::{Mutex, RwLock};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::sorted_table::Value;
use crate::cond_var::Condvar;

use bincode::Options;

use lru::LruCache;

use crate::Params;
use crate::manifest::Manifest;

pub type ValueOffset = u32;
pub type ValueBatchId = u64;

pub type ValueId = (ValueBatchId, ValueOffset);

const NUM_SHARDS: usize = 16;

type BatchShard = LruCache<ValueBatchId, Arc<ValueBatch>>;

pub struct ValueLog {
    params: Arc<Params>,
    manifest: Arc<Manifest>,

    pending_batch: RwLock<ValueBatch>,
    pending_batch_file: Mutex<File>,

    batch_caches: Vec<Mutex<BatchShard>>,

    batch_ids: Mutex<BTreeSet<ValueBatchId>>,
    batch_ids_cond: Condvar,
}

pub struct ValueBatch {
    identifier: ValueBatchId,
    data: Vec<u8>
}

impl ValueLog {
    pub async fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Arc<Self> {
        let id = manifest.next_value_batch_id().await;
        let pending_batch = RwLock::new( ValueBatch{ identifier: id, data: vec![] } );

        let pending_batch_file = {
            let fpath = params.db_path.join(Path::new("pending.lld"));
            let file = File::create(&fpath).await.unwrap();

            Mutex::new(file)
        };

        let mut batch_caches = Vec::new();
        let max_value_files = params.max_open_files / 2;
        let shard_size = max_value_files / NUM_SHARDS;
        assert!(shard_size > 0);

        let batch_ids = Mutex::new( BTreeSet::new() );
        let batch_ids_cond = Condvar::new();

        for _ in 0..NUM_SHARDS {
            let cache = Mutex::new( BatchShard::new(shard_size) );
            batch_caches.push(cache);
        }

        let obj = Arc::new(Self{
            params, manifest, pending_batch, pending_batch_file,
            batch_caches, batch_ids, batch_ids_cond,
        });

        {
            let obj = obj.clone();

            tokio::spawn(async move {
                obj.garbage_collect().await;
            });
        }

        obj
    }

    pub async fn sync(&self) {
        let file = self.pending_batch_file.lock().await;
        file.sync_data().await.unwrap();
    }

    async fn garbage_collect(&self) {
        loop {
            let bid = {
                let mut bid = None;
                let mut bid_lock = self.batch_ids.lock().await;

                while bid.is_none() {
                    bid_lock = self.batch_ids_cond.wait(bid_lock, &self.batch_ids).await;
                    bid = bid_lock.first();
                }

                *bid.unwrap()
            };

            let mut live_values = BTreeSet::new();
            {
                let batch = self.get_batch(bid).await;
                let mut pos = 0u32;

                while (pos as usize) < batch.data.len() {
                    let (key, new_pos) = batch.get_key(pos);

                    live_values.insert(key.to_vec());
                    pos = new_pos;
                }
            }

        }
    }

    #[inline]
    fn batch_to_shard_id(batch_id: ValueBatchId) -> usize {
        (batch_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, batch_id: &ValueBatchId) -> std::path::PathBuf {
        let fname = format!("values{:08}.lld", batch_id);
        self.params.db_path.join(Path::new(&fname))
    }

    pub async fn next_pending(&self) {
        let id = self.manifest.next_value_batch_id().await;
        let mut file = self.pending_batch_file.lock().await;
        let mut pending_batch = self.pending_batch.write().await;
        let last_id = pending_batch.identifier;

        let mut batch = ValueBatch{ identifier: id, data: vec![] };
        std::mem::swap(&mut *pending_batch, &mut batch);

        {
            let mut batch_ids = self.batch_ids.lock().await;
            batch_ids.insert(last_id);

            self.batch_ids_cond.notify_all();
        }

        // Rewrite to disk
        // (Because the pending file is uncompressed)
        let fpath = self.get_file_path(&last_id);
        crate::disk::write(&fpath, &batch.data).await;

        // Store in the cache so we don't have to load immediately 
        {
            let shard_id = Self::batch_to_shard_id(last_id);
            let mut shard = self.batch_caches[shard_id].lock().await;
            shard.put(last_id, Arc::new(batch));
        }

        // Now we can reset the pending file
        file.set_len(0).await.unwrap();
        file.sync_data().await.unwrap();

        let fpath = self.get_file_path(&id);
        *file = File::create(&fpath).await.unwrap()
    }

    pub async fn add_value(&self, key: &[u8], mut val: Value) -> (ValueId, usize) {
        let mut file = self.pending_batch_file.lock().await;
        let mut pending_values = self.pending_batch.write().await;

        let key_len = (key.len() as u32).to_le_bytes();
        let val_len = (val.len() as u32).to_le_bytes();
        let out_len = val.len();

        let start_pos = pending_values.data.len() as u32;

        file.write_all(&key_len).await.unwrap();
        file.write_all(&val_len).await.unwrap();
        file.write_all(&key).await.unwrap();
        file.write_all(&val).await.unwrap();

        pending_values.data.extend_from_slice(&key_len);
        pending_values.data.extend_from_slice(&val_len);
        pending_values.data.extend_from_slice(key);
        pending_values.data.append(&mut val);

        let id = (pending_values.identifier, start_pos);
        (id, out_len)
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
            let data = crate::disk::read(&fpath).await;

            let batch = Arc::new( ValueBatch{ identifier: id, data } );

            cache.put(id, batch.clone());
            batch
        }
    }

    pub async fn get<V: serde::de::DeserializeOwned>(&self, value_ref: ValueId) -> V {
        let (id, offset) = value_ref;
        let batch = self.get_batch(id).await;

        let val = batch.get_value(offset);
        super::get_encoder().deserialize(val).unwrap()
    }

    pub async fn get_pending<V: serde::de::DeserializeOwned>(&self, value_ref: ValueId) -> V {
        let pending_values = self.pending_batch.read().await;
        let (batch_id, offset) = value_ref;

        assert!(pending_values.identifier == batch_id);

        let val = pending_values.get_value(offset);
        super::get_encoder().deserialize(val).unwrap()
    }
}

impl ValueBatch {
    pub fn get_key(&self, pos: ValueOffset) -> (&[u8], ValueOffset) {
        let mut offset = pos as usize;

        let len_len = std::mem::size_of::<u32>();

        let klen = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        let vlen = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        let key_ref = &self.data[offset..offset+(klen as usize)];
        offset += (klen+vlen) as usize;

        (key_ref, offset as u32)
    }

    pub fn get_value(&self, pos: ValueOffset) -> &[u8] {
        let mut offset = pos as usize;

        let len_len = std::mem::size_of::<u32>();

        let klen = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        let vlen = u32::from_le_bytes(self.data[offset..offset+len_len].try_into().unwrap());
        offset += len_len;

        offset += klen as usize;

        &self.data[offset..offset+(vlen as usize)]
    }
}
