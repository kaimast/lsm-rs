use std::sync::Arc;
use std::path::Path;
use std::collections::BTreeMap;
use std::convert::TryInto;

use tokio::sync::{Mutex, RwLock};

use crate::sorted_table::Value;

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
    pending_values: RwLock<BTreeMap<ValueBatchId, Vec<u8>>>,
    batch_caches: Vec<Mutex<BatchShard>>
}

pub struct ValueBatch {
    data: Vec<u8>
}

impl ValueLog {
    pub async fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let id = manifest.next_value_batch_id().await;

        let mut pending_values = BTreeMap::new();
        pending_values.insert(id, vec![]);

        let mut batch_caches = Vec::new();
        let max_value_files = params.max_open_files / 2;
        let shard_size = max_value_files / NUM_SHARDS;
        assert!(shard_size > 0);

        for _ in 0..NUM_SHARDS {
            let cache = Mutex::new( BatchShard::new(shard_size) );
            batch_caches.push(cache);
        }

        Self{
            pending_values: RwLock::new(pending_values),
            batch_caches, params, manifest
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
        let mut pending_values = self.pending_values.write().await;
        let id = self.manifest.next_value_batch_id().await;
        pending_values.insert(id, vec![]);
    }

    pub async fn flush_pending(&self) {
        let mut pending_values = self.pending_values.write().await;
        let (id, data) = pending_values.pop_first().unwrap();

        let batch = ValueBatch{ data };

        // Store on disk before grabbing the lock
        let fpath = self.get_file_path(&id);

        crate::disk::write(&fpath, &batch.data.as_slice()).await;

        let shard_id = Self::batch_to_shard_id(id);
        let mut cache = self.batch_caches[shard_id].lock().await;
        cache.put(id, Arc::new(batch));
    }

    pub async fn add_value(&self, val: Value) -> (ValueId, usize) {
        let mut pending_values = self.pending_values.write().await;
        let mut entry = pending_values.last_entry().unwrap();

        let vdata = super::get_encoder().serialize(&val).expect("Failed to serialize value");
        let batch_data = entry.get_mut();

        let start_pos = batch_data.len() as u32;
        let val_len = vdata.len();

        ValueBatch::serialize_entry(batch_data, val);

        let id = (*entry.key(), start_pos);

        (id, val_len as usize)
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

            let batch = Arc::new( ValueBatch{ data } );

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
        let pending_values = self.pending_values.read().await;
        let batch = pending_values.get(&value_ref.0).unwrap();

        let offset = value_ref.1 as usize;
        let vdata = ValueBatch::deserialize_entry(&batch, offset);

        super::get_encoder().deserialize(vdata).unwrap()
    }
}

impl ValueBatch {
    pub fn get_value(&self, pos: ValueOffset) -> &[u8] {
        let offset = pos as usize;
        Self::deserialize_entry(&self.data, offset)
    }

    fn serialize_entry(data: &mut Vec<u8>, mut vdata: Vec<u8>) {
        let val_len = vdata.len() as u32;
        let mut len_data = val_len.to_le_bytes().to_vec();
        data.append(&mut len_data);
        data.append(&mut vdata);
    }

    fn deserialize_entry(data: &[u8], offset: usize) -> &[u8] {
        let vlen_len = std::mem::size_of::<u32>();
        let vlen = u32::from_le_bytes(data[offset..offset+vlen_len].try_into().unwrap());

        &data[offset+vlen_len..offset+vlen_len+(vlen as usize)]
    }
}

#[cfg(test)]
mod tests {
    use super::ValueBatch;

    #[test]
    fn serialize_value() {
        let mut data = vec![];
        let vdata: Vec<u8> = vec![1,2,3,4];

        ValueBatch::serialize_entry(&mut data, vdata.clone());

        let vdata2 = ValueBatch::deserialize_entry(&data, 0);

        assert_eq!(&vdata, vdata2);
    }
}
