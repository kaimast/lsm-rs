use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "async-io")]
use tokio_uring::fs::File;

#[cfg(not(feature = "async-io"))]
use std::fs::File;

#[cfg(not(feature = "async-io"))]
use std::io::Write;

use cfg_if::cfg_if;

use crate::disk;
use crate::values::{ValueBatchId, ValueId, ValueLog, ValueOffset, ValueRef};
use crate::{Error, Value};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Debug)]
pub(super) struct ValueBatch {
    fold_table: Option<HashMap<ValueOffset, ValueOffset>>,
    data: Vec<u8>,
}

pub struct ValueBatchBuilder<'a> {
    vlog: &'a ValueLog,
    identifier: ValueBatchId,
    /// The locations of the values within this block
    offsets: Vec<u8>,
    /// The value data
    data: Vec<u8>,
}

#[derive(AsBytes, FromBytes, FromZeroes)]
#[repr(packed)]
pub(super) struct ValueBatchHeader {
    pub folded: u8, //boolean flag
    pub num_values: u32,
}

impl ValueBatchHeader {
    pub fn is_folded(&self) -> bool {
        self.folded != 0
    }
}

impl<'a> ValueBatchBuilder<'a> {
    pub fn new(identifier: ValueBatchId, vlog: &'a ValueLog) -> Self {
        Self {
            identifier,
            vlog,
            data: vec![],
            offsets: vec![],
        }
    }

    pub async fn finish(self) -> Result<ValueBatchId, Error> {
        let fpath = self.vlog.get_file_path(&self.identifier);
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;

        let header = ValueBatchHeader {
            folded: 0,
            num_values,
        };

        let header_bytes = header.as_bytes();
        let offsets_len = self.offsets.len();

        // write file header
        cfg_if! {
            if #[cfg(feature="async-io")] {
                let file = File::create(&fpath).await?;
                file.write_all_at(header_bytes.to_vec(), 0).await.0?;
                file.write_all_at(self.offsets, header_bytes.len() as u64).await.0?;
            } else {
                let mut file = File::create(&fpath)?;
                file.write_all(header_bytes)?;
                file.write_all(&self.offsets)?;
            }
        }

        //TODO use same fd
        let data_pos = header_bytes.len() + offsets_len;
        disk::write(&fpath, &self.data, data_pos as u64).await?;

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
        // The encoded length of this value
        let val_len = (val.len() as u32).to_le_bytes();

        let offset = self.data.len() as u32;
        let mut offset_data = offset.to_le_bytes().to_vec();
        self.offsets.append(&mut offset_data);

        self.data.extend_from_slice(&val_len);
        self.data.append(&mut val);

        (self.identifier, offset)
    }
}

impl ValueBatch {
    pub fn from_existing(
        fold_table: Option<HashMap<ValueOffset, ValueOffset>>,
        data: Vec<u8>,
    ) -> Self {
        Self { fold_table, data }
    }

    pub fn get_value(self_ptr: Arc<ValueBatch>, pos: ValueOffset) -> ValueRef {
        let pos = if let Some(fold_table) = &self_ptr.fold_table {
            *fold_table.get(&pos).expect("No such entry")
        } else {
            pos
        };

        let mut offset = pos as usize;

        let len_len = size_of::<u32>();

        let vlen_data = self_ptr.data[offset..offset + len_len].try_into().unwrap();
        let vlen = u32::from_le_bytes(vlen_data);

        offset += len_len;

        ValueRef {
            batch: self_ptr,
            offset,
            length: vlen as usize,
        }
    }

    /// Access the raw data of this batch
    pub(super) fn get_data(&self) -> &[u8] {
        &self.data
    }
}
