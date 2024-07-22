use std::collections::HashMap;

use std::sync::Arc;

use byte_slice_cast::AsSliceOf;

use crate::disk;
use crate::values::{ValueBatchId, ValueId, ValueLog, ValueOffset, ValueRef};
use crate::{Error, Value};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

/**
 * The layout is as follows:
 *   - value batch header
 *   - delete markers (padded to WORD_SIZE)
 *   - offsets (padded to WORD_SIZE)
 *   - value entries
 */
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
    value_data: Vec<u8>,
}

#[derive(AsBytes, FromBytes, FromZeroes)]
#[repr(packed)]
pub(super) struct ValueBatchHeader {
    pub folded: u32, //boolean flag
    pub num_values: u32,
}

#[derive(Debug, AsBytes, FromBytes, FromZeroes)]
#[repr(packed)]
pub struct ValueEntryHeader {
    pub length: u64,
}

impl ValueBatchHeader {
    pub fn is_folded(&self) -> bool {
        self.folded != 0
    }

    pub fn offsets_len(&self) -> usize {
        let len = if self.is_folded() {
            (self.num_values as usize) * 2 * size_of::<u32>()
        } else {
            (self.num_values as usize) * size_of::<u32>()
        };

        crate::pad_offset(len)
    }

    pub fn delete_markers_len(&self) -> usize {
        crate::pad_offset(self.num_values as usize)
    }
}

impl<'a> ValueBatchBuilder<'a> {
    pub fn new(identifier: ValueBatchId, vlog: &'a ValueLog) -> Self {
        Self {
            identifier,
            vlog,
            value_data: vec![],
            offsets: vec![],
        }
    }

    /// Add another value to this batch
    pub async fn add_value(&mut self, mut val: Value) -> ValueId {
        // Add padding (if needed)
        let offset = crate::pad_offset(self.value_data.len());
        assert!(offset >= self.value_data.len());
        self.value_data.resize(offset, 0u8);

        self.offsets.extend_from_slice((offset as u32).as_bytes());

        let entry_header = ValueEntryHeader {
            length: val.len() as u64,
        };

        self.value_data.extend_from_slice(entry_header.as_bytes());
        self.value_data.append(&mut val);

        (self.identifier, offset as u32)
    }

    /// Create the batch
    /// Only used by Self::finish() and tests
    async fn build(&mut self) -> Result<ValueBatch, Error> {
        let fpath = self.vlog.get_file_path(&self.identifier);
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;

        let header = ValueBatchHeader {
            folded: 0,
            num_values,
        };

        let mut delete_markers = vec![0u8; crate::pad_offset(num_values as usize)];
        crate::add_padding(&mut self.offsets);

        let mut full_data = header.as_bytes().to_vec();
        full_data.append(&mut delete_markers);
        full_data.append(&mut self.offsets);
        full_data.append(&mut self.value_data);

        disk::write(&fpath, &full_data, 0).await?;

        Ok(ValueBatch {
            fold_table: None,
            data: full_data,
        })
    }

    /// Create the batch and write it to disk
    pub async fn finish(mut self) -> Result<ValueBatchId, Error> {
        let batch = self.build().await?;

        // Store in the cache so we don't have to load immediately
        {
            let shard_id = ValueLog::batch_to_shard_id(self.identifier);
            let mut shard = self.vlog.batch_caches[shard_id].lock().await;
            shard.put(self.identifier, Arc::new(batch));
        }

        log::trace!("Created value batch #{}", self.identifier);
        Ok(self.identifier)
    }
}

impl ValueBatch {
    pub fn from_existing(data: Vec<u8>) -> Self {
        let mut obj = Self {
            fold_table: None,
            data,
        };

        // Compute fold table?
        if obj.is_folded() {
            log::trace!("Loading fold table for batch");
            let header_len = size_of::<ValueBatchHeader>();
            let offsets_start = header_len + (obj.total_num_values() as usize);
            let offsets_end = offsets_start + 2 * size_of::<u32>();
            let offsets = obj.data[offsets_start..offsets_end]
                .as_slice_of::<[u32; 2]>()
                .unwrap();

            let mut fold_table = HashMap::new();
            for entry in offsets {
                fold_table.insert(entry[0], entry[1]);
            }

            obj.fold_table = Some(fold_table);
        }

        obj
    }

    fn get_values_offset(&self) -> usize {
        let header = self.get_header();
        size_of::<ValueBatchHeader>() + header.delete_markers_len() + header.offsets_len()
    }

    pub fn get_ref(self_ptr: Arc<ValueBatch>, pos: ValueOffset) -> ValueRef {
        let pos = if let Some(fold_table) = &self_ptr.fold_table {
            *fold_table.get(&pos).expect("No such entry")
        } else {
            pos
        };

        let mut offset = (pos as usize) + self_ptr.get_values_offset();
        let vheader = ValueEntryHeader::ref_from_prefix(&self_ptr.data[offset..]).unwrap();
        offset += size_of::<ValueEntryHeader>();

        ValueRef {
            length: vheader.length as usize,
            batch: self_ptr,
            offset,
        }
    }

    /// Access the raw data of this batch
    pub(super) fn get_data(&self) -> &[u8] {
        &self.data
    }

    fn get_header(&self) -> &ValueBatchHeader {
        ValueBatchHeader::ref_from_prefix(&self.data[..]).unwrap()
    }

    pub(super) fn get_delete_flags(&self) -> &[u8] {
        let header = self.get_header();
        let header_len = size_of::<ValueBatchHeader>();
        &self.data[header_len..header_len + (header.num_values as usize)]
    }

    pub fn is_folded(&self) -> bool {
        self.get_header().is_folded()
    }

    /// The number of all values in this batch, even deleted ones
    #[allow(dead_code)]
    pub fn total_num_values(&self) -> u32 {
        self.get_header().num_values
    }

    /// The number of all values in this batch that have not been deleted yet
    pub fn num_active_values(&self) -> u32 {
        let flags = self.get_delete_flags();
        let mut result = 0;
        for flag in flags.iter() {
            if flag == &0u8 {
                result += 1;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::manifest::Manifest;
    use crate::values::ValueLog;
    use crate::Params;

    use super::{ValueBatch, ValueBatchBuilder};

    #[cfg(feature = "async-io")]
    use tokio_uring::test as async_test;

    #[cfg(not(feature = "async-io"))]
    use tokio::test as async_test;

    #[async_test]
    async fn build() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("lsm-value-log-test-")
            .tempdir()
            .unwrap();
        let _ = env_logger::builder().is_test(true).try_init();

        let params = Arc::new(Params {
            db_path: tmp_dir.path().to_path_buf(),
            ..Default::default()
        });

        let manifest = Arc::new(Manifest::new(params.clone()).await);
        let vlog = ValueLog::new(params, manifest).await;

        let batch_id = 1;
        let mut builder = ValueBatchBuilder::new(batch_id, &vlog);

        let value1 = "foo".to_string().into_bytes();
        let val_id1 = builder.add_value(value1.clone()).await;
        assert_eq!(val_id1, (batch_id, 0));

        let value2 = "huge".to_string().repeat(1_000_000).into_bytes();
        let val_id2 = builder.add_value(value2.clone()).await;
        assert!(val_id2.1 > val_id1.1 + (value1.len() as u32));

        let batch = Arc::new(builder.build().await.unwrap());
        assert_eq!(
            ValueBatch::get_ref(batch.clone(), val_id1.1).get_value(),
            value1
        );
        assert_eq!(ValueBatch::get_ref(batch, val_id2.1).get_value(), value2);
    }
}
