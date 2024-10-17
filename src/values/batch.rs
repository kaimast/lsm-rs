use std::collections::HashMap;

use std::sync::Arc;

use byte_slice_cast::AsSliceOf;

use crate::disk;
use crate::values::{ValueBatchId, ValueId, ValueLog, ValueOffset, ValueRef};
use crate::{Error, Value};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
    metadata: Vec<u8>,
    value_data: Vec<u8>,
}

pub struct ValueBatchBuilder<'a> {
    vlog: &'a ValueLog,
    identifier: ValueBatchId,
    /// The locations of the values within this block
    offsets: Vec<u8>,
    /// The value data
    value_data: Vec<u8>,
}

#[derive(Debug, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(packed)]
pub(super) struct ValueBatchHeader {
    pub folded: u32, //boolean flag
    pub num_values: u32,
}

#[derive(Debug, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(packed)]
pub(super) struct ValueEntryHeader {
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

    /// Create the batch and write it to disk
    pub async fn finish(mut self) -> Result<ValueBatchId, Error> {
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;

        let header = ValueBatchHeader {
            folded: 0,
            num_values,
        };

        let mut delete_markers = vec![0u8; crate::pad_offset(num_values as usize)];
        crate::add_padding(&mut self.offsets);

        let mut metadata = header.as_bytes().to_vec();
        metadata.append(&mut delete_markers);
        metadata.append(&mut self.offsets);

        // Write metadata uncompressed so it can be updated easily
        let meta_path = self.vlog.get_meta_file_path(&self.identifier);
        disk::write_uncompressed(&meta_path, metadata.clone()).await?;

        let data_path = self.vlog.get_data_file_path(&self.identifier);
        disk::write(&data_path, &self.value_data).await?;

        let batch = Arc::new(ValueBatch {
            fold_table: None,
            metadata,
            value_data: self.value_data,
        });

        // Store in the cache so we don't have to load immediately
        {
            let shard_id = ValueLog::batch_to_shard_id(self.identifier);
            let mut shard = self.vlog.batch_caches[shard_id].lock().await;
            shard.put(self.identifier, batch);
        }

        log::trace!("Created value batch #{}", self.identifier);
        Ok(self.identifier)
    }
}

impl ValueBatch {
    pub fn from_existing(metadata: Vec<u8>, value_data: Vec<u8>) -> Self {
        let mut obj = Self {
            fold_table: None,
            metadata,
            value_data,
        };

        // Compute fold table?
        if obj.is_folded() {
            log::trace!("Loading fold table for batch");
            let header_len = size_of::<ValueBatchHeader>();
            let header = obj.get_header();

            let offsets_start = header_len + header.delete_markers_len();
            let offsets_end = offsets_start + header.offsets_len();
            let offsets = obj.metadata[offsets_start..offsets_end]
                .as_slice_of::<[u32; 2]>()
                .unwrap();

            let mut fold_table = HashMap::new();

            // remove padding
            for entry in &offsets[..(header.num_values as usize)] {
                fold_table.insert(entry[0], entry[1]);
            }

            obj.fold_table = Some(fold_table);
        }

        obj
    }

    pub fn get_ref(self_ptr: Arc<ValueBatch>, pos: ValueOffset) -> ValueRef {
        let pos = if let Some(fold_table) = &self_ptr.fold_table {
            *fold_table.get(&pos).expect("No such entry")
        } else {
            pos
        };

        let mut offset = pos as usize;
        let (vheader, _) =
            ValueEntryHeader::ref_from_prefix(&self_ptr.value_data[offset..]).unwrap();

        offset += size_of::<ValueEntryHeader>();

        ValueRef {
            length: vheader.length as usize,
            batch: self_ptr,
            offset,
        }
    }

    /// Access the raw data of this batch
    pub(super) fn get_value_data(&self) -> &[u8] {
        &self.value_data
    }

    fn get_header(&self) -> &ValueBatchHeader {
        ValueBatchHeader::ref_from_prefix(&self.metadata[..])
            .unwrap()
            .0
    }

    pub(super) fn get_delete_flags(&self) -> &[u8] {
        let header = self.get_header();
        let header_len = size_of::<ValueBatchHeader>();
        let flags = &self.metadata[header_len..header_len + header.delete_markers_len()];

        // Remove padding
        &flags[..(header.num_values as usize)]
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
