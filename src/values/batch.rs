use std::sync::Arc;

use crate::Error;
use crate::disk;
use crate::values::{ValueBatchId, ValueId, ValueLog, ValueOffset, ValueRef};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/**
 * The layout is as follows:
 *   - value batch header
 *   - offsets (padded to WORD_SIZE)
 *   - value entries
 */
#[derive(Debug)]
pub(super) struct ValueBatch {
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

#[derive(Debug, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub(super) struct ValueBatchHeader {
    pub num_values: u32,
}

pub const BATCH_HEADER_LEN: usize = std::mem::size_of::<ValueBatchHeader>();

#[derive(Debug, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub(super) struct ValueEntryHeader {
    pub key_length: u32,
    pub value_length: u32,
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
    pub async fn add_entry(&mut self, key: &[u8], val: &[u8]) -> ValueId {
        // Add padding (if needed)
        let offset = crate::pad_offset(self.value_data.len());
        assert!(offset >= self.value_data.len());
        self.value_data.resize(offset, 0u8);

        self.offsets.extend_from_slice((offset as u32).as_bytes());

        let entry_header = ValueEntryHeader {
            key_length: key.len().try_into().expect("Key is too long"),
            value_length: val.len().try_into().expect("Value is too long"),
        };

        self.value_data.extend_from_slice(entry_header.as_bytes());
        self.value_data.extend_from_slice(key);
        self.value_data.extend_from_slice(val);

        (self.identifier, offset as u32)
    }

    /// Create the batch and write it to disk
    pub async fn finish(mut self) -> Result<ValueBatchId, Error> {
        let num_values = (self.offsets.len() / size_of::<u32>()) as u32;

        let header = ValueBatchHeader { num_values };

        crate::add_padding(&mut self.offsets);

        let mut data = header.as_bytes().to_vec();
        let file_path = self.vlog.get_batch_file_path(&self.identifier);

        data.extend_from_slice(&self.value_data);
        disk::write(&file_path, &data)
            .await
            .map_err(|err| Error::from_io_error("Failed to write value log batch", err))?;

        let batch = Arc::new(ValueBatch { data });

        // Store in the cache so we don't have to load immediately
        {
            let shard_id = ValueLog::batch_to_shard_id(self.identifier);
            let mut shard = self.vlog.batch_caches[shard_id].lock().await;
            shard.put(self.identifier, batch);
        }

        self.vlog
            .index
            .add_batch(self.identifier, num_values as usize)
            .await?;

        log::trace!("Created value batch #{}", self.identifier);
        Ok(self.identifier)
    }
}

impl ValueBatch {
    pub fn from_existing(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn get_ref(self_ptr: Arc<ValueBatch>, pos: ValueOffset) -> ValueRef {
        let mut offset = pos as usize;
        let data = &self_ptr.get_value_data()[offset..];

        let (vheader, _) = ValueEntryHeader::ref_from_prefix(data).unwrap();

        offset += size_of::<ValueEntryHeader>();
        offset += vheader.key_length as usize;

        ValueRef {
            length: vheader.value_length as usize,
            batch: self_ptr,
            offset,
        }
    }

    pub fn get_entries(&self, offsets: &[ValueOffset]) -> Vec<(Vec<u8>, Vec<u8>)> {
        offsets
            .iter()
            .map(|offset| {
                let mut offset = *offset as usize;
                let data = &self.get_value_data()[offset..];

                let (vheader, _) = ValueEntryHeader::ref_from_prefix(data).unwrap();

                offset += size_of::<ValueEntryHeader>();

                let key = data[offset..(vheader.key_length as usize)].to_vec();
                offset += vheader.key_length as usize;

                let value = data[offset..(vheader.value_length as usize)].to_vec();

                (key, value)
            })
            .collect()
    }

    /// Access the raw data of this batch
    #[inline]
    pub(super) fn get_value_data(&self) -> &[u8] {
        &self.data[BATCH_HEADER_LEN..]
    }

    #[inline]
    fn get_header(&self) -> &ValueBatchHeader {
        ValueBatchHeader::ref_from_prefix(&self.data[..]).unwrap().0
    }

    /// The number of all values in this batch, even deleted ones
    #[allow(dead_code)]
    pub fn total_num_values(&self) -> u32 {
        self.get_header().num_values
    }
}
