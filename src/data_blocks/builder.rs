use cfg_if::cfg_if;

use std::sync::Arc;

use crate::manifest::SeqNumber;
use crate::{Error, disk};

use zerocopy::IntoBytes;

use super::block::{DataBlockHeader, EntryHeader};
use super::{DataBlock, DataBlockId, DataBlocks, PrefixedKey};

#[cfg(feature = "bloom-filters")]
use bloomfilter::Bloom;

#[cfg(feature = "bloom-filters")]
use super::block::{BLOOM_HEADER_SIZE, BLOOM_ITEM_COUNT, BLOOM_LENGTH};

#[cfg(feature = "wisckey")]
use crate::data_blocks::ValueId;

pub struct DataBlockBuilder {
    data_blocks: Arc<DataBlocks>,
    data: Vec<u8>,

    /// The position/index of the next entry
    /// This is also the current number of entries in this block builder
    position: u32,

    /// The restart list keeps track of when the keys are fully reset
    /// This enables using binary search in get() instead of seeking linearly
    restart_list: Vec<u32>,

    #[cfg(feature = "bloom-filters")]
    bloom_filter: Bloom<[u8]>,
}

impl DataBlockBuilder {
    #[tracing::instrument(skip(data_blocks))]
    pub(super) fn new(data_blocks: Arc<DataBlocks>) -> Self {
        // Reserve space for the header
        let data = vec![0u8; std::mem::size_of::<DataBlockHeader>()];

        Self {
            data_blocks,
            data,
            position: 0,
            restart_list: vec![],
            #[cfg(feature = "bloom-filters")]
            bloom_filter: Bloom::new(BLOOM_LENGTH, BLOOM_ITEM_COUNT)
                .expect("Failed to create bloom filter"),
        }
    }

    pub fn add_entry(
        &mut self,
        mut key: PrefixedKey,
        full_key: &[u8],
        seq_number: SeqNumber,
        entry_type: u8,
        #[cfg(not(feature = "wisckey"))] entry_data: &[u8],
        #[cfg(feature = "wisckey")] value_ref: ValueId,
    ) {
        if self.position % self.data_blocks.params.block_restart_interval == 0 {
            assert!(key.prefix_len == 0);
            self.restart_list.push(self.data.len() as u32);
        }

        cfg_if! {
            if #[cfg(feature="bloom-filters")] {
                self.bloom_filter.set(full_key);
            } else {
                let _ = full_key;
            }
        }

        let header = EntryHeader {
            prefix_len: key.prefix_len,
            suffix_len: key.suffix.len() as u32,
            seq_number,
            entry_type,
            #[cfg(feature = "wisckey")]
            value_batch: value_ref.0,
            #[cfg(feature = "wisckey")]
            value_offset: value_ref.1,
            #[cfg(not(feature = "wisckey"))]
            value_length: entry_data.len() as u64,
        };

        self.data.extend_from_slice(header.as_bytes());

        self.data.append(&mut key.suffix);

        #[cfg(not(feature = "wisckey"))]
        self.data.extend_from_slice(entry_data);

        self.position += 1;
    }

    /// Finish building an return the data blocks
    ///
    /// This will return Ok(None) if the builder did not have any entries
    /// An error might be generated if we failed to write to disk
    #[tracing::instrument(skip(self))]
    pub async fn finish(mut self) -> Result<Option<DataBlockId>, Error> {
        if self.position == 0 {
            return Ok(None);
        }

        let identifier = self
            .data_blocks
            .manifest
            .generate_next_data_block_id()
            .await;

        #[cfg(feature = "bloom-filters")]
        let bloom_filter: &[u8; BLOOM_LENGTH + BLOOM_HEADER_SIZE] =
            self.bloom_filter.as_slice().try_into().unwrap();

        let header = DataBlockHeader {
            #[cfg(feature = "bloom-filters")]
            bloom_filter: *bloom_filter,
            number_of_entries: self.position,
            restart_list_start: self.data.len() as u32,
        };

        // Write header
        self.data[..std::mem::size_of::<DataBlockHeader>()].copy_from_slice(header.as_bytes());

        // Write restart list
        for restart_offset in self.restart_list.drain(..) {
            let mut offset = restart_offset.to_le_bytes().to_vec();
            self.data.append(&mut offset);
        }

        let block = Arc::new(DataBlock {
            data: self.data,
            num_entries: header.number_of_entries,
            restart_interval: self.data_blocks.params.block_restart_interval,
            restart_list_start: header.restart_list_start as usize,
            #[cfg(feature = "bloom-filters")]
            bloom_filter: self.bloom_filter,
        });
        let shard_id = DataBlocks::block_to_shard_id(identifier);

        // Store on disk before grabbing the lock
        let block_data = &block.data;
        let fpath = self.data_blocks.get_file_path(&identifier);

        disk::write(&fpath, block_data).await.map_err(|err| {
            Error::from_io_error(format!("Failed to write data block at `{fpath:?}`"), err)
        })?;

        self.data_blocks.block_caches[shard_id]
            .lock()
            .put(identifier, block);

        Ok(Some(identifier))
    }

    /// How big is the block now?
    pub fn current_size(&self) -> usize {
        self.data.len()
    }
}
