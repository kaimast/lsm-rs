use std::sync::Arc;

use cfg_if::cfg_if;

use crate::manifest::SeqNumber;
use crate::{disk, Error};

use super::{DataBlock, DataBlockId, DataBlocks, PrefixedKey};

#[cfg(feature = "bloom-filters")]
use bloomfilter::Bloom;

#[cfg(feature = "bloom-filters")]
use super::block::{BLOOM_KEY_NUM, BLOOM_LENGTH};

#[cfg(not(feature = "wisckey"))]
use super::DataLen;

#[cfg(feature = "wisckey")]
use super::ValueId;

pub struct DataBlockBuilder {
    data_blocks: Arc<DataBlocks>,
    data: Vec<u8>,

    position: u32,
    restart_list: Vec<u32>,

    #[cfg(feature = "bloom-filters")]
    bloom_filter: Bloom<[u8]>,
}

impl DataBlockBuilder {
    #[tracing::instrument(skip(data_blocks))]
    pub(super) fn new(data_blocks: Arc<DataBlocks>) -> Self {
        let mut data = vec![];

        // The restart list keeps track of when the keys are fully reset
        // This enables using binary search in get() instead of seeking linearly
        let restart_list = vec![];

        // Reserve space for the header
        data.extend_from_slice(0u32.to_le_bytes().as_ref());
        data.extend_from_slice(0u32.to_le_bytes().as_ref());

        #[cfg(feature = "bloom-filters")]
        let bloom_filter = {
            let filter = Bloom::new(BLOOM_LENGTH, BLOOM_KEY_NUM);
            data.extend_from_slice(&[0u8; BLOOM_LENGTH]);
            data.extend_from_slice(&bincode::serialize(&filter.sip_keys()).unwrap());

            filter
        };

        let position = 0;

        Self {
            data_blocks,
            data,
            position,
            restart_list,
            #[cfg(feature = "bloom-filters")]
            bloom_filter,
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

        let pkey_len = key.prefix_len.to_le_bytes();
        let skey_len = (key.suffix.len() as u32).to_le_bytes();
        let seq_number = seq_number.to_le_bytes();

        self.data.extend_from_slice(&pkey_len[..]);
        self.data.extend_from_slice(&skey_len[..]);
        self.data.append(&mut key.suffix);

        cfg_if::cfg_if! {
            if #[cfg(feature = "wisckey")] {
                let block_id = value_ref.0.to_le_bytes();
                let offset = value_ref.1.to_le_bytes();

                self.data.extend_from_slice(&seq_number[..]);
                self.data.extend_from_slice(&[entry_type]);
                self.data.extend_from_slice(&block_id[..]);
                self.data.extend_from_slice(&offset[..]);
            } else {
                let entry_len = std::mem::size_of::<SeqNumber>() + 1 + entry_data.len();

                let mut entry_len = (entry_len as DataLen).to_le_bytes().to_vec();
                self.data.append(&mut entry_len);
                self.data.extend_from_slice(&seq_number[..]);
                self.data.extend_from_slice(&[entry_type]);
                self.data.extend_from_slice(entry_data);
            }
        }

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

        let identifier = self.data_blocks.manifest.next_data_block_id().await;

        let rl_len_len = std::mem::size_of::<u32>();
        let len_len = std::mem::size_of::<u32>();
        let restart_list_start = self.data.len() as u32;

        self.data[..rl_len_len].copy_from_slice(&restart_list_start.to_le_bytes());
        self.data[rl_len_len..rl_len_len + len_len].copy_from_slice(&self.position.to_le_bytes());

        #[cfg(feature = "bloom-filters")]
        self.data[rl_len_len + len_len..rl_len_len + len_len + BLOOM_LENGTH]
            .copy_from_slice(&self.bloom_filter.bitmap());

        for restart_offset in self.restart_list.drain(..) {
            let mut offset = restart_offset.to_le_bytes().to_vec();
            self.data.append(&mut offset);
        }

        #[cfg(feature = "bloom-filters")]
        let bloom_filter = Bloom::new(BLOOM_LENGTH, BLOOM_KEY_NUM);

        let block = Arc::new(DataBlock {
            data: self.data,
            num_entries: self.position,
            restart_interval: self.data_blocks.params.block_restart_interval,
            restart_list_start: restart_list_start as usize,
            #[cfg(feature = "bloom-filters")]
            bloom_filter,
        });
        let shard_id = DataBlocks::block_to_shard_id(identifier);

        // Store on disk before grabbing the lock
        let block_data = &block.data;
        let fpath = self.data_blocks.get_file_path(&identifier);

        disk::write(&fpath, block_data, 0).await?;

        self.data_blocks.block_caches[shard_id]
            .lock()
            .put(identifier, block);

        Ok(Some(identifier))
    }
}
