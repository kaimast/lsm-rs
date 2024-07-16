use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::Arc;

use crate::data_blocks::{DataBlockBuilder, DataBlockId, DataBlocks, PrefixedKey};
use crate::index_blocks::IndexBlock;
use crate::manifest::SeqNumber;
use crate::{Error, Key, KvTrait, Params, WriteOp};

#[cfg(feature = "wisckey")]
use crate::values::ValueId;

use super::{SortedTable, TableId};

/// Helper class to construct a table
/// only used during compaction
pub struct TableBuilder<'a> {
    identifier: TableId,
    params: &'a Params,
    data_blocks: Arc<DataBlocks>,
    min_key: Key,
    max_key: Key,

    data_block: DataBlockBuilder,
    block_index: Vec<(Key, DataBlockId)>,
    last_key: Key,
    block_entry_count: usize,
    size: u64,
    restart_count: u32,
    index_key: Option<Key>,
}

impl<'a> TableBuilder<'a> {
    #[tracing::instrument(skip(params, data_blocks, min_key, max_key))]
    pub fn new(
        identifier: TableId,
        params: &'a Params,
        data_blocks: Arc<DataBlocks>,
        min_key: Key,
        max_key: Key,
    ) -> TableBuilder<'a> {
        let block_index = vec![];
        let last_key = vec![];
        let block_entry_count = 0;
        let size = 0;
        let restart_count = 0;
        let index_key = None;
        let data_block = DataBlocks::build_block(data_blocks.clone());

        Self {
            identifier,
            params,
            data_blocks,
            block_index,
            data_block,
            last_key,
            block_entry_count,
            size,
            restart_count,
            index_key,
            min_key,
            max_key,
        }
    }

    #[cfg(feature = "wisckey")]
    #[tracing::instrument(skip(self, key, seq_number, value_ref))]
    pub async fn add_value(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        value_ref: ValueId,
    ) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value_ref)
            .await
    }

    #[cfg(feature = "wisckey")]
    #[tracing::instrument(skip(self, key, seq_number))]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, ValueId::default())
            .await
    }

    #[cfg(not(feature = "wisckey"))]
    #[tracing::instrument(skip(self, key, seq_number, value))]
    pub async fn add_value(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        value: &[u8],
    ) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::PUT_OP, value)
            .await
    }

    #[cfg(not(feature = "wisckey"))]
    #[tracing::instrument(skip(self, key, seq_number))]
    pub async fn add_deletion(&mut self, key: &[u8], seq_number: SeqNumber) -> Result<(), Error> {
        self.add_entry(key, seq_number, WriteOp::DELETE_OP, &[])
            .await
    }

    async fn add_entry(
        &mut self,
        key: &[u8],
        seq_number: SeqNumber,
        op_type: u8,
        #[cfg(feature = "wisckey")] value: ValueId,
        #[cfg(not(feature = "wisckey"))] value: &[u8],
    ) -> Result<(), Error> {
        if self.index_key.is_none() {
            self.index_key = Some(key.to_vec());
        }
        let mut prefix_len = 0;

        // After a certain interval we reset the prefixed keys
        // So that it is possible to binary search blocks
        if self.restart_count == self.params.block_restart_interval {
            self.restart_count = 0;
        } else {
            // Calculate key prefix length
            while prefix_len < key.len()
                && prefix_len < self.last_key.len()
                && key[prefix_len] == self.last_key[prefix_len]
            {
                prefix_len += 1;
            }
        }

        let suffix = key[prefix_len..].to_vec();

        self.block_entry_count += 1;
        self.restart_count += 1;

        let pkey = PrefixedKey::new(prefix_len, suffix);

        self.last_key = key.to_vec();

        self.data_block
            .add_entry(pkey, key, seq_number, op_type, value);

        if self.block_entry_count >= self.params.max_key_block_size {
            self.size += self.data_block.current_size() as u64;

            let mut next_block = DataBlocks::build_block(self.data_blocks.clone());
            std::mem::swap(&mut next_block, &mut self.data_block);

            let id = next_block.finish().await?.unwrap();
            self.block_index.push((self.index_key.take().unwrap(), id));
            self.block_entry_count = 0;
            self.restart_count = 0;
            self.last_key.clear();
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn finish(mut self) -> Result<SortedTable, Error> {
        let block_size = self.data_block.current_size();

        // Block will only be created if it contained entries
        if let Some(id) = self.data_block.finish().await? {
            self.size += block_size as u64;
            self.block_index.push((self.index_key.take().unwrap(), id));
        }

        log::debug!("Created new table with {} blocks", self.block_index.len());

        let index = IndexBlock::new(
            self.params,
            self.identifier,
            self.block_index,
            self.size,
            self.min_key,
            self.max_key,
        )
        .await?;

        let allowed_seeks = if let Some(count) = self.params.seek_based_compaction {
            ((index.get_size() / 1024).max(1) as i32) * (count as i32)
        } else {
            0
        };

        Ok(SortedTable {
            index,
            allowed_seeks: AtomicI32::new(allowed_seeks),
            identifier: self.identifier,
            compaction_flag: AtomicBool::new(false),
            data_blocks: self.data_blocks,
        })
    }
}
