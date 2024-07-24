use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;

use crate::data_blocks::{DataBlock, DataEntry, DataEntryType};
use crate::manifest::SeqNumber;
use crate::{EntryRef, Key};

use super::SortedTable;

#[cfg(feature = "wisckey")]
use crate::values::{ValueId, ValueLog};

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
pub trait InternalIterator: Send {
    fn at_end(&self) -> bool;
    async fn step(&mut self);

    /// Returns None if this refers to a deletion
    #[cfg(feature = "wisckey")]
    async fn get_entry(&self, value_log: &ValueLog) -> Option<EntryRef>;
    #[cfg(not(feature = "wisckey"))]
    fn get_entry(&self) -> Option<EntryRef>;

    fn get_key(&self) -> &[u8];
    fn get_seq_number(&self) -> SeqNumber;
    fn get_entry_type(&self) -> DataEntryType;
}

/// Returns the entries within a table in order
pub struct TableIterator {
    block_pos: i64,
    block_offset: u32,
    key: Key,
    entry: DataEntry,
    table: Arc<SortedTable>,
    reverse: bool,
}

impl TableIterator {
    pub async fn new(table: Arc<SortedTable>, reverse: bool) -> Self {
        let last_key = vec![];

        if reverse {
            let num_blocks = table.index.num_data_blocks() as i64;
            assert!(num_blocks > 0); // tables must have at least one data block

            let block_id = table.index.get_block_id((num_blocks - 1) as usize);
            let first_block = table.data_blocks.get_block(&block_id).await;

            let len = first_block.get_num_entries();
            assert!(len > 0);
            let (key, entry) = DataBlock::get_entry_at_index(&first_block, len - 1);

            // Are we already at the end of the first block?
            let (block_pos, block_offset) = if len == 1 {
                let next_pos = num_blocks - 2;
                if next_pos >= 0 {
                    let block_id = table.index.get_block_id(next_pos as usize);
                    let next_block = table.data_blocks.get_block(&block_id).await;
                    let len = next_block.get_num_entries();
                    assert!(len > 0);
                    (next_pos, len - 1)
                } else {
                    (next_pos, 0)
                }
            } else {
                (num_blocks - 1, len - 2)
            };

            Self {
                block_pos,
                block_offset,
                key,
                entry,
                table,
                reverse,
            }
        } else {
            let block_id = table.index.get_block_id(0);
            let first_block = table.data_blocks.get_block(&block_id).await;
            let byte_len = first_block.byte_len();
            let (key, entry) = DataBlock::get_entry_at_offset(first_block, 0, &last_key);

            let next_offset = entry.get_next_offset();

            // Are we already at the end of the first block?
            let (block_pos, block_offset) = if byte_len == next_offset {
                (1, 0)
            } else {
                (0, next_offset)
            };

            Self {
                block_pos,
                block_offset,
                key,
                entry,
                table,
                reverse,
            }
        }
    }

    #[cfg(feature = "wisckey")]
    pub fn get_value_id(&self) -> Option<ValueId> {
        self.entry.get_value_id()
    }
}

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
impl InternalIterator for TableIterator {
    fn at_end(&self) -> bool {
        if self.reverse {
            self.block_pos < -1
        } else {
            self.block_pos > self.table.index.num_data_blocks() as i64
        }
    }

    fn get_key(&self) -> &[u8] {
        &self.key
    }

    fn get_seq_number(&self) -> SeqNumber {
        self.entry.get_sequence_number()
    }

    #[cfg(feature = "wisckey")]
    async fn get_entry(&self, value_log: &ValueLog) -> Option<EntryRef> {
        match self.entry.get_type() {
            DataEntryType::Put => Some(EntryRef::SortedTable {
                value_ref: value_log
                    .get_ref(self.entry.get_value_id().unwrap())
                    .await
                    .expect("No such value?"),
                entry: self.entry.clone(),
            }),
            DataEntryType::Delete => None,
        }
    }

    #[cfg(not(feature = "wisckey"))]
    fn get_entry(&self) -> Option<EntryRef> {
        match self.entry.get_type() {
            DataEntryType::Put => Some(EntryRef::SortedTable {
                entry: self.entry.clone(),
            }),
            DataEntryType::Delete => None,
        }
    }

    fn get_entry_type(&self) -> DataEntryType {
        self.entry.get_type()
    }

    #[tracing::instrument(skip(self))]
    async fn step(&mut self) {
        if self.reverse {
            match self.block_pos.cmp(&(-1)) {
                Ordering::Less => {
                    panic!("Cannot step(); already at end");
                }
                Ordering::Equal => self.block_pos -= 1,
                Ordering::Greater => {
                    let block_id = self.table.index.get_block_id(self.block_pos as usize);
                    let block = self.table.data_blocks.get_block(&block_id).await;

                    let (key, entry) = DataBlock::get_entry_at_index(&block, self.block_offset);

                    self.key = key;
                    self.entry = entry;

                    // At the end of the block?
                    if self.block_offset == 0 {
                        self.block_pos -= 1;

                        if self.block_pos >= 0 {
                            let block_id = self.table.index.get_block_id(self.block_pos as usize);
                            let block = self.table.data_blocks.get_block(&block_id).await;
                            self.block_offset = block.get_num_entries() - 1;
                        } else {
                            self.block_offset = 0;
                        }
                    } else {
                        self.block_offset -= 1;
                    }
                }
            }
        } else {
            let num_blocks = self.table.index.num_data_blocks() as i64;
            match self.block_pos.cmp(&num_blocks) {
                Ordering::Equal => {
                    self.block_pos += 1;
                    return;
                }
                Ordering::Greater => {
                    panic!("Cannot step(); already at end");
                }
                Ordering::Less => {
                    let block_id = self.table.index.get_block_id(self.block_pos as usize);
                    let block = self.table.data_blocks.get_block(&block_id).await;
                    let byte_len = block.byte_len();

                    let (key, entry) =
                        DataBlock::get_entry_at_offset(block, self.block_offset, &self.key);

                    let next_offset = entry.get_next_offset();

                    self.key = key;
                    self.entry = entry;

                    // At the end of the block?
                    if next_offset >= byte_len {
                        self.block_pos += 1;
                        self.block_offset = 0;
                    } else {
                        self.block_offset = next_offset;
                    }
                }
            }
        }
    }
}
