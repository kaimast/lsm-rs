use std::cmp::Ordering;
use std::convert::TryInto;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use lru::LruCache;

use crate::manifest::{Manifest, SeqNumber};
use crate::sorted_table::Key;
use crate::Params;
use crate::{disk, Error, WriteOp};

#[cfg(feature = "wisckey")]
use crate::values::{ValueBatchId, ValueId, ValueOffset};

pub type DataBlockId = u64;
const NUM_SHARDS: NonZeroUsize = NonZeroUsize::new(64).unwrap();

#[derive(Debug)]
pub struct PrefixedKey {
    prefix_len: u32,
    suffix: Vec<u8>,
}

impl PrefixedKey {
    pub fn new(prefix_len: usize, suffix: Vec<u8>) -> Self {
        Self {
            prefix_len: prefix_len as u32,
            suffix,
        }
    }
}

type BlockShard = LruCache<DataBlockId, Arc<DataBlock>>;

#[cfg(not(feature = "wisckey"))]
type DataLen = u64;

pub enum DataEntryType {
    Put,
    Delete,
}

#[derive(Clone, Debug)]
pub struct DataEntry {
    block: Arc<DataBlock>,
    offset: usize,

    #[cfg(not(feature = "wisckey"))]
    length: usize,
}

enum SearchResult {
    ExactMatch(DataEntry),
    Range(u32, u32),
}

#[cfg(feature = "wisckey")]
pub const ENTRY_LENGTH: usize =
    size_of::<SeqNumber>() + size_of::<u8>() + size_of::<ValueBatchId>() + size_of::<ValueOffset>();

impl DataEntry {
    #[inline(always)]
    fn get_length(&self) -> usize {
        cfg_if::cfg_if! {
            if #[cfg(feature="wisckey")] {
                ENTRY_LENGTH
            } else {
                self.length
            }
        }
    }

    #[inline(always)]
    fn data(&self) -> &[u8] {
        &self.block.data[self.offset..self.offset + self.get_length()]
    }

    pub fn get_sequence_number(&self) -> u64 {
        let seq_len = size_of::<SeqNumber>();
        u64::from_le_bytes(self.data()[0..seq_len].try_into().unwrap())
    }

    pub fn get_type(&self) -> DataEntryType {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let type_data = self.data()[seq_len];

        if type_data == WriteOp::PUT_OP {
            DataEntryType::Put
        } else if type_data == WriteOp::DELETE_OP {
            DataEntryType::Delete
        } else {
            panic!("Unknown data entry type");
        }
    }

    #[cfg(not(feature = "wisckey"))]
    pub fn get_value(&self) -> Option<&[u8]> {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let type_data = self.data()[seq_len];

        let header_len = seq_len + 1;

        if type_data == WriteOp::PUT_OP {
            Some(&self.data()[header_len..])
        } else if type_data == WriteOp::DELETE_OP {
            None
        } else {
            panic!("Unknown write op");
        }
    }

    #[cfg(feature = "wisckey")]
    pub fn get_value_ref(&self) -> Option<ValueId> {
        let seq_len = std::mem::size_of::<SeqNumber>();
        let batch_id_len = std::mem::size_of::<ValueBatchId>();
        let offset_len = std::mem::size_of::<ValueOffset>();

        let type_data = self.data()[seq_len];

        if type_data == WriteOp::PUT_OP {
            let offset = seq_len + 1;
            let batch_id = ValueBatchId::from_le_bytes(
                self.data()[offset..offset + batch_id_len]
                    .try_into()
                    .unwrap(),
            );
            let offset = offset + batch_id_len;
            let value_offset = ValueOffset::from_le_bytes(
                self.data()[offset..offset + offset_len].try_into().unwrap(),
            );

            Some((batch_id, value_offset))
        } else if type_data == WriteOp::DELETE_OP {
            None
        } else {
            panic!("Unknown write op");
        }
    }
}

#[derive(Debug)]
pub struct DataBlocks {
    params: Arc<Params>,
    block_caches: Vec<Mutex<BlockShard>>,
    manifest: Arc<Manifest>,
}

pub struct DataBlockBuilder {
    data_blocks: Arc<DataBlocks>,
    data: Vec<u8>,

    position: usize,
    restart_list: Vec<u32>,
}

impl DataBlockBuilder {
    fn new(data_blocks: Arc<DataBlocks>) -> Self {
        let mut data = vec![];

        // The restart list keeps track of when the keys are fully reset
        // This enables using binary search in get() instead of seeking linearly
        let restart_list = vec![];

        // Reserve space to mark where the restart list starts
        data.append(&mut 0u32.to_le_bytes().to_vec());

        let position = 0;

        Self {
            data_blocks,
            data,
            position,
            restart_list,
        }
    }

    pub fn add_entry(
        &mut self,
        mut key: PrefixedKey,
        seq_number: SeqNumber,
        entry_type: u8,
        #[cfg(not(feature = "wisckey"))] entry_data: &[u8],
        #[cfg(feature = "wisckey")] value_ref: ValueId,
    ) {
        if self.position % self.data_blocks.params.block_restart_interval == 0 {
            assert!(key.prefix_len == 0);
            self.restart_list.push(self.data.len() as u32);
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

    pub async fn finish(mut self) -> Result<Option<DataBlockId>, Error> {
        if self.position == 0 {
            return Ok(None);
        }

        let identifier = self.data_blocks.manifest.next_data_block_id().await;

        let rl_len_len = std::mem::size_of::<u32>();
        let restart_list_start = self.data.len() as u32;

        self.data[..rl_len_len].copy_from_slice(&restart_list_start.to_le_bytes());

        for restart_offset in self.restart_list.drain(..) {
            let mut offset = restart_offset.to_le_bytes().to_vec();
            self.data.append(&mut offset);
        }

        let block = Arc::new(DataBlock {
            data: self.data,
            restart_list_start: restart_list_start as usize,
        });
        let shard_id = DataBlocks::block_to_shard_id(identifier);

        // Store on disk before grabbing the lock
        let block_data = &block.data;
        let fpath = self.data_blocks.get_file_path(&identifier);

        disk::write(&fpath, block_data, 0).await?;

        let mut cache = self.data_blocks.block_caches[shard_id].lock().await;
        cache.put(identifier, block);

        Ok(Some(identifier))
    }
}

impl DataBlocks {
    pub fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        let max_data_files = NonZeroUsize::new(params.max_open_files / 2)
            .expect("Max open files needs to be greater than 2");

        let shard_size = NonZeroUsize::new(max_data_files.get() / NUM_SHARDS)
            .expect("Not enough open files to support the number of shards");

        let mut block_caches = Vec::new();
        for _ in 0..NUM_SHARDS.get() {
            block_caches.push(Mutex::new(BlockShard::new(shard_size)));
        }

        Self {
            params,
            block_caches,
            manifest,
        }
    }

    #[inline]
    fn block_to_shard_id(block_id: DataBlockId) -> usize {
        (block_id as usize) % NUM_SHARDS
    }

    #[inline]
    fn get_file_path(&self, block_id: &DataBlockId) -> std::path::PathBuf {
        let fname = format!("key{block_id:08}.data");
        self.params.db_path.join(Path::new(&fname))
    }

    #[tracing::instrument]
    pub fn build_block(self_ptr: Arc<DataBlocks>) -> DataBlockBuilder {
        DataBlockBuilder::new(self_ptr)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_block(&self, id: &DataBlockId) -> Arc<DataBlock> {
        let shard_id = Self::block_to_shard_id(*id);

        let mut cache = self.block_caches[shard_id].lock().await;
        if let Some(block) = cache.get(id) {
            block.clone()
        } else {
            log::trace!("Loading data block from disk");
            let fpath = self.get_file_path(id);
            let data = disk::read(&fpath, 0)
                .await
                .expect("Failed to load data block from disk");
            let block = Arc::new(DataBlock::new_from_data(data));

            cache.put(*id, block.clone());
            block
        }
    }
}

/**
 * For WiscKey the data layout is the following:
 * 1. 4 bytes marking where the restart list starts
 * 2. Variable length of Block Entries, where each entry is:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Variable length key suffix
 *  - Fixed size value reference, with
 *    - seq_number (8 bytes)
 *    - entry type (1 byte)
 *    - value block id
 *    - offset
 * 3. Variable length or restart list (each entry is 4bytes; so we don't need length information)
 *
 * Otherwise:
 * 1. 4 bytes marking where the restart list starts
 * 2. Variable length of Block Entries, where each entry is:
 *  - Key prefix len (4 bytes)
 *  - Key suffix len (4 bytes)
 *  - Variable length key suffix
 *  - Value length (8 bytes)
 *  - Entry Type (1 byte)
 *  - Sequence number (8 bytes)
 *  - Variable length value
 * 3. Variable length or restart list (each entry is 4bytes; so we don't need length information)
 */
//TODO support data block layouts without prefixed keys
#[derive(Debug)]
pub struct DataBlock {
    restart_list_start: usize,
    data: Vec<u8>,
}

impl DataBlock {
    pub fn new_from_data(data: Vec<u8>) -> Self {
        let rls_len = std::mem::size_of::<u32>();
        let restart_list_start = u32::from_le_bytes(data[..rls_len].try_into().unwrap()) as usize;

        assert!(!data.is_empty(), "No data?");
        assert!(restart_list_start <= data.len(), "Data corrupted?");

        Self {
            data,
            restart_list_start,
        }
    }

    /// Get the key and entry at the specified offset (must be valid!)
    /// The third entry in this result is the new offset after the entry
    #[tracing::instrument(skip(self_ptr))]
    pub fn get_offset(
        self_ptr: Arc<DataBlock>,
        offset: u32,
        previous_key: &[u8],
    ) -> (Key, DataEntry, u32) {
        let rl_len_len = std::mem::size_of::<u32>();
        let mut offset = (offset as usize) + rl_len_len;

        assert!(offset < self_ptr.restart_list_start);

        let len_len = std::mem::size_of::<u32>();
        let pkey_len =
            u32::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
        offset += len_len;

        let skey_len =
            u32::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
        offset += len_len;

        let kdata = [
            &previous_key[..pkey_len as usize],
            &self_ptr.data[offset..offset + (skey_len as usize)],
        ]
        .concat();
        offset += skey_len as usize;

        #[cfg(not(feature = "wisckey"))]
        let entry_len = {
            let len_len = std::mem::size_of::<DataLen>();
            let elen =
                DataLen::from_le_bytes(self_ptr.data[offset..offset + len_len].try_into().unwrap());
            offset += len_len;

            elen as usize
        };

        // WiscKey has constant-size entries
        #[cfg(feature = "wisckey")]
        let entry_len = ENTRY_LENGTH;

        let entry = DataEntry {
            block: self_ptr,
            offset,
            #[cfg(not(feature = "wisckey"))]
            length: entry_len,
        };

        offset += entry_len;
        offset -= rl_len_len;

        (kdata, entry, offset as u32)
    }

    /// Length of this block in bytes (without the reset list)
    pub fn byte_len(&self) -> u32 {
        // "Cut-off" the beginning and end
        let rl_len_len = std::mem::size_of::<u32>();
        let rl_len = self.data.len() - self.restart_list_start;

        (self.data.len() - rl_len_len - rl_len) as u32
    }

    #[inline(always)]
    fn restart_list_len(&self) -> usize {
        let offset_len = std::mem::size_of::<u32>();
        let rl_len = self.data.len() - self.restart_list_start;

        assert!(rl_len % offset_len == 0);
        rl_len / offset_len
    }

    #[inline(always)]
    fn get_restart_offset(&self, pos: u32) -> u32 {
        let rl_len_len = std::mem::size_of::<u32>() as u32;
        let offset_len = std::mem::size_of::<u32>();

        let pos = self.restart_list_start + (pos as usize) * offset_len;

        u32::from_le_bytes(self.data[pos..pos + offset_len].try_into().unwrap()) - rl_len_len
    }

    #[tracing::instrument(skip(self_ptr, key))]
    fn binary_search(self_ptr: &Arc<DataBlock>, key: &[u8]) -> SearchResult {
        let rl_len = self_ptr.restart_list_len();

        let mut start: u32 = 0;
        let mut end = (rl_len as u32) - 1;

        // binary search
        while end - start > 1 {
            let mid = start + (end - start) / 2;
            let offset = self_ptr.get_restart_offset(mid);
            let (this_key, entry, _) = Self::get_offset(self_ptr.clone(), offset, &[]);

            match this_key.as_slice().cmp(key) {
                Ordering::Equal => {
                    // Exact match
                    return SearchResult::ExactMatch(entry);
                }
                Ordering::Less => {
                    // continue with right half
                    start = mid;
                }
                Ordering::Greater => {
                    // continue with left half
                    end = mid;
                }
            }
        }

        // There is no reset at the very end so we need to include
        // that part in the sequential search
        let end = if end + 1 == rl_len as u32 {
            self_ptr.byte_len()
        } else {
            self_ptr.get_restart_offset(end)
        };

        SearchResult::Range(start, end)
    }

    #[tracing::instrument(skip(self_ptr, key))]
    pub fn get(self_ptr: &Arc<DataBlock>, key: &[u8]) -> Option<DataEntry> {
        let (start, end) = match Self::binary_search(self_ptr, key) {
            SearchResult::ExactMatch(entry) => {
                return Some(entry);
            }
            SearchResult::Range(start, end) => (start, end),
        };

        let mut pos = self_ptr.get_restart_offset(start);

        let mut last_key = vec![];
        while pos < end {
            let (this_key, entry, new_pos) = Self::get_offset(self_ptr.clone(), pos, &last_key);

            if key == this_key {
                return Some(entry);
            }

            pos = new_pos;
            last_key = this_key;
        }

        // Not found
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[cfg(feature = "async-io")]
    use tokio_uring::test as async_test;

    #[cfg(not(feature = "async-io"))]
    use tokio::test as async_test;

    #[cfg(feature = "wisckey")]
    #[async_test]
    async fn store_and_load() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));
        let mut builder = DataBlocks::build_block(data_blocks.clone());

        let key1 = PrefixedKey {
            prefix_len: 0,
            suffix: vec![5],
        };
        let seq1 = 14234524;
        let val1 = (4, 2);
        builder.add_entry(key1, seq1, WriteOp::PUT_OP, val1);

        let key2 = PrefixedKey {
            prefix_len: 1,
            suffix: vec![2],
        };
        let seq2 = 424234;
        let val2 = (4, 5);
        builder.add_entry(key2, seq2, WriteOp::PUT_OP, val2);

        let id = builder.finish().await.unwrap().unwrap();
        let data_block1 = data_blocks.get_block(&id).await;
        let data_block2 = Arc::new(DataBlock::new_from_data(data_block1.data.clone()));

        let prev_key = vec![];
        let (key, entry, pos) = DataBlock::get_offset(data_block2.clone(), 0, &prev_key);

        assert_eq!(key, vec![5]);
        assert_eq!(entry.get_value_ref(), Some(val1));

        let (key, entry, pos) = DataBlock::get_offset(data_block2.clone(), pos, &key);

        assert_eq!(key, vec![5, 2]);
        assert_eq!(entry.get_value_ref(), Some(val2));
        assert_eq!(pos, data_block2.byte_len());
    }

    #[cfg(not(feature = "wisckey"))]
    #[async_test]
    async fn store_and_load() {
        let dir = tempdir().unwrap();
        let mut params = Params::default();
        params.db_path = dir.path().to_path_buf();

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));
        let mut builder = DataBlocks::build_block(data_blocks.clone());

        let key1 = PrefixedKey {
            prefix_len: 0,
            suffix: vec![5],
        };
        let seq1 = 14234524;
        let val1 = vec![4, 2];
        builder.add_entry(key1, seq1, WriteOp::PUT_OP, &val1);

        let key2 = PrefixedKey {
            prefix_len: 1,
            suffix: vec![2],
        };
        let seq2 = 424234;
        let val2 = vec![24, 50];
        builder.add_entry(key2, seq2, WriteOp::PUT_OP, &val2);

        let id = builder.finish().await.unwrap().unwrap();
        let data_block1 = data_blocks.get_block(&id).await;
        let data_block2 = Arc::new(DataBlock::new_from_data(data_block1.data.clone()));

        let prev_key = vec![];
        let (key, entry, pos) = DataBlock::get_offset(data_block2.clone(), 0, &prev_key);

        assert_eq!(key, vec![5]);
        assert_eq!(entry.get_value(), Some(&val1[..]));

        let (key, entry, pos) = DataBlock::get_offset(data_block2.clone(), pos, &key);

        assert_eq!(key, vec![5, 2]);
        assert_eq!(entry.get_value(), Some(&val2[..]));
        assert_eq!(pos, data_block2.byte_len());
    }
}
