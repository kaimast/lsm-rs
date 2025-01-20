use std::cmp::Ordering;
use std::mem::size_of;
use std::path::Path;

use crate::data_blocks::DataBlockId;
use crate::sorted_table::TableId;
use crate::{Error, disk};
use crate::{Key, Params};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(Debug, IntoBytes, KnownLayout, Immutable, FromBytes)]
#[repr(C, packed)]
struct IndexBlockHeader {
    size: u64,
    min_key_len: u32,
    max_key_len: u32,
    num_data_blocks: u32,
    _padding: u32,
}

#[derive(IntoBytes, KnownLayout, Immutable, FromBytes)]
#[repr(C, packed)]
struct IndexEntryHeader {
    block_id: DataBlockId,
    key_len: u32,
    _padding: u32,
}

/** Index blocks hold metadata about a sorted table
 * Each table has exactly one index block
 *
 * Layout:
 *   - Header
 *   - Min key bytes
 *   - Max key bytes
 *   - Offset list
 *   - Index Entries
**/
pub struct IndexBlock {
    data: Vec<u8>,
}

impl IndexBlock {
    pub async fn new(
        params: &Params,
        id: TableId,
        index: Vec<(Key, DataBlockId)>,
        size: u64,
        min_key: Key,
        max_key: Key,
    ) -> Result<Self, Error> {
        let header = IndexBlockHeader {
            size,
            min_key_len: min_key.len() as u32,
            max_key_len: max_key.len() as u32,
            num_data_blocks: index.len() as u32,
            _padding: 0,
        };

        let mut block_data = header.as_bytes().to_vec();
        block_data.extend_from_slice(&min_key);
        block_data.extend_from_slice(&max_key);

        crate::add_padding(&mut block_data);

        // Reserve space for offsets
        let offset_start = block_data.len();
        let offset_len = crate::pad_offset(index.len());
        block_data.append(&mut vec![0u8; offset_len * size_of::<u32>()]);

        for (pos, (key, block_id)) in index.into_iter().enumerate() {
            let header = IndexEntryHeader {
                block_id,
                key_len: key.len() as u32,
                _padding: 0,
            };

            let entry_offset = block_data.len() as u32;

            block_data[offset_start + pos * size_of::<u32>()
                ..offset_start + (pos + 1) * size_of::<u32>()]
                .copy_from_slice(entry_offset.as_bytes());

            block_data.extend_from_slice(header.as_bytes());
            block_data.extend_from_slice(&key);
        }

        // Store on disk before grabbing the lock
        let fpath = Self::get_file_path(params, &id);
        disk::write(&fpath, &block_data)
            .await
            .map_err(|err| Error::from_io_error("Failed to write index block", err))?;

        Ok(IndexBlock { data: block_data })
    }

    pub async fn load(params: &Params, id: TableId) -> Result<Self, Error> {
        log::trace!("Loading index block from disk");
        let fpath = Self::get_file_path(params, &id);
        let data = disk::read(&fpath, 0)
            .await
            .map_err(|err| Error::from_io_error("Failed to read index block", err))?;

        Ok(IndexBlock { data })
    }

    /// where is this index block located on disk?
    #[inline]
    fn get_file_path(params: &Params, block_id: &TableId) -> std::path::PathBuf {
        let fname = format!("idx{:08}.data", block_id);
        params.db_path.join(Path::new(&fname))
    }

    fn get_header(&self) -> &IndexBlockHeader {
        IndexBlockHeader::ref_from_prefix(&self.data[..]).unwrap().0
    }

    fn get_entry_offset(&self, pos: usize) -> usize {
        let header = self.get_header();
        assert!((pos as u32) < header.num_data_blocks);

        let offset = size_of::<IndexBlockHeader>()
            + header.min_key_len as usize
            + header.max_key_len as usize;

        let offset_offset = crate::pad_offset(offset) + pos * size_of::<u32>();
        *u32::ref_from_prefix(&self.data[offset_offset..]).unwrap().0 as usize
    }

    /// Get the unique id for the data block at the specified index
    pub fn get_block_id(&self, pos: usize) -> DataBlockId {
        let offset = self.get_entry_offset(pos);

        let entry_header = IndexEntryHeader::ref_from_bytes(
            &self.data[offset..offset + size_of::<IndexEntryHeader>()],
        )
        .unwrap();

        entry_header.block_id
    }

    /// Get the key for the data block at the specified index
    pub fn get_block_key(&self, pos: usize) -> &[u8] {
        let offset = self.get_entry_offset(pos);

        let (entry_header, _) = IndexEntryHeader::ref_from_prefix(&self.data[offset..]).unwrap();

        let key_start = offset + size_of::<IndexEntryHeader>();
        &self.data[key_start..key_start + (entry_header.key_len as usize)]
    }

    /// How many data blocks does this table have?
    pub fn num_data_blocks(&self) -> usize {
        self.get_header().num_data_blocks as usize
    }

    /// The size of this table in bytes
    /// (for WiscKey this just counts the references, not the values themselves)
    pub fn get_size(&self) -> usize {
        self.get_header().size as usize
    }

    /// Whats the minimum key in this table?
    pub fn get_min(&self) -> &[u8] {
        let header = self.get_header();
        let key_offset = size_of::<IndexBlockHeader>();

        &self.data[key_offset..key_offset + (header.min_key_len as usize)]
    }

    /// What is the maximum key in this table?
    pub fn get_max(&self) -> &[u8] {
        let header = self.get_header();
        let key_offset = size_of::<IndexBlockHeader>() + (header.min_key_len as usize);

        &self.data[key_offset..key_offset + (header.max_key_len as usize)]
    }

    /// Search for a specific key
    /// This will a return a data block id that *might* hold this entry or None
    #[tracing::instrument(skip(self, key))]
    pub fn binary_search(&self, key: &[u8]) -> Option<DataBlockId> {
        if key < self.get_min() || key > self.get_max() {
            return None;
        }

        let header = self.get_header();

        let mut start = 0;
        let mut end = (header.num_data_blocks as usize) - 1;

        while end - start > 1 {
            let mid = (end - start) / 2 + start;
            let mid_key = self.get_block_key(mid);

            match mid_key.cmp(key) {
                Ordering::Equal => {
                    return Some(self.get_block_id(mid));
                }
                Ordering::Greater => {
                    end = mid;
                }
                Ordering::Less => {
                    start = mid;
                }
            }
        }

        assert!(key >= self.get_block_key(start));

        if key >= self.get_block_key(end) {
            Some(self.get_block_id(end))
        } else {
            Some(self.get_block_id(start))
        }
    }
}
