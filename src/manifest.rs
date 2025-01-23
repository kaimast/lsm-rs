use std::collections::HashSet;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use byte_slice_cast::{AsByteSlice, AsSliceOf};

use memmap2::MmapMut;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use tokio::sync::RwLock;

use crate::data_blocks::{DataBlockId, MIN_DATA_BLOCK_ID};
use crate::sorted_table::TableId;
use crate::{Error, Params};

#[cfg(feature = "wisckey")]
use crate::values::{FreelistPageId, MIN_FREELIST_PAGE_ID, MIN_VALUE_BATCH_ID, ValueBatchId};

pub type SeqNumber = u64;
pub type LevelId = u32;

pub const INVALID_TABLE_ID: TableId = 0;
pub const FIRST_TABLE_ID: TableId = 1;

/// The metadata for the database as a whole
#[derive(Default, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, align(8))]
struct DatabaseMetadata {
    next_table_id: TableId,
    num_levels: u32,
    _padding: u32,
    seq_number_offset: SeqNumber,
    log_offset: u64,
    next_data_block_id: DataBlockId,
    #[cfg(feature = "wisckey")]
    value_log: ValueLogMetadata,
}

#[cfg(feature = "wisckey")]
#[derive(Default, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, align(8))]
/// Metadata for the Wisckey value log
///
/// Invariants
/// * minimum_batch < next_batch_id
/// * minimum_freelist_pag < next_freelist_page
struct ValueLogMetadata {
    next_batch_id: ValueBatchId,
    minimum_batch: ValueBatchId,
    next_freelist_page_id: FreelistPageId,
    minimum_freelist_page: FreelistPageId,
}

/// For each level there is a file containing
/// an ordered list of all table identifiers
#[derive(IntoBytes, Default, Immutable, FromBytes, KnownLayout)]
#[repr(C, packed)]
struct LevelMetadataHeader {
    num_tables: u64,
}

struct LevelMetadata {
    params: Arc<Params>,
    identifier: LevelId,
    data: MmapMut,
}

impl LevelMetadata {
    pub fn get_table_ids(&self) -> &[TableId] {
        let (header, _) = LevelMetadataHeader::ref_from_prefix(&self.data[..]).unwrap();

        let start = size_of::<LevelMetadataHeader>();
        let end = start + (header.num_tables as usize * size_of::<TableId>());

        self.data[start..end].as_slice_of::<TableId>().unwrap()
    }

    /// Returns false if the entry already existed
    pub fn insert(&mut self, id: TableId) -> bool {
        let tables = self.get_table_ids();

        let pos = match tables.binary_search(&id) {
            Ok(_) => return false,
            Err(pos) => pos,
        };

        let mut tables = tables.to_vec();
        tables.insert(pos, id);

        let (header, _) = LevelMetadataHeader::mut_from_prefix(&mut self.data[..]).unwrap();
        header.num_tables = tables.len() as u64;

        let start = size_of::<LevelMetadataHeader>();
        let new_data: &[u8] = tables.as_byte_slice();

        // Resize file?
        if start + new_data.len() >= self.data.len() {
            let fname = self
                .params
                .db_path
                .join(format!("{LEVEL_PREFIX}{}{LEVEL_SUFFIX}", self.identifier));

            let file = OpenOptions::new()
                .create(false)
                .truncate(false)
                .read(true)
                .write(true)
                .open(fname)
                .expect("Failed to reopen file");

            let new_size = (self.data.len() + PAGE_SIZE) as u64;
            log::trace!(
                "Resizing metadata file for level #{} to {new_size}",
                self.identifier
            );
            file.set_len(new_size)
                .expect("Failed to resize level metadata file");
            self.data = unsafe { MmapMut::map_mut(&file) }.unwrap();
        }

        self.data[start..start + new_data.len()].copy_from_slice(new_data);
        true
    }

    /// Returns false if no such entry existed
    pub fn remove(&mut self, id: &TableId) -> bool {
        let tables = self.get_table_ids();

        let pos = match tables.binary_search(id) {
            Ok(pos) => pos,
            Err(_) => return false,
        };

        let mut tables = tables.to_vec();
        tables.remove(pos);

        let (header, next) = LevelMetadataHeader::mut_from_prefix(&mut self.data[..]).unwrap();
        header.num_tables = tables.len() as u64;

        let new_data: &[u8] = tables.as_byte_slice();
        next[..new_data.len()].copy_from_slice(new_data);

        true
    }

    pub fn flush(&mut self) {
        self.data.flush().unwrap();
    }
}

/// Keeps track of the LSM meta-data
/// Will persist to disk
pub struct Manifest {
    params: Arc<Params>,
    metadata: RwLock<MmapMut>,
    levels: RwLock<Vec<LevelMetadata>>,
}

const MANIFEST_NAME: &str = "database.meta";
const LEVEL_PREFIX: &str = "level";
const LEVEL_SUFFIX: &str = ".meta";
const PAGE_SIZE: usize = 4 * 1024;

impl Manifest {
    /// Create new manifest for an empty database
    pub async fn new(params: Arc<Params>) -> Self {
        let meta = DatabaseMetadata {
            next_table_id: FIRST_TABLE_ID,
            num_levels: params.num_levels as u32,
            _padding: 0,
            seq_number_offset: 1,
            log_offset: 0,
            next_data_block_id: MIN_DATA_BLOCK_ID,
            #[cfg(feature = "wisckey")]
            value_log: ValueLogMetadata {
                next_batch_id: MIN_VALUE_BATCH_ID,
                minimum_batch: 0,
                next_freelist_page_id: MIN_FREELIST_PAGE_ID,
                minimum_freelist_page: 0,
            },
        };

        let mut meta_mmap = {
            let manifest_path = params.db_path.join(Path::new(MANIFEST_NAME));

            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(manifest_path)
                .unwrap();

            file.set_len(size_of::<DatabaseMetadata>() as u64).unwrap();
            unsafe { MmapMut::map_mut(&file) }.unwrap()
        };
        meta_mmap.copy_from_slice(meta.as_bytes());
        meta_mmap.flush().unwrap();

        let mut levels = Vec::new();

        for idx in 0..params.num_levels {
            let level_path = params
                .db_path
                .join(format!("{LEVEL_PREFIX}{idx}{LEVEL_SUFFIX}"));

            let mut level_mmap = {
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(level_path)
                    .unwrap();
                file.set_len(PAGE_SIZE as u64).unwrap();
                unsafe { MmapMut::map_mut(&file) }.unwrap()
            };

            let level_header = LevelMetadataHeader { num_tables: 0 };
            level_mmap[..size_of::<LevelMetadataHeader>()].copy_from_slice(level_header.as_bytes());
            level_mmap.flush().unwrap();

            levels.push(LevelMetadata {
                params: params.clone(),
                identifier: idx as LevelId,
                data: level_mmap,
            });
        }

        Self {
            metadata: RwLock::new(meta_mmap),
            levels: RwLock::new(levels),
            params,
        }
    }

    pub async fn open(params: Arc<Params>) -> Result<Self, Error> {
        let manifest_path = params.db_path.join(Path::new(MANIFEST_NAME));

        let file = OpenOptions::new()
            .create(false)
            .truncate(false)
            .read(true)
            .write(true)
            .open(manifest_path)
            .map_err(|err| Error::from_io_error("Failed to open manifest", err))?;

        let data = unsafe { MmapMut::map_mut(&file) }.unwrap();

        let meta = DatabaseMetadata::ref_from_bytes(&data[..]).unwrap();
        if meta.num_levels != params.num_levels as u32 {
            panic!("Number of levels is incompatible");
        }

        let mut table_count = 0;
        let mut levels = vec![];

        for idx in 0..meta.num_levels {
            let fname = params
                .db_path
                .join(format!("{LEVEL_PREFIX}{idx}{LEVEL_SUFFIX}"));

            let file = OpenOptions::new()
                .create(false)
                .truncate(false)
                .read(true)
                .write(true)
                .open(fname)
                .map_err(|err| Error::from_io_error("Failed to open manifest", err))?;

            let data = unsafe { MmapMut::map_mut(&file) }.unwrap();

            let (header, _) = LevelMetadataHeader::ref_from_prefix(&data[..]).unwrap();
            table_count += header.num_tables;

            levels.push(LevelMetadata {
                identifier: idx as LevelId,
                params: params.clone(),
                data,
            });
        }

        log::debug!("Found {table_count} tables");

        Ok(Self {
            metadata: RwLock::new(data),
            levels: RwLock::new(levels),
            params,
        })
    }

    pub async fn generate_next_data_block_id(&self) -> DataBlockId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        let id = meta.next_data_block_id;
        meta.next_data_block_id += 1;

        mmap.flush().unwrap();

        id
    }

    pub async fn generate_next_table_id(&self) -> TableId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        let id = meta.next_table_id;
        meta.next_table_id += 1;

        mmap.flush().unwrap();

        id
    }

    pub async fn get_log_offset(&self) -> u64 {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from_bytes(&mmap[..]).unwrap();

        meta.log_offset
    }

    pub async fn set_log_offset(&self, offset: u64) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        assert!(meta.log_offset < offset);
        meta.log_offset = offset;

        mmap.flush().unwrap();
    }

    #[cfg(feature = "wisckey")]
    pub async fn get_minimum_value_batch(&self) -> u64 {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from_bytes(&mmap[..]).unwrap();

        meta.value_log.minimum_batch
    }

    #[cfg(feature = "wisckey")]
    pub async fn set_minimum_value_batch(&self, offset: ValueBatchId) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        assert!(meta.value_log.minimum_batch < offset);
        meta.value_log.minimum_batch = offset;

        mmap.flush().unwrap();
    }

    #[cfg(feature = "wisckey")]
    pub async fn set_minimum_freelist_page(&self, offset: FreelistPageId) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        assert!(meta.value_log.minimum_freelist_page < offset);
        meta.value_log.minimum_batch = offset;

        mmap.flush().unwrap();
    }

    pub async fn get_seq_number_offset(&self) -> SeqNumber {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from_bytes(&mmap[..]).unwrap();

        meta.seq_number_offset
    }

    /// Set the current sequence number
    pub async fn set_seq_number_offset(&self, offset: SeqNumber) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        if offset <= meta.seq_number_offset {
            panic!(
                "Sequence number must montonically increase. Old value ({}) was >= than new value {offset}",
                meta.seq_number_offset
            );
        }

        meta.seq_number_offset = offset;

        mmap.flush().unwrap();
    }

    #[cfg(feature = "wisckey")]
    pub async fn generate_next_value_freelist_id(&self) -> FreelistPageId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        let id = meta.value_log.next_freelist_page_id;
        meta.value_log.next_freelist_page_id += 1;

        mmap.flush().unwrap();

        id
    }

    #[cfg(feature = "wisckey")]
    pub async fn generate_next_value_batch_id(&self) -> ValueBatchId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from_bytes(&mut mmap[..]).unwrap();

        let id = meta.value_log.next_batch_id;
        meta.value_log.next_batch_id += 1;

        mmap.flush().unwrap();

        id
    }

    #[cfg(feature = "wisckey")]
    pub async fn most_recent_freelist_page_id(&self) -> ValueBatchId {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from_bytes(&mmap[..]).unwrap();

        meta.value_log.next_freelist_page_id - 1
    }

    #[cfg(feature = "wisckey")]
    pub async fn most_recent_value_batch_id(&self) -> ValueBatchId {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from_bytes(&mmap[..]).unwrap();

        meta.value_log.next_batch_id - 1
    }

    /// Get the identifier of all tables on all levels
    /// Only used during recovery
    pub async fn get_table_ids(&self) -> Vec<Vec<TableId>> {
        let mut result = Vec::with_capacity(self.params.num_levels);

        for level in self.levels.read().await.iter() {
            result.push(level.get_table_ids().to_vec());
        }

        result
    }

    /// Store updates to the tables in the manifest
    ///
    /// Note, must be called while holding logs to the affected levels to prevent race conditions
    pub async fn update_table_set(
        &self,
        add: Vec<(LevelId, TableId)>,
        remove: Vec<(LevelId, TableId)>,
    ) {
        log::trace!("Updating table set: add={add:?} remove={remove:?}");

        let mut levels = self.levels.write().await;
        let mut affected = HashSet::new();

        for (level, id) in add.into_iter() {
            let was_new = levels
                .get_mut(level as usize)
                .expect("No such level")
                .insert(id);

            if !was_new {
                panic!("Table with id={id} already existed on level #{level}");
            }
            affected.insert(level);
        }

        for (level, id) in remove.into_iter() {
            let existed = levels
                .get_mut(level as usize)
                .expect("No such level")
                .remove(&id);

            if !existed {
                panic!("No table with id={id} existed on level #{level}");
            }
            affected.insert(level);
        }

        for level_id in affected.into_iter() {
            levels[level_id as usize].flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    #[cfg(feature = "tokio-uring")]
    use kioto_uring_executor::test as async_test;

    #[cfg(feature = "monoio")]
    use monoio::test as async_test;

    #[cfg(not(feature = "_async-io"))]
    use tokio::test as async_test;

    use crate::params::Params;

    use super::Manifest;

    #[async_test]
    async fn update_table_set() {
        let dir = tempdir().unwrap();
        let params = Arc::new(Params {
            db_path: dir.path().to_path_buf(),
            ..Default::default()
        });

        let manifest = Manifest::new(params).await;

        assert!(manifest.get_table_ids().await[0].is_empty());
        assert!(manifest.get_table_ids().await[1].is_empty());

        manifest
            .update_table_set(vec![(0, 1), (0, 2)], vec![])
            .await;

        assert_eq!(manifest.get_table_ids().await[0], vec![1, 2]);
        assert!(manifest.get_table_ids().await[1].is_empty());

        manifest
            .update_table_set(vec![(1, 3)], vec![(0, 1), (0, 2)])
            .await;

        assert!(manifest.get_table_ids().await[0].is_empty());
        assert_eq!(manifest.get_table_ids().await[1], vec![3]);
    }
}
