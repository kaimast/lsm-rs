use std::collections::HashSet;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use memmap2::MmapMut;

use zerocopy::{AsBytes, FromBytes, FromZeroes};

use tokio::sync::RwLock;

use crate::data_blocks::DataBlockId;
use crate::sorted_table::TableId;
use crate::{Error, Params};

#[cfg(feature = "wisckey")]
use crate::values::ValueBatchId;

pub type SeqNumber = u64;
pub type LevelId = u32;

pub const INVALID_TABLE_ID: TableId = 0;
pub const FIRST_TABLE_ID: TableId = 1;

/// The metadata for the database as a whole
#[derive(AsBytes, Default, FromBytes, FromZeroes)]
#[repr(packed)]
struct DatabaseMetadata {
    next_table_id: TableId,
    num_levels: u16,
    seq_number_offset: SeqNumber,
    log_offset: u64,
    next_data_block_id: DataBlockId,
    #[cfg(feature = "wisckey")]
    next_value_batch_id: ValueBatchId,
    #[cfg(feature = "wisckey")]
    value_log_offset: ValueBatchId,
}

/// For each level there is a file containing
/// an ordered list of all table identifiers
#[derive(AsBytes, Default, FromBytes, FromZeroes)]
#[repr(packed)]
struct LevelMetadataHeader {
    num_tables: u64,
}

struct LevelMetadata {
    data: MmapMut,
}

impl LevelMetadata {
    pub fn get_tables(&self) -> &[TableId] {
        let header = LevelMetadataHeader::ref_from_prefix(&self.data[..]).unwrap();

        let start = size_of::<LevelMetadataHeader>();
        let end = start + (header.num_tables as usize * size_of::<TableId>());

        unsafe { std::mem::transmute(&self.data[start..end]) }
    }

    /// Returns false if the entry already existed
    pub fn insert(&mut self, id: TableId) -> bool {
        let tables = self.get_tables();

        let pos = match tables.binary_search(&id) {
            Ok(_) => return false,
            Err(pos) => pos,
        };

        let mut tables = tables.to_vec();
        tables.insert(pos, id);

        let header = LevelMetadataHeader::mut_from_prefix(&mut self.data[..]).unwrap();
        header.num_tables = tables.len() as u64;

        let start = size_of::<LevelMetadataHeader>();
        let new_data: &[u8] = unsafe { std::mem::transmute(&tables[..]) };

        if new_data.len() - start >= self.data.len() {
            todo!(); //RESIZE
        }

        self.data[start..new_data.len()].copy_from_slice(new_data);
        true
    }

    /// Returns false if no such entry existed
    pub fn remove(&mut self, id: &TableId) -> bool {
        let tables = self.get_tables();

        let pos = match tables.binary_search(id) {
            Ok(pos) => pos,
            Err(_) => return false,
        };

        let mut tables = tables.to_vec();
        tables.remove(pos);

        let header = LevelMetadataHeader::mut_from_prefix(&mut self.data[..]).unwrap();
        header.num_tables = tables.len() as u64;

        let start = size_of::<LevelMetadataHeader>();
        let new_data: &[u8] = unsafe { std::mem::transmute(&tables[..]) };

        self.data[start..new_data.len()].copy_from_slice(new_data);
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
            num_levels: params.num_levels as u16,
            seq_number_offset: 1,
            log_offset: 0,
            next_data_block_id: 1,
            #[cfg(feature = "wisckey")]
            next_value_batch_id: 1,
            #[cfg(feature = "wisckey")]
            value_log_offset: 0,
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

            levels.push(LevelMetadata { data: level_mmap });
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
            .open(manifest_path)?;

        let data = unsafe { MmapMut::map_mut(&file) }.unwrap();

        let meta = DatabaseMetadata::ref_from(&data[..]).unwrap();
        if meta.num_levels != params.num_levels as u16 {
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
                .open(fname)?;

            let data = unsafe { MmapMut::map_mut(&file) }.unwrap();

            let header = LevelMetadataHeader::ref_from_prefix(&data[..]).unwrap();
            table_count += header.num_tables;

            levels.push(LevelMetadata { data });
        }

        log::debug!("Found {table_count} tables");

        Ok(Self {
            metadata: RwLock::new(data),
            levels: RwLock::new(levels),
            params,
        })
    }

    pub async fn next_data_block_id(&self) -> DataBlockId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        let id = meta.next_data_block_id;
        meta.next_data_block_id += 1;

        mmap.flush().unwrap();

        id
    }

    pub async fn next_table_id(&self) -> TableId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        let id = meta.next_table_id;
        meta.next_table_id += 1;

        mmap.flush().unwrap();

        id
    }

    pub async fn get_log_offset(&self) -> u64 {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from(&mmap[..]).unwrap();

        meta.log_offset
    }

    pub async fn set_log_offset(&self, offset: u64) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        assert!(meta.log_offset < offset);
        meta.log_offset = offset;

        mmap.flush().unwrap();
    }

    #[cfg(feature = "wisckey")]
    pub async fn get_value_log_offset(&self) -> u64 {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from(&mmap[..]).unwrap();

        meta.value_log_offset
    }

    #[cfg(feature = "wisckey")]
    pub async fn set_value_log_offset(&self, offset: u64) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        assert!(meta.value_log_offset < offset);
        meta.value_log_offset = offset;

        mmap.flush().unwrap();
    }

    pub async fn get_seq_number_offset(&self) -> SeqNumber {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from(&mmap[..]).unwrap();

        meta.seq_number_offset
    }

    pub async fn set_seq_number_offset(&self, offset: SeqNumber) {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        assert!(meta.seq_number_offset < offset);
        meta.seq_number_offset = offset;

        mmap.flush().unwrap();
    }

    #[cfg(feature = "wisckey")]
    pub async fn next_value_batch_id(&self) -> ValueBatchId {
        let mut mmap = self.metadata.write().await;
        let meta = DatabaseMetadata::mut_from(&mut mmap[..]).unwrap();

        let id = meta.next_value_batch_id;
        meta.next_value_batch_id += 1;

        mmap.flush().unwrap();

        id
    }

    #[cfg(feature = "wisckey")]
    pub async fn most_recent_value_batch_id(&self) -> ValueBatchId {
        let mmap = self.metadata.read().await;
        let meta = DatabaseMetadata::ref_from(&mmap[..]).unwrap();

        meta.next_value_batch_id - 1
    }

    /// Get the identifier of all tables on all levels
    /// Only used during recovery
    pub async fn get_tables(&self) -> Vec<Vec<TableId>> {
        let mut result = Vec::with_capacity(self.params.num_levels);

        for level in self.levels.read().await.iter() {
            result.push(level.get_tables().to_vec());
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
        let mut levels = self.levels.write().await;
        let mut affected = HashSet::new();

        for (level, id) in add.into_iter() {
            let was_new = levels
                .get_mut(level as usize)
                .expect("No such level")
                .insert(id);

            assert!(was_new);
            affected.insert(level);
        }

        for (level, id) in remove.into_iter() {
            let existed = levels
                .get_mut(level as usize)
                .expect("No such level")
                .remove(&id);

            assert!(existed);
            affected.insert(level);
        }

        for level_id in affected.into_iter() {
            levels[level_id as usize].flush();
        }
    }
}
