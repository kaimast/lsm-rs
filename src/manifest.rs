use serde::{Serialize, Deserialize};

use std::io::SeekFrom;
use std::sync::Arc;
use std::path::Path;
use std::collections::HashSet;

#[ cfg(feature="async-io") ]
use tokio::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};

#[ cfg(not(feature="async-io")) ]
use std::io::{Seek, Read, Write};

use tokio::sync::{RwLock, Mutex, MutexGuard};

use crate::{Error, Params};
use crate::sorted_table::TableId;
use crate::data_blocks::DataBlockId;

#[ cfg(feature="wisckey") ]
use crate::values::ValueBatchId;

use cfg_if::cfg_if;

pub type SeqNumber = u64;
pub type LevelId = u32;

#[ derive(Debug, Default, Serialize, Deserialize) ]
struct MetaData {
    next_table_id: TableId,
    seq_number_offset: SeqNumber,
    log_offset: u64,
    next_data_block_id: DataBlockId,
    #[ cfg(feature="wisckey") ]
    next_value_batch_id: ValueBatchId,
    #[ cfg(feature="wisckey") ]
    value_log_offset: ValueBatchId,
}

pub type LevelData = HashSet<TableId>;

/// Keeps track of the LSM meta-data
/// Will persist to disk
pub struct Manifest {
    params: Arc<Params>,
    meta: RwLock<MetaData>,
    tables: Mutex<Vec<LevelData>>,
}

const MANIFEST_NAME: &str = "Manifest";

impl Manifest {
    /// Create new manifest for an empty database
    pub async fn new(params: Arc<Params>) -> Self {
        let meta = MetaData{
            next_table_id: 1,
            seq_number_offset: 1,
            log_offset: 0,
            next_data_block_id: 1,
            #[ cfg(feature="wisckey") ]
            next_value_batch_id: 1,
            #[ cfg(feature="wisckey") ]
            value_log_offset: 0,
        };

        let tables = Mutex::new( Vec::new() );
        let obj = Self{ meta: RwLock::new(meta), params, tables };

        {
            let meta = obj.meta.write().await;
            obj.sync_header(&*meta).await;
        }

        {
            // set all levels to empty initially
            let mut tables = obj.tables.lock().await;
            tables.resize(obj.params.num_levels, HashSet::default());

            obj.write_table_set(&*tables).await;
        }

        obj
    }

    pub async fn open(params: Arc<Params>) -> Result<Self, Error> {
        let header_len = bincode::serialized_size(&MetaData::default()).unwrap() as usize;
        let mut data = Vec::new();
        let manifest_path = params.db_path.join(Path::new(MANIFEST_NAME));

        cfg_if! {
            if #[cfg(feature="async-io") ] {
                let mut file = tokio::fs::OpenOptions::new()
                    .read(true).write(false).create(false).truncate(false)
                    .open(manifest_path).await?;

                file.read_to_end(&mut data).await?;
            } else {
                let mut file = std::fs::OpenOptions::new()
                    .read(true).write(false).create(false).truncate(false)
                    .open(manifest_path)?;

                file.read_to_end(&mut data)?;
            }
        };

        if data.len() < header_len {
            return Err(Error::Io("Invalid MANIFEST file".to_string()));
        }

        let meta = bincode::deserialize(&data[..header_len])?;

        let tables: Vec<LevelData> = bincode::deserialize(&data[header_len..])?;

        log::debug!("Found {} tables", tables.len());

        Ok(Self{
            meta: RwLock::new(meta), tables: Mutex::new(tables), params
        })
    }

    pub async fn next_data_block_id(&self) -> DataBlockId {
        let mut meta = self.meta.write().await;
        let id = meta.next_data_block_id;
        meta.next_data_block_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    pub async fn next_table_id(&self) -> TableId {
        let mut meta = self.meta.write().await;
        let id = meta.next_table_id;

        meta.next_table_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    pub async fn get_log_offset(&self) -> u64 {
        let meta = self.meta.read().await;
        meta.log_offset
    }

    pub async fn set_log_offset(&self, offset: u64) {
        let mut meta = self.meta.write().await;
        assert!(meta.log_offset < offset);

        meta.log_offset = offset;
        self.sync_header(&*meta).await;
    }

    #[ cfg(feature="wisckey") ]
    pub async fn get_value_log_offset(&self) -> u64 {
        let meta = self.meta.read().await;
        meta.value_log_offset
    }

    #[ allow(dead_code) ]
    #[ cfg(feature="wisckey") ]
    pub async fn set_value_log_offset(&self, offset: u64) {
        let mut meta = self.meta.write().await;
        assert!(meta.value_log_offset < offset);

        meta.value_log_offset = offset;
        self.sync_header(&*meta).await;
    }

    pub async fn get_seq_number_offset(&self) -> SeqNumber {
        let meta = self.meta.read().await;
        meta.seq_number_offset
    }

    pub async fn set_seq_number_offset(&self, offset: SeqNumber) {
        let mut meta = self.meta.write().await;
        meta.seq_number_offset = offset;
    }

    #[ cfg(feature="wisckey") ]
    pub async fn next_value_batch_id(&self) -> ValueBatchId {
        let mut meta = self.meta.write().await;
        let id = meta.next_value_batch_id;
        meta.next_value_batch_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    #[ cfg(feature="wisckey") ]
    pub async fn most_recent_value_batch_id(&self) -> ValueBatchId {
        let meta = self.meta.read().await;
        meta.next_value_batch_id - 1
    }

    pub async fn get_table_set(&self) -> MutexGuard<'_, Vec<LevelData>> {
        self.tables.lock().await
    }

    pub async fn update_table_set(&self, mut add: Vec<(LevelId, TableId)>, mut remove: Vec<(LevelId, TableId)>) {
        let mut tables = self.tables.lock().await;

        for (level, id) in add.drain(..) {
            tables.get_mut(level as usize).expect("No such level")
                .insert(id);
        }

        for (level, id) in remove.drain(..) {
            tables.get_mut(level as usize).expect("No such level")
                .remove(&id);
        }

        self.write_table_set(&*tables).await;
    }

    async fn write_table_set(&self, tables: &[LevelData]) {
        let data = bincode::serialize(tables).unwrap();
        let manifest_path = self.params.db_path.join(Path::new(MANIFEST_NAME));

        let header_len = bincode::serialized_size(&MetaData::default()).unwrap();

        cfg_if! {
            if #[cfg(feature="async-io") ] {
                let mut file = tokio::fs::OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(manifest_path).await.expect("Failed to open MANIFEST file");

                // Truncate old table list
                file.set_len(header_len).await.unwrap();
                file.seek(SeekFrom::Start(header_len)).await.unwrap();

                file.write_all(&data).await.unwrap();
            } else {
                let mut file = std::fs::OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(manifest_path).expect("Failed to open MANIFEST file");

                // Truncate old table list
                file.set_len(header_len).unwrap();
                file.seek(SeekFrom::Start(header_len)).unwrap();

                file.write_all(&data).unwrap();
            }
        }
    }

    async fn sync_header(&self, meta: &MetaData) {
        let data = bincode::serialize(&*meta).unwrap();
        let manifest_path = self.params.db_path.join(Path::new(MANIFEST_NAME));

        cfg_if! {
            if #[cfg(feature="async-io") ] {
                let mut file = tokio::fs::OpenOptions::new()
                    .read(true).write(true).create(true).truncate(false)
                    .open(manifest_path).await
                    .expect("Failed to create or open MANIFEST file");

                file.seek(SeekFrom::Start(0)).await.unwrap();
                file.write_all(&data).await.unwrap();

            } else {
                let mut file = match std::fs::OpenOptions::new()
                        .read(true).write(true).create(true).truncate(false)
                        .open(manifest_path) {
                    Ok(file) => file,
                    Err(err) => {
                        let manifest_path = self.params.db_path.join(Path::new(MANIFEST_NAME));
                        panic!("Failed to create or open MANIFEST file at {}: {}", manifest_path.as_os_str().to_str().unwrap(), err);
                    }
                };

                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&data).unwrap();
            }
        }
    }
}

