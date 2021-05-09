use serde::{Serialize, Deserialize};

use std::io::SeekFrom;
use std::sync::Arc;
use std::path::Path;
use std::collections::HashSet;

#[ cfg(feature="async-io") ]
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[ cfg(not(feature="async-io")) ]
use std::io::{Seek, Write};

use tokio::sync::Mutex;

use crate::Params;

use crate::sorted_table::TableId;
use crate::data_blocks::DataBlockId;

#[ cfg(feature="wisckey") ]
use crate::values::ValueBatchId;

use cfg_if::cfg_if;

#[ derive(Debug, Default, Serialize, Deserialize) ]
struct MetaData {
    next_table_id: TableId,
    next_data_block_id: DataBlockId,
    #[ cfg(feature="wisckey") ]
    next_value_batch_id: ValueBatchId,
}

pub type LevelData = HashSet<TableId>;

/// Keeps track of the LSM meta-data
/// Will persist to disk
pub struct Manifest {
    params: Arc<Params>,
    meta: Mutex<MetaData>,
    tables: Mutex<Vec<LevelData>>,
}

const MANIFEST_NAME: &str = "MANIFEST";

impl Manifest {
    /// Create new manifest for an empty database
    pub async fn new(params: Arc<Params>) -> Self {
        let meta = MetaData{
            next_table_id: 1,
            next_data_block_id: 1,
            #[ cfg(feature="wisckey") ]
            next_value_batch_id: 1,
        };

        let tables = Mutex::new( Vec::new() );
        let obj = Self{ meta: Mutex::new(meta), params, tables };

        {
            let meta = obj.meta.lock().await;
            obj.sync_header(&*meta).await;
        }

        {
            // set all levels to empty initially
            let mut tables = obj.tables.lock().await;
            tables.resize(obj.params.num_levels, HashSet::default());
        }

        obj
    }

    pub async fn next_data_block_id(&self) -> DataBlockId {
        let mut meta = self.meta.lock().await;
        let id = meta.next_data_block_id;
        meta.next_data_block_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    pub async fn next_table_id(&self) -> TableId {
        let mut meta = self.meta.lock().await;
        let id = meta.next_table_id;
        meta.next_table_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    #[ cfg(feature="wisckey") ]
    pub async fn next_value_batch_id(&self) -> ValueBatchId {
        let mut meta = self.meta.lock().await;
        let id = meta.next_value_batch_id;
        meta.next_value_batch_id += 1;

        self.sync_header(&*meta).await;

        id
    }

    pub async fn update_table_set(&self, mut add: Vec<(usize, TableId)>, mut remove: Vec<(usize, TableId)>) {
        let mut tables = self.tables.lock().await;

        for (level, id) in add.drain(..) {
            tables.get_mut(level).expect("No such level")
                .insert(id);
        }

        for (level, id) in remove.drain(..) {
            tables.get_mut(level).expect("No such level")
                .remove(&id);
        }

        self.write_table_set(&*tables).await;
    }

    async fn write_table_set(&self, tables: &[LevelData]) {
        let data = bincode::serialize(tables).unwrap();
        let manifest_path = self.params.db_path.join(std::path::Path::new("MANIFEST"));

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

