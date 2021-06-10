use crate::{Error, Params};
use crate::sorted_table::{SortedTable, TableBuilder, TableId, Key};
use crate::manifest::{LevelId, Manifest};
use crate::data_blocks::{DataBlocks, DataEntry};

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};

/// TODO add slowdown writes trigger
const L0_COMPACTION_TRIGGER: usize = 4;

pub type TableVec = Vec<Arc<SortedTable>>;

#[ derive(Debug) ]
pub struct Level {
    index: LevelId,
    #[ allow(clippy::mutex_atomic) ]
    next_compaction: Mutex<usize>,
    data_blocks: Arc<DataBlocks>,
    tables: RwLock<TableVec>,
    params: Arc<Params>,
    manifest: Arc<Manifest>,
}

impl Level {
    pub fn new(index: LevelId,  data_blocks: Arc<DataBlocks>, params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        #[ allow(clippy::mutex_atomic) ]
        Self {
            index, params, manifest,
            next_compaction: Mutex::new(0),
            data_blocks,
            tables: RwLock::new(Vec::new())
        }
    }

    pub async fn load_table(&self, id: TableId) -> Result<(), Error> {
        let table = SortedTable::load(id, self.data_blocks.clone(), &*self.params).await?;

        let mut tables = self.tables.write().await;
        tables.push(Arc::new(table));

        log::trace!("Loaded table {} on level {}", id, self.index);
        Ok(())
    }

    pub async fn build_table(&self, min_key: Key, max_key: Key) -> TableBuilder<'_> {
        let identifier = self.manifest.next_table_id().await;
        TableBuilder::new(identifier, &*self.params, self.data_blocks.clone(), min_key, max_key)
    }

    pub async fn add_l0_table(&self, table: SortedTable) {
        let mut tables = self.tables.write().await;
        tables.push(Arc::new(table));
    }

    pub async fn get(&self, key: &[u8]) -> Option<DataEntry> {
        let tables = self.tables.read().await;

        // Iterate from back to front (newest to oldest)
        // as L0 may have overlapping entries
        for table in tables.iter().rev() {
            if let Some(entry) = table.get(key).await {
                return Some(entry);
            }
        }

        None
    }

    pub async fn get_total_size(&self) -> usize {
        let tables = self.tables.read().await;
        let mut total_size = 0;

        for t in tables.iter() {
            total_size += t.get_size();
        }

        total_size
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        // This doesn't include the size of the values (for now)
        let mut result: usize = 1048576;
        let mut level = self.index;
        while level > 1 {
            result *= 10;
            level -= 1;
        }

        result
    }

    #[inline]
    pub async fn num_tables(&self) -> usize {
        let tables = self.tables.read().await;
        tables.len()
    }

    #[inline]
    pub async fn needs_compaction(&self) -> bool {

        if self.index == 0 {
            self.num_tables().await > L0_COMPACTION_TRIGGER
        } else {
            self.get_total_size().await > self.max_size()
        }
    }

    pub async fn start_compaction(&self) -> (Vec<usize>, Vec<Arc<SortedTable>>) {
        #[ allow(clippy::mutex_atomic) ]
        let mut next_compaction = self.next_compaction.lock().await;
        let all_tables = self.tables.read().await;

        if all_tables.is_empty() {
            panic!("Cannot start compaction; level {} is empty", self.index);
        }

        if *next_compaction >= all_tables.len() {
            *next_compaction = 0;
        }

        let offset = *next_compaction;
        let table = all_tables[offset].clone();
        *next_compaction += 1;

        let mut tables = vec![table];
        let mut offsets = vec![offset];

        // Level 0 might have overlapping tables
        if self.index == 0 {
            let mut min = tables[0].get_min().to_vec();
            let mut max = tables[0].get_max().to_vec();

            let mut change = true;
            while change {
                change = false;
                for (pos, table) in all_tables.iter().enumerate() {
                    let mut found = false;
                    for offset in offsets.iter() {
                        if pos == *offset {
                            found = true;
                            break;
                        }
                    }

                    if found {
                        continue;
                    }

                    if table.overlaps(&min, &max) {
                        min = std::cmp::min(&min[..], table.get_min()).to_vec();
                        max = std::cmp::max(&max[..], table.get_max()).to_vec();

                        offsets.push(pos);
                        tables.push(table.clone());
                        change = true;
                        break;
                    }
                }
            }
        }

        offsets.sort_unstable();
        (offsets, tables)
    }

    pub async fn get_overlaps(&self, min: &[u8], max: &[u8]) -> Vec<(usize, Arc<SortedTable>)> {
        let mut overlaps = Vec::new();
        let tables = self.tables.read().await;

        for (pos, table) in tables.iter().enumerate() {
            if table.overlaps(min, max) {
                overlaps.push((pos, table.clone()));
            }
        }

        overlaps
    }

    #[inline]
    pub async fn get_tables(&self) -> tokio::sync::RwLockWriteGuard<'_, TableVec> {
        self.tables.write().await
    }

    #[inline]
    pub async fn get_tables_ro(&self) -> tokio::sync::RwLockReadGuard<'_, TableVec> {
        self.tables.read().await
    }
}
