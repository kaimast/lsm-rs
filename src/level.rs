use crate::data_blocks::{DataBlocks, DataEntry};
use crate::manifest::{LevelId, Manifest};
use crate::sorted_table::{Key, SortedTable, TableBuilder, TableId};
use crate::{Error, Params};

use std::sync::Arc;
use tokio::sync::RwLock;

use parking_lot::Mutex as PMutex;

/// TODO add slowdown writes trigger
const L0_COMPACTION_TRIGGER: usize = 4;

pub type TableVec = Vec<Arc<SortedTable>>;

pub struct TablePlaceholder {
    min: Key,
    max: Key,
    id: TableId,
}

impl TablePlaceholder {
    fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.max.as_slice() >= min && self.min.as_slice() <= max
    }
}

pub struct Level {
    index: LevelId,
    next_compaction_offset: PMutex<usize>,
    do_seek_based_compaction: bool,
    seek_based_compaction: PMutex<Option<Arc<SortedTable>>>,
    data_blocks: Arc<DataBlocks>,
    tables: RwLock<TableVec>,
    params: Arc<Params>,
    manifest: Arc<Manifest>,
    // Tables in the process of being created
    table_placeholders: RwLock<Vec<TablePlaceholder>>,
}

impl Level {
    pub fn new(
        index: LevelId,
        data_blocks: Arc<DataBlocks>,
        params: Arc<Params>,
        manifest: Arc<Manifest>,
    ) -> Self {
        Self {
            index,
            do_seek_based_compaction: params.seek_based_compaction.is_some(),
            params,
            manifest,
            data_blocks,
            seek_based_compaction: PMutex::new(None),
            next_compaction_offset: PMutex::new(0),
            tables: RwLock::new(vec![]),
            table_placeholders: RwLock::new(vec![]),
        }
    }

    pub async fn remove_table_placeholder(&self, id: TableId) {
        let mut placeholders = self.table_placeholders.write().await;
        for (pos, placeholder) in placeholders.iter().enumerate() {
            if placeholder.id == id {
                placeholders.remove(pos);
                return;
            }
        }

        panic!("no such placeholder");
    }

    pub async fn load_table(&self, id: TableId) -> Result<(), Error> {
        let table = SortedTable::load(id, self.data_blocks.clone(), &self.params).await?;

        let mut tables = self.tables.write().await;
        tables.push(Arc::new(table));

        log::trace!("Loaded table {id} on level {}", self.index);
        Ok(())
    }

    pub async fn build_table(
        &self,
        identifier: TableId,
        min_key: Key,
        max_key: Key,
    ) -> TableBuilder<'_> {
        TableBuilder::new(
            identifier,
            &self.params,
            self.data_blocks.clone(),
            min_key,
            max_key,
        )
    }

    pub async fn add_l0_table(&self, table: SortedTable) {
        let mut tables = self.tables.write().await;
        tables.push(Arc::new(table));
    }

    #[tracing::instrument(skip(self), fields(index=self.index))]
    pub async fn get(&self, key: &[u8]) -> (bool, Option<DataEntry>) {
        let tables = self.tables.read().await;
        let mut compaction_triggered = false;

        // Iterate from back to front (newest to oldest)
        // as L0 may have overlapping entries
        for table in tables.iter().rev() {
            if let Some(entry) = table.get(key).await {
                return (compaction_triggered, Some(entry));
            }

            if self.do_seek_based_compaction && table.has_maximum_seeks() {
                let mut seek_based_compaction = self.seek_based_compaction.lock();
                if seek_based_compaction.is_none() {
                    log::trace!(
                        "Seek-based compaction triggered for table #{}",
                        table.get_id()
                    );
                    *seek_based_compaction = Some(table.clone());
                    compaction_triggered = true;
                }
            }
        }

        (compaction_triggered, None)
    }

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

    pub async fn maybe_start_compaction(&self) -> Result<Option<Vec<Arc<SortedTable>>>, ()> {
        let all_tables = self.tables.write().await;

        let (table, offset) = {
            let mut next_offset = self.next_compaction_offset.lock();

            let size_based_compaction = if self.index == 0 {
                all_tables.len() > L0_COMPACTION_TRIGGER
            } else {
                let mut total_size = 0;

                for t in all_tables.iter() {
                    total_size += t.get_size();
                }

                total_size > self.max_size()
            };

            // Prefer size-based compaction over seek-based compaction
            if size_based_compaction {
                if all_tables.is_empty() {
                    panic!("Cannot start compaction; level {} is empty", self.index);
                }

                if *next_offset >= all_tables.len() {
                    *next_offset = 0;
                }

                let offset = *next_offset;
                let table = all_tables[offset].clone();

                *next_offset += 1;

                (table, offset)
            } else if let Some(table) = self.seek_based_compaction.lock().take() {
                let mut offset = None;

                for (pos, this_table) in all_tables.iter().enumerate() {
                    if this_table.get_id() == table.get_id() {
                        offset = Some(pos);
                        break;
                    }
                }

                if let Some(offset) = offset {
                    (table, offset)
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        };

        // Abort due to concurrency?
        if !table.maybe_start_compaction() {
            return Err(());
        }

        let mut tables = vec![table];
        let mut offsets = vec![offset];

        // Level 0 might have overlapping tables
        if self.index == 0 {
            let mut min = tables[0].get_min().to_vec();
            let mut max = tables[0].get_max().to_vec();

            //TODO how greedy should this be?
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

                    if table.overlaps(&min, &max) && table.maybe_start_compaction() {
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

        Ok(Some(tables))
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_overlaps(
        &self,
        min: &[u8],
        max: &[u8],
        fast_path: Option<TableId>,
    ) -> Option<(TableId, Vec<Arc<SortedTable>>)> {
        let mut overlaps: Vec<Arc<SortedTable>> = Vec::new();
        let tables = self.tables.read().await;

        let mut min = min;
        let mut max = max;

        for table in tables.iter() {
            if table.overlaps(min, max) {
                if !table.maybe_start_compaction() {
                    // Abort
                    for table in overlaps.into_iter() {
                        table.finish_compaction();
                    }
                    return None;
                }

                overlaps.push(table.clone());
                min = table.get_min().min(min);
                max = table.get_max().max(max);
            }
        }

        // set placeholder to avoid race conditions
        let mut placeholders = self.table_placeholders.write().await;

        for placeholder in placeholders.iter() {
            if placeholder.overlaps(min, max) {
                return None;
            }
        }

        let table_id = if let Some(table_id) = fast_path && overlaps.is_empty() {
            table_id
        } else {
            self.manifest.next_table_id().await
        };

        placeholders.push(TablePlaceholder {
            id: table_id,
            min: min.to_vec(),
            max: max.to_vec(),
        });

        Some((table_id, overlaps))
    }

    /// Get a reference to all tables with an exclusive/write lock
    #[inline]
    pub async fn get_tables(&self) -> tokio::sync::RwLockWriteGuard<'_, TableVec> {
        self.tables.write().await
    }

    /// Get a reference to all tables with a read-only lock
    #[inline]
    pub async fn get_tables_ro(&self) -> tokio::sync::RwLockReadGuard<'_, TableVec> {
        self.tables.read().await
    }
}
