use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use bitvec::vec::BitVec;

use tokio::sync::{RwLock, RwLockReadGuard};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::manifest::Manifest;
use crate::values::ValueId;
use crate::{Error, Params, disk};

use super::{MIN_VALUE_BATCH_ID, ValueBatchId};

pub type ValueIndexPageId = u64;

/// The minimum valid data block identifier
pub const MIN_VALUE_INDEX_PAGE_ID: ValueIndexPageId = 1;

#[derive(KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, align(8))]
struct IndexPageHeader {
    identifier: ValueIndexPageId,
    start_batch: ValueBatchId,
    num_batches: u64,
    num_entries: u64,
}

#[derive(KnownLayout, Immutable, IntoBytes, PartialEq, Eq, Copy, Clone)]
#[repr(u8)]
enum ValueBatchState {
    Active = 0,
    Compacted = 1,
    Deleted = 2,
}

impl TryFrom<u8> for ValueBatchState {
    type Error = ();

    fn try_from(val: u8) -> Result<Self, ()> {
        for option in [Self::Active, Self::Compacted, Self::Deleted] {
            if val == option as u8 {
                return Ok(option);
            }
        }

        Err(())
    }
}

/// An (up to) 4kb chunk of the value index
///
/// Invariants:
///   - offsets.len() == batches.len()
struct IndexPage {
    header: IndexPageHeader,

    /// True if some changes to this page might not
    /// have been written to disk yet
    dirty: bool,

    /// Tracks the value batches covered by this page
    /// and where they start in the bitmap
    offsets: Vec<u16>,

    /// Tracks which batches in this index page have already
    /// been garbage collected or deleted
    batches: Vec<ValueBatchState>,

    /// The actual bitmap
    entries: BitVec<u8>,

    /// Once a page is sealed, no new batches can be added
    /// (not stored on disk)
    sealed: bool,
}

impl IndexPage {
    pub fn new(identifier: ValueIndexPageId, start_batch: ValueBatchId) -> Self {
        log::trace!("Creating new value index page with id={identifier}");

        Self {
            header: IndexPageHeader {
                identifier,
                start_batch,
                num_batches: 0,
                num_entries: 0,
            },
            dirty: true,
            sealed: false,
            offsets: Default::default(),
            batches: Default::default(),
            entries: Default::default(),
        }
    }

    pub async fn open(path: &Path, sealed: bool) -> Result<Self, Error> {
        let data = disk::read(path, 0).await.map_err(|err| {
            Error::from_io_error(
                format!("Failed to read value index page at `{path:?}`"),
                err,
            )
        })?;

        let (header, ref mut data) = IndexPageHeader::read_from_prefix(&data).unwrap();

        log::trace!(
            "Opening existing value index page with id={} path={path:?}",
            header.identifier
        );

        let mut offsets = Vec::with_capacity(header.num_batches as usize);
        for _ in 0..header.num_batches {
            let (entry, next) = u16::read_from_prefix(data).unwrap();
            offsets.push(entry);
            *data = next;
        }

        let len = header.num_batches as usize;
        let batches: Vec<ValueBatchState> = data[..len]
            .iter()
            .map(|s| (*s).try_into().expect("Invalid state"))
            .collect();
        let entries = BitVec::from_slice(&data[len..]);

        Ok(Self {
            header,
            offsets,
            entries,
            batches,
            sealed,
            dirty: false,
        })
    }

    pub fn get_identifier(&self) -> ValueIndexPageId {
        self.header.identifier
    }

    /// Add a new value batch to this page
    ///
    /// - Returns true if there was enoguh space
    /// - Note, this will not update the page until calling sync()
    pub fn expand(&mut self, num_entries: usize) -> bool {
        assert!(!self.sealed);

        const MAX_SIZE: usize = 4 * 1024;
        assert!(num_entries < MAX_SIZE);

        let current_entries = self.entries.len();
        let new_size =
            (self.offsets.len() + 1) * std::mem::size_of::<u16>() + (current_entries + num_entries);

        // Is there enough space?
        if new_size <= MAX_SIZE {
            log::trace!(
                "Adding {num_entries} entries to value index page #{}",
                self.header.identifier
            );

            self.header.num_batches += 1;

            self.offsets.push(current_entries as u16);
            self.batches.push(ValueBatchState::Active);

            // We assume all entries are in use for a new batch
            self.entries.resize(current_entries + num_entries, true);
            self.dirty = true;

            true
        } else {
            self.sealed = true;
            false
        }
    }

    /// Write the current state of the page to the disk
    pub async fn sync(&mut self, path: &Path) -> Result<bool, Error> {
        if !self.dirty {
            return Ok(false);
        }

        log::trace!("Writing with value index page to disk (path={path:?})");

        let mut data = self.header.as_bytes().to_vec();
        data.extend_from_slice(self.offsets.as_bytes());
        data.extend_from_slice(self.batches.as_bytes());
        data.extend_from_slice(self.entries.as_raw_slice());

        disk::write(path, &data).await.map_err(|err| {
            Error::from_io_error(
                format!("Failed to write value index page at `{path:?}`"),
                err,
            )
        })?;

        self.dirty = false;
        Ok(true)
    }

    /// Are any of the values in this page still in use?
    pub fn is_in_use(&self) -> bool {
        for val in self.entries.iter() {
            if *val {
                return true;
            }
        }
        false
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// How many entries in the specified batch are still in use?
    pub fn count_active_entries(&self, batch_id: ValueBatchId) -> usize {
        let (start_pos, end_pos) = self.get_batch_range(batch_id);
        let mut count = 0;
        for pos in start_pos..end_pos {
            if self.entries[pos] {
                count += 1;
            }
        }

        count
    }

    /// The list of all entries (offsets) still in use in
    /// a given batch
    pub fn get_active_entries(&self, batch_id: ValueBatchId) -> Vec<u32> {
        let (start_pos, end_pos) = self.get_batch_range(batch_id);
        self.entries[start_pos..end_pos]
            .iter()
            .enumerate()
            .filter_map(|(offset, active)| {
                if *active {
                    let pos = offset + start_pos;
                    Some(pos as u32)
                } else {
                    None
                }
            })
            .collect()
    }

    #[inline]
    fn get_batch_range(&self, batch_id: ValueBatchId) -> (usize, usize) {
        let start_idx = batch_id
            .checked_sub(self.header.start_batch)
            .expect("Incompatible batch id") as usize;

        let start_pos = self.offsets[start_idx] as usize;
        let end_pos = if let Some(offset) = self.offsets.get(start_idx + 1) {
            *offset as usize
        } else {
            self.entries.len()
        };

        (start_pos, end_pos)
    }

    /// Mark a value as deleted (using its id)
    ///
    /// - Returns the offset within the page that got changed
    /// - Changes will not be persisted until we sync()
    pub fn mark_value_as_deleted(&mut self, vid: ValueId) -> u16 {
        let (start_pos, end_pos) = self.get_batch_range(vid.0);

        let mut marker = self.entries[start_pos..end_pos]
            .get_mut(vid.1 as usize)
            .expect("Entry index out of range");

        if !*marker {
            panic!("Entry already marked as deleted");
        }

        *marker = false;
        self.dirty = true;

        (start_pos as u16) + (vid.1 as u16)
    }

    /// Mark a value as deleted (using its offset within the page)
    pub fn mark_value_as_deleted_at(&mut self, offset: u16) {
        let mut marker = self
            .entries
            .get_mut(offset as usize)
            .expect("Offset out of range");

        if *marker {
            *marker = false;
            self.dirty = true;
        }
    }

    pub fn mark_batch_as_deleted(&mut self, batch_id: ValueBatchId) -> u16 {
        let idx = (batch_id - self.header.start_batch) as u16;
        self.mark_batch_as_deleted_at(idx);
        idx
    }

    pub fn mark_batch_as_deleted_at(&mut self, index: u16) {
        let marker = self.batches.get_mut(index as usize).expect("Out of range?");

        if *marker == ValueBatchState::Deleted {
            panic!("Batch already deleted?");
        }

        *marker = ValueBatchState::Deleted;
    }

    pub fn mark_batch_as_compacted(&mut self, batch_id: ValueBatchId) -> u16 {
        let idx = batch_id - self.header.start_batch;
        let marker = self.batches.get_mut(idx as usize).expect("Out of range?");

        if *marker == ValueBatchState::Compacted {
            panic!("Batch already compacted?");
        }

        if *marker == ValueBatchState::Deleted {
            panic!("Batch already deleted?");
        }

        *marker = ValueBatchState::Deleted;
        idx as u16
    }
}

/// Keeps track of which entries in the value log are still
/// in use.
///
/// This is kept in a separate file to reduce the amount of
/// write amplification caused by value deletion.
/// A single page in the value index can hold information
/// for up to about 30k values.
pub struct ValueIndex {
    params: Arc<Params>,
    manifest: Arc<Manifest>,

    // Assuming a resonable number of entries (<1million)
    // this should never exceed 10mb, so we simply keep
    // the entire value index in memory.
    // For larger database it is safe to assume better hardware
    // with more available memory.
    pages: RwLock<VecDeque<(ValueBatchId, IndexPage)>>,
}

impl ValueIndex {
    pub async fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Result<Self, Error> {
        let obj = Self {
            params,
            manifest,
            pages: Default::default(),
        };

        {
            let mut pages = obj.pages.write().await;
            obj.create_new_page(&mut pages, MIN_VALUE_BATCH_ID).await;
        }

        Ok(obj)
    }

    pub async fn open(params: Arc<Params>, manifest: Arc<Manifest>) -> Result<Self, Error> {
        let obj = Self {
            params,
            manifest,
            pages: Default::default(),
        };

        let mut pages = obj.pages.write().await;

        let min_id = obj.manifest.get_minimum_value_index_page_id().await;
        let max_id = obj.manifest.get_most_recent_value_index_page_id().await;

        for page_id in min_id..=max_id {
            let sealed = page_id < max_id;

            let path = obj.get_page_file_path(&page_id);
            let page = IndexPage::open(&path, sealed).await?;
            let min_batch = page.header.start_batch;

            pages.push_back((min_batch, page));
        }

        drop(pages);
        Ok(obj)
    }

    pub async fn sync(&self) -> Result<(), Error> {
        let mut count = 0;
        let mut pages = self.pages.write().await;

        for (_, page) in pages.iter_mut() {
            if !page.dirty {
                continue;
            }

            let path = self.get_page_file_path(&page.get_identifier());
            let updated = page.sync(&path).await?;
            assert!(updated);
            count += 1;
        }

        log::trace!("Flushed {count} value index pages to disk");
        Ok(())
    }

    pub async fn num_pages(&self) -> usize {
        self.pages.read().await.len()
    }

    #[inline]
    fn get_page_file_path(&self, page_id: &ValueIndexPageId) -> std::path::PathBuf {
        self.params.db_path.join(format!("vindex{page_id:08}.data"))
    }

    #[inline]
    fn find_page_idx(
        pages: &VecDeque<(ValueBatchId, IndexPage)>,
        batch_id: ValueBatchId,
    ) -> Option<usize> {
        // We only keep the minimum batch id for every page
        // So the search might not return an exact match
        match pages.binary_search_by_key(&batch_id, |(k, _)| *k) {
            Ok(i) => Some(i),
            Err(0) => None, // already garbage collected?
            Err(i) => Some(i - 1),
        }
    }

    #[inline]
    fn find_page_for_batch<'a>(
        pages: &'a RwLockReadGuard<'_, VecDeque<(ValueBatchId, IndexPage)>>,
        batch_id: ValueBatchId,
    ) -> Option<&'a IndexPage> {
        let idx = Self::find_page_idx(pages, batch_id)?;
        Some(&pages[idx].1)
    }

    /// Returns the number of active entries for the
    /// specified batch
    pub async fn count_active_entries(&self, batch_id: ValueBatchId) -> usize {
        let pages = self.pages.read().await;
        match Self::find_page_for_batch(&pages, batch_id) {
            Some(p) => p.count_active_entries(batch_id),
            None => 0,
        }
    }

    /// Returns a list of active entries (their offsets)
    /// for the specified batch
    pub async fn get_active_entries(&self, batch_id: ValueBatchId) -> Vec<u32> {
        let pages = self.pages.read().await;
        match Self::find_page_for_batch(&pages, batch_id) {
            Some(p) => p.get_active_entries(batch_id),
            None => vec![],
        }
    }

    pub async fn add_batch(&self, batch_id: ValueBatchId, num_entries: usize) -> Result<(), Error> {
        let mut pages = self.pages.write().await;

        let p = if let Some((_, p)) = pages.back_mut()
            && p.expand(num_entries)
        {
            p
        } else {
            self.create_new_page(&mut pages, batch_id).await;
            let page = &mut pages.back_mut().unwrap().1;
            let success = page.expand(num_entries);
            assert!(success, "Data did not fit in new page?");
            page
        };

        // Persist changes to disk
        let path = self.get_page_file_path(&p.get_identifier());
        p.sync(&path).await?;
        Ok(())
    }

    async fn create_new_page(
        &self,
        pages: &mut VecDeque<(ValueBatchId, IndexPage)>,
        min_batch: ValueBatchId,
    ) {
        let page_id = self.manifest.generate_next_value_index_id().await;

        let page = IndexPage::new(page_id, min_batch);
        pages.push_back((min_batch, page));
    }

    pub async fn mark_batch_as_deleted(
        &self,
        batch_id: ValueBatchId,
    ) -> Result<(ValueIndexPageId, u16), Error> {
        let mut pages = self.pages.write().await;
        let page_idx = Self::find_page_idx(&pages, batch_id).expect("Outdated batch?");

        let page = &mut pages[page_idx].1;
        let offset = page.mark_batch_as_deleted(batch_id);
        Ok((page.get_identifier(), offset))
    }

    /// Mark a batch at the specififed page and offset as
    /// deleted
    ///
    /// Only used during recovery
    pub async fn mark_batch_as_deleted_at(
        &self,
        page_id: ValueIndexPageId,
        index: u16,
    ) -> Result<(), Error> {
        let mut pages = self.pages.write().await;
        match pages.binary_search_by_key(&page_id, |(_, p)| p.get_identifier()) {
            Ok(idx) => pages[idx].1.mark_batch_as_deleted_at(index),
            Err(_) => panic!("No page with id={page_id}"),
        }
        Ok(())
    }

    pub async fn mark_batch_as_compacted(
        &self,
        batch_id: ValueBatchId,
    ) -> Result<(ValueIndexPageId, u16), Error> {
        let mut pages = self.pages.write().await;
        let page_idx = Self::find_page_idx(&pages, batch_id).expect("Outdated batch?");

        let page = &mut pages[page_idx].1;
        let offset = page.mark_batch_as_compacted(batch_id);
        Ok((page.get_identifier(), offset))
    }

    pub async fn mark_value_as_deleted(
        &self,
        vid: ValueId,
    ) -> Result<(ValueIndexPageId, u16), Error> {
        let mut pages = self.pages.write().await;
        let page_idx = Self::find_page_idx(&pages, vid.0).expect("Outdated batch?");

        let page = &mut pages[page_idx].1;
        let offset = page.mark_value_as_deleted(vid);
        let page_id = page.get_identifier();

        // Tries to clean up unused pages
        // This only removes the oldest page(s)
        // and ensures there is always at least one page left
        //
        // TODO allow gaps as well!
        if page_idx == 0 && !page.is_in_use() && page.is_sealed() {
            loop {
                let id = {
                let (_, page) = pages.front().unwrap();
                let id = page.get_identifier();

                // There is always one unsealed page
                if !page.is_sealed() || page.is_in_use() {
                    self.manifest.set_minimum_value_index_page_id(id).await;
                    break;
                }

                    id
                };

                log::trace!("Removing index page with id={id}");
                let fpath = self.get_page_file_path(&id);
                disk::remove_file(&fpath).await.map_err(|err| {
                    Error::from_io_error("Failed to remove value index page", err)
                })?;

                pages.pop_front();
            }
        }

        Ok((page_id, offset))
    }

    pub async fn mark_value_as_deleted_at(&self, page_id: ValueIndexPageId, offset: u16) {
        let mut pages = self.pages.write().await;

        match pages.binary_search_by_key(&page_id, |(_, p)| p.get_identifier()) {
            Ok(idx) => pages[idx].1.mark_value_as_deleted_at(offset),
            Err(_) => panic!("No page with id={page_id}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[cfg(feature = "tokio-uring")]
    use kioto_uring_executor::test as async_test;

    #[cfg(feature = "monoio")]
    use monoio::test as async_test;

    #[cfg(not(feature = "_async-io"))]
    use tokio::test as async_test;

    use tempfile::{Builder, TempDir};

    use super::ValueIndex;

    use crate::manifest::Manifest;
    use crate::params::Params;

    async fn test_init() -> (TempDir, ValueIndex) {
        let tmp_dir = Builder::new()
            .prefix("lsm-value-index-test-")
            .tempdir()
            .unwrap();
        let _ = env_logger::builder().is_test(true).try_init();

        let params = Params {
            db_path: tmp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        (tmp_dir, ValueIndex::new(params, manifest).await.unwrap())
    }

    #[async_test]
    async fn add_batch() {
        let (_tmp_dir, value_index) = test_init().await;

        let batch_id = 1;
        let num_entries = 100;

        value_index.add_batch(batch_id, num_entries).await.unwrap();
        value_index
            .add_batch(batch_id + 1, num_entries)
            .await
            .unwrap();

        assert_eq!(value_index.num_pages().await, 1);
        assert_eq!(
            value_index.count_active_entries(batch_id).await,
            num_entries
        );
    }

    #[async_test]
    async fn multiple_pages() {
        let (_tmp_dir, value_index) = test_init().await;

        let batch_id = 1;
        let num_entries = 4000;

        value_index.add_batch(batch_id, num_entries).await.unwrap();
        value_index
            .add_batch(batch_id + 1, num_entries)
            .await
            .unwrap();

        assert_eq!(value_index.num_pages().await, 2);
        assert_eq!(
            value_index.count_active_entries(batch_id).await,
            num_entries
        );
    }

    #[async_test]
    async fn delete_entry() {
        let (_tmp_dir, value_index) = test_init().await;

        let batch_id = 1;
        let num_entries = 100;

        value_index.add_batch(batch_id, num_entries).await.unwrap();
        value_index
            .mark_value_as_deleted((batch_id, 2))
            .await
            .unwrap();
        value_index
            .mark_value_as_deleted((batch_id, 32))
            .await
            .unwrap();
        value_index
            .mark_value_as_deleted((batch_id, 59))
            .await
            .unwrap();

        assert_eq!(value_index.num_pages().await, 1);
        assert_eq!(
            value_index.count_active_entries(batch_id).await,
            num_entries - 3
        );
    }

    #[async_test]
    async fn remove_page() {
        let (_tmp_dir, value_index) = test_init().await;

        let batch_id = 1;
        let num_entries = 4000;

        value_index.add_batch(batch_id, num_entries).await.unwrap();
        value_index
            .add_batch(batch_id + 1, num_entries)
            .await
            .unwrap();

        for idx in 0..num_entries {
            value_index
                .mark_value_as_deleted((batch_id, idx as u32))
                .await
                .unwrap();
        }

        assert_eq!(value_index.num_pages().await, 1);
        assert_eq!(value_index.count_active_entries(batch_id).await, 0);
        assert_eq!(
            value_index.count_active_entries(batch_id + 1).await,
            num_entries
        );
    }
}
