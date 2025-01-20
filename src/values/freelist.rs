use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use bitvec::vec::BitVec;

use super::ValueBatchId;
use tokio::sync::{RwLock, RwLockReadGuard};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::manifest::Manifest;
use crate::values::ValueId;
use crate::{Error, Params, disk};

pub type FreelistPageId = u64;

/// The minimum valid data block identifier
pub const MIN_FREELIST_PAGE_ID: FreelistPageId = 1;

#[derive(KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, align(8))]
struct FreelistPageHeader {
    identifier: FreelistPageId,
    start_batch: ValueBatchId,
    num_batches: u64,
    num_entries: u64,
}

/// An (up to) 4kb chunk of the freelist
struct FreelistPage {
    header: FreelistPageHeader,

    /// Tracks the value batches covered by this page
    /// and where they start in the bitmap
    offsets: Vec<u16>,
    entries: BitVec<u8>,
}

impl FreelistPage {
    pub fn new(identifier: FreelistPageId, start_batch: ValueBatchId) -> Self {
        log::trace!("Creating new freelist page with id={identifier}");

        Self {
            header: FreelistPageHeader {
                identifier,
                start_batch,
                num_batches: 0,
                num_entries: 0,
            },
            offsets: Default::default(),
            entries: Default::default(),
        }
    }

    pub async fn open(path: &Path) -> Result<Self, Error> {
        let data = disk::read(path, 0)
            .await
            .map_err(|err| Error::from_io_error("Failed to read freelist page", err))?;

        let (header, ref mut data) = FreelistPageHeader::read_from_prefix(&data).unwrap();

        log::trace!(
            "Opening existing freelist page with id={} path={path:?}",
            header.identifier
        );

        let mut offsets = Vec::with_capacity(header.num_batches as usize);
        for _ in 0..header.num_batches {
            let (entry, next) = u16::read_from_prefix(data).unwrap();
            offsets.push(entry);
            *data = next;
        }

        let entries = BitVec::from_slice(data);

        Ok(Self {
            header,
            offsets,
            entries,
        })
    }

    pub fn get_identifier(&self) -> FreelistPageId {
        self.header.identifier
    }

    pub fn expand(&mut self, num_entries: usize) -> bool {
        const MAX_SIZE: usize = 4 * 1024;
        assert!(num_entries < MAX_SIZE);

        let current_entries = self.entries.len();
        let new_size =
            (self.offsets.len() + 1) * std::mem::size_of::<u16>() + (current_entries + num_entries);

        // Is there enough space?
        if new_size <= MAX_SIZE {
            log::trace!(
                "Adding {num_entries} entries to freelist page #{}",
                self.header.identifier
            );

            self.offsets.push(current_entries as u16);

            // We assume all entries are in use for a new batch
            self.entries.resize(current_entries + num_entries, true);

            true
        } else {
            false
        }
    }

    pub async fn flush(&self, path: &Path) -> Result<(), Error> {
        let mut data = self.header.as_bytes().to_vec();
        data.extend_from_slice(self.offsets.as_bytes());
        data.extend_from_slice(self.entries.as_raw_slice());

        disk::write(path, &data).await.map_err(|err| {
            Error::from_io_error(format!("Failed to write freelist page at `{path:?}`"), err)
        })?;
        Ok(())
    }

    /// Are any of the values in this freelist still in use?
    pub fn is_in_use(&self) -> bool {
        for val in self.entries.iter() {
            if *val {
                return true;
            }
        }
        false
    }

    pub fn get_active_entries(&self, batch_id: ValueBatchId) -> usize {
        let (start_pos, end_pos) = self.get_batch_range(batch_id);
        let mut count = 0;
        for pos in start_pos..end_pos {
            if self.entries[pos] {
                count += 1;
            }
        }

        count
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

    pub fn mark_value_as_deleted(&mut self, vid: ValueId) {
        let (start_pos, end_pos) = self.get_batch_range(vid.0);
        let mut marker = self.entries[start_pos..end_pos]
            .get_mut(vid.1 as usize)
            .expect("Entry index out of range");

        if !*marker {
            panic!("Entry already marked as deleted");
        }

        *marker = false;
    }
}

/// Keeps track of which entries in the value log are still
/// in use.
///
/// This is kept in a separate file to reduce the amount of
/// write amplification caused by value deletion.
/// A single page in the freelist can hold information
/// for up to about 32k values.
///
/// Note: This is technically an "occupied" list.
/// a bit set to 1 means the value is still in use.
pub struct ValueFreelist {
    params: Arc<Params>,
    manifest: Arc<Manifest>,

    // Assuming a resonable number of entries (<1million)
    // this should never exceed 10mb.
    // So, we simply keep the entire freelist in memory
    pages: RwLock<VecDeque<(ValueBatchId, FreelistPage)>>,
}

impl ValueFreelist {
    pub fn new(params: Arc<Params>, manifest: Arc<Manifest>) -> Self {
        Self {
            params,
            manifest,
            pages: Default::default(),
        }
    }

    pub async fn open(params: Arc<Params>, manifest: Arc<Manifest>) -> Result<Self, Error> {
        let obj = Self {
            params,
            manifest,
            pages: Default::default(),
        };

        let mut pages = obj.pages.write().await;
        let max_id = obj.manifest.most_recent_freelist_page_id().await;

        for page_id in MIN_FREELIST_PAGE_ID..=max_id {
            let path = obj.get_page_file_path(&page_id);
            let page = FreelistPage::open(&path).await?;
            let min_batch = page.header.start_batch;

            pages.push_back((min_batch, page));
        }

        drop(pages);
        Ok(obj)
    }

    pub async fn num_pages(&self) -> usize {
        self.pages.read().await.len()
    }

    #[inline]
    fn get_page_file_path(&self, page_id: &FreelistPageId) -> std::path::PathBuf {
        self.params.db_path.join(format!("free{page_id:08}.data"))
    }

    #[inline]
    fn find_page_idx(
        pages: &VecDeque<(ValueBatchId, FreelistPage)>,
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
        pages: &'a RwLockReadGuard<'_, VecDeque<(ValueBatchId, FreelistPage)>>,
        batch_id: ValueBatchId,
    ) -> Option<&'a FreelistPage> {
        let idx = Self::find_page_idx(pages, batch_id)?;
        Some(&pages[idx].1)
    }

    pub async fn get_active_entries(&self, batch_id: ValueBatchId) -> usize {
        let pages = self.pages.read().await;
        match Self::find_page_for_batch(&pages, batch_id) {
            Some(p) => p.get_active_entries(batch_id),
            None => 0,
        }
    }

    pub async fn add_batch(&self, batch_id: ValueBatchId, num_entries: usize) -> Result<(), Error> {
        let mut pages = self.pages.write().await;

        let p = if let Some((_, p)) = pages.back_mut()
            && p.expand(num_entries)
        {
            p
        } else {
            // Add a new page
            let page_id = self.manifest.generate_next_value_freelist_id().await;
            let mut page = FreelistPage::new(page_id, batch_id);
            let success = page.expand(num_entries);
            assert!(success, "Data did not fit in new page?");
            pages.push_back((batch_id, page));
            &pages.back_mut().unwrap().1
        };

        // Persist changes to disk
        let path = self.get_page_file_path(&p.get_identifier());
        p.flush(&path).await?;
        Ok(())
    }

    pub async fn mark_value_as_deleted(&self, vid: ValueId) -> Result<(), Error> {
        let mut pages = self.pages.write().await;
        let page_idx = Self::find_page_idx(&pages, vid.0).expect("Outdated batch?");

        pages[page_idx].1.mark_value_as_deleted(vid);

        // TODO allow gaps as well!
        while page_idx == 0 && !pages[page_idx].1.is_in_use() {
            let id = pages[page_idx].1.get_identifier();
            self.manifest.set_minimum_freelist_page(id).await;

            let fpath = self.get_page_file_path(&id);
            disk::remove_file(&fpath)
                .await
                .map_err(|err| Error::from_io_error("Failed to remove freelist page", err))?;

            pages.pop_front();
        }

        Ok(())
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

    use super::ValueFreelist;

    use crate::manifest::Manifest;
    use crate::params::Params;

    async fn test_init() -> (TempDir, ValueFreelist) {
        let tmp_dir = Builder::new()
            .prefix("lsm-freelist-test-")
            .tempdir()
            .unwrap();
        let _ = env_logger::builder().is_test(true).try_init();

        let params = Params {
            db_path: tmp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let params = Arc::new(params);
        let manifest = Arc::new(Manifest::new(params.clone()).await);

        (tmp_dir, ValueFreelist::new(params, manifest))
    }

    #[async_test]
    async fn add_batch() {
        let (_tmp_dir, freelist) = test_init().await;

        let batch_id = 1;
        let num_entries = 100;

        freelist.add_batch(batch_id, num_entries).await.unwrap();
        freelist.add_batch(batch_id + 1, num_entries).await.unwrap();

        assert_eq!(freelist.num_pages().await, 1);
        assert_eq!(freelist.get_active_entries(batch_id).await, num_entries);
    }

    #[async_test]
    async fn multiple_pages() {
        let (_tmp_dir, freelist) = test_init().await;

        let batch_id = 1;
        let num_entries = 4000;

        freelist.add_batch(batch_id, num_entries).await.unwrap();
        freelist.add_batch(batch_id + 1, num_entries).await.unwrap();

        assert_eq!(freelist.num_pages().await, 2);
        assert_eq!(freelist.get_active_entries(batch_id).await, num_entries);
    }

    #[async_test]
    async fn delete_entry() {
        let (_tmp_dir, freelist) = test_init().await;

        let batch_id = 1;
        let num_entries = 100;

        freelist.add_batch(batch_id, num_entries).await.unwrap();
        freelist.mark_value_as_deleted((batch_id, 2)).await.unwrap();
        freelist
            .mark_value_as_deleted((batch_id, 32))
            .await
            .unwrap();
        freelist
            .mark_value_as_deleted((batch_id, 59))
            .await
            .unwrap();

        assert_eq!(freelist.num_pages().await, 1);
        assert_eq!(freelist.get_active_entries(batch_id).await, num_entries - 3);
    }

    #[async_test]
    async fn remove_page() {
        let (_tmp_dir, freelist) = test_init().await;

        let batch_id = 1;
        let num_entries = 4000;

        freelist.add_batch(batch_id, num_entries).await.unwrap();
        freelist.add_batch(batch_id + 1, num_entries).await.unwrap();

        for idx in 0..num_entries {
            freelist
                .mark_value_as_deleted((batch_id, idx as u32))
                .await
                .unwrap();
        }

        assert_eq!(freelist.num_pages().await, 1);
        assert_eq!(freelist.get_active_entries(batch_id).await, 0);
        assert_eq!(freelist.get_active_entries(batch_id + 1).await, num_entries);
    }
}
