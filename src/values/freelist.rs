use std::path::Path;
use std::sync::Arc;

use bitvec::vec::BitVec;

use super::ValueBatchId;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::manifest::Manifest;
use crate::values::ValueId;
use crate::{Error, Params, disk};

pub type FreelistPageId = u64;

/// The minimum valid data block identifier
pub const MIN_FREELIST_PAGE_ID: FreelistPageId = 1;

#[derive(KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
struct FreelistPageHeader {
    identifier: FreelistPageId,
    num_batches: u16,
    num_entries: u16,
}

#[derive(Clone, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
struct FreelistBatchEntry {
    batch_id: ValueBatchId,
    offset: u16,
}

struct FreelistPage {
    header: FreelistPageHeader,

    /// Tracks the value batches covered by this page
    /// and where they start in the bitmap
    batches: Vec<FreelistBatchEntry>,
    entries: BitVec<u8>,
}

impl FreelistPage {
    pub fn new(identifier: FreelistPageId) -> Self {
        Self {
            header: FreelistPageHeader {
                identifier,
                num_batches: 0,
                num_entries: 0,
            },
            batches: Default::default(),
            entries: Default::default(),
        }
    }

    pub async fn open(path: &Path) -> Result<Self, Error> {
        let data = disk::read(path, 0).await?;
        let (header, ref mut data) = FreelistPageHeader::read_from_prefix(&data).unwrap();

        let mut batches = Vec::with_capacity(header.num_batches.into());
        for _ in 0..header.num_batches {
            let (entry, next) = FreelistBatchEntry::read_from_prefix(data).unwrap();
            batches.push(entry.clone());
            *data = next;
        }

        let entries = BitVec::from_slice(data);

        Ok(Self {
            header,
            batches,
            entries,
        })
    }

    pub fn get_identifier(&self) -> FreelistPageId {
        self.header.identifier
    }

    pub fn expand(&mut self, batch_id: ValueBatchId, num_entries: usize) -> bool {
        const MAX_SIZE: usize = 4 * 1024 * 1024;
        assert!(num_entries < MAX_SIZE);

        let current_entries = self.entries.len();
        assert!(current_entries + num_entries < MAX_SIZE);

        let new_size = (self.batches.len() + 1)
            * std::mem::size_of::<(ValueBatchId, u16)>()
            * (current_entries + num_entries);

        if new_size <= MAX_SIZE {
            self.batches.push(FreelistBatchEntry {
                batch_id,
                offset: current_entries as u16,
            });
            self.entries.resize(current_entries + num_entries, true);

            true
        } else {
            false
        }
    }

    pub async fn flush(&self, path: &Path) -> Result<(), Error> {
        let mut data = self.header.as_bytes().to_vec();
        data.extend_from_slice(self.batches.as_bytes());
        data.extend_from_slice(self.entries.as_raw_slice());

        disk::write(path, &data).await?;
        Ok(())
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
        let Ok(start_idx) = self.batches.binary_search_by_key(&batch_id, |e| e.batch_id) else {
            panic!("No such batch in freelist!");
        };

        let start_pos = self.batches[start_idx].offset as usize;
        let end_pos = if start_idx + 1 < self.batches.len() {
            self.batches[start_idx].offset as usize
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
pub struct ValueFreelist {
    params: Arc<Params>,
    manifest: Arc<Manifest>,

    // Assuming a resonable number of entries (<1million)
    // this should never exceed 10mb.
    // So, we simply keep the entire freelist in memory
    pages: RwLock<Vec<(ValueBatchId, FreelistPage)>>,
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
            let min_batch = page
                .batches
                .first()
                .expect("Freelist contains no batches?")
                .batch_id;

            pages.push((min_batch, page));
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
    fn find_page_idx(pages: &[(ValueBatchId, FreelistPage)], batch_id: ValueBatchId) -> usize {
        // We only keep the minimum batch id for every page
        // So the search might not return an exact match
        match pages.binary_search_by_key(&batch_id, |(k, _)| *k) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    panic!("No valid entry for batch id {batch_id}");
                }
                i - 1
            }
        }
    }

    #[inline]
    fn find_page_for_batch<'a>(
        pages: &'a RwLockReadGuard<'_, Vec<(ValueBatchId, FreelistPage)>>,
        batch_id: ValueBatchId,
    ) -> &'a FreelistPage {
        let idx = Self::find_page_idx(pages, batch_id);
        &pages[idx].1
    }

    #[inline]
    fn find_page_for_batch_mut<'a>(
        pages: &'a mut RwLockWriteGuard<'_, Vec<(ValueBatchId, FreelistPage)>>,
        batch_id: ValueBatchId,
    ) -> &'a mut FreelistPage {
        let idx = Self::find_page_idx(pages, batch_id);
        &mut pages[idx].1
    }

    pub async fn get_active_entries(&self, batch_id: ValueBatchId) -> usize {
        let pages = self.pages.read().await;
        Self::find_page_for_batch(&pages, batch_id).get_active_entries(batch_id)
    }

    pub async fn add_batch(&self, batch_id: ValueBatchId, num_entries: usize) -> Result<(), Error> {
        let mut pages = self.pages.write().await;

        loop {
            if let Some((_, p)) = pages.last_mut()
                && p.expand(batch_id, num_entries)
            {
                let path = self.get_page_file_path(&p.get_identifier());
                return p.flush(&path).await;
            }

            // Add a new page
            // (will be flushed in the next iteration)
            let page_id = self.manifest.generate_next_value_freelist_id().await;
            pages.push((batch_id, FreelistPage::new(page_id)));
        }
    }

    pub async fn mark_value_as_deleted(&self, vid: ValueId) {
        let mut pages = self.pages.write().await;
        Self::find_page_for_batch_mut(&mut pages, vid.0).mark_value_as_deleted(vid);
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
            .prefix("lsm-value-log-test-")
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
        let (_, freelist) = test_init().await;

        let batch_id = 1;
        let num_entries = 100;

        freelist.add_batch(batch_id, num_entries).await.unwrap();
        freelist.add_batch(batch_id + 1, num_entries).await.unwrap();

        assert_eq!(freelist.num_pages().await, 2);
        assert_eq!(freelist.get_active_entries(batch_id).await, num_entries);
    }
}
