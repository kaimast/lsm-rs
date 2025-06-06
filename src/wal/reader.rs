use std::sync::Arc;

use zerocopy::FromBytes;

#[cfg(feature = "wisckey")]
use crate::values::{ValueBatchId, ValueIndex};

use crate::memtable::Memtable;
use crate::{Error, Params, disk};

use super::{LogEntryType, PAGE_SIZE, WalWriter, WriteOp};

/// WAL reader used during recovery
pub struct WalReader {
    params: Arc<Params>,
    position: usize,
    current_page: Vec<u8>,
}

#[derive(Default)]
pub struct RecoveryResult {
    pub new_position: usize,
    pub entries_recovered: usize,
    #[cfg(feature = "wisckey")]
    pub value_batches_to_delete: Vec<ValueBatchId>,
}

impl WalReader {
    pub async fn new(params: Arc<Params>, start_position: usize) -> Result<Self, Error> {
        let position = start_position;
        let fpos = position / PAGE_SIZE;

        let fpath = WalWriter::get_file_path(&params, fpos);
        log::trace!("Opening next log file at {fpath:?}");

        let current_page = disk::read_uncompressed(&fpath, 0)
            .await
            .map_err(|err| Error::from_io_error("Failed to open WAL file", err))?;

        Ok(Self {
            params,
            current_page,
            position,
        })
    }

    #[cfg(feature = "wisckey")]
    pub async fn run(
        &mut self,
        memtable: &mut Memtable,
        value_index: &mut ValueIndex,
    ) -> Result<RecoveryResult, Error> {
        let mut result = RecoveryResult::default();

        // Re-insert ops into memtable
        loop {
            let mut log_type = [0u8; 1];
            let success = self.read_from_log(&mut log_type[..], true).await?;

            if !success {
                break;
            }

            if log_type[0] == LogEntryType::Write as u8 {
                self.parse_write_entry(memtable).await?
            } else if log_type[0] == LogEntryType::DeleteValue as u8 {
                self.parse_value_deletion_entry(value_index).await?
            } else if log_type[0] == LogEntryType::DeleteBatch as u8 {
                self.parse_batch_deletion_entry(value_index).await?
            } else {
                panic!("Unexpected log entry type! {}", log_type[0]);
            }

            result.entries_recovered += 1;
        }

        log::debug!(
            "Found {} entries in write-ahead log",
            result.entries_recovered
        );
        result.new_position = self.position;
        Ok(result)
    }

    #[cfg(not(feature = "wisckey"))]
    pub async fn run(&mut self, memtable: &mut Memtable) -> Result<RecoveryResult, Error> {
        let mut result = RecoveryResult::default();

        // Re-insert ops into memtable
        loop {
            let mut log_type = [0u8; 1];
            let success = self.read_from_log(&mut log_type[..], true).await?;

            if !success {
                break;
            }

            if log_type[0] == LogEntryType::Write as u8 {
                self.parse_write_entry(memtable).await?
            } else {
                panic!("Unexpected log entry type!");
            }

            result.entries_recovered += 1;
        }

        log::debug!(
            "Found {} entries in write-ahead log",
            result.entries_recovered
        );
        result.new_position = self.position;
        Ok(result)
    }

    async fn parse_write_entry(&mut self, memtable: &mut Memtable) -> Result<(), Error> {
        let op_type: u8 = self.read_value().await?;
        let key_len: u64 = self.read_value().await?;

        let mut key = vec![0; key_len as usize];
        self.read_from_log(&mut key, false).await?;

        if op_type == WriteOp::PUT_OP {
            let val_len: u64 = self.read_value().await?;
            let mut value = vec![0; val_len as usize];
            self.read_from_log(&mut value, false).await?;
            memtable.put(key, value);
        } else if op_type == WriteOp::DELETE_OP {
            memtable.delete(key);
        } else {
            panic!("Unexpected op type!");
        }

        Ok(())
    }

    /// Fetches a value from the current position at the log
    /// and advances the position
    ///
    /// This might open the next page of the log, if needed
    async fn read_value<T: Sized + FromBytes>(&mut self) -> Result<T, Error> {
        let mut data = vec![0u8; std::mem::size_of::<T>()];
        self.read_from_log(&mut data, false).await?;
        Ok(T::read_from_bytes(&data).unwrap())
    }

    #[cfg(feature = "wisckey")]
    async fn parse_value_deletion_entry(
        &mut self,
        value_index: &mut ValueIndex,
    ) -> Result<(), Error> {
        let page_id = self.read_value().await?;
        let offset = self.read_value().await?;
        value_index.mark_value_as_deleted_at(page_id, offset).await;

        Ok(())
    }

    #[cfg(feature = "wisckey")]
    async fn parse_batch_deletion_entry(
        &mut self,
        value_index: &mut ValueIndex,
    ) -> Result<(), Error> {
        let page_id = self.read_value().await?;
        let offset = self.read_value().await?;
        value_index
            .mark_batch_as_deleted_at(page_id, offset)
            .await?;
        Ok(())
    }

    /// Read the next entry from the log
    /// (only used during recovery)
    ///
    /// TODO: Change this to just fetch an entire page at a time
    async fn read_from_log(&mut self, out: &mut [u8], maybe: bool) -> Result<bool, Error> {
        let buffer_len = out.len();
        let mut buffer_pos = 0;
        assert!(buffer_len > 0);

        while buffer_pos < buffer_len {
            let offset = self.position % PAGE_SIZE;
            let file_remaining = self
                .current_page
                .len()
                .checked_sub(offset)
                .expect("Invalid offset. Page too small?");
            let buffer_remaining = buffer_len - buffer_pos;

            let len = buffer_remaining.min(file_remaining);

            if len > 0 {
                out[buffer_pos..buffer_pos + len]
                    .copy_from_slice(&self.current_page[offset..offset + len]);
                buffer_pos += len;
                self.position += len;
            } else if self.position % PAGE_SIZE != 0 {
                log::trace!(
                    "WAL reader is done. Current file was not full; assuming it is the most recent."
                );
                assert!(self.current_page.len() < PAGE_SIZE);
                return Ok(false);
            }

            // Move to next file?
            if self.position % PAGE_SIZE == 0 {
                let fpos = self.position / PAGE_SIZE;
                let fpath = WalWriter::get_file_path(&self.params, fpos);
                log::trace!("Opening next log file at {fpath:?}");

                self.current_page = match disk::read_uncompressed(&fpath, 0).await {
                    Ok(data) => data,
                    Err(err) => {
                        if maybe && err.kind() == std::io::ErrorKind::NotFound {
                            // At last file but it is still exactly
                            // one page
                            log::trace!("WAL reader is done. No next log file found");
                            return Ok(false);
                        } else {
                            return Err(Error::from_io_error("Failed to open WAL file", err));
                        }
                    }
                }
            }
        }

        Ok(true)
    }
}
