use std::sync::Arc;

#[cfg(feature = "wisckey")]
use crate::values::ValueFreelist;

#[cfg(feature = "tokio-uring")]
use tokio_uring::fs::File;

#[cfg(feature = "monoio")]
use monoio::fs::File;

use crate::memtable::Memtable;
use crate::{disk, Error, Params};

use super::{LogEntry, PAGE_SIZE, WalWriter, WriteOp};

/// WAL reader used during recovery
pub struct WalReader {
    params: Arc<Params>,
    position: usize,
    current_page: Vec<u8>,
}

impl WalReader {
    pub async fn new(params: Arc<Params>, start_position: usize) -> Result<Self, Error> {
        let position = start_position;
        let fpos = position / PAGE_SIZE;
        let offset = position % PAGE_SIZE;

        let fpath = WalWriter::get_file_path(&params, fpos);
        let current_page = disk::read_uncompressed(&fpath, offset as u64).await.map_err(|err| Error::from_io_error("Failed to open WAL file", err))?;

        Ok(Self {
            params,
            current_page,
            position: start_position,
        })
    }

    #[cfg(feature = "wisckey")]
    pub async fn run(
        &mut self,
        memtable: &mut Memtable,
        freelist: &mut ValueFreelist,
    ) -> Result<usize, Error> {
        // Re-insert ops into memtable
        let mut count = 0;

        loop {
            let mut log_type = [0u8; 1];
            let success = self
                .read_from_log(&mut log_type[..], true)
                .await?;

            if !success {
                break;
            }

            if log_type[0] == LogEntry::WRITE {
                self.parse_write_entry(memtable).await?
            } else if log_type[0] == LogEntry::VALUE_DELETION {
                self.parse_value_deletion_entry(freelist).await?
            } else {
                panic!("Unexpected log entry type!");
            }

            count += 1;
        }

        log::debug!("Found {count} entries in write-ahead log");
        Ok(self.position)
    }

    #[cfg(not(feature = "wisckey"))]
    pub async fn run(&mut self, memtable: &mut Memtable) -> Result<usize, Error> {
        // Re-insert ops into memtable
        let mut count = 0;

        loop {
            let mut log_type = [0u8; 1];
            let success = self
                .read_from_log(&mut log_type[..], true)
                .await?;

            if !success {
                break;
            }

            if log_type[0] == LogEntry::WRITE {
                self.parse_write_entry(memtable).await?
            } else {
                panic!("Unexpected log entry type!");
            }

            count += 1;
        }

        log::debug!("Found {count} entries in write-ahead log");
        Ok(self.position)
    }

    async fn parse_write_entry(&mut self, memtable: &mut Memtable) -> Result<(), Error> {
        const KEY_LEN_SIZE: usize = std::mem::size_of::<u64>();
        const HEADER_SIZE: usize = std::mem::size_of::<u8>() + KEY_LEN_SIZE;

        let mut op_header = [0u8; HEADER_SIZE];
        self.read_from_log(&mut op_header[..], false)
            .await?;

        let op_type = op_header[0];

        let key_len_data: &[u8; KEY_LEN_SIZE] = &op_header[1..].try_into().unwrap();
        let key_len = u64::from_le_bytes(*key_len_data);

        let mut key = vec![0; key_len as usize];
        self.read_from_log(&mut key, false).await?;

        if op_type == WriteOp::PUT_OP {
            let mut val_len = [0u8; 8];
            self.read_from_log(&mut val_len, false).await?;

            let val_len = u64::from_le_bytes(val_len);
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

    #[cfg(feature = "wisckey")]
    async fn parse_value_deletion_entry(
        &mut self,
        freelist: &mut ValueFreelist,
    ) -> Result<(), Error> {
        let mut page_id = [0u8; 8];
        self.read_from_log(&mut page_id, false).await?;
        let page_id = u64::from_le_bytes(page_id);

        let mut offset = [0u8; 2];
        self.read_from_log(&mut offset, false).await?;
        let offset = u16::from_le_bytes(offset);

        freelist.unset_entry(page_id, offset).await;
        Ok(())
    }

    /// Read the next entry from the log
    /// (only used during recovery)
    ///
    /// TODO: Change this to just fetch an entire page at a time
    async fn read_from_log(&mut self, out: &mut [u8], maybe: bool) -> Result<bool, Error> {
        let buffer_len = out.len();
        let mut buffer_pos: usize = 0;

        assert!(buffer_len > 0);

        while buffer_pos < buffer_len {
            let offset = (self.position % PAGE_SIZE) as usize;
            let remaining = self.current_page.len() - (offset as usize); 
            let len = ((buffer_len - buffer_pos) as usize).min(remaining);

            if len > 0 {
                out[buffer_pos..].copy_from_slice(&self.current_page[offset..offset+len]);
            }

            buffer_pos += len;
            self.position += len;

            // Move to next file?
            if (offset as usize) == PAGE_SIZE {
                // Try to open next file
                let fpos = self.position / PAGE_SIZE;
                let fpath = WalWriter::get_file_path(&self.params, fpos);

                self.current_page = match disk::read_uncompressed(&fpath, 0).await {
                    Ok(file) => file,
                    Err(err) => if maybe {
                        return Ok(false);
                    } else {
                        return Err(Error::from_io_error("Failed to open WAL file", err));
                    }
                }
            }
        }

        Ok(true)
    }
}
