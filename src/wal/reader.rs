use cfg_if::cfg_if;

use std::sync::Arc;

#[cfg(not(feature = "_async-io"))]
use std::io::{Read, Seek};

#[cfg(feature = "wisckey")]
use crate::values::ValueFreelist;

#[cfg(feature = "tokio-uring")]
use tokio_uring::fs::File;

#[cfg(feature = "monoio")]
use monoio::fs::File;

#[cfg(not(feature = "_async-io"))]
use std::fs::File;

use crate::memtable::Memtable;
use crate::{Error, Params};

use super::{LogEntry, PAGE_SIZE, WalWriter, WriteOp};

/// WAL reader used during recovery
pub struct WalReader {
    params: Arc<Params>,
    log_file: File,
    position: u64,
}

impl WalReader {
    pub async fn new(params: Arc<Params>, start_position: u64) -> Result<Self, Error> {
        let position = start_position;
        let fpos = position / PAGE_SIZE;

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let mut log_file = Self::open_file(&params, fpos).await.map_err(|err| Error::from_io_error("Failed to open write-ahead log", err))?;
            } else {
                let file_offset = position % PAGE_SIZE;
                let mut log_file = WalWriter::open_file(&params, fpos).await.map_err(|err| Error::from_io_error("Failed to open write-ahead log", err))?;
                log_file.seek(std::io::SeekFrom::Start(file_offset)).unwrap();
            }
        }

        Ok(Self {
            params,
            log_file,
            position: start_position,
        })
    }

    #[cfg(feature = "wisckey")]
    pub async fn run(
        &mut self,
        memtable: &mut Memtable,
        freelist: &mut ValueFreelist,
    ) -> Result<u64, Error> {
        // Re-insert ops into memtable
        let mut count = 0;

        loop {
            let mut log_type = [0u8; 1];
            let success = self
                .read_from_log(&mut log_type[..], true)
                .await
                .map_err(|err| Error::from_io_error("Failed to read write-ahead log", err))?;

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
    pub async fn run(&mut self, memtable: &mut Memtable) -> Result<u64, Error> {
        // Re-insert ops into memtable
        let mut count = 0;

        loop {
            let mut log_type = [0u8; 1];
            let success = self
                .read_from_log(&mut log_type[..], true)
                .await
                .map_err(|err| Error::from_io_error("Failed to read write-ahead log", err))?;

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
            .await
            .map_err(|err| Error::from_io_error("Failed to read write-ahead log", err))?;

        let op_type = op_header[1];

        let key_len_data: &[u8; KEY_LEN_SIZE] = &op_header[1..].try_into().unwrap();
        let key_len = u64::from_le_bytes(*key_len_data);

        let mut key = vec![0; key_len as usize];
        self.read_from_log(&mut key, false).await.unwrap();

        if op_type == WriteOp::PUT_OP {
            let mut val_len = [0u8; 8];
            self.read_from_log(&mut val_len, false).await.unwrap();

            let val_len = u64::from_le_bytes(val_len);
            let mut value = vec![0; val_len as usize];

            self.read_from_log(&mut value, false).await.unwrap();
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
        self.read_from_log(&mut page_id, false).await.unwrap();
        let page_id = u64::from_le_bytes(page_id);

        let mut offset = [0u8; 2];
        self.read_from_log(&mut offset, false).await.unwrap();
        let offset = u16::from_le_bytes(offset);

        freelist.unset_entry(page_id, offset).await;
        Ok(())
    }

    /// Read the next entry from the log
    /// (only used during recovery)
    ///
    /// TODO: Change this to just fetch an entire page at a time
    async fn read_from_log(&mut self, out: &mut [u8], maybe: bool) -> Result<bool, std::io::Error> {
        let start_pos = self.position;
        let buffer_len = out.len() as u64;
        let mut buffer_pos = 0;

        assert!(buffer_len > 0);

        while buffer_pos < buffer_len {
            let mut file_offset = self.position % PAGE_SIZE;
            let file_remaining = PAGE_SIZE - file_offset;

            assert!(file_remaining > 0);

            let read_len = file_remaining.min(buffer_len - buffer_pos);

            let read_start = buffer_pos as usize;
            let read_end = (read_len + buffer_pos) as usize;

            let read_slice = &mut out[read_start..read_end];

            cfg_if! {
                if #[cfg(feature="_async-io")] {
                    let buf = vec![0u8; read_slice.len()];
                    let (read_result, buf) = log_file.read_exact_at(buf, file_offset).await;
                    if read_result.is_ok() {
                        read_slice.copy_from_slice(&buf);
                    }
                } else {
                    let read_result = self.log_file.read_exact(read_slice);
                }
            }

            match read_result {
                Ok(_) => {
                    self.position += read_len;
                    file_offset += read_len;
                }
                Err(err) => {
                    if maybe {
                        return Ok(false);
                    } else {
                        return Err(err);
                    }
                }
            }

            assert!(file_offset <= PAGE_SIZE);
            buffer_pos = self.position - start_pos;

            if file_offset == PAGE_SIZE {
                // Try to open next file
                let fpos = self.position / PAGE_SIZE;

                self.log_file = match WalWriter::open_file(&self.params, fpos).await {
                    Ok(file) => file,
                    Err(err) => {
                        if maybe {
                            self.log_file = WalWriter::create_file(&self.params, fpos).await?;
                            return Ok(buffer_pos == buffer_len);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
        }

        Ok(true)
    }
}
