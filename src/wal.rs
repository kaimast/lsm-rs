use crate::{Key, Params};
use crate::sorted_table::Value;
use crate::memtable::Memtable;
use crate::WriteOp;

use std::path::Path;
use std::collections::VecDeque;
use std::sync::Arc;
use std::io::IoSlice;

#[ cfg(feature="async-io") ]
use tokio::fs::{OpenOptions, File, remove_file};

#[ cfg(feature="async-io") ]
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use std::convert::TryInto;

#[ cfg(not(feature="async-io")) ]
use std::fs::{OpenOptions, File, remove_file};

#[ cfg(not(feature="async-io")) ]
use std::io::{Read, Seek, Write};

use cfg_if::cfg_if;

const PAGE_SIZE: u64 = 4*1024;

pub struct WriteAheadLog{
    #[ allow(dead_code) ]
    params: Arc<Params>,
    // The current log file
    log_file: File,
    // Everything below offset can be garbage collected
    offset: u64,
    // The absolute position of the log we are at
    // (must be >= offset)
    position: u64,
}

impl WriteAheadLog{
    pub async fn new(params: Arc<Params>) -> Result<Self, String> {
        let log_file = Self::create_file(&*params, 0).await;
        Ok( Self{ params, log_file, offset: 0, position: 0 } )
    }

    pub async fn open(params: Arc<Params>, offset: u64, memtable: &mut Memtable) -> Result<Self, String> {
        let position = offset;
        let mut count: usize = 0;

        let fpos = position / PAGE_SIZE;
        let file_offset = position % PAGE_SIZE;

        let mut log_file = Self::open_file(&*params, fpos).await?;

        cfg_if! {
            if #[cfg(feature="async-io")] {
                log_file.seek(futures::io::SeekFrom::Start(file_offset)).await.unwrap();
            } else {
                log_file.seek(std::io::SeekFrom::Start(file_offset)).unwrap();
            }
        }

        let mut obj = Self{ position, offset, log_file, params };

        // Re-insert ops into memtable
        loop {
            let mut op_header = [0u8; 9];
            let op_type;
            let key_len;

            let success = obj.read_from_log(&mut op_header[..], true).await?;

            if !success {
                break;
            }

            op_type = op_header[0];

            let key_data: &[u8; 8] = &op_header[1..].try_into().unwrap();
            key_len = u64::from_le_bytes(*key_data);

            let mut key = vec![0; key_len as usize];
            obj.read_from_log(&mut key, false).await?;

            if op_type == WriteOp::PUT_OP {
                let mut val_len = [0u8; 8];
                obj.read_from_log(&mut val_len, false).await?;

                let val_len = u64::from_le_bytes(val_len);
                let mut value = vec![0; val_len as usize];

                obj.read_from_log(&mut value, false).await?;
                memtable.put(key, value);

            } else if op_type == WriteOp::DELETE_OP {
                memtable.delete(key);
            } else {
                panic!("Unexpected op type!");
            }

            count += 1;
        }

        log::debug!("Found {} entries in Write-Ahead-Log", count);

        Ok(obj)
    }

    /// Stores an operation and returns the new position in the logfile
    pub async fn store(&mut self, op: &WriteOp) -> u64 {
        // we do not use serde here to avoid copying data

        let op_type = op.get_type().to_le_bytes();

        let key = op.get_key();
        let klen = op.get_key_length().to_le_bytes();
        let vlen = op.get_value_length().to_le_bytes();

        let mut buffers: VecDeque<IoSlice> = vec![
            IoSlice::new(op_type.as_slice()),
            IoSlice::new(klen.as_slice()),
            IoSlice::new(key)
        ].into();

        match op {
            WriteOp::Put(_, value) => {
                buffers.push_back(IoSlice::new(vlen.as_slice()));
                buffers.push_back(IoSlice::new(value));
            },
            WriteOp::Delete(_) => {}
        }

        self.write_all_vectored(buffers).await;
        self.position
    }

    async fn read_from_log(&mut self, out: &mut [u8], maybe: bool) -> Result<bool, String> {
        let start_pos = self.position;
        let buffer_len = out.len() as u64;
        let mut buffer_pos = 0;

        while buffer_pos < buffer_len {
            let mut file_offset = self.position % PAGE_SIZE;
            let file_remaining = PAGE_SIZE - file_offset;

            assert!(file_remaining > 0);

            let read_len = file_remaining.min(buffer_len - buffer_pos);

            let read_start = buffer_pos as usize;
            let read_end = (read_len+buffer_pos) as usize;

            let read_slice = &mut out[read_start..read_end];

            cfg_if! {
                if #[cfg(feature="async-io")] {
                    let read_result = self.log_file.read_exact(read_slice).await;
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
                        return Err(format!("Failed to read log file data: {}", err));
                    }
                }
            }


            assert!(file_offset <= PAGE_SIZE);
            buffer_pos = self.position - start_pos;

            if file_offset == PAGE_SIZE {
                // Try open next file
                let fpos = self.position / PAGE_SIZE;
                self.log_file = match Self::open_file(&*self.params, fpos).await {
                    Ok(file) => file,
                    Err(_) => {
                        if maybe {
                            self.log_file = Self::create_file(&*self.params, fpos).await;
                            return Ok(buffer_pos == buffer_len);
                        } else {
                            return Err("Failed to read log file data: file is missing".to_string());
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    async fn write_all_vectored<'a>(&mut self, mut buffers: VecDeque<IoSlice<'a>>) {
        while !buffers.is_empty() {
            let mut file_offset = self.position % PAGE_SIZE;
            let mut to_write = vec![];
            let mut advance_by = None;

            // Figure out how much we can fit into the current file
            while let Some(buffer) = buffers.pop_front() {
                assert!(!buffer.is_empty());
                assert!(file_offset < PAGE_SIZE);

                let remaining = PAGE_SIZE - file_offset;

                if buffer.len() < (remaining as usize) {
                    to_write.push(buffer);

                    self.position += buffer.len() as u64;
                    file_offset += buffer.len() as u64;

                } else if buffer.len() == (remaining as usize) {
                    to_write.push(buffer);

                    self.position += buffer.len() as u64;
                    file_offset += buffer.len() as u64;
                    break;

                } else {
                    buffers.push_front(buffer);
                    to_write.push(IoSlice::new(&buffers[0][..(remaining as usize)]));

                    advance_by = Some(remaining as usize);

                    self.position += remaining;
                    file_offset += remaining;
                    break;
                }
            }

            if !to_write.is_empty() {
                cfg_if! {
                    if #[ cfg(feature="async-io") ] {
                        // TODO Not supported yet by tokio...
                        // self.log_file.write_all_vectored(&mut to_write)
                        //   .await.expect("Failed to write to log file");

                        for slice in to_write.drain(..) {
                            self.log_file.write_all(&slice[..]).await
                                .expect("Failed to write to log file");
                        }
                    } else {
                        // Try doing one write syscall if possible
                        self.log_file.write_all_vectored(&mut to_write)
                            .expect("Failed to write to log file");
                    }
                }

                if let Some(offset) = advance_by.take() {
                    buffers[0] = IoSlice::advance(&mut [buffers[0]], offset)[0];
                }
            }

            assert!(advance_by.is_none());
            assert!(file_offset <= PAGE_SIZE);

            // Create a new file?
            if file_offset == PAGE_SIZE {
                let file_pos = self.position / PAGE_SIZE;
                self.log_file = Self::create_file(&*self.params, file_pos).await;
            }
        }
    }

    async fn create_file(params: &Params, file_pos: u64) -> File {
        let fpath = params.db_path.join(Path::new(&format!("log{:08}.data", file_pos+1)));
        log::trace!("Creating new log file at {:?}", fpath);

        cfg_if! {
            if #[cfg(feature="async-io")] {
                File::create(fpath).await.unwrap()
            } else {
                File::create(fpath).unwrap()
            }
        }
    }

    async fn open_file(params: &Params, fpos: u64) -> Result<File, String> {
        let fpath = params.db_path.join(Path::new(&format!("log{:08}.data", fpos+1)));
        log::trace!("Opening file at {:?}", fpath);

        cfg_if! {
            if #[cfg(feature="async-io")] {
                match OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false).open(fpath).await {
                    Ok(log_file) => Ok(log_file),
                    Err(err) => {
                        return Err(format!("Failed to open log file: {}", err));
                    }
                }
            } else {
                 match OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false).open(fpath) {
                    Ok(log_file) => Ok(log_file),
                    Err(err) => {
                        return Err(format!("Failed to open log file: {}", err));
                    }
                }
            }
        }
    }

    pub async fn sync(&mut self) {
        cfg_if! {
            if #[cfg(feature="async-io") ] {
                self.log_file.sync_data().await.expect("Failed to sync log file!");
            } else {
                self.log_file.sync_data().expect("Failed to sync log file!");
            }
        }
    }

    pub fn get_log_position(&self) -> u64 {
        self.position
    }

    /// Once the memtable has been flushed we can remove old log entries
    pub async fn set_offset(&mut self, new_offset: u64) {
        if new_offset <= self.offset {
            panic!("Not a valid offset");
        }

        let old_file_pos = self.offset / PAGE_SIZE;
        let new_file_pos = new_offset / PAGE_SIZE;

        for fpos in old_file_pos..new_file_pos {
            let fpath = self.params.db_path.join(Path::new(&format!("log{:08}.data", fpos+1)));
            log::trace!("Removing file {:?}", fpath);

            cfg_if! {
                if #[cfg(feature="async-io") ] {
                    remove_file(fpath).await.expect("Failed to remove log file");
                } else {
                    remove_file(fpath).expect("Failed to remove log file");
                }
            }
        }

        self.offset = new_offset;
    }
}
