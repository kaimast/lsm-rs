use std::path::Path;
use std::sync::Arc;

#[cfg(not(feature = "_async-io"))]
use std::fs::{File, remove_file};

#[cfg(not(feature = "_async-io"))]
use std::io::Write;

#[cfg(feature = "tokio-uring")]
use tokio_uring::{
    buf::BoundedBuf,
    fs::{File, remove_file},
};

#[cfg(feature = "monoio")]
use monoio::{buf::IoBuf, fs::File};

#[cfg(feature = "monoio")]
use std::fs::remove_file;

use cfg_if::cfg_if;

use crate::Params;
use crate::wal::{LogInner, OpenOptions, PAGE_SIZE};

/// The task that actually writes the log to disk
pub struct WalWriter {
    log_file: File,
    position: u64,
    params: Arc<Params>,
}

impl WalWriter {
    pub async fn new(params: Arc<Params>) -> Self {
        let log_file = Self::create_file(&params, 0).await.unwrap_or_else(|err| {
            panic!(
                "Failed to create WAL file in directory {:?}: {err}",
                params.db_path
            )
        });

        Self {
            log_file,
            params,
            position: 0,
        }
    }

    /// Start the writer at a specific position after opening a log
    pub async fn continue_from(position: u64, params: Arc<Params>) -> Self {
        let fpos = position / PAGE_SIZE;

        let log_file = if position % PAGE_SIZE == 0 {
            // At the beginning of a new file
            Self::create_file(&params, fpos)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to create WAL file in directory {:?}: {err}",
                        params.db_path
                    )
                })
        } else {
            Self::open_file(&params, fpos).await.unwrap_or_else(|err| {
                panic!(
                    "Failed to open WAL file in directory {:?}: {err}",
                    params.db_path
                )
            })
        };

        Self {
            log_file,
            params,
            position,
        }
    }

    /// Returns true if the writer is done and the associated task should terminate
    pub async fn update_log(&mut self, inner: &LogInner) -> bool {
        let (to_write, sync_flag, sync_pos, new_offset, stop_flag) = loop {
            // This works around the following bug:
            // https://github.com/rust-lang/rust/issues/63768
            let fut = inner.queue_cond.notified();
            tokio::pin!(fut);

            {
                let mut lock = inner.status.write();
                let to_write = std::mem::take(&mut lock.queue);
                let sync_flag = lock.sync_flag;
                let sync_pos = lock.sync_pos;
                let stop_flag = lock.stop_flag;

                let new_offset = if lock.offset_pos > lock.flush_pos {
                    Some((lock.offset_pos, lock.flush_pos))
                } else {
                    assert_eq!(lock.offset_pos, lock.flush_pos);
                    None
                };

                // Check whether there is something to do
                if !to_write.is_empty() || new_offset.is_some() || sync_flag || stop_flag {
                    assert_eq!(self.position, lock.write_pos);

                    lock.sync_flag = false;
                    break (to_write, sync_flag, sync_pos, new_offset, stop_flag);
                }

                // wait for change to queue and retry
                assert_eq!(lock.write_pos, lock.queue_pos);
                fut.as_mut().enable();
            }

            fut.await;
        };

        // Don't hold lock while write
        for buf in to_write.into_iter() {
            self.write_all(buf).await.expect("Write failed");
        }

        // Only sync if necessary
        // We do not need to hold the lock while syncing
        // because there is only one write-ahead writer
        if sync_flag && sync_pos < self.position {
            self.sync().await;
            inner.status.write().sync_pos = self.position;
        }

        if let Some((new_offset, old_offset)) = new_offset {
            self.set_offset(new_offset, old_offset).await;
        }

        // Notify about finished write(s)
        {
            let mut lock = inner.status.write();
            assert!(lock.write_pos <= self.position);
            lock.write_pos = self.position;

            if let Some((new_offset, _)) = new_offset {
                lock.flush_pos = new_offset;
            }

            inner.write_cond.notify_waiters();
        }

        if stop_flag {
            log::debug!("WAL writer finished");
        }

        stop_flag
    }

    async fn set_offset(&mut self, new_offset: u64, old_offset: u64) {
        let old_file_pos = old_offset / PAGE_SIZE;
        let new_file_pos = new_offset / PAGE_SIZE;

        for fpos in old_file_pos..new_file_pos {
            let fpath = self
                .params
                .db_path
                .join(Path::new(&format!("log{:08}.data", fpos + 1)));
            log::trace!("Removing file {fpath:?}");

            cfg_if! {
                if #[cfg(feature="tokio-uring")] {
                    remove_file(&fpath).await
                        .unwrap_or_else(|err| {
                            panic!("Failed to remove log file {fpath:?}: {err}");
                        });
                } else {
                    remove_file(&fpath)
                        .unwrap_or_else(|err| {
                            panic!("Failed to remove log file {fpath:?}: {err}");
                        });
                }
            }
        }
    }

    async fn sync(&mut self) {
        cfg_if! {
            if #[cfg(feature="_async-io") ] {
                self.log_file.sync_data().await
                    .expect("Data sync failed");
            } else {
                self.log_file.sync_data()
                   .expect("Data sync failed");
            }
        }
    }

    #[allow(unused_mut)]
    async fn write_all(&mut self, mut data: Vec<u8>) -> Result<(), std::io::Error> {
        let mut buf_pos = 0;
        while buf_pos < data.len() {
            let mut file_offset = self.position % PAGE_SIZE;

            // Figure out how much we can fit into the current file
            assert!(file_offset < PAGE_SIZE);

            let page_remaining = PAGE_SIZE - file_offset;
            let buffer_remaining = data.len() - buf_pos;
            let write_len = (buffer_remaining).min(page_remaining as usize);

            assert!(write_len > 0);
            cfg_if! {
                if #[cfg(feature="tokio-uring")] {
                    let to_write = data.slice(buf_pos..buf_pos + write_len);
                    let (res, buf) = self.log_file.write_all_at(to_write, file_offset).await;
                    res.expect("Failed to write to log file");

                    data = buf.into_inner();
                } else if #[cfg(feature="monoio")] {
                    let to_write = data.slice(buf_pos..buf_pos + write_len);
                    let (res, buf) = self.log_file.write_all_at(to_write, file_offset).await;
                    res.expect("Failed to write to log file");

                    data = buf.into_inner();


                }else {
                    let to_write = &data[buf_pos..buf_pos + write_len];
                    self.log_file.write_all(to_write).expect("Failed to write log file");
                }
            }

            buf_pos += write_len;
            self.position += write_len as u64;
            file_offset += write_len as u64;

            assert!(file_offset <= PAGE_SIZE);

            // Create a new file?
            if file_offset == PAGE_SIZE {
                let file_pos = self.position / PAGE_SIZE;
                self.log_file = Self::create_file(&self.params, file_pos).await?;
            }
        }

        Ok(())
    }

    /// Create a new file that is part of the log
    pub async fn create_file(params: &Params, file_pos: u64) -> Result<File, std::io::Error> {
        let fpath = params
            .db_path
            .join(Path::new(&format!("log{:08}.data", file_pos + 1)));
        log::trace!("Creating new log file at {fpath:?}");

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                File::create(fpath).await
            } else {
                File::create(fpath)
            }
        }
    }

    /// Open an existing log file (used during recovery)
    pub async fn open_file(params: &Params, fpos: u64) -> Result<File, std::io::Error> {
        let fpath = params
            .db_path
            .join(Path::new(&format!("log{:08}.data", fpos + 1)));
        log::trace!("Opening file at {fpath:?}");

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let log_file = OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(fpath).await?;
            } else {
                 let log_file = OpenOptions::new()
                    .read(true).write(true).create(false).truncate(false)
                    .open(fpath)?;
            }
        }

        Ok(log_file)
    }
}
