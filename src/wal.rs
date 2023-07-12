use crate::memtable::Memtable;
use crate::Params;
use crate::WriteOp;

use std::path::Path;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

#[cfg(feature = "async-io")]
use tokio_uring_executor as executor;

#[cfg(feature = "async-io")]
use tokio_uring::buf::BoundedBuf;

#[cfg(feature = "async-io")]
use tokio_uring::fs::{remove_file, File, OpenOptions};

use std::convert::TryInto;

#[cfg(not(feature = "async-io"))]
use std::fs::{remove_file, File, OpenOptions};

#[cfg(not(feature = "async-io"))]
use std::io::{Read, Seek, Write};

use cfg_if::cfg_if;

/// The log is split individual files (pages) that can be
/// garbage collected once the logged data is not needed anymore
const PAGE_SIZE: u64 = 4 * 1024;

/// Invariants:
///  - sync_pos <= write_pos <= queue_pos
///  - flush_pos <= offset_pos
struct LogStatus {
    /// Absolute count of queued write operations
    queue_pos: u64,

    /// Absolute count of fulfilled write operations
    write_pos: u64,

    /// At what write count did we last invoke sync?
    sync_pos: u64,

    /// Pending data to be written
    queue: Vec<Vec<u8>>,

    /// Was a sync requested?
    sync_flag: bool,

    /// Where the current flush offset is
    /// (anything below this is not needed anymore)
    offset_pos: u64,

    /// How much has actually been flushed? (cleaned up)
    flush_pos: u64,
}

struct LogInner {
    status: RwLock<LogStatus>,
    queue_cond: Notify,
    write_cond: Notify,
}

struct WalWriter {
    log_file: File,
    position: u64,
    params: Arc<Params>,
}

/// The write-ahead log keeps track of the most recent changes
/// It can be used to recover from crashes
pub struct WriteAheadLog {
    inner: Arc<LogInner>,
}

impl WalWriter {
    async fn new(params: Arc<Params>) -> Self {
        let log_file = Self::create_file(&params, 0)
            .await
            .expect("Failed to create WAL file");

        Self {
            log_file,
            params,
            position: 0,
        }
    }

    /// Start the writer at a specific position after opening a log
    async fn continue_from(position: u64, params: Arc<Params>) -> Self {
        let log_file = if position % PAGE_SIZE == 0 {
            // At the beginning of a new file
            Self::create_file(&params, 0)
                .await
                .expect("Failed to create WAL file")
        } else {
            let offset = position % PAGE_SIZE;
            Self::open_file(&params, offset)
                .await
                .expect("Failed to create WAL file")
        };

        Self {
            log_file,
            params,
            position,
        }
    }

    async fn write_log(&mut self, inner: Arc<LogInner>) {
        loop {
            let (to_write, sync_flag, new_offset) = {
                let mut lock = inner.status.write().await;
                let to_write = { std::mem::take(&mut lock.queue) };
                let sync_flag = lock.sync_flag;

                let new_offset = if lock.offset_pos > lock.flush_pos {
                    Some((lock.offset_pos, lock.flush_pos))
                } else {
                    assert_eq!(lock.offset_pos, lock.flush_pos);
                    None
                };

                // Nothing to do?
                if to_write.is_empty() && new_offset.is_none() && !sync_flag {
                    assert_eq!(lock.write_pos, lock.queue_pos);

                    // wait for change to queue and retry
                    let fut = inner.queue_cond.notified();
                    tokio::pin!(fut);

                    fut.as_mut().enable();
                    drop(lock);
                    fut.await;

                    continue;
                }

                assert_eq!(self.position, lock.write_pos);

                lock.sync_flag = false;
                (to_write, sync_flag, new_offset)
            };

            // Don't hold lock while write
            for buf in to_write.into_iter() {
                self.write_all(buf).await.expect("Write failed");
            }

            if sync_flag {
                let mut lock = inner.status.write().await;

                // Only sync if necessary
                if lock.sync_pos < self.position {
                    self.sync().await;
                    lock.sync_pos = self.position;
                }
            }

            if let Some((new_offset, old_offset)) = new_offset {
                self.set_offset(new_offset, old_offset).await;
            }

            // Notify about finished write(s)
            {
                let mut lock = inner.status.write().await;
                assert!(lock.write_pos <= self.position);
                lock.write_pos = self.position;

                if let Some((new_offset, _)) = new_offset {
                    lock.flush_pos = new_offset;
                }

                inner.write_cond.notify_waiters();
            }
        }
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
                if #[cfg(feature="async-io") ] {
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
            if #[cfg(feature="async-io") ] {
                self.log_file.sync_data().await
                    .expect("Data sync failed");
            } else {
                self.log_file.sync_data()
                   .expect("Data sync failed");
            }
        }
    }

    #[allow(unused_mut)]
    async fn write_all<'a>(&mut self, mut data: Vec<u8>) -> Result<(), std::io::Error> {
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
                if #[cfg(feature = "async-io")] {
                    let to_write = data.slice(buf_pos..buf_pos + write_len);
                    let (res, buf) = self.log_file.write_all_at(to_write, file_offset).await;
                    res.expect("Failed to write to log file");

                    data = buf.into_inner();
                } else {
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

    async fn create_file(params: &Params, file_pos: u64) -> Result<File, std::io::Error> {
        let fpath = params
            .db_path
            .join(Path::new(&format!("log{:08}.data", file_pos + 1)));
        log::trace!("Creating new log file at {fpath:?}");

        cfg_if! {
            if #[cfg(feature="async-io")] {
                File::create(fpath).await
            } else {
                File::create(fpath)
            }
        }
    }

    async fn open_file(params: &Params, fpos: u64) -> Result<File, std::io::Error> {
        let fpath = params
            .db_path
            .join(Path::new(&format!("log{:08}.data", fpos + 1)));
        log::trace!("Opening file at {fpath:?}");

        cfg_if! {
            if #[cfg(feature="async-io")] {
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

impl WriteAheadLog {
    pub async fn new(params: Arc<Params>) -> Result<Self, std::io::Error> {
        let status = LogStatus {
            queue_pos: 0,
            write_pos: 0,
            sync_pos: 0,
            flush_pos: 0,
            offset_pos: 0,
            queue: vec![],
            sync_flag: false,
        };

        let inner = Arc::new(LogInner {
            status: RwLock::new(status),
            queue_cond: Notify::new(),
            write_cond: Notify::new(),
        });

        cfg_if::cfg_if! {
            if #[cfg(feature = "async-io")] {
                unsafe {
                    let inner = inner.clone();
                    executor::unsafe_spawn(async {
                        let mut writer = WalWriter::new(params).await;
                        writer.write_log(inner).await;
                    });
                }
            } else {
                {
                    let inner = inner.clone();
                    tokio::spawn(async {
                        let mut writer = WalWriter::new(params).await;
                        writer.write_log(inner).await;
                    });
                }
            }
        }

        Ok(Self { inner })
    }

    /// Open an existing log
    pub async fn open(
        params: Arc<Params>,
        start_position: u64,
        memtable: &mut Memtable,
    ) -> Result<Self, std::io::Error> {
        // This reads the file(s) in the current thread because we cannot
        // send stuff between threads easily

        let mut position = start_position;
        let mut count: usize = 0;

        let fpos = position / PAGE_SIZE;

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let mut log_file = WalWriter::open_file(&params, fpos).await?;
            } else {
                let file_offset = position % PAGE_SIZE;
                let mut log_file = WalWriter::open_file(&params, fpos).await?;
                log_file.seek(std::io::SeekFrom::Start(file_offset)).unwrap();
            }
        }

        // Re-insert ops into memtable
        loop {
            let mut op_header = [0u8; 9];
            let success = Self::read_from_log(
                &mut log_file,
                &mut position,
                &mut op_header[..],
                &params,
                true,
            )
            .await?;

            if !success {
                break;
            }

            let op_type = op_header[0];

            let key_data: &[u8; 8] = &op_header[1..].try_into().unwrap();
            let key_len = u64::from_le_bytes(*key_data);

            let mut key = vec![0; key_len as usize];
            Self::read_from_log(&mut log_file, &mut position, &mut key, &params, false).await?;

            if op_type == WriteOp::PUT_OP {
                let mut val_len = [0u8; 8];
                Self::read_from_log(&mut log_file, &mut position, &mut val_len, &params, false)
                    .await?;

                let val_len = u64::from_le_bytes(val_len);
                let mut value = vec![0; val_len as usize];

                Self::read_from_log(&mut log_file, &mut position, &mut value, &params, false)
                    .await?;
                memtable.put(key, value);
            } else if op_type == WriteOp::DELETE_OP {
                memtable.delete(key);
            } else {
                panic!("Unexpected op type!");
            }

            count += 1;
        }

        log::debug!("Found {count} entries in Write-Ahead-Log");

        let status = LogStatus {
            queue_pos: position,
            write_pos: position,
            sync_pos: position,
            flush_pos: start_position,
            offset_pos: start_position,
            queue: vec![],
            sync_flag: false,
        };

        let inner = Arc::new(LogInner {
            status: RwLock::new(status),
            queue_cond: Notify::new(),
            write_cond: Notify::new(),
        });

        cfg_if::cfg_if! {
            if #[cfg(feature = "async-io")] {
                unsafe {
                    let inner = inner.clone();
                    executor::unsafe_spawn(async move {
                        let mut writer = WalWriter::continue_from(position, params).await;
                        writer.write_log(inner).await;
                    });
                }
            } else {
                {
                    let inner = inner.clone();
                    tokio::spawn(async  move {
                        let mut writer = WalWriter::continue_from(position, params).await;
                        writer.write_log(inner).await;
                    });
                }
            }
        }

        Ok(Self { inner })
    }

    async fn read_from_log(
        log_file: &mut File,
        position: &mut u64,
        out: &mut [u8],
        params: &Params,
        maybe: bool,
    ) -> Result<bool, std::io::Error> {
        let start_pos = *position;
        let buffer_len = out.len() as u64;
        let mut buffer_pos = 0;

        while buffer_pos < buffer_len {
            let mut file_offset = *position % PAGE_SIZE;
            let file_remaining = PAGE_SIZE - file_offset;

            assert!(file_remaining > 0);

            let read_len = file_remaining.min(buffer_len - buffer_pos);

            let read_start = buffer_pos as usize;
            let read_end = (read_len + buffer_pos) as usize;

            let read_slice = &mut out[read_start..read_end];

            cfg_if! {
                if #[cfg(feature="async-io")] {
                    let buf = vec![0u8; read_slice.len()];
                    let (read_result, buf) = log_file.read_exact_at(buf, *position).await;
                    read_slice.copy_from_slice(&buf);
                } else {
                    let read_result = log_file.read_exact(read_slice);
                }
            }

            match read_result {
                Ok(_) => {
                    *position += read_len;
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
            buffer_pos = *position - start_pos;

            if file_offset == PAGE_SIZE {
                // Try open next file
                let fpos = *position / PAGE_SIZE;
                *log_file = match WalWriter::open_file(params, fpos).await {
                    Ok(file) => file,
                    Err(err) => {
                        if maybe {
                            *log_file = WalWriter::create_file(params, fpos).await?;
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

    /// Stores an operation and returns the new position in the logfile
    #[tracing::instrument(skip(self))]
    pub async fn store(&mut self, op: &WriteOp) -> Result<u64, std::io::Error> {
        let op_type = op.get_type().to_le_bytes();

        let key = op.get_key();
        let klen = op.get_key_length().to_le_bytes();
        let vlen = op.get_value_length().to_le_bytes();

        let mut data = vec![];
        data.extend_from_slice(op_type.as_slice());
        data.extend_from_slice(klen.as_slice());
        data.extend_from_slice(key);

        match op {
            WriteOp::Put(_, value) => {
                data.extend_from_slice(vlen.as_slice());
                data.extend_from_slice(value);
            }
            WriteOp::Delete(_) => {}
        }

        // Queue write
        let end_pos = {
            let mut lock = self.inner.status.write().await;
            let write_len = data.len() as u64;

            let start_pos = lock.queue_pos;
            lock.queue.push(data);
            lock.queue_pos += write_len;

            self.inner.queue_cond.notify_waiters();

            start_pos + write_len
        };

        // Wait until write has been processed
        loop {
            let lock = self.inner.status.read().await;
            if lock.write_pos >= end_pos {
                break;
            }

            // Wait for next write
            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            fut.as_mut().enable();
            drop(lock);
            fut.await;
        }

        Ok(end_pos)
    }
    pub async fn sync(&mut self) -> Result<(), std::io::Error> {
        let last_pos = {
            let mut lock = self.inner.status.write().await;

            // Nothing to sync?
            if lock.sync_pos == lock.write_pos {
                return Ok(());
            }

            assert!(lock.sync_pos < lock.write_pos);

            lock.sync_flag = true;
            self.inner.queue_cond.notify_waiters();

            lock.sync_pos
        };

        loop {
            let lock = self.inner.status.read().await;
            if lock.sync_pos > last_pos {
                return Ok(());
            }

            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            fut.as_mut().enable();
            drop(lock);
            fut.await;
        }
    }

    pub async fn get_log_position(&self) -> u64 {
        self.inner.status.read().await.write_pos
    }

    /// Once the memtable has been flushed we can remove old log entries
    pub async fn set_offset(&mut self, new_offset: u64) {
        {
            let mut lock = self.inner.status.write().await;

            if new_offset <= lock.offset_pos {
                panic!(
                    "Offset can only be increased! Requested {new_offset}, but was {}",
                    lock.offset_pos
                );
            }

            lock.offset_pos = new_offset;
            self.inner.queue_cond.notify_waiters();
        }

        loop {
            let lock = self.inner.status.read().await;
            if lock.flush_pos >= new_offset {
                return;
            }

            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            fut.as_mut().enable();
            drop(lock);
            fut.await;
        }
    }
}
