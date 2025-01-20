#![allow(clippy::await_holding_lock)]

#[cfg(not(feature = "_async-io"))]
use std::fs::{File, OpenOptions};

#[cfg(not(feature = "_async-io"))]
use std::io::{Read, Seek};

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{Notify, oneshot};

#[cfg(feature = "tokio-uring")]
use tokio_uring::fs::{File, OpenOptions};

#[cfg(feature = "monoio")]
use monoio::fs::{File, OpenOptions};

use cfg_if::cfg_if;

use crate::memtable::Memtable;
use crate::{Error, Params, WriteOp};

mod writer;
use writer::WalWriter;

#[cfg(test)]
mod tests;

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

    /// Indicates the log should shut down
    stop_flag: bool,
}

struct LogInner {
    status: RwLock<LogStatus>,
    queue_cond: Notify,
    write_cond: Notify,
}

/// The write-ahead log keeps track of the most recent changes
/// It can be used to recover from crashes
pub struct WriteAheadLog {
    inner: Arc<LogInner>,
    // Allows to wait for the write to shut down
    finish_receiver: parking_lot::Mutex<Option<oneshot::Receiver<()>>>,
}

impl WriteAheadLog {
    pub async fn new(params: Arc<Params>) -> Result<Self, Error> {
        let status = LogStatus {
            queue_pos: 0,
            write_pos: 0,
            sync_pos: 0,
            flush_pos: 0,
            offset_pos: 0,
            queue: vec![],
            stop_flag: false,
            sync_flag: false,
        };

        let inner = Arc::new(LogInner {
            status: RwLock::new(status),
            queue_cond: Default::default(),
            write_cond: Default::default(),
        });

        let finish_receiver = Self::start_writer(inner.clone(), params);

        Ok(Self {
            inner,
            finish_receiver: parking_lot::Mutex::new(Some(finish_receiver)),
        })
    }

    fn start_writer(inner: Arc<LogInner>, params: Arc<Params>) -> oneshot::Receiver<()> {
        let (finish_sender, finish_receiver) = oneshot::channel();

        let run_writer = async move {
            let mut writer = WalWriter::new(params).await;
            loop {
                let done = writer
                    .update_log(&inner)
                    .await
                    .expect("Write-ahead logging task failed");

                if done {
                    break;
                }
            }
            let _ = finish_sender.send(());
        };

        cfg_if::cfg_if! {
            if #[cfg(feature="monoio")] {
                {
                    monoio::spawn(run_writer);
                }
            } else if #[cfg(feature = "_async-io")] {
                unsafe {
                    kioto_uring_executor::unsafe_spawn(run_writer);
                }
            } else {
                {
                    tokio::spawn(run_writer);
                }
            }
        }

        finish_receiver
    }

    /// Open an existing log
    pub async fn open(
        params: Arc<Params>,
        start_position: u64,
        memtable: &mut Memtable,
    ) -> Result<Self, Error> {
        // This reads the file(s) in the current thread because we cannot
        // send stuff between threads easily

        let mut position = start_position;
        let mut count: usize = 0;

        let fpos = position / PAGE_SIZE;

        cfg_if! {
            if #[cfg(feature="_async-io")] {
                let mut log_file = WalWriter::open_file(&params, fpos).await.map_err(|err| Error::from_io_error("Failed to open write-ahead log", err))?;
            } else {
                let file_offset = position % PAGE_SIZE;
                let mut log_file = WalWriter::open_file(&params, fpos).await.map_err(|err| Error::from_io_error("Failed to open write-ahead log", err))?;
                log_file.seek(std::io::SeekFrom::Start(file_offset)).unwrap();
            }
        }

        // Re-insert ops into memtable
        loop {
            const KEY_LEN_SIZE: usize = std::mem::size_of::<u64>();
            const HEADER_SIZE: usize = std::mem::size_of::<u8>() + KEY_LEN_SIZE;

            let mut op_header = [0u8; HEADER_SIZE];
            let success = Self::read_from_log(
                &mut log_file,
                &mut position,
                &mut op_header[..],
                &params,
                true,
            )
            .await
            .map_err(|err| Error::from_io_error("Failed to read write-ahead log", err))?;

            if !success {
                break;
            }

            let op_type = op_header[0];

            let key_len_data: &[u8; KEY_LEN_SIZE] = &op_header[1..].try_into().unwrap();
            let key_len = u64::from_le_bytes(*key_len_data);

            let mut key = vec![0; key_len as usize];
            Self::read_from_log(&mut log_file, &mut position, &mut key, &params, false)
                .await
                .unwrap();

            if op_type == WriteOp::PUT_OP {
                let mut val_len = [0u8; 8];
                Self::read_from_log(&mut log_file, &mut position, &mut val_len, &params, false)
                    .await
                    .unwrap();

                let val_len = u64::from_le_bytes(val_len);
                let mut value = vec![0; val_len as usize];

                Self::read_from_log(&mut log_file, &mut position, &mut value, &params, false)
                    .await
                    .unwrap();
                memtable.put(key, value);
            } else if op_type == WriteOp::DELETE_OP {
                memtable.delete(key);
            } else {
                panic!("Unexpected op type!");
            }

            count += 1;
        }

        log::debug!("Found {count} entries in write-ahead log");

        let status = LogStatus {
            queue_pos: position,
            write_pos: position,
            sync_pos: position,
            flush_pos: start_position,
            offset_pos: start_position,
            queue: vec![],
            sync_flag: false,
            stop_flag: false,
        };

        let inner = Arc::new(LogInner {
            status: RwLock::new(status),
            queue_cond: Default::default(),
            write_cond: Default::default(),
        });

        let finish_receiver = Self::continue_writer(inner.clone(), position, params);

        Ok(Self {
            inner,
            finish_receiver: parking_lot::Mutex::new(Some(finish_receiver)),
        })
    }

    fn continue_writer(
        inner: Arc<LogInner>,
        position: u64,
        params: Arc<Params>,
    ) -> oneshot::Receiver<()> {
        let (finish_sender, finish_receiver) = oneshot::channel();

        let run_writer = async move {
            let mut writer = WalWriter::continue_from(position, params).await;
            loop {
                let done = writer
                    .update_log(&inner)
                    .await
                    .expect("Write-ahead logging task failed");

                if done {
                    break;
                }
            }
            let _ = finish_sender.send(());
        };

        cfg_if::cfg_if! {
            if #[cfg(feature = "tokio-uring")] {
                unsafe {
                    kioto_uring_executor::unsafe_spawn(run_writer);
                }
            } else if #[cfg(feature="monoio")] {
                {
                    monoio::spawn(run_writer);
                }
            } else {
                {
                    tokio::spawn(run_writer);
                }
            }
        }

        finish_receiver
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

        assert!(buffer_len > 0);

        while buffer_pos < buffer_len {
            let mut file_offset = *position % PAGE_SIZE;
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
                // Try to open next file
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
    #[tracing::instrument(skip(self, batch))]
    pub async fn store(&self, batch: &[WriteOp]) -> Result<u64, Error> {
        let mut writes = vec![];

        for op in batch {
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

            writes.push(data);
        }

        // Queue write
        let end_pos = {
            let mut lock = self.inner.status.write();
            let mut end_pos = lock.queue_pos;

            for data in writes.into_iter() {
                let write_len = data.len() as u64;
                lock.queue.push(data);
                lock.queue_pos += write_len;
                end_pos += write_len;
            }

            self.inner.queue_cond.notify_waiters();
            end_pos
        };

        /*
        // Wait until write has been processed
        let mut status = self.inner.status.read();
        while status.write_pos < end_pos {
            status = self.inner.write_cond.rw_read_wait(status).await;
        }*/

        loop {
            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            {
                let lock = self.inner.status.read();
                if lock.write_pos >= end_pos {
                    break;
                }

                // Wait for next write
                fut.as_mut().enable();
            }
            fut.await;
        }

        Ok(end_pos)
    }

    pub async fn stop(&self) -> Result<(), Error> {
        log::trace!("Shutting down write-ahead log. Waiting for writer to terminate.");

        self.inner.status.write().stop_flag = true;
        self.inner.queue_cond.notify_waiters();

        self.finish_receiver
            .lock()
            .take()
            .expect("Already stopped?")
            .await
            .unwrap();

        log::debug!("Write-ahead log shut down");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn sync(&self) -> Result<(), Error> {
        let last_pos = {
            let mut lock = self.inner.status.write();

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
            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            {
                let lock = self.inner.status.read();

                if lock.sync_pos > last_pos {
                    return Ok(());
                }

                fut.as_mut().enable();
            }
            fut.await;
        }
    }

    /// Once the memtable has been flushed we can remove old log entries
    #[tracing::instrument(skip(self))]
    pub async fn set_offset(&self, new_offset: u64) {
        {
            let mut lock = self.inner.status.write();

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
            // This works around the following bug:
            // https://github.com/rust-lang/rust/issues/63768
            let fut = self.inner.write_cond.notified();
            tokio::pin!(fut);

            {
                let lock = self.inner.status.read();
                if lock.flush_pos >= new_offset {
                    return;
                }
                fut.as_mut().enable();
            }

            fut.await;
        }
    }
}
