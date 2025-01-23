#![allow(clippy::await_holding_lock)]

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, oneshot};

#[cfg(feature = "wisckey")]
use crate::values::{FreelistPageId, ValueFreelist};

use crate::memtable::Memtable;
use crate::{Error, Params, WriteOp};

mod writer;
use writer::WalWriter;

mod reader;
use reader::WalReader;

#[cfg(test)]
mod tests;

/// In the vanilla configuration, the log only stores
/// write operations.
/// For Wisckey, it also stores changes to the freelist
/// to reduce write amplification.
pub enum LogEntry<'a> {
    Write(&'a WriteOp),
    #[cfg(feature = "wisckey")]
    CompactBatch(FreelistPageId, u16),
    #[cfg(feature = "wisckey")]
    ValueDeletion(FreelistPageId, u16),
}

impl LogEntry<'_> {
    const WRITE: u8 = 0;
    #[cfg(feature = "wisckey")]
    const VALUE_DELETION: u8 = 1;
    #[cfg(feature = "wisckey")]
    const COMPACT_BATCH: u8 = 2;

    pub fn get_type(&self) -> u8 {
        match self {
            Self::Write(_) => Self::WRITE,
            #[cfg(feature = "wisckey")]
            Self::ValueDeletion(_, _) => Self::VALUE_DELETION,
            #[cfg(feature = "wisckey")]
            Self::CompactBatch(_, _) => Self::COMPACT_BATCH,
        }
    }
}

/// The log is split individual files (pages) that can be
/// garbage collected once the logged data is not needed anymore
const PAGE_SIZE: usize = 4 * 1024;

/// The state of the log (internal DS shared between writer
/// task and WAL object)
///
/// Invariants:
///  - sync_pos <= write_pos <= queue_pos
///  - flush_pos <= offset_pos
struct LogStatus {
    /// Absolute count of queued write operations
    queue_pos: usize,

    /// Absolute count of fulfilled write operations
    write_pos: usize,

    /// At what write count did we last invoke sync?
    sync_pos: usize,

    /// Pending data to be written
    queue: Vec<Vec<u8>>,

    /// Was a sync requested?
    sync_flag: bool,

    /// Where the current flush offset is
    /// (anything below this is not needed anymore)
    offset_pos: usize,

    /// How much has actually been flushed? (cleaned up)
    flush_pos: usize,

    /// Indicates the log should shut down
    stop_flag: bool,
}

impl LogStatus {
    fn new(position: usize, start_position: usize) -> Self {
        Self {
            queue_pos: position,
            write_pos: position,
            sync_pos: position,
            flush_pos: start_position,
            offset_pos: start_position,
            queue: vec![],
            sync_flag: false,
            stop_flag: false,
        }
    }
}

struct LogInner {
    status: RwLock<LogStatus>,
    queue_cond: Notify,
    write_cond: Notify,
}

impl LogInner {
    fn new(status: LogStatus) -> Self {
        Self {
            status: RwLock::new(status),
            queue_cond: Default::default(),
            write_cond: Default::default(),
        }
    }
}

/// The write-ahead log keeps track of the most recent changes
/// It can be used to recover from crashes
pub struct WriteAheadLog {
    inner: Arc<LogInner>,

    /// Allows waiting for the background write task to shut down
    finish_receiver: Mutex<Option<oneshot::Receiver<()>>>,
}

impl WriteAheadLog {
    /// Creates a new and empty write-ahead log
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
            finish_receiver: Mutex::new(Some(finish_receiver)),
        })
    }

    /// Open an existing log and insert entries into memtable
    ///
    /// This is similar to `new` but fetches state from disk first.
    #[cfg(feature = "wisckey")]
    pub async fn open(
        params: Arc<Params>,
        start_position: u64,
        memtable: &mut Memtable,
        freelist: &mut ValueFreelist,
    ) -> Result<Self, Error> {
        // This reads the file(s) in the current thread
        // because we cannot send stuff between threads easily

        let start_position = start_position as usize;

        let mut reader = WalReader::new(params.clone(), start_position).await?;

        let position = reader.run(memtable, freelist).await?;

        let status = LogStatus::new(position, start_position);
        let inner = Arc::new(LogInner::new(status));
        let finish_receiver = Self::continue_writer(inner.clone(), position, params);

        Ok(Self {
            inner,
            finish_receiver: Mutex::new(Some(finish_receiver)),
        })
    }

    #[cfg(not(feature = "wisckey"))]
    pub async fn open(
        params: Arc<Params>,
        start_position: u64,
        memtable: &mut Memtable,
    ) -> Result<Self, Error> {
        // This reads the file(s) in the current thread
        // because we cannot send stuff between threads easily

        let start_position = start_position as usize;

        let mut reader = WalReader::new(params.clone(), start_position).await?;

        let position = reader.run(memtable).await?;

        let status = LogStatus::new(position, start_position);
        let inner = Arc::new(LogInner::new(status));
        let finish_receiver = Self::continue_writer(inner.clone(), position, params);

        Ok(Self {
            inner,
            finish_receiver: Mutex::new(Some(finish_receiver)),
        })
    }

    /// Spawns the background task that will actually write
    /// to the WAL.
    ///
    /// There is exactly one task that writes to the log
    /// so that we have to worry about ordering less.
    fn start_writer(inner: Arc<LogInner>, params: Arc<Params>) -> oneshot::Receiver<()> {
        let (finish_sender, finish_receiver) = oneshot::channel();

        let run_writer = async move {
            let mut writer = WalWriter::new(params).await;
            let mut done = false;

            while !done {
                done = writer
                    .update_log(&inner)
                    .await
                    .expect("Write-ahead logging task failed");
            }
            let _ = finish_sender.send(());
        };

        cfg_if::cfg_if! {
            if #[cfg(feature="monoio")] {
                monoio::spawn(run_writer);
            } else if #[cfg(feature="tokio-uring")] {
                unsafe {
                    kioto_uring_executor::unsafe_spawn(run_writer);
                }
            } else {
                tokio::spawn(run_writer);
            }
        }

        finish_receiver
    }

    /// Start the background task that writes to the log
    ///
    /// This is similar to `start_writer`, but when we open
    /// an existing log.
    fn continue_writer(
        inner: Arc<LogInner>,
        position: usize,
        params: Arc<Params>,
    ) -> oneshot::Receiver<()> {
        let (finish_sender, finish_receiver) = oneshot::channel();

        let run_writer = async move {
            let mut writer = WalWriter::continue_from(position, params).await;
            let mut done = false;

            while !done {
                done = writer
                    .update_log(&inner)
                    .await
                    .expect("Write-ahead logging task failed");
            }
            let _ = finish_sender.send(());
        };

        cfg_if::cfg_if! {
            if #[cfg(feature = "tokio-uring")] {
                unsafe {
                    kioto_uring_executor::unsafe_spawn(run_writer);
                }
            } else if #[cfg(feature="monoio")] {
                monoio::spawn(run_writer);
            } else {
                tokio::spawn(run_writer);
            }
        }

        finish_receiver
    }

    /// Stores an operation and returns the new position in
    /// the logfile
    #[tracing::instrument(skip(self, batch))]
    pub async fn store(&self, batch: &[LogEntry<'_>]) -> Result<u64, Error> {
        let mut writes = vec![];

        for entry in batch {
            let mut data = vec![entry.get_type()];

            match entry {
                LogEntry::Write(op) => {
                    let op_type = op.get_type().to_le_bytes();

                    let key = op.get_key();
                    let klen = op.get_key_length().to_le_bytes();
                    let vlen = op.get_value_length().to_le_bytes();

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
                #[cfg(feature = "wisckey")]
                LogEntry::ValueDeletion(page_id, offset) => {
                    let page_id = page_id.to_le_bytes();
                    let offset = offset.to_le_bytes();

                    data.extend_from_slice(page_id.as_slice());
                    data.extend_from_slice(offset.as_slice());

                    writes.push(data);
                }
                #[cfg(feature = "wisckey")]
                LogEntry::CompactBatch(page_id, offset) => {
                    let page_id = page_id.to_le_bytes();
                    let offset = offset.to_le_bytes();

                    data.extend_from_slice(page_id.as_slice());
                    data.extend_from_slice(offset.as_slice());

                    writes.push(data);
                }
            }
        }

        // Queue write
        let end_pos = {
            let mut lock = self.inner.status.write();
            let mut end_pos = lock.queue_pos;

            for data in writes.into_iter() {
                let write_len = data.len();
                lock.queue.push(data);
                lock.queue_pos += write_len;
                end_pos += write_len;
            }

            self.inner.queue_cond.notify_waiters();
            end_pos
        };

        // Wait until write has been processed
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

        Ok(end_pos as u64)
    }

    /// Gracefully stop the write-ahead log
    ///
    /// This is intended to only be called during shutdown
    /// and shall be called exactly once.
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
        let new_offset = new_offset as usize;

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
