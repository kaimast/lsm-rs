/// Tests for the write-ahead log, especially its behavior during recovery
use tempfile::TempDir;

use super::*;

#[cfg(feature = "wisckey")]
use crate::{manifest::Manifest, values::ValueIndex};

#[cfg(feature = "tokio-uring")]
use kioto_uring_executor::test as async_test;

#[cfg(feature = "monoio")]
use monoio::test as async_test;

#[cfg(not(feature = "_async-io"))]
use tokio::test as async_test;

async fn test_init() -> (TempDir, Arc<Params>, WriteAheadLog) {
    let _ = env_logger::builder().is_test(true).try_init();

    let tempdir = tempfile::Builder::new()
        .prefix("lsm-wal-test-")
        .tempdir()
        .expect("Failed to create temporary directory");

    log::debug!("Created tempdir at {:?}", tempdir.path());

    let params = Arc::new(Params {
        db_path: tempdir.path().to_path_buf(),
        ..Default::default()
    });

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();
    (tempdir, params, wal)
}

async fn test_cleanup(tempdir: TempDir, wal: WriteAheadLog) {
    // Finish all writes before we stop the tests
    wal.stop().await.expect("WAL sync failed");

    // Ensure that the tempdir is dropped last
    drop(wal);

    log::trace!("Removing tempdir at {:?}", tempdir.path());
    drop(tempdir);
}

#[cfg(feature = "wisckey")]
async fn reopen_wal(params: Arc<Params>, offset: u64) -> (Memtable, WriteAheadLog) {
    let mut memtable = Memtable::new(0);

    let manifest = Arc::new(Manifest::new(params.clone()).await);
    let mut freelist = ValueIndex::new(params.clone(), manifest).await.unwrap();
    let (wal, _) = WriteAheadLog::open(params, offset, &mut memtable, &mut freelist)
        .await
        .unwrap();

    (memtable, wal)
}

#[cfg(not(feature = "wisckey"))]
async fn reopen_wal(params: Arc<Params>, offset: u64) -> (Memtable, WriteAheadLog) {
    let mut memtable = Memtable::new(0);

    let (wal, _) = WriteAheadLog::open(params, offset, &mut memtable)
        .await
        .unwrap();

    (memtable, wal)
}

#[async_test]
async fn empty_sync() {
    let (tempdir, _, wal) = test_init().await;

    assert_eq!(wal.inner.status.read().sync_pos, 0);
    assert_eq!(wal.inner.status.read().write_pos, 0);

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn write_and_sync() {
    let (tempdir, _, wal) = test_init().await;

    let key = vec![1, 2];
    let value = vec![2, 3];
    let op = WriteOp::Put(key.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op)]).await.unwrap();
    wal.sync().await.unwrap();

    assert_eq!(wal.inner.status.read().sync_pos, 22);
    assert_eq!(wal.inner.status.read().write_pos, 22);

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn write_large_value() {
    let (tempdir, _, wal) = test_init().await;

    let key = vec![1, 2];
    let value = vec![1; 2 * PAGE_SIZE];
    let op = WriteOp::Put(key.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op)]).await.unwrap();
    wal.sync().await.unwrap();

    assert_eq!(wal.inner.status.read().sync_pos, 8212);
    assert_eq!(wal.inner.status.read().write_pos, 8212);

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen() {
    let (tempdir, params, wal) = test_init().await;

    let key = vec![1, 2];
    let value = vec![2, 3];
    let op = WriteOp::Put(key.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op)]).await.unwrap();
    wal.sync().await.unwrap();
    drop(wal);

    let (memtable, wal) = reopen_wal(params, 0).await;
    assert_eq!(wal.inner.status.read().sync_pos, 22);
    assert_eq!(wal.inner.status.read().write_pos, 22);

    let entry = memtable.get(&key).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen_with_offset1() {
    let (tempdir, params, wal) = test_init().await;

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value = vec![2, 3];

    let op1 = WriteOp::Put(key1.clone(), value.clone());
    let op2 = WriteOp::Put(key2.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op1)]).await.unwrap();
    wal.store(&[LogEntry::Write(&op2)]).await.unwrap();
    wal.sync().await.unwrap();

    drop(wal);

    let (memtable, wal) = reopen_wal(params, 22).await;

    assert_eq!(wal.inner.status.read().sync_pos, 45);
    assert_eq!(wal.inner.status.read().write_pos, 45);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen_with_offset_and_cleanup1() {
    let (tempdir, params, wal) = test_init().await;

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value = vec![2, 3];

    let op1 = WriteOp::Put(key1.clone(), value.clone());
    let op2 = WriteOp::Put(key2.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op1)]).await.unwrap();
    wal.store(&[LogEntry::Write(&op2)]).await.unwrap();
    wal.sync().await.unwrap();

    let offset = 22;
    wal.set_offset(offset).await;
    drop(wal);

    let (memtable, wal) = reopen_wal(params, offset).await;

    assert_eq!(wal.inner.status.read().sync_pos, 45);
    assert_eq!(wal.inner.status.read().write_pos, 45);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen_with_offset_and_cleanup2() {
    let (tempdir, params, wal) = test_init().await;

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value1 = vec![2; 2 * PAGE_SIZE];
    let value2 = vec![2, 3];

    let op1 = WriteOp::Put(key1.clone(), value1.clone());
    let op2 = WriteOp::Put(key2.clone(), value2.clone());

    wal.store(&[LogEntry::Write(&op1)]).await.unwrap();
    wal.store(&[LogEntry::Write(&op2)]).await.unwrap();
    wal.sync().await.unwrap();

    let offset = 8212;
    wal.set_offset(offset).await;

    drop(wal);

    let (memtable, wal) = reopen_wal(params, offset).await;

    assert_eq!(wal.inner.status.read().sync_pos, 8235);
    assert_eq!(wal.inner.status.read().write_pos, 8235);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value2).as_deref());

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen_with_offset2() {
    let (tempdir, params, wal) = test_init().await;

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value1 = vec![2; 2 * PAGE_SIZE];
    let value2 = vec![2, 3];

    let op1 = WriteOp::Put(key1.clone(), value1.clone());
    let op2 = WriteOp::Put(key2.clone(), value2.clone());

    wal.store(&[LogEntry::Write(&op1)]).await.unwrap();
    wal.store(&[LogEntry::Write(&op2)]).await.unwrap();
    wal.sync().await.unwrap();

    drop(wal);

    let (memtable, wal) = reopen_wal(params, 8212).await;

    assert_eq!(wal.inner.status.read().sync_pos, 8235);
    assert_eq!(wal.inner.status.read().write_pos, 8235);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value2).as_deref());

    test_cleanup(tempdir, wal).await;
}

#[async_test]
async fn reopen_large_file() {
    let (tempdir, params, wal) = test_init().await;

    let key = vec![1, 2];
    let value = vec![2; 2 * PAGE_SIZE];
    let op = WriteOp::Put(key.clone(), value.clone());

    wal.store(&[LogEntry::Write(&op)]).await.unwrap();
    wal.sync().await.unwrap();

    drop(wal);

    let (memtable, wal) = reopen_wal(params, 0).await;

    assert_eq!(wal.inner.status.read().sync_pos, 8212);
    assert_eq!(wal.inner.status.read().write_pos, 8212);

    let entry = memtable.get(&key).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());

    test_cleanup(tempdir, wal).await;
}
