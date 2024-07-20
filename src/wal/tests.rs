use tempfile::tempdir;

use super::*;

#[cfg(feature = "async-io")]
use tokio_uring_executor::test as async_test;

#[cfg(not(feature = "async-io"))]
use tokio::test as async_test;

#[async_test]
async fn empty_sync() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let wal = WriteAheadLog::new(Arc::new(params)).await.unwrap();

    assert_eq!(wal.inner.status.read().sync_pos, 0);
    assert_eq!(wal.inner.status.read().write_pos, 0);
}

#[async_test]
async fn write_and_sync() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let wal = WriteAheadLog::new(Arc::new(params)).await.unwrap();

    let key = vec![1, 2];
    let value = vec![2, 3];
    wal.store(&[WriteOp::Put(key, value)]).await.unwrap();
    wal.sync().await.unwrap();

    assert_eq!(wal.inner.status.read().sync_pos, 21);
    assert_eq!(wal.inner.status.read().write_pos, 21);
}

#[async_test]
async fn write_large_value() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let wal = WriteAheadLog::new(Arc::new(params)).await.unwrap();

    let key = vec![1, 2];
    let value = vec![1; 2 * (PAGE_SIZE as usize)];
    wal.store(&[WriteOp::Put(key, value)]).await.unwrap();
    wal.sync().await.unwrap();

    assert_eq!(wal.inner.status.read().sync_pos, 8211);
    assert_eq!(wal.inner.status.read().write_pos, 8211);
}

#[async_test]
async fn reopen() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key = vec![1, 2];
    let value = vec![2, 3];
    wal.store(&[WriteOp::Put(key.clone(), value.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();
    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 0, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 21);
    assert_eq!(wal.inner.status.read().write_pos, 21);

    let entry = memtable.get(&key).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());
}

#[async_test]
async fn reopen_with_offset1() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value = vec![2, 3];
    wal.store(&[WriteOp::Put(key1.clone(), value.clone())])
        .await
        .unwrap();
    wal.store(&[WriteOp::Put(key2.clone(), value.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();
    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 21, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 43);
    assert_eq!(wal.inner.status.read().write_pos, 43);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());
}

#[async_test]
async fn reopen_with_offset_and_cleanup1() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value = vec![2, 3];
    wal.store(&[WriteOp::Put(key1.clone(), value.clone())])
        .await
        .unwrap();
    wal.store(&[WriteOp::Put(key2.clone(), value.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();

    wal.set_offset(21).await;
    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 21, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 43);
    assert_eq!(wal.inner.status.read().write_pos, 43);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());
}

#[async_test]
async fn reopen_with_offset_and_cleanup2() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value1 = vec![2; 2 * (PAGE_SIZE as usize)];
    let value2 = vec![2, 3];

    wal.store(&[WriteOp::Put(key1.clone(), value1.clone())])
        .await
        .unwrap();
    wal.store(&[WriteOp::Put(key2.clone(), value2.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();

    wal.set_offset(8211).await;

    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 8211, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 8233);
    assert_eq!(wal.inner.status.read().write_pos, 8233);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value2).as_deref());
}

#[async_test]
async fn reopen_with_offset2() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key1 = vec![1, 2];
    let key2 = vec![1, 2, 3];
    let value1 = vec![2; 2 * (PAGE_SIZE as usize)];
    let value2 = vec![2, 3];

    wal.store(&[WriteOp::Put(key1.clone(), value1.clone())])
        .await
        .unwrap();
    wal.store(&[WriteOp::Put(key2.clone(), value2.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();

    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 8211, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 8233);
    assert_eq!(wal.inner.status.read().write_pos, 8233);

    assert!(memtable.get(&key1).is_none());
    let entry = memtable.get(&key2).unwrap();
    assert_eq!(entry.get_value(), Some(value2).as_deref());
}

#[async_test]
async fn reopen_large_file() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();
    let params = Arc::new(params);

    let wal = WriteAheadLog::new(params.clone()).await.unwrap();

    let key = vec![1, 2];
    let value = vec![2; 2 * (PAGE_SIZE as usize)];
    wal.store(&[WriteOp::Put(key.clone(), value.clone())])
        .await
        .unwrap();
    wal.sync().await.unwrap();
    drop(wal);

    let mut memtable = Memtable::new(0);
    let wal = WriteAheadLog::open(params.clone(), 0, &mut memtable)
        .await
        .unwrap();
    assert_eq!(wal.inner.status.read().sync_pos, 8211);
    assert_eq!(wal.inner.status.read().write_pos, 8211);

    let entry = memtable.get(&key).unwrap();
    assert_eq!(entry.get_value(), Some(value).as_deref());
}
