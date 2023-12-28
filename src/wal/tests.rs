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
