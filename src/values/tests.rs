#[cfg(feature = "async-io")]
use tokio_uring::test as async_test;

#[cfg(not(feature = "async-io"))]
use tokio::test as async_test;

use super::*;

use tempfile::{Builder, TempDir};

async fn test_init() -> (TempDir, ValueLog) {
    let tmp_dir = Builder::new()
        .prefix("lsm-value-log-test-")
        .tempdir()
        .unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let params = Params {
        db_path: tmp_dir.path().to_path_buf(),
        ..Default::default()
    };

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    (tmp_dir, ValueLog::new(params, manifest).await)
}

#[async_test]
async fn delete_value() {
    const SIZE: usize = 1_000;

    let (_tmpdir, values) = test_init().await;

    let mut builder = values.make_batch().await;

    let mut data = vec![];
    data.resize(SIZE, b'a');

    let _ = builder.add_value(data.clone()).await;
    let vid2 = builder.add_value(data.clone()).await;

    let batch_id = builder.finish().await.unwrap();

    let batch = values.get_batch(batch_id).await.unwrap();
    assert_eq!(batch.num_active_values(), 2);
    assert_eq!(batch.total_num_values(), 2);

    values.mark_value_deleted(vid2).await.unwrap();
    let batch = values.get_batch(batch_id).await.unwrap();

    assert_eq!(batch.num_active_values(), 1);
    assert_eq!(batch.total_num_values(), 2);
}

#[async_test]
async fn delete_batch() {
    const SIZE: usize = 1_000;

    let (_tmpdir, values) = test_init().await;
    let mut builder = values.make_batch().await;

    let mut data = vec![];
    data.resize(SIZE, b'a');

    let vid = builder.add_value(data).await;

    let batch_id = builder.finish().await.unwrap();
    let batch = values.get_batch(batch_id).await.unwrap();

    assert_eq!(batch.num_active_values(), 1);
    assert_eq!(batch.total_num_values(), 1);

    values.mark_value_deleted(vid).await.unwrap();

    let result = values.get_batch(batch_id).await;
    assert!(result.is_err());
}

#[async_test]
async fn get_put_many() {
    let (_tmpdir, values) = test_init().await;

    let mut builder = values.make_batch().await;
    let mut vids = vec![];

    for pos in 0..1000u32 {
        let value = format!("Number {pos}").into_bytes();
        let vid = builder.add_value(value).await;
        vids.push(vid);
    }

    builder.finish().await.unwrap();

    for (pos, vid) in vids.iter().enumerate() {
        let value = format!("Number {pos}").into_bytes();

        let result = values.get_ref(*vid).await.unwrap();
        assert_eq!(result.get_value(), value);
    }
}

#[async_test]
async fn fold() {
    let (_tmpdir, values) = test_init().await;

    let mut vids = vec![];
    let mut builder = values.make_batch().await;

    for pos in 0..20u32 {
        let value = format!("Number {pos}").into_bytes();
        let vid = builder.add_value(value).await;
        vids.push(vid);
    }

    let batch_id = builder.finish().await.unwrap();

    for value_id in vids.iter().take(19).skip(2) {
        values.mark_value_deleted(*value_id).await.unwrap();
    }

    let batch = values.get_batch(batch_id).await.unwrap();
    assert!(batch.is_folded());
    assert_eq!(batch.num_active_values(), 3);

    for pos in [0u32, 1u32, 19u32] {
        let vid = vids[pos as usize];
        let value = format!("Number {pos}").into_bytes();

        let result = values.get_ref(vid).await.unwrap();
        assert_eq!(result.get_value(), value);
    }
}

#[async_test]
async fn get_put_large_value() {
    let (_tmpdir, values) = test_init().await;

    const SIZE: usize = 1_000_000;
    let mut builder = values.make_batch().await;

    let mut data = vec![];
    data.resize(SIZE, b'a');

    let vid = builder.add_value(data.clone()).await;

    builder.finish().await.unwrap();

    assert!(values.get_ref(vid).await.unwrap().get_value() == data);
}
