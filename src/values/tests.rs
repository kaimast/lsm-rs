#[cfg(feature = "tokio-uring")]
use kioto_uring_executor::test as async_test;

#[cfg(feature = "monoio")]
use monoio::test as async_test;

#[cfg(not(feature = "_async-io"))]
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
async fn delete_batch() {
    const SIZE: usize = 1_000;

    let (_tmpdir, values) = test_init().await;
    let mut builder = values.make_batch().await;

    let key = "hello".as_bytes().to_vec();
    let value = vec![b'a'; SIZE];

    let vid = builder.add_entry(&key, &value).await;

    let batch_id = builder.finish().await.unwrap();
    let batch = values.get_batch(batch_id).await.unwrap();

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
        let key = format!("key_{pos}").as_bytes().to_vec();
        let value = format!("Number {pos}").into_bytes();
        let vid = builder.add_entry(&key, &value).await;
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
async fn get_put_large_value() {
    let (_tmpdir, values) = test_init().await;

    const SIZE: usize = 1_000_000;
    let mut builder = values.make_batch().await;

    let key = "hello".as_bytes().to_vec();
    let data = vec![b'a'; SIZE];

    let vid = builder.add_entry(&key, &data).await;

    builder.finish().await.unwrap();

    assert!(values.get_ref(vid).await.unwrap().get_value() == data);
}
