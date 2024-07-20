use futures::stream::StreamExt;
use tempfile::{Builder, TempDir};

use lsm::{Database, Params, StartMode, WriteBatch, WriteOptions};

const SM: StartMode = StartMode::CreateOrOverride;

#[cfg(feature = "async-io")]
use tokio_uring_executor::test as async_test;

#[cfg(not(feature = "async-io"))]
use tokio::test as async_test;

async fn test_init() -> (TempDir, Database) {
    let tmp_dir = Builder::new().prefix("lsm-async-test-").tempdir().unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params {
        db_path,
        ..Default::default()
    };
    let database = Database::new_with_params(SM, params)
        .await
        .expect("Failed to create database instance");

    (tmp_dir, database)
}

#[async_test]
async fn get_put() {
    let (_tmpdir, database) = test_init().await;

    let key1 = String::from("Foo").into_bytes();
    let key2 = String::from("Foz").into_bytes();
    let value1 = String::from("Bar").into_bytes();
    let value2 = String::from("Baz").into_bytes();

    assert!(database.get(&key1).await.unwrap().is_none());
    assert!(database.get(&key2).await.unwrap().is_none());

    database.put(key1.clone(), value1.clone()).await.unwrap();

    assert_eq!(
        database.get(&key1).await.unwrap().unwrap().get_value(),
        value1
    );
    assert!(database.get(&key2).await.unwrap().is_none());

    database.put(key1.clone(), value2.clone()).await.unwrap();
    assert_eq!(
        database.get(&key1).await.unwrap().unwrap().get_value(),
        value2
    );

    database.stop().await.unwrap();
}

#[async_test]
async fn iterate() {
    const COUNT: u64 = 2500;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    let mut pos = 0;
    let mut iter = database.iter().await;

    while let Some((key, val)) = iter.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, COUNT);

    database.stop().await.unwrap();
}

#[async_test]
async fn range_iterate() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let val = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, val, &options).await.unwrap();
    }

    let mut pos = 0;
    let start = "key_00300".to_string().into_bytes();
    let end = "key_10150".to_string().into_bytes();

    let mut iter = database.range_iter(&start, &end).await;

    while let Some((key, val)) = iter.next().await {
        let real_pos = pos + 300;
        let expected_key = format!("key_{real_pos:05}").into_bytes();
        let expected_val = format!("some_string_{real_pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, 9850);

    database.stop().await.unwrap();
}

#[async_test]
async fn range_iterate_reverse() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    let mut pos = 0;
    let start = "key_10150".to_string().into_bytes();
    let end = "key_00300".to_string().into_bytes();

    let mut iter = database.reverse_range_iter(&start, &end).await;

    while let Some((key, val)) = iter.next().await {
        let real_pos = 10150 - pos;

        let expected_key = format!("key_{real_pos:05}").into_bytes();
        let expected_val = format!("some_string_{real_pos}").into_bytes();
        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, 9850);

    database.stop().await.unwrap();
}

#[async_test]
async fn range_iterate_empty() {
    let (_tmpdir, database) = test_init().await;

    const COUNT: u64 = 500;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    // Pick a range that is outside of the put range
    let start = "key_05300".to_string().into_bytes();
    let end = "key_10150".to_string().into_bytes();

    let mut iter = database.range_iter(&start, &end).await;

    if iter.next().await.is_some() {
        panic!("Found a key where there should be none");
    }

    database.stop().await.unwrap();
}

#[async_test]
async fn get_put_many() {
    const COUNT: u64 = 1_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            format!("some_string_{pos}").into_bytes(),
        );
    }

    database.stop().await.unwrap();
}

// Use multi-threading to enable background compaction
#[async_test(flavor = "multi_thread", worker_threads = 4)]
async fn get_put_delete_large_entry() {
    const SIZE: usize = 1000;

    let (_tmpdir, database) = test_init().await;

    let options = WriteOptions { sync: true };

    for _ in 0..10 {
        let key = "key_424245".to_string().into_bytes();

        let mut value = Vec::new();
        value.resize(SIZE, b'a');

        database
            .put_opts(key.clone(), value.clone(), &options)
            .await
            .unwrap();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value
        );

        database.delete(key.clone()).await.unwrap();

        assert!(database.get(&key).await.unwrap().is_none());
    }

    database.stop().await.unwrap();
}

#[async_test(flavor = "multi_thread", worker_threads = 4)]
async fn get_put_delete_many() {
    const COUNT: u64 = 1_003;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        database.delete(key).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        assert!(database.get(&key).await.unwrap().is_none());
    }

    database.stop().await.unwrap();
}

#[async_test]
async fn override_some() {
    const COUNT: u64 = 1_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value,
        );
    }

    database.stop().await.unwrap();
}

#[async_test]
async fn override_many() {
    const NCOUNT: u64 = 2_000;
    const COUNT: u64 = 501;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..NCOUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();

        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value,
        );
    }

    for pos in COUNT..NCOUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value,
        );
    }

    database.stop().await.unwrap();
}

#[async_test]
async fn batched_write() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init().await;
    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key{pos}").into_bytes();
        let value = format!("value{pos}").into_bytes();

        batch.put(key, value);
    }

    database.write(batch).await.unwrap();

    for pos in 0..COUNT {
        let key = format!("key{pos}").into_bytes();
        let value = format!("value{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value
        );
    }

    database.stop().await.unwrap();
}
