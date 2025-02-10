use futures::stream::StreamExt;
use tempfile::TempDir;

use lsm::{Database, Params, StartMode, WriteBatch, WriteOptions};

const SM: StartMode = StartMode::CreateOrOverride;

#[cfg(feature = "tokio-uring")]
use kioto_uring_executor::test as async_test;

#[cfg(feature = "monoio")]
use monoio::test as async_test;

#[cfg(not(feature = "_async-io"))]
use tokio::test as async_test;

use rand::Rng;

#[cfg(feature = "monoio")]
use monoio::time::{Duration, sleep};

#[cfg(not(feature = "monoio"))]
use tokio::time::{Duration, sleep};

async fn test_init() -> (TempDir, Database) {
    let _ = env_logger::builder().is_test(true).try_init();

    let tmp_dir = tempfile::Builder::new()
        .prefix("lsm-async-test-")
        .tempdir()
        .unwrap();

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
async fn range_iterate_random() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let val = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, val, &options).await.unwrap();
    }

    // Generate random start and end values for iteration within range
    let mut rng = rand::rng();
    let range_start: u64 = rng.random_range(0..COUNT);
    let range_end: u64 = rng.random_range(range_start..COUNT);

    let mut pos = range_start;
    let start = format!("key_{range_start:05}").into_bytes();
    let end = format!("key_{range_end:05}").into_bytes();

    let mut iter = database.range_iter(&start, &end).await;

    while let Some((key, val)) = iter.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, range_end);

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
async fn range_iterate_overlap() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let val = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, val, &options).await.unwrap();
    }

    // Define start and end values for overlapping ranges
    let start1 = "key_02800".to_string().into_bytes();
    let end1 = "key_04000".to_string().into_bytes();
    let start2 = "key_03500".to_string().into_bytes();
    let end2 = "key_05600".to_string().into_bytes();
    let start3 = "key_05500".to_string().into_bytes();
    let end3 = "key_07100".to_string().into_bytes();

    let mut pos = 2800;
    let mut iter1 = database.range_iter(&start1, &end1).await;

    while let Some((key, val)) = iter1.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, 4000);

    pos = 3500;
    let mut iter2 = database.range_iter(&start2, &end2).await;

    while let Some((key, val)) = iter2.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, 5600);

    pos = 5500;
    let mut iter3 = database.range_iter(&start3, &end3).await;

    while let Some((key, val)) = iter3.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 1;
    }

    assert_eq!(pos, 7100);

    database.stop().await.unwrap();
}

#[async_test]
async fn range_iterate_sparse_keys() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    //Insert Sparse Key-Value pairs with a gap of 2500
    for pos in (0..COUNT).step_by(2500) {
        let key = format!("key_{pos:05}").into_bytes();
        let val = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, val, &options).await.unwrap();
    }

    let mut pos = 0;
    let start = "key_00000".to_string().into_bytes();
    let end = "key_20000".to_string().into_bytes();

    let mut iter = database.range_iter(&start, &end).await;

    while let Some((key, val)) = iter.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();

        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

        pos += 2500;
    }

    assert_eq!(pos, 20000);

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

#[async_test]
async fn get_put_many_random() {
    // Generate a random count between 1 and 1000
    let mut rng = rand::rng();
    let count: u64 = rng.random_range(1..=1000);

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..count {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..count {
        let key = format!("key_{pos}").into_bytes();
        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            format!("some_string_{pos}").into_bytes(),
        );
    }

    database.stop().await.unwrap();
}

#[cfg_attr(feature = "monoio", monoio::test(timer_enabled = true))]
#[cfg_attr(not(feature = "monoio"), async_test)]
async fn get_put_many_delay() {
    const COUNT: u64 = 1_000;

    let (_tmpdir, database) = test_init().await;

    // Write with fsync to check persistence
    let options = WriteOptions { sync: true };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    // Introduce a slight delay before reading back the values to test persistence
    sleep(Duration::from_secs(1)).await;

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
#[cfg_attr(feature = "monoio", async_test)]
#[cfg_attr(
    not(feature = "monoio"),
    async_test(flavor = "multi_thread", worker_threads = 4)
)]
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

#[cfg_attr(feature = "monoio", async_test)]
#[cfg_attr(
    not(feature = "monoio"),
    async_test(flavor = "multi_thread", worker_threads = 4)
)]
async fn get_put_delete_variable_entry() {
    let (_tmpdir, database) = test_init().await;

    let options = WriteOptions { sync: true };

    // Test with variable sizes ranging from small to large
    let sizes = vec![1, 500, 1000, 2000];
    for size in sizes {
        let key = format!("key_size_{}", size).into_bytes();
        let mut value = Vec::new();
        value.resize(size, b'a');

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

#[cfg_attr(feature = "monoio", async_test)]
#[cfg_attr(
    not(feature = "monoio"),
    async_test(flavor = "multi_thread", worker_threads = 4)
)]
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
async fn override_one_random() {
    const COUNT: u64 = 1_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    // Modify the value of one specific random key
    let mut rng = rand::rng();
    let random_pos: u64 = rng.random_range(0..1000);
    let modify_key = format!("key_{random_pos}").into_bytes();
    let new_value = format!("some_other_string_{random_pos}").into_bytes();
    database
        .put_opts(modify_key.clone(), new_value.clone(), &options)
        .await
        .unwrap();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = if pos == random_pos {
            format!("some_other_string_{pos}").into_bytes()
        } else {
            format!("some_string_{pos}").into_bytes()
        };

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
async fn override_many_random() {
    const NCOUNT: u64 = 2_000;

    let mut rng = rand::rng();
    let random_count: u64 = rng.random_range(0..2000);

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..NCOUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..random_count {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();

        database.put_opts(key, value, &options).await.unwrap();
    }

    for pos in 0..random_count {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value,
        );
    }

    for pos in random_count..NCOUNT {
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

#[async_test]
async fn batched_overwrite() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init().await;
    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        batch.put(key, value);
    }

    database.write(batch).await.unwrap();

    let mut batch_overwrite = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let new_value = format!("some_other_string_{pos}").into_bytes();

        batch_overwrite.put(key.clone(), new_value);
    }

    database.write(batch_overwrite).await.unwrap();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let expected_value = format!("some_other_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            expected_value
        );
    }

    database.stop().await.unwrap();
}

#[async_test]
async fn batched_overwrite_delete() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init().await;
    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        batch.put(key, value);
    }

    database.write(batch).await.unwrap();

    //Perform overwrite and delete in the same batch
    let mut batch_overwrite_delete = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();

        // Delete every tenth key
        if pos % 10 == 0 {
            batch_overwrite_delete.delete(key.clone());
        }
        // Overwrite every other key
        else {
            let new_value = format!("some_other_string_{pos}").into_bytes();
            batch_overwrite_delete.put(key.clone(), new_value);
        }
    }

    database.write(batch_overwrite_delete).await.unwrap();

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();

        if pos % 10 == 0 {
            assert!(database.get(&key).await.unwrap().is_none());
        } else {
            let expected_value = format!("some_other_string_{pos}").into_bytes();
            assert_eq!(
                database.get(&key).await.unwrap().unwrap().get_value(),
                expected_value
            );
        }
    }
    database.stop().await.unwrap();
}
