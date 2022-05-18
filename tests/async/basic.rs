use futures::stream::StreamExt;
use tempfile::{Builder, TempDir};

use lsm::{Database, KvTrait, Params, StartMode, WriteBatch, WriteOptions};

const SM: StartMode = StartMode::CreateOrOverride;

async fn test_init<K: KvTrait, V: KvTrait>() -> (TempDir, Database<K, V>) {
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

#[tokio::test]
async fn get_put() {
    let (_tmpdir, database) = test_init().await;

    let key1 = String::from("Foo");
    let key2 = String::from("Foz");
    let value = String::from("Bar");
    let value2 = String::from("Baz");

    assert_eq!(database.get(&key1).await.unwrap(), None);
    assert_eq!(database.get(&key2).await.unwrap(), None);

    database.put(&key1, &value).await.unwrap();

    assert_eq!(database.get(&key1).await.unwrap(), Some(value.clone()));
    assert_eq!(database.get(&key2).await.unwrap(), None);

    database.put(&key1, &value2).await.unwrap();
    assert_eq!(database.get(&key1).await.unwrap(), Some(value2.clone()));

    database.stop().await.unwrap();
}

#[tokio::test]
async fn iterate() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    let mut pos = 0;
    let mut iter = database.iter().await;

    while let Some((key, val)) = iter.next().await {
        assert_eq!(pos as u64, key);
        assert_eq!(format!("some_string_{}", pos), val);

        pos += 1;
    }

    assert_eq!(pos, COUNT);

    database.stop().await.unwrap();
}

#[tokio::test]
async fn range_iterate() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    let mut pos = 0;
    let mut iter = database.range_iter(&300, &10150).await;

    while let Some((key, val)) = iter.next().await {
        let real_pos = pos + 300;
        assert_eq!(real_pos as u64, key);
        assert_eq!(format!("some_string_{}", real_pos), val);

        pos += 1;
    }

    assert_eq!(pos, 9850);

    database.stop().await.unwrap();
}

#[tokio::test]
async fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{}", pos))
        );
    }

    database.stop().await.unwrap();
}

// Use multi-threading to enable background compaction
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_put_delete_large_entry() {
    const SIZE: usize = 1_000_000;

    let (_tmpdir, database) = test_init().await;

    let mut options = WriteOptions::default();
    options.sync = true;

    for _ in 0..10 {
        let mut value = Vec::new();
        value.resize(SIZE, 'a' as u8);

        let value = String::from_utf8(value).unwrap();
        let key: u64 = 424245;

        database.put_opts(&key, &value, &options).await.unwrap();

        assert_eq!(database.get(&key).await.unwrap(), Some(value));

        database.delete(&key).await.unwrap();

        assert_eq!(database.get(&key).await.unwrap(), None);
    }

    database.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_put_delete_many() {
    const COUNT: u64 = 10_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        database.delete(&key).await.unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(database.get(&pos).await.unwrap(), None);
    }

    database.stop().await.unwrap();
}

#[tokio::test]
async fn override_some() {
    const COUNT: u64 = 10_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_other_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_other_string_{}", pos))
        );
    }

    database.stop().await.unwrap();
}

#[tokio::test]
async fn override_many() {
    const NCOUNT: u64 = 200_000;
    const COUNT: u64 = 50_000;

    let (_tmpdir, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..NCOUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_other_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_other_string_{}", pos))
        );
    }

    for pos in COUNT..NCOUNT {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{}", pos))
        );
    }

    database.stop().await.unwrap();
}

#[tokio::test]
async fn batched_write() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init().await;
    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key{}", pos);
        batch.put(&key, &pos);
    }

    database.write(batch).await.unwrap();

    for pos in 0..COUNT {
        let key = format!("key{}", pos);
        assert_eq!(database.get(&key).await.unwrap(), Some(pos));
    }

    database.stop().await.unwrap();
}
