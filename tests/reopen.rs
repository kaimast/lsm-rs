use lsm::{Database, Params, StartMode, WriteOptions};
use tempfile::{Builder, TempDir};

use futures::stream::StreamExt;

#[cfg(feature = "tokio-uring")]
use tokio_uring_executor::test as async_test;

#[cfg(feature = "monoio")]
use monoio::test as async_test;

#[cfg(not(feature = "_async-io"))]
use tokio::test as async_test;

async fn test_init() -> (TempDir, Params, Database) {
    let tmp_dir = Builder::new()
        .prefix("lsm-async-test-reopen-")
        .tempdir()
        .unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params {
        db_path,
        ..Default::default()
    };
    let database = Database::new_with_params(StartMode::CreateOrOverride, params.clone())
        .await
        .expect("Failed to create database instance");

    (tmp_dir, params, database)
}

#[async_test]
async fn get_put() {
    let (_tmpdir, params, database) = test_init().await;

    let key1 = String::from("Foo").into_bytes();
    let value1 = String::from("Bar").into_bytes();
    let value2 = String::from("Baz").into_bytes();

    assert!(database.get(&key1).await.unwrap().is_none());

    database.put(key1.clone(), value1.clone()).await.unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .await
        .expect("Failed to create database instance");

    assert_eq!(
        database.get(&key1).await.unwrap().unwrap().get_value(),
        value1
    );
    database.put(key1.clone(), value2.clone()).await.unwrap();

    drop(database);

    // Reopen again
    let database = Database::new_with_params(StartMode::Open, params)
        .await
        .expect("Failed to create database instance");

    assert_eq!(
        database.get(&key1).await.unwrap().unwrap().get_value(),
        value2
    );
}

#[async_test]
async fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, params, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).await.unwrap();
    }

    database.synchronize().await.unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .await
        .expect("Failed to create database instance");

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();

        assert_eq!(
            database.get(&key).await.unwrap().unwrap().get_value(),
            value,
        );
    }

    // Ensure iteration still works
    let mut iterator = database.iter().await;
    let mut pos = 0;
    while let Some((key, value)) = iterator.next().await {
        assert_eq!(format!("key_{pos:05}").into_bytes(), key);
        assert_eq!(format!("some_string_{pos}").into_bytes(), value.get_value());
        pos += 1;
    }
}

#[async_test]
async fn get_put_large() {
    const COUNT: usize = 100;
    const SIZE: usize = 100_000;

    let (_tmpdir, params, database) = test_init().await;

    // Write without fsync to speed up tests
    let options = WriteOptions { sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("value_{pos}").repeat(SIZE).into_bytes();

        database.put_opts(key, value, &options).await.unwrap();
    }

    database.synchronize().await.unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .await
        .expect("Failed to create database instance");

    let mut iterator = database.iter().await;
    let mut pos = 0;

    while let Some((key, value)) = iterator.next().await {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_value = format!("value_{pos}").repeat(SIZE).into_bytes();

        assert_eq!(expected_key, key);
        // Value is very long, so don't print
        assert!(expected_value == value.get_value());

        pos += 1;
    }
}
