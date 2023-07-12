use lsm::{Database, KvTrait, Params, StartMode, WriteOptions};
use tempfile::{Builder, TempDir};

#[cfg(feature = "async-io")]
use tokio_uring_executor::test as async_test;

#[cfg(not(feature = "async-io"))]
use tokio::test as async_test;

async fn test_init<K: KvTrait, V: KvTrait>() -> (TempDir, Params, Database<K, V>) {
    let tmp_dir = Builder::new().prefix("lsm-sync-test-").tempdir().unwrap();
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

    let key1 = String::from("Foo");
    let value1 = String::from("Bar");
    let value2 = String::from("Baz");

    assert_eq!(database.get(&key1).await.unwrap(), None);

    database.put(&key1, &value1).await.unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .await
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).await.unwrap(), Some(value1.clone()));
    database.put(&key1, &value2).await.unwrap();

    drop(database);

    // Reopen again
    let database = Database::new_with_params(StartMode::Open, params)
        .await
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).await.unwrap(), Some(value2.clone()));
}

#[async_test]
async fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, params, database) = test_init().await;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .await
        .expect("Failed to create database instance");

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{pos}"))
        );
    }
}
