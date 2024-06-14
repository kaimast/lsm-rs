use lsm_sync::{Database, KvTrait, Params, StartMode, WriteOptions};
use tempfile::{Builder, TempDir};

fn test_init<K: KvTrait, V: KvTrait>() -> (TempDir, Params, Database<K, V>) {
    let tmp_dir = Builder::new()
        .prefix("lsm-sync-test-reopen-")
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
        .expect("Failed to create database instance");

    (tmp_dir, params, database)
}

#[test]
fn get_put() {
    let (_tmpdir, params, database) = test_init();

    let key1 = String::from("Foo");
    let value1 = String::from("Bar");
    let value2 = String::from("Baz");

    assert_eq!(database.get(&key1).unwrap(), None);

    database.put(&key1, &value1).unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).unwrap(), Some(value1.clone()));
    database.put(&key1, &value2).unwrap();

    drop(database);

    // Reopen again
    let database = Database::new_with_params(StartMode::Open, params)
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).unwrap(), Some(value2.clone()));
}

#[test]
fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, params, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .expect("Failed to create database instance");

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).unwrap(),
            Some(format!("some_string_{pos}"))
        );
    }
}
