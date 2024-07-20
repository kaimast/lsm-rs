use lsm_sync::{Database, Params, StartMode, WriteOptions};
use tempfile::{Builder, TempDir};

fn test_init() -> (TempDir, Params, Database) {
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

    let key1 = String::from("Foo").into_bytes();
    let value1 = String::from("Bar").into_bytes();
    let value2 = String::from("Baz").into_bytes();

    assert!(database.get(&key1).unwrap().is_none());

    database.put(key1.clone(), value1.clone()).unwrap();
    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).unwrap().unwrap().get_value(), value1.clone());
    database.put(key1.clone(), value2.clone()).unwrap();

    drop(database);

    // Reopen again
    let database = Database::new_with_params(StartMode::Open, params)
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1).unwrap().unwrap().get_value(), value2);
}

#[test]
fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, params, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params.clone())
        .expect("Failed to create database instance");

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
 
        assert_eq!(
            database.get(&key).unwrap().unwrap().get_value(),
            value.as_slice(),
        );
    }
}
