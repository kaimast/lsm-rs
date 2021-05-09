use lsm::{Database, StartMode, KV_Trait, Params};
use tempfile::{Builder, TempDir};

fn test_init<K: KV_Trait, V: KV_Trait>() -> (TempDir, Params, Database<K, V>) {
    let tmp_dir = Builder::new().prefix("lsm-sync-test-").tempdir().unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params{ db_path, ..Default::default() };
    let database = Database::new_with_params(StartMode::CreateOrOverride, params.clone())
        .expect("Failed to create database instance");

    (tmp_dir, params, database)
}


#[test]
fn get_put() {
    let (_tmpdir, params, database) = test_init();

    let key1 = String::from("Foo");
    let key2 = String::from("Foz");
    let value = String::from("Bar");
    let value2 = String::from("Baz");

    assert_eq!(database.get(&key1), None);
    assert_eq!(database.get(&key2), None);

    database.put(&key1, &value).unwrap();
    database.put(&key1, &value2).unwrap();

    drop(database);

    // Reopen
    let database = Database::new_with_params(StartMode::Open, params)
        .expect("Failed to create database instance");

    assert_eq!(database.get(&key1), Some(value.clone()));
    assert_eq!(database.get(&key1), Some(value2.clone()));
}

