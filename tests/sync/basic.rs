use lsm::{Database, StartMode, KV_Trait, Params, WriteBatch, WriteOptions};
use tempfile::{Builder, TempDir};

const SM: StartMode = StartMode::CreateOrOverride;

fn test_init<K: KV_Trait, V: KV_Trait>() -> (TempDir, Database<K, V>) {
    let tmp_dir = Builder::new().prefix("lsm-sync-test-").tempdir().unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params{ db_path, ..Default::default() };
    let database = Database::new_with_params(SM, params)
        .expect("Failed to create database instance");

    (tmp_dir, database)
}

#[test]
fn get_put() {
    let (_tmpdir, database) = test_init();

    let key1 = String::from("Foo");
    let key2 = String::from("Foz");
    let value = String::from("Bar");
    let value2 = String::from("Baz");

    assert_eq!(database.get(&key1).unwrap(), None);
    assert_eq!(database.get(&key2).unwrap(), None);

    database.put(&key1, &value).unwrap();

    assert_eq!(database.get(&key1).unwrap(), Some(value.clone()));
    assert_eq!(database.get(&key2).unwrap(), None);

    database.put(&key1, &value2).unwrap();
    assert_eq!(database.get(&key1).unwrap(), Some(value2.clone()));
}

#[test]
fn iterate() {
    const COUNT: u64 = 5_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).unwrap();
    }

    let mut count = 0;

    for (pos, (key, val)) in database.iter().enumerate() {
        assert_eq!(pos as u64, key);
        assert_eq!(val, format!("some_string_{}", pos));

        count += 1;
    }

    assert_eq!(count, COUNT);
}

#[test]
fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(database.get(&pos).unwrap(), Some(format!("some_string_{}", pos)));
    }
}

#[test]
fn get_put_delete_many() {
    const COUNT: u64 = 10_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        database.delete(&key).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(database.get(&pos).unwrap(), None);
    }
}

#[test]
fn override_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_other_string_{}", pos);
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(database.get(&pos).unwrap(), Some(format!("some_other_string_{}", pos)));
    }
}

#[test]
fn batched_write() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init();

    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key{}", pos);
        batch.put(&key, &pos);
    }

    database.write(batch).unwrap();

    for pos in 0..COUNT {
        let key = format!("key{}", pos);
        assert_eq!(database.get(&key).unwrap(), Some(pos));
    }
}
