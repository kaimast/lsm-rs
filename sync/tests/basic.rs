use lsm_sync::{Database, KvTrait, Params, StartMode, WriteBatch, WriteOptions};
use tempfile::{Builder, TempDir};

const SM: StartMode = StartMode::CreateOrOverride;

fn test_init<K: KvTrait, V: KvTrait>() -> (TempDir, Database<K, V>) {
    let tmp_dir = Builder::new().prefix("lsm-sync-test-").tempdir().unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params {
        db_path,
        ..Default::default()
    };
    let database =
        Database::new_with_params(SM, params).expect("Failed to create database instance");

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
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    let mut count = 0;

    for (pos, (key, val)) in database.iter().enumerate() {
        assert_eq!(pos as u64, key);
        assert_eq!(val, format!("some_string_{pos}"));

        count += 1;
    }

    assert_eq!(count, COUNT);
}

#[test]
fn range_iterate() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    let mut pos = 0;
    let mut iter = database.range_iter(&300, &10150);

    while let Some((key, val)) = iter.next() {
        let real_pos = pos + 300;
        assert_eq!(real_pos as u64, key);
        assert_eq!(format!("some_string_{real_pos}"), val);

        pos += 1;
    }

    assert_eq!(pos, 9850);

    database.stop().unwrap();
}

#[test]
fn range_iterate_reverse() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    let mut pos = 0;
    let mut iter = database.reverse_range_iter(&10150, &300);

    while let Some((key, val)) = iter.next() {
        let real_pos = 10150 - pos;
        assert_eq!(real_pos as u64, key);
        assert_eq!(format!("some_string_{real_pos}"), val);

        pos += 1;
    }

    assert_eq!(pos, 9850);

    database.stop().unwrap();
}

#[test]
fn range_iterate_empty() {
    let (_tmpdir, database) = test_init();

    const COUNT: u64 = 5_000;

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    // Pick a range that is outside of the put range
    let mut iter = database.range_iter(&5300, &10150);

    while let Some((_key, _val)) = iter.next() {
        panic!("Found a key where there should be none");
    }

    database.stop().unwrap();
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
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).unwrap(),
            Some(format!("some_string_{pos}"))
        );
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
        let value = format!("some_string_{pos}");
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
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_other_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).unwrap(),
            Some(format!("some_other_string_{pos}"))
        );
    }
}

#[test]
fn override_subset() {
    const NCOUNT: u64 = 100_000;
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let mut options = WriteOptions::default();
    options.sync = false;

    for pos in 0..NCOUNT {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_other_string_{pos}");
        database.put_opts(&key, &value, &options).unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(
            database.get(&pos).unwrap(),
            Some(format!("some_other_string_{pos}"))
        );
    }

    for pos in COUNT..NCOUNT {
        assert_eq!(
            database.get(&pos).unwrap(),
            Some(format!("some_string_{pos}"))
        );
    }

    database.stop().unwrap();
}

#[test]
fn batched_write() {
    const COUNT: u64 = 1000;

    let (_tmpdir, database) = test_init();

    let mut batch = WriteBatch::new();

    for pos in 0..COUNT {
        let key = format!("key{pos}");
        batch.put(&key, &pos);
    }

    database.write(batch).unwrap();

    for pos in 0..COUNT {
        let key = format!("key{pos}");
        assert_eq!(database.get(&key).unwrap(), Some(pos));
    }
}
