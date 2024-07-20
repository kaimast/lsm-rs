use lsm_sync::{Database, Params, StartMode, WriteBatch, WriteOptions};
use tempfile::{Builder, TempDir};

const SM: StartMode = StartMode::CreateOrOverride;

fn test_init() -> (TempDir, Database) {
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

    let key1 = "Foo".to_string().into_bytes();
    let key2 = "Foz".to_string().into_bytes();
    let value1 = "Bar".to_string().into_bytes();
    let value2 = "Baz".to_string().into_bytes();

    assert!(database.get(&key1).unwrap().is_none());
    assert!(database.get(&key2).unwrap().is_none());

    database.put(key1.clone(), value1.clone()).unwrap();

    assert_eq!(database.get(&key1).unwrap().unwrap().get_value(), value1);
    assert!(database.get(&key2).unwrap().is_none());

    database.put(key1.clone(), value2.clone()).unwrap();
    assert_eq!(database.get(&key1).unwrap().unwrap().get_value(), value2);
}

#[test]
fn iterate() {
    const COUNT: u64 = 5_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    let mut count = 0;

    for (pos, (key, val)) in database.iter().enumerate() {
        let expected_key = format!("key_{pos:05}").into_bytes();
        let expected_val = format!("some_string_{pos}").into_bytes();
       
        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value()); 

        count += 1;
    }

    assert_eq!(count, COUNT);
}

#[test]
fn range_iterate() {
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    let start = "key_00300".to_string().into_bytes();
    let end = "key_10150".to_string().into_bytes();
    let iter = database.range_iter(&start, &end);

    let mut pos = 0;
    for (key, val) in iter {
        let real_pos = pos + 300;
        let expected_key = format!("key_{real_pos:05}").into_bytes();
        let expected_val = format!("some_string_{real_pos}").into_bytes();
 
        assert_eq!(expected_key, key);
        assert_eq!(expected_val, val.get_value());

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
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    let start = "key_10150".to_string().into_bytes();
    let end =   "key_00300".to_string().into_bytes();
    let iter = database.reverse_range_iter(&start, &end);

    let mut pos = 0;
    for (key, val) in iter {
        let real_pos = 10150 - pos;
        let expected_key = format!("key_{real_pos:05}").into_bytes();
       
        assert_eq!(expected_key, key);
        assert_eq!(format!("some_string_{real_pos}").into_bytes(), val.get_value());
    
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
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos:05}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    // Pick a range that is outside of the put range
    let start = "key_05300".to_string().into_bytes();
    let end = "key_10150".to_string().into_bytes();
    let mut iter = database.range_iter(&start, &end);

    if let Some((_key, _val)) = iter.next() {
        panic!("Found a key where there should be none");
    }

    database.stop().unwrap();
}

#[test]
fn get_put_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
 
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
 
        assert_eq!(
            database.get(&key).unwrap().unwrap().get_value(),
            value, 
        );
    }
}

#[test]
fn get_put_delete_many() {
    const COUNT: u64 = 10_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
 
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        database.delete(key).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        assert!(database.get(&key).unwrap().is_none());
    }
}

#[test]
fn override_many() {
    const COUNT: u64 = 100_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
 
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();
        
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();
 
        assert_eq!(
            database.get(&key).unwrap().unwrap().get_value(),
            value
        );
    }
}

#[test]
fn override_subset() {
    const NCOUNT: u64 = 100_000;
    const COUNT: u64 = 25_000;

    let (_tmpdir, database) = test_init();

    // Write without fsync to speed up tests
    let options = WriteOptions{ sync: false };

    for pos in 0..NCOUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();
        database.put_opts(key, value, &options).unwrap();
    }

    for pos in 0..COUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_other_string_{pos}").into_bytes();
 
        assert_eq!(
            database.get(&key).unwrap().unwrap().get_value(),
            value,
        );
    }

    for pos in COUNT..NCOUNT {
        let key = format!("key_{pos}").into_bytes();
        let value = format!("some_string_{pos}").into_bytes();
  
        assert_eq!(
            database.get(&key).unwrap().unwrap().get_value(),
            value,
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
        let key = format!("key{pos}").into_bytes();
        let value = format!("value{pos}").into_bytes(); 
        batch.put(key, value);
    }

    database.write(batch).unwrap();

    for pos in 0..COUNT {
        let key = format!("key{pos}").into_bytes();
        let value = format!("value{pos}").into_bytes();

        let entry = database.get(&key).unwrap();

        assert!(entry.is_some());
        assert_eq!(entry.unwrap().get_value(), value.as_slice());
    }
}
