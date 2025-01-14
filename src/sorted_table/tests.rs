use super::*;

use crate::manifest::Manifest;

use tempfile::tempdir;

#[cfg(feature = "tokio-uring")]
use tokio_uring_executor::test as async_test;

#[cfg(not(feature = "_async-io"))]
use tokio::test as async_test;

#[cfg(feature = "wisckey")]
#[async_test]
async fn iterate() {
    let dir = tempdir().unwrap();
    let params = Params {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    };

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5];
    let key2 = vec![15];

    let value_id1 = (4, 2);
    let value_id2 = (4, 50);

    let id = 124234;
    let mut builder = TableBuilder::new(id, &params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, value_id1).await.unwrap();

    builder.add_value(&key2, 4, value_id2).await.unwrap();

    let table = builder.finish().await.unwrap();

    let mut iter = TableIterator::new(Arc::new(table), false).await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_value_id(), Some(value_id1));

    iter.step().await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_value_id(), Some(value_id2));

    iter.step().await;

    assert!(iter.at_end());
}

#[cfg(not(feature = "wisckey"))]
#[async_test]
async fn iterate() {
    let dir = tempdir().unwrap();
    let params = Params {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    };

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5, 10, 3];
    let key2 = vec![15, 10, 3];

    let value1 = vec![4, 2];
    let value2 = vec![4, 50];

    let id = 124234;
    let mut builder = TableBuilder::new(id, &params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, &value1).await.unwrap();

    builder.add_value(&key2, 4, &value2).await.unwrap();

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, false).await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_entry().unwrap().get_value(), &value1);

    iter.step().await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_entry().unwrap().get_value(), &value2);

    iter.step().await;

    assert!(iter.at_end());
}

#[cfg(not(feature = "wisckey"))]
#[async_test]
async fn reverse_iterate() {
    let dir = tempdir().unwrap();
    let params = Params {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    };

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5, 10, 3];
    let key2 = vec![15, 10, 3];

    let value1 = vec![4, 2];
    let value2 = vec![4, 50];

    let id = 124234;
    let mut builder = TableBuilder::new(id, &params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, &value1).await.unwrap();

    builder.add_value(&key2, 4, &value2).await.unwrap();

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, true).await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_entry().unwrap().get_value(), &value2);

    iter.step().await;

    assert!(!iter.at_end());
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_entry().unwrap().get_value(), &value1);

    iter.step().await;

    assert!(iter.at_end());
}

#[cfg(feature = "wisckey")]
#[async_test]
async fn iterate_many() {
    const COUNT: u32 = 5_000;

    let dir = tempdir().unwrap();
    let params = Params {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    };

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let min_key = (0u32).to_le_bytes().to_vec();
    let max_key = COUNT.to_le_bytes().to_vec();

    let id = 1;
    let mut builder = TableBuilder::new(id, &params, data_blocks, min_key, max_key);

    for pos in 0..COUNT {
        let key = (pos).to_le_bytes().to_vec();
        let seq_num = (500 + pos) as u64;

        builder.add_value(&key, seq_num, (100, pos)).await.unwrap();
    }

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, false).await;

    for pos in 0..COUNT {
        assert!(!iter.at_end());

        assert_eq!(iter.get_key(), &pos.to_le_bytes().to_vec());
        assert_eq!(iter.get_value_id(), Some((100, pos)));
        assert_eq!(iter.get_seq_number(), 500 + pos as u64);

        iter.step().await;
    }

    assert!(iter.at_end());
}
