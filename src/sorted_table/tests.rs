use super::*;

use crate::manifest::Manifest;

use tempfile::tempdir;

#[cfg(feature = "async-io")]
use tokio_uring::test as async_test;

#[cfg(not(feature = "async-io"))]
use tokio::test as async_test;

#[cfg(feature = "wisckey")]
#[async_test]
async fn iterate() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5];
    let key2 = vec![15];

    let vref1 = (4, 2);
    let vref2 = (4, 50);

    let id = 124234;
    let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, vref1).await.unwrap();

    builder.add_value(&key2, 4, vref2).await.unwrap();

    let table = builder.finish().await.unwrap();

    let mut iter = TableIterator::new(Arc::new(table), false).await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_value(), ValueResult::Reference(vref1));

    iter.step().await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_value(), ValueResult::Reference(vref2));

    iter.step().await;

    assert_eq!(iter.at_end(), true);
}

#[cfg(not(feature = "wisckey"))]
#[async_test]
async fn iterate() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5];
    let key2 = vec![15];

    let value1 = vec![4, 2];
    let value2 = vec![4, 50];

    let id = 124234;
    let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, &value1).await.unwrap();

    builder.add_value(&key2, 4, &value2).await.unwrap();

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, false).await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_value(), Some(&value1 as &[u8]));

    iter.step().await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_value(), Some(&value2 as &[u8]));

    iter.step().await;

    assert_eq!(iter.at_end(), true);
}

#[cfg(not(feature = "wisckey"))]
#[async_test]
async fn reverse_iterate() {
    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let key1 = vec![5];
    let key2 = vec![15];

    let value1 = vec![4, 2];
    let value2 = vec![4, 50];

    let id = 124234;
    let mut builder = TableBuilder::new(id, &*params, data_blocks, key1.clone(), key2.clone());

    builder.add_value(&key1, 1, &value1).await.unwrap();

    builder.add_value(&key2, 4, &value2).await.unwrap();

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, true).await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key2);
    assert_eq!(iter.get_value(), Some(&value2 as &[u8]));

    iter.step().await;

    assert_eq!(iter.at_end(), false);
    assert_eq!(iter.get_key(), &key1);
    assert_eq!(iter.get_value(), Some(&value1 as &[u8]));

    iter.step().await;

    assert_eq!(iter.at_end(), true);
}

#[cfg(feature = "wisckey")]
#[async_test]
async fn iterate_many() {
    const COUNT: u32 = 5_000;

    let dir = tempdir().unwrap();
    let mut params = Params::default();
    params.db_path = dir.path().to_path_buf();

    let params = Arc::new(params);
    let manifest = Arc::new(Manifest::new(params.clone()).await);

    let data_blocks = Arc::new(DataBlocks::new(params.clone(), manifest));

    let min_key = (0u32).to_le_bytes().to_vec();
    let max_key = (COUNT as u32).to_le_bytes().to_vec();

    let id = 1;
    let mut builder = TableBuilder::new(id, &*params, data_blocks, min_key, max_key);

    for pos in 0..COUNT {
        let key = (pos as u32).to_le_bytes().to_vec();
        let seq_num = (500 + pos) as u64;

        builder.add_value(&key, seq_num, (100, pos)).await.unwrap();
    }

    let table = Arc::new(builder.finish().await.unwrap());

    let mut iter = TableIterator::new(table, false).await;

    for pos in 0..COUNT {
        assert_eq!(iter.at_end(), false);

        assert_eq!(iter.get_key(), &(pos as u32).to_le_bytes().to_vec());
        assert_eq!(iter.get_value(), ValueResult::Reference((100, pos)));
        assert_eq!(iter.get_seq_number(), 500 + pos as u64);

        iter.step().await;
    }

    assert_eq!(iter.at_end(), true);
}
