use tempfile::{Builder, TempDir};
use std::{fs::File, io::BufWriter};

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_flame::{FlushGuard, FlameLayer};

use lsm::{Database, StartMode, KvTrait, Params, WriteOptions};

async fn bench_init<K: KvTrait, V: KvTrait>() -> (FlushGuard<BufWriter<File>>, TempDir, Database<K, V>) {
    let fmt_layer = fmt::Layer::default();

    let (flame_layer, tracing_guard) = FlameLayer::with_file("./tracing.folded").unwrap();

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(flame_layer)
        .init();

    let tmp_dir = Builder::new().prefix("lsm-async-test-").tempdir().unwrap();
    let _ = env_logger::builder().is_test(true).try_init();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params{ db_path, ..Default::default() };
    const SM: StartMode = StartMode::CreateOrOverride;

    let database = Database::new_with_params(SM, params).await
        .expect("Failed to create database instance");

    (tracing_guard, tmp_dir, database)
}

#[tokio::main]
async fn main() {
    const COUNT: u64 = 3_400;

    let (_tracing, _tmpdir, database) = bench_init().await;

    let mut options = WriteOptions::default();
    options.sync = false;


    for pos in 0..COUNT {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    for pos in 0..COUNT {
        assert_eq!(database.get(&pos).await.unwrap(), Some(format!("some_string_{}", pos)));
    }

    database.stop().await.unwrap();
}
