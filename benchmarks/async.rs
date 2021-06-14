use std::{fs::File, io::BufWriter};

use tempfile::{Builder, TempDir};

use clap::{App, Arg};

use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_flame::{FlushGuard, FlameLayer};

use lsm::{Database, StartMode, KvTrait, Params, WriteOptions};

async fn bench_init<K: KvTrait, V: KvTrait>() -> (Option<FlushGuard<BufWriter<File>>>, TempDir, Database<K, V>) {
    let arg_matches = App::new("lsm-benchmark")
        .author("Kai Mast <kaimast@cs.cornell.edu>")
        .arg(Arg::new("enable_tracing")
                .takes_value(false)
                .long("enable_tracing")
            )
        .get_matches();

    let tracing_guard = if arg_matches.is_present("enable_tracing") {
        let fmt_layer = fmt::Layer::default();

        let (flame_layer, tracing_guard) = FlameLayer::with_file("./tracing.folded").unwrap();

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(flame_layer)
            .init();

        Some(tracing_guard)
    } else {
        None
    };

    let _ = env_logger::builder().is_test(true).try_init();
    let tmp_dir = Builder::new().prefix("lsm-async-benchmark-").tempdir().unwrap();

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
    const COUNT: u64 = 10_000;

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
