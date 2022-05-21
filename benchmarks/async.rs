use std::{fs::File, io::BufWriter};

use tempfile::{Builder, TempDir};

use clap::{Arg, Command};

use tracing_flame::{FlameSubscriber, FlushGuard};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;

use lsm::{Database, KvTrait, Params, StartMode, WriteOptions};

async fn bench_init<K: KvTrait, V: KvTrait>(
) -> (Option<FlushGuard<BufWriter<File>>>, TempDir, Database<K, V>) {
    let arg_matches = Command::new("lsm-benchmark")
        .author("Kai Mast <kaimast@cs.cornell.edu>")
        .arg(
            Arg::new("enable_tracing")
                .takes_value(false)
                .long("enable_tracing"),
        )
        .get_matches();

    let tracing_guard = if arg_matches.is_present("enable_tracing") {
        let fmt_subscriber = fmt::Subscriber::default();
        let (flame_subscriber, tracing_guard) = FlameSubscriber::with_file("./tracing.folded")
            .expect("Failed to set up flame subscriber");

        let collector = Registry::default()
            .with(fmt_subscriber)
            .with(flame_subscriber);

        tracing::collect::set_global_default(collector)
            .expect("setting global tracing subscriber failed");

        Some(tracing_guard)
    } else {
        None
    };

    let _ = env_logger::builder().is_test(true).try_init();
    let tmp_dir = Builder::new()
        .prefix("lsm-async-benchmark-")
        .tempdir()
        .unwrap();

    let mut db_path = tmp_dir.path().to_path_buf();
    db_path.push("storage.lsm");

    let params = Params {
        db_path,
        ..Default::default()
    };
    const SM: StartMode = StartMode::CreateOrOverride;

    let database = Database::new_with_params(SM, params)
        .await
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
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{}", pos))
        );
    }

    database.stop().await.unwrap();
}
