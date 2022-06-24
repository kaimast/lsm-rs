use std::{fs::File, io::BufWriter};

use clap::Parser;

use tempfile::{Builder, TempDir};

use tracing_flame::{FlameLayer, FlushGuard};
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

use lsm::{Database, KvTrait, Params, StartMode, WriteOptions};

#[derive(Parser)]
#[clap(rename_all = "snake-case")]
#[ clap(author, version, about, long_about = None) ]
struct Args {
    #[clap(long)]
    enable_tracing: bool,
    #[clap(long)]
    log_level_stats: Option<String>,
    #[clap(long, default_value = "100000")]
    num_entries: usize,
}

async fn bench_init<K: KvTrait, V: KvTrait>(
    args: &Args,
) -> (Option<FlushGuard<BufWriter<File>>>, TempDir, Database<K, V>) {
    let tracing_guard = if args.enable_tracing {
        let fmt_layer = fmt::Layer::default();

        let (flame_layer, _guard) = FlameLayer::with_file("./lsm-trace.folded").unwrap();

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(flame_layer)
            .init();
        Some(_guard)
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
        log_level_stats: args.log_level_stats.clone(),
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
    let args = Args::parse();

    let (_tracing, _tmpdir, database) = bench_init(&args).await;

    log::info!("Starting read/write benchmark");

    let mut options = WriteOptions::default();
    options.sync = false;

    log::debug!("Writing {} entries", args.num_entries);

    for pos in 0..args.num_entries {
        let key = pos;
        let value = format!("some_string_{}", pos);
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    log::debug!("Reading {} entries", args.num_entries);

    for pos in 0..args.num_entries {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{}", pos))
        );
    }

    database.stop().await.unwrap();
}
