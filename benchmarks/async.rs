use clap::Parser;

use tempfile::{Builder, TempDir};

use tracing_subscriber::prelude::*;
use tracing_tracy::TracyLayer;

use lsm::{Database, KvTrait, Params, StartMode, WriteOptions};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long)]
    enable_tracing: bool,
    #[clap(long)]
    log_level_stats: Option<String>,
    #[clap(long, default_value = "100000")]
    num_entries: usize,
}

async fn bench_init<K: KvTrait, V: KvTrait>(args: &Args) -> (TempDir, Database<K, V>) {
    if args.enable_tracing {
        tracing_subscriber::registry()
            .with(TracyLayer::default())
            .init();
    }

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

    (tmp_dir, database)
}

async fn main_inner() {
    let args = Args::parse();

    let (_tmpdir, database) = bench_init(&args).await;

    log::info!("Starting read/write benchmark");

    let mut options = WriteOptions::default();
    options.sync = false;

    log::debug!("Writing {} entries", args.num_entries);

    for pos in 0..args.num_entries {
        let key = pos;
        let value = format!("some_string_{pos}");
        database.put_opts(&key, &value, &options).await.unwrap();
    }

    log::debug!("Reading {} entries", args.num_entries);

    for pos in 0..args.num_entries {
        assert_eq!(
            database.get(&pos).await.unwrap(),
            Some(format!("some_string_{pos}"))
        );
    }

    database.stop().await.unwrap();
    log::info!("Done");
}

#[cfg(feature = "async-io")]
fn main() {
    tokio_uring::start(async {
        main_inner().await;
    });
}

#[cfg(not(feature = "async-io"))]
#[tokio::main]
async fn main() {
    main_inner().await;
}
