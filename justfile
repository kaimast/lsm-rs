LOG_LEVEL := "debug"

all: test lint

test: sync-tests async-tests no-wisckey-tests no-compression-tests #async-io-tests

sync-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --features=sync

async-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --features=async

async-io-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --features=async,async-io

no-compression-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,wisckey

no-wisckey-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,snappy-compression

no-wisckey-sync-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=snappy-compression,sync

lint: sync-lint async-lint no-wisckey-lint async-io-lint

clean:
    rm -rf target/

sync-lint:
    cargo clippy --features=sync -- -D warnings

async-lint:
    cargo clippy -- -D warnings

async-io-lint:
    cargo clippy --features=async-io -- -D warnings

no-wisckey-lint:
    cargo clippy --no-default-features --features=snappy-compression -- -D warnings
