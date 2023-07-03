LOG_LEVEL := "debug"

all: test lint

test: sync-tests async-tests no-wisckey-tests no-compression-tests #async-io-tests

sync-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=sync

async-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async

async-io-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,async-io

no-compression-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,wisckey

no-wisckey-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,snappy-compression

no-wisckey-sync-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=snappy-compression,sync

lint: sync-lint async-lint no-wisckey-lint async-io-lint

fix-formatting:
    cargo fmt

check-formatting:
    cargo fmt --check

clean:
    rm -rf target/

udeps:
    cargo udeps

sync-lint:
    cargo clippy --no-default-features --features=sync -- -D warnings

async-lint:
    cargo clippy --no-default-features --features=async -- -D warnings

async-io-lint:
    cargo clippy  --no-default-features--features=async,async-io -- -D warnings

no-wisckey-lint:
    cargo clippy --no-default-features --features=snappy-compression -- -D warnings
