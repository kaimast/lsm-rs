LOG_LEVEL := "debug"

all: test lint

test: sync-test async-test no-compression-test async-io-test wisckey-test wisckey-no-compression-test wisckey-sync-test

sync-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=sync,bloom-filters

async-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,snappy-compression

async-io-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,async-io,bloom-filters -- --test-threads=1

no-compression-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async

wisckey-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,snappy-compression,wisckey

wisckey-no-compression-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=async,wisckey

wisckey-sync-test:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=snappy-compression,sync,wisckey

lint: sync-lint async-lint wisckey-lint wisckey-no-compression-lint async-io-lint async-io-wisckey-lint

fix-formatting:
    cargo fmt

check-formatting:
    cargo fmt --check

clean:
    rm -rf target/

update-dependencies:
    cargo update

udeps:
    cargo udeps --all-targets --release

sync-lint:
    cargo clippy --no-default-features --features=sync -- -D warnings

async-lint:
    cargo clippy --no-default-features --features=async -- -D warnings

async-io-lint:
    cargo clippy --no-default-features --features=async,async-io,bloom-filters -- -D warnings

wisckey-lint:
    cargo clippy --no-default-features --features=snappy-compression,wisckey -- -D warnings

wisckey-no-compression-lint:
    cargo clippy --no-default-features --features=async,wisckey

async-io-wisckey-lint:
    cargo clippy --no-default-features --features=async-io,async,snappy-compression,wisckey -- -D warnings
