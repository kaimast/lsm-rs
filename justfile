LOG_LEVEL := "debug"

all: tests lint

tests: sync-tests async-tests no-compression-tests tokio-uring-tests wisckey-tests wisckey-no-compression-tests wisckey-sync-tests

sync-tests:
    cd sync && just default-tests

async-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features

tokio-uring-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=tokio-uring,bloom-filters -- --test-threads=1

monoio-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=monoio,bloom-filters -- --test-threads=1

tokio-uring-wisckey-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=tokio-uring,wisckey,bloom-filters -- --test-threads=1

no-compression-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features

wisckey-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=snappy-compression,wisckey

wisckey-no-compression-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=wisckey

wisckey-sync-tests:
    cd sync && just wisckey-tests

lint: sync-lint async-lint wisckey-lint wisckey-no-compression-lint tokio-uring-lint tokio-uring-wisckey-lint monoio-lint

fix-formatting:
    cargo fmt
    cd sync && just fix-formatting

check-formatting:
    cargo fmt --check
    cd sync && just check-formatting

clean:
    rm -rf target/

update-dependencies:
    cargo update
    cd sync && cargo update

udeps:
    cargo udeps --all-targets --release
    cd sync && just udeps

sync-lint:
    cd sync && just lint

async-lint:
    cargo clippy --no-default-features -- -D warnings

tokio-uring-lint:
    cargo clippy --no-default-features --features=tokio-uring,bloom-filters -- -D warnings

monoio-lint:
    cargo clippy --no-default-features --features=monoio,bloom-filters -- -D warnings

wisckey-lint:
    cargo clippy --no-default-features --features=snappy-compression,wisckey -- -D warnings

wisckey-no-compression-lint:
    cargo clippy --no-default-features --features=wisckey

tokio-uring-wisckey-lint:
    cargo clippy --no-default-features --features=tokio-uring,snappy-compression,wisckey -- -D warnings
