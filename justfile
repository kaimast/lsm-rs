LOG_LEVEL := "debug"

# Common prefix for lints
CLIPPY := "cargo clippy --no-default-features --tests"

all: tests lint

tests: sync-tests async-tests no-compression-tests \
       tokio-uring-tests wisckey-tests \
       wisckey-no-compression-tests wisckey-sync-tests \
       monoio-tests monoio-wisckey-tests

sync-tests:
    cd sync && just default-tests

async-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features

tokio-uring-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=tokio-uring,bloom-filters -- --test-threads=1

monoio-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=monoio,bloom-filters -- --test-threads=1

monoio-wisckey-tests:
    env RUST_BACKTRACE=1 RUST_LOG={{LOG_LEVEL}} cargo test --no-default-features --features=monoio,wisckey,bloom-filters -- --test-threads=1

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

lint: sync-lint async-lint wisckey-lint \
      wisckey-no-compression-lint tokio-uring-lint \
      tokio-uring-wisckey-lint monoio-lint monoio-wisckey-lint \
      bigtest-lint

fix-formatting:
    cargo fmt
    cd sync && just fix-formatting
    cd bigtest && cargo fmt

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
    {{CLIPPY}} -- -D warnings

tokio-uring-lint:
    {{CLIPPY}} --features=tokio-uring,bloom-filters -- -D warnings

monoio-lint:
    {{CLIPPY}} --features=monoio,bloom-filters -- -D warnings

monoio-wisckey-lint:
    {{CLIPPY}} --features=monoio,wisckey,bloom-filters -- -D warnings

wisckey-lint:
    {{CLIPPY}} --features=snappy-compression,wisckey -- -D warnings

wisckey-no-compression-lint:
    {{CLIPPY}} --features=wisckey -- -D warnings

tokio-uring-wisckey-lint:
    {{CLIPPY}} --features=tokio-uring,snappy-compression,wisckey -- -D warnings

bigtest-lint:
    {{CLIPPY}} --package=lsm-bigtest

bigtest:
    cargo run --release --package=lsm-bigtest
