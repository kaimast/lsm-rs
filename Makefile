.PHONY: sync-tests async-tests no-wisckey-tests no-compression-tests
.PHONY: sync-lint async-lint

CARGO=cargo +nightly
LOG_LEVEL?=debug

all: test lint

test: sync-tests async-tests no-wisckey-tests no-compression-tests

sync-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test --features=sync

async-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test

no-compression-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=wisckey

no-wisckey-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=snappy-compression

lint: sync-lint async-lint

sync-lint:
	${CARGO} clippy --features=sync -- -D warnings

async-lint:
	${CARGO} clippy -- -D warnings
