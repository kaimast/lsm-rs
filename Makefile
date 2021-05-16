.PHONY: sync-tests async-tests no-wisckey-fold-tests no-compression-tests async-io-tests
.PHONY: sync-lint async-lint no-wisckey-fold-lint async-io-lint

CARGO=cargo +nightly
LOG_LEVEL?=debug

all: test lint

test: sync-tests async-tests no-wisckey-fold-tests no-compression-tests async-io-tests

sync-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --features=sync

async-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test

async-io-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --features=async-io

no-compression-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=wisckey-fold

no-wisckey-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=snappy-compression

wisckey-reinsert-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=snappy-compression,wisckey-reinsert

no-wisckey-sync-tests:
	env RUST_LOG=${LOG_LEVEL} ${CARGO} test --no-default-features --features=snappy-compression,sync

lint: sync-lint async-lint no-wisckey-fold-lint async-io-lint

clean:
	rm -rf target/

sync-lint:
	${CARGO} clippy --features=sync -- -D warnings

async-lint:
	${CARGO} clippy -- -D warnings

async-io-lint:
	${CARGO} clippy --features=async-io -- -D warnings

no-wisckey-fold-lint:
	${CARGO} clippy --no-default-features --features=snappy-compression -- -D warnings
