.PHONY: sync-tests async-tests
.PHONY: sync-lint async-lint

CARGO=cargo +nightly
LOG_LEVEL?=debug

all: test lint

test: sync-tests async-tests

sync-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test --features=sync

async-tests:
	env RUST_TEST_THREADS=1 RUST_LOG=${LOG_LEVEL} ${CARGO} test

lint: sync-lint async-lint

sync-lint:
	${CARGO} clippy --features=sync -- -D warnings

async-lint:
	${CARGO} clippy -- -D warnings
