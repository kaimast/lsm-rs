# Modular, Asynchronous Implementation of a Log-Structured Merge Tree

[![ci-badge](https://github.com/kaimast/lsm-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/kaimast/lsm-rs/actions)
[![license-badge](https://img.shields.io/crates/l/lsm)](https://github.com/kaimast/lsm-rs/blob/main/LICENSE)
[![crates-badge](https://img.shields.io/crates/v/lsm)](https://crates.io/crates/lsm)

**Note: This is an experimental implementation and not intended for production environments.**
 Please use the [leveldb](https://github.com/skade/leveldb) or [rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) crate for this purpose.

This implementation does *not* aim to reimplement LevelDB. The major differences are:
* *Separation of keys and values*: like [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), values are store separately to increase compaction speed
* *Concurrent compaction*: Multiple threads can compact at the same time for higher write throughput (not fully implemented yet)
* *Async-support*: All API calls are exposed as async functions. Note, that internally tokio still uses blocking IO in a separate thread pool. Eventually, there will be support for [io_uring](https://github.com/tokio-rs/tokio/issues/2411).

## Supported Architectures:
Currently, the code is only tested on Linux machines, but it should run on all systems supported by the rust compiler.

## Planned Features:
* Bloom filters for faster lookups
* FLSM: Like [PebblesDB](https://github.com/utsaslab/pebblesdb) LSM-rs will fragment the keyspace to reduce write amplification and increase compaction speed
* More modularity and configuration options

## Feature Flags
* `snappy-compression`: Use the snappy to compress data on disk (enabled by default)
* `sync`: Expose the synchronous API instead of async one (Note: in this case the implementation will launch a tokio instance internally and hide it from the caller)

## Tests
This library ships with several tests. Note, that you cannot run them concurrently as they will access the same on-disk location.
To run tests (on Linux) execute the following command.

```
env RUST_TEST_THREADS=1 RUST_BACKTRACE=1 RUST_LOG=debug cargo test 
```

## Similar Crates
* [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb): Rust bindings for RocksDB
* [leveldb](https://github.com/skade/leveldb): Rust bindings for LevelDB
* [wickdb](https://github.com/Fullstop000/wickdb): Rust re-implementation of vanilla LevelDB
