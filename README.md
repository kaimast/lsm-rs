# Modular, Asynchronous Implementation of a Log-Structured Merge Tree

[![ci-badge](https://github.com/kaimast/lsm-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/kaimast/lsm-rs/actions)
[![license-badge](https://img.shields.io/crates/l/lsm)](https://github.com/kaimast/lsm-rs/blob/main/LICENSE)
[![crates-badge](https://img.shields.io/crates/v/lsm)](https://crates.io/crates/lsm)

**Note: This is an experimental implementation and not intended for production environments.**
 Please use the [leveldb](https://github.com/skade/leveldb) or [rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) crate for this purpose.

This implementation does *not* aim to reimplement LevelDB. The major differences are:
* *Separation of keys and values*: like [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), values are stored separately to increase compaction speed
* *Concurrent compaction*: Multiple threads can compact at the same time for higher write throughput (not fully implemented yet)
* *Async-support*: All API calls are exposed as async functions.

## Latest Version:
The version on crates.io is quite outdated. I recommend using the `main` git branch.

## Supported Architectures:
Currently, the code is only tested on Linux machines, but it should run on all systems supported by the rust compiler.

## Planned Features:
* Bloom filters for faster lookups
* FLSM: Like [PebblesDB](https://github.com/utsaslab/pebblesdb) LSM-rs will fragment the keyspace to reduce write amplification and increase compaction speed
* Transactions: Modify multiple values at once and atomically
* Custom sorting functions
* More modularity and configuration options

## Feature Flags
* `wisckey`: Store keys and values separately. This usually results in higher throughput with slightly higher CPU-usage (enabled by default)
* `snappy-compression`: Use the snappy to compress data on disk (enabled by default)
* `sync`: Expose a synchronous API instead of an async one. Note, that in this case the implementation will launch a tokio instance internally and hide it from the caller.
* `async-io`: Use tokio's disk IO instead of that of the standard library. Note, that internally tokio still uses blocking IO in a separate thread pool and this is [generally slower](https://github.com/tokio-rs/tokio/issues/3664). Eventually, there will be support for [io_uring](https://github.com/tokio-rs/tokio/issues/2411).

## Sort Order
This crate uses [bincode](https://github.com/bincode-org/bincode) to serialize keys and values.
Keys are sorted by comparing their binary representation and ordering those [lexographically](https://doc.rust-lang.org/std/cmp/trait.Ord.html#lexicographical-comparison).
We plan to add custom order and serialization mechanisms in the future.

## Tests
This library ships with several tests. Note, that you cannot run them concurrently as they will access the same on-disk location.
We provide a [justfile](https://github.com/casey/just) for convenience:

```sh
just test #runs all tests for all configurations
just lint #runs cargo clippy
```

## Similar Crates
* [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb): Rust bindings for RocksDB
* [leveldb](https://github.com/skade/leveldb): Rust bindings for LevelDB
* [wickdb](https://github.com/Fullstop000/wickdb): Rust re-implementation of vanilla LevelDB
