# Modular, Asynchronous Implementation of a Log-Structured Merge Tree

[![ci-badge](https://github.com/kaimast/lsm-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/kaimast/lsm-rs/actions)
[![license-badge](https://img.shields.io/crates/l/lsm)](https://github.com/kaimast/lsm-rs/blob/main/LICENSE)
[![crates-badge](https://img.shields.io/crates/v/lsm)](https://crates.io/crates/lsm)

**Note: This is an experimental implementation and not intended for production environments.**
 Please use the [leveldb](https://github.com/skade/leveldb) or [rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) crate for this purpose.

This implementation does *not* aim to reimplement LevelDB. The major differences are:
* *Separation of keys and values*: like [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), values are stored separately to increase compaction speed
* *Concurrent compaction*: Multiple threads can compact at the same time for higher write throughput
* *Async-support*: All API calls are exposed as async functions.
* *io_uring-support*: For async file system access on Linux
* *Bloom filters* for faster lookups

## Latest Version:
The version on crates.io is quite outdated. It is recommended using the `main` git branch.

## Supported Architectures:
Currently, the code is only tested on Linux machines, but it should run on all systems supported by the rust compiler.

## Planned Features:
* FLSM: Like [PebblesDB](https://github.com/utsaslab/pebblesdb) LSM-rs will fragment the keyspace to reduce write amplification and increase compaction speed
* Custom sorting functions
* More modularity and configuration options

## Feature Flags
* `snappy-compression`: Use the [snappy format](https://docs.rs/snap/1.0.5/snap/) to compress data on disk *(enabled by default)*
* `bloom-filters`: Add bloom filters to data blocks for more efficient searching. *(enabled by default)*
* `async-io`: Use `tokio_uring` for I/O instead of that of the standard library. Note, that this only works recent version of the Linux kernel. *(disabled by default)*
* `wisckey`: Store keys and values separately. This usually results in higher throughput with slightly higher CPU-usage *(disabled by default)*

## Synchronous API
This crate exposes an async API intended to be used with Tokio or a similar runtime.
Alternatively, you can use the lsm-sync crate included in this repo, which internally uses Tokio but expose a synchronous API.

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

## Notes on io-uring
Currently, this uses [tokio-uring-executor](https://github.com/kaimast/tokio-uring-executor), a very simplistic multi-threaded wrapper around `tokio-uring`.
Eventually `tokio-uring` will [support multiple threads natively](https://github.com/tokio-rs/tokio-uring/issues/258) and this workaround will be removed.

## Similar Crates
* [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb): Rust bindings for RocksDB
* [leveldb](https://github.com/skade/leveldb): Rust bindings for LevelDB
* [wickdb](https://github.com/Fullstop000/wickdb): Rust re-implementation of vanilla LevelDB
