# Modular, Asynchronous Implementation of a Log-Structured Merge Tree

[![ci-badge](https://github.com/kaimast/lsm-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/kaimast/lsm-rs/actions)
[![license-badge](https://img.shields.io/crates/l/lsm)](https://github.com/kaimast/lsm-rs/blob/main/LICENSE)
[![crates-badge](https://img.shields.io/crates/v/lsm)](https://crates.io/crates/lsm)

**Note: While this implementation is used by us and has not caused major problems, we do not recommend it yet production environments.**
 Please use the [leveldb](https://github.com/skade/leveldb) or [rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) crate for this purpose.

This implementation does *not* aim to reimplement LevelDB. The major differences are:
* *Separation of keys and values*: Values can be stored seperately to increase compaction speed as outlined in the [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) paper
* *Concurrent compaction*: Multiple threads can compact at the same time for higher write throughput
* *Async-support*: All API calls are exposed as async functions
* *io_uring-support*: For async file system access on Linux. Optional and still considered experimental.
* *Bloom filters* for faster lookups

## Latest Version
The version on crates.io is quite outdated as it does not allow to publish crates with git dependencies.
It is recommended to use the `main` git branch instead.

## Supported Platfomrs and Architectures
Currently, the code is only tested with Linux on x86 machines, but it should run on most systems supported by the Rust compiler.

## On-Disk Format
LSM stores data using [zerocopy](https://github.com/google/zerocopy) and (when possible) `mmap` to achieve high performance.
The implementation does not account for endianness so on-disk formats are not portable.
Replication across machines should be handled at a different layer of the system. However, we may add a converter tool in the future or an `endianess` feature flag if needed.

## Planned Features
* FLSM: Like [PebblesDB](https://github.com/utsaslab/pebblesdb) LSM-rs will fragment the keyspace to reduce write amplification and increase compaction speed
* Custom sorting functions
* More modularity and configuration options

## Feature Flags
* `snappy-compression`: Use the [snappy format](https://docs.rs/snap/1.0.5/snap/) to compress data on disk *(enabled by default)*
* `bloom-filters`: Add bloom filters to data blocks for more efficient searching. *(enabled by default)*
* `async-io`: Use `tokio_uring` for I/O instead of that of the standard library. Note, that this only works recent version of the Linux kernel. *(disabled by default)*
* `wisckey`: Store keys and values separately. This usually results in higher throughput with slightly higher CPU-usage. *(disabled by default)*

## Synchronous API
This crate exposes an async API intended to be used with Tokio or a similar runtime.
Alternatively, you can use the lsm-sync crate included in this repo, which internally uses Tokio but expose a synchronous API.

## Sort Order
This crate uses [bincode](https://github.com/bincode-org/bincode) to serialize keys and values.
Keys are sorted by comparing their binary representation and ordering those [lexographically](https://doc.rust-lang.org/std/cmp/trait.Ord.html#lexicographical-comparison).
We plan to add custom order and serialization mechanisms in the future.

## Tests
This library ships with several tests. We provide a [justfile](https://github.com/casey/just) for convenience:

```sh
just test #runs all tests for all configurations
just lint #runs cargo clippy
```

## Notes on io-uring
Currently, the io-uring feature relies on [tokio-uring-executor](https://github.com/kaimast/tokio-uring-executor), a simplistic multi-threaded wrapper around `tokio-uring`.
Eventually `tokio-uring` will [support multiple threads natively](https://github.com/tokio-rs/tokio-uring/issues/258) and this workaround will be removed.

## Similar Crates
This is an incomplete list of crates that provide similar functionality. Please reach out if you know of others to add.

### LSM trees
* [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb): Rust bindings for RocksDB
* [leveldb](https://github.com/skade/leveldb): Rust bindings for LevelDB
* [wickdb](https://github.com/Fullstop000/wickdb): Rust re-implementation of vanilla LevelDB
* [agatedb](https://github.com/tikv/agatedb): A WiscKey implementation in Rust for TiKV

### Other Key-Value Stores
These differ significantly in their approach but also provide a key-value store abstraction
* [redb](https://github.com/cberner/redb)

