// For writing to the log
#![feature(map_first_last)]
#![feature(trait_alias)]
#![feature(write_all_vectored)]
#![feature(array_methods)]
#![feature(get_mut_unchecked)]
#![feature(io_slice_advance)]

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use bincode::Options;

#[cfg(feature = "sync")]
pub mod sync_iter;

#[cfg(feature = "sync")]
pub use sync_iter as iterate;

#[cfg(not(feature = "sync"))]
pub mod async_iter;

#[cfg(not(feature = "sync"))]
pub use async_iter as iterate;

#[cfg(feature = "wisckey")]
mod values;

mod sorted_table;
use sorted_table::{Key, Value};

mod level_logger;

mod memtable;
mod tasks;

mod logic;
use logic::DbLogic;

mod data_blocks;
mod disk;
mod index_blocks;
mod level;
mod manifest;
mod wal;

#[derive(Debug)]
pub enum WriteOp {
    Put(Key, Value),
    Delete(Key),
}

impl WriteOp {
    const PUT_OP: u8 = 1;
    const DELETE_OP: u8 = 2;

    pub fn get_key(&self) -> &[u8] {
        match self {
            Self::Put(key, _) => key,
            Self::Delete(key) => key,
        }
    }

    pub fn get_type(&self) -> u8 {
        match self {
            Self::Put(_, _) => Self::PUT_OP,
            Self::Delete(_) => Self::DELETE_OP,
        }
    }

    fn get_key_length(&self) -> u64 {
        match self {
            Self::Put(key, _) | Self::Delete(key) => key.len() as u64,
        }
    }

    #[allow(dead_code)]
    fn get_value_length(&self) -> u64 {
        match self {
            Self::Put(_, value) => value.len() as u64,
            Self::Delete(_) => 0u64,
        }
    }
}
#[cfg(not(feature = "sync"))]
mod async_api;
#[cfg(not(feature = "sync"))]
pub use async_api::Database;

#[cfg(feature = "sync")]
mod sync_api;
#[cfg(feature = "sync")]
pub use sync_api::Database;

/// Keys and values must be (de-)serializable
pub trait KvTrait = Send
    + serde::Serialize
    + serde::de::DeserializeOwned
    + 'static
    + Unpin
    + Clone
    + std::fmt::Debug;

/// A WriteBatch allows to bundle multiple updates together for higher throughput
///
/// Note: The batch will not be applied to the database until it is passed to `Database::write`
#[derive(Debug)]
pub struct WriteBatch<K: KvTrait, V: KvTrait> {
    _marker: PhantomData<fn(K, V)>,
    writes: Vec<WriteOp>,
}

#[derive(Clone, Debug)]
pub enum Error {
    Io(String),
    InvalidParams(String),
    Serialization(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Io(msg) => {
                fmt.write_fmt(format_args!("Io Error: {msg}"))?;
            }
            Self::InvalidParams(msg) => {
                fmt.write_fmt(format_args!("Invalid Parameter: {msg}"))?;
            }
            Self::Serialization(msg) => {
                fmt.write_fmt(format_args!("Serialization Error: {msg}"))?;
            }
        }

        Ok(())
    }
}

impl From<std::io::Error> for Error {
    fn from(inner: std::io::Error) -> Self {
        Self::Io(inner.to_string())
    }
}

impl From<bincode::ErrorKind> for Error {
    fn from(inner: bincode::ErrorKind) -> Self {
        Self::Serialization(inner.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(inner: Box<bincode::ErrorKind>) -> Self {
        Self::Serialization((*inner).to_string())
    }
}

impl<K: KvTrait, V: KvTrait> WriteBatch<K, V> {
    pub fn new() -> Self {
        Self {
            writes: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Record a put operation in the write batch
    /// Will not be applied to the Database until the WriteBatch is written
    pub fn put(&mut self, key: &K, value: &V) {
        let enc = get_encoder();
        self.writes.push(WriteOp::Put(
            enc.serialize(key).unwrap(),
            enc.serialize(value).unwrap(),
        ));
    }

    pub fn delete(&mut self, key: &K) {
        let enc = get_encoder();
        self.writes
            .push(WriteOp::Delete(enc.serialize(key).unwrap()));
    }
}

impl<K: KvTrait, V: KvTrait> Default for WriteBatch<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Allows specifying details of a write
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Should the call block until it is guaranteed to be written to disk?
    pub sync: bool,
}

impl WriteOptions {
    pub const fn new() -> Self {
        Self { sync: true }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Allow specifying how the datastore behaves during startup
#[derive(Debug, Clone)]
pub enum StartMode {
    /// Reuse existing database, or create if non-existent
    CreateOrOpen,
    /// Open existing database, or fail if non-existent
    Open,
    /// Create a new, or override an existing, database
    CreateOrOverride,
}

/// Parameters to customize the creation of the database
#[derive(Debug, Clone)]
pub struct Params {
    /// Where in the filesystem should the database be stored?
    pub db_path: PathBuf,
    /// Maximum size of a memtable (keys+values),
    /// This indirectly also defines how large a value block can be
    pub max_memtable_size: usize,
    /// How many levels does this store have (default: 5)
    pub num_levels: usize,
    /// How many open files should be held in memory?
    pub max_open_files: usize,
    /// Maximum number of entries in a key block
    pub max_key_block_size: usize,
    /// How often should the full key be stored in a data block?
    /// Larger numbers result in smaller on-disk files, but seeks will be slower
    pub block_restart_interval: usize,
    /// Write the size of each level to a csv file
    pub log_level_stats: Option<String>,
    /// How many concurrent compaction tasks should there be
    pub compaction_concurrency: usize,
    /// How many seeks (per kb) before compaction is triggered?
    pub seek_based_compaction: Option<u32>,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            db_path: Path::new("./storage.lsm").to_path_buf(),
            max_memtable_size: 64 * 1024,
            num_levels: 5,
            max_open_files: 1000,
            max_key_block_size: 1024,
            block_restart_interval: 16,
            log_level_stats: None,
            compaction_concurrency: 4,
            seek_based_compaction: Some(10),
        }
    }
}

fn get_encoder(
) -> bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian> {
    // Use BigEndian to make integers sortable properly
    bincode::options().with_big_endian()
}
