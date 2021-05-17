// For writing to the log
#![ feature(map_first_last) ]
#![ feature(trait_alias) ]
#![ feature(async_stream) ]
#![ feature(write_all_vectored) ]
#![ feature(array_methods) ]
#![ feature(get_mut_unchecked) ]
#![ feature(io_slice_advance) ]
#![ feature(box_into_inner) ]

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use bincode::Options;

#[ cfg(feature="sync") ]
pub mod sync_iter;

#[ cfg(feature="sync") ]
pub use sync_iter as iterate;

#[ cfg(not(feature="sync")) ]
pub mod async_iter;

#[ cfg(not(feature="sync")) ]
pub use async_iter as iterate;

#[ cfg(feature="wisckey") ]
mod values;

mod sorted_table;
use sorted_table::{Key, Value};

mod tasks;
mod memtable;

mod logic;
use logic::DbLogic;

mod disk;
mod level;
mod manifest;
mod cond_var;
mod entry;
mod data_blocks;
mod index_blocks;
mod wal;

pub enum WriteOp {
    Put(Key, Value),
    Delete(Key)
}

impl WriteOp {
    const PUT_OP: u8 = 1;
    const DELETE_OP: u8 = 2;

    pub fn get_key(&self) -> &[u8] {
        match self {
            Self::Put(key, _) => key,
            Self::Delete(key) => key
        }
    }

    pub fn get_type(&self) -> u8 {
        match self {
            Self::Put(_, _) => Self::PUT_OP,
            Self::Delete(_) => Self::DELETE_OP
        }
    }

    fn get_key_length(&self) -> u64 {
        match self {
            Self::Put(key, _) | Self::Delete(key) => key.len() as u64
        }
    }

    #[ allow(dead_code) ]
    fn get_value_length(&self) -> u64 {
        match self {
            Self::Put(_, value) => value.len() as u64,
            Self::Delete(_) => 0u64
        }
    }
}
#[ cfg(not(feature="sync")) ]
mod async_api;
#[ cfg(not(feature="sync")) ]
pub use async_api::Database;

#[ cfg(feature="sync") ]
mod sync_api;
#[ cfg(feature="sync") ]
pub use sync_api::Database;

/// Keys and values must be (de-)serializable
pub trait KV_Trait = Send+serde::Serialize+serde::de::DeserializeOwned+'static+Unpin+Clone;

/// A WriteBatch allows to bundle multiple updates together for higher throughput
///
/// Note: The batch will not be applied to the database until it is passed to `Database::write`
pub struct WriteBatch<K: KV_Trait, V: KV_Trait> {
    _marker: PhantomData<fn(K,V)>,
    writes: Vec<WriteOp>,
}

#[ derive(Clone, Debug) ]
pub enum Error {
    Io(String),
    InvalidParams(String),
    Serialization(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Io(msg) => {
                fmt.write_fmt(format_args!("Io Error: {}", msg))?;
            }
            Self::InvalidParams(msg) => {
                fmt.write_fmt(format_args!("Invalid Parameter: {}", msg))?;
            }
            Self::Serialization(msg) => {
                fmt.write_fmt(format_args!("Serialization Error: {}", msg))?;
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
        let inner = Box::into_inner(inner);
        Self::Serialization(inner.to_string())
    }
}

#[ cfg(feature="async-io") ]
impl From<futures_io::Error> for Error {
    fn from(inner: futures_io::Error) -> Self {
        Self::Io(inner.to_string())
    }
}


impl<K: KV_Trait, V: KV_Trait> WriteBatch<K, V> {
    pub fn new() -> Self {
        Self{
            writes: Vec::new(),
            _marker: PhantomData
        }
    }

    /// Record a put operation in the write batch
    /// Will not be applied to the Database until the WriteBatch is written
    pub fn put(&mut self, key: &K, value: &V) {
        let enc = get_encoder();
        self.writes.push(
            WriteOp::Put(enc.serialize(key).unwrap(), enc.serialize(value).unwrap())
        );
    }

    pub fn delete(&mut self, key: &K) {
        let enc = get_encoder();
        self.writes.push(
            WriteOp::Delete(enc.serialize(key).unwrap())
        );
    }
}

impl<K: KV_Trait, V: KV_Trait> Default for WriteBatch<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Allows specifying details of a write
pub struct WriteOptions {
    /// Should the call block until it is guaranteed to be written to disk?
    pub sync: bool
}

impl WriteOptions {
    pub const fn new() -> Self {
        Self{ sync: true }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Allow specifying how the datastore behaves during startup
#[ derive(Debug, Clone) ]
pub enum StartMode {
    /// Reuse existing database, or create if non-existent
    CreateOrOpen,
    /// Open existing database, or fail if non-existent
    Open,
    /// Create a new, or override an existing, database
    CreateOrOverride
}

/// Parameters to customize the creation of the database
#[ derive(Debug, Clone) ]
pub struct Params {
    /// Where in the filesystem should the databasse be stored?
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
}

impl Default for Params {
    fn default() -> Self {
        Self {
            db_path: Path::new("./storage.lsm").to_path_buf(),
            max_memtable_size: 64*1024,
            num_levels: 5,
            max_open_files: 1000,
            max_key_block_size: 1024,
            block_restart_interval: 16,
        }
    }
}

#[inline]
fn get_encoder() ->
        bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian> {
    // Use BigEndian to make integers sortable properly
    bincode::options().with_big_endian()
}


