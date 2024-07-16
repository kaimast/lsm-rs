#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(const_option)]
// Temporary workaround for the io_uring code
#![allow(clippy::arc_with_non_send_sync)]

use bincode::Options;

pub mod iterate;

#[cfg(feature = "wisckey")]
pub mod values;

mod params;
pub use params::Params;

mod write_batch;
pub use write_batch::{WriteBatch, WriteOp, WriteOptions};

pub mod sorted_table;
use sorted_table::{Key, Value};

mod level_logger;

pub mod memtable;
pub mod tasks;

pub mod logic;
use logic::DbLogic;

pub mod manifest;

mod data_blocks;
mod database;
mod disk;
mod index_blocks;
mod level;
mod wal;

pub use database::Database;

/// Use 8 byte word aligment for on disk storage to make lookups more efficient
/// and ensure interoperability with rkyv
const WORD_ALIGNMENT: usize = 8;

/// Move the offset within a buffer to account for word alignment
fn align_offset(offset: usize) -> usize {
    offset + (offset % WORD_ALIGNMENT)
}

/// Keys and values must be (de-)serializable
pub trait KvTrait = Send
    + serde::Serialize
    + serde::de::DeserializeOwned
    + 'static
    + Unpin
    + Clone
    + std::fmt::Debug;

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

pub fn get_encoder(
) -> bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian> {
    // Use BigEndian to make integers sortable properly
    bincode::options().with_big_endian()
}
