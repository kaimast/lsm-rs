#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(const_option)]
// Temporary workaround for the io_uring code
#![allow(clippy::arc_with_non_send_sync)]

use bincode::Options;
use cfg_if::cfg_if;

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

mod params;
pub use params::Params;

mod write_batch;
pub use write_batch::{WriteBatch, WriteOp, WriteOptions};

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

cfg_if! {
    if #[cfg(feature = "sync")] {
        mod sync_api;
        pub use sync_api::Database;
    } else {
        mod async_api;
        pub use async_api::Database;
    }
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

fn get_encoder(
) -> bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian> {
    // Use BigEndian to make integers sortable properly
    bincode::options().with_big_endian()
}
