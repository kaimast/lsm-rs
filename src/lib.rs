#![feature(trait_alias)]
#![feature(let_chains)]
#![feature(get_mut_unchecked)]
// Temporary workaround for the io_uring code
#![allow(clippy::arc_with_non_send_sync)]

pub mod iterate;

#[cfg(feature = "wisckey")]
pub mod values;

mod params;
pub use params::Params;

mod write_batch;
pub use write_batch::{WriteBatch, WriteOp, WriteOptions};

pub mod sorted_table;

mod level_logger;

pub mod memtable;
pub mod tasks;

pub mod logic;
pub use logic::EntryRef;

pub mod manifest;

mod data_blocks;
mod database;
mod disk;
mod index_blocks;
mod level;
mod wal;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// Shorthand for a list of key-value pairs
#[cfg(feature = "wisckey")]
type EntryList = Vec<(Key, Value)>;

pub use database::Database;

/// How many bytes do we align by?
const WORD_SIZE: usize = 8;

fn pad_offset(offset: usize) -> usize {
    offset + compute_padding(offset)
}

fn compute_padding(offset: usize) -> usize {
    let remainder = offset % WORD_SIZE;
    if remainder == 0 {
        0
    } else {
        WORD_SIZE - remainder
    }
}

fn add_padding(data: &mut Vec<u8>) {
    let padding = compute_padding(data.len());
    if padding > 0 {
        data.resize(data.len() + padding, 0u8);
    }
}

#[derive(Clone, Debug)]
pub enum Error {
    Io { context: String, message: String },
    InvalidParams(String),
    Serialization(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::Io { context, message } => {
                fmt.write_fmt(format_args!("{context}: {message}"))?;
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

impl Error {
    fn from_io_error<S: ToString>(context: S, inner: std::io::Error) -> Self {
        Self::Io {
            context: context.to_string(),
            message: format!("{inner}"),
        }
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
