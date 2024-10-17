#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(trivial_bounds)]
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
use sorted_table::{Key, Value};

mod level_logger;

pub mod memtable;
pub mod tasks;

pub mod logic;
use logic::DbLogic;
pub use logic::EntryRef;

pub mod manifest;

mod data_blocks;
mod database;
mod disk;
mod index_blocks;
mod level;
mod wal;

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
