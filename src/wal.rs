use std::sync::Arc;

use crate::{Key, Params};
use crate::sorted_table::Value;

use std::path::Path;
use std::fs::File;
use std::io::{IoSlice, Write};

pub struct WriteAheadLog{
    #[ allow(dead_code) ]
    params: Arc<Params>,
    log_file: File
}

pub enum WriteOp {
    Put(Key, Value),
    Delete(Key)
}

impl WriteOp {
    const PUT_OP: u8 = 1;
    const DELETE_OP: u8 = 2;

    fn get_type(&self) -> u8 {
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

    fn get_value_length(&self) -> u64 {
        match self {
            Self::Put(_, value) => value.len() as u64,
            Self::Delete(_) => 0u64
        }
    }
}

impl WriteAheadLog{
    pub fn new(params: Arc<Params>) -> Self {
        let fpath = params.db_path.join(Path::new("LOG"));
        let log_file = File::create(fpath).expect("Failed to open log file");

        Self{ params, log_file }
    }

    pub fn store(&mut self, op: &WriteOp) {
        // we do not use serde here to avoid copying data

        let op_type = op.get_type().to_le_bytes();
        let klen = op.get_key_length().to_le_bytes();
        let vlen = op.get_value_length().to_le_bytes();

        let mut write_vector = match op {
            WriteOp::Put(key, value) => {
                vec![
                    IoSlice::new(op_type.as_slice()),
                    IoSlice::new(klen.as_slice()),
                    IoSlice::new(vlen.as_slice()),
                    IoSlice::new(key),
                    IoSlice::new(value)
                ]
            }
            WriteOp::Delete(key) => {
                vec![
                    IoSlice::new(op_type.as_slice()),
                    IoSlice::new(klen.as_slice()),
                    IoSlice::new(key)
                ]
            }
        };

        // Try doing one write syscall if possible
        self.log_file.write_all_vectored(&mut write_vector[..]).expect("Failed to write to log file");
    }

    pub fn sync(&mut self) {
        self.log_file.sync_data().expect("Failed to sync log file!");
    }

    /// Once the memtable has been flushed we can remove all log entries
    #[ allow(dead_code)]
    pub fn clear(&mut self) {
        todo!();
    }
}
