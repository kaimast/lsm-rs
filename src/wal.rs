use std::sync::Arc;

use crate::{Key, Params};
use crate::sorted_table::Value;

use std::path::Path;

#[ cfg(feature="async-io") ]
use tokio::fs::File;

#[ cfg(feature="async-io") ]
use tokio::io::AsyncWriteExt;

#[ cfg(not(feature="async-io")) ]
use std::fs::File;

#[ cfg(not(feature="async-io")) ]
use std::io::{IoSlice, Write};

use cfg_if::cfg_if;

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

impl WriteAheadLog{
    pub async fn new(params: Arc<Params>) -> Self {
        let fpath = params.db_path.join(Path::new("LOG"));

        cfg_if! {
            if #[cfg(feature="async-io")] {
                let log_file = File::create(fpath).await.expect("Failed to open log file");
            } else {
                let log_file = File::create(fpath).expect("Failed to open log file");
            }
        }

        Self{ params, log_file }
    }

    pub async fn store(&mut self, op: &WriteOp) {
        // we do not use serde here to avoid copying data

        let op_type = op.get_type().to_le_bytes();

        let key = op.get_key();
        let klen = op.get_key_length().to_le_bytes();

        cfg_if! {
            if #[ cfg(feature="async-io") ] {
                cfg_if! {
                    if #[ cfg(feature="wisckey") ] {
                        // Value will be stored in the vlog, so no need to store it here as well
                        self.log_file.write_all(op_type.as_slice()).await.unwrap();
                        self.log_file.write_all(klen.as_slice()).await.unwrap();
                        self.log_file.write_all(key).await.unwrap();
                    } else {
                        let vlen = op.get_value_length().to_le_bytes();

                        self.log_file.write_all(op_type.as_slice()).await.unwrap();
                        self.log_file.write_all(klen.as_slice()).await.unwrap();
                        self.log_file.write_all(key).await.unwrap();

                        match op {
                            WriteOp::Put(_, value) => {
                                self.log_file.write_all(vlen.as_slice()).await.unwrap();
                                self.log_file.write_all(value).await.unwrap();
                            },
                            WriteOp::Delete(_) => {}
                        }
                    }
                }
            } else {
                let mut buffers = vec![
                    IoSlice::new(op_type.as_slice()),
                    IoSlice::new(klen.as_slice()),
                    IoSlice::new(key)
                ];

                cfg_if! {
                    if #[ cfg(not(feature="wisckey")) ] {
                        let vlen = op.get_value_length().to_le_bytes();

                        match op {
                            WriteOp::Put(_, value) => {
                                buffers.push(IoSlice::new(vlen.as_slice()));
                                buffers.push(IoSlice::new(value));
                            },
                            WriteOp::Delete(_) => {}
                        }

                        self.log_file.write_all_vectored(&mut buffers)
                            .expect("Failed to write to log file");
                    } else {
                        // Try doing one write syscall if possible
                        self.log_file.write_all_vectored(&mut buffers)
                            .expect("Failed to write to log file");
                    }
                }
            }
        }
    }

    pub async fn sync(&mut self) {
        cfg_if! {
            if #[cfg(feature="async-io") ] {
                self.log_file.sync_data().await.expect("Failed to sync log file!");
            } else {
                self.log_file.sync_data().expect("Failed to sync log file!");
            }
        }
    }

    /// Once the memtable has been flushed we can remove all log entries
    #[ allow(dead_code)]
    pub fn clear(&mut self) {
        todo!();
    }
}
