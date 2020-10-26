use std::sync::Arc;

use crate::sorted_table::Key;
use crate::values::Value;
use crate::Params;

use std::path::Path;
use std::fs::File;
use std::io::{IoSlice, Write};

pub struct WriteAheadLog{
    #[ allow(dead_code) ]
    params: Arc<Params>,
    log_file: File
}

impl WriteAheadLog{
    pub fn new(params: Arc<Params>) -> Self {
        let fpath = params.db_path.join(Path::new("LOG"));
        let log_file = File::create(fpath).expect("Failed to open log file");

        Self{ params, log_file }
    }

    pub fn store<K: Key, V: Value>(&mut self, key: &K, value: &V) {
        let kdata = bincode::serialize(key).unwrap();
        let vdata = bincode::serialize(value).unwrap();

        let klen = (kdata.len() as u64).to_le_bytes();
        let vlen = (vdata.len() as u64).to_le_bytes();

        let mut write_vector = vec![
            IoSlice::new(klen.as_slice()),
            IoSlice::new(vlen.as_slice()),
            IoSlice::new(kdata.as_slice()),
            IoSlice::new(vdata.as_slice())
        ];

        self.log_file.write_all_vectored(&mut write_vector[..]).expect("Failed to write to log file");
    }

    pub fn sync(&mut self) {
        self.log_file.sync_data().expect("Failed to sync log file!");
    }

    /// Once the memtable has been flushed we can remove all log entries
    pub fn clear(&mut self) {
        todo!();
    }
}
