use std::sync::Arc;

use serde::{Serialize, Deserialize};

use crate::Params;

use bincode::Options;

#[ derive(Serialize, Deserialize) ]
struct Inner {
    levels: Vec<Vec<usize>>
}

/// Keeps track of the LSM meta-data
/// Will persist to disk
pub struct Manifest {
    params: Arc<Params>,
    inner: Inner
}

impl Manifest {
    /// Create new manifest for an empty database
    pub fn new(params: Arc<Params>) -> Self {
        let inner = Inner{ levels: vec![] };
        Self{ params, inner }
    }

    pub fn store(&self) {
        let data = super::get_encoder().serialize(&self.inner).unwrap();
        let manifest_path = self.params.db_path.join(std::path::Path::new("MANIFEST"));

        std::fs::write(&manifest_path, data).expect("Failed to store manifest");
    }
}

