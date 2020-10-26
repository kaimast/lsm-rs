use std::sync::Arc;

use crate::Params;

/// Keeps track of the LSM meta-data
/// Will persist to disk
pub struct Manifest {
    params: Arc<Params>,
    levels: Vec<Vec<usize>>
}

impl Manifest {
    /// Create new manifest for an empty database
    pub fn new(params: Arc<Params>) -> Self {
        Self{ params, levels: vec![] }
    }
}
