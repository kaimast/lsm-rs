use crate::values::ValueId;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub value_ref: ValueId,
    pub seq_number: u64
}
