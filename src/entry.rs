use crate::values::ValueId;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Entry {
    pub value_ref: ValueId,
    pub seq_number: u64
}
