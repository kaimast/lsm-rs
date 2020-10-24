use serde::{Serialize, Deserialize};

use crate::sorted_table::Key;
use crate::values::Value;

#[derive(Serialize,Deserialize,Debug)]
pub enum Operation<V: Value, K: Key> {
    Put {
        #[serde(deserialize_with = "K::deserialize")]
        key: K,
        #[serde(deserialize_with = "V::deserialize")]
        value: V
    }
}
