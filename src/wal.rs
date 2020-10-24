use crate::sorted_table::Key;
use crate::values::Value;

pub struct WriteAheadLog {
}

impl WriteAheadLog {
    pub fn new() -> Self {
        Self{}
    }

    pub fn store<K: Key, V: Value>(&mut self, _key: &K, _value: &V) {
        //FIXME
    }
}
