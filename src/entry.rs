use crate::values::ValueId;
use crate::sorted_table::Key;

pub struct Entry<K: Key> {
    pub key: K,
    pub value_ref: ValueId,
    pub seq_number: u64
}

impl<K: Key> PartialEq for Entry<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq_number == other.seq_number
    }
}

impl<K: Key> Eq for Entry<K> {}

impl<K: Key> PartialOrd for Entry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.key == other.key {
            Some( self.seq_number.cmp(&other.seq_number) )
        } else {
            Some(self.key.cmp(&other.key))
        }
    }
}

impl<K: Key> Ord for Entry<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(&other).unwrap()
    }
}


