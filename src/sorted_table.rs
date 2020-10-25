use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::entry::Entry;
use crate::values::ValueId;

pub trait Key = Ord+Serialize+DeserializeOwned+Send+Sync+Clone;

#[ derive(Serialize, Deserialize) ]
struct PrefixedKey {
    prefix_len: usize,
    suffix: Vec<u8>
}

pub struct SortedTable<K: Key> {
    min: K,
    max: K,
    entries: Vec<(PrefixedKey, Entry)>
}

impl<K: Key> SortedTable<K> {
    pub fn new(mut entries: Vec<(K, Entry)>) -> Self {
        if entries.is_empty() {
            panic!("Cannot create empty table");
        }

        entries.sort_by(|elem1, elem2| -> std::cmp::Ordering {
            if elem1.0 == elem2.0 {
                // If keys are equal, sort by sequence number
                elem1.1.seq_number.cmp(&elem2.1.seq_number)
            } else {
                elem1.0.cmp(&elem2.0)
            }
        });

        let min = entries[0].0.clone();
        let max = entries[entries.len()-1].0.clone();

        let mut prefixed_entries = Vec::new();
        let mut last_kdata = vec![];
        for (key, entry) in entries.drain(..) {
            let kdata = bincode::serialize(&key).expect("Failed to serialize key");
            let mut prefix_len = 0;

            while prefix_len < kdata.len() && prefix_len < last_kdata.len()
                && kdata[prefix_len] == last_kdata[prefix_len] {
                prefix_len += 1;
            }

            let suffix = kdata[prefix_len..].to_vec();

            prefixed_entries.push((PrefixedKey{prefix_len, suffix}, entry));
            last_kdata = kdata;
        }

        Self{ entries: prefixed_entries, min, max }
    }

    #[inline]
    pub fn get_min(&self) -> &K {
        &self.min
    }

    #[inline]
    pub fn get_max(&self) -> &K {
        &self.max
    }

    pub fn get(&self, key: &K) -> Option<ValueId> {
        if key < self.get_min() || key > self.get_max() {
            return None;
        }

        let mut last_kdata = vec![];

        for (pkey, entry) in self.entries.iter() {
            let kdata = [&last_kdata[..pkey.prefix_len], &pkey.suffix[..]].concat();
            let this_key: K = bincode::deserialize(&kdata).expect("Failed to deserialize key");

            if &this_key == key {
                return Some(entry.value_ref);
            }

            last_kdata = kdata;
        }

        None
    }
}
