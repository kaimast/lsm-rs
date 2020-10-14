use serde::{Serialize, de::DeserializeOwned};

use crate::entry::Entry;

pub trait Key = Ord+Serialize+DeserializeOwned+Send+Sync;

pub struct SSTable<K: Key> {
    entries: Vec<Entry<K>>
}
