mod values;
use values::{Value, ValueLog};

mod sstable;
use sstable::Key;

struct PendingEntry<K: Key> {
    key: K,
    value_pos: usize
}

struct Memtable<K: Key> {
    entries: Vec<PendingEntry<K>>
}

struct Datastore<K: Key, V: Value> {
    memtable: Memtable<K>,
    value_log: ValueLog<V>
}
