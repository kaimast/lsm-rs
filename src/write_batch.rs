use std::marker::PhantomData;

use crate::{get_encoder, Key, KvTrait, Value};

use bincode::Options;

#[derive(Debug)]
pub enum WriteOp {
    Put(Key, Value),
    Delete(Key),
}

/// A WriteBatch allows to bundle multiple updates together for higher throughput
///
/// Note: The batch will not be applied to the database until it is passed to `Database::write`
#[derive(Debug)]
pub struct WriteBatch<K: KvTrait, V: KvTrait> {
    _marker: PhantomData<fn(K, V)>,
    pub(crate) writes: Vec<WriteOp>,
}

impl WriteOp {
    pub(crate) const PUT_OP: u8 = 1;
    pub(crate) const DELETE_OP: u8 = 2;

    pub fn get_key(&self) -> &[u8] {
        match self {
            Self::Put(key, _) => key,
            Self::Delete(key) => key,
        }
    }

    pub fn get_type(&self) -> u8 {
        match self {
            Self::Put(_, _) => Self::PUT_OP,
            Self::Delete(_) => Self::DELETE_OP,
        }
    }

    pub(crate) fn get_key_length(&self) -> u64 {
        match self {
            Self::Put(key, _) | Self::Delete(key) => key.len() as u64,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_value_length(&self) -> u64 {
        match self {
            Self::Put(_, value) => value.len() as u64,
            Self::Delete(_) => 0u64,
        }
    }
}

impl<K: KvTrait, V: KvTrait> WriteBatch<K, V> {
    pub fn new() -> Self {
        Self {
            writes: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Record a put operation in the write batch
    /// Will not be applied to the Database until the WriteBatch is written
    pub fn put(&mut self, key: &K, value: &V) {
        let enc = get_encoder();
        self.writes.push(WriteOp::Put(
            enc.serialize(key).unwrap(),
            enc.serialize(value).unwrap(),
        ));
    }

    pub fn delete(&mut self, key: &K) {
        let enc = get_encoder();
        self.writes
            .push(WriteOp::Delete(enc.serialize(key).unwrap()));
    }
}

impl<K: KvTrait, V: KvTrait> Default for WriteBatch<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Allows specifying details of a write
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Should the call block until it is guaranteed to be written to disk?
    pub sync: bool,
}

impl WriteOptions {
    pub const fn new() -> Self {
        Self { sync: true }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}
