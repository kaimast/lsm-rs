#[cfg(feature="wisckey")]
use crate::values::ValueId;

use serde::{Serialize, Deserialize};

#[cfg(feature="wisckey")]
#[derive(Serialize, Deserialize, Clone, Debug,PartialEq)]
pub enum Entry {
    Value {
        value_ref: ValueId,
        seq_number: u64
    },
    Deletion {
        seq_number: u64,
        // not actually used but ensures the enum is always of the same size
        _value_ref: ValueId,
    }
}

#[cfg(not(feature="wisckey"))]
#[derive(Serialize, Deserialize, Clone, Debug,PartialEq)]
pub enum Entry {
    Value {
        seq_number: u64,
        #[ serde(with="serde_bytes") ]
        value: Vec<u8>,
    },
    Deletion {
        seq_number: u64,
    }
}

impl Entry {
    pub fn get_sequence_number(&self) -> u64 {
        match self {
            Self::Value{seq_number, ..} | Self::Deletion{seq_number, ..} => *seq_number
        }
    }

    #[ cfg(not(feature="wisckey")) ]
    #[ allow(dead_code) ]
    pub fn get_value(&self) -> Option<&[u8]> {
        match self {
            Self::Value{value, ..} => Some(&value),
            Self::Deletion{..} => None,
        }
    }

    #[ cfg(feature="wisckey") ]
    #[ allow(dead_code) ]
    pub fn get_value_ref(&self) -> Option<&ValueId> {
        match self {
            Self::Value{value_ref, ..} => Some(value_ref),
            Self::Deletion{..} => None,
        }
    }

}

#[ cfg(feature="wisckey") ]
impl Default for Entry {
    fn default() -> Self {
        Entry::Value{ seq_number: 0, value_ref: (0,0) }
    }
}
