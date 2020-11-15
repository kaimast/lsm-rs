use std::sync::Arc;

use crate::values::ValueId;
use crate::sorted_table::{Key, TableIterator};
use crate::memtable::MemtableIterator;

pub struct DbIterator {
    mem_iters: Vec<MemtableIterator>
/*
    level_0_iters: Vec<TableIterator>,
    level_iters: Vec<TableIterator>,
    level_tables: Vec<Arc< >>
*/
}

impl Iterator for DbIterator {
    type Item = (Key, ValueId);

    fn next(&mut self) -> Option<Self::Item> {
        todo!();
    }
}
