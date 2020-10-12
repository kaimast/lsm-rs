use std::sync::{Arc, Condvar, Mutex};

use crate::sstable::Key;
use crate::values::Value;

use crate::DbLogic;

pub struct TaskManager<K: Key, V: Value> {
    datastore: Arc<DbLogic<K, V>>,
    state_change: Mutex<bool>,
    sc_condition: Condvar
}

impl<K: Key, V: Value> TaskManager<K, V> {
    pub fn new(datastore: Arc<DbLogic<K,V>>) -> Self {
        let state_change = Mutex::new(false);
        let sc_condition = Condvar::new();



        Self{ datastore, state_change, sc_condition }
    }

    pub fn wake_up(&self) {
    }

    pub fn work_loop(tasks: Arc<TaskManager<K,V>>) {
        log::trace!("Task work loop started");

        while tasks.datastore.is_running() {


        }

        log::trace!("Task work loop shut down");
    }
}
