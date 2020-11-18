use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use super::KV_Trait;

use crate::DbLogic;

pub trait Task {
    fn wake_up(&self);
}

pub struct TaskManager<K: KV_Trait, V: KV_Trait> {
    datastore: Arc<DbLogic<K, V>>,
    last_change: Mutex<Instant>,
    sc_condition: Condvar
}

impl<K: KV_Trait, V: KV_Trait>TaskManager<K, V> {
    pub fn new(datastore: Arc<DbLogic<K,V>>) -> Self {
        let last_change = Mutex::new(Instant::now());
        let sc_condition = Condvar::new();

        Self{ datastore, last_change, sc_condition }
    }

    pub fn wake_up(&self) {
        let mut last_change = self.last_change.lock().unwrap();
        *last_change = Instant::now();
        self.sc_condition.notify_one();
    }

    pub fn work_loop(tasks: Arc<TaskManager<K, V>>) {
        log::trace!("Task work loop started");
        let mut last_update = Instant::now();
        let mut idle = false;

        while tasks.datastore.is_running() {
            let mut lchange = tasks.last_change.lock().unwrap();

            while idle && *lchange < last_update {
                lchange = tasks.sc_condition.wait(lchange).unwrap();
            }
            drop(lchange);

            if tasks.datastore.do_compaction() {
                last_update = Instant::now();
                idle = false;
            } else {
                idle = true;
            }
        }

        log::trace!("Task work loop shut down");
    }
}
