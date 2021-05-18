use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use super::KV_Trait;

use tokio::sync::Mutex;

use crate::cond_var::Condvar;
use crate::{DbLogic, Error};

use async_trait::async_trait;

#[ async_trait ]
pub trait Task: Sync+Send {
    async fn run(&self) -> Result<bool, Error>;
}

#[ derive(Debug, PartialEq, Eq, Hash) ]
pub enum TaskType {
    Compaction,
    #[ cfg(feature="wisckey-reinsert") ]
    GarbageCollect
}

struct TaskHandle {
    stop_flag: Arc<AtomicBool>,
    task: Box<dyn Task>,
    last_change: Mutex<Instant>,
    sc_condition: Condvar
}

pub struct TaskManager {
    stop_flag: Arc<AtomicBool>,
    tasks: HashMap<TaskType, Arc<TaskHandle>>
}

struct CompactionTask<K: KV_Trait, V: KV_Trait> {
   datastore: Arc<DbLogic<K, V>>
}

impl<K: KV_Trait, V: KV_Trait> CompactionTask<K, V> {
    fn new_boxed(datastore: Arc<DbLogic<K, V>>) -> Box<dyn Task> {
        Box::new(Self{ datastore })
    }
}

#[ async_trait ]
impl<K: KV_Trait, V: KV_Trait> Task for CompactionTask<K, V> {
    async fn run(&self) -> Result<bool, Error> {
        Ok( self.datastore.do_compaction().await? )
    }
}

// Folding is done as part of compaction
// So we don't need an extra task for that
#[ cfg(feature="wisckey-reinsert") ]
struct GarbageCollectTask {

}

#[ cfg(feature="wisckey-reinsert") ]
impl GarbageCollectTask {
    fn new_boxed() -> Box<dyn Task> {
        Box::new(Self{})
    }
}

#[ cfg(feature="wisckey-reinsert") ]
#[ async_trait ]
impl Task for GarbageCollectTask {
    async fn run(&self) -> Result<bool, Error> {
        Ok( false )
    }
}

impl TaskHandle {
    fn new(stop_flag: Arc<AtomicBool>, task: Box<dyn Task>) -> Self {
        let last_change = Mutex::new(Instant::now());
        let sc_condition = Condvar::new();

        Self{ stop_flag, task, last_change, sc_condition }
    }

    async fn wake_up(&self) {
        let mut last_change = self.last_change.lock().await;
        *last_change = Instant::now();
        self.sc_condition.notify_one();
    }

    #[ inline(always) ]
    fn is_running(&self) -> bool {
        !self.stop_flag.load(Ordering::SeqCst)
    }

    async fn work_loop(&self) {
        log::trace!("Task work loop started");
        let mut last_update = Instant::now();
        let mut idle = false;

        loop {
            {
                let mut lchange = self.last_change.lock().await;

                while self.is_running() && idle && *lchange < last_update {
                    lchange = self.sc_condition.wait(lchange, &self.last_change).await;
                }
            }

            if !self.is_running() {
                break;
            }

            let did_work = match self.task.run().await {
                Ok(res) => res,
                Err(err) => {
                    log::error!("Background task failed: {}", err);
                    break;
                }
            };

            if did_work {
                last_update = Instant::now();
                idle = false;
            } else {
                idle = true;
            }
        }

        log::trace!("Task work loop ended");
    }
}

impl TaskManager {
    pub async fn new<K: KV_Trait, V: KV_Trait>(datastore: Arc<DbLogic<K,V>>) -> Self {
        let mut tasks = HashMap::default();
        let stop_flag = Arc::new(AtomicBool::new(false));

        tasks.insert(TaskType::Compaction,
                     Arc::new(TaskHandle::new(
                             stop_flag.clone(), CompactionTask::new_boxed(datastore)
                     )));

        #[ cfg(feature="wisckey-reinsert") ]
        tasks.insert(TaskType::GarbageCollect,
                     Arc::new(TaskHandle::new(
                             stop_flag.clone(), GarbageCollectTask::new_boxed()
                     )));

        // Spawn all tasks
        for (_, task) in tasks.iter() {
            let task = task.clone();

            tokio::spawn(async move {
                task.work_loop().await;
            });
        }

        Self{ stop_flag, tasks }
    }

    pub async fn wake_up(&self, task_type: &TaskType) {
        let task = self.tasks.get(task_type).expect("No such task");
        task.wake_up().await;
    }

    pub fn stop_all(&self) {
        self.stop_flag.store(false, Ordering::SeqCst);

        for (_, task) in self.tasks.iter() {
            task.sc_condition.notify_all();
        }
    }
}
