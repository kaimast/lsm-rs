use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;

use super::KV_Trait;

use tokio::task::JoinHandle;
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
}

struct TaskHandle {
    stop_flag: Arc<AtomicBool>,
    task: Box<dyn Task>,
    last_change: Mutex<Instant>,
    sc_condition: Condvar,
}

/// This structure manages background tasks
/// Currently there is only compaction, but there might be more in the future
pub struct TaskManager {
    stop_flag: Arc<AtomicBool>,
    tasks: HashMap<TaskType, (StdMutex<JoinHandle<Result<(), Error>>>, Arc<TaskHandle>)>
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

    async fn work_loop(&self) -> Result<(), Error> {
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

            let did_work = self.task.run().await?;

            if did_work {
                last_update = Instant::now();
                idle = false;
            } else {
                idle = true;
            }
        }

        log::trace!("Task work loop ended");
        Ok(())
    }
}

impl TaskManager {
    pub async fn new<K: KV_Trait, V: KV_Trait>(datastore: Arc<DbLogic<K,V>>) -> Self {
        let mut tasks = HashMap::default();
        let stop_flag = Arc::new(AtomicBool::new(false));

        let hdl = Arc::new(TaskHandle::new(stop_flag.clone(), CompactionTask::new_boxed(datastore) ));
        let future = {
            let hdl = hdl.clone();

            StdMutex::new( tokio::spawn(async move {
                hdl.work_loop().await
            }) )
        };

        tasks.insert(TaskType::Compaction, (future, hdl));

        Self{ stop_flag, tasks }
    }

    pub async fn wake_up(&self, task_type: &TaskType) {
        let (_fut, hdl) = self.tasks.get(task_type).expect("No such task");
        hdl.wake_up().await;
    }

    pub fn terminate(&self) {
        self.stop_flag.store(false, Ordering::SeqCst);

        for (_, (fut, _hdl)) in self.tasks.iter() {
            let locked = fut.lock().unwrap();
            locked.abort();
        }
    }

    pub async fn stop_all(&self) -> Result<(), Error> {
        log::trace!("Stopping all background tasks");

        self.stop_flag.store(true, Ordering::SeqCst);

        for (_, (_fut, hdl)) in self.tasks.iter() {
            hdl.sc_condition.notify_all();
        }

        for (_, (fut, _hdl)) in self.tasks.iter() {
            let mut locked = fut.lock().unwrap();
            match (&mut *locked).await {
                Ok(res) => { res?; }
                Err(_) => { /* ignore */ },
            }
        }

        Ok(())
    }
}
