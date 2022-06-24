use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex as StdMutex;

use super::KvTrait;

use tokio::sync::{Mutex, Notify};

use crate::{DbLogic, Error};

use async_trait::async_trait;

#[async_trait]
pub trait Task: Sync + Send {
    async fn run(&self) -> Result<bool, Error>;
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum TaskType {
    Compaction,
}

struct UpdateCond {
    last_change: Mutex<Instant>,
    condition: Notify,
}

struct TaskHandle {
    stop_flag: Arc<AtomicBool>,
    task: Box<dyn Task>,
    update_cond: Arc<UpdateCond>,
}

type JoinHandle = tokio::task::JoinHandle<Result<(), Error>>;

/// This structure manages background tasks
/// Currently there is only compaction, but there might be more in the future
pub struct TaskManager {
    stop_flag: Arc<AtomicBool>,
    tasks: HashMap<TaskType, TaskGroup>,
}

struct TaskGroup {
    condition: Arc<UpdateCond>,
    tasks: Vec<(StdMutex<Option<JoinHandle>>, Arc<TaskHandle>)>,
}

struct CompactionTask<K: KvTrait, V: KvTrait> {
    datastore: Arc<DbLogic<K, V>>,
}

impl<K: KvTrait, V: KvTrait> CompactionTask<K, V> {
    fn new_boxed(datastore: Arc<DbLogic<K, V>>) -> Box<dyn Task> {
        Box::new(Self { datastore })
    }
}

#[async_trait]
impl<K: KvTrait, V: KvTrait> Task for CompactionTask<K, V> {
    async fn run(&self) -> Result<bool, Error> {
        Ok(self.datastore.do_compaction().await?)
    }
}

impl UpdateCond {
    fn new() -> Self {
        Self {
            last_change: Mutex::new(Instant::now()),
            condition: Notify::new(),
        }
    }

    /// Notify the task that there is new work to do
    async fn wake_up(&self) {
        let mut last_change = self.last_change.lock().await;
        *last_change = Instant::now();
        self.condition.notify_one();
    }
}

impl TaskHandle {
    fn new(stop_flag: Arc<AtomicBool>, update_cond: Arc<UpdateCond>, task: Box<dyn Task>) -> Self {
        Self {
            stop_flag,
            update_cond,
            task,
        }
    }

    #[inline(always)]
    fn is_running(&self) -> bool {
        !self.stop_flag.load(Ordering::SeqCst)
    }

    async fn work_loop(&self) -> Result<(), Error> {
        log::trace!("Task work loop started");
        let mut last_update = Instant::now();
        let mut idle = false;

        loop {
            {
                let mut lchange = self.update_cond.last_change.lock().await;

                while self.is_running() && idle && *lchange < last_update {
                    lchange = self.update_cond.condition.wait(lchange).await;
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
    pub async fn new<K: KvTrait, V: KvTrait>(
        datastore: Arc<DbLogic<K, V>>,
        num_compaction_tasks: usize,
    ) -> Self {
        let mut tasks = HashMap::default();
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut compaction_tasks = vec![];
        let update_cond = Arc::new(UpdateCond::new());

        for _ in 0..num_compaction_tasks {
            let hdl = Arc::new(TaskHandle::new(
                stop_flag.clone(),
                update_cond.clone(),
                CompactionTask::new_boxed(datastore.clone()),
            ));
            let future = {
                let hdl = hdl.clone();
                let future = tokio::spawn(async move { hdl.work_loop().await });

                StdMutex::new(Some(future))
            };

            compaction_tasks.push((future, hdl));
        }

        let task_group = TaskGroup {
            tasks: compaction_tasks,
            condition: update_cond,
        };

        tasks.insert(TaskType::Compaction, task_group);

        Self { stop_flag, tasks }
    }

    pub async fn wake_up(&self, task_type: &TaskType) {
        let task_group = self.tasks.get(task_type).expect("No such task");

        task_group.condition.wake_up().await;
    }

    pub fn terminate(&self) {
        self.stop_flag.store(false, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            for (fut, _) in task_group.tasks.iter() {
                if let Some(future) = fut.lock().take() {
                    future.abort();
                }
            }
        }
    }

    pub async fn stop_all(&self) -> Result<(), Error> {
        log::trace!("Stopping all background tasks");

        self.stop_flag.store(true, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            task_group.condition.condition.notify_waiters();
        }

        for (_, task_group) in self.tasks.iter() {
            for (join_hdl, _) in task_group.tasks.iter() {
                let inner = join_hdl.lock().take();

                if let Some(future) = inner {
                    // Ignore already terminated/aborted tasks
                    if let Ok(res) = future.await {
                        res?;
                    }
                }
            }
        }

        Ok(())
    }
}
