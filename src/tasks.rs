use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex as StdMutex;

use super::KvTrait;

use tokio::sync::Mutex;
use tokio_condvar::Condvar;

use crate::{DbLogic, Error};

use async_trait::async_trait;

#[cfg(feature = "async-io")]
#[async_trait(?Send)]
pub trait Task {
    async fn run(&self) -> Result<bool, Error>;
}

#[cfg(not(feature = "async-io"))]
#[async_trait]
pub trait Task: Sync + Send {
    async fn run(&self) -> Result<bool, Error>;
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum TaskType {
    MemtableCompaction,
    LevelCompaction,
}

struct TaskHandle {
    stop_flag: Arc<AtomicBool>,
    task: Box<dyn Task>,
    update_cond: Arc<UpdateCond>,
}

/// This structure manages background tasks
/// Currently there is only compaction, but there might be more in the future
pub struct TaskManager {
    stop_flag: Arc<AtomicBool>,
    tasks: HashMap<TaskType, TaskGroup>,
}

/// Holds a group of tasks that do the same thing
/// e.g., all compaction tasks
struct TaskGroup {
    condition: Arc<UpdateCond>,
//    #[allow(dead_code)]
//    tasks: Vec<StdMutex<Arc<TaskHandle>>>,
}

/// Keeps track of a condition variables shared within a task group
struct UpdateCond {
    last_change: Mutex<Instant>,
    condition: Condvar,
}

struct MemtableCompactionTask<K: KvTrait, V: KvTrait> {
    datastore: Arc<DbLogic<K, V>>,
    level_update_cond: Arc<UpdateCond>,
}

struct LevelCompactionTask<K: KvTrait, V: KvTrait> {
    datastore: Arc<DbLogic<K, V>>,
}

impl<K: KvTrait, V: KvTrait> MemtableCompactionTask<K, V> {
    fn new_boxed(
        datastore: Arc<DbLogic<K, V>>,
        level_update_cond: Arc<UpdateCond>,
    ) -> Box<dyn Task> {
        Box::new(Self {
            datastore,
            level_update_cond,
        })
    }
}

impl<K: KvTrait, V: KvTrait> LevelCompactionTask<K, V> {
    fn new_boxed(datastore: Arc<DbLogic<K, V>>) -> Box<dyn Task> {
        Box::new(Self { datastore })
    }
}

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
impl<K: KvTrait, V: KvTrait> Task for MemtableCompactionTask<K, V> {
    async fn run(&self) -> Result<bool, Error> {
        let did_work = self.datastore.do_memtable_compaction().await?;
        if did_work {
            self.level_update_cond.wake_up().await;
        }
        Ok(did_work)
    }
}

#[cfg_attr(feature="async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "async-io"), async_trait)]
impl<K: KvTrait, V: KvTrait> Task for LevelCompactionTask<K, V> {
    async fn run(&self) -> Result<bool, Error> {
        Ok(self.datastore.do_level_compaction().await?)
    }
}

impl UpdateCond {
    fn new() -> Self {
        Self {
            last_change: Mutex::new(Instant::now()),
            condition: Condvar::new(),
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

    async fn work_loop(&self) {
        log::trace!("Task work loop started");
        let mut last_update = Instant::now();

        // Indicates whether work was done in the last iteration
        let mut idle = false;

        loop {
            // Record the time we last checked for changes
            let now = Instant::now();

            if idle {
                let mut lchange = self.update_cond.last_change.lock().await;

                while self.is_running() && idle && *lchange < last_update {
                    lchange = self.update_cond.condition.wait(lchange).await;
                }
            }

            if !self.is_running() {
                break;
            }

            let did_work = self.task.run().await
                .expect("Task failed");
            last_update = now;

            if did_work {
                idle = false;
            } else {
                log::trace!("Task did not do any work");
                idle = true;
            }
        }

        log::trace!("Task work loop ended");
    }
}

impl TaskManager {
    pub async fn new<K: KvTrait, V: KvTrait>(
        datastore: Arc<DbLogic<K, V>>,
        num_compaction_tasks: usize,
    ) -> Self {
        let mut tasks = HashMap::default();
        let stop_flag = Arc::new(AtomicBool::new(false));

        let level_update_cond = Arc::new(UpdateCond::new());

        {
            let memtable_update_cond = Arc::new(UpdateCond::new());

            let hdl = Arc::new(TaskHandle::new(
                stop_flag.clone(),
                memtable_update_cond.clone(),
                MemtableCompactionTask::new_boxed(datastore.clone(), level_update_cond.clone()),
            ));
            {
                let hdl = hdl.clone();
                let task = async move { hdl.work_loop().await };

                cfg_if::cfg_if! {
                    if #[cfg(feature="async-io")] {
                        unsafe {
                            tokio_uring_executor::unsafe_spawn(task);
                        }
                    } else {
                        tokio::spawn(task);
                    }
                }
            }

            let task_group = TaskGroup {
                //tasks: vec![StdMutex::new(hdl)],
                condition: memtable_update_cond,
            };

            tasks.insert(TaskType::MemtableCompaction, task_group);
        }

        {
            let mut compaction_tasks = vec![];

            for _ in 0..num_compaction_tasks {
                let hdl = Arc::new(TaskHandle::new(
                    stop_flag.clone(),
                    level_update_cond.clone(),
                    LevelCompactionTask::new_boxed(datastore.clone()),
                ));
                {
                    let hdl = hdl.clone();
                    let task = async move { hdl.work_loop().await };

                    cfg_if::cfg_if! {
                        if #[cfg(feature="async-io")] {
                            unsafe {
                                tokio_uring_executor::unsafe_spawn(task);
                            }
                        } else {
                            tokio::spawn(task);
                        }
                    }
                }

                compaction_tasks.push(StdMutex::new(hdl));
            }

            let task_group = TaskGroup {
                //tasks: compaction_tasks,
                condition: level_update_cond,
            };

            tasks.insert(TaskType::LevelCompaction, task_group);
        }

        Self { stop_flag, tasks }
    }

    #[tracing::instrument(skip(self))]
    pub async fn wake_up(&self, task_type: &TaskType) {
        let task_group = self.tasks.get(task_type).expect("No such task");
        task_group.condition.wake_up().await;
    }

    pub fn terminate(&self) {
        self.stop_flag.store(false, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            task_group.condition.condition.notify_all();
        }

    /*
        for (_, task_group) in self.tasks.iter() {
            for (fut, _) in task_group.tasks.iter() {
                if let Some(future) = fut.lock().take() {
                    future.abort();
                }
            }
        }*/
    }

    pub async fn stop_all(&self) -> Result<(), Error> {
        log::trace!("Stopping all background tasks");

        self.stop_flag.store(true, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            task_group.condition.condition.notify_all();
        }
/*
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
        }*/

        Ok(())
    }
}
