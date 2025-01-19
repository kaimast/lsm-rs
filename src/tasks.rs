use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use tokio::sync::Notify;

use crate::Error;
use crate::logic::DbLogic;

use async_trait::async_trait;

#[cfg(feature = "_async-io")]
#[async_trait(?Send)]
pub trait Task {
    async fn run(&self) -> Result<bool, Error>;
}

#[cfg(not(feature = "_async-io"))]
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
}

/// Keeps track of a condition variables shared within a task group
struct UpdateCond {
    last_change: RwLock<Instant>,
    condition: Notify,
}

struct MemtableCompactionTask {
    datastore: Arc<DbLogic>,
    level_update_cond: Arc<UpdateCond>,
}

struct LevelCompactionTask {
    datastore: Arc<DbLogic>,
}

impl MemtableCompactionTask {
    fn new_boxed(datastore: Arc<DbLogic>, level_update_cond: Arc<UpdateCond>) -> Box<dyn Task> {
        Box::new(Self {
            datastore,
            level_update_cond,
        })
    }
}

impl LevelCompactionTask {
    fn new_boxed(datastore: Arc<DbLogic>) -> Box<dyn Task> {
        Box::new(Self { datastore })
    }
}

#[cfg_attr(feature="_async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "_async-io"), async_trait)]
impl Task for MemtableCompactionTask {
    async fn run(&self) -> Result<bool, Error> {
        let did_work = self.datastore.do_memtable_compaction().await?;
        if did_work {
            self.level_update_cond.wake_up();
        }
        Ok(did_work)
    }
}

#[cfg_attr(feature="_async-io", async_trait(?Send))]
#[cfg_attr(not(feature = "_async-io"), async_trait)]
impl Task for LevelCompactionTask {
    async fn run(&self) -> Result<bool, Error> {
        Ok(self.datastore.do_level_compaction().await?)
    }
}

impl UpdateCond {
    fn new() -> Self {
        Self {
            last_change: RwLock::new(Instant::now()),
            condition: Default::default(),
        }
    }

    /// Notify the task that there is new work to do
    fn wake_up(&self) {
        let mut last_change = self.last_change.write();
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
            let now = Instant::now();

            loop {
                let fut = self.update_cond.condition.notified();
                tokio::pin!(fut);

                {
                    let lchange = self.update_cond.last_change.read();

                    if !self.is_running() || !idle || *lchange > last_update {
                        break;
                    }

                    // wait for change to queue and retry
                    fut.as_mut().enable();
                }

                fut.await;
            }

            if !self.is_running() {
                break;
            }

            let did_work = self.task.run().await.expect("Task failed");
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
    pub async fn new(datastore: Arc<DbLogic>, num_compaction_tasks: usize) -> Self {
        let mut tasks = HashMap::default();
        let stop_flag = Arc::new(AtomicBool::new(false));

        let level_update_cond = Arc::new(UpdateCond::new());

        #[cfg(feature = "tokio-uring")]
        let mut spawn_pos = kioto_uring_executor::new_spawn_ring();

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
                    if #[cfg(feature="tokio-uring")] {
                        unsafe {
                            kioto_uring_executor::unsafe_spawn_at(spawn_pos.get(), task);
                            spawn_pos.advance();
                        }
                    } else if #[cfg(feature="monoio")] {
                        monoio::spawn(task);
                    } else {
                        tokio::spawn(task);
                    }
                }
            }

            let task_group = TaskGroup {
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
                        if #[cfg(feature="tokio-uring")] {
                            unsafe {
                                kioto_uring_executor::unsafe_spawn_at(spawn_pos.get(), task);
                                spawn_pos.advance();
                            }
                        } else if #[cfg(feature="monoio")] {
                            monoio::spawn(task);
                        } else {
                            tokio::spawn(task);
                        }
                    }
                }

                compaction_tasks.push(Mutex::new(hdl));
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
    pub fn wake_up(&self, task_type: &TaskType) {
        let task_group = self.tasks.get(task_type).expect("No such task");
        task_group.condition.wake_up();
    }

    pub fn terminate(&self) {
        self.stop_flag.store(false, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            task_group.condition.condition.notify_one();
        }
    }

    pub async fn stop_all(&self) -> Result<(), Error> {
        log::trace!("Stopping all background tasks");

        self.stop_flag.store(true, Ordering::SeqCst);

        for (_, task_group) in self.tasks.iter() {
            task_group.condition.condition.notify_waiters();
        }

        Ok(())
    }
}
