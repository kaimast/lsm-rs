use std::path::{Path, PathBuf};

/// Parameters to customize the creation of the database
#[derive(Debug, Clone)]
pub struct Params {
    /// Where in the filesystem should the database be stored?
    pub db_path: PathBuf,
    /// Maximum size of a memtable (keys+values),
    /// This indirectly also defines how large a value block can be
    pub max_memtable_size: usize,
    /// How many levels does this store have (default: 5)
    pub num_levels: usize,
    /// How many open files should be held in memory?
    pub max_open_files: usize,
    /// Maximum number of entries in a key block
    pub max_key_block_size: usize,
    /// How often should the full key be stored in a data block?
    /// Larger numbers result in smaller on-disk files, but seeks will be slower
    pub block_restart_interval: u32,
    /// Write the size of each level to a csv file
    pub log_level_stats: Option<String>,
    /// How many concurrent compaction tasks should there be
    pub compaction_concurrency: usize,
    /// How many seeks (per kb) before compaction is triggered?
    pub seek_based_compaction: Option<u32>,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            db_path: Path::new("./storage.lsm").to_path_buf(),
            max_memtable_size: 5 * 1024 * 1024,
            num_levels: 5,
            max_open_files: 1_000_000,
            max_key_block_size: 512,
            block_restart_interval: 16,
            log_level_stats: None,
            compaction_concurrency: 4,
            seek_based_compaction: Some(10),
        }
    }
}


