use std::fs::File;
use std::time::Instant;

use crate::manifest::LevelId;

use parking_lot::Mutex;

struct Inner {
    start: Instant,
    outfile: csv::Writer<File>,
    num_tables: Vec<usize>,
}

/// Locks changes to the number of tables in a level
pub(crate) struct LevelLogger {
    inner: Mutex<Inner>,
}

impl LevelLogger {
    pub fn new(path: &str, num_levels: usize) -> Self {
        let outfile = csv::Writer::from_path(path).expect("Failed to create log file");

        let inner = Inner::new(outfile, num_levels);

        Self {
            inner: Mutex::new(inner),
        }
    }

    pub fn l0_table_added(&self) {
        let mut inner = self.inner.lock();
        inner.num_tables[0] += 1;

        inner.write();
    }

    pub fn compaction(&self, level: LevelId, added: usize, removed: usize) {
        let mut inner = self.inner.lock();
        inner.num_tables[level as usize] -= removed;
        inner.num_tables[level as usize + 1] += added;

        inner.write();
    }
}

impl Inner {
    fn new(mut outfile: csv::Writer<File>, num_levels: usize) -> Self {
        let num_tables = vec![0; num_levels];

        let mut header = vec![format!("time")];
        for idx in 0..num_levels {
            header.push(format!("level{idx}"));
        }

        outfile.write_record(&header).unwrap();

        Self {
            outfile,
            num_tables,
            start: Instant::now(),
        }
    }

    fn write(&mut self) {
        let mut record = vec![];
        record.push(format!("{}", self.start.elapsed().as_millis()));

        for count in self.num_tables.iter() {
            record.push(format!("{count}"));
        }

        self.outfile.write_record(&record).unwrap();
    }
}
