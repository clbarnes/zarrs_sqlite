/// Information about a checkpoint operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointResult {
    /// True if the checkpoint operation could not complete because another connection was busy.
    pub busy: bool,
    /// None if the database is not in WAL mode.
    pub pages_written: Option<PageWriteCounts>,
}

impl CheckpointResult {
    pub(crate) fn new(busy_int: i64, to_wal: i64, to_db: i64) -> Self {
        Self {
            busy: busy_int != 0,
            pages_written: PageWriteCounts::maybe_new(to_wal, to_db),
        }
    }

    pub fn wal_mode(&self) -> bool {
        self.pages_written.is_some()
    }
}

/// How many pages were written to the WAL and to the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageWriteCounts {
    pub to_wal: u64,
    pub to_db: u64,
}

impl PageWriteCounts {
    pub(crate) fn maybe_new(to_wal: i64, to_db: i64) -> Option<Self> {
        if to_wal < 0 || to_db < 0 {
            None
        } else {
            Some(Self {
                to_wal: to_wal as u64,
                to_db: to_db as u64,
            })
        }
    }
}
