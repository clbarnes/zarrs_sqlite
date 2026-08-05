use std::path::PathBuf;

use crate::Metadata;

#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) path: Option<PathBuf>,
    pub(crate) write: bool,
    pub(crate) exclusive: bool,
    pub(crate) create: bool,
    pub(crate) truncate: bool,
    pub(crate) update_timestamp_on_write: bool,
    created_by: Option<String>,
}

impl Options {
    /// Create a file-backed database.
    pub fn new_local(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            write: false,
            exclusive: false,
            create: false,
            truncate: false,
            update_timestamp_on_write: false,
            created_by: Default::default(),
        }
    }

    /// Create an in-memory database.
    ///
    /// Should always be used with [Self::create()].
    pub fn new_memory() -> Self {
        Self {
            path: None,
            write: false,
            exclusive: false,
            create: false,
            truncate: false,
            update_timestamp_on_write: false,
            created_by: Default::default(),
        }
    }

    /// Allow writing into an existing, well-formed zarr-SQLite store
    pub fn write(mut self) -> Self {
        self.write = true;
        self
    }

    /// Allow creating an SQLite file if it does not exist.
    ///
    /// Implies `write`.
    pub fn create(mut self) -> Self {
        self.create = true;
        if let Some(p) = self.path.as_deref()
            && p.extension() != Some(std::ffi::OsStr::new("zarrdb"))
        {
            log::warn!(
                "Zarr SQLite stores SHOULD have extension .zarrdb; got {}",
                p.display()
            );
        }
        self.write()
    }

    /// Fail if the SQLite file already exists.
    ///
    /// Ignored if `truncate` is set.
    /// Implies `create` and `write`.
    pub fn exclusive(mut self) -> Self {
        self.exclusive = true;
        self.create()
    }

    /// If the SQLite file already exists, delete it first.
    /// Implies `write`, but not `create`.
    pub fn truncate(mut self) -> Self {
        self.truncate = true;
        self.write()
    }

    /// Ignored if the database is not newly created by this builder.
    pub fn created_by(mut self, created_by: impl Into<String>) -> Self {
        self.created_by = Some(created_by.into());
        self
    }

    /// Update the modified timestamp in the metadata every time the data are mutated.
    ///
    /// By default, the timestamp is only updated when the store is opened for writing.
    pub fn update_timestamp_on_write(mut self) -> Self {
        self.update_timestamp_on_write = true;
        self
    }

    pub(crate) fn check_existence(&self) -> crate::Result<bool> {
        log::debug!("Opening DB with {:?}", self);
        let init = if let Some(p) = self.path.as_deref() {
            log::debug!("Looking for DB at {}", p.display());
            let exists = p.is_file();
            if exists {
                log::debug!("Found existing DB");
                if self.truncate {
                    log::debug!("Truncating existing DB");
                    std::fs::remove_file(p)?;
                    true
                } else if self.exclusive {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "File already exists",
                    )
                    .into());
                } else {
                    false
                }
            } else if !self.create {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "File does not exist",
                )
                .into());
            } else {
                true
            }
        } else if !self.create {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "In-memory database does not exist",
            )
            .into());
        } else {
            log::debug!("Creating new DB in memory");
            true
        };
        Ok(init)
    }

    pub(crate) fn make_metadata(&self) -> Metadata {
        let created_by = self
            .created_by
            .clone()
            .unwrap_or_else(|| crate::DEFAULT_CREATED_BY.to_string());
        Metadata::with_created_by(created_by)
    }
}
