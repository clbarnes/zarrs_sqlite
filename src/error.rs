use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    #[cfg(feature = "backend-turso")]
    Turso(#[from] turso::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Invalid store metadata: key={key}, value={value:?}")]
    InvalidMetadata { key: String, value: Option<String> },
    #[error("Invalid {{major}}.{{minor}} version: {version}")]
    InvalidVersion { version: String },
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("{0}")]
    General(String),
}

impl Error {
    pub(crate) fn invalid_version(version: impl Into<String>) -> Self {
        Error::InvalidVersion {
            version: version.into(),
        }
    }

    pub(crate) fn invalid_timestamp(ts: impl Into<String>) -> Self {
        Error::InvalidTimestamp(ts.into())
    }
}

impl From<Error> for zarrs_storage::StorageError {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => zarrs_storage::StorageError::IOError(Arc::new(e)),
            _ => zarrs_storage::StorageError::Other(format!("zarrs_sqlite: {}", err)),
        }
    }
}
