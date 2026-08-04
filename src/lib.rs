#[cfg(feature = "backend-turso")]
mod turso_store;
#[cfg(feature = "backend-turso")]
pub use turso_store::TursoStore;

#[cfg(feature = "backend-rusqlite")]
mod rusqlite_store;
#[cfg(feature = "backend-rusqlite")]
pub use rusqlite_store::RusqliteStore;

mod error;
pub use error::{Error, Result};

mod metadata;
pub use metadata::{Flags, Metadata, Timestamp, Version};

mod options;
mod queries;
pub use options::Options;

pub use jiff;

pub const APPLICATION_ID: u32 = 0x10b50760;
pub const EARLIEST_SUPPORTED_VERSION: Version = Version { major: 1, minor: 0 };
pub const LATEST_VERSION: Version = Version { major: 1, minor: 0 };
const DEFAULT_CREATED_BY: &str = concat!("zarrs_sqlite v", env!("CARGO_PKG_VERSION"));
