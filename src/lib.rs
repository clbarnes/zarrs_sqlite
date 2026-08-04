#[cfg(feature = "backend-turso")]
mod turso_store;
#[cfg(feature = "backend-turso")]
pub use turso_store::TursoStore;

mod error;
pub use error::{Error, Result};

mod metadata;
pub use metadata::{Flags, SqliteStoreMetadata, SqliteTimestamp, Version};

mod queries;

pub use jiff;

pub const APPLICATION_ID: u32 = 0x10b50760;
pub const EARLIEST_SUPPORTED_VERSION: Version = Version { major: 1, minor: 0 };
pub const LATEST_VERSION: Version = Version { major: 1, minor: 0 };
