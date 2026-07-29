use std::{
    fmt::{Debug, Write},
    path::PathBuf,
};

use futures::StreamExt;
use turso::{Connection, Database};
use zarrs_storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
    AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError, StoreKey,
    StoreKeys, StoreKeysPrefixes, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{APPLICATION_ID, SqliteStoreMetadata};

#[derive(Debug, Clone)]
pub struct TursoStore {
    database: Database,
    write: bool,
}

fn turso_to_storage_error(e: turso::Error) -> StorageError {
    StorageError::Other(format!("Turso error: {e}"))
}

impl TursoStore {
    fn connection(&self) -> Result<LoggingConnection, StorageError> {
        self.database
            .connect()
            .map_err(turso_to_storage_error)
            .map(LoggingConnection::from)
    }

    async fn update_modified_time(&self, timestamp: jiff::Timestamp) -> Result<(), StorageError> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO sqlitestore_metadata(k, v) VALUES ('modified_time', ?1)",
            (timestamp.to_string(),),
        )
        .await
        .map_err(turso_to_storage_error)?;
        Ok(())
    }

    async fn update_modified_time_now(&self) -> Result<(), StorageError> {
        self.update_modified_time(jiff::Timestamp::now()).await
    }

    async fn list_child_keys(
        &self,
        prefix: &StorePrefix,
        conn: &LoggingConnection,
    ) -> Result<StoreKeys, StorageError> {
        let mut rows = conn
            .query(
                "SELECT k FROM zarr WHERE k LIKE ? and k NOT LIKE ?;",
                (format!("{prefix}%"), format!("{prefix}%/%")),
            )
            .await
            .map_err(turso_to_storage_error)?;

        let mut keys = Vec::default();

        while let Some(row) = rows.next().await.map_err(turso_to_storage_error)? {
            let key: String = row.get(0).map_err(turso_to_storage_error)?;
            if let Ok(k) = StoreKey::new(key.clone()) {
                keys.push(k);
            }
        }
        Ok(keys)
    }

    async fn list_child_prefixes(
        &self,
        prefix: &StorePrefix,
        conn: &LoggingConnection,
    ) -> Result<Vec<StorePrefix>, StorageError> {
        let mut rows = conn
            .query(
                "SELECT DISTINCT substr(k, 1, instr(substr(k, ?), '/') + ?)
                 FROM zarr
                 WHERE k LIKE ?;",
                (
                    prefix.as_str().len() as i64 + 1,
                    prefix.as_str().len() as i64,
                    format!("{prefix}%/%"),
                ),
            )
            .await
            .map_err(turso_to_storage_error)?;

        let mut prefixes = Vec::default();

        while let Some(row) = rows.next().await.map_err(turso_to_storage_error)? {
            let prefix_str: String = row.get(0).map_err(turso_to_storage_error)?;
            if let Ok(p) = StorePrefix::new(prefix_str) {
                prefixes.push(p);
            }
        }
        Ok(prefixes)
    }

    pub async fn read_metadata(&self) -> Result<SqliteStoreMetadata, StorageError> {
        let conn = self.connection()?;
        let mut rows = conn
            .query("SELECT k, v FROM sqlitestore_metadata;", ())
            .await
            .map_err(turso_to_storage_error)?;

        let mut version_str = None;
        let mut compatible_flags = None;
        let mut incompatible_flags = None;
        let mut created_by = None;
        let mut created_time = None;

        while let Some(row) = rows.next().await.map_err(turso_to_storage_error)? {
            let key: String = row.get(0).map_err(turso_to_storage_error)?;
            let value: String = row.get(1).map_err(turso_to_storage_error)?;
            match key.as_str() {
                "sqlitestore_version" => version_str = Some(value),
                "compatible_flags" => compatible_flags = Some(value),
                "incompatible_flags" => incompatible_flags = Some(value),
                "created_by" => created_by = Some(value),
                "created_time" => created_time = Some(value),
                _ => {}
            }
        }

        if let (
            Some(version_str),
            Some(compatible_flags),
            Some(incompatible_flags),
            Some(created_by),
            Some(created_time),
        ) = (
            version_str,
            compatible_flags,
            incompatible_flags,
            created_by,
            created_time,
        ) {
            SqliteStoreMetadata::from_strs(
                version_str,
                compatible_flags,
                incompatible_flags,
                created_by,
                created_time,
            )
            .map_err(|e| StorageError::Other(format!("Failed to parse metadata: {}", e)))
        } else {
            Err(StorageError::Other("Incomplete metadata".into()))
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoggingConnection {
    conn: Connection,
}

impl From<Connection> for LoggingConnection {
    fn from(conn: Connection) -> Self {
        Self { conn }
    }
}

impl LoggingConnection {
    async fn execute(
        &self,
        sql: impl AsRef<str>,
        params: impl turso::IntoParams + Debug,
    ) -> turso::Result<u64> {
        log::debug!("Executing SQL: {}\nwith params: {params:?}", sql.as_ref());
        self.conn.execute(sql, params).await
    }

    async fn query(
        &self,
        sql: impl AsRef<str>,
        params: impl turso::IntoParams + std::fmt::Debug,
    ) -> turso::Result<turso::Rows> {
        log::debug!("Executing SQL: {}\n with params: {params:?}", sql.as_ref());
        self.conn.query(sql, params).await
    }
}

pub struct TursoStoreBuilder {
    builder: turso::Builder,
    path: Option<PathBuf>,
    write: bool,
    exclusive: bool,
    create: bool,
    truncate: bool,
    created_by: String,
}

impl TursoStoreBuilder {
    pub fn new(path: impl AsRef<str>) -> Self {
        let s = path.as_ref();
        let path = if s != ":memory:" {
            Some(PathBuf::from(s))
        } else {
            None
        };
        Self {
            builder: turso::Builder::new_local(s),
            path,
            write: false,
            exclusive: false,
            create: false,
            truncate: false,
            created_by: Default::default(),
        }
    }

    /// Should always be used with `create`.
    pub fn new_memory() -> Self {
        Self::new(":memory:")
    }

    /// Allow writing into an existing, well-formed zarr-SQLite store
    pub fn write(&mut self) -> &mut Self {
        self.write = true;
        self
    }

    /// Allow creating an SQLite file if it does not exist.
    ///
    /// Implies `write`.
    pub fn create(&mut self) -> &mut Self {
        self.create = true;
        self.write = true;
        self
    }

    /// Fail if the SQLite file already exists.
    ///
    /// Ignored if `truncate` is set.
    /// Implies `create` and `write`.
    pub fn exclusive(&mut self) -> &mut Self {
        self.exclusive = true;
        self.create = true;
        self.write = true;
        self
    }

    /// If the SQLite file already exists, delete it first.
    /// Implies `write`, but not `create`.
    pub fn truncate(&mut self) -> &mut Self {
        self.truncate = true;
        self.write = true;
        self
    }

    /// Ignored if the database is not newly created by this builder.
    pub fn created_by(&mut self, created_by: impl Into<String>) -> &mut Self {
        self.created_by = created_by.into();
        self
    }

    /// Alternatively use `try_into` to block on the result.
    pub async fn build(self) -> Result<TursoStore, StorageError> {
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
        let database = self.builder.build().await.map_err(turso_to_storage_error)?;
        if init {
            let conn = database.connect().map_err(turso_to_storage_error)?;
            conn.execute(format!("PRAGMA application_id = 0x{APPLICATION_ID:x};"), ())
                .await
                .map_err(turso_to_storage_error)?;
            conn.execute(
                "CREATE TABLE sqlitestore_metadata(
                k TEXT PRIMARY KEY NOT NULL,
                v TEXT NOT NULL
            );",
                (),
            )
            .await
            .map_err(turso_to_storage_error)?;
            conn.execute(
                "CREATE TABLE zarr(
                k TEXT PRIMARY KEY NOT NULL,
                v BLOB NOT NULL
            );",
                (),
            )
            .await
            .map_err(turso_to_storage_error)?;
            let metadata = SqliteStoreMetadata::with_created_by(self.created_by);
            conn.execute(
                "INSERT INTO sqlitestore_metadata (k, v) VALUES
                    ('sqlitestore_version', ?1),
                    ('compatible_flags', ?2),
                    ('incompatible_flags', ?3),
                    ('created_by', ?4),
                    ('created_time', ?5)",
                (
                    metadata.sqlitestore_version.to_string(),
                    metadata.compatible_flags.to_string(),
                    metadata.incompatible_flags.to_string(),
                    metadata.created_by.clone(),
                    metadata.created_time.to_string(),
                ),
            )
            .await
            .map_err(turso_to_storage_error)?;
        }
        // TODO: read metadata if init is false
        Ok(TursoStore {
            database,
            write: self.write,
        })
    }
}

impl TryFrom<TursoStoreBuilder> for TursoStore {
    type Error = StorageError;

    fn try_from(builder: TursoStoreBuilder) -> Result<Self, Self::Error> {
        futures::executor::block_on(builder.build())
    }
}

#[derive(Debug, Clone, Copy)]
struct Substr<'a> {
    name: &'a str,
    range: ByteRange,
}

impl std::fmt::Display for Substr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.range {
            ByteRange::FromStart(offset, maybe_len) => match maybe_len {
                Some(len) => write!(f, "substr({}, {}, {})", self.name, offset, len),
                None => write!(f, "substr({}, {})", self.name, offset),
            },
            ByteRange::Suffix(len) => write!(f, "substr({}, -{})", self.name, len),
        }
    }
}

fn write_substrs(
    s: &mut String,
    name: &str,
    byte_ranges: impl IntoIterator<Item = ByteRange>,
) -> Result<usize, StorageError> {
    let mut count = 0;
    for range in byte_ranges {
        let substr = Substr { name, range };
        if count == 0 {
            s.write_fmt(format_args!("{substr}"))
                .map_err(|_| StorageError::Other("could not generate SQL".into()))?;
        } else {
            s.write_fmt(format_args!(", {substr}"))
                .map_err(|_| StorageError::Other("could not generate SQL".into()))?;
        }
        count += 1;
    }
    Ok(count)
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncReadableStorageTraits for TursoStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let Some(row) = conn
            .query("SELECT v FROM zarr WHERE k = ? LIMIT 1;", (key.as_str(),))
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(None);
        };
        let val: Vec<u8> = row.get(0).map_err(turso_to_storage_error)?;
        Ok(Some(val.into()))
    }

    async fn get_partial(
        &self,
        key: &StoreKey,
        byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let q = format!(
            "SELECT {} FROM zarr WHERE k = ? LIMIT 1;",
            Substr {
                name: "v",
                range: byte_range
            }
        );
        let Some(row) = conn
            .query(q, (key.as_str(),))
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(None);
        };
        let val: Vec<u8> = row.get(0).map_err(turso_to_storage_error)?;
        Ok(Some(val.into()))
    }

    async fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        let mut s = String::from("SELECT ");

        let count = write_substrs(&mut s, "v", byte_ranges)?;
        if count == 0 {
            return Ok(Some(Box::pin(futures::stream::empty())));
        }
        s.push_str(" FROM zarr WHERE k = ? LIMIT 1;");

        let conn = self.connection()?;

        let Some(row) = conn
            .query(s, (key.as_str(),))
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(None);
        };

        let stream = futures::stream::iter((0..row.column_count()).map(move |i| {
            let val: Vec<u8> = row.get(i).map_err(turso_to_storage_error)?;
            Ok(Bytes::from(val))
        }))
        .boxed();
        Ok(Some(stream))
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let conn = self.connection()?;
        let Some(res) = conn
            .query(
                "SELECT length(v) FROM zarr WHERE k = ? LIMIT 1;",
                (key.as_str(),),
            )
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(None);
        };
        let size = res.get(0).map_err(turso_to_storage_error)?;
        Ok(Some(size))
    }

    fn supports_get_partial(&self) -> bool {
        true
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncListableStorageTraits for TursoStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let conn = self.connection()?;
        let mut rows = conn
            .query("SELECT k FROM zarr;", ())
            .await
            .map_err(turso_to_storage_error)?;
        let mut out = Vec::default();
        while let Some(row) = rows.next().await.map_err(turso_to_storage_error)? {
            let key: String = row.get(0).map_err(turso_to_storage_error)?;
            if let Ok(k) = StoreKey::new(key) {
                out.push(k)
            }
        }
        Ok(out)
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let conn = self.connection()?;
        let mut rows = conn
            .query(
                "SELECT k FROM zarr WHERE k LIKE ?;",
                (format!("{}%", prefix.as_str()),),
            )
            .await
            .map_err(turso_to_storage_error)?;

        let mut out = Vec::default();
        while let Some(row) = rows.next().await.map_err(turso_to_storage_error)? {
            let key: String = row.get(0).map_err(turso_to_storage_error)?;
            if let Ok(k) = StoreKey::new(key) {
                out.push(k)
            }
        }
        Ok(out)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let conn = self.connection()?;

        let (keys, prefixes) = futures::try_join!(
            self.list_child_keys(prefix, &conn),
            self.list_child_prefixes(prefix, &conn)
        )?;

        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let conn = self.connection()?;
        let Some(row) = conn
            .query(
                "SELECT SUM(LENGTH(v)) FROM zarr WHERE k LIKE ?;",
                (format!("{prefix}%"),),
            )
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(0);
        };
        let s = row.get(0).map_err(turso_to_storage_error)?;
        Ok(s)
    }

    async fn size(&self) -> Result<u64, StorageError> {
        let conn = self.connection()?;
        let Some(row) = conn
            .query("SELECT SUM(LENGTH(v)) FROM zarr;", ())
            .await
            .map_err(turso_to_storage_error)?
            .next()
            .await
            .map_err(turso_to_storage_error)?
        else {
            return Ok(0);
        };
        let s = row.get(0).map_err(turso_to_storage_error)?;
        Ok(s)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncWritableStorageTraits for TursoStore {
    async fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        let val: &[u8] = &value;
        conn.execute(
            "INSERT OR REPLACE INTO zarr(k, v) VALUES (?, ?)",
            (key.as_str(), val),
        )
        .await
        .map_err(turso_to_storage_error)?;
        self.update_modified_time_now().await?;
        Ok(())
    }

    async fn set_partial(
        &self,
        _key: &StoreKey,
        _offset: u64,
        _value: Bytes,
    ) -> Result<(), StorageError> {
        if !self.write {
            Err(StorageError::ReadOnly)
        } else {
            Err(StorageError::Unsupported(
                "set_partial is unsupported".into(),
            ))
        }
    }

    async fn set_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _offset_values: OffsetBytesIterator<'a>,
    ) -> Result<(), StorageError> {
        if !self.write {
            Err(StorageError::ReadOnly)
        } else {
            Err(StorageError::Unsupported(
                "set_partial_many is unsupported".into(),
            ))
        }
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        conn.execute("DELETE v FROM zarr WHERE k = ? LIMIT 1;", (key.as_str(),))
            .await
            .map_err(turso_to_storage_error)?;

        self.update_modified_time_now().await?;
        Ok(())
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        conn.execute(
            "DELETE v FROM zarr WHERE k LIKE ?;",
            (format!("{prefix}%"),),
        )
        .await
        .map_err(turso_to_storage_error)?;
        self.update_modified_time_now().await?;
        Ok(())
    }

    fn supports_set_partial(&self) -> bool {
        false
    }
}
