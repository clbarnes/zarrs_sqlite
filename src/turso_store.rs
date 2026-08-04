use std::{fmt::Debug, path::PathBuf};

use futures::StreamExt;
use turso::{Connection, Database};
use zarrs_storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
    AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError, StoreKey,
    StoreKeys, StoreKeysPrefixes, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{
    SqliteStoreMetadata,
    queries::{self, SUPPORTS_GET_PARTIAL, SUPPORTS_SET_PARTIAL},
};

/// Zarr store backed by an SQLite database using the [turso](https://github.com/tursodatabase/turso) engine.
///
/// Implements [zarrs_storage::AsyncReadableWritableListableStorageTraits].
#[derive(Debug, Clone)]
pub struct TursoStore {
    database: Database,
    write: bool,
    update_timestamp_on_write: bool,
}

impl TursoStore {
    /// If path is None, use an in-memory store.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use zarrs_sqlite::TursoStore;
    ///
    /// let mut builder = TursoStore::builder(None);
    /// builder.create();
    /// let store = builder.build();
    /// ```
    pub fn builder(path: Option<&str>) -> TursoStoreBuilder {
        match path {
            Some(p) => TursoStoreBuilder::new(p),
            None => TursoStoreBuilder::new_memory(),
        }
    }

    fn connection(&self) -> Result<LoggingConnection, crate::Error> {
        let conn = self.database.connect()?;
        Ok(conn.into())
    }

    // async fn insert_created_at(&self) -> Result<(), crate::Error> {
    //     let conn = self.connection()?;
    //     conn.execute(
    //         "INSERT INTO zarr_sqlitestore_metadata(k, v) VALUES ('created_at', datetime('now', 'utc', 'subsec'));",
    //         (),
    //     )
    //     .await?;
    //     Ok(())
    // }

    async fn update_modified_at(&self) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        conn.execute(queries::update_modified_at_query(), ())
            .await?;
        Ok(())
    }

    /// Get direct children of this prefix, i.e. keys that start with the prefix and do not have a slash after the prefix.
    async fn list_child_keys(
        &self,
        prefix: &StorePrefix,
        conn: &LoggingConnection,
    ) -> Result<StoreKeys, crate::Error> {
        let (query, params) = queries::list_child_keys_query(prefix);
        let mut rows = conn.query(query, params).await?;

        let mut keys = Vec::default();

        while let Some(row) = rows.next().await? {
            let key: String = row.get(0)?;
            if let Ok(k) = StoreKey::new(key.clone()) {
                keys.push(k);
            }
        }
        Ok(keys)
    }

    /// N.B. this trims and deduplicates the prefixes in the DB engine.
    async fn list_child_prefixes(
        &self,
        prefix: &StorePrefix,
        conn: &LoggingConnection,
    ) -> Result<Vec<StorePrefix>, crate::Error> {
        let (query, params) = queries::list_dir_prefixes_query(prefix);
        let mut rows = conn.query(query, params).await?;

        let mut prefixes = Vec::default();

        while let Some(row) = rows.next().await? {
            let prefix_str: String = row.get(0)?;
            if let Ok(p) = StorePrefix::new(prefix_str) {
                prefixes.push(p);
            }
        }
        Ok(prefixes)
    }

    /// Get the metadata of the store.
    pub async fn read_metadata(&self) -> Result<SqliteStoreMetadata, crate::Error> {
        let conn = self.connection()?;
        let mut rows = conn.query(queries::read_metadata_query(), ()).await?;

        let mut builder = SqliteStoreMetadata::builder();

        while let Some(row) = rows.next().await? {
            let key: String = row.get(0)?;
            let value: String = row.get(1)?;
            builder.add_key_value(key, value)?;
        }

        builder.build()
    }

    /// Set up the tables and pragma required by the store.
    async fn create_schema(&self) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        conn.execute_batch(queries::create_schema_queries()).await?;
        Ok(())
    }

    /// Overwrite the metadata of the store. This will not delete any unknown metadata keys, but will overwrite any known keys.
    async fn write_metadata(&self, metadata: &SqliteStoreMetadata) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        if !metadata.unknown.is_empty() {
            let results = futures::future::join_all(metadata.unknown.iter().map(|(k, v)| {
                let (query, params) = queries::insert_unknown_metadata_query(k, v);
                conn.execute(query, params)
            }))
            .await;
            for r in results {
                r?;
            }
        }
        let (query, params) = queries::insert_metadata_query(metadata);
        conn.execute(query, params).await?;
        Ok(())
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

    async fn execute_batch(&self, sql: impl AsRef<str>) -> turso::Result<()> {
        log::debug!("Executing SQL batch: {}", sql.as_ref());
        self.conn.execute_batch(sql).await
    }
}

pub struct TursoStoreBuilder {
    builder: turso::Builder,
    path: Option<PathBuf>,
    write: bool,
    exclusive: bool,
    create: bool,
    truncate: bool,
    update_timestamp_on_write: bool,
    created_by: String,
}

impl TursoStoreBuilder {
    fn new(path: impl AsRef<str>) -> Self {
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
            update_timestamp_on_write: false,
            created_by: Default::default(),
        }
    }

    /// Should always be used with `create`.
    fn new_memory() -> Self {
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

    /// Update the modified timestamp in the metadata every time the data are mutated.
    ///
    /// By default, the timestamp is only updated when the store is opened for writing.
    pub fn update_timestamp_on_write(&mut self) -> &mut Self {
        self.update_timestamp_on_write = true;
        self
    }

    /// Alternatively use `try_into` to block on the result.
    pub async fn build(self) -> Result<TursoStore, crate::Error> {
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
        let database = self.builder.build().await?;

        let store = TursoStore {
            database,
            write: self.write,
            update_timestamp_on_write: self.update_timestamp_on_write,
        };

        if init {
            store.create_schema().await?;
            let metadata = SqliteStoreMetadata::with_created_by(self.created_by);
            store.write_metadata(&metadata).await?;
        } else if store.write && !store.update_timestamp_on_write {
            store.update_modified_at().await?;
        }
        // TODO: read metadata if init is false
        Ok(store)
    }
}

fn turso_to_storage_error(err: turso::Error) -> StorageError {
    crate::Error::from(err).into()
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncReadableStorageTraits for TursoStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::get_query(key);
        let Some(row) = conn
            .query(query, params)
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
        let (query, params) = queries::get_partial_query(key, byte_range);
        let Some(row) = conn
            .query(query, params)
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
        let Some((query, params)) = queries::get_partial_many_query(key, byte_ranges) else {
            return Ok(Some(Box::pin(futures::stream::empty())));
        };

        let conn = self.connection()?;

        let Some(row) = conn
            .query(query, params)
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
        let (query, params) = queries::get_size_query(key);
        let Some(res) = conn
            .query(query, params)
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
        SUPPORTS_GET_PARTIAL
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncListableStorageTraits for TursoStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let conn = self.connection()?;
        let query = queries::list_all_query();
        let mut rows = conn
            .query(query, ())
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
        let (query, params) = queries::list_prefix_query(prefix);
        let mut rows = conn
            .query(query, params)
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
        let (query, params) = queries::size_prefix_query(prefix);
        let Some(row) = conn
            .query(query, params)
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
            .query(queries::size_total_query(), ())
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
        let (query, params) = queries::set_query(key, &value[..]);
        conn.execute(query, params)
            .await
            .map_err(turso_to_storage_error)?;
        if self.update_timestamp_on_write {
            self.update_modified_at().await?;
        }
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
        let (query, params) = queries::erase_query(key);
        conn.execute(query, params)
            .await
            .map_err(turso_to_storage_error)?;

        if self.update_timestamp_on_write {
            self.update_modified_at().await?;
        }
        Ok(())
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        let (query, params) = queries::erase_prefix_query(prefix);
        conn.execute(query, params)
            .await
            .map_err(turso_to_storage_error)?;
        if self.update_timestamp_on_write {
            self.update_modified_at().await?;
        }
        Ok(())
    }

    fn supports_set_partial(&self) -> bool {
        SUPPORTS_SET_PARTIAL
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use temp_testdir::TempDir;

    use super::TursoStore;
    use zarrs_storage::{
        AsyncReadableWritableListableStorage, Bytes, StoreKey, StorePrefix, byte_range::ByteRange,
    };

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn zarrdb_path(dir: impl AsRef<std::path::Path>) -> String {
        dir.as_ref()
            .join("test.zarrdb")
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn no_create_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let b = TursoStore::builder(Some(&p));
        assert!(b.build().await.is_err());
    }

    #[tokio::test]
    async fn create_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let mut b = TursoStore::builder(Some(&p));
        b.create();
        let _store = b.build().await.unwrap();
    }

    #[tokio::test]
    async fn open_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let mut b = TursoStore::builder(Some(&p));
        b.create();
        let _store = b.build().await.unwrap();

        let b2 = TursoStore::builder(Some(&p));
        let _store2 = b2.build().await.unwrap();
    }

    #[tokio::test]
    async fn read_metadata() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let mut b = TursoStore::builder(Some(&p));
        b.create();
        let store = b.build().await.unwrap();
        let meta = store.read_metadata().await.unwrap();
        assert_eq!(meta.sqlitestore_version, crate::LATEST_VERSION);
    }

    async fn make_memstore() -> AsyncReadableWritableListableStorage {
        let mut b = TursoStore::builder(None);
        b.create();
        let store = b.build().await.unwrap();
        Arc::new(store)
    }

    #[tokio::test]
    async fn truncate_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let mut b = TursoStore::builder(Some(&p));
        b.create();
        let store = b.build().await.unwrap();
        let orig_meta = store.read_metadata().await.unwrap();

        let mut b2 = TursoStore::builder(Some(&p));
        b2.truncate();
        let store2 = b2.build().await.unwrap();
        let new_meta = store2.read_metadata().await.unwrap();
        assert!(new_meta.created_at > orig_meta.created_at);
    }

    #[tokio::test]
    async fn roundtrip_bytes() {
        init();
        let store = make_memstore().await;

        let key = StoreKey::new("test_key").unwrap();
        let data = b"Hello, world!";
        store.set(&key, Bytes::from_static(data)).await.unwrap();

        let read_data = store.get(&key).await.unwrap().unwrap();
        assert_eq!(data, &read_data[..]);
    }

    #[tokio::test]
    async fn partial_bytes() {
        init();
        let store = make_memstore().await;

        let key = StoreKey::new("test_key").unwrap();
        let data = b"Hello, world!";
        store.set(&key, Bytes::from_static(data)).await.unwrap();

        let test_partial_read = async |br: ByteRange, expected: &[u8]| {
            let read = store.get_partial(&key, br).await.unwrap().unwrap();
            assert_eq!(expected, &read[..]);
        };

        test_partial_read(ByteRange::FromStart(0, Some(5)), &data[0..5]).await;
        test_partial_read(ByteRange::FromStart(8, None), &data[8..]).await;
        test_partial_read(ByteRange::FromStart(8, Some(3)), &data[8..11]).await;
        test_partial_read(ByteRange::Suffix(6), &data[data.len() - 6..]).await;
    }

    fn check_strlike_contents(test: &[impl ToString], reference: &[impl ToString]) {
        let mut t: Vec<_> = test.iter().map(ToString::to_string).collect();
        t.sort();

        let mut r: Vec<_> = reference.iter().map(ToString::to_string).collect();
        r.sort();

        assert_eq!(t, r);
    }

    #[tokio::test]
    async fn list_keys() {
        init();
        let store = make_memstore().await;

        let keys: Vec<_> = ["a", "a/b", "a/c/d"]
            .into_iter()
            .map(|s| StoreKey::new(s).unwrap())
            .collect();
        let data = Bytes::from_static(b"Hello, world!");

        for k in keys.iter() {
            store.set(k, data.clone()).await.unwrap();
        }

        let read_keys = store.list().await.unwrap();
        check_strlike_contents(&read_keys, &keys);

        let read_children = store
            .list_dir(&StorePrefix::new("a/").unwrap())
            .await
            .unwrap();
        check_strlike_contents(read_children.keys(), &["a/b"]);
        check_strlike_contents(read_children.prefixes(), &["a/c/"]);

        let read_descendants = store
            .list_prefix(&StorePrefix::new("a/").unwrap())
            .await
            .unwrap();
        check_strlike_contents(&read_descendants, &["a/b", "a/c/d"]);
    }
}
