use std::fmt::Debug;

use futures::StreamExt;
use turso::{Connection, Database};
use zarrs_storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
    AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError, StoreKey,
    StoreKeys, StoreKeysPrefixes, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{
    Metadata,
    options::Options,
    queries::{self, SUPPORTS_GET_PARTIAL, SUPPORTS_SET_PARTIAL},
    types::CheckpointResult,
};

const MEMORY_PATH: &str = ":memory:";

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
    pub async fn new(options: &Options) -> Result<Self, crate::Error> {
        let init = options.check_existence()?;
        let turso_builder = match options.path.as_deref() {
            Some(p) => {
                let p_str = p.to_str().ok_or_else(|| {
                    crate::Error::General(format!("Path is not valid UTF-8: {}", p.display()))
                })?;
                turso::Builder::new_local(p_str)
            }
            None => turso::Builder::new_local(MEMORY_PATH),
        };
        let store = Self {
            database: turso_builder.build().await?,
            write: options.write,
            update_timestamp_on_write: options.update_timestamp_on_write,
        };
        if init {
            store.create_schema().await?;
            let metadata = options.make_metadata();
            store.write_metadata(&metadata).await?;
        } else if store.write && !store.update_timestamp_on_write {
            store.update_modified_at().await?;
        }
        Ok(store)
    }

    /// Write the current write-ahead log to the database
    /// and truncate the WAL.
    ///
    /// Should be called when a program is about to exit, to ensure that all writes are persisted.
    /// Only necessary for write-enabled file-backed stores.
    pub async fn checkpoint(&self) -> Result<Option<CheckpointResult>, crate::Error> {
        if !self.write {
            return Ok(None);
        }
        let conn = self.connection()?;

        let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE);", ()).await?;
        let row = rows
            .next()
            .await?
            .expect("PRAGMA wal_checkpoint should return a row");
        let res = CheckpointResult::new(row.get(0)?, row.get(1)?, row.get(2)?);

        Ok(Some(res))
    }

    /// Defragment the database file.
    ///
    /// May transiently take up twice the space of the database file.
    pub async fn vacuum(&self) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        conn.execute("VACUUM;", ()).await?;
        Ok(())
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
        if !self.write {
            return Ok(());
        }
        let conn = self.connection()?;
        conn.execute(queries::update_modified_at_query(), ())
            .await?;
        Ok(())
    }

    /// Get the metadata of the store.
    pub async fn read_metadata(&self) -> Result<Metadata, crate::Error> {
        let conn = self.connection()?;
        let mut rows = conn.query(queries::read_metadata_query(), ()).await?;

        let mut builder = Metadata::builder();

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

    async fn list_dir_inner(
        &self,
        query: &str,
        params: impl turso::IntoParams + Debug,
    ) -> Result<StoreKeysPrefixes, StorageError> {
        let mut keys = Vec::default();
        let mut prefixes = Vec::default();

        let conn = self.connection()?;
        let mut rows = conn
            .query(query, params)
            .await
            .map_err(crate::Error::from)?;
        while let Some(row) = rows.next().await.map_err(crate::Error::from)? {
            let tp_val = row.get_value(0).map_err(crate::Error::from)?;
            let tp = tp_val.as_text().expect("returned value should be text");
            let value_val = row.get_value(1).map_err(crate::Error::from)?;
            let value = value_val.as_text().expect("returned value should be text");
            match tp.as_str() {
                "k" => match StoreKey::new(value) {
                    Ok(k) => keys.push(k),
                    Err(e) => log::warn!("Ignoring invalid store key '{value}': {e}"),
                },
                "p" => match StorePrefix::new(value) {
                    Ok(p) => prefixes.push(p),
                    Err(e) => log::warn!("Ignoring invalid store key '{value}': {e}"),
                },
                s => {
                    log::warn!("Ignoring list_dir value of unknown type '{s}': '{value}'")
                }
            }
        }
        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    /// Overwrite the metadata of the store. This will not delete any unknown metadata keys, but will overwrite any known keys.
    async fn write_metadata(&self, metadata: &Metadata) -> Result<(), crate::Error> {
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
        let (q, p) = queries::insert_core_metadata_query(metadata);
        conn.execute(q, p).await?;
        if let Some((q, p)) = queries::maybe_insert_created_by_query(metadata) {
            conn.execute(q, p).await?;
        }
        if let Some((q, p)) = queries::maybe_insert_modified_at_query(metadata) {
            conn.execute(q, p).await?;
        }
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

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AsyncReadableStorageTraits for TursoStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::get_query(key);
        let Some(row) = conn
            .query(query, params)
            .await
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };
        let val: Vec<u8> = row.get(0).map_err(crate::Error::from)?;
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
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };
        let val: Vec<u8> = row.get(0).map_err(crate::Error::from)?;
        Ok(Some(val.into()))
    }

    async fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        let Some((query, params, _)) = queries::get_partial_many_query(key, byte_ranges) else {
            return Ok(Some(Box::pin(futures::stream::empty())));
        };

        let conn = self.connection()?;

        let Some(row) = conn
            .query(query, params)
            .await
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };

        let stream = futures::stream::iter((0..row.column_count()).map(move |i| {
            let val: Vec<u8> = row.get(i).map_err(crate::Error::from)?;
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
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };
        let size = res.get(0).map_err(crate::Error::from)?;
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
        let mut rows = conn.query(query, ()).await.map_err(crate::Error::from)?;
        let mut out = Vec::default();
        while let Some(row) = rows.next().await.map_err(crate::Error::from)? {
            let key: String = row.get(0).map_err(crate::Error::from)?;
            if let Ok(k) = StoreKey::new(key) {
                out.push(k)
            }
        }
        Ok(out)
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let Some((query, params)) = queries::list_prefix_query(prefix) else {
            return self.list().await;
        };
        let conn = self.connection()?;
        let mut rows = conn
            .query(query, params)
            .await
            .map_err(crate::Error::from)?;

        let mut out = Vec::default();
        while let Some(row) = rows.next().await.map_err(crate::Error::from)? {
            let key: String = row.get(0).map_err(crate::Error::from)?;
            if let Ok(k) = StoreKey::new(key) {
                out.push(k)
            }
        }
        Ok(out)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let Some((query, params)) = queries::list_dir_query(prefix) else {
            let q = queries::list_dir_root_query();
            return self.list_dir_inner(q, ()).await;
        };
        self.list_dir_inner(query, params).await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let Some((query, params)) = queries::size_prefix_query(prefix) else {
            return self.size().await;
        };
        let conn = self.connection()?;
        let Some(row) = conn
            .query(query, params)
            .await
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(0);
        };
        let s = row.get(0).map_err(crate::Error::from)?;
        Ok(s)
    }

    async fn size(&self) -> Result<u64, StorageError> {
        let conn = self.connection()?;
        let Some(row) = conn
            .query(queries::size_total_query(), ())
            .await
            .map_err(crate::Error::from)?
            .next()
            .await
            .map_err(crate::Error::from)?
        else {
            return Ok(0);
        };
        let s = row.get(0).map_err(crate::Error::from)?;
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
            .map_err(crate::Error::from)?;
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
            .map_err(crate::Error::from)?;

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
            .map_err(crate::Error::from)?;
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
    use crate::tests::init;
    use std::{fs, sync::Arc};
    use temp_testdir::TempDir;

    use super::TursoStore;
    use crate::Options;
    use zarrs_storage::{
        AsyncReadableWritableListableStorage, Bytes, StoreKey, StorePrefix, byte_range::ByteRange,
    };

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
        let res = TursoStore::new(&Options::new_local(&p)).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn create_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let _store = TursoStore::new(&Options::new_local(&p).create())
            .await
            .unwrap();
        assert!(std::fs::exists(&p).unwrap());
    }

    #[tokio::test]
    async fn open_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let mut _store1 = TursoStore::new(&Options::new_local(&p).create())
            .await
            .unwrap();

        let _store2 = TursoStore::new(&Options::new_local(&p)).await.unwrap();
    }

    #[tokio::test]
    async fn read_metadata() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let store = TursoStore::new(&Options::new_local(&p).create())
            .await
            .unwrap();
        let meta = store.read_metadata().await.unwrap();
        assert_eq!(meta.sqlitestore_version, crate::LATEST_VERSION);
    }

    async fn make_memstore() -> AsyncReadableWritableListableStorage {
        let store = TursoStore::new(&Options::new_memory().create())
            .await
            .unwrap();
        Arc::new(store)
    }

    #[tokio::test]
    async fn truncate_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let store = TursoStore::new(&Options::new_local(&p).create().created_by("first"))
            .await
            .unwrap();
        let orig_meta = store.read_metadata().await.unwrap();
        assert_eq!(orig_meta.created_by.as_deref(), Some("first"));

        let store2 = TursoStore::new(&Options::new_local(&p).truncate().created_by("second"))
            .await
            .unwrap();
        let new_meta = store2.read_metadata().await.unwrap();
        assert_eq!(new_meta.created_by.as_deref(), Some("second"));
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

    #[tokio::test]
    async fn wal_checkpoint() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);
        let wal = p.clone() + "-wal";
        let store = TursoStore::new(&Options::new_local(&p).create())
            .await
            .unwrap();
        let storage: AsyncReadableWritableListableStorage = Arc::new(store.clone());
        let key = StoreKey::new("test_key").unwrap();
        let data = Bytes::from_static(b"Hello, world!");
        storage.set(&key, data).await.unwrap();

        let wal_contents_before = fs::read(&wal).expect("could not read WAL before checkpoint");
        store.checkpoint().await.expect("could not checkpoint");
        let wal_contents_after = fs::read(&wal).expect("could not read WAL after checkpoint");
        assert!(
            wal_contents_after.len() < wal_contents_before.len(),
            "WAL file should be smaller after checkpoint"
        );
    }
}
