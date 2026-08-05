use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OpenFlags, OptionalExtension};
use std::fmt::Debug;
use zarrs_storage::{
    Bytes, ListableStorageTraits, MaybeBytes, MaybeBytesIterator, OffsetBytesIterator,
    ReadableStorageTraits, StorageError, StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
    WritableStorageTraits,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{
    Options,
    queries::{self, SUPPORTS_GET_PARTIAL, SUPPORTS_SET_PARTIAL},
};

/// Zarr store backed by an SQLite database using the [rusqlite](https://github.com/rusqlite/rusqlite) crate,
/// which binds to libsqlite3.
#[derive(Debug)]
pub struct RusqliteStore {
    pool: r2d2::Pool<SqliteConnectionManager>,
    write: bool,
    update_timestamp_on_write: bool,
}

impl RusqliteStore {
    pub fn new(options: &Options) -> Result<Self, crate::Error> {
        let init = options.check_existence()?;
        let mut flags = OpenFlags::empty();
        if options.write {
            flags |= OpenFlags::SQLITE_OPEN_READ_WRITE;
            if options.create || options.truncate {
                flags |= OpenFlags::SQLITE_OPEN_CREATE;
            }
        } else {
            flags |= OpenFlags::SQLITE_OPEN_READ_ONLY;
        }
        let mgr = match options.path.as_ref() {
            Some(path) => SqliteConnectionManager::file(path),
            None => SqliteConnectionManager::memory(),
        }
        .with_flags(flags);
        let pool = r2d2::Pool::new(mgr)?;
        let store = Self {
            pool,
            write: options.write,
            update_timestamp_on_write: options.update_timestamp_on_write,
        };
        if init {
            log::debug!("Creating new zarr-SQLite store at {:?}", options.path);
            store.create_schema()?;
            let metadata = options.make_metadata();
            store.write_metadata(&metadata)?;
        } else if store.write && !store.update_timestamp_on_write {
            store.update_modified_at()?;
        }
        Ok(store)
    }

    fn update_modified_at(&self) -> Result<(), crate::Error> {
        if !self.write {
            return Ok(());
        }
        let conn = self.connection()?;
        conn.execute(crate::queries::update_modified_at_query(), [])?;
        Ok(())
    }

    fn create_schema(&self) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        conn.execute_batch(&crate::queries::create_schema_queries())?;
        Ok(())
    }

    fn write_metadata(&self, metadata: &crate::Metadata) -> Result<(), crate::Error> {
        let conn = self.connection()?;
        for (k, v) in metadata.unknown.iter() {
            let (query, params) = crate::queries::insert_unknown_metadata_query(k, v);
            conn.execute(query, params)?;
        }
        let (query, params) = queries::insert_metadata_query(metadata);
        conn.execute(query, params)?;
        Ok(())
    }

    pub fn read_metadata(&self) -> Result<crate::Metadata, crate::Error> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(crate::queries::read_metadata_query())?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut builder = crate::Metadata::builder();
        for row in rows {
            let (k, v) = row?;
            builder.add_key_value(k, v)?;
        }
        builder.build()
    }

    fn connection(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, crate::Error> {
        let conn = self.pool.get()?;
        Ok(conn)
    }

    fn list_child_keys(
        &self,
        conn: &r2d2::PooledConnection<SqliteConnectionManager>,
        prefix: &StorePrefix,
    ) -> Result<StoreKeys, StorageError> {
        let (query, params) = queries::list_child_keys_query(prefix);
        let mut stmt = conn.prepare(query).map_err(crate::Error::from)?;
        let it = stmt
            .query_map(params, |r| r.get::<_, String>(0))
            .map_err(crate::Error::from)?
            .filter_map(|res| match res {
                Ok(key_str) => match StoreKey::new(&key_str) {
                    Ok(store_key) => Some(Ok(store_key)),
                    Err(_e) => None,
                },
                Err(e) => Some(Err(crate::Error::from(e).into())),
            });
        it.collect()
    }

    fn list_child_prefixes(
        &self,
        conn: &r2d2::PooledConnection<SqliteConnectionManager>,
        prefix: &StorePrefix,
    ) -> Result<Vec<StorePrefix>, StorageError> {
        let (query, params) = queries::list_dir_prefixes_query(prefix);
        let mut stmt = conn.prepare(query).map_err(crate::Error::from)?;
        let it = stmt
            .query_map(params, |r| r.get::<_, String>(0))
            .map_err(crate::Error::from)?
            .filter_map(|res| match res {
                Ok(prefix_str) => match StorePrefix::new(&prefix_str) {
                    Ok(store_prefix) => Some(Ok(store_prefix)),
                    Err(_e) => None,
                },
                Err(e) => Some(Err(crate::Error::from(e).into())),
            });
        it.collect()
    }
}

impl ReadableStorageTraits for RusqliteStore {
    fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<MaybeBytesIterator<'a>, StorageError> {
        let conn = self.connection()?;
        let Some((query, params, count)) = queries::get_partial_many_query(key, byte_ranges) else {
            return Ok(Some(Box::new([].into_iter())));
        };
        let Some(r) = conn
            .query_row(&query, params, |row| {
                Ok((0..count)
                    .map(|i| match row.get::<_, Vec<u8>>(i) {
                        Ok(v) => Ok(Bytes::from(v)),
                        Err(e) => Err(crate::Error::from(e).into()),
                    })
                    .collect::<Vec<_>>())
            })
            .optional()
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };

        Ok(Some(Box::new(r.into_iter())))
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::get_size_query(key);
        let size: Option<i64> = conn
            .query_row(query, params, |r| r.get(0))
            .optional()
            .map_err(crate::Error::from)?;
        Ok(size.map(|s| s as u64))
    }

    fn supports_get_partial(&self) -> bool {
        SUPPORTS_GET_PARTIAL
    }

    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::get_query(key);
        let Some(b) = conn
            .query_one(query, params, |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };
        Ok(Some(b.into()))
    }

    fn get_partial(
        &self,
        key: &StoreKey,
        byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::get_partial_query(key, byte_range);
        let Some(b) = conn
            .query_one(&query, params, |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .map_err(crate::Error::from)?
        else {
            return Ok(None);
        };
        Ok(Some(b.into()))
    }
}

impl ListableStorageTraits for RusqliteStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        let conn = self.connection()?;
        let query = queries::list_all_query();
        let mut stmt = conn.prepare(query).map_err(crate::Error::from)?;
        let it = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(crate::Error::from)?
            .filter_map(|res| match res {
                Ok(key_str) => match StoreKey::new(&key_str) {
                    Ok(store_key) => Some(Ok(store_key)),
                    Err(_e) => None,
                },
                Err(e) => Some(Err(crate::Error::from(e).into())),
            });
        it.collect::<Result<Vec<StoreKey>, StorageError>>()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::list_prefix_query(prefix);
        let mut stmt = conn.prepare(query).map_err(crate::Error::from)?;
        let it = stmt
            .query_map(params, |r| r.get::<_, String>(0))
            .map_err(crate::Error::from)?
            .filter_map(|res| match res {
                Ok(key_str) => match StoreKey::new(&key_str) {
                    Ok(store_key) => Some(Ok(store_key)),
                    Err(_e) => None,
                },
                Err(e) => Some(Err(crate::Error::from(e).into())),
            });
        it.collect::<Result<Vec<StoreKey>, StorageError>>()
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let conn = self.connection()?;
        Ok(StoreKeysPrefixes::new(
            self.list_child_keys(&conn, prefix)?,
            self.list_child_prefixes(&conn, prefix)?,
        ))
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let conn = self.connection()?;
        let (query, params) = queries::size_prefix_query(prefix);
        let size: i64 = conn
            .query_row(query, params, |r| r.get(0))
            .map_err(crate::Error::from)?;
        Ok(size as u64)
    }

    fn size(&self) -> Result<u64, StorageError> {
        let conn = self.connection()?;
        let query = queries::size_total_query();
        let size: i64 = conn
            .query_row(query, [], |r| r.get(0))
            .map_err(crate::Error::from)?;
        Ok(size as u64)
    }
}

impl WritableStorageTraits for RusqliteStore {
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        let (query, params) = queries::set_query(key, &value);
        conn.execute(query, params).map_err(crate::Error::from)?;
        if self.update_timestamp_on_write {
            self.update_modified_at()?;
        }
        Ok(())
    }

    fn set_partial_many(
        &self,
        _key: &StoreKey,
        _offset_values: OffsetBytesIterator,
    ) -> Result<(), StorageError> {
        if !self.write {
            Err(StorageError::ReadOnly)
        } else {
            Err(StorageError::Unsupported(
                "set_partial_many is unsupported".into(),
            ))
        }
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        let (query, params) = queries::erase_query(key);
        conn.execute(query, params).map_err(crate::Error::from)?;
        if self.update_timestamp_on_write {
            self.update_modified_at()?;
        }
        Ok(())
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if !self.write {
            return Err(StorageError::ReadOnly);
        }
        let conn = self.connection()?;
        let (query, params) = queries::erase_prefix_query(prefix);
        conn.execute(query, params).map_err(crate::Error::from)?;
        if self.update_timestamp_on_write {
            self.update_modified_at()?;
        }
        Ok(())
    }

    fn supports_set_partial(&self) -> bool {
        SUPPORTS_SET_PARTIAL
    }

    fn set_partial(
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
}

#[cfg(test)]
mod tests {
    use crate::tests::init;
    use std::{path::Path, sync::Arc};
    use temp_testdir::TempDir;

    use super::RusqliteStore;
    use crate::Options;
    use zarrs_storage::{
        Bytes, ReadableWritableListableStorage, StoreKey, StorePrefix, byte_range::ByteRange,
    };

    fn zarrdb_path(dir: impl AsRef<std::path::Path>) -> String {
        dir.as_ref()
            .join("test.zarrdb")
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn no_create_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let res = RusqliteStore::new(&Options::new_local(&p));
        assert!(res.is_err());
    }

    #[test]
    fn create_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let _store = RusqliteStore::new(&Options::new_local(&p).create()).unwrap();
        assert!(std::fs::exists(&p).unwrap());
    }

    fn make_and_drop_store(path: impl AsRef<Path>) {
        RusqliteStore::new(&Options::new_local(path.as_ref()).create()).unwrap();
    }

    #[test]
    fn open_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        make_and_drop_store(&p);

        let _store2 = RusqliteStore::new(&Options::new_local(&p)).unwrap();
    }

    #[test]
    fn read_metadata() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let store = RusqliteStore::new(&Options::new_local(&p).create()).unwrap();
        let meta = store.read_metadata().unwrap();
        assert_eq!(meta.sqlitestore_version, crate::LATEST_VERSION);
    }

    fn make_memstore() -> ReadableWritableListableStorage {
        let store = RusqliteStore::new(&Options::new_memory().create()).unwrap();
        Arc::new(store)
    }

    fn make_and_get_meta(path: impl AsRef<Path>) -> crate::Metadata {
        let store = RusqliteStore::new(&Options::new_local(path.as_ref()).create()).unwrap();
        store.read_metadata().unwrap()
    }

    #[test]
    fn truncate_store() {
        init();
        let dir = TempDir::default();
        let p = zarrdb_path(&dir);

        assert!(!std::fs::exists(&p).unwrap());
        let orig_meta = make_and_get_meta(&p);

        let store2 = RusqliteStore::new(&Options::new_local(&p).truncate()).unwrap();
        let new_meta = store2.read_metadata().unwrap();
        assert!(new_meta.created_at > orig_meta.created_at);
    }

    #[test]
    fn roundtrip_bytes() {
        init();
        let store = make_memstore();

        let key = StoreKey::new("test_key").unwrap();
        let data = b"Hello, world!";
        store.set(&key, Bytes::from_static(data)).unwrap();

        let read_data = store.get(&key).unwrap().unwrap();
        assert_eq!(data, &read_data[..]);
    }

    #[test]
    fn partial_bytes() {
        init();
        let store = make_memstore();

        let key = StoreKey::new("test_key").unwrap();
        let data = b"Hello, world!";
        store.set(&key, Bytes::from_static(data)).unwrap();

        let test_partial_read = |br: ByteRange, expected: &[u8]| {
            let read = store.get_partial(&key, br).unwrap().unwrap();
            assert_eq!(expected, &read[..]);
        };

        test_partial_read(ByteRange::FromStart(0, Some(5)), &data[0..5]);
        test_partial_read(ByteRange::FromStart(8, None), &data[8..]);
        test_partial_read(ByteRange::FromStart(8, Some(3)), &data[8..11]);
        test_partial_read(ByteRange::Suffix(6), &data[data.len() - 6..]);
    }

    fn check_strlike_contents(test: &[impl ToString], reference: &[impl ToString]) {
        let mut t: Vec<_> = test.iter().map(ToString::to_string).collect();
        t.sort();

        let mut r: Vec<_> = reference.iter().map(ToString::to_string).collect();
        r.sort();

        assert_eq!(t, r);
    }

    #[test]
    fn list_keys() {
        init();
        let store = make_memstore();

        let keys: Vec<_> = ["a", "a/b", "a/c/d"]
            .into_iter()
            .map(|s| StoreKey::new(s).unwrap())
            .collect();
        let data = Bytes::from_static(b"Hello, world!");

        for k in keys.iter() {
            store.set(k, data.clone()).unwrap();
        }

        let read_keys = store.list().unwrap();
        check_strlike_contents(&read_keys, &keys);

        let read_children = store.list_dir(&StorePrefix::new("a/").unwrap()).unwrap();
        check_strlike_contents(read_children.keys(), &["a/b"]);
        check_strlike_contents(read_children.prefixes(), &["a/c/"]);

        let read_descendants = store.list_prefix(&StorePrefix::new("a/").unwrap()).unwrap();
        check_strlike_contents(&read_descendants, &["a/b", "a/c/d"]);
    }
}
