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
        let mut flags = OpenFlags::default();
        if options.write {
            flags |= OpenFlags::SQLITE_OPEN_READ_WRITE;
        } else {
            flags |= OpenFlags::SQLITE_OPEN_READ_ONLY;
        }
        if options.create {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
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
