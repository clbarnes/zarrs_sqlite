use std::fmt::Write;

use zarrs_storage::{
    StoreKey, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{APPLICATION_ID, Metadata};

pub const SUPPORTS_GET_PARTIAL: bool = true;
pub const SUPPORTS_SET_PARTIAL: bool = false;

pub fn set_pragma_query() -> String {
    format!("PRAGMA application_id = 0x{APPLICATION_ID:x};")
}

pub fn create_metadata_table_query() -> &'static str {
    "CREATE TABLE zarr_sqlitestore_metadata(
        k TEXT PRIMARY KEY NOT NULL,
        v TEXT NOT NULL
    );"
}

pub fn create_zarr_table_query() -> &'static str {
    "CREATE TABLE zarr_sqlitestore(
        k TEXT PRIMARY KEY NOT NULL,
        v BLOB NOT NULL
    );"
}

pub fn create_schema_queries() -> String {
    let mut s = set_pragma_query();
    s.push('\n');
    s.push_str(create_metadata_table_query());
    s.push('\n');
    s.push_str(create_zarr_table_query());
    s
}

pub fn update_modified_at_query() -> &'static str {
    "INSERT OR REPLACE INTO zarr_sqlitestore_metadata(k, v) VALUES ('modified_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc', 'subsec'));"
}

pub fn read_metadata_query() -> &'static str {
    "SELECT k, v FROM zarr_sqlitestore_metadata;"
}

pub fn insert_unknown_metadata_query<'a>(
    k: &'a impl AsRef<str>,
    v: &'a impl AsRef<str>,
) -> (&'static str, (&'a str, &'a str)) {
    (
        "INSERT OR REPLACE INTO zarr_sqlitestore_metadata(k, v) VALUES(?1, ?2);",
        (k.as_ref(), v.as_ref()),
    )
}

pub fn insert_metadata_query(metadata: &Metadata) -> (&'static str, [String; 6]) {
    (
        "INSERT OR REPLACE INTO zarr_sqlitestore_metadata (k, v) VALUES
                ('sqlitestore_version', ?1),
                ('compatible_flags', ?2),
                ('incompatible_flags', ?3),
                ('created_by', ?4),
                ('created_at', ?5),
                ('modified_at', ?6);",
        [
            metadata.sqlitestore_version.to_string(),
            metadata.compatible_flags.to_string(),
            metadata.incompatible_flags.to_string(),
            metadata.created_by.clone(),
            metadata.created_at.to_string(),
            metadata.modified_at.to_string(),
        ],
    )
}

/// Returns rows with 1 string column.
pub fn list_child_keys_query(prefix: &StorePrefix) -> (&'static str, (String, String)) {
    (
        "SELECT k FROM zarr WHERE k LIKE ? and k NOT LIKE ?;",
        (format!("{prefix}%"), format!("{prefix}%/%")),
    )
}

/// Returns rows with 1 string column.
pub fn list_dir_prefixes_query(prefix: &StorePrefix) -> (&'static str, (i64, i64, String)) {
    (
        "SELECT DISTINCT substr(k, 1, instr(substr(k, ?), '/') + ?)
         FROM zarr
         WHERE k LIKE ?;",
        (
            prefix.as_str().len() as i64 + 1,
            prefix.as_str().len() as i64,
            format!("{prefix}%/%"),
        ),
    )
}

/// Returns rows with 1 blob column.
pub fn get_query(k: &StoreKey) -> (&'static str, (&str,)) {
    ("SELECT v FROM zarr WHERE k = ?;", (k.as_str(),))
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
                Some(len) => write!(f, "substr({}, {}, {})", self.name, offset + 1, len),
                None => write!(f, "substr({}, {})", self.name, offset + 1),
            },
            ByteRange::Suffix(len) => write!(f, "substr({}, -{})", self.name, len),
        }
    }
}

fn write_substrs(
    s: &mut String,
    name: &str,
    byte_ranges: impl IntoIterator<Item = ByteRange>,
) -> usize {
    let mut count = 0;
    for range in byte_ranges {
        let substr = Substr { name, range };
        if count == 0 {
            s.write_fmt(format_args!("{substr}")).unwrap();
        } else {
            s.write_fmt(format_args!(", {substr}")).unwrap();
        }
        count += 1;
    }
    count
}

/// Returns 0-1 rows with 1 blob column.
pub fn get_partial_query(key: &StoreKey, byte_range: ByteRange) -> (String, (&str,)) {
    let s = format!(
        "SELECT {} FROM zarr WHERE k = ? LIMIT 1;",
        Substr {
            name: "v",
            range: byte_range
        }
    );
    (s, (key.as_str(),))
}

/// Returns 0-1 rows with N blob columns.
pub fn get_partial_many_query<'a>(
    key: &'a StoreKey,
    byte_ranges: ByteRangeIterator<'_>,
) -> Option<(String, (&'a str,), usize)> {
    let mut s = String::from("SELECT ");

    let count = write_substrs(&mut s, "v", byte_ranges);
    if count == 0 {
        return None;
    }
    s.push_str(" FROM zarr WHERE k = ? LIMIT 1;");
    Some((s, (key.as_str(),), count))
}

/// Returns 0-1 rows with 1 integer column.
pub fn get_size_query(k: &StoreKey) -> (&'static str, (&str,)) {
    (
        "SELECT length(v) FROM zarr WHERE k = ? LIMIT 1;",
        (k.as_str(),),
    )
}

/// Returns rows with 1 string column.
pub fn list_all_query() -> &'static str {
    "SELECT k FROM zarr;"
}

/// Returns rows with 1 string column.
pub fn list_prefix_query(prefix: &StorePrefix) -> (&'static str, (String,)) {
    (
        "SELECT k FROM zarr WHERE k LIKE ?;",
        (format!("{prefix}%"),),
    )
}

pub fn size_prefix_query(prefix: &StorePrefix) -> (&'static str, (String,)) {
    (
        "SELECT sum(length(v)) FROM zarr WHERE k LIKE ?;",
        (format!("{prefix}%"),),
    )
}

/// Returns 1 row with 1 integer column.
pub fn size_total_query() -> &'static str {
    "SELECT sum(length(v)) FROM zarr;"
}

pub fn set_query<'a>(key: &'a StoreKey, value: &'a [u8]) -> (&'static str, (&'a str, &'a [u8])) {
    (
        "INSERT OR REPLACE INTO zarr(k, v) VALUES(?1, ?2);",
        (key.as_str(), value),
    )
}

pub fn erase_query(key: &StoreKey) -> (&'static str, (&str,)) {
    ("DELETE FROM zarr WHERE k = ?;", (key.as_str(),))
}

pub fn erase_prefix_query(prefix: &StorePrefix) -> (&'static str, (String,)) {
    ("DELETE FROM zarr WHERE k LIKE ?;", (format!("{prefix}%"),))
}
