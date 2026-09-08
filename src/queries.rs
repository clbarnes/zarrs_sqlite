use std::fmt::Write;

use zarrs_storage::{
    StoreKey, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use crate::{APPLICATION_ID, Metadata};

pub const SUPPORTS_GET_PARTIAL: bool = true;
pub const SUPPORTS_SET_PARTIAL: bool = false;

pub fn set_application_id_pragma() -> String {
    format!("PRAGMA application_id = 0x{APPLICATION_ID:x};")
}

pub fn create_metadata_table_query() -> &'static str {
    "CREATE TABLE zarr_sqlitestore_metadata(
        k TEXT PRIMARY KEY NOT NULL,
        v TEXT NOT NULL
    );"
}

pub fn create_zarr_table_query() -> &'static str {
    "CREATE TABLE zarr (
        k TEXT PRIMARY KEY NOT NULL,
        v BLOB NOT NULL
    );"
}

pub fn create_schema_queries() -> String {
    format!(
        "BEGIN;\n{}\n{}\n{}\nCOMMIT;",
        set_application_id_pragma(),
        create_metadata_table_query(),
        create_zarr_table_query()
    )
}

pub fn update_modified_at_query() -> &'static str {
    "INSERT INTO zarr_sqlitestore_metadata(k, v) VALUES ('modified_at', strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc', 'subsec')) ON CONFLICT(k)
    DO UPDATE SET v = excluded.v;;"
}

pub fn read_metadata_query() -> &'static str {
    "SELECT k, v FROM zarr_sqlitestore_metadata;"
}

pub fn insert_unknown_metadata_query<'a>(
    k: &'a impl AsRef<str>,
    v: &'a impl AsRef<str>,
) -> (&'static str, (&'a str, &'a str)) {
    (
        "INSERT INTO zarr_sqlitestore_metadata(k, v) VALUES(?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v;",
        (k.as_ref(), v.as_ref()),
    )
}

pub fn insert_core_metadata_query(metadata: &Metadata) -> (&'static str, [String; 3]) {
    (
        "INSERT INTO zarr_sqlitestore_metadata (k, v) VALUES
                ('sqlitestore_version', ?1),
                ('compatible_flags', ?2),
                ('incompatible_flags', ?3)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v;",
        [
            metadata.sqlitestore_version.to_string(),
            metadata.compatible_flags.to_string(),
            metadata.incompatible_flags.to_string(),
        ],
    )
}

fn maybe_insert_metadata_kv_query<'a>(
    k: &'a str,
    v: Option<&'a str>,
) -> Option<(&'static str, (&'a str, &'a str))> {
    v.as_ref().map(|v| {
        (
            "INSERT INTO zarr_sqlitestore_metadata(k, v) VALUES(?1, ?2)
            ON CONFLICT DO UPDATE SET v=excluded.v;",
            (k, *v),
        )
    })
}

pub fn maybe_insert_created_by_query(metadata: &Metadata) -> Option<(&'static str, (&str, &str))> {
    maybe_insert_metadata_kv_query("created_by", metadata.created_by.as_deref())
}

pub fn maybe_insert_modified_at_query(metadata: &Metadata) -> Option<(&'static str, (String,))> {
    metadata.modified_at.as_ref().map(|modified_at| {
        (
            "INSERT INTO zarr_sqlitestore_metadata(k, v) VALUES('modified_at', ?1)
            ON CONFLICT DO UPDATE SET v=excluded.v;",
            (modified_at.to_string(),),
        )
    })
}

/// If the store prefix is not the root (empty), returns a string that is the prefix with the trailing `/` replaced by `0`.
///
/// Used in descendant-listing queries.
fn prefix_upper(prefix: &StorePrefix) -> Option<String> {
    if prefix.as_str().is_empty() {
        return None;
    }
    let mut s = prefix.to_string();
    s.pop().expect("prefix should not be empty");
    s.push('0');
    Some(s)
}

/// Returns None if root.
pub fn list_prefix_query(prefix: &StorePrefix) -> Option<(&'static str, (String, String))> {
    let upper = prefix_upper(prefix)?;

    let q = "SELECT k FROM zarr WHERE k > ?1 AND k < ?2;";
    Some((q, (prefix.to_string(), upper)))
}

/// Query for list_dir on the root of the store (empty prefix).
///
/// Query returns 2 columns: `type` (either `'k'` for key or `'p'` for prefix) and `path` (which will end with `/` for prefixes).
pub fn list_dir_root_query() -> &'static str {
    "SELECT
        'k' AS type,
        k AS path
    FROM zarr
    WHERE instr(k, '/') = 0

    UNION

    SELECT DISTINCT
        'p' AS type,
        substr(k, 1, instr(k, '/')) AS path
    FROM zarr
    WHERE instr(k, '/') > 0

    ORDER BY path;"
}

/// Query for list_dir.
///
/// Query returns 2 columns: `type` (either `'k'` for key or `'p'` for prefix) and `path` (which will end with `/` for prefixes).
pub fn list_dir_query(prefix: &StorePrefix) -> Option<(&'static str, (String, String))> {
    let upper = prefix_upper(prefix)?;

    let q = "WITH matches AS (
        SELECT
            k,
            substr(k, length(?1) + 1) AS rest
        FROM zarr
        WHERE k > ?1
          AND k < ?2
    )
    SELECT
        'k' AS type,
        k AS path
    FROM matches
    WHERE instr(rest, '/') = 0

    UNION

    SELECT DISTINCT
        'p' AS type,
        ?1 || substr(rest, 1, instr(rest, '/')) AS path
    FROM matches
    WHERE instr(rest, '/') > 0;";
    Some((q, (prefix.to_string(), upper)))
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

pub fn size_prefix_query(prefix: &StorePrefix) -> Option<(&'static str, (String, String))> {
    let upper = prefix_upper(prefix)?;
    Some((
        "SELECT k FROM zarr WHERE k > ?1 AND k < ?2;",
        (prefix.to_string(), upper),
    ))
}

/// Returns 1 row with 1 integer column.
pub fn size_total_query() -> &'static str {
    "SELECT sum(length(v)) FROM zarr;"
}

pub fn set_query<'a>(key: &'a StoreKey, value: &'a [u8]) -> (&'static str, (&'a str, &'a [u8])) {
    (
        "INSERT INTO zarr(k, v) VALUES(?1, ?2) ON CONFLICT (k) DO UPDATE SET v = excluded.v;",
        (key.as_str(), value),
    )
}

pub fn erase_query(key: &StoreKey) -> (&'static str, (&str,)) {
    ("DELETE FROM zarr WHERE k = ?;", (key.as_str(),))
}

pub fn erase_prefix_query(prefix: &StorePrefix) -> (&'static str, (String,)) {
    ("DELETE FROM zarr WHERE k LIKE ?;", (format!("{prefix}%"),))
}
