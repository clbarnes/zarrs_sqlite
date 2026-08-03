use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    str::FromStr,
};

const FLAG_SEPARATOR: char = ',';

/// Semver-style major.minor version number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
}

impl Version {
    pub fn new(major: u64, minor: u64) -> Self {
        Version { major, minor }
    }

    /// Whether the major versions are the same and this version's minor version is greater than or equal to the other version's.
    ///
    /// i.e. this version's reader SHOULD be able to read data by the other version's writer.
    pub fn minor_compatible(&self, other: &Version) -> bool {
        self.major_compatible(other) && self.minor >= other.minor
    }

    /// Whether the major versions are the same.
    ///
    /// i.e. this version's reader SHOULD be able to at least partially read data by the other version's writer.
    pub fn major_compatible(&self, other: &Version) -> bool {
        self.major == other.major
    }
}

impl From<(u64, u64)> for Version {
    fn from((major, minor): (u64, u64)) -> Self {
        Version { major, minor }
    }
}

impl Default for Version {
    fn default() -> Self {
        crate::LATEST_VERSION
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl FromStr for Version {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut it = s.split('.');
        let invalid_ver = || crate::Error::invalid_version(s);
        let major = it
            .next()
            .ok_or_else(invalid_ver)?
            .parse::<u64>()
            .map_err(|_| invalid_ver())?;
        let minor = it
            .next()
            .ok_or_else(invalid_ver)?
            .parse::<u64>()
            .map_err(|_| invalid_ver())?;
        if it.next().is_some() {
            return Err(invalid_ver());
        }
        Ok(Version { major, minor })
    }
}

/// A set of string feature flags.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Flags(BTreeSet<String>);

impl Flags {
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|s| s.as_ref())
    }
}

impl Display for Flags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for flag in self.0.iter().filter(|s| !s.is_empty()) {
            if first {
                first = false;
                write!(f, "{flag}")?;
            } else {
                write!(f, "{FLAG_SEPARATOR}{flag}")?;
            }
        }
        Ok(())
    }
}

impl FromStr for Flags {
    /// Infallible
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.split(FLAG_SEPARATOR).map(ToString::to_string).collect())
    }
}

impl FromIterator<String> for Flags {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self(iter.into_iter().filter(|s| !s.is_empty()).collect())
    }
}

#[derive(Debug, Clone)]
pub struct SqliteStoreMetadata {
    pub sqlitestore_version: Version,
    pub compatible_flags: Flags,
    pub incompatible_flags: Flags,
    pub created_by: String,
    pub created_at: SqliteTimestamp,
    pub modified_at: SqliteTimestamp,
    /// Any unknown key-value pairs found in the metadata table.
    pub unknown: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SqliteStoreMetadataBuilder {
    version: Option<Version>,
    compatible_flags: Option<Flags>,
    incompatible_flags: Option<Flags>,
    created_by: Option<String>,
    created_at: Option<SqliteTimestamp>,
    modified_at: Option<SqliteTimestamp>,
    unknown: BTreeMap<String, String>,
}

impl SqliteStoreMetadataBuilder {
    pub(crate) fn add_key_value(
        &mut self,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<bool, crate::Error> {
        let key = key.as_ref();
        let value = value.as_ref();
        let invalid = || crate::Error::InvalidMetadata {
            key: key.to_string(),
            value: Some(value.to_string()),
        };
        match key {
            "sqlitestore_version" => {
                self.version = Some(value.parse().map_err(|_| invalid())?);
            }
            "compatible_flags" => {
                self.compatible_flags = Some(value.parse().map_err(|_| invalid())?);
            }
            "incompatible_flags" => {
                self.incompatible_flags = Some(value.parse().map_err(|_| invalid())?);
            }
            "created_by" => {
                self.created_by = Some(value.to_string());
            }
            "created_at" => {
                self.created_at = Some(value.parse().map_err(|_| invalid())?);
            }
            "modified_at" => {
                self.modified_at = Some(value.parse().map_err(|_| invalid())?);
            }
            _ => {
                self.unknown.insert(key.to_string(), value.to_string());
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn build(self) -> Result<SqliteStoreMetadata, crate::Error> {
        let version = self.version.ok_or_else(|| crate::Error::InvalidMetadata {
            key: "sqlitestore_version".to_string(),
            value: None,
        })?;
        let compatible_flags = self.compatible_flags.unwrap_or_default();
        let incompatible_flags = self.incompatible_flags.unwrap_or_default();
        let created_by = self.created_by.unwrap_or_default();
        let created_at = self
            .created_at
            .ok_or_else(|| crate::Error::InvalidMetadata {
                key: "created_at".to_string(),
                value: None,
            })?;
        let modified_at = self.modified_at.unwrap_or(created_at);
        Ok(SqliteStoreMetadata {
            sqlitestore_version: version,
            compatible_flags,
            incompatible_flags,
            created_by,
            created_at,
            modified_at,
            unknown: self.unknown,
        })
    }
}

impl Default for SqliteStoreMetadata {
    fn default() -> Self {
        let t = SqliteTimestamp::now();
        Self {
            sqlitestore_version: crate::LATEST_VERSION,
            compatible_flags: Default::default(),
            incompatible_flags: Default::default(),
            created_by: String::new(),
            created_at: t,
            modified_at: t,
            unknown: Default::default(),
        }
    }
}

impl SqliteStoreMetadata {
    pub fn with_created_by(created_by: impl Into<String>) -> Self {
        Self {
            created_by: created_by.into(),
            ..Default::default()
        }
    }

    pub(crate) fn builder() -> SqliteStoreMetadataBuilder {
        SqliteStoreMetadataBuilder::default()
    }
}

/// Timestamp type wrapping [jiff::Timestamp],
/// representing timestamps stored as RFC-3339 strings in UTC in the SQLite metadata table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SqliteTimestamp(jiff::Timestamp);

impl FromStr for SqliteTimestamp {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ts = match s.parse::<jiff::Timestamp>() {
            Ok(ts) => ts,
            Err(_) => {
                let dt: jiff::civil::DateTime =
                    s.parse().map_err(|_| crate::Error::invalid_timestamp(s))?;
                let zoned = dt
                    .to_zoned(jiff::tz::TimeZone::UTC)
                    .expect("UTC zone should be valid");
                zoned.timestamp()
            }
        };
        Ok(Self(ts))
    }
}

impl Display for SqliteTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fmt = if self.has_subsec() {
            "%Y-%m-%dT%H:%M:%S%.3fZ"
        } else {
            "%Y-%m-%dT%H:%M:%SZ"
        };
        write!(f, "{}", self.0.strftime(fmt))
    }
}

impl SqliteTimestamp {
    pub fn now() -> Self {
        Self(jiff::Timestamp::now())
    }

    pub fn now_second() -> Self {
        use jiff::{RoundMode, Timestamp, TimestampRound, Unit};
        let ts = Timestamp::now()
            .round(
                TimestampRound::default()
                    .smallest(Unit::Second)
                    .mode(RoundMode::Floor),
            )
            .expect("round to second should succeed");
        Self(ts)
    }

    pub(crate) fn has_subsec(&self) -> bool {
        self.0.subsec_nanosecond() > 0
    }

    pub fn inner(&self) -> &jiff::Timestamp {
        &self.0
    }
}

impl From<jiff::Timestamp> for SqliteTimestamp {
    fn from(ts: jiff::Timestamp) -> Self {
        SqliteTimestamp(ts)
    }
}

#[cfg(test)]
mod tests {
    use crate::SqliteTimestamp;

    use super::{Flags, Version};

    #[test]
    fn roundtrip_version() {
        let version = Version { major: 1, minor: 2 };
        let s = version.to_string();
        assert_eq!(s, "1.2");
        let parsed = s.parse::<super::Version>().unwrap();
        assert_eq!(version, parsed);
    }

    #[test]
    fn roundtrip_flags() {
        let flags = Flags::from_iter(["a".to_string(), "b".to_string()]);
        let s = flags.to_string();
        let parsed: Flags = s.parse().unwrap();
        assert_eq!(flags, parsed);
    }

    #[test]
    fn roundtrip_subsec_date() {
        let sqlite_ts = "1999-12-31 23:59:59.999";
        let ts_fmt = "%Y-%m-%d %H:%M:%S%.3f";
        let parsed: jiff::civil::DateTime = sqlite_ts.parse().unwrap();
        assert_eq!(format!("{}", parsed.strftime(ts_fmt)), sqlite_ts);
    }

    #[cfg(feature = "backend-turso")]
    async fn query_single<T: turso::core::types::FromValue>(q: &str) -> T {
        let db = turso::Builder::new_local(":memory:").build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut rows = conn.query(q, ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get(0).unwrap()
    }

    #[cfg(feature = "backend-turso")]
    #[tokio::test]
    async fn roundtrip_sqlite_date_subsec() {
        use crate::SqliteTimestamp;

        let ts_str: String =
            query_single("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc', 'subsec')").await;
        let ts: SqliteTimestamp = ts_str.parse().unwrap();
        assert_eq!(ts.to_string(), ts_str);
    }

    #[cfg(feature = "backend-turso")]
    #[tokio::test]
    async fn roundtrip_sqlite_date() {
        use crate::SqliteTimestamp;

        let ts_str: String =
            query_single("SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc')").await;
        let ts: SqliteTimestamp = ts_str.parse().unwrap();
        assert_eq!(ts.to_string(), ts_str);
    }

    #[test]
    fn parse_timestamp() {
        for s in &[
            "2024-06-01T12:34:56.789",
            "2024-06-01 12:34:56.789",
            "2024-06-01 12:34:56",
            "2024-06-01 12:34:56+00:00",
            "2024-06-01 12:34:56+02:00",
        ] {
            let ts: SqliteTimestamp = s.parse().unwrap();
            println!("Parsed timestamp from '{}': {:?}", s, ts);
        }
    }
}
