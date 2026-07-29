use std::{collections::BTreeSet, fmt::Display, str::FromStr};

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
    pub fn can_read(&self, other: &Version) -> bool {
        self.may_read(other) && self.minor >= other.minor
    }

    /// Whether the major versions are the same.
    ///
    /// A reader with the same major version but a lower minor version should be able to partially read data.
    pub fn may_read(&self, other: &Version) -> bool {
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut it = s.split('.');
        let major = it
            .next()
            .ok_or_else(|| "no major version part found".to_string())?
            .parse::<u64>()
            .map_err(|e| e.to_string())?;
        let minor = it
            .next()
            .ok_or_else(|| "no minor version part found".to_string())?
            .parse::<u64>()
            .map_err(|e| e.to_string())?;
        if it.next().is_some() {
            return Err("too many version parts found".to_string());
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
    pub created_time: jiff::Timestamp,
}

impl SqliteStoreMetadata {
    pub fn from_strs(
        version_str: impl AsRef<str>,
        compatible_flags: impl AsRef<str>,
        incompatible_flags: impl AsRef<str>,
        created_by: impl Into<String>,
        created_time: impl AsRef<str>,
    ) -> Result<Self, String> {
        let sqlitestore_version = version_str.as_ref().parse::<Version>()?;
        let created_time = created_time
            .as_ref()
            .parse::<jiff::Timestamp>()
            .map_err(|e| e.to_string())?;

        // Flags::from_str is infallible
        let compatible_flags: Flags = compatible_flags.as_ref().parse().unwrap();
        let incompatible_flags: Flags = incompatible_flags.as_ref().parse().unwrap();

        Ok(Self {
            sqlitestore_version,
            compatible_flags,
            incompatible_flags,
            created_by: created_by.into(),
            created_time,
        })
    }
}

impl Default for SqliteStoreMetadata {
    fn default() -> Self {
        Self {
            sqlitestore_version: crate::LATEST_VERSION,
            compatible_flags: Default::default(),
            incompatible_flags: Default::default(),
            created_by: String::new(),
            created_time: jiff::Timestamp::now(),
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
}

#[cfg(test)]
mod tests {
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
}
