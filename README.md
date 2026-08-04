# zarrs_sqlite

An SQLite-based Zarr store for the [zarrs](https://zarrs.dev/) ecosystem.

Work in progress; see the [specification proposal](https://github.com/auxym/zarr-sqlite-python/pull/4).

## Usage

```rust
/// `None` would build an in-memory database.
let mut builder = zarrs_sqlite::TursoStore::builder(Some("path/to/file.zarrdb"));

// By default, this will be read-only and the database must already exist.

// The below allows the builder to create a new database, and truncates any existing database.
builder.create().truncate();

let inner = builder.build().await.unwrap();
let store: zarrs::storage::AsyncReadableWritableListableStorage = Arc::new(inner);
```

## Features

### Backends

This crate supports multiple SQLite backends, each behind a cargo feature.

| store | feature | backend | notes |
| - | - | - | - |
| `zarrs_sqlite::TursoStore` | `backend-turso` | [turso](https://github.com/tursodatabase/turso) | Async (requires tokio), WAL mode, pure rust |
| `zarrs_sqlite::RusqliteStore` | `backend-rusqlite` | [rusqlite](https://github.com/rusqlite/rusqlite) + [r2d2](https://github.com/sfackler/r2d2) | Sync, binds to libsqlite3; default |
