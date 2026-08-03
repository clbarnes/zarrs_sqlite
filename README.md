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

This crate may support multiple SQLite backends in future,
each behind a cargo feature.
Today, the only supported backend is [turso](https://github.com/tursodatabase/turso) (the default `backend-turso` feature).

`backend-turso` provides `zarrs_sqlite::TursoStore`, which implements `zarrs::storage::AsyncReadableWritableListableStorageTraits`.
It requires the use of a [tokio](https://crates.io/crates/tokio) runtime.
