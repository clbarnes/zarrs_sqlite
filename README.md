# zarrs_sqlite

An SQLite-based Zarr store for the [zarrs](https://zarrs.dev/) ecosystem, implementing the [zarr-sqlite-python specification](https://github.com/auxym/zarr-sqlite-python/blob/main/SPEC.md).

## Usage

```rust
use std::sync::Arc;
use zarrs::storage::ReadableWritableListableStorage;
use zarrs_sqlite::{Options, RusqliteStore};

// Use new_local("path/to/file.zarrdb") for a file-backed database.
let options = Options::new_memory()
    // Allow (but do not require) creation of new database.
    .create()
    // Allow (but do not require) deleting an existing database.
    .truncate();

// Stores are opened read-only by default, unless `.create()`, `.truncate()`, or `.write()` are used.

let inner = RusqliteStore::new(&options).expect("could not open store");
// Alternatively use `TursoStore` for WAL + async.

let store: ReadableWritableListableStorage = Arc::new(inner);
```

## Features

### Backends

This crate supports multiple SQLite backends, each behind a cargo feature.

#### `RusqliteStore`

- feature: `backend-rusqlite` (default)
- crates: [rusqlite](https://github.com/rusqlite/rusqlite) + [r2d2](https://github.com/sfackler/r2d2)

Synchronous store which uses connection pooling and binds to libsqlite3.

#### `TursoStore`

- feature: `backend-turso`
- crates: [turso](https://github.com/tursodatabase/turso) + [tokio](https://github.com/tokio-rs/tokio)

Asynchronous store written in pure rust which uses write-ahead log journaling by default.

After you've finished writing to the store, use `my_store.checkpoint().await` to write the contents of the WAL to the main database file.
