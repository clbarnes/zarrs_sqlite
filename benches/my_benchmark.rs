use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use zarrs::storage::{AsyncReadableStorageTraits, AsyncWritableStorageTraits, Bytes, StoreKey};
use zarrs_sqlite::TursoStore;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn create_memory(c: &mut Criterion) {
    c.bench_function("create_memory_store", |b| {
        b.to_async(runtime()).iter(async || {
            let mut b = TursoStore::builder(None);
            b.create();
            let _s = b.build().await.unwrap();
        })
    });
}

/// Read chunks of different sizes from the store.
///
/// Hope that the throughput stays constant as the chunk size increases;
/// or find a good chunk size recommendation.
fn read_memory(c: &mut Criterion) {
    let rt = runtime();
    let key = StoreKey::new("key").unwrap();

    let mut group = c.benchmark_group("read_memory");

    for size_kib in [0, 1, 8, 64, 128] {
        let size = size_kib * 1024;
        let v: Vec<_> = (0u64..size).map(|x| (x % 256) as u8).collect();
        let value = Bytes::from_owner(v);
        let store = rt.block_on(async {
            let mut b = TursoStore::builder(None);
            b.create();
            let s = b.build().await.unwrap();
            s.set(&key, value).await.unwrap();
            s
        });
        group.throughput(criterion::Throughput::Bytes(size));
        group.bench_function(format!("{size_kib}KiB"), |b| {
            b.to_async(runtime()).iter(async || {
                let _s = store.get(&key).await.unwrap().unwrap();
            })
        });
    }
}

/// Benchmark reading from store concurrently.
/// Hope that the throughput goes up (even if the time also goes up).
fn read_memory_concurrent(c: &mut Criterion) {
    let rt = runtime();
    let key = StoreKey::new("key").unwrap();

    let mut group = c.benchmark_group("read_memory_concurrent");
    let size: u64 = 1024 * 1024;
    let v: Vec<_> = (0..size).map(|x| (x % 256) as u8).collect();
    let value = Bytes::from_owner(v);

    let store = Arc::new(rt.block_on(async {
        let mut b = TursoStore::builder(None);
        b.create();
        let s = b.build().await.unwrap();
        s.set(&key, value).await.unwrap();
        s
    }));

    for n_reads in [1, 2, 4, 8, 16, 32, 64, 128] {
        group.throughput(criterion::Throughput::Bytes(size * n_reads));
        group.bench_function(format!("{n_reads}threads"), |b| {
            b.to_async(runtime()).iter(async || {
                let mut set = tokio::task::JoinSet::new();
                for _ in 0..n_reads {
                    let storage_inner = store.clone();
                    let key = key.clone();
                    set.spawn(async move {
                        let _s = storage_inner.get(&key).await.unwrap().unwrap();
                    });
                }
                let _res = set.join_all().await;
            })
        });
    }
}

criterion_group!(benches, create_memory, read_memory, read_memory_concurrent);
criterion_main!(benches);
