use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;
use zarrs::storage::{Bytes, ReadableStorageTraits, StoreKey, WritableStorageTraits};
use zarrs_sqlite::{Options, RusqliteStore as Store};

fn create_memory(c: &mut Criterion) {
    c.bench_function("rusqlite_create_memory_store", |b| {
        b.iter(async || {
            let _s = Store::new(black_box(&Options::new_memory().create())).unwrap();
        })
    });
}

/// Read chunks of different sizes from the store.
///
/// Hope that the throughput stays constant as the chunk size increases;
/// or find a good chunk size recommendation.
fn read_memory(c: &mut Criterion) {
    let key = StoreKey::new("key").unwrap();

    let mut group = c.benchmark_group("rusqlite_read_memory");

    for size_kib in [0, 1, 8, 64, 128] {
        let size = size_kib * 1024;
        let v: Vec<_> = (0u64..size).map(|x| (x % 256) as u8).collect();
        let value = Bytes::from_owner(v);
        let store = Store::new(&Options::new_memory().create()).unwrap();
        store.set(&key, value).unwrap();
        group.throughput(criterion::Throughput::Bytes(size));
        group.bench_function(format!("{size_kib}KiB"), |b| {
            b.iter(|| {
                let _s = store.get(&key).unwrap().unwrap();
            })
        });
    }
}

/// Benchmark reading from store concurrently.
/// Hope that the throughput goes up (even if the time also goes up).
fn read_memory_concurrent(c: &mut Criterion) {
    let key = StoreKey::new("key").unwrap();

    let mut group = c.benchmark_group("rusqlite_read_memory_concurrent");
    let size: u64 = 1024 * 1024;
    let v: Vec<_> = (0..size).map(|x| (x % 256) as u8).collect();
    let value = Bytes::from_owner(v);

    let store = Arc::new(Store::new(&Options::new_memory().create()).unwrap());
    store.set(&key, value).unwrap();

    for n_reads in [1usize, 2, 4, 8, 16, 32, 64, 128] {
        group.throughput(criterion::Throughput::Bytes(size * n_reads as u64));
        group.bench_function(format!("{n_reads}threads"), |b| {
            let s1 = store.clone();
            let key1 = key.clone();
            b.iter(move || {
                let barrier = std::sync::Barrier::new(n_reads);
                std::thread::scope(|s| {
                    let mut handles = Vec::with_capacity(n_reads);
                    for _ in 0..n_reads {
                        let handle = s.spawn(|| {
                            barrier.wait();
                            s1.get(&key1).unwrap().unwrap();
                        });
                        handles.push(handle);
                    }
                    handles.into_iter().for_each(|h| h.join().unwrap());
                })
            })
        });
    }
}

criterion_group!(benches, create_memory, read_memory, read_memory_concurrent);
criterion_main!(benches);
