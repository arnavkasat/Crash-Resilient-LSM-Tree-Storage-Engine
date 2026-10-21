//! Criterion micro-benchmarks.
//!
//! These measure per-operation latency. For sustained throughput over a large
//! dataset, use the `lsmbench` binary instead.
//!
//! ```text
//! cargo bench
//! cargo run --release --bin lsmbench -- --keys 2000000
//! ```

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lsmkv::testkit::{key_for, value_for, Rng};
use lsmkv::{Db, Options, SyncMode, WriteBatch};

const VALUE_SIZE: usize = 100;

/// Only even keys are loaded, so a lookup of an odd key is a true miss that
/// still falls inside the key range of real files.
fn hit_key(i: u64) -> Vec<u8> {
    key_for(i * 2)
}

fn miss_key(i: u64) -> Vec<u8> {
    key_for(i * 2 + 1)
}

struct Bed {
    dir: PathBuf,
    db: Option<Db>,
}

impl Bed {
    fn new(tag: &str, opts: Options) -> Bed {
        let dir = std::env::temp_dir().join(format!("lsmkv-bench-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Db::open(&dir, opts).unwrap();
        Bed { dir, db: Some(db) }
    }

    fn db(&self) -> &Db {
        self.db.as_ref().unwrap()
    }
}

impl Drop for Bed {
    fn drop(&mut self) {
        self.db.take();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn populate(db: &Db, n: u64) {
    let mut batch = WriteBatch::new();
    for i in 0..n {
        batch.put(hit_key(i), value_for(i, VALUE_SIZE));
        if batch.len() == 500 {
            db.write(&batch).unwrap();
            batch.clear();
        }
    }
    db.write(&batch).unwrap();
    db.flush().unwrap();
    db.compact_all().unwrap();
}

fn write_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    for (name, sync_mode) in [
        ("buffered", SyncMode::Os),
        ("fsync_every_write", SyncMode::EveryWrite),
    ] {
        let bed = Bed::new(
            &format!("write-{name}"),
            Options {
                sync_mode,
                ..Default::default()
            },
        );
        let mut i = 0u64;
        group.bench_function(BenchmarkId::new("put", name), |b| {
            b.iter(|| {
                bed.db().put(key_for(i), value_for(i, VALUE_SIZE)).unwrap();
                i += 1;
            })
        });
    }

    // Batching amortises the log append and (in fsync mode) the fsync itself.
    let bed = Bed::new(
        "write-batch",
        Options {
            sync_mode: SyncMode::Os,
            ..Default::default()
        },
    );
    let mut i = 0u64;
    group.throughput(Throughput::Elements(100));
    group.bench_function("batch_of_100", |b| {
        b.iter(|| {
            let mut batch = WriteBatch::new();
            for _ in 0..100 {
                batch.put(key_for(i), value_for(i, VALUE_SIZE));
                i += 1;
            }
            bed.db().write(&batch).unwrap();
        })
    });
    group.finish();
}

fn read_benchmarks(c: &mut Criterion) {
    const N: u64 = 200_000;
    let bed = Bed::new(
        "read",
        Options {
            sync_mode: SyncMode::Os,
            ..Default::default()
        },
    );
    populate(bed.db(), N);

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    let mut rng = Rng::new(1);
    group.bench_function("random_hit", |b| {
        b.iter(|| {
            let i = rng.below(N);
            black_box(bed.db().get(&hit_key(i)).unwrap());
        })
    });

    let mut rng = Rng::new(2);
    group.bench_function("random_miss", |b| {
        b.iter(|| {
            let i = rng.below(N);
            black_box(bed.db().get(&miss_key(i)).unwrap());
        })
    });

    // How much the Bloom filter and the block cache are actually worth: the
    // same reads against the same data, with the accelerators switched off.
    let bare = Bed::new(
        "read-bare",
        Options {
            sync_mode: SyncMode::Os,
            bloom_bits_per_key: 0,
            block_cache_size: 64 << 10,
            ..Default::default()
        },
    );
    populate(bare.db(), N);
    let mut rng = Rng::new(1);
    group.bench_function("random_hit_without_bloom_or_cache", |b| {
        b.iter(|| {
            let i = rng.below(N);
            black_box(bare.db().get(&hit_key(i)).unwrap());
        })
    });
    let mut rng = Rng::new(2);
    group.bench_function("random_miss_without_bloom_or_cache", |b| {
        b.iter(|| {
            let i = rng.below(N);
            black_box(bare.db().get(&miss_key(i)).unwrap());
        })
    });
    group.finish();
}

fn scan_benchmarks(c: &mut Criterion) {
    const N: u64 = 100_000;
    let bed = Bed::new(
        "scan",
        Options {
            sync_mode: SyncMode::Os,
            ..Default::default()
        },
    );
    populate(bed.db(), N);

    let mut group = c.benchmark_group("scan");
    group.sample_size(20);
    for len in [100u64, 1_000] {
        group.throughput(Throughput::Elements(len));
        let mut rng = Rng::new(3);
        group.bench_with_input(BenchmarkId::from_parameter(len), &len, |b, &len| {
            b.iter(|| {
                let start = rng.below(N - len);
                let out = bed
                    .db()
                    .scan(
                        std::ops::Bound::Included(&hit_key(start)),
                        std::ops::Bound::Excluded(&hit_key(start + len)),
                    )
                    .unwrap();
                black_box(out.len());
            })
        });
    }
    group.finish();
}

criterion_group!(benches, write_benchmarks, read_benchmarks, scan_benchmarks);
criterion_main!(benches);
