//! `lsmbench` - sustained throughput harness.
//!
//! Criterion (`cargo bench`) measures per-operation latency on a warm engine.
//! This one answers the other question: how many operations per second the
//! engine sustains while it is also flushing and compacting, and what the
//! Bloom filters and block cache are actually worth.
//!
//! ```text
//! cargo run --release --bin lsmbench -- --keys 1000000
//! cargo run --release --bin lsmbench -- --keys 200000000 --value-size 100   # ~20 GB
//! ```

use std::time::Instant;

use lsmkv::testkit::{key_for, value_for, Rng};
use lsmkv::{Db, Options, SyncMode, WriteBatch};

struct Args {
    dir: String,
    keys: u64,
    value_size: usize,
    read_ops: u64,
    batch: usize,
    sync_mode: SyncMode,
    keep: bool,
    compare: bool,
    random_order: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        dir: std::env::temp_dir()
            .join("lsmkv-bench")
            .to_string_lossy()
            .into_owned(),
        keys: 1_000_000,
        value_size: 100,
        read_ops: 200_000,
        batch: 1,
        sync_mode: SyncMode::Os,
        keep: false,
        compare: true,
        random_order: false,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let next = |i: usize| argv.get(i + 1).cloned().unwrap_or_default();
        match argv[i].as_str() {
            "--dir" => {
                args.dir = next(i);
                i += 1;
            }
            "--keys" => {
                args.keys = next(i).parse().unwrap_or(args.keys);
                i += 1;
            }
            "--value-size" => {
                args.value_size = next(i).parse().unwrap_or(args.value_size);
                i += 1;
            }
            "--read-ops" => {
                args.read_ops = next(i).parse().unwrap_or(args.read_ops);
                i += 1;
            }
            "--batch" => {
                args.batch = next(i).parse().unwrap_or(args.batch);
                i += 1;
            }
            "--sync" => {
                args.sync_mode = if next(i) == "every" {
                    SyncMode::EveryWrite
                } else {
                    SyncMode::Os
                };
                i += 1;
            }
            "--keep" => args.keep = true,
            "--random" => args.random_order = true,
            "--no-compare" => args.compare = false,
            "--help" | "-h" => {
                println!(
                    "usage: lsmbench [--dir D] [--keys N] [--value-size S] [--read-ops N]\n\
                     \t[--batch N] [--sync os|every] [--random] [--keep] [--no-compare]"
                );
                std::process::exit(0);
            }
            other => eprintln!("ignoring unknown argument {other}"),
        }
        i += 1;
    }
    args
}

/// Loaded keys are the even ones, so a "miss" key lands *between* two real
/// keys instead of sorting past every file. A miss that falls outside every
/// file's range is answered from the manifest alone and measures nothing.
fn hit_key(i: u64) -> Vec<u8> {
    key_for(i * 2)
}

fn miss_key(i: u64) -> Vec<u8> {
    key_for(i * 2 + 1)
}

fn rate(ops: u64, seconds: f64) -> String {
    let per_sec = ops as f64 / seconds;
    if per_sec >= 1_000.0 {
        format!("{:>9.1}K ops/sec", per_sec / 1_000.0)
    } else {
        format!("{per_sec:>9.1} ops/sec")
    }
}

fn load(db: &Db, args: &Args) -> f64 {
    let start = Instant::now();
    let mut batch = WriteBatch::new();
    let mut next_report = args.keys / 10;
    // Random order makes level ranges overlap, so compaction has to do real
    // merging work instead of relabelling files.
    let mut rng = Rng::new(0xA5A5);
    for n in 0..args.keys {
        let i = if args.random_order {
            rng.below(args.keys)
        } else {
            n
        };
        batch.put(hit_key(i), value_for(i, args.value_size));
        if batch.len() >= args.batch {
            db.write(&batch).unwrap();
            batch.clear();
        }
        if n == next_report && n > 0 {
            let elapsed = start.elapsed().as_secs_f64();
            eprintln!("  .. {n} keys written ({})", rate(n, elapsed));
            next_report += args.keys / 10;
        }
    }
    if !batch.is_empty() {
        db.write(&batch).unwrap();
    }
    db.sync().unwrap();
    start.elapsed().as_secs_f64()
}

/// `miss_ratio` is a percentage. `hot` concentrates 90% of the reads on 10% of
/// the key space, which is what most real workloads look like and what the
/// block cache exists for.
fn random_reads(db: &Db, args: &Args, seed: u64, miss_ratio: u64, hot: bool) -> (f64, u64) {
    let mut rng = Rng::new(seed);
    let mut found = 0u64;
    let hot_keys = (args.keys / 10).max(1);
    let start = Instant::now();
    for _ in 0..args.read_ops {
        let i = if hot && rng.below(100) < 90 {
            rng.below(hot_keys)
        } else {
            rng.below(args.keys)
        };
        let key = if miss_ratio > 0 && rng.below(100) < miss_ratio {
            miss_key(i)
        } else {
            hit_key(i)
        };
        if db.get(&key).unwrap().is_some() {
            found += 1;
        }
    }
    (start.elapsed().as_secs_f64(), found)
}

fn main() {
    let args = parse_args();
    let dataset_bytes = args.keys * (args.value_size as u64 + 17);
    println!(
        "lsmkv benchmark: {} keys x {} B  (~{:.2} GB of user data)",
        args.keys,
        args.value_size,
        dataset_bytes as f64 / (1u64 << 30) as f64
    );
    println!(
        "directory {}  sync={}  batch={}  order={}",
        args.dir,
        if args.sync_mode == SyncMode::EveryWrite {
            "fsync-every-write"
        } else {
            "os"
        },
        args.batch,
        if args.random_order {
            "random"
        } else {
            "sequential"
        }
    );
    let _ = std::fs::remove_dir_all(&args.dir);

    let opts = Options {
        sync_mode: args.sync_mode,
        ..Default::default()
    };
    let db = Db::open(&args.dir, opts).unwrap();

    println!(
        "\n[load] {} writes",
        if args.random_order {
            "random-order"
        } else {
            "sequential"
        }
    );
    let load_secs = load(&db, &args);
    println!("  write throughput : {}", rate(args.keys, load_secs));

    println!("\n[compact] draining level 0");
    let t = Instant::now();
    db.flush().unwrap();
    db.compact_all().unwrap();
    println!("  compaction took  : {:.2}s", t.elapsed().as_secs_f64());

    println!("\n[read] random point lookups");
    let (hit_secs, found) = random_reads(&db, &args, 11, 0, false);
    println!(
        "  uniform, all hit : {} ({found} found)",
        rate(args.read_ops, hit_secs)
    );
    let (miss_secs, _) = random_reads(&db, &args, 12, 100, false);
    println!("  uniform, all miss: {}", rate(args.read_ops, miss_secs));
    let (mixed_secs, _) = random_reads(&db, &args, 13, 50, false);
    println!("  uniform, 50% miss: {}", rate(args.read_ops, mixed_secs));
    let (hot_secs, _) = random_reads(&db, &args, 14, 20, true);
    println!("  hot set, 20% miss: {}", rate(args.read_ops, hot_secs));

    println!("\n{}", db.stats());
    drop(db);

    if args.compare {
        // Same data, same reads, filters and cache switched off. The gap is
        // what those two structures buy on this workload.
        println!("[compare] Bloom filter + block cache off");
        let bare_dir = format!("{}-bare", args.dir);
        let _ = std::fs::remove_dir_all(&bare_dir);
        let bare = Db::open(
            &bare_dir,
            Options {
                sync_mode: args.sync_mode,
                bloom_bits_per_key: 0,
                block_cache_size: 64 << 10,
                ..Default::default()
            },
        )
        .unwrap();
        load(&bare, &args);
        bare.flush().unwrap();
        bare.compact_all().unwrap();

        let (bare_hit, _) = random_reads(&bare, &args, 11, 0, false);
        let (bare_miss, _) = random_reads(&bare, &args, 12, 100, false);
        let (bare_mixed, _) = random_reads(&bare, &args, 13, 50, false);
        let (bare_hot, _) = random_reads(&bare, &args, 14, 20, true);
        println!("  uniform, all hit : {}", rate(args.read_ops, bare_hit));
        println!("  uniform, all miss: {}", rate(args.read_ops, bare_miss));
        println!("  uniform, 50% miss: {}", rate(args.read_ops, bare_mixed));
        println!("  hot set, 20% miss: {}", rate(args.read_ops, bare_hot));

        // Negative = faster with the filters and cache on.
        let change = |with: f64, without: f64| (with / without - 1.0) * 100.0;
        println!("\n[summary] change in mean read latency, filters+cache on vs off");
        println!("  uniform, all hit : {:+.1}%", change(hit_secs, bare_hit));
        println!("  uniform, all miss: {:+.1}%", change(miss_secs, bare_miss));
        println!(
            "  uniform, 50% miss: {:+.1}%",
            change(mixed_secs, bare_mixed)
        );
        println!("  hot set, 20% miss: {:+.1}%", change(hot_secs, bare_hot));
        drop(bare);
        if !args.keep {
            let _ = std::fs::remove_dir_all(&bare_dir);
        }
    }

    if !args.keep {
        let _ = std::fs::remove_dir_all(&args.dir);
    } else {
        println!("\ndatabase left in {}", args.dir);
    }
}
