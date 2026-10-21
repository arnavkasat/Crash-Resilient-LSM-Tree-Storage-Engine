//! Crash tests.
//!
//! A child process (`lsmctl crash-writer`) writes keys with `SyncMode::EveryWrite`
//! and prints `ACK n` only *after* the durable `put` has returned. The parent
//! kills it with `SIGKILL` at an arbitrary moment - no unwinding, no
//! destructors, no flush - then reopens the database and demands that every
//! acknowledged write is still there, with the right value.
//!
//! That is the whole contract: **an acknowledged write is never lost.** Writes
//! still in flight when the process died may be, and that is allowed.
//!
//! `cargo test --test crash` runs a short sweep. The full 1,000-cycle run is
//! `cargo test --release --test crash -- --ignored --nocapture`, or set
//! `LSMKV_CRASH_ITERS` to pick a count.

mod common;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::TempDir;
use lsmkv::testkit::{key_for, value_for, Rng};
use lsmkv::Db;

const VALUE_SIZE: usize = 100;
const WRITE_BUFFER: usize = 64 << 10;
const KEYS_PER_RUN: u64 = 200_000;

fn iterations(default: usize) -> usize {
    std::env::var("LSMKV_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn spawn_writer(dir: &Path, first_key: u64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_lsmctl"))
        .arg("crash-writer")
        .arg(dir)
        .arg((first_key + KEYS_PER_RUN).to_string())
        .arg(VALUE_SIZE.to_string())
        .arg(WRITE_BUFFER.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start lsmctl; run `cargo build` first")
}

/// Runs a writer, kills it after `lifetime`, and returns the highest key the
/// engine acknowledged before dying.
fn run_until_killed(dir: &Path, lifetime: Duration) -> i64 {
    let mut child = spawn_writer(dir, 0);
    let stdout = child.stdout.take().unwrap();
    let highest = Arc::new(AtomicI64::new(-1));

    let reader = {
        let highest = highest.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout)
                .lines()
                .map_while(std::result::Result::ok)
            {
                if let Some(n) = line.strip_prefix("ACK ") {
                    if let Ok(n) = n.parse::<i64>() {
                        highest.store(n, Ordering::SeqCst);
                    }
                }
            }
        })
    };

    std::thread::sleep(lifetime);
    // SIGKILL: the process cannot flush, close files, or run any cleanup.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();
    highest.load(Ordering::SeqCst)
}

/// Reopens the database and checks every acknowledged write.
fn verify(dir: &Path, highest: i64) {
    let db = Db::open_default(dir).expect("database did not reopen after the crash");
    for i in 0..=highest.max(-1) {
        if i < 0 {
            break;
        }
        let i = i as u64;
        match db.get(&key_for(i)).expect("read failed after recovery") {
            Some(v) => assert_eq!(
                v,
                value_for(i, VALUE_SIZE),
                "key {i} was acknowledged but came back with the wrong value"
            ),
            None => panic!("key {i} was acknowledged before the crash but is gone after it"),
        }
    }
}

#[test]
fn an_acknowledged_write_survives_sigkill() {
    let dir = TempDir::new("crash-once");
    let highest = run_until_killed(dir.path(), Duration::from_millis(400));
    assert!(highest >= 0, "the writer never acknowledged anything");
    verify(dir.path(), highest);
}

#[test]
fn crash_sweep_at_many_different_moments() {
    let n = iterations(20);
    let mut rng = Rng::new(0xC0FFEE);
    let mut total_verified: i64 = 0;

    for iter in 0..n {
        let dir = TempDir::new(&format!("crash-sweep-{iter}"));
        // Spread the kills across start-up, steady state, and the window where
        // flushes and compactions are running.
        let lifetime = Duration::from_millis(40 + rng.below(360));
        let highest = run_until_killed(dir.path(), lifetime);
        verify(dir.path(), highest);
        total_verified += highest + 1;
    }
    println!("{n} crash cycles, {total_verified} acknowledged writes verified");
}

#[test]
fn a_database_survives_being_killed_over_and_over() {
    // Every cycle reopens the same directory, so the test also covers
    // recovering from state a *previous* crash left behind: orphaned SSTables,
    // half-filled logs, and manifests from any point in the sequence.
    let dir = TempDir::new("crash-repeat");
    let mut rng = Rng::new(7);
    let mut high_water: i64 = -1;

    for cycle in 0..iterations(8) {
        let lifetime = Duration::from_millis(60 + rng.below(240));
        let highest = run_until_killed(dir.path(), lifetime);
        verify(dir.path(), highest);
        // Each cycle restarts from key 0, so durable progress must not go
        // backwards across crashes.
        if highest > high_water {
            high_water = highest;
        }
        // Everything verified in an earlier cycle must still be readable now.
        let db = Db::open_default(dir.path()).unwrap();
        for i in 0..=high_water.min(highest).max(-1) {
            if i < 0 {
                break;
            }
            assert!(
                db.get(&key_for(i as u64)).unwrap().is_some(),
                "cycle {cycle}: key {i} disappeared after a later crash"
            );
        }
    }
}

#[test]
#[ignore = "long: run with --release for the full 1,000-cycle sweep"]
fn thousand_crash_cycles() {
    let n = iterations(1_000);
    let mut rng = Rng::new(0x5EED);
    let mut verified: i64 = 0;
    let start = std::time::Instant::now();

    for iter in 0..n {
        let dir = TempDir::new(&format!("crash-1k-{iter}"));
        let lifetime = Duration::from_millis(30 + rng.below(270));
        let highest = run_until_killed(dir.path(), lifetime);
        verify(dir.path(), highest);
        verified += highest + 1;
        if iter % 50 == 0 {
            println!("cycle {iter}/{n}: {verified} acknowledged writes verified so far");
        }
    }
    println!(
        "{n} crash cycles in {:.1}s, {verified} acknowledged writes verified, 0 lost",
        start.elapsed().as_secs_f64()
    );
}
