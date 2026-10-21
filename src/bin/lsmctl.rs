//! `lsmctl` - a small command-line front end for the engine.
//!
//! It doubles as the driver for the crash tests: `crash-writer` writes keys in
//! a loop and prints `ACK <n>` only after a durable `put` has returned, so the
//! parent process knows exactly which writes the engine has promised to keep
//! when it kills the child.

use std::io::{BufWriter, Write};
use std::ops::Bound;
use std::process::ExitCode;

use lsmkv::testkit::{key_for, value_for};
use lsmkv::{Db, Options, SyncMode};

fn usage() -> ExitCode {
    eprintln!(
        "usage:
  lsmctl put <dir> <key> <value>
  lsmctl get <dir> <key>
  lsmctl delete <dir> <key>
  lsmctl scan <dir> [start] [end]
  lsmctl stats <dir>
  lsmctl compact <dir>
  lsmctl crash-writer <dir> <count> <value_size> [write_buffer_bytes]
  lsmctl verify <dir> <highest_acked> <value_size>"
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        return usage();
    }
    match run(&args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("lsmctl: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> lsmkv::Result<ExitCode> {
    let cmd = args[0].as_str();
    let dir = args.get(1).cloned().unwrap_or_default();
    if dir.is_empty() {
        return Ok(usage());
    }

    match cmd {
        "put" => {
            let db = Db::open_default(&dir)?;
            db.put(args[2].clone(), args[3].clone())?;
        }
        "get" => {
            let db = Db::open_default(&dir)?;
            match db.get(args[2].as_bytes())? {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => {
                    eprintln!("(not found)");
                    return Ok(ExitCode::FAILURE);
                }
            }
        }
        "delete" => {
            let db = Db::open_default(&dir)?;
            db.delete(args[2].clone())?;
        }
        "scan" => {
            let db = Db::open_default(&dir)?;
            let start = args.get(2).map(|s| s.as_bytes().to_vec());
            let end = args.get(3).map(|s| s.as_bytes().to_vec());
            let entries = db.scan(
                start.as_deref().map_or(Bound::Unbounded, Bound::Included),
                end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            )?;
            let stdout = std::io::stdout();
            let mut out = BufWriter::new(stdout.lock());
            for (k, v) in entries {
                let _ = writeln!(
                    out,
                    "{} = {}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
        }
        "stats" => {
            let db = Db::open_default(&dir)?;
            print!("{}", db.stats());
        }
        "compact" => {
            let db = Db::open_default(&dir)?;
            db.flush()?;
            db.compact_all()?;
            print!("{}", db.stats());
        }
        "crash-writer" => return crash_writer(args),
        "verify" => return verify(args),
        _ => return Ok(usage()),
    }
    Ok(ExitCode::SUCCESS)
}

/// Writes keys forever (up to `count`), acknowledging each durable write on
/// stdout. Killed at an arbitrary moment by the crash tests.
fn crash_writer(args: &[String]) -> lsmkv::Result<ExitCode> {
    let dir = &args[1];
    let count: u64 = args[2].parse().unwrap_or(10_000);
    let value_size: usize = args[3].parse().unwrap_or(100);
    let write_buffer: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(64 << 10);

    let opts = Options {
        sync_mode: SyncMode::EveryWrite,
        write_buffer_size: write_buffer,
        target_file_size: (write_buffer * 2) as u64,
        level_base_size: (write_buffer * 8) as u64,
        l0_compaction_trigger: 2,
        ..Default::default()
    };
    let db = Db::open(dir, opts)?;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for i in 0..count {
        db.put(key_for(i), value_for(i, value_size))?;
        // Unbuffered and flushed: the parent must never see an ACK for a write
        // that has not already returned from a synchronous put.
        writeln!(out, "ACK {i}").ok();
        out.flush().ok();
    }
    Ok(ExitCode::SUCCESS)
}

/// Checks that every acknowledged write survived.
fn verify(args: &[String]) -> lsmkv::Result<ExitCode> {
    let dir = &args[1];
    let highest: i64 = args[2].parse().unwrap_or(-1);
    let value_size: usize = args[3].parse().unwrap_or(100);
    let db = Db::open_default(dir)?;
    for i in 0..=highest.max(-1) {
        if i < 0 {
            continue;
        }
        let i = i as u64;
        match db.get(&key_for(i))? {
            Some(v) if v == value_for(i, value_size) => {}
            Some(_) => {
                eprintln!("key {i} came back with the wrong value");
                return Ok(ExitCode::FAILURE);
            }
            None => {
                eprintln!("acknowledged key {i} was lost");
                return Ok(ExitCode::FAILURE);
            }
        }
    }
    println!("verified {} acknowledged writes", highest + 1);
    Ok(ExitCode::SUCCESS)
}
