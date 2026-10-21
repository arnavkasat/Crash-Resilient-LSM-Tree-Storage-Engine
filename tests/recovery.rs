//! Recovery behaviour: what the engine does with the on-disk state a crash
//! can leave behind.

mod common;

use std::fs;
use std::path::Path;

use common::{deterministic_options, TempDir};
use lsmkv::testkit::{key_for, value_for};
use lsmkv::{Db, Options, SyncMode};

fn wal_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    v.sort();
    v
}

fn sst_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "sst"))
        .collect();
    v.sort();
    v
}

#[test]
fn unflushed_writes_are_replayed_from_the_log() {
    let dir = TempDir::new("replay");
    let opts = Options {
        write_buffer_size: 1 << 30,
        ..deterministic_options()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..500 {
            db.put(key_for(i), value_for(i, 50)).unwrap();
        }
        // Deliberately no flush: everything lives in the MemTable and the log.
        assert!(
            sst_files(dir.path()).is_empty(),
            "nothing should have been flushed yet"
        );
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.stats().recovered_batches, 500);
    for i in 0..500 {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 50)),
            "key {i}"
        );
    }
}

#[test]
fn a_torn_final_record_loses_only_the_write_in_flight() {
    let dir = TempDir::new("torn");
    let opts = Options {
        write_buffer_size: 1 << 30,
        ..deterministic_options()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..200 {
            db.put(key_for(i), value_for(i, 50)).unwrap();
        }
    }

    // Chop the tail of the live log, the way a power loss mid-append would.
    let log = wal_files(dir.path()).pop().unwrap();
    let len = fs::metadata(&log).unwrap().len();
    let f = fs::OpenOptions::new().write(true).open(&log).unwrap();
    f.set_len(len - 12).unwrap();
    drop(f);

    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..199 {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 50)),
            "key {i} was lost"
        );
    }
    // The database must still be usable afterwards.
    db.put("after", "recovery").unwrap();
    assert_eq!(db.get(b"after").unwrap(), Some(b"recovery".to_vec()));
}

#[test]
fn an_sstable_left_behind_by_an_interrupted_flush_is_discarded() {
    let dir = TempDir::new("orphan");
    let opts = deterministic_options();
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        db.put("a", "1").unwrap();
        db.flush().unwrap();
    }

    // A crash between writing an SSTable and committing the manifest leaves a
    // file no version refers to. Recovery must delete it, not trust it.
    let orphan = dir.path().join("0000009999.sst");
    fs::write(&orphan, b"not even a valid table").unwrap();

    let db = Db::open(dir.path(), opts).unwrap();
    assert!(!orphan.exists(), "orphaned SSTable was not cleaned up");
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn an_interrupted_manifest_write_leaves_the_old_version_intact() {
    let dir = TempDir::new("manifest-tmp");
    let opts = deterministic_options();
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..100 {
            db.put(key_for(i), value_for(i, 40)).unwrap();
        }
        db.flush().unwrap();
    }

    // A half-written MANIFEST.tmp is what a crash during a manifest commit
    // leaves. Because the real manifest is swapped in by rename, the partial
    // file is simply ignored.
    fs::write(dir.path().join("MANIFEST.tmp"), b"LSMKV1\nlog 3\nnext_f").unwrap();

    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..100 {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 40)),
            "key {i}"
        );
    }
}

#[test]
fn a_damaged_manifest_is_reported_rather_than_silently_ignored() {
    let dir = TempDir::new("bad-manifest");
    {
        let db = Db::open(dir.path(), deterministic_options()).unwrap();
        db.put("a", "1").unwrap();
        db.flush().unwrap();
    }
    let path = dir.path().join("MANIFEST");
    let mut text = fs::read_to_string(&path).unwrap();
    text.push_str("file 0 424242 10 61 62\n");
    fs::write(&path, text).unwrap();

    match Db::open(dir.path(), deterministic_options()) {
        Err(lsmkv::Error::Corruption(_)) => {}
        Err(other) => panic!("expected a corruption error, got {other}"),
        Ok(_) => panic!("a damaged manifest was accepted"),
    }
}

#[test]
fn logs_are_dropped_once_their_data_is_in_an_sstable() {
    let dir = TempDir::new("log-gc");
    let opts = deterministic_options();
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..2_000 {
        db.put(key_for(i), value_for(i, 100)).unwrap();
    }
    db.flush().unwrap();
    // One live log: the one the current MemTable is writing to.
    assert_eq!(
        wal_files(dir.path()).len(),
        1,
        "stale logs were not reclaimed"
    );
    assert!(!sst_files(dir.path()).is_empty());
}

#[test]
fn repeated_reopens_are_stable() {
    let dir = TempDir::new("reopen-loop");
    let opts = deterministic_options();
    for round in 0..10u64 {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..200 {
            let k = round * 200 + i;
            db.put(key_for(k), value_for(k, 60)).unwrap();
        }
        for k in 0..(round + 1) * 200 {
            assert_eq!(
                db.get(&key_for(k)).unwrap(),
                Some(value_for(k, 60)),
                "round {round} key {k}"
            );
        }
    }
    let db = Db::open(dir.path(), deterministic_options()).unwrap();
    for k in 0..2_000 {
        assert_eq!(db.get(&key_for(k)).unwrap(), Some(value_for(k, 60)));
    }
}

#[test]
fn os_sync_mode_still_survives_a_clean_process_exit() {
    // With SyncMode::Os the bytes are in the page cache rather than on the
    // platter: a power cut can lose them, but a process exit cannot.
    let dir = TempDir::new("os-sync");
    let opts = Options {
        sync_mode: SyncMode::Os,
        ..deterministic_options()
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        for i in 0..1_000 {
            db.put(key_for(i), value_for(i, 50)).unwrap();
        }
    }
    let db = Db::open(dir.path(), opts).unwrap();
    for i in 0..1_000 {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 50)),
            "key {i}"
        );
    }
}
