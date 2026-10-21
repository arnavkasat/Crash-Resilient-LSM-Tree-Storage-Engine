//! End-to-end behaviour of the storage engine.

mod common;

use std::ops::Bound;

use common::{deterministic_options, small_options, TempDir};
use lsmkv::testkit::{key_for, value_for, Rng};
use lsmkv::{Db, Options, WriteBatch};

#[test]
fn put_get_delete_roundtrip() {
    let dir = TempDir::new("basic");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    assert_eq!(db.get(b"missing").unwrap(), None);
    db.put("a", "1").unwrap();
    db.put("b", "2").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));

    db.put("a", "overwritten").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"overwritten".to_vec()));

    db.delete("a").unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn values_survive_flush_and_compaction() {
    let dir = TempDir::new("flush");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    const N: u64 = 5_000;
    for i in 0..N {
        db.put(key_for(i), value_for(i, 100)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();

    let stats = db.stats();
    assert!(stats.flushes > 0, "nothing was flushed: {stats}");
    assert!(
        stats.files_per_level.iter().skip(1).sum::<usize>() > 0,
        "compaction never moved anything below level 0: {stats}"
    );

    for i in 0..N {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 100)),
            "key {i}"
        );
    }
    assert_eq!(db.get(&key_for(N + 1)).unwrap(), None);
}

#[test]
fn deletes_survive_compaction() {
    let dir = TempDir::new("delete");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    const N: u64 = 3_000;
    for i in 0..N {
        db.put(key_for(i), value_for(i, 80)).unwrap();
    }
    for i in (0..N).step_by(3) {
        db.delete(key_for(i)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();

    for i in 0..N {
        let got = db.get(&key_for(i)).unwrap();
        if i % 3 == 0 {
            assert_eq!(got, None, "key {i} should have stayed deleted");
        } else {
            assert_eq!(got, Some(value_for(i, 80)), "key {i}");
        }
    }
}

#[test]
fn a_resurrected_key_beats_its_tombstone() {
    let dir = TempDir::new("resurrect");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    db.put("k", "first").unwrap();
    db.flush().unwrap();
    db.delete("k").unwrap();
    db.flush().unwrap();
    db.put("k", "second").unwrap();
    db.flush().unwrap();
    db.compact_all().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn write_batches_apply_atomically() {
    let dir = TempDir::new("batch");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    let mut batch = WriteBatch::new();
    batch.put("x", "1").put("y", "2").delete("z");
    db.write(&batch).unwrap();

    assert_eq!(db.get(b"x").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"y").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"z").unwrap(), None);
    db.write(&WriteBatch::new()).unwrap();
}

#[test]
fn scan_merges_every_level_in_order() {
    let dir = TempDir::new("scan");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    const N: u64 = 2_000;
    for i in 0..N {
        db.put(key_for(i), value_for(i, 60)).unwrap();
    }
    db.flush().unwrap();
    // Overwrite a slice of the range so the newest values live in the MemTable
    // while the originals are still on disk.
    for i in 500..600 {
        db.put(key_for(i), b"updated".to_vec()).unwrap();
    }
    for i in 700..800 {
        db.delete(key_for(i)).unwrap();
    }

    let all = db.scan(Bound::Unbounded, Bound::Unbounded).unwrap();
    assert_eq!(all.len() as u64, N - 100, "deleted keys must not appear");
    assert!(
        all.windows(2).all(|w| w[0].0 < w[1].0),
        "scan output is not sorted"
    );
    assert_eq!(all[550].1, b"updated".to_vec());

    let window = db
        .scan(
            Bound::Included(&key_for(100)),
            Bound::Excluded(&key_for(200)),
        )
        .unwrap();
    assert_eq!(window.len(), 100);
    assert_eq!(window[0].0, key_for(100));
    assert_eq!(window[99].0, key_for(199));

    let empty = db.scan(Bound::Included(b"zzz"), Bound::Unbounded).unwrap();
    assert!(empty.is_empty());
}

#[test]
fn data_survives_reopen() {
    let dir = TempDir::new("reopen");
    const N: u64 = 4_000;
    {
        let db = Db::open(dir.path(), deterministic_options()).unwrap();
        for i in 0..N {
            db.put(key_for(i), value_for(i, 90)).unwrap();
        }
        for i in (0..N).step_by(7) {
            db.delete(key_for(i)).unwrap();
        }
    }
    let db = Db::open(dir.path(), deterministic_options()).unwrap();
    for i in 0..N {
        let got = db.get(&key_for(i)).unwrap();
        if i % 7 == 0 {
            assert_eq!(got, None, "deleted key {i} came back");
        } else {
            assert_eq!(got, Some(value_for(i, 90)), "key {i}");
        }
    }
}

#[test]
fn background_compaction_keeps_the_tree_in_shape() {
    let dir = TempDir::new("background");
    let db = Db::open(dir.path(), small_options()).unwrap();

    const N: u64 = 20_000;
    for i in 0..N {
        db.put(key_for(i), value_for(i, 100)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();

    let stats = db.stats();
    assert!(stats.compactions > 0, "no compaction ran: {stats}");
    assert!(
        stats.files_per_level[0] <= small_options().l0_stop_writes_trigger,
        "level 0 was never drained: {stats}"
    );
    for i in (0..N).step_by(97) {
        assert_eq!(
            db.get(&key_for(i)).unwrap(),
            Some(value_for(i, 100)),
            "key {i}"
        );
    }
}

#[test]
fn concurrent_readers_and_writers_agree() {
    use std::sync::Arc;

    let dir = TempDir::new("concurrent");
    let db = Arc::new(Db::open(dir.path(), small_options()).unwrap());

    const N: u64 = 8_000;
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            for i in 0..N {
                db.put(key_for(i), value_for(i, 80)).unwrap();
            }
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut rng = Rng::new(t + 1);
                for _ in 0..5_000 {
                    let i = rng.below(N);
                    // A key is either absent (not written yet) or exactly right -
                    // never a torn or stale value.
                    if let Some(v) = db.get(&key_for(i)).unwrap() {
                        assert_eq!(v, value_for(i, 80), "key {i} read back wrong");
                    }
                }
            })
        })
        .collect();

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    for i in (0..N).step_by(31) {
        assert_eq!(db.get(&key_for(i)).unwrap(), Some(value_for(i, 80)));
    }
}

#[test]
fn large_values_and_binary_keys() {
    let dir = TempDir::new("binary");
    let db = Db::open(dir.path(), deterministic_options()).unwrap();

    let big = vec![0xABu8; 300_000];
    db.put(vec![0u8, 255, 7], big.clone()).unwrap();
    db.put(vec![255u8, 0], b"edge".to_vec()).unwrap();
    db.put(Vec::new(), b"empty key".to_vec()).unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(&[0u8, 255, 7]).unwrap(), Some(big));
    assert_eq!(db.get(&[255u8, 0]).unwrap(), Some(b"edge".to_vec()));
    assert_eq!(db.get(b"").unwrap(), Some(b"empty key".to_vec()));
}

#[test]
fn refuses_to_create_when_told_not_to() {
    let dir = TempDir::new("nocreate");
    let missing = dir.path().join("nope");
    let opts = Options {
        create_if_missing: false,
        ..Default::default()
    };
    assert!(Db::open(&missing, opts).is_err());
}
