//! The engine: wiring the MemTable, the write-ahead log, the SSTable levels,
//! the manifest and the compactor into one key-value store.
//!
//! # Durability contract
//!
//! In [`SyncMode::EveryWrite`], a `put` or `delete` that returns `Ok` has been
//! `fsync`ed to the write-ahead log. If the machine loses power one instruction
//! later, reopening the database will return that write. Nothing that was
//! acknowledged is ever lost; a write that was still in flight may be, which is
//! exactly the guarantee the crash tests check.
//!
//! # Lock discipline
//!
//! One mutex guards the mutable state (MemTable, log, current version). Every
//! piece of real I/O - building an SSTable, merging during compaction, reading
//! a block - happens *outside* that mutex, against an immutable snapshot taken
//! while holding it.

use std::collections::HashSet;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;

use crate::cache::BlockCache;
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::merge::{KvSource, MergingIter, VecSource};
use crate::options::Options;
use crate::sst::{Table, TableBuilder};
use crate::table_cache::TableCache;
use crate::version::{
    load_manifest, pick_level_for_flush, save_manifest, sync_dir, table_path, wal_path, FileMeta,
    Version, NUM_LEVELS,
};
use crate::wal::{self, WalWriter, WriteBatch};

/// Counters, mostly for the benchmarks and for `Db::stats()`.
#[derive(Default, Debug)]
pub struct Stats {
    pub writes: AtomicU64,
    pub reads: AtomicU64,
    pub flushes: AtomicU64,
    pub compactions: AtomicU64,
    /// Compactions satisfied by relabelling a file instead of rewriting it.
    pub trivial_moves: AtomicU64,
    pub bytes_compacted: AtomicU64,
    pub write_stalls: AtomicU64,
    pub recovered_batches: AtomicU64,
}

/// A human-readable snapshot of engine state.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub writes: u64,
    pub reads: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub trivial_moves: u64,
    pub bytes_compacted: u64,
    pub write_stalls: u64,
    pub recovered_batches: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub files_per_level: Vec<usize>,
    pub bytes_per_level: Vec<u64>,
    pub memtable_bytes: usize,
}

impl std::fmt::Display for StatsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "writes={} reads={}", self.writes, self.reads)?;
        writeln!(
            f,
            "flushes={} compactions={} (of which {} trivial moves) bytes_compacted={} stalls={}",
            self.flushes,
            self.compactions,
            self.trivial_moves,
            self.bytes_compacted,
            self.write_stalls
        )?;
        let total = self.cache_hits + self.cache_misses;
        let rate = if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 * 100.0 / total as f64
        };
        writeln!(
            f,
            "block cache: {} hits / {} lookups ({rate:.1}%)",
            self.cache_hits, total
        )?;
        writeln!(f, "memtable: {} bytes", self.memtable_bytes)?;
        for (level, (files, bytes)) in self
            .files_per_level
            .iter()
            .zip(&self.bytes_per_level)
            .enumerate()
        {
            if *files > 0 {
                writeln!(f, "  L{level}: {files} files, {bytes} bytes")?;
            }
        }
        Ok(())
    }
}

struct Core {
    mem: MemTable,
    /// The MemTable that has been sealed and is waiting to become an SSTable.
    imm: Option<Arc<MemTable>>,
    /// Log holding `imm`'s writes; deletable once the flush is committed.
    imm_log_number: u64,
    version: Arc<Version>,
    wal: WalWriter,
    log_number: u64,
    next_file_number: u64,
    seq: u64,
    /// Where the last compaction of each level stopped, so compaction works
    /// its way across the key space instead of hammering one file.
    compact_pointer: Vec<Vec<u8>>,
    bg_working: bool,
    bg_error: Option<String>,
}

impl Core {
    fn allocate_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }
}

struct DbInner {
    dir: PathBuf,
    opts: Options,
    core: Mutex<Core>,
    cv: Condvar,
    shutdown: AtomicBool,
    table_cache: Arc<TableCache>,
    stats: Stats,
}

/// A crash-resilient LSM-tree key-value store.
pub struct Db {
    inner: Arc<DbInner>,
    bg: Option<JoinHandle<()>>,
}

impl Db {
    /// Opens (or creates) a database, replaying the write-ahead log.
    pub fn open(path: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = path.as_ref().to_path_buf();
        if !dir.exists() {
            if !opts.create_if_missing {
                return Err(Error::InvalidArgument(format!(
                    "{} does not exist and create_if_missing is false",
                    dir.display()
                )));
            }
            fs::create_dir_all(&dir)?;
        }

        let block_cache = Arc::new(BlockCache::new(opts.block_cache_size));
        let table_cache = Arc::new(TableCache::new(&dir, block_cache, opts.max_open_tables));

        let version = load_manifest(&dir)?.unwrap_or_else(Version::new);
        let (sst_files, wal_files) = scan_dir(&dir)?;
        let mut next_file_number = version.next_file_number;
        for n in sst_files.iter().chain(wal_files.iter()) {
            next_file_number = next_file_number.max(*n + 1);
        }

        // --- replay ------------------------------------------------------
        // Logs numbered below the manifest's log_number were flushed into
        // SSTables before the manifest was committed, so they add nothing.
        let mut mem = MemTable::new();
        let mut seq = 0u64;
        let mut recovered = 0u64;
        let mut replayed: Vec<u64> = wal_files
            .iter()
            .copied()
            .filter(|n| *n >= version.log_number)
            .collect();
        replayed.sort_unstable();
        for number in &replayed {
            let rec = wal::replay(&wal_path(&dir, *number))?;
            for batch in rec.batches {
                seq = seq.max(batch.seq);
                recovered += 1;
                for (key, value) in batch.entries {
                    mem.insert(key, value);
                }
            }
            // A torn final record is the normal shape of a crash, not an error.
        }

        // --- rebuild a clean starting state -------------------------------
        let mut version = version;
        if !mem.is_empty() {
            let number = next_file_number;
            next_file_number += 1;
            let meta = build_table_from_memtable(&dir, number, &mem, &opts)?;
            let level = pick_level_for_flush(&version, &opts, &meta.smallest, &meta.largest);
            version = version.apply(
                &[],
                vec![(level, Arc::new(meta))],
                version.log_number,
                next_file_number,
            );
        }
        let log_number = next_file_number;
        next_file_number += 1;
        let wal = WalWriter::create(&wal_path(&dir, log_number), opts.sync_mode)?;
        version.log_number = log_number;
        version.next_file_number = next_file_number;
        save_manifest(&dir, &version)?;

        // Only now, with the new manifest durable, is it safe to drop the old
        // logs and any SSTable left behind by a crash mid-flush.
        let live: HashSet<u64> = version.live_files().into_iter().collect();
        for n in sst_files {
            if !live.contains(&n) {
                let _ = fs::remove_file(table_path(&dir, n));
            }
        }
        for n in wal_files {
            if n != log_number {
                let _ = fs::remove_file(wal_path(&dir, n));
            }
        }
        sync_dir(&dir)?;

        let core = Core {
            mem: MemTable::new(),
            imm: None,
            imm_log_number: 0,
            version: Arc::new(version),
            wal,
            log_number,
            next_file_number,
            seq,
            compact_pointer: vec![Vec::new(); NUM_LEVELS],
            bg_working: false,
            bg_error: None,
        };
        let stats = Stats::default();
        stats.recovered_batches.store(recovered, Ordering::Relaxed);

        let inner = Arc::new(DbInner {
            dir,
            opts,
            core: Mutex::new(core),
            cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            table_cache,
            stats,
        });

        let bg = if inner.opts.background_compaction {
            let worker = inner.clone();
            Some(
                std::thread::Builder::new()
                    .name("lsmkv-compactor".into())
                    .spawn(move || {
                        worker.background_loop();
                    })?,
            )
        } else {
            None
        };

        let db = Db { inner, bg };
        db.inner.maybe_compact_now()?;
        Ok(db)
    }

    /// Opens with the default options.
    pub fn open_default(path: impl AsRef<Path>) -> Result<Db> {
        Db::open(path, Options::default())
    }

    /// Stores `value` under `key`, durably in [`SyncMode::EveryWrite`].
    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(&batch)
    }

    /// Removes `key`, leaving a tombstone so older SSTables stop answering for it.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(&batch)
    }

    /// Applies a batch atomically: after a crash it is either entirely present
    /// or entirely absent, because it occupies a single log record.
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.inner.write(batch)
    }

    /// Looks up `key`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    /// Collects every live entry in `[start, end)`, merged across the MemTable
    /// and all levels.
    pub fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan(start, end)
    }

    /// Seals the current MemTable and waits for it to reach level 0.
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    /// Forces the log to stable storage. Only meaningful in [`SyncMode::Os`].
    pub fn sync(&self) -> Result<()> {
        let mut core = self.inner.core.lock().unwrap();
        core.wal.sync()
    }

    /// Runs compaction until no level is over budget.
    pub fn compact_all(&self) -> Result<()> {
        loop {
            if !self.inner.do_one_unit_of_work()? {
                return Ok(());
            }
        }
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.inner.stats_snapshot()
    }

    /// Number of entries across all SSTables, including tombstones not yet
    /// compacted away. Intended for tests and diagnostics.
    pub fn count_sst_entries(&self) -> Result<u64> {
        let version = self.inner.core.lock().unwrap().version.clone();
        let mut total = 0;
        for level in &version.levels {
            for f in level {
                total += self.inner.table_cache.get(f.number)?.num_entries();
            }
        }
        Ok(total)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.cv.notify_all();
        if let Some(handle) = self.bg.take() {
            let _ = handle.join();
        }
        if let Ok(mut core) = self.inner.core.lock() {
            let _ = core.wal.sync();
        }
    }
}

impl DbInner {
    // ---------------------------------------------------------------- writes

    fn write(&self, batch: &WriteBatch) -> Result<()> {
        let mut core = self.make_room_for_write(self.core.lock().unwrap())?;

        core.seq += batch.len() as u64;
        let seq = core.seq;
        // The log append (and its fsync) happens before the MemTable is
        // touched: if we crash in between, replay simply reapplies the batch.
        core.wal.append(batch, seq)?;
        for (key, value) in batch.clone().into_entries() {
            core.mem.insert(key, value);
        }
        self.stats
            .writes
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        drop(core);

        if !self.opts.background_compaction {
            self.maybe_compact_now()?;
        }
        Ok(())
    }

    /// Makes sure there is space in the MemTable, sealing it and/or waiting for
    /// the compactor when there is not.
    ///
    /// Takes the guard by value and hands it back, because waiting on the
    /// condition variable has to give the lock up.
    fn make_room_for_write<'a>(
        &'a self,
        mut core: MutexGuard<'a, Core>,
    ) -> Result<MutexGuard<'a, Core>> {
        loop {
            if let Some(err) = &core.bg_error {
                return Err(Error::Background(err.clone()));
            }
            if core.mem.approximate_size() < self.opts.write_buffer_size {
                return Ok(core);
            }

            // Either a flush is already in flight and the slot is occupied, or
            // level 0 has piled up faster than the compactor can drain it. In
            // both cases the right answer is to slow the writer down rather
            // than let read amplification run away.
            let must_wait =
                core.imm.is_some() || core.version.num_files(0) >= self.opts.l0_stop_writes_trigger;
            if must_wait {
                self.stats.write_stalls.fetch_add(1, Ordering::Relaxed);
                self.cv.notify_all();
                if self.opts.background_compaction {
                    let (guard, _) = self.cv.wait_timeout(core, ms(20)).unwrap();
                    core = guard;
                } else {
                    drop(core);
                    self.do_one_unit_of_work()?;
                    core = self.core.lock().unwrap();
                }
                continue;
            }

            // Seal the MemTable and start a fresh log for the new one. The
            // sealed table stays readable until the flush is committed.
            let new_log = core.allocate_file_number();
            let wal = WalWriter::create(&wal_path(&self.dir, new_log), self.opts.sync_mode)?;
            let old_log = core.log_number;
            let sealed = std::mem::take(&mut core.mem);
            core.imm = Some(Arc::new(sealed));
            core.imm_log_number = old_log;
            core.wal = wal;
            core.log_number = new_log;
            self.cv.notify_all();
            return Ok(core);
        }
    }

    // ----------------------------------------------------------------- reads

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        // A compaction can unlink a file between taking the version snapshot
        // and opening it. That is rare and self-correcting: retry with a newer
        // version.
        for _ in 0..4 {
            let version = {
                let core = self.core.lock().unwrap();
                if let Some(v) = core.mem.get(key) {
                    return Ok(v);
                }
                if let Some(imm) = &core.imm {
                    if let Some(v) = imm.get(key) {
                        return Ok(v);
                    }
                }
                core.version.clone()
            };

            let mut retry = false;
            for meta in version.files_for_key(key) {
                let table = match self.table_cache.get(meta.number) {
                    Ok(t) => t,
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        retry = true;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                match table.get(key)? {
                    Some(value) => return Ok(value),
                    None => continue,
                }
            }
            if !retry {
                return Ok(None);
            }
        }
        Err(Error::Background(
            "lookup kept racing with compaction".into(),
        ))
    }

    fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (mem_entries, imm_entries, version) = {
            let core = self.core.lock().unwrap();
            let mem = core.mem.range(start, end);
            let imm = core
                .imm
                .as_ref()
                .map(|m| m.range(start, end))
                .unwrap_or_default();
            (mem, imm, core.version.clone())
        };

        // Sources in newest-to-oldest order; the merger resolves duplicates.
        let mut sources: Vec<Box<dyn KvSource + Send>> = Vec::new();
        sources.push(Box::new(VecSource::new(mem_entries)));
        sources.push(Box::new(VecSource::new(imm_entries)));

        let seek_key: Vec<u8> = match start {
            Bound::Included(k) | Bound::Excluded(k) => k.to_vec(),
            Bound::Unbounded => Vec::new(),
        };
        let mut l0: Vec<_> = version.levels[0].clone();
        l0.sort_by_key(|f| std::cmp::Reverse(f.number));
        for meta in l0 {
            let table = self.table_cache.get(meta.number)?;
            sources.push(Box::new(table.seek(&seek_key)?));
        }
        for level in 1..NUM_LEVELS {
            for meta in &version.levels[level] {
                if matches!(end, Bound::Included(e) | Bound::Excluded(e) if meta.smallest.as_slice() > e)
                {
                    continue;
                }
                if meta.largest.as_slice() < seek_key.as_slice() {
                    continue;
                }
                let table = self.table_cache.get(meta.number)?;
                sources.push(Box::new(table.seek(&seek_key)?));
            }
        }

        let mut iter = MergingIter::new(sources);
        let mut out = Vec::new();
        while let Some((key, value)) = iter.next_entry()? {
            match start {
                Bound::Excluded(s) if key.as_slice() <= s => continue,
                Bound::Included(s) if key.as_slice() < s => continue,
                _ => {}
            }
            match end {
                Bound::Included(e) if key.as_slice() > e => break,
                Bound::Excluded(e) if key.as_slice() >= e => break,
                _ => {}
            }
            // Tombstones are live entries internally but invisible to callers.
            if let Some(value) = value {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ background

    fn background_loop(&self) {
        loop {
            {
                let mut core = self.core.lock().unwrap();
                while !self.shutdown.load(Ordering::SeqCst)
                    && !self.work_pending(&core)
                    && core.bg_error.is_none()
                {
                    let (guard, _) = self.cv.wait_timeout(core, ms(200)).unwrap();
                    core = guard;
                }
                if self.shutdown.load(Ordering::SeqCst) || core.bg_error.is_some() {
                    return;
                }
            }
            match self.do_one_unit_of_work() {
                Ok(_) => {}
                Err(e) => {
                    let mut core = self.core.lock().unwrap();
                    // Stop writing rather than keep going on a tree we can no
                    // longer describe correctly.
                    core.bg_error = Some(e.to_string());
                    self.cv.notify_all();
                    return;
                }
            }
            self.cv.notify_all();
        }
    }

    fn work_pending(&self, core: &Core) -> bool {
        core.imm.is_some()
            || pick_compaction(&core.version, &self.opts, &core.compact_pointer).is_some()
    }

    fn maybe_compact_now(&self) -> Result<()> {
        if self.opts.background_compaction {
            self.cv.notify_all();
            return Ok(());
        }
        while self.do_one_unit_of_work()? {}
        Ok(())
    }

    /// Performs at most one flush or one compaction. Returns false when there
    /// was nothing to do.
    fn do_one_unit_of_work(&self) -> Result<bool> {
        let job = {
            let mut core = self.core.lock().unwrap();
            if core.bg_working {
                return Ok(false);
            }
            let job = if let Some(imm) = core.imm.clone() {
                Some(Job::Flush {
                    imm,
                    log_number: core.imm_log_number,
                })
            } else {
                pick_compaction(&core.version, &self.opts, &core.compact_pointer)
                    .map(|c| Job::Compact(Box::new(c)))
            };
            if job.is_some() {
                core.bg_working = true;
            }
            job
        };

        let Some(job) = job else { return Ok(false) };
        let result = match job {
            Job::Flush { imm, log_number } => self.run_flush(&imm, log_number),
            Job::Compact(c) => self.run_compaction(*c),
        };
        {
            let mut core = self.core.lock().unwrap();
            core.bg_working = false;
        }
        self.cv.notify_all();
        result.map(|_| true)
    }

    /// Turns the sealed MemTable into a level-0 (or deeper) SSTable.
    fn run_flush(&self, imm: &Arc<MemTable>, imm_log_number: u64) -> Result<()> {
        let (number, version) = {
            let mut core = self.core.lock().unwrap();
            (core.allocate_file_number(), core.version.clone())
        };

        // Built without the lock held: writers keep going into the new MemTable.
        let meta = build_table_from_memtable(&self.dir, number, imm, &self.opts)?;
        let level = pick_level_for_flush(&version, &self.opts, &meta.smallest, &meta.largest);

        let mut core = self.core.lock().unwrap();
        let next_file_number = core.next_file_number;
        let new_version = core.version.apply(
            &[],
            vec![(level, Arc::new(meta))],
            core.log_number,
            next_file_number,
        );
        // Manifest first: until it names the new file, recovery must keep
        // replaying the log that holds the same data.
        save_manifest(&self.dir, &new_version)?;
        core.version = Arc::new(new_version);
        core.imm = None;
        drop(core);

        let _ = fs::remove_file(wal_path(&self.dir, imm_log_number));
        sync_dir(&self.dir)?;
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        self.cv.notify_all();
        Ok(())
    }

    /// Merges one level into the next.
    fn run_compaction(&self, c: Compaction) -> Result<()> {
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);

        if c.is_trivial_move() {
            self.stats.trivial_moves.fetch_add(1, Ordering::Relaxed);
            // Nothing in the next level overlaps, so the file can simply be
            // relabelled - no bytes are rewritten.
            let meta = c.inputs[0][0].clone();
            let mut core = self.core.lock().unwrap();
            let next_file_number = core.next_file_number;
            let new_version = core.version.apply(
                &[(c.level, meta.number)],
                vec![(c.level + 1, meta.clone())],
                core.log_number,
                next_file_number,
            );
            save_manifest(&self.dir, &new_version)?;
            core.version = Arc::new(new_version);
            core.compact_pointer[c.level] = meta.largest.clone();
            return Ok(());
        }

        // Sources newest-first: level `n` shadows level `n+1`, and within
        // level 0 a higher file number is newer.
        let mut sources: Vec<Box<dyn KvSource + Send>> = Vec::new();
        let mut tables: Vec<Arc<Table>> = Vec::new();
        for meta in c.inputs[0].iter().chain(c.inputs[1].iter()) {
            tables.push(self.table_cache.get(meta.number)?);
        }
        for table in &tables {
            sources.push(Box::new(table.iter()));
        }

        // A tombstone may only be dropped once no deeper level can still hold
        // an older value for that key.
        let drop_tombstones = c.deeper_levels_empty;

        let mut iter = MergingIter::new(sources);
        let mut outputs: Vec<(u64, FileMeta)> = Vec::new();
        let mut builder: Option<(u64, TableBuilder)> = None;
        let mut bytes_in = 0u64;

        while let Some((key, value)) = iter.next_entry()? {
            if value.is_none() && drop_tombstones {
                continue;
            }
            if builder.is_none() {
                let number = self.core.lock().unwrap().allocate_file_number();
                let b = TableBuilder::create(&table_path(&self.dir, number), &self.opts)?;
                builder = Some((number, b));
            }
            let (_, b) = builder.as_mut().unwrap();
            bytes_in += key.len() as u64 + value.as_ref().map_or(0, |v| v.len()) as u64;
            b.add(&key, value.as_deref())?;

            if b.file_size() >= self.opts.target_file_size {
                let (number, b) = builder.take().unwrap();
                let (size, smallest, largest) = b.finish()?;
                outputs.push((
                    number,
                    FileMeta {
                        number,
                        size,
                        smallest,
                        largest,
                    },
                ));
            }
        }
        if let Some((number, b)) = builder.take() {
            let (size, smallest, largest) = b.finish()?;
            outputs.push((
                number,
                FileMeta {
                    number,
                    size,
                    smallest,
                    largest,
                },
            ));
        }
        sync_dir(&self.dir)?;

        let removed: Vec<(usize, u64)> = c.inputs[0]
            .iter()
            .map(|f| (c.level, f.number))
            .chain(c.inputs[1].iter().map(|f| (c.level + 1, f.number)))
            .collect();
        let added: Vec<(usize, Arc<FileMeta>)> = outputs
            .iter()
            .map(|(_, m)| (c.level + 1, Arc::new(m.clone())))
            .collect();
        let largest_compacted = c.inputs[0].last().map(|f| f.largest.clone());

        {
            let mut core = self.core.lock().unwrap();
            let next_file_number = core.next_file_number;
            let new_version =
                core.version
                    .apply(&removed, added, core.log_number, next_file_number);
            save_manifest(&self.dir, &new_version)?;
            core.version = Arc::new(new_version);
            if let Some(largest) = largest_compacted {
                core.compact_pointer[c.level] = largest;
            }
        }

        // Only after the manifest no longer names them: a crash before this
        // point simply leaves the old files in place, which is still correct.
        for (level, number) in removed {
            let _ = level;
            self.table_cache.evict(number);
            let _ = fs::remove_file(table_path(&self.dir, number));
        }
        sync_dir(&self.dir)?;
        self.stats
            .bytes_compacted
            .fetch_add(bytes_in, Ordering::Relaxed);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        {
            let mut core = self.core.lock().unwrap();
            if core.mem.is_empty() && core.imm.is_none() {
                return Ok(());
            }
            if core.imm.is_none() && !core.mem.is_empty() {
                let new_log = core.allocate_file_number();
                let wal = WalWriter::create(&wal_path(&self.dir, new_log), self.opts.sync_mode)?;
                let old_log = core.log_number;
                let sealed = std::mem::take(&mut core.mem);
                core.imm = Some(Arc::new(sealed));
                core.imm_log_number = old_log;
                core.wal = wal;
                core.log_number = new_log;
            }
        }
        self.cv.notify_all();
        loop {
            {
                let core = self.core.lock().unwrap();
                if let Some(err) = &core.bg_error {
                    return Err(Error::Background(err.clone()));
                }
                if core.imm.is_none() {
                    return Ok(());
                }
            }
            if self.opts.background_compaction {
                std::thread::yield_now();
                let core = self.core.lock().unwrap();
                let _ = self.cv.wait_timeout(core, ms(20)).unwrap();
            } else {
                self.do_one_unit_of_work()?;
            }
        }
    }

    fn stats_snapshot(&self) -> StatsSnapshot {
        let core = self.core.lock().unwrap();
        let version = core.version.clone();
        let memtable_bytes = core.mem.approximate_size();
        drop(core);
        let (hits, misses) = self.table_cache.block_cache().stats();
        StatsSnapshot {
            writes: self.stats.writes.load(Ordering::Relaxed),
            reads: self.stats.reads.load(Ordering::Relaxed),
            flushes: self.stats.flushes.load(Ordering::Relaxed),
            compactions: self.stats.compactions.load(Ordering::Relaxed),
            trivial_moves: self.stats.trivial_moves.load(Ordering::Relaxed),
            bytes_compacted: self.stats.bytes_compacted.load(Ordering::Relaxed),
            write_stalls: self.stats.write_stalls.load(Ordering::Relaxed),
            recovered_batches: self.stats.recovered_batches.load(Ordering::Relaxed),
            cache_hits: hits,
            cache_misses: misses,
            files_per_level: (0..NUM_LEVELS).map(|l| version.num_files(l)).collect(),
            bytes_per_level: (0..NUM_LEVELS).map(|l| version.total_bytes(l)).collect(),
            memtable_bytes,
        }
    }
}

enum Job {
    Flush { imm: Arc<MemTable>, log_number: u64 },
    Compact(Box<Compaction>),
}

/// One planned compaction: `inputs[0]` from `level`, `inputs[1]` from `level + 1`.
struct Compaction {
    level: usize,
    inputs: [Vec<Arc<FileMeta>>; 2],
    deeper_levels_empty: bool,
}

impl Compaction {
    fn is_trivial_move(&self) -> bool {
        self.inputs[0].len() == 1 && self.inputs[1].is_empty()
    }
}

/// Chooses the most over-budget level, if any.
///
/// Level 0 is scored by file count, because its files overlap and every one of
/// them has to be consulted on a miss. Deeper levels are scored by bytes
/// against an exponentially growing budget, which is what keeps the tree
/// shallow and write amplification bounded.
fn pick_compaction(
    version: &Version,
    opts: &Options,
    compact_pointer: &[Vec<u8>],
) -> Option<Compaction> {
    let mut best_level = 0usize;
    let mut best_score = 0.0f64;
    for level in 0..NUM_LEVELS - 1 {
        let score = if level == 0 {
            version.num_files(0) as f64 / opts.l0_compaction_trigger as f64
        } else {
            version.total_bytes(level) as f64 / opts.max_bytes_for_level(level) as f64
        };
        if score > best_score {
            best_score = score;
            best_level = level;
        }
    }
    if best_score < 1.0 {
        return None;
    }

    let level = best_level;
    let mut inputs0: Vec<Arc<FileMeta>> = if level == 0 {
        let mut files = version.levels[0].clone();
        files.sort_by_key(|f| f.number);
        files
    } else {
        let files = &version.levels[level];
        // Resume where the previous compaction of this level stopped.
        let pointer = &compact_pointer[level];
        let chosen = files
            .iter()
            .find(|f| f.smallest.as_slice() > pointer.as_slice())
            .or_else(|| files.first())?;
        vec![chosen.clone()]
    };
    if inputs0.is_empty() {
        return None;
    }
    inputs0.sort_by(|a, b| a.smallest.cmp(&b.smallest));

    let smallest = inputs0.iter().map(|f| f.smallest.clone()).min().unwrap();
    let largest = inputs0.iter().map(|f| f.largest.clone()).max().unwrap();
    let inputs1 = version.overlapping(level + 1, &smallest, &largest);

    let deeper_levels_empty = (level + 2..NUM_LEVELS).all(|l| version.num_files(l) == 0);

    Some(Compaction {
        level,
        inputs: [inputs0, inputs1],
        deeper_levels_empty,
    })
}

fn build_table_from_memtable(
    dir: &Path,
    number: u64,
    mem: &MemTable,
    opts: &Options,
) -> Result<FileMeta> {
    let path = table_path(dir, number);
    let mut builder = TableBuilder::create(&path, opts)?;
    for (key, value) in mem.iter() {
        builder.add(key, value.as_deref())?;
    }
    let (size, smallest, largest) = builder.finish()?;
    Ok(FileMeta {
        number,
        size,
        smallest,
        largest,
    })
}

/// Lists the SSTable and log file numbers present in a database directory.
fn scan_dir(dir: &Path) -> Result<(Vec<u64>, Vec<u64>)> {
    let mut sst = Vec::new();
    let mut wal = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some((stem, ext)) = name.rsplit_once('.') else {
            continue;
        };
        let Ok(number) = stem.parse::<u64>() else {
            continue;
        };
        match ext {
            "sst" => sst.push(number),
            "wal" => wal.push(number),
            _ => {}
        }
    }
    sst.sort_unstable();
    wal.sort_unstable();
    Ok((sst, wal))
}

fn ms(n: u64) -> std::time::Duration {
    std::time::Duration::from_millis(n)
}
