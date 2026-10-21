//! Keeps SSTable readers (and therefore their file descriptors and index
//! blocks) open across lookups, so a point read does not pay an `open` and an
//! index parse every time.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::cache::BlockCache;
use crate::error::Result;
use crate::sst::Table;
use crate::version::table_path;

pub struct TableCache {
    dir: PathBuf,
    block_cache: Arc<BlockCache>,
    capacity: usize,
    open: Mutex<HashMap<u64, (Arc<Table>, u64)>>,
    tick: Mutex<u64>,
}

impl TableCache {
    pub fn new(dir: &Path, block_cache: Arc<BlockCache>, capacity: usize) -> TableCache {
        TableCache {
            dir: dir.to_path_buf(),
            block_cache,
            capacity: capacity.max(4),
            open: Mutex::new(HashMap::new()),
            tick: Mutex::new(0),
        }
    }

    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.block_cache
    }

    /// Returns a reader for `file_number`, opening the file if necessary.
    pub fn get(&self, file_number: u64) -> Result<Arc<Table>> {
        let now = {
            let mut t = self.tick.lock().unwrap();
            *t += 1;
            *t
        };
        {
            let mut open = self.open.lock().unwrap();
            if let Some(entry) = open.get_mut(&file_number) {
                entry.1 = now;
                return Ok(entry.0.clone());
            }
        }
        // Opening happens outside the lock so one slow open cannot block every
        // other reader.
        let table = Arc::new(Table::open(
            &table_path(&self.dir, file_number),
            file_number,
            self.block_cache.clone(),
        )?);
        let mut open = self.open.lock().unwrap();
        open.insert(file_number, (table.clone(), now));
        if open.len() > self.capacity {
            // Evict the least recently used reader. Any reader already handed
            // out stays valid - it owns its own `Arc`.
            let victim = open
                .iter()
                .min_by_key(|(_, (_, stamp))| *stamp)
                .map(|(k, _)| *k);
            if let Some(victim) = victim {
                open.remove(&victim);
            }
        }
        Ok(table)
    }

    /// Forgets a file that compaction has retired, together with its blocks.
    ///
    /// Readers that already hold the `Arc` keep working: on Linux an unlinked
    /// file stays readable through an open descriptor, so compaction never has
    /// to wait for in-flight reads.
    pub fn evict(&self, file_number: u64) {
        self.open.lock().unwrap().remove(&file_number);
        self.block_cache.erase_file(file_number);
    }

    pub fn open_count(&self) -> usize {
        self.open.lock().unwrap().len()
    }
}
