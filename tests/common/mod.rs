#![allow(dead_code)]

//! Shared helpers for the integration tests.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lsmkv::Options;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temporary directory that removes itself when dropped.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "lsmkv-it-{tag}-{}-{}-{n}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Small buffers and thresholds, so tests exercise flushing and compaction
/// without writing gigabytes.
pub fn small_options() -> Options {
    Options {
        write_buffer_size: 16 << 10,
        block_size: 512,
        target_file_size: 32 << 10,
        level_base_size: 128 << 10,
        l0_compaction_trigger: 2,
        l0_stop_writes_trigger: 8,
        block_cache_size: 1 << 20,
        ..Default::default()
    }
}

/// Same, but with compaction run inline so a test is fully deterministic.
pub fn deterministic_options() -> Options {
    Options {
        background_compaction: false,
        ..small_options()
    }
}
