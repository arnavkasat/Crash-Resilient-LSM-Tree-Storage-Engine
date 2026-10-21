//! Tunables. The defaults aim at a few-GB working set on an SSD.

/// Controls how aggressively a write is made durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// `fsync` the write-ahead log before every `put`/`delete` returns.
    /// A write that returns survives a power loss. This is the mode the crash
    /// tests run in.
    EveryWrite,
    /// Let the OS schedule the flush. Faster, but a machine crash can lose
    /// recent writes. A *process* crash still loses nothing, because the bytes
    /// are already in the kernel's page cache.
    Os,
}

#[derive(Clone, Debug)]
pub struct Options {
    /// Create the database directory if it does not exist.
    pub create_if_missing: bool,
    /// Durability of individual writes.
    pub sync_mode: SyncMode,
    /// MemTable size that triggers a flush to a level-0 SSTable.
    pub write_buffer_size: usize,
    /// Uncompressed size target for one SSTable block.
    pub block_size: usize,
    /// Size at which a compaction output rolls over to a new file.
    pub target_file_size: u64,
    /// Number of level-0 files that triggers a compaction into level 1.
    pub l0_compaction_trigger: usize,
    /// Number of level-0 files at which writers are stalled, so compaction can
    /// catch up instead of letting read amplification run away.
    pub l0_stop_writes_trigger: usize,
    /// Byte budget for level 1. Each deeper level gets 10x the previous one,
    /// which is what keeps the tree to a handful of levels and bounds write
    /// amplification at roughly 10x per level.
    pub level_base_size: u64,
    /// Bloom filter bits per key. 10 gives roughly a 1% false-positive rate;
    /// 0 switches filters off entirely (useful for measuring what they buy).
    pub bloom_bits_per_key: usize,
    /// Total size of the shared block cache.
    pub block_cache_size: usize,
    /// Run compaction on a background thread. Turning this off makes the
    /// engine fully deterministic, which the tests rely on.
    pub background_compaction: bool,
    /// Keep SSTable readers open across lookups.
    pub max_open_tables: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            sync_mode: SyncMode::EveryWrite,
            write_buffer_size: 4 << 20,
            block_size: 4 << 10,
            target_file_size: 4 << 20,
            l0_compaction_trigger: 4,
            l0_stop_writes_trigger: 20,
            level_base_size: 16 << 20,
            bloom_bits_per_key: 10,
            block_cache_size: 64 << 20,
            background_compaction: true,
            max_open_tables: 512,
        }
    }
}

impl Options {
    /// Byte budget for a level. Level 0 is counted in files, not bytes.
    pub fn max_bytes_for_level(&self, level: usize) -> u64 {
        let mut size = self.level_base_size;
        for _ in 1..level.max(1) {
            size = size.saturating_mul(10);
        }
        size
    }
}
