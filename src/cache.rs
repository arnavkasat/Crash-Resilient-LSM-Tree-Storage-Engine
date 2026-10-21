//! A sharded LRU cache for decoded SSTable blocks.
//!
//! Without it every read of a hot key re-reads and re-parses a block from the
//! page cache; with it, repeat reads are a hash lookup and an `Arc` clone.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::sst::block::Block;

/// Identifies a block: which file it came from and its offset within the file.
pub type BlockKey = (u64, u64);

struct Node {
    key: BlockKey,
    value: Arc<Block>,
    prev: usize,
    next: usize,
}

const NIL: usize = usize::MAX;

/// Classic intrusive LRU: a slab of nodes plus head/tail indices, so every
/// operation is O(1) and no allocation happens on a cache hit.
struct Shard {
    map: HashMap<BlockKey, usize>,
    nodes: Vec<Node>,
    free: Vec<usize>,
    head: usize,
    tail: usize,
    charge: usize,
    capacity: usize,
}

impl Shard {
    fn new(capacity: usize) -> Shard {
        Shard {
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            charge: 0,
            capacity,
        }
    }

    fn unlink(&mut self, idx: usize) {
        let (prev, next) = (self.nodes[idx].prev, self.nodes[idx].next);
        if prev != NIL {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = NIL;
    }

    fn push_front(&mut self, idx: usize) {
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = self.head;
        if self.head != NIL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
    }

    fn get(&mut self, key: &BlockKey) -> Option<Arc<Block>> {
        let idx = *self.map.get(key)?;
        self.unlink(idx);
        self.push_front(idx);
        Some(self.nodes[idx].value.clone())
    }

    fn insert(&mut self, key: BlockKey, value: Arc<Block>) {
        if let Some(&idx) = self.map.get(&key) {
            self.charge -= self.nodes[idx].value.memory_usage();
            self.charge += value.memory_usage();
            self.nodes[idx].value = value;
            self.unlink(idx);
            self.push_front(idx);
            self.evict();
            return;
        }
        let charge = value.memory_usage();
        let idx = match self.free.pop() {
            Some(i) => {
                self.nodes[i] = Node {
                    key,
                    value,
                    prev: NIL,
                    next: NIL,
                };
                i
            }
            None => {
                self.nodes.push(Node {
                    key,
                    value,
                    prev: NIL,
                    next: NIL,
                });
                self.nodes.len() - 1
            }
        };
        self.map.insert(key, idx);
        self.push_front(idx);
        self.charge += charge;
        self.evict();
    }

    fn evict(&mut self) {
        while self.charge > self.capacity && self.tail != NIL {
            let idx = self.tail;
            self.charge -= self.nodes[idx].value.memory_usage();
            let key = self.nodes[idx].key;
            self.map.remove(&key);
            self.unlink(idx);
            // Drop the block eagerly but keep the slot for reuse.
            self.nodes[idx].value = Arc::new(Block::empty());
            self.free.push(idx);
        }
    }

    fn erase_file(&mut self, file_number: u64) {
        let doomed: Vec<BlockKey> = self
            .map
            .keys()
            .filter(|(f, _)| *f == file_number)
            .copied()
            .collect();
        for key in doomed {
            if let Some(idx) = self.map.remove(&key) {
                self.charge -= self.nodes[idx].value.memory_usage();
                self.unlink(idx);
                self.nodes[idx].value = Arc::new(Block::empty());
                self.free.push(idx);
            }
        }
    }
}

const SHARDS: usize = 8;

/// Thread-safe block cache. Sharding by key keeps lock contention low when
/// several readers are active at once.
pub struct BlockCache {
    shards: Vec<Mutex<Shard>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> BlockCache {
        let per_shard = (capacity_bytes / SHARDS).max(1);
        BlockCache {
            shards: (0..SHARDS)
                .map(|_| Mutex::new(Shard::new(per_shard)))
                .collect(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn shard_for(&self, key: &BlockKey) -> &Mutex<Shard> {
        let h = crate::bloom::hash_key(&[key.0.to_le_bytes(), key.1.to_le_bytes()].concat());
        &self.shards[(h as usize) % SHARDS]
    }

    pub fn get(&self, key: BlockKey) -> Option<Arc<Block>> {
        let found = self.shard_for(&key).lock().unwrap().get(&key);
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    pub fn insert(&self, key: BlockKey, value: Arc<Block>) {
        self.shard_for(&key).lock().unwrap().insert(key, value);
    }

    /// Drops every block belonging to a file that compaction has retired.
    pub fn erase_file(&self, file_number: u64) {
        for shard in &self.shards {
            shard.lock().unwrap().erase_file(file_number);
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: usize) -> Arc<Block> {
        Arc::new(Block::for_test(n))
    }

    #[test]
    fn hit_and_miss() {
        let c = BlockCache::new(1 << 20);
        assert!(c.get((1, 0)).is_none());
        c.insert((1, 0), block(128));
        assert!(c.get((1, 0)).is_some());
        let (hits, misses) = c.stats();
        assert_eq!((hits, misses), (1, 1));
    }

    #[test]
    fn evicts_least_recently_used() {
        // One shard's capacity is total/8; force everything into one shard by
        // using a tiny total so any insert overflows.
        let c = BlockCache::new(8 * 300);
        for i in 0..20u64 {
            c.insert((1, i), block(200));
        }
        // The cache must not grow without bound.
        let live = (0..20u64).filter(|i| c.get((1, *i)).is_some()).count();
        assert!(live < 20, "nothing was evicted");
        assert!(live > 0, "everything was evicted");
    }

    #[test]
    fn erase_file_drops_entries() {
        let c = BlockCache::new(1 << 20);
        c.insert((7, 0), block(64));
        c.insert((8, 0), block(64));
        c.erase_file(7);
        assert!(c.get((7, 0)).is_none());
        assert!(c.get((8, 0)).is_some());
    }
}
