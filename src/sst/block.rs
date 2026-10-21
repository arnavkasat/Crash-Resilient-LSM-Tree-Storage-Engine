//! The block: the unit of SSTable storage, caching and I/O.
//!
//! Entries are sorted and prefix-compressed against the previous key. Every
//! `restart_interval` entries a *restart point* stores a full key, which gives
//! the binary search inside a block somewhere to land.
//!
//! ```text
//! entry := varint shared | varint non_shared | varint tag | key_delta | value
//!          tag = 0            -> tombstone (no value bytes)
//!          tag = len + 1      -> value of `len` bytes
//! block := entry* | u32 restart_offset* | u32 num_restarts
//! ```

use std::sync::Arc;

use crate::error::{corrupt, Result};
use crate::util::{get_u32, get_varint, put_u32, put_varint, shared_prefix, Entry};

pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Builds one block in memory.
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
    finished: bool,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> BlockBuilder {
        BlockBuilder {
            buf: Vec::with_capacity(4096),
            restarts: vec![0],
            counter: 0,
            restart_interval: restart_interval.max(1),
            last_key: Vec::new(),
            finished: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Bytes this block will occupy once finished.
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    /// Keys must arrive in strictly increasing order.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) {
        debug_assert!(!self.finished);
        debug_assert!(
            self.last_key.is_empty() || key > &self.last_key[..],
            "keys out of order"
        );

        let shared = if self.counter < self.restart_interval {
            shared_prefix(&self.last_key, key)
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };

        put_varint(&mut self.buf, shared as u64);
        put_varint(&mut self.buf, (key.len() - shared) as u64);
        match value {
            None => put_varint(&mut self.buf, 0),
            Some(v) => put_varint(&mut self.buf, v.len() as u64 + 1),
        }
        self.buf.extend_from_slice(&key[shared..]);
        if let Some(v) = value {
            self.buf.extend_from_slice(v);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn finish(&mut self) -> &[u8] {
        if !self.finished {
            for r in &self.restarts {
                self.buf.extend_from_slice(&r.to_le_bytes());
            }
            put_u32(&mut self.buf, self.restarts.len() as u32);
            self.finished = true;
        }
        &self.buf
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }
}

/// A decoded, read-only block. Shared between readers via the block cache.
#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    restart_offset: usize,
    num_restarts: usize,
}

impl Block {
    /// A real, well-formed block holding one entry of `payload` bytes.
    #[cfg(test)]
    pub fn for_test(payload: usize) -> Block {
        let mut b = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);
        b.add(b"k", Some(&vec![0u8; payload]));
        Block::decode(b.finish().to_vec()).unwrap()
    }

    pub fn empty() -> Block {
        Block {
            data: Vec::new(),
            restart_offset: 0,
            num_restarts: 0,
        }
    }

    pub fn decode(data: Vec<u8>) -> Result<Block> {
        if data.len() < 4 {
            return corrupt("block shorter than its trailer");
        }
        let num_restarts = get_u32(&data, data.len() - 4)? as usize;
        let needed = 4 + num_restarts * 4;
        if num_restarts == 0 || data.len() < needed {
            return corrupt("block restart array does not fit");
        }
        let restart_offset = data.len() - needed;
        Ok(Block {
            data,
            restart_offset,
            num_restarts,
        })
    }

    pub fn memory_usage(&self) -> usize {
        self.data.len() + std::mem::size_of::<Block>()
    }

    fn restart_point(&self, i: usize) -> usize {
        get_u32(&self.data, self.restart_offset + i * 4).unwrap_or(0) as usize
    }

    /// Decodes the entry at `off`; returns (key, value, next offset).
    fn entry_at(&self, off: usize, prev_key: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>, usize)> {
        let (shared, p) = get_varint(&self.data, off)?;
        let (non_shared, p) = get_varint(&self.data, p)?;
        let (tag, p) = get_varint(&self.data, p)?;
        let shared = shared as usize;
        let non_shared = non_shared as usize;
        if shared > prev_key.len() {
            return corrupt("entry shares more bytes than the previous key has");
        }
        let key_end = p + non_shared;
        if key_end > self.restart_offset {
            return corrupt("entry key runs past the end of the block");
        }
        let mut key = Vec::with_capacity(shared + non_shared);
        key.extend_from_slice(&prev_key[..shared]);
        key.extend_from_slice(&self.data[p..key_end]);

        let (value, next) = if tag == 0 {
            (None, key_end)
        } else {
            let len = tag as usize - 1;
            let value_end = key_end + len;
            if value_end > self.restart_offset {
                return corrupt("entry value runs past the end of the block");
            }
            (Some(self.data[key_end..value_end].to_vec()), value_end)
        };
        Ok((key, value, next))
    }

    /// Full key stored at restart point `i`.
    fn key_at_restart(&self, i: usize) -> Result<Vec<u8>> {
        let off = self.restart_point(i);
        let (key, _, _) = self.entry_at(off, &[])?;
        Ok(key)
    }

    /// Cursor positioned at the first entry.
    pub fn iter(self: &Arc<Self>) -> BlockIter {
        BlockIter {
            block: self.clone(),
            offset: 0,
            prev_key: Vec::new(),
        }
    }

    /// Cursor positioned at the first entry with `key >= target`.
    pub fn seek(self: &Arc<Self>, target: &[u8]) -> Result<BlockIter> {
        if self.num_restarts == 0 {
            return Ok(BlockIter {
                block: self.clone(),
                offset: 0,
                prev_key: Vec::new(),
            });
        }
        // Find the last restart point whose key is <= target.
        let (mut left, mut right) = (0usize, self.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            if self.key_at_restart(mid)?.as_slice() <= target {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        let mut iter = BlockIter {
            block: self.clone(),
            offset: self.restart_point(left),
            prev_key: Vec::new(),
        };
        // Linear scan from the restart point; at most restart_interval entries.
        loop {
            let save = (iter.offset, iter.prev_key.clone());
            match iter.next()? {
                Some((key, _)) if key.as_slice() < target => continue,
                _ => {
                    iter.offset = save.0;
                    iter.prev_key = save.1;
                    return Ok(iter);
                }
            }
        }
    }

    /// Point lookup. `Ok(None)` means the key is not in this block.
    pub fn get(self: &Arc<Self>, target: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let mut iter = self.seek(target)?;
        match iter.next()? {
            Some((key, value)) if key == target => Ok(Some(value)),
            _ => Ok(None),
        }
    }
}

/// Forward cursor over the entries of a block.
pub struct BlockIter {
    block: Arc<Block>,
    offset: usize,
    prev_key: Vec<u8>,
}

impl BlockIter {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Entry>> {
        if self.offset >= self.block.restart_offset {
            return Ok(None);
        }
        let (key, value, next) = self.block.entry_at(self.offset, &self.prev_key)?;
        self.offset = next;
        self.prev_key.clear();
        self.prev_key.extend_from_slice(&key);
        Ok(Some((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(pairs: &[(&str, Option<&str>)]) -> Arc<Block> {
        let mut b = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);
        for (k, v) in pairs {
            b.add(k.as_bytes(), v.map(|s| s.as_bytes()));
        }
        Arc::new(Block::decode(b.finish().to_vec()).unwrap())
    }

    #[test]
    fn roundtrip_with_prefix_compression() {
        let pairs: Vec<(String, String)> = (0..200)
            .map(|i| (format!("user:{i:06}"), format!("value-{i}")))
            .collect();
        let mut b = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);
        for (k, v) in &pairs {
            b.add(k.as_bytes(), Some(v.as_bytes()));
        }
        let block = Arc::new(Block::decode(b.finish().to_vec()).unwrap());

        let mut iter = block.iter();
        for (k, v) in &pairs {
            let (gk, gv) = iter.next().unwrap().unwrap();
            assert_eq!(gk, k.as_bytes());
            assert_eq!(gv.unwrap(), v.as_bytes());
        }
        assert!(iter.next().unwrap().is_none());
    }

    #[test]
    fn get_finds_every_key() {
        let pairs: Vec<(String, String)> = (0..300)
            .map(|i| (format!("k{i:05}"), format!("v{i}")))
            .collect();
        let mut b = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);
        for (k, v) in &pairs {
            b.add(k.as_bytes(), Some(v.as_bytes()));
        }
        let block = Arc::new(Block::decode(b.finish().to_vec()).unwrap());
        for (k, v) in &pairs {
            let got = block.get(k.as_bytes()).unwrap().unwrap().unwrap();
            assert_eq!(got, v.as_bytes());
        }
        assert!(block.get(b"k99999").unwrap().is_none());
        assert!(block.get(b"a").unwrap().is_none());
    }

    #[test]
    fn tombstones_survive_encoding() {
        let block = build(&[("a", Some("1")), ("b", None), ("c", Some("3"))]);
        assert_eq!(block.get(b"b").unwrap(), Some(None));
        assert_eq!(block.get(b"a").unwrap(), Some(Some(b"1".to_vec())));
    }

    #[test]
    fn seek_lands_on_first_key_at_or_after() {
        let block = build(&[("b", Some("1")), ("d", Some("2")), ("f", Some("3"))]);
        let mut it = block.seek(b"c").unwrap();
        assert_eq!(it.next().unwrap().unwrap().0, b"d");
        let mut it = block.seek(b"a").unwrap();
        assert_eq!(it.next().unwrap().unwrap().0, b"b");
        let mut it = block.seek(b"z").unwrap();
        assert!(it.next().unwrap().is_none());
    }

    #[test]
    fn truncated_block_is_rejected() {
        let block = build(&[("a", Some("1"))]);
        let mut raw = block.data.clone();
        raw.truncate(2);
        assert!(Block::decode(raw).is_err());
    }
}
