//! The MemTable: the in-memory half of the LSM tree.
//!
//! A `BTreeMap` keeps keys sorted so a flush is a single ordered pass, and
//! range scans can be merged with the on-disk tables without a sort step.
//! A deletion is stored as a `None` value - a *tombstone* - because the key may
//! still exist in an older SSTable and must be shadowed until compaction drops
//! it.

use std::collections::BTreeMap;
use std::ops::Bound;

pub use crate::util::Entry;

#[derive(Default, Debug)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Approximate heap footprint, used to decide when to flush.
    bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable::default()
    }

    fn charge(key: &[u8], value: Option<&[u8]>) -> usize {
        // Keys and values plus BTreeMap/Option bookkeeping.
        key.len() + value.map_or(0, |v| v.len()) + 64
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.bytes += Self::charge(&key, value.as_deref());
        if let Some(old) = self.map.insert(key, value) {
            // Overwriting: the old entry's charge goes away. The key is still
            // there, so only the value is refunded.
            self.bytes = self.bytes.saturating_sub(old.map_or(0, |v| v.len()) + 64);
        }
    }

    /// `None` - no entry here. `Some(None)` - a tombstone. `Some(Some(v))` - a value.
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.map.get(key).cloned()
    }

    pub fn approximate_size(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.map.iter()
    }

    /// Entries in `[start, end)`, cloned so the caller can release the lock.
    pub fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<Entry> {
        let start = match start {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match end {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.map
            .range((start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let mut m = MemTable::new();
        assert_eq!(m.get(b"a"), None);
        m.insert(b"a".to_vec(), Some(b"1".to_vec()));
        assert_eq!(m.get(b"a"), Some(Some(b"1".to_vec())));
        m.insert(b"a".to_vec(), None);
        assert_eq!(m.get(b"a"), Some(None), "a delete must leave a tombstone");
    }

    #[test]
    fn iteration_is_sorted() {
        let mut m = MemTable::new();
        for k in ["c", "a", "b"] {
            m.insert(k.as_bytes().to_vec(), Some(b"v".to_vec()));
        }
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn size_grows_and_overwrite_does_not_double_count() {
        let mut m = MemTable::new();
        for i in 0..100 {
            m.insert(format!("k{i}").into_bytes(), Some(vec![0u8; 100]));
        }
        let after_insert = m.approximate_size();
        assert!(after_insert >= 100 * 100);
        for i in 0..100 {
            m.insert(format!("k{i}").into_bytes(), Some(vec![0u8; 100]));
        }
        assert_eq!(m.len(), 100);
        assert!(m.approximate_size() < after_insert * 2);
    }

    #[test]
    fn range_respects_bounds() {
        let mut m = MemTable::new();
        for k in ["a", "b", "c", "d"] {
            m.insert(k.as_bytes().to_vec(), Some(b"v".to_vec()));
        }
        let got = m.range(Bound::Included(&b"b"[..]), Bound::Excluded(&b"d"[..]));
        let keys: Vec<_> = got.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    }
}
