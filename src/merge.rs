//! N-way merge over sorted sources.
//!
//! Sources are supplied newest-first. When several of them hold the same key,
//! the one from the newest source wins and the rest are discarded - that single
//! rule is what makes an overwrite in a MemTable shadow the value still sitting
//! in a level-3 SSTable.

use crate::error::Result;
use crate::memtable::Entry;

pub trait KvSource {
    fn next_entry(&mut self) -> Result<Option<Entry>>;
}

impl KvSource for crate::sst::TableIter {
    fn next_entry(&mut self) -> Result<Option<Entry>> {
        self.next()
    }
}

/// A source backed by an already-materialised, sorted vector - used for the
/// MemTable, whose contents are cloned out under the lock.
pub struct VecSource {
    entries: std::vec::IntoIter<Entry>,
}

impl VecSource {
    pub fn new(entries: Vec<Entry>) -> VecSource {
        debug_assert!(
            entries.windows(2).all(|w| w[0].0 < w[1].0),
            "source must be sorted"
        );
        VecSource {
            entries: entries.into_iter(),
        }
    }
}

impl KvSource for VecSource {
    fn next_entry(&mut self) -> Result<Option<Entry>> {
        Ok(self.entries.next())
    }
}

pub struct MergingIter {
    sources: Vec<Box<dyn KvSource + Send>>,
    heads: Vec<Option<Entry>>,
    primed: bool,
}

impl MergingIter {
    /// `sources` must be ordered newest to oldest.
    pub fn new(sources: Vec<Box<dyn KvSource + Send>>) -> MergingIter {
        let heads = (0..sources.len()).map(|_| None).collect();
        MergingIter {
            sources,
            heads,
            primed: false,
        }
    }

    fn prime(&mut self) -> Result<()> {
        if !self.primed {
            for i in 0..self.sources.len() {
                self.heads[i] = self.sources[i].next_entry()?;
            }
            self.primed = true;
        }
        Ok(())
    }

    /// Next merged entry. Tombstones are passed through; the caller decides
    /// whether to drop them (a scan does, compaction to a non-final level
    /// does not).
    pub fn next_entry(&mut self) -> Result<Option<Entry>> {
        self.prime()?;

        let mut best: Option<usize> = None;
        for (i, head) in self.heads.iter().enumerate() {
            if let Some((key, _)) = head {
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if key < &self.heads[b].as_ref().unwrap().0 {
                            best = Some(i);
                        }
                    }
                }
            }
        }

        let Some(winner) = best else { return Ok(None) };
        let entry = self.heads[winner].take().unwrap();
        self.heads[winner] = self.sources[winner].next_entry()?;

        // Drop the same key from every older source.
        for i in 0..self.heads.len() {
            while matches!(&self.heads[i], Some((k, _)) if *k == entry.0) {
                self.heads[i] = self.sources[i].next_entry()?;
            }
        }
        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(pairs: &[(&str, Option<&str>)]) -> Box<dyn KvSource + Send> {
        Box::new(VecSource::new(
            pairs
                .iter()
                .map(|(k, v)| (k.as_bytes().to_vec(), v.map(|s| s.as_bytes().to_vec())))
                .collect(),
        ))
    }

    fn drain(mut it: MergingIter) -> Vec<(String, Option<String>)> {
        let mut out = Vec::new();
        while let Some((k, v)) = it.next_entry().unwrap() {
            out.push((
                String::from_utf8(k).unwrap(),
                v.map(|v| String::from_utf8(v).unwrap()),
            ));
        }
        out
    }

    #[test]
    fn newest_source_wins() {
        let it = MergingIter::new(vec![
            src(&[("b", Some("new"))]),
            src(&[("a", Some("old")), ("b", Some("old")), ("c", Some("old"))]),
        ]);
        assert_eq!(
            drain(it),
            vec![
                ("a".into(), Some("old".into())),
                ("b".into(), Some("new".into())),
                ("c".into(), Some("old".into())),
            ]
        );
    }

    #[test]
    fn tombstone_in_newest_source_shadows_older_value() {
        let it = MergingIter::new(vec![src(&[("a", None)]), src(&[("a", Some("v"))])]);
        assert_eq!(drain(it), vec![("a".into(), None)]);
    }

    #[test]
    fn handles_empty_and_single_sources() {
        assert!(drain(MergingIter::new(vec![])).is_empty());
        assert!(drain(MergingIter::new(vec![src(&[])])).is_empty());
        let it = MergingIter::new(vec![src(&[("a", Some("1"))])]);
        assert_eq!(drain(it), vec![("a".into(), Some("1".into()))]);
    }

    #[test]
    fn output_is_sorted_across_many_sources() {
        let sources: Vec<Box<dyn KvSource + Send>> = (0..5)
            .map(|s| {
                let entries: Vec<Entry> = (0..20)
                    .map(|i| {
                        (
                            format!("k{:04}", i * 5 + s).into_bytes(),
                            Some(vec![s as u8]),
                        )
                    })
                    .collect();
                Box::new(VecSource::new(entries)) as Box<dyn KvSource + Send>
            })
            .collect();
        let out = drain(MergingIter::new(sources));
        assert_eq!(out.len(), 100);
        assert!(out.windows(2).all(|w| w[0].0 < w[1].0));
    }
}
