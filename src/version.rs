//! The set of SSTables that make up the database at one point in time, and the
//! manifest that records it durably.
//!
//! A `Version` is immutable and shared behind an `Arc`: a reader clones the
//! pointer, releases the lock, and keeps a consistent view of the tree even
//! while compaction installs a new version underneath it.
//!
//! The manifest is rewritten in full and swapped in with `rename`, which is
//! atomic on POSIX. After a crash the file on disk is therefore either the
//! complete old version or the complete new one - never a blend of the two.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::crc::crc32c;
use crate::error::{corrupt, Result};
use crate::options::Options;
use crate::util::{from_hex, to_hex};

pub const NUM_LEVELS: usize = 7;
const MANIFEST_HEADER: &str = "LSMKV1";

/// What the manifest knows about one SSTable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMeta {
    pub number: u64,
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

impl FileMeta {
    /// True when this file's key range touches `[start, end]`.
    pub fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        !(self.largest.as_slice() < start || self.smallest.as_slice() > end)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.smallest.as_slice() <= key && key <= self.largest.as_slice()
    }
}

/// An immutable snapshot of the tree.
#[derive(Clone, Debug, Default)]
pub struct Version {
    pub levels: Vec<Vec<Arc<FileMeta>>>,
    /// Logs with a number below this have been flushed and can be deleted.
    pub log_number: u64,
    pub next_file_number: u64,
}

impl Version {
    pub fn new() -> Version {
        Version {
            levels: vec![Vec::new(); NUM_LEVELS],
            log_number: 1,
            next_file_number: 2,
        }
    }

    pub fn num_files(&self, level: usize) -> usize {
        self.levels.get(level).map_or(0, |l| l.len())
    }

    pub fn total_bytes(&self, level: usize) -> u64 {
        self.levels
            .get(level)
            .map_or(0, |l| l.iter().map(|f| f.size).sum())
    }

    pub fn total_files(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    /// Files in `level` whose range overlaps `[start, end]`.
    ///
    /// Level 0 files may overlap each other, so they are returned
    /// newest-first; deeper levels are disjoint and stay in key order.
    pub fn overlapping(&self, level: usize, start: &[u8], end: &[u8]) -> Vec<Arc<FileMeta>> {
        let Some(files) = self.levels.get(level) else {
            return Vec::new();
        };
        let mut out: Vec<Arc<FileMeta>> = files
            .iter()
            .filter(|f| f.overlaps(start, end))
            .cloned()
            .collect();
        if level == 0 {
            out.sort_by_key(|f| std::cmp::Reverse(f.number));
        }
        out
    }

    /// Files that could hold `key`, in the order they must be consulted:
    /// newest level-0 file first, then one candidate per deeper level.
    pub fn files_for_key(&self, key: &[u8]) -> Vec<Arc<FileMeta>> {
        let mut out = Vec::new();
        let mut l0: Vec<&Arc<FileMeta>> =
            self.levels[0].iter().filter(|f| f.contains(key)).collect();
        l0.sort_by_key(|f| std::cmp::Reverse(f.number));
        out.extend(l0.into_iter().cloned());

        for level in 1..self.levels.len() {
            let files = &self.levels[level];
            // Deeper levels are sorted and disjoint, so one binary search picks
            // the only file that can hold the key.
            if let Ok(idx) = files.binary_search_by(|f| {
                if f.largest.as_slice() < key {
                    std::cmp::Ordering::Less
                } else if f.smallest.as_slice() > key {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                out.push(files[idx].clone());
            }
        }
        out
    }

    /// Produces the next version: `removed` files deleted, `added` installed.
    pub fn apply(
        &self,
        removed: &[(usize, u64)],
        added: Vec<(usize, Arc<FileMeta>)>,
        log_number: u64,
        next_file_number: u64,
    ) -> Version {
        let mut levels = self.levels.clone();
        for (level, number) in removed {
            levels[*level].retain(|f| f.number != *number);
        }
        for (level, meta) in added {
            levels[level].push(meta);
        }
        for (level, files) in levels.iter_mut().enumerate() {
            if level == 0 {
                files.sort_by_key(|f| f.number);
            } else {
                files.sort_by(|a, b| a.smallest.cmp(&b.smallest));
            }
        }
        Version {
            levels,
            log_number,
            next_file_number,
        }
    }

    /// Every file number referenced by this version.
    pub fn live_files(&self) -> Vec<u64> {
        self.levels.iter().flatten().map(|f| f.number).collect()
    }

    fn encode(&self) -> Vec<u8> {
        let mut text = String::new();
        text.push_str(MANIFEST_HEADER);
        text.push('\n');
        text.push_str(&format!("log {}\n", self.log_number));
        text.push_str(&format!("next_file {}\n", self.next_file_number));
        for (level, files) in self.levels.iter().enumerate() {
            for f in files {
                text.push_str(&format!(
                    "file {} {} {} {} {}\n",
                    level,
                    f.number,
                    f.size,
                    to_hex(&f.smallest),
                    to_hex(&f.largest)
                ));
            }
        }
        let crc = crc32c(text.as_bytes());
        text.push_str(&format!("crc {crc:08x}\n"));
        text.into_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Version> {
        let text = match std::str::from_utf8(bytes) {
            Ok(t) => t,
            Err(_) => return corrupt("manifest is not valid UTF-8"),
        };
        let Some(crc_pos) = text.rfind("\ncrc ") else {
            return corrupt("manifest has no checksum line");
        };
        let body = &text[..crc_pos + 1];
        let crc_line = text[crc_pos + 1..].trim_end();
        let stored = crc_line
            .strip_prefix("crc ")
            .and_then(|h| u32::from_str_radix(h, 16).ok());
        match stored {
            Some(c) if c == crc32c(body.as_bytes()) => {}
            _ => return corrupt("manifest checksum mismatch"),
        }

        let mut lines = body.lines();
        if lines.next() != Some(MANIFEST_HEADER) {
            return corrupt("unrecognised manifest header");
        }
        let mut version = Version::new();
        for line in lines {
            let parts: Vec<&str> = line.split_whitespace().collect();
            match parts.as_slice() {
                ["log", n] => version.log_number = parse_u64(n)?,
                ["next_file", n] => version.next_file_number = parse_u64(n)?,
                ["file", level, number, size, smallest, largest] => {
                    let level: usize = parse_u64(level)? as usize;
                    if level >= NUM_LEVELS {
                        return corrupt("manifest names a level that does not exist");
                    }
                    version.levels[level].push(Arc::new(FileMeta {
                        number: parse_u64(number)?,
                        size: parse_u64(size)?,
                        smallest: from_hex(smallest)?,
                        largest: from_hex(largest)?,
                    }));
                }
                [] => {}
                _ => return corrupt(format!("unrecognised manifest line: {line}")),
            }
        }
        for (level, files) in version.levels.iter_mut().enumerate() {
            if level == 0 {
                files.sort_by_key(|f| f.number);
            } else {
                files.sort_by(|a, b| a.smallest.cmp(&b.smallest));
            }
        }
        Ok(version)
    }
}

fn parse_u64(s: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|_| crate::error::Error::Corruption(format!("bad number: {s}")))
}

pub fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

fn manifest_tmp_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST.tmp")
}

/// Commits a version.
///
/// The sequence - write temp, fsync temp, rename, fsync directory - is what
/// makes an install atomic: the rename only becomes visible once the new
/// contents are on stable storage, and fsyncing the directory makes the rename
/// itself durable.
pub fn save_manifest(dir: &Path, version: &Version) -> Result<()> {
    let tmp = manifest_tmp_path(dir);
    {
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(&version.encode())?;
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    fs::rename(&tmp, manifest_path(dir))?;
    sync_dir(dir)?;
    Ok(())
}

pub fn load_manifest(dir: &Path) -> Result<Option<Version>> {
    let path = manifest_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut buf = Vec::new();
    File::open(&path)?.read_to_end(&mut buf)?;
    Ok(Some(Version::decode(&buf)?))
}

/// `fsync` on a directory, so creations, renames and unlinks inside it are
/// durable. Skipping this is the classic way to "lose" a file that was
/// fsynced itself.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let f = OpenOptions::new().read(true).open(dir)?;
    f.sync_all()?;
    Ok(())
}

pub fn table_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:010}.sst"))
}

pub fn wal_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:010}.wal"))
}

/// Picks the level a freshly flushed MemTable should land in. Pushing it past
/// level 0 when nothing overlaps saves a compaction later.
pub fn pick_level_for_flush(
    version: &Version,
    opts: &Options,
    smallest: &[u8],
    largest: &[u8],
) -> usize {
    if !version.overlapping(0, smallest, largest).is_empty() {
        return 0;
    }
    let mut level = 0;
    while level < 2 {
        if !version.overlapping(level + 1, smallest, largest).is_empty() {
            break;
        }
        if version.total_bytes(level + 2) > opts.max_bytes_for_level(level + 2) {
            break;
        }
        level += 1;
    }
    level
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(number: u64, smallest: &str, largest: &str) -> Arc<FileMeta> {
        Arc::new(FileMeta {
            number,
            size: 1000,
            smallest: smallest.as_bytes().to_vec(),
            largest: largest.as_bytes().to_vec(),
        })
    }

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lsmkv-ver-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn manifest_roundtrip() {
        let mut v = Version::new();
        v.levels[0].push(meta(5, "a", "c"));
        v.levels[1].push(meta(6, "d", "f"));
        v.log_number = 9;
        v.next_file_number = 10;

        let dir = tmpdir("roundtrip");
        save_manifest(&dir, &v).unwrap();
        let loaded = load_manifest(&dir).unwrap().unwrap();
        assert_eq!(loaded.log_number, 9);
        assert_eq!(loaded.next_file_number, 10);
        assert_eq!(loaded.levels[0], v.levels[0]);
        assert_eq!(loaded.levels[1], v.levels[1]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_with_binary_keys_roundtrips() {
        let mut v = Version::new();
        v.levels[0].push(Arc::new(FileMeta {
            number: 1,
            size: 10,
            smallest: vec![0, 10, 255],
            largest: vec![255, 0, 7],
        }));
        let dir = tmpdir("binary");
        save_manifest(&dir, &v).unwrap();
        let loaded = load_manifest(&dir).unwrap().unwrap();
        assert_eq!(loaded.levels[0][0].smallest, vec![0, 10, 255]);
        assert_eq!(loaded.levels[0][0].largest, vec![255, 0, 7]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn damaged_manifest_is_rejected() {
        let dir = tmpdir("damaged");
        let mut v = Version::new();
        v.levels[0].push(meta(5, "a", "c"));
        save_manifest(&dir, &v).unwrap();

        let path = manifest_path(&dir);
        let mut text = fs::read_to_string(&path).unwrap();
        text = text.replace("file 0 5 1000", "file 0 7 1000");
        fs::write(&path, text).unwrap();
        assert!(load_manifest(&dir).is_err(), "checksum must catch the edit");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_manifest_is_not_an_error() {
        let dir = tmpdir("missing");
        assert!(load_manifest(&dir).unwrap().is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn overlapping_and_lookup_order() {
        let mut v = Version::new();
        v.levels[0].push(meta(1, "a", "m"));
        v.levels[0].push(meta(2, "c", "z"));
        v.levels[1].push(meta(3, "a", "f"));
        v.levels[1].push(meta(4, "g", "z"));

        let files = v.files_for_key(b"d");
        let numbers: Vec<u64> = files.iter().map(|f| f.number).collect();
        assert_eq!(
            numbers,
            vec![2, 1, 3],
            "newest level-0 file must be consulted first"
        );

        let overlap = v.overlapping(1, b"e", b"h");
        assert_eq!(overlap.len(), 2);
        assert!(v.overlapping(1, b"zzz", b"zzzz").is_empty());
    }

    #[test]
    fn apply_installs_and_retires_files() {
        let v = Version::new();
        let v = v.apply(&[], vec![(0, meta(1, "a", "b"))], 1, 2);
        assert_eq!(v.num_files(0), 1);
        let v = v.apply(&[(0, 1)], vec![(1, meta(2, "a", "b"))], 1, 3);
        assert_eq!(v.num_files(0), 0);
        assert_eq!(v.num_files(1), 1);
        assert_eq!(v.live_files(), vec![2]);
    }
}
