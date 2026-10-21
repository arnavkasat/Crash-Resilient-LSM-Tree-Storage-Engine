//! The write-ahead log: the reason a committed write survives a crash.
//!
//! Every mutation is appended here *before* it is applied to the MemTable, and
//! (in `SyncMode::EveryWrite`) `fsync`ed before `put` returns. Recovery replays
//! the logs the manifest says are still live.
//!
//! ```text
//! record := u32 crc | u32 length | payload
//! payload := u64 sequence | u32 count | entry*
//! entry  := u8 kind (0 = put, 1 = delete) | u32 klen | key | u32 vlen | value
//! ```
//!
//! The CRC covers the length field as well as the payload, so a record torn by
//! a power loss mid-append fails the check instead of being replayed as
//! garbage. Replay stops at the first record that does not verify: everything
//! before it was fully written, so everything acknowledged is recovered.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use crate::crc::crc32c;
use crate::error::{corrupt, Result};
use crate::options::SyncMode;
use crate::util::{get_u32, get_u64, put_u32, put_u64};

pub const KIND_PUT: u8 = 0;
pub const KIND_DELETE: u8 = 1;

const HEADER_SIZE: usize = 8;

/// A group of mutations that are applied together, all-or-nothing.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    entries: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.entries.push((KIND_PUT, key.into(), value.into()));
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.entries.push((KIND_DELETE, key.into(), Vec::new()));
        self
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// (key, value) pairs, where `None` marks a deletion.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.entries.iter().map(|(kind, k, v)| {
            (
                k.as_slice(),
                if *kind == KIND_DELETE {
                    None
                } else {
                    Some(v.as_slice())
                },
            )
        })
    }

    pub(crate) fn into_entries(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.entries
            .into_iter()
            .map(|(kind, k, v)| (k, if kind == KIND_DELETE { None } else { Some(v) }))
            .collect()
    }

    fn encode(&self, seq: u64) -> Vec<u8> {
        let mut payload = Vec::with_capacity(64);
        put_u64(&mut payload, seq);
        put_u32(&mut payload, self.entries.len() as u32);
        for (kind, key, value) in &self.entries {
            payload.push(*kind);
            put_u32(&mut payload, key.len() as u32);
            payload.extend_from_slice(key);
            put_u32(&mut payload, value.len() as u32);
            payload.extend_from_slice(value);
        }

        let mut record = Vec::with_capacity(payload.len() + HEADER_SIZE);
        let mut body = Vec::with_capacity(payload.len() + 4);
        put_u32(&mut body, payload.len() as u32);
        body.extend_from_slice(&payload);
        put_u32(&mut record, crc32c(&body));
        record.extend_from_slice(&body);
        record
    }
}

/// Appends records to one log file.
pub struct WalWriter {
    file: BufWriter<File>,
    sync_mode: SyncMode,
    bytes: u64,
}

impl WalWriter {
    pub fn create(path: &Path, sync_mode: SyncMode) -> Result<WalWriter> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let bytes = file.metadata()?.len();
        Ok(WalWriter {
            file: BufWriter::with_capacity(1 << 16, file),
            sync_mode,
            bytes,
        })
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes
    }

    /// Appends a batch and, in `EveryWrite` mode, makes it durable before
    /// returning. The caller may only acknowledge the write after this returns.
    pub fn append(&mut self, batch: &WriteBatch, seq: u64) -> Result<()> {
        let record = batch.encode(seq);
        self.file.write_all(&record)?;
        self.bytes += record.len() as u64;
        self.file.flush()?;
        if self.sync_mode == SyncMode::EveryWrite {
            self.file.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Forces everything written so far to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }
}

/// One replayed batch.
pub struct RecoveredBatch {
    pub seq: u64,
    pub entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

pub struct Recovery {
    pub batches: Vec<RecoveredBatch>,
    /// True when the log ended in a partially written record, i.e. the process
    /// died mid-append. Expected after a crash, and not an error.
    pub truncated_tail: bool,
}

/// Replays a log file, stopping at the first record that fails to verify.
pub fn replay(path: &Path) -> Result<Recovery> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let mut batches = Vec::new();
    let mut off = 0usize;
    let mut truncated_tail = false;

    while off < buf.len() {
        if off + HEADER_SIZE > buf.len() {
            truncated_tail = true;
            break;
        }
        let stored_crc = get_u32(&buf, off)?;
        let len = get_u32(&buf, off + 4)? as usize;
        let body_start = off + 4;
        let payload_start = off + HEADER_SIZE;
        if payload_start + len > buf.len() {
            truncated_tail = true;
            break;
        }
        let actual_crc = crc32c(&buf[body_start..payload_start + len]);
        if actual_crc != stored_crc {
            // Either a torn tail or bit rot. Either way we must not replay it.
            truncated_tail = true;
            break;
        }
        batches.push(decode_payload(&buf[payload_start..payload_start + len])?);
        off = payload_start + len;
    }

    Ok(Recovery {
        batches,
        truncated_tail,
    })
}

fn decode_payload(payload: &[u8]) -> Result<RecoveredBatch> {
    let seq = get_u64(payload, 0)?;
    let count = get_u32(payload, 8)? as usize;
    let mut off = 12;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if off >= payload.len() {
            return corrupt("write batch ended early");
        }
        let kind = payload[off];
        off += 1;
        let klen = get_u32(payload, off)? as usize;
        off += 4;
        if off + klen > payload.len() {
            return corrupt("key runs past the end of the record");
        }
        let key = payload[off..off + klen].to_vec();
        off += klen;
        let vlen = get_u32(payload, off)? as usize;
        off += 4;
        if off + vlen > payload.len() {
            return corrupt("value runs past the end of the record");
        }
        let value = payload[off..off + vlen].to_vec();
        off += vlen;
        entries.push((
            key,
            if kind == KIND_DELETE {
                None
            } else {
                Some(value)
            },
        ));
    }
    Ok(RecoveredBatch { seq, entries })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Seek;

    fn tmp(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lsmkv-wal-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("1.wal")
    }

    fn write_some(path: &Path, n: u64) {
        let mut w = WalWriter::create(path, SyncMode::EveryWrite).unwrap();
        for i in 0..n {
            let mut b = WriteBatch::new();
            b.put(format!("key{i}"), format!("value{i}"));
            if i % 3 == 0 {
                b.delete(format!("old{i}"));
            }
            w.append(&b, i).unwrap();
        }
    }

    #[test]
    fn replay_returns_everything_appended() {
        let path = tmp("replay");
        write_some(&path, 100);
        let rec = replay(&path).unwrap();
        assert!(!rec.truncated_tail);
        assert_eq!(rec.batches.len(), 100);
        assert_eq!(rec.batches[0].seq, 0);
        assert_eq!(rec.batches[0].entries[0].0, b"key0");
        assert_eq!(
            rec.batches[0].entries[1].1, None,
            "delete must replay as a tombstone"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn torn_tail_is_dropped_and_the_rest_survives() {
        let path = tmp("torn");
        write_some(&path, 50);
        let len = std::fs::metadata(&path).unwrap().len();
        // Simulate a power loss part-way through the final append.
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 7).unwrap();
        drop(f);

        let rec = replay(&path).unwrap();
        assert!(rec.truncated_tail);
        assert_eq!(
            rec.batches.len(),
            49,
            "only the half-written record is lost"
        );
    }

    #[test]
    fn bit_rot_is_caught_by_the_checksum() {
        let path = tmp("rot");
        write_some(&path, 20);
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(std::io::SeekFrom::Start(40)).unwrap();
            f.write_all(&[0x5a]).unwrap();
        }
        let rec = replay(&path).unwrap();
        assert!(rec.truncated_tail);
        assert!(
            rec.batches.len() < 20,
            "a corrupt record must not be replayed"
        );
    }

    #[test]
    fn empty_log_replays_cleanly() {
        let path = tmp("empty");
        File::create(&path).unwrap();
        let rec = replay(&path).unwrap();
        assert!(rec.batches.is_empty());
        assert!(!rec.truncated_tail);
    }
}
