//! Sorted String Tables: the immutable, sorted files that hold everything a
//! MemTable has flushed.
//!
//! ```text
//! +--------------------+
//! | data block 0       |  prefix-compressed entries + restart array
//! | data block 1       |
//! | ...                |
//! +--------------------+
//! | bloom filter block |  one filter over every key in the file
//! +--------------------+
//! | index block        |  sparse: one entry per data block (last key -> handle)
//! +--------------------+
//! | footer (40 bytes)  |  handles for the two blocks above + magic
//! +--------------------+
//! ```
//!
//! Each block is followed by a 5-byte trailer (1 type byte + CRC-32C), so a
//! torn or bit-rotted block is detected on read rather than served silently.

pub mod block;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::bloom::{BloomBuilder, BloomFilter};
use crate::cache::BlockCache;
use crate::crc::crc32c;
use crate::error::{corrupt, Error, Result};
use crate::options::Options;
use crate::util::{get_u32, get_u64, put_u32, put_u64, Entry};
use block::{Block, BlockBuilder, BlockIter, DEFAULT_RESTART_INTERVAL};

const BLOCK_TRAILER_SIZE: usize = 5;
const FOOTER_SIZE: usize = 40;
const MAGIC: u64 = 0x4c53_4d4b_5600_0001;

#[derive(Clone, Copy, Debug)]
struct BlockHandle {
    offset: u64,
    size: u32,
}

impl BlockHandle {
    fn encode(&self, buf: &mut Vec<u8>) {
        put_u64(buf, self.offset);
        put_u32(buf, self.size);
    }

    fn decode(buf: &[u8]) -> Result<BlockHandle> {
        if buf.len() < 12 {
            return corrupt("short block handle");
        }
        Ok(BlockHandle {
            offset: get_u64(buf, 0)?,
            size: get_u32(buf, 8)?,
        })
    }
}

/// Writes one SSTable to disk.
pub struct TableBuilder {
    file: BufWriter<File>,
    offset: u64,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    /// `None` when filters are switched off (`bloom_bits_per_key == 0`).
    bloom: Option<BloomBuilder>,
    block_size: usize,
    num_entries: u64,
    smallest: Option<Vec<u8>>,
    largest: Vec<u8>,
    pending_handle: Option<(Vec<u8>, BlockHandle)>,
}

impl TableBuilder {
    pub fn create(path: &Path, opts: &Options) -> Result<TableBuilder> {
        let file = File::create(path)?;
        Ok(TableBuilder {
            file: BufWriter::with_capacity(1 << 16, file),
            offset: 0,
            data_block: BlockBuilder::new(DEFAULT_RESTART_INTERVAL),
            index_block: BlockBuilder::new(1),
            bloom: (opts.bloom_bits_per_key > 0)
                .then(|| BloomBuilder::new(opts.bloom_bits_per_key)),
            block_size: opts.block_size,
            num_entries: 0,
            smallest: None,
            largest: Vec::new(),
            pending_handle: None,
        })
    }

    /// Appends one entry. Keys must be added in ascending order.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if let Some((last_key, handle)) = self.pending_handle.take() {
            // Deferred so the index key can be any separator in
            // [last key of the block, first key of the next block).
            let mut buf = Vec::with_capacity(12);
            handle.encode(&mut buf);
            self.index_block.add(&last_key, Some(&buf));
        }
        if self.smallest.is_none() {
            self.smallest = Some(key.to_vec());
        }
        self.largest.clear();
        self.largest.extend_from_slice(key);

        if let Some(bloom) = &mut self.bloom {
            bloom.add(key);
        }
        self.data_block.add(key, value);
        self.num_entries += 1;

        if self.data_block.estimated_size() >= self.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Bytes written so far; compaction uses this to decide when to roll over
    /// to a new output file.
    pub fn file_size(&self) -> u64 {
        self.offset + self.data_block.estimated_size() as u64
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let last_key = self.data_block.last_key().to_vec();
        let handle = {
            let contents = self.data_block.finish();
            write_block(&mut self.file, &mut self.offset, contents)?
        };
        self.data_block.reset();
        self.pending_handle = Some((last_key, handle));
        Ok(())
    }

    /// Flushes everything and fsyncs the file. Returns (file size, smallest
    /// key, largest key).
    pub fn finish(mut self) -> Result<(u64, Vec<u8>, Vec<u8>)> {
        self.flush_data_block()?;
        if let Some((last_key, handle)) = self.pending_handle.take() {
            let mut buf = Vec::with_capacity(12);
            handle.encode(&mut buf);
            self.index_block.add(&last_key, Some(&buf));
        }

        // With filters off, a one-byte placeholder keeps the layout fixed and
        // decodes back to "no filter".
        let bloom_bytes = self
            .bloom
            .as_ref()
            .map_or_else(|| vec![0u8], |b| b.finish());
        let bloom_handle = write_raw(&mut self.file, &mut self.offset, &bloom_bytes)?;

        let index_handle = {
            let contents = self.index_block.finish();
            write_block(&mut self.file, &mut self.offset, contents)?
        };

        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        put_u64(&mut footer, bloom_handle.offset);
        put_u32(&mut footer, bloom_handle.size);
        put_u64(&mut footer, index_handle.offset);
        put_u32(&mut footer, index_handle.size);
        put_u64(&mut footer, self.num_entries);
        put_u64(&mut footer, MAGIC);
        debug_assert_eq!(footer.len(), FOOTER_SIZE);
        self.file.write_all(&footer)?;
        self.offset += FOOTER_SIZE as u64;

        self.file.flush()?;
        let file = self
            .file
            .into_inner()
            .map_err(|e| Error::Io(e.into_error()))?;
        // The table must be durable before the manifest can point at it.
        file.sync_all()?;

        let smallest = self.smallest.unwrap_or_default();
        Ok((self.offset, smallest, self.largest))
    }
}

fn write_block(
    file: &mut BufWriter<File>,
    offset: &mut u64,
    contents: &[u8],
) -> Result<BlockHandle> {
    write_raw(file, offset, contents)
}

fn write_raw(file: &mut BufWriter<File>, offset: &mut u64, contents: &[u8]) -> Result<BlockHandle> {
    let handle = BlockHandle {
        offset: *offset,
        size: contents.len() as u32,
    };
    file.write_all(contents)?;
    let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
    trailer[0] = 0; // no compression
    let crc = crc32c(&[contents, &trailer[..1]].concat());
    trailer[1..].copy_from_slice(&crc.to_le_bytes());
    file.write_all(&trailer)?;
    *offset += contents.len() as u64 + BLOCK_TRAILER_SIZE as u64;
    Ok(handle)
}

/// Reads one SSTable. Cheap to clone around behind an `Arc`.
pub struct Table {
    file: File,
    file_number: u64,
    cache: Arc<BlockCache>,
    index: Arc<Block>,
    bloom: Option<BloomFilter>,
    num_entries: u64,
}

impl Table {
    pub fn open(path: &Path, file_number: u64, cache: Arc<BlockCache>) -> Result<Table> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_SIZE as u64 {
            return corrupt(format!("{} is too small to be an SSTable", path.display()));
        }
        let mut footer = [0u8; FOOTER_SIZE];
        file.read_exact_at(&mut footer, len - FOOTER_SIZE as u64)?;
        if get_u64(&footer, 32)? != MAGIC {
            return corrupt(format!("{} has a bad magic number", path.display()));
        }
        let bloom_handle = BlockHandle {
            offset: get_u64(&footer, 0)?,
            size: get_u32(&footer, 8)?,
        };
        let index_handle = BlockHandle {
            offset: get_u64(&footer, 12)?,
            size: get_u32(&footer, 20)?,
        };
        let num_entries = get_u64(&footer, 24)?;

        let index_bytes = read_handle(&file, index_handle)?;
        let index = Arc::new(Block::decode(index_bytes)?);
        let bloom = BloomFilter::decode(&read_handle(&file, bloom_handle)?);

        Ok(Table {
            file,
            file_number,
            cache,
            index,
            bloom,
            num_entries,
        })
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    fn read_block(&self, handle: BlockHandle) -> Result<Arc<Block>> {
        let key = (self.file_number, handle.offset);
        if let Some(block) = self.cache.get(key) {
            return Ok(block);
        }
        let block = Arc::new(Block::decode(read_handle(&self.file, handle)?)?);
        self.cache.insert(key, block.clone());
        Ok(block)
    }

    /// Point lookup.
    ///
    /// * `Ok(None)` - the key is not in this file.
    /// * `Ok(Some(None))` - the file holds a tombstone for the key.
    /// * `Ok(Some(Some(v)))` - the file holds value `v`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }
        let mut index_iter = self.index.seek(key)?;
        let handle = match index_iter.next()? {
            Some((_, Some(v))) => BlockHandle::decode(&v)?,
            _ => return Ok(None), // key sorts after every block in this file
        };
        self.read_block(handle)?.get(key)
    }

    /// Cursor over every entry in the file, in key order.
    pub fn iter(self: &Arc<Self>) -> TableIter {
        TableIter {
            table: self.clone(),
            index_iter: self.index.iter(),
            data_iter: None,
            done: false,
        }
    }

    /// Cursor positioned at the first key >= `target`.
    pub fn seek(self: &Arc<Self>, target: &[u8]) -> Result<TableIter> {
        let mut index_iter = self.index.seek(target)?;
        let data_iter = match index_iter.next()? {
            Some((_, Some(v))) => Some(self.read_block(BlockHandle::decode(&v)?)?.seek(target)?),
            _ => None,
        };
        Ok(TableIter {
            table: self.clone(),
            index_iter,
            data_iter,
            done: false,
        })
    }
}

fn read_handle(file: &File, handle: BlockHandle) -> Result<Vec<u8>> {
    let total = handle.size as usize + BLOCK_TRAILER_SIZE;
    let mut buf = vec![0u8; total];
    file.read_exact_at(&mut buf, handle.offset)?;
    let stored = get_u32(&buf, handle.size as usize + 1)?;
    let actual = crc32c(&buf[..handle.size as usize + 1]);
    if stored != actual {
        return corrupt(format!(
            "block at offset {} failed its checksum (stored {stored:#x}, computed {actual:#x})",
            handle.offset
        ));
    }
    buf.truncate(handle.size as usize);
    Ok(buf)
}

/// Streaming cursor over an SSTable, used by compaction and range scans.
pub struct TableIter {
    table: Arc<Table>,
    index_iter: BlockIter,
    data_iter: Option<BlockIter>,
    done: bool,
}

impl TableIter {
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Entry>> {
        loop {
            if self.done {
                return Ok(None);
            }
            if let Some(it) = &mut self.data_iter {
                if let Some(entry) = it.next()? {
                    return Ok(Some(entry));
                }
            }
            match self.index_iter.next()? {
                Some((_, Some(v))) => {
                    let handle = BlockHandle::decode(&v)?;
                    self.data_iter = Some(self.table.read_block(handle)?.iter());
                }
                _ => {
                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lsmkv-sst-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn build(path: &Path, n: usize) -> Options {
        let opts = Options {
            block_size: 512,
            ..Default::default()
        };
        let mut b = TableBuilder::create(path, &opts).unwrap();
        for i in 0..n {
            let key = format!("key{i:08}");
            if i % 7 == 0 {
                b.add(key.as_bytes(), None).unwrap();
            } else {
                b.add(key.as_bytes(), Some(format!("value{i}").as_bytes()))
                    .unwrap();
            }
        }
        b.finish().unwrap();
        opts
    }

    #[test]
    fn roundtrip_get_and_iterate() {
        let dir = tmpdir("roundtrip");
        let path = dir.join("1.sst");
        build(&path, 2000);
        let table = Arc::new(Table::open(&path, 1, Arc::new(BlockCache::new(1 << 20))).unwrap());
        assert_eq!(table.num_entries(), 2000);

        for i in 0..2000 {
            let key = format!("key{i:08}");
            let got = table.get(key.as_bytes()).unwrap().expect("key missing");
            if i % 7 == 0 {
                assert_eq!(got, None, "expected tombstone for {i}");
            } else {
                assert_eq!(got.unwrap(), format!("value{i}").into_bytes());
            }
        }
        assert!(table.get(b"key99999999").unwrap().is_none());
        assert!(table.get(b"aaa").unwrap().is_none());

        let mut iter = table.iter();
        let mut count = 0;
        let mut last: Option<Vec<u8>> = None;
        while let Some((k, _)) = iter.next().unwrap() {
            if let Some(prev) = &last {
                assert!(&k > prev, "iteration is not sorted");
            }
            last = Some(k);
            count += 1;
        }
        assert_eq!(count, 2000);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn seek_positions_correctly() {
        let dir = tmpdir("seek");
        let path = dir.join("1.sst");
        build(&path, 500);
        let table = Arc::new(Table::open(&path, 1, Arc::new(BlockCache::new(1 << 20))).unwrap());
        let mut it = table.seek(b"key00000250").unwrap();
        assert_eq!(it.next().unwrap().unwrap().0, b"key00000250");
        let mut it = table.seek(b"key00000250x").unwrap();
        assert_eq!(it.next().unwrap().unwrap().0, b"key00000251");
        let mut it = table.seek(b"zzz").unwrap();
        assert!(it.next().unwrap().is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupted_block_is_detected() {
        let dir = tmpdir("corrupt");
        let path = dir.join("1.sst");
        build(&path, 500);
        // Flip a byte in the first data block.
        {
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.write_at(&[0xff], 20).unwrap();
            f.flush().unwrap();
        }
        let table = Arc::new(Table::open(&path, 1, Arc::new(BlockCache::new(1 << 20))).unwrap());
        // Some key in the damaged block must now report corruption rather than
        // returning wrong data.
        let mut saw_error = false;
        for i in 0..50 {
            if table.get(format!("key{i:08}").as_bytes()).is_err() {
                saw_error = true;
                break;
            }
        }
        assert!(saw_error, "checksum did not catch the flipped byte");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn truncated_file_is_rejected() {
        let dir = tmpdir("truncated");
        let path = dir.join("1.sst");
        build(&path, 500);
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 10).unwrap();
        drop(f);
        assert!(Table::open(&path, 1, Arc::new(BlockCache::new(1 << 20))).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
