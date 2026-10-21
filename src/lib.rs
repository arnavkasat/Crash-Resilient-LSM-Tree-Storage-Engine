//! # lsmkv
//!
//! A crash-resilient log-structured merge-tree key-value storage engine.
//!
//! ```text
//!      put/delete                         get
//!          |                               |
//!          v                               v
//!   +--------------+  (1) append     +-----------+
//!   | write-ahead  |<----------------| MemTable  |  sorted, in memory
//!   |     log      |  (2) apply      +-----------+
//!   +--------------+                       |  flush
//!          ^                                v
//!          |                         +-------------+
//!     replay on open                 | L0 SSTables |  may overlap
//!                                    +-------------+
//!                                          | compaction
//!                                          v
//!                                    +-------------+
//!                                    | L1..L6      |  sorted, disjoint
//!                                    +-------------+
//! ```
//!
//! Every write goes to the log first and is `fsync`ed before it is
//! acknowledged, so a crash can only ever lose writes that had not returned.
//! Reads consult the MemTable, then the sealed MemTable, then each level in
//! turn; a Bloom filter and a sparse index per SSTable keep a miss down to
//! roughly one block read, and an LRU block cache serves repeat reads from
//! memory.
//!
//! ## Example
//!
//! ```no_run
//! use lsmkv::{Db, Options};
//!
//! # fn main() -> lsmkv::Result<()> {
//! let db = Db::open("/tmp/lsmkv-example", Options::default())?;
//! db.put("user:1", "ada")?;
//! assert_eq!(db.get(b"user:1")?, Some(b"ada".to_vec()));
//! db.delete("user:1")?;
//! assert_eq!(db.get(b"user:1")?, None);
//! # Ok(())
//! # }
//! ```

pub mod bloom;
pub mod cache;
pub mod crc;
pub mod db;
pub mod error;
pub mod memtable;
pub mod merge;
pub mod options;
pub mod sst;
pub mod table_cache;
pub mod testkit;
pub mod util;
pub mod version;
pub mod wal;

pub use db::{Db, Stats, StatsSnapshot};
pub use error::{Error, Result};
pub use options::{Options, SyncMode};
pub use wal::WriteBatch;
