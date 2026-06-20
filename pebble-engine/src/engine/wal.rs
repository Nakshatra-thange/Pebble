use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::error::EngineError;
use crate::engine::format::{encode_wal_record, decode_wal_record, OP_PUT, OP_DELETE};
use crate::engine::metrics::SharedMetrics;

/// A single decoded entry replayed from the WAL
#[derive(Debug, Clone)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Wal {
    path: PathBuf,
    file: File,
    metrics: SharedMetrics,
}

impl Wal {
    /// Open (or create) a WAL file at the given path.
    pub fn open(path: impl AsRef<Path>, metrics: SharedMetrics) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let wal = Wal { path, file, metrics };
        wal.sync_size_to_metrics();
        Ok(wal)
    }

    /// Append a Put record, fsync before returning.
    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let record = encode_wal_record(key, value, OP_PUT);
        self.write_record(&record)
    }

    /// Append a Delete record, fsync before returning.
    pub fn append_delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        let record = encode_wal_record(key, b"", OP_DELETE);
        self.write_record(&record)
    }

    fn write_record(&mut self, record: &[u8]) -> Result<(), EngineError> {
        self.file.write_all(record)?;
        // fsync: this is the durability guarantee — we don't ack until it's on disk
        self.file.sync_all()?;
        self.sync_size_to_metrics();
        Ok(())
    }

    /// Replay the entire WAL, returning entries in order.
    /// Stops at the first corrupt/truncated record (torn-write safe).
    /// Records recovery duration into metrics.
    pub fn replay(&mut self) -> Result<Vec<WalEntry>, EngineError> {
        let start = std::time::Instant::now();

        self.file.seek(SeekFrom::Start(0))?;
        let mut raw = Vec::new();
        self.file.read_to_end(&mut raw)?;

        let mut entries = Vec::new();
        let mut cursor = 0usize;

        loop {
            if cursor >= raw.len() {
                break;
            }

            match decode_wal_record(&raw[cursor..]) {
                Some((key, value, op, consumed)) => {
                    let entry = if op == OP_PUT {
                        WalEntry::Put { key, value }
                    } else {
                        WalEntry::Delete { key }
                    };
                    entries.push(entry);
                    cursor += consumed;
                }
                None => {
                    // Torn write or truncated record — stop here, discard the tail
                    eprintln!(
                        "[WAL] Stopping replay at byte {} / {} — corrupt or truncated record",
                        cursor,
                        raw.len()
                    );
                    // Truncate the file to the last valid position so the tail is gone
                    self.file.set_len(cursor as u64)?;
                    self.file.seek(SeekFrom::End(0))?;
                    break;
                }
            }
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            let mut m = self.metrics.lock().unwrap();
            m.recovery_time_ms = elapsed_ms;
        }
        self.sync_size_to_metrics();

        Ok(entries)
    }

    /// Delete the WAL file — called after a successful memtable flush to disk.
    /// The flushed SSTable is the new source of truth; WAL is no longer needed.
    pub fn truncate(&mut self) -> Result<(), EngineError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        self.sync_size_to_metrics();
        Ok(())
    }

    pub fn size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    fn sync_size_to_metrics(&self) {
        let size = self.size_bytes();
        if let Ok(mut m) = self.metrics.lock() {
            m.wal_size_bytes = size;
        }
    }
}
