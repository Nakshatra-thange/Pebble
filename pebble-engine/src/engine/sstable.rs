use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::error::EngineError;
use crate::engine::format::{
    decode_u32, decode_u64, encode_index_entry, encode_sstable_record, encode_u64, OP_DELETE,
    OP_PUT,
};
use crate::engine::memtable::MemValue;

/// One entry from a sparse index — a key and the file offset of its record
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

/// A read handle to an SSTable file on disk
pub struct SSTable {
    pub path: PathBuf,
    pub index: Vec<IndexEntry>, // sparse index loaded into memory at open
    file: File,
}

impl SSTable {
    /// Flush a sorted list of (key, MemValue) pairs to a new SSTable file.
    /// Writes: data records → sparse index → footer (index_offset, bloom_offset=0 for now)
    /// Returns an open SSTable ready for reads.
    pub fn flush(
        path: impl AsRef<Path>,
        entries: Vec<(Vec<u8>, MemValue)>,
    ) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        let mut sparse_index: Vec<IndexEntry> = Vec::new();
        let mut current_offset: u64 = 0;

        // ── Write data records ──────────────────────────────────────────────
        // Every Nth key gets an index entry (sparse = every 16th key here).
        // This keeps the index small while still letting us seek close to any key.
        const INDEX_STRIDE: usize = 16;

        for (i, (key, mem_val)) in entries.iter().enumerate() {
            if i % INDEX_STRIDE == 0 {
                sparse_index.push(IndexEntry {
                    key: key.clone(),
                    offset: current_offset,
                });
            }

            let (op, value) = match mem_val {
                MemValue::Value(v) => (OP_PUT, v.as_slice()),
                MemValue::Tombstone => (OP_DELETE, b"" as &[u8]),
            };

            let record = encode_sstable_record(key, value, op);
            file.write_all(&record)?;
            current_offset += record.len() as u64;
        }

        // ── Write sparse index ──────────────────────────────────────────────
        let index_offset = current_offset;
        for entry in &sparse_index {
            let encoded = encode_index_entry(&entry.key, entry.offset);
            file.write_all(&encoded)?;
        }

        // ── Write footer (16 bytes) ─────────────────────────────────────────
        // bloom_offset = 0 for now (Day 5 will fill this in)
        file.write_all(&encode_u64(index_offset))?;
        file.write_all(&encode_u64(0u64))?; // bloom placeholder

        file.sync_all()?;

        Ok(SSTable {
            path,
            index: sparse_index,
            file,
        })
    }

    /// Open an existing SSTable file and load its sparse index into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;

        // ── Read footer ─────────────────────────────────────────────────────
        let file_len = file.seek(SeekFrom::End(0))?;
        if file_len < 16 {
            return Err(EngineError::Corruption(format!(
                "SSTable too small: {} bytes",
                file_len
            )));
        }

        file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;
        let index_offset = decode_u64(&footer[0..8]);
        // bloom_offset = footer[8..16] — used on Day 5

        // ── Read sparse index ───────────────────────────────────────────────
        file.seek(SeekFrom::Start(index_offset))?;
        let index_end = file_len - 16;
        let index_bytes = (index_end - index_offset) as usize;

        let mut index_buf = vec![0u8; index_bytes];
        file.read_exact(&mut index_buf)?;

        let mut sparse_index: Vec<IndexEntry> = Vec::new();
        let mut cursor = 0usize;

        while cursor + 4 <= index_buf.len() {
            let key_len = decode_u32(&index_buf[cursor..]) as usize;
            cursor += 4;
            if cursor + key_len + 8 > index_buf.len() {
                break;
            }
            let key = index_buf[cursor..cursor + key_len].to_vec();
            cursor += key_len;
            let offset = decode_u64(&index_buf[cursor..]);
            cursor += 8;
            sparse_index.push(IndexEntry { key, offset });
        }

        Ok(SSTable {
            path,
            index: sparse_index,
            file,
        })
    }

    /// Look up a key. Returns Some(MemValue) if found, None if not present.
    /// Uses the sparse index to seek close, then scans forward linearly.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<MemValue>, EngineError> {
        // Find the last index entry whose key <= target key
        let seek_offset = self.index_seek(key);

        // Read the index_offset from footer to know where data ends
        let file_len = self.file.seek(SeekFrom::End(0))?;
        let data_end = self.data_end_offset(file_len)?;

        self.file.seek(SeekFrom::Start(seek_offset))?;

        loop {
            let pos = self.file.seek(SeekFrom::Current(0))?;
            if pos >= data_end {
                break;
            }

            let (rec_key, rec_val, op, _) = match self.read_one_record()? {
                Some(r) => r,
                None => break,
            };

            if rec_key.as_slice() == key {
                return Ok(Some(if op == OP_PUT {
                    MemValue::Value(rec_val)
                } else {
                    MemValue::Tombstone
                }));
            }

            // Keys are sorted — if we've passed the target, it's not here
            if rec_key.as_slice() > key {
                break;
            }
        }

        Ok(None)
    }

    /// Range scan — returns all entries with key in [start, end).
    /// Caller merges results from multiple SSTables.
    pub fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, MemValue)>, EngineError> {
        let seek_offset = self.index_seek(start);
        let file_len = self.file.seek(SeekFrom::End(0))?;
        let data_end = self.data_end_offset(file_len)?;

        self.file.seek(SeekFrom::Start(seek_offset))?;
        let mut results = Vec::new();

        loop {
            let pos = self.file.seek(SeekFrom::Current(0))?;
            if pos >= data_end {
                break;
            }

            let (rec_key, rec_val, op, _) = match self.read_one_record()? {
                Some(r) => r,
                None => break,
            };

            if rec_key.as_slice() >= end {
                break;
            }

            if rec_key.as_slice() >= start {
                let val = if op == OP_PUT {
                    MemValue::Value(rec_val)
                } else {
                    MemValue::Tombstone
                };
                results.push((rec_key, val));
            }
        }

        Ok(results)
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Find the file offset to seek to when looking for `key`.
    /// Returns the offset of the last index entry whose key <= target,
    /// or 0 if target is before the first index entry.
    fn index_seek(&self, key: &[u8]) -> u64 {
        let mut best_offset = 0u64;
        for entry in &self.index {
            if entry.key.as_slice() <= key {
                best_offset = entry.offset;
            } else {
                break;
            }
        }
        best_offset
    }

    /// Returns the byte offset where the data section ends (= index_offset).
    fn data_end_offset(&mut self, file_len: u64) -> Result<u64, EngineError> {
        if file_len < 16 {
            return Err(EngineError::Corruption("SSTable too small".into()));
        }
        self.file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 8];
        self.file.read_exact(&mut footer)?;
        Ok(decode_u64(&footer))
    }

    /// Read one record from the current file cursor.
    /// Returns (key, value, op, bytes_consumed) or None at EOF/corrupt.
    fn read_one_record(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>, u8, usize)>, EngineError> {
        let mut header = [0u8; 9]; // key_len(4) + val_len(4) + op(1)
        match self.file.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let key_len = decode_u32(&header[0..4]) as usize;
        let val_len = decode_u32(&header[4..8]) as usize;
        let op = header[8];

        let mut key = vec![0u8; key_len];
        let mut val = vec![0u8; val_len];
        self.file.read_exact(&mut key)?;
        if val_len > 0 {
            self.file.read_exact(&mut val)?;
        }

        let consumed = 9 + key_len + val_len;
        Ok(Some((key, val, op, consumed)))
    }

    pub fn file_size(&self) -> u64 {
        std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0)
    }
}