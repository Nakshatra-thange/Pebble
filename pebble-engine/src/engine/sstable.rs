use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::bloom::BloomFilter;
use crate::engine::error::EngineError;
use crate::engine::format::{
    decode_u32, decode_u64, encode_index_entry, encode_sstable_record, encode_u64, OP_DELETE,
    OP_PUT,
};
use crate::engine::memtable::MemValue;

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

pub struct SSTable {
    pub path: PathBuf,
    pub index: Vec<IndexEntry>,
    pub bloom: Option<BloomFilter>,
    file: File,
    key_count: usize,
}

impl SSTable {
    /// Flush sorted entries to a new SSTable file.
    ///
    /// File layout:
    ///   [ data records ][ bloom filter ][ sparse index ][ footer: index_offset(8) + bloom_offset(8) ]
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

        let key_count = entries.len();
        let mut sparse_index: Vec<IndexEntry> = Vec::new();
        let mut current_offset: u64 = 0;
        const INDEX_STRIDE: usize = 16;

        // ── Build bloom filter from all keys ────────────────────────────────
        let mut bloom = BloomFilter::new(key_count, 0.01); // 1% FPR target
        for (key, _) in &entries {
            bloom.insert(key);
        }

        // ── Write data records ───────────────────────────────────────────────
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

        // ── Write bloom filter ───────────────────────────────────────────────
        let bloom_offset = current_offset;
        let bloom_bytes = bloom.encode();
        file.write_all(&bloom_bytes)?;
        current_offset += bloom_bytes.len() as u64;

        // ── Write sparse index ───────────────────────────────────────────────
        let index_offset = current_offset;
        for entry in &sparse_index {
            let encoded = encode_index_entry(&entry.key, entry.offset);
            file.write_all(&encoded)?;
        }

        // ── Write footer ─────────────────────────────────────────────────────
        file.write_all(&encode_u64(index_offset))?;
        file.write_all(&encode_u64(bloom_offset))?;
        file.sync_all()?;

        Ok(SSTable {
            path,
            index: sparse_index,
            bloom: Some(bloom),
            file,
            key_count,
        })
    }

    /// Open an existing SSTable — loads sparse index and bloom filter into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;

        let file_len = file.seek(SeekFrom::End(0))?;
        if file_len < 16 {
            return Err(EngineError::Corruption(format!(
                "SSTable too small: {} bytes",
                file_len
            )));
        }

        // ── Read footer ──────────────────────────────────────────────────────
        file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;
        let index_offset = decode_u64(&footer[0..8]);
        let bloom_offset = decode_u64(&footer[8..16]);

        // ── Load bloom filter ────────────────────────────────────────────────
        let bloom = if bloom_offset > 0 && bloom_offset < index_offset {
            file.seek(SeekFrom::Start(bloom_offset))?;
            let bloom_len = (index_offset - bloom_offset) as usize;
            let mut bloom_buf = vec![0u8; bloom_len];
            file.read_exact(&mut bloom_buf)?;
            BloomFilter::decode(&bloom_buf)
        } else {
            None
        };

        // ── Load sparse index ────────────────────────────────────────────────
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
            bloom,
            file,
            key_count: 0, // unknown when reopening; bloom tracks what matters
        })
    }

    /// Point lookup — checks bloom filter before doing any disk seek.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<MemValue>, EngineError> {
        // ── Bloom filter check ───────────────────────────────────────────────
        // If the filter says "definitely not here", skip this SSTable entirely.
        // Zero disk seeks for misses — this is the whole point.
        if let Some(ref bloom) = self.bloom {
            if !bloom.might_contain(key) {
                return Ok(None); // definite miss — no disk I/O
            }
        }

        // Bloom said "maybe" — do the actual seek
        let seek_offset = self.index_seek(key);
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
            if rec_key.as_slice() > key {
                break;
            }
        }

        Ok(None)
    }

    /// Range scan — bloom filter not applied here (ranges touch many keys)
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

    pub fn file_size(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    pub fn bloom_info(&self) -> Option<(u64, u8)> {
        self.bloom
            .as_ref()
            .map(|b| (b.num_bits(), b.num_hash_fns()))
    }

    pub fn expected_fpr(&self) -> Option<f64> {
        self.bloom
            .as_ref()
            .map(|b| b.expected_fpr(self.key_count))
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

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

    fn data_end_offset(&mut self, file_len: u64) -> Result<u64, EngineError> {
        if file_len < 16 {
            return Err(EngineError::Corruption("SSTable too small".into()));
        }
        // bloom_offset is the end of data (bloom starts right after data)
        self.file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 16];
        self.file.read_exact(&mut footer)?;
        Ok(decode_u64(&footer[8..16])) // bloom_offset = data end
    }

    fn read_one_record(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>, u8, usize)>, EngineError> {
        let mut header = [0u8; 9];
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

        Ok(Some((key, val, op, 9 + key_len + val_len)))
    }
}