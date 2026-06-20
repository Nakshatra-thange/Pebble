use std::collections::BTreeMap;
use crate::engine::error::EngineError;
use crate::engine::metrics::SharedMetrics;

/// A value stored in the memtable — either a live value or a tombstone (delete marker)
#[derive(Debug, Clone)]
pub enum MemValue {
    Value(Vec<u8>),
    Tombstone,
}

pub struct Memtable {
    data: BTreeMap<Vec<u8>, MemValue>,
    /// Approximate size in bytes (keys + values) — used to decide when to flush
    size_bytes: usize,
    /// Flush threshold — when size_bytes exceeds this, caller should flush to SSTable
    pub flush_threshold: usize,
    metrics: SharedMetrics,
}

impl Memtable {
    pub fn new(flush_threshold: usize, metrics: SharedMetrics) -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
            flush_threshold,
            metrics,
        }
    }

    /// Insert or overwrite a key. Returns true if this write made the memtable full.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let added = key.len() + value.len();

        // If we're overwriting, subtract the old size first
        if let Some(old) = self.data.get(&key) {
            let old_size = match old {
                MemValue::Value(v) => key.len() + v.len(),
                MemValue::Tombstone => key.len(),
            };
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }

        self.size_bytes += added;
        self.data.insert(key, MemValue::Value(value));

        {
            let mut m = self.metrics.lock().unwrap();
            m.record_write();
        }

        self.is_full()
    }

    /// Mark a key as deleted. Tombstones must be written so compaction knows
    /// to drop old versions of this key in SSTables.
    pub fn delete(&mut self, key: Vec<u8>) -> bool {
        if let Some(old) = self.data.get(&key) {
            let old_size = match old {
                MemValue::Value(v) => key.len() + v.len(),
                MemValue::Tombstone => key.len(),
            };
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        }

        self.size_bytes += key.len(); // tombstone costs key bytes only
        self.data.insert(key, MemValue::Tombstone);

        {
            let mut m = self.metrics.lock().unwrap();
            m.record_write();
        }

        self.is_full()
    }

    /// Get a value from the memtable.
    /// Returns None if the key was never written.
    /// Returns Some(MemValue::Tombstone) if the key was deleted — callers must
    /// treat this as "definitely deleted", stop searching older SSTables.
    pub fn get(&self, key: &[u8]) -> Option<&MemValue> {
        {
            // Separate scope so we don't hold metrics lock while returning a ref
            let mut m = self.metrics.lock().unwrap();
            m.record_read();
        }
        self.data.get(key)
    }

    /// Range scan — returns all entries between start (inclusive) and end (exclusive),
    /// in sorted order. Includes tombstones — callers handle them.
    pub fn scan<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a MemValue)> {
        self.data
            .range(start.to_vec()..end.to_vec())
    }

    /// True when the memtable has grown past the flush threshold
    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.flush_threshold
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Consume the memtable and return all entries in sorted order.
    /// Called just before flushing to an SSTable.
    pub fn drain_sorted(self) -> Vec<(Vec<u8>, MemValue)> {
        self.data.into_iter().collect()
    }

    /// Rebuild a memtable from WAL replay entries.
    /// This is called on startup after WAL replay.
    pub fn restore_from_wal(
        entries: Vec<crate::engine::wal::WalEntry>,
        flush_threshold: usize,
        metrics: SharedMetrics,
    ) -> Self {
        let mut mem = Memtable::new(flush_threshold, metrics);
        for entry in entries {
            match entry {
                crate::engine::wal::WalEntry::Put { key, value } => {
                    mem.put(key, value);
                }
                crate::engine::wal::WalEntry::Delete { key } => {
                    mem.delete(key);
                }
            }
        }
        mem
    }
}