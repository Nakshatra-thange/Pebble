use std::fs;
use std::path::{Path, PathBuf};

use crate::engine::compaction::compact;
use crate::engine::error::EngineError;
use crate::engine::memtable::{Memtable, MemValue};
use crate::engine::metrics::{new_shared_metrics, SharedMetrics};
use crate::engine::sstable::SSTable;
use crate::engine::wal::Wal;

const DEFAULT_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024; // 4 MB
/// Trigger compaction when SSTable count exceeds this
const COMPACTION_TRIGGER: usize = 4;

pub struct Engine {
    pub dir: PathBuf,
    pub wal: Wal,
    pub memtable: Memtable,
    pub sstables: Vec<SSTable>, // index 0 = newest
    pub metrics: SharedMetrics,
}

impl Engine {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, EngineError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let metrics = new_shared_metrics();

        let wal_path = dir.join("wal.log");
        let mut wal = Wal::open(&wal_path, metrics.clone())?;
        let wal_entries = wal.replay()?;
        let entry_count = wal_entries.len();

        let memtable = Memtable::restore_from_wal(
            wal_entries,
            DEFAULT_FLUSH_THRESHOLD,
            metrics.clone(),
        );

        if entry_count > 0 {
            eprintln!("[Engine] Recovered {} WAL entries into memtable", entry_count);
        }

        let mut sstable_paths = Self::find_sstable_paths(&dir)?;
        sstable_paths.sort_by(|a, b| b.cmp(a)); // descending = newest first

        let mut sstables = Vec::new();
        for path in &sstable_paths {
            match SSTable::open(path) {
                Ok(sst) => sstables.push(sst),
                Err(e) => eprintln!("[Engine] Warning: could not open {:?}: {}", path, e),
            }
        }

        {
            let mut m = metrics.lock().unwrap();
            m.sstable_count = sstables.len() as u64;
        }

        eprintln!(
            "[Engine] Opened: {} SSTables, recovery_time={}ms",
            sstables.len(),
            metrics.lock().unwrap().recovery_time_ms
        );

        Ok(Engine { dir, wal, memtable, sstables, metrics })
    }

    // ── Public API ────────────────────────────────────────────────────────────

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.wal.append_put(key, value)?;
        let needs_flush = self.memtable.put(key.to_vec(), value.to_vec());
        if needs_flush {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.wal.append_delete(key)?;
        let needs_flush = self.memtable.delete(key.to_vec());
        if needs_flush {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// Full read path: memtable → SSTables newest→oldest.
    /// Stops at first hit. Tombstone = deleted, return None.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        // 1. Memtable (newest)
        match self.memtable.get(key) {
            Some(MemValue::Value(v)) => return Ok(Some(v.clone())),
            Some(MemValue::Tombstone) => return Ok(None),
            None => {}
        }

        // 2. SSTables newest → oldest
        // Bloom filter checked inside sst.get() — skips files with no match
        for sst in &mut self.sstables {
            match sst.get(key)? {
                Some(MemValue::Value(v)) => return Ok(Some(v)),
                Some(MemValue::Tombstone) => return Ok(None),
                None => continue,
            }
        }

        Ok(None)
    }

    /// Range scan across memtable + all SSTables, merged newest-wins.
    pub fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        use std::collections::BTreeMap;
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Oldest SSTables first — newer sources will overwrite
        for sst in self.sstables.iter_mut().rev() {
            for (k, v) in sst.scan(start, end)? {
                merged.insert(k, match v {
                    MemValue::Value(val) => Some(val),
                    MemValue::Tombstone => None,
                });
            }
        }

        // Memtable wins over everything
        for (k, v) in self.memtable.scan(start, end) {
            merged.insert(k.clone(), match v {
                MemValue::Value(val) => Some(val.clone()),
                MemValue::Tombstone => None,
            });
        }

        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    // ── Flush + Compaction ────────────────────────────────────────────────────

    pub fn flush_memtable(&mut self) -> Result<(), EngineError> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let seq = self.next_sstable_seq();
        let sst_path = self.dir.join(format!("sst_{:08}.sst", seq));

        eprintln!(
            "[Engine] Flushing memtable ({} bytes) → {:?}",
            self.memtable.size_bytes(),
            sst_path.file_name().unwrap()
        );

        let old_mem = std::mem::replace(
            &mut self.memtable,
            Memtable::new(DEFAULT_FLUSH_THRESHOLD, self.metrics.clone()),
        );
        let entries = old_mem.drain_sorted();
        let sst = SSTable::flush(&sst_path, entries)?;
        self.sstables.insert(0, sst); // newest at front
        self.wal.truncate()?;

        {
            let mut m = self.metrics.lock().unwrap();
            m.sstable_count = self.sstables.len() as u64;
        }

        eprintln!("[Engine] SSTables after flush: {}", self.sstables.len());

        // Trigger compaction if we have too many SSTables
        if self.sstables.len() >= COMPACTION_TRIGGER {
            self.run_compaction()?;
        }

        Ok(())
    }

    pub fn run_compaction(&mut self) -> Result<(), EngineError> {
        let next_seq = self.next_sstable_seq();
        let result = compact(
            &mut self.sstables,
            &self.dir,
            next_seq,
            self.metrics.clone(),
        )?;

        let result = match result {
            Some(r) => r,
            None => return Ok(()),
        };

        // ── Swap in the new SSTable, remove compacted ones ────────────────────
        let num_compacted = result.consumed_paths.len();
        let start_idx = self.sstables.len() - num_compacted;

        // Remove the compacted SSTables from our list
        self.sstables.drain(start_idx..);

        // Insert the new merged SSTable at the correct age position
        if let Some(new_sst) = result.new_sst {
            self.sstables.push(new_sst); // goes at end = "oldest remaining"
        }

        // Delete the old files from disk
        for path in &result.consumed_paths {
            if let Err(e) = fs::remove_file(path) {
                eprintln!("[Compaction] Warning: could not delete {:?}: {}", path, e);
            }
        }

        {
            let mut m = self.metrics.lock().unwrap();
            m.sstable_count = self.sstables.len() as u64;
        }

        eprintln!(
            "[Compaction] Done. {} SSTables → 1. Total now: {}",
            num_compacted,
            self.sstables.len()
        );

        Ok(())
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    fn next_sstable_seq(&self) -> u64 {
        self.sstables
            .iter()
            .filter_map(|sst| {
                sst.path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("sst_"))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0)
            + 1
    }

    fn find_sstable_paths(dir: &Path) -> Result<Vec<PathBuf>, EngineError> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                paths.push(path);
            }
        }
        Ok(paths)
    }
}