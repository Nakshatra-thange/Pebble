use std::path::{Path, PathBuf};
use std::fs;

use crate::engine::error::EngineError;
use crate::engine::memtable::{Memtable, MemValue};
use crate::engine::metrics::{new_shared_metrics, SharedMetrics};
use crate::engine::sstable::SSTable;
use crate::engine::wal::Wal;

const DEFAULT_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024; // 4 MB

pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    memtable: Memtable,
    sstables: Vec<SSTable>, // index 0 = newest
    pub metrics: SharedMetrics,
}

impl Engine {
    /// Open or create an engine at the given directory.
    /// On startup: replays WAL → rebuilds memtable, then opens all SSTables.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, EngineError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let metrics = new_shared_metrics();

        // ── WAL recovery ────────────────────────────────────────────────────
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

        // ── Open existing SSTables (newest first) ───────────────────────────
        let mut sstable_paths = Self::find_sstable_paths(&dir)?;
        // Sort descending by sequence number so index 0 = newest
        sstable_paths.sort_by(|a, b| b.cmp(a));

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

        eprintln!("[Engine] Opened with {} SSTables", sstables.len());

        Ok(Engine {
            dir,
            wal,
            memtable,
            sstables,
            metrics,
        })
    }

    // ── Public API ───────────────────────────────────────────────────────────

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

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        // 1. Check memtable first (newest data)
        match self.memtable.get(key) {
            Some(MemValue::Value(v)) => return Ok(Some(v.clone())),
            Some(MemValue::Tombstone) => return Ok(None), // deleted
            None => {}
        }

        // 2. Check SSTables newest → oldest
        for sst in &mut self.sstables {
            match sst.get(key)? {
                Some(MemValue::Value(v)) => return Ok(Some(v)),
                Some(MemValue::Tombstone) => return Ok(None),
                None => continue,
            }
        }

        Ok(None)
    }

    /// Range scan across memtable + all SSTables.
    /// Merges results: memtable wins over SSTables; newer SSTables win over older.
    pub fn scan(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        use std::collections::BTreeMap;

        // Collect into a map so newer sources overwrite older ones.
        // Process oldest first so newest writes win.
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // SSTables oldest → newest
        for sst in self.sstables.iter_mut().rev() {
            for (k, v) in sst.scan(start, end)? {
                let val = match v {
                    MemValue::Value(val) => Some(val),
                    MemValue::Tombstone => None,
                };
                merged.insert(k, val);
            }
        }

        // Memtable (newest) wins over everything
        for (k, v) in self.memtable.scan(start, end) {
            let val = match v {
                MemValue::Value(val) => Some(val.clone()),
                MemValue::Tombstone => None,
            };
            merged.insert(k.clone(), val);
        }

        // Filter out tombstones, return live values only
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    /// Flush the current memtable to a new SSTable, then truncate the WAL.
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

        // Swap the memtable out, drain into a new SSTable
        let old_memtable = std::mem::replace(
            &mut self.memtable,
            Memtable::new(DEFAULT_FLUSH_THRESHOLD, self.metrics.clone()),
        );
        let entries = old_memtable.drain_sorted();
        let sst = SSTable::flush(&sst_path, entries)?;

        // Insert at front so index 0 = newest
        self.sstables.insert(0, sst);

        // WAL entries are now covered by the SSTable — safe to truncate
        self.wal.truncate()?;

        {
            let mut m = self.metrics.lock().unwrap();
            m.sstable_count = self.sstables.len() as u64;
        }

        eprintln!(
            "[Engine] Flush complete. SSTables on disk: {}",
            self.sstables.len()
        );

        Ok(())
    }

    fn next_sstable_seq(&self) -> u64 {
        // Sequence = max existing + 1, or 1 if none
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