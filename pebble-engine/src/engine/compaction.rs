use std::path::{Path, PathBuf};

use crate::engine::error::EngineError;
use crate::engine::memtable::MemValue;
use crate::engine::metrics::SharedMetrics;
use crate::engine::sstable::SSTable;

/// Compaction strategy: merge the oldest N SSTables into one.
/// Drops tombstones that are safe to remove (no older SSTable can have the key).
/// Returns the new merged SSTable and the paths of the files it replaced.
pub fn compact(
    sstables: &mut Vec<SSTable>,  // index 0 = newest
    dir: &Path,
    next_seq: u64,
    metrics: SharedMetrics,
) -> Result<Option<CompactionResult>, EngineError> {
    // Need at least 2 SSTables to compact
    if sstables.len() < 2 {
        return Ok(None);
    }

    // Compact the oldest 2 SSTables (last two in the vec)
    // Generalize: compact everything beyond index 0 if you want full compaction.
    // For now: take the two oldest — this keeps write amplification bounded.
    let num_to_compact = sstables.len().min(4); // compact up to 4 at once
    let start_idx = sstables.len() - num_to_compact;

    eprintln!(
        "[Compaction] Merging {} SSTables (oldest {} files)",
        num_to_compact, num_to_compact
    );

    // Collect paths before we consume the SSTables
    let consumed_paths: Vec<PathBuf> = sstables[start_idx..]
        .iter()
        .map(|s| s.path.clone())
        .collect();

    // ── Merge entries: oldest first, newer entries win ───────────────────────
    // Process oldest → newest so that newer values overwrite older ones.
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<Vec<u8>, MemValue> = BTreeMap::new();

    for sst in sstables[start_idx..].iter_mut().rev() {
        // scan entire SSTable (start = empty, end = max possible key)
        let all_entries = sst.scan(b"", b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")?;
        for (key, val) in all_entries {
            merged.entry(key).or_insert(val);
        }
    }

    // ── Drop tombstones safely ───────────────────────────────────────────────
    // A tombstone is safe to drop only when we're compacting ALL SSTables —
    // meaning no older file can still have a live version of this key.
    // If we're compacting everything (start_idx == 0), tombstones can go.
    let drop_tombstones = start_idx == 0;

    let final_entries: Vec<(Vec<u8>, MemValue)> = merged
        .into_iter()
        .filter(|(_, v)| {
            if drop_tombstones {
                matches!(v, MemValue::Value(_))
            } else {
                true // keep tombstones if older SSTables still exist
            }
        })
        .collect();

    eprintln!(
        "[Compaction] Merged into {} entries (tombstones dropped: {})",
        final_entries.len(),
        drop_tombstones
    );

    if final_entries.is_empty() {
        // All entries were tombstones — nothing to write
        return Ok(Some(CompactionResult {
            new_sst: None,
            consumed_paths,
            entries_written: 0,
        }));
    }

    // ── Write new SSTable ────────────────────────────────────────────────────
    let new_path = dir.join(format!("sst_{:08}.sst", next_seq));
    let new_sst = SSTable::flush(&new_path, final_entries.clone())?;

    // ── Update metrics ───────────────────────────────────────────────────────
    {
        let mut m = metrics.lock().unwrap();
        m.compaction_count += 1;
    }

    eprintln!(
        "[Compaction] Wrote {:?} ({} bytes)",
        new_path.file_name().unwrap(),
        new_sst.file_size()
    );

    Ok(Some(CompactionResult {
        new_sst: Some(new_sst),
        consumed_paths,
        entries_written: final_entries.len(),
    }))
}

pub struct CompactionResult {
    pub new_sst: Option<SSTable>,
    pub consumed_paths: Vec<PathBuf>,
    pub entries_written: usize,
}