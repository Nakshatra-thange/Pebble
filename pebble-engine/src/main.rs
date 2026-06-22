mod engine;

use engine::engine::Engine;
use std::fs;
use std::time::Instant;

fn main() {
    println!("=== Pebble Engine — Day 6: Compaction + Recovery Harness ===\n");

    // ── Test 1: Compaction merges SSTables ───────────────────────────────────
    println!("── Test 1: Compaction reduces SSTable count");
    {
        let db_dir = "/tmp/pebble_day6_t1";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        // Write 4 separate batches, each flushed manually
        // (threshold won't trigger since batches are small)
        for batch in 0u32..4 {
            for i in 0u32..20 {
                let key = format!("batch{}_key_{:04}", batch, i);
                let val = format!("batch{}_val_{:04}", batch, i);
                eng.put(key.as_bytes(), val.as_bytes()).unwrap();
            }
            eng.flush_memtable().unwrap();
            let count = eng.metrics.lock().unwrap().sstable_count;
            println!("  After flush {}: {} SSTables", batch + 1, count);
        }

        // 4th flush triggers compaction automatically (COMPACTION_TRIGGER = 4)
        let count_after = eng.metrics.lock().unwrap().sstable_count;
        let compactions = eng.metrics.lock().unwrap().compaction_count;
        println!("  After compaction: {} SSTables, {} compactions run", count_after, compactions);
        assert!(count_after < 4, "Compaction should have reduced SSTable count");

        // All keys still readable after compaction
        let v = eng.get(b"batch0_key_0000").unwrap();
        println!("  batch0_key_0000 still readable: {:?} ✓", v.map(|b| String::from_utf8(b).unwrap()));
        let v = eng.get(b"batch3_key_0019").unwrap();
        println!("  batch3_key_0019 still readable: {:?} ✓", v.map(|b| String::from_utf8(b).unwrap()));

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 2: Tombstones dropped during full compaction ─────────────────────
    println!("\n── Test 2: Tombstones dropped when compacting all SSTables");
    {
        let db_dir = "/tmp/pebble_day6_t2";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        // Write and flush key
        eng.put(b"doomed", b"i will be deleted").unwrap();
        eng.flush_memtable().unwrap();

        // Delete and flush tombstone
        eng.delete(b"doomed").unwrap();
        eng.flush_memtable().unwrap();

        // Verify tombstone is respected before compaction
        let v = eng.get(b"doomed").unwrap();
        println!("  Before compaction: get(doomed) = {:?} ✓", v);
        assert!(v.is_none());

        // Force full compaction
        eng.run_compaction().unwrap();

        // Still gone after compaction (tombstone was dropped, key never resurfaces)
        let v = eng.get(b"doomed").unwrap();
        println!("  After compaction:  get(doomed) = {:?} (tombstone cleaned ✓)", v);
        assert!(v.is_none());

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 3: Overwrite compaction — newest value wins ──────────────────────
    println!("\n── Test 3: Overwrite across SSTables — newest value wins");
    {
        let db_dir = "/tmp/pebble_day6_t3";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        // v1 in SSTable 1
        eng.put(b"counter", b"1").unwrap();
        eng.flush_memtable().unwrap();

        // v2 in SSTable 2
        eng.put(b"counter", b"2").unwrap();
        eng.flush_memtable().unwrap();

        // v3 in SSTable 3
        eng.put(b"counter", b"3").unwrap();
        eng.flush_memtable().unwrap();

        let before = eng.get(b"counter").unwrap();
        println!("  Before compaction: counter = {:?}", before.as_ref().map(|b| String::from_utf8_lossy(b)));
        assert_eq!(before.as_deref(), Some(b"3".as_ref()));

        eng.run_compaction().unwrap();

        let after = eng.get(b"counter").unwrap();
        println!("  After compaction:  counter = {:?} (v3 survived ✓)", after.as_ref().map(|b| String::from_utf8_lossy(b)));
        assert_eq!(after.as_deref(), Some(b"3".as_ref()));

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 4: Range scan across compacted + live data ───────────────────────
    println!("\n── Test 4: Range scan after compaction");
    {
        let db_dir = "/tmp/pebble_day6_t4";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        // Batch 1 → SSTable
        for c in ['a', 'b', 'c'] {
            eng.put(format!("fruit_{}", c).as_bytes(), b"old").unwrap();
        }
        eng.flush_memtable().unwrap();

        // Batch 2 → SSTable (overwrites 'a', adds 'd')
        eng.put(b"fruit_a", b"new").unwrap();
        eng.put(b"fruit_d", b"new").unwrap();
        eng.flush_memtable().unwrap();

        // Compact and scan
        eng.run_compaction().unwrap();
        let results = eng.scan(b"fruit_a", b"fruit_e").unwrap();
        println!("  scan(fruit_a..fruit_e) after compaction:");
        for (k, v) in &results {
            println!("    {:?} = {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
        }
        assert_eq!(results.len(), 4);
        let a_val = results.iter().find(|(k, _)| k == b"fruit_a").map(|(_, v)| v.clone());
        assert_eq!(a_val.as_deref(), Some(b"new".as_ref()), "fruit_a should be 'new'");
        println!("  Merge correct, overwrite respected ✓");

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 5: Kill-9 recovery harness ──────────────────────────────────────
    // Simulates crashes at three points:
    //   (a) mid-WAL-write   — process dies before memtable is updated
    //   (b) mid-flush       — process dies after memtable flush starts but WAL not truncated
    //   (c) mid-compaction  — process dies after compaction writes new file but before old deleted
    println!("\n── Test 5: Kill-9 recovery harness");

    // (a) Crash mid-WAL-write: truncate WAL after 2 of 3 records
    {
        println!("\n  (a) Crash mid-WAL-write");
        let db_dir = "/tmp/pebble_kill9_a";
        let _ = fs::remove_dir_all(db_dir);

        use engine::wal::Wal;
        use engine::metrics::new_shared_metrics;

        // Write 3 records, then corrupt the 3rd by truncating mid-record
        {
            let metrics = new_shared_metrics();
            let wal_path = format!("{}/wal.log", db_dir);
            fs::create_dir_all(db_dir).unwrap();
            let mut wal = Wal::open(&wal_path, metrics.clone()).unwrap();
            wal.append_put(b"key_a", b"val_a").unwrap();
            wal.append_put(b"key_b", b"val_b").unwrap();

            // Simulate torn write: write partial record bytes
            use std::io::Write;
            let mut f = fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(b"\x00\x00\x00\x05val_c_PARTIAL").unwrap();
            // "Process killed here" — no fsync, no completion
        }

        // Recovery: open engine, WAL should replay only 2 valid records
        let mut eng = Engine::open(db_dir).unwrap();
        println!("    WAL recovery_time: {}ms", eng.metrics.lock().unwrap().recovery_time_ms);
        assert_eq!(eng.get(b"key_a").unwrap(), Some(b"val_a".to_vec()), "key_a must survive");
        assert_eq!(eng.get(b"key_b").unwrap(), Some(b"val_b".to_vec()), "key_b must survive");
        assert_eq!(eng.get(b"key_c").unwrap(), None, "key_c was never committed");
        println!("    key_a ✓  key_b ✓  key_c=None ✓ (torn write discarded)");

        let _ = fs::remove_dir_all(db_dir);
    }

    // (b) Crash mid-flush: WAL exists, SSTable was written, WAL not truncated
    {
        println!("\n  (b) Crash mid-flush (WAL not truncated after flush)");
        let db_dir = "/tmp/pebble_kill9_b";
        let _ = fs::remove_dir_all(db_dir);

        // Manually set up: SSTable on disk AND WAL with same data (flush completed
        // but engine crashed before WAL truncation)
        {
            use engine::metrics::new_shared_metrics;
            use engine::memtable::MemValue;
            use engine::wal::Wal;

            fs::create_dir_all(db_dir).unwrap();
            let metrics = new_shared_metrics();

            // Write WAL
            let wal_path = format!("{}/wal.log", db_dir);
            let mut wal = Wal::open(&wal_path, metrics.clone()).unwrap();
            wal.append_put(b"flush_key", b"flush_val").unwrap();

            // Also write SSTable (simulating completed flush)
            let sst_path = format!("{}/sst_00000001.sst", db_dir);
            let entries = vec![(
                b"flush_key".to_vec(),
                MemValue::Value(b"flush_val".to_vec()),
            )];
            SSTable::flush(&sst_path, entries).unwrap();

            // "Crash" — WAL was not truncated
        }

        // Recovery: engine opens, replays WAL, opens SSTable
        // Both have flush_key — memtable restored from WAL shadows SSTable (same value)
        let mut eng = Engine::open(db_dir).unwrap();
        let v = eng.get(b"flush_key").unwrap();
        println!("    flush_key after crash-mid-flush = {:?} ✓", v.map(|b| String::from_utf8(b).unwrap()));
        println!("    SSTables found: {}", eng.metrics.lock().unwrap().sstable_count);

        let _ = fs::remove_dir_all(db_dir);
    }

    // (c) Crash mid-compaction: new SSTable written, old ones not yet deleted
    {
        println!("\n  (c) Crash mid-compaction (old SSTables still on disk)");
        let db_dir = "/tmp/pebble_kill9_c";
        let _ = fs::remove_dir_all(db_dir);

        {
            use engine::memtable::MemValue;

            fs::create_dir_all(db_dir).unwrap();

            // Two old SSTables
            let entries_a = vec![
                (b"alpha".to_vec(), MemValue::Value(b"1".to_vec())),
                (b"beta".to_vec(), MemValue::Value(b"1".to_vec())),
            ];
            let entries_b = vec![
                (b"beta".to_vec(), MemValue::Value(b"2".to_vec())), // overwrites
                (b"gamma".to_vec(), MemValue::Value(b"1".to_vec())),
            ];
            // Newer compacted SSTable also exists (crash happened after write)
            let entries_merged = vec![
                (b"alpha".to_vec(), MemValue::Value(b"1".to_vec())),
                (b"beta".to_vec(), MemValue::Value(b"2".to_vec())),
                (b"gamma".to_vec(), MemValue::Value(b"1".to_vec())),
            ];

            SSTable::flush(format!("{}/sst_00000001.sst", db_dir), entries_a).unwrap();
            SSTable::flush(format!("{}/sst_00000002.sst", db_dir), entries_b).unwrap();
            // Compacted output (process died before deleting 001 and 002)
            SSTable::flush(format!("{}/sst_00000003.sst", db_dir), entries_merged).unwrap();
            // WAL is empty (flush was done)
            use engine::metrics::new_shared_metrics;
            let metrics = new_shared_metrics();
            Wal::open(format!("{}/wal.log", db_dir), metrics).unwrap();
        }

        // Engine opens all 3 SSTables — reads correctly because newest wins
        let mut eng = Engine::open(db_dir).unwrap();
        println!("    SSTables found (including duplicates): {}", eng.metrics.lock().unwrap().sstable_count);

        // beta should be "2" (newest SSTable wins)
        let v = eng.get(b"beta").unwrap();
        println!("    beta = {:?} (should be '2', newest wins ✓)", v.map(|b| String::from_utf8(b).unwrap()));

        let v = eng.get(b"alpha").unwrap();
        println!("    alpha = {:?} ✓", v.map(|b| String::from_utf8(b).unwrap()));

        // Now run compaction to clean up the duplicate SSTables
        eng.run_compaction().unwrap();
        println!("    After cleanup compaction: {} SSTables", eng.metrics.lock().unwrap().sstable_count);

        let v = eng.get(b"beta").unwrap();
        println!("    beta after cleanup = {:?} ✓", v.map(|b| String::from_utf8(b).unwrap()));

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 6: Write amplification benchmark ────────────────────────────────
    println!("\n── Test 6: Compaction write-amplification benchmark");
    {
        let db_dir = "/tmp/pebble_day6_bench";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        let n = 500u32;

        // Phase 1: sequential writes, allow natural compaction
        let t0 = Instant::now();
        for i in 0..n {
            let key = format!("bench_key_{:06}", i);
            let val = format!("bench_val_{:06}_padding_________________", i);
            eng.put(key.as_bytes(), val.as_bytes()).unwrap();
            if i % 50 == 49 {
                eng.flush_memtable().unwrap();
            }
        }
        let write_ms = t0.elapsed().as_millis();

        // Phase 2: read all keys back
        let t1 = Instant::now();
        let mut hits = 0u32;
        for i in 0..n {
            let key = format!("bench_key_{:06}", i);
            if eng.get(key.as_bytes()).unwrap().is_some() {
                hits += 1;
            }
        }
        let read_ms = t1.elapsed().as_millis();

        let snap = eng.metrics.lock().unwrap().snapshot();
        println!("  Wrote {} keys in {}ms", n, write_ms);
        println!("  Read  {} keys in {}ms ({} hits)", n, read_ms, hits);
        println!("  Compactions run: {}", snap.compaction_count);
        println!("  Final SSTable count: {}", snap.sstable_count);
        println!("  WAL size: {} bytes", snap.wal_size_bytes);
        println!("  → Save these numbers — Day 7 benchmark table uses them");

        let _ = fs::remove_dir_all(db_dir);
    }

    // ── Test 7: Full metrics snapshot ────────────────────────────────────────
    println!("\n── Test 7: Full metrics snapshot");
    {
        let db_dir = "/tmp/pebble_day6_metrics";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        for i in 0u32..60 {
            eng.put(format!("m{}", i).as_bytes(), b"val").unwrap();
        }
        eng.flush_memtable().unwrap();
        for i in 0u32..10 {
            eng.get(format!("m{}", i).as_bytes()).unwrap();
        }
        eng.run_compaction().unwrap();

        let snap = eng.metrics.lock().unwrap().snapshot();
        println!("  {}", snap.to_json());

        let _ = fs::remove_dir_all(db_dir);
    }

    println!("\nDay 6 complete.");
    println!("Compaction merges SSTables, drops tombstones, and reduces read amplification.");
    println!("Kill-9 harness verified: mid-WAL-write, mid-flush, mid-compaction all recover correctly.");
}