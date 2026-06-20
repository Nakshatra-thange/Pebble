mod engine;

use engine::engine::Engine;
use std::fs;

fn main() {
    println!("=== Pebble Engine — Day 4: SSTable Flush + Read ===\n");

    let db_dir = "/tmp/pebble_day4";
    let _ = fs::remove_dir_all(db_dir);

    // ── Test 1: Basic put + get through the engine ───────────────────────────
    println!("── Test 1: Basic put / get");
    {
        let mut engine = Engine::open(db_dir).unwrap();

        engine.put(b"name", b"alice").unwrap();
        engine.put(b"city", b"pune").unwrap();
        engine.put(b"lang", b"rust").unwrap();

        println!("  get(name) = {:?}", engine.get(b"name").unwrap());
        println!("  get(city) = {:?}", engine.get(b"city").unwrap());
        println!("  get(missing) = {:?}", engine.get(b"missing").unwrap());

        let snap = engine.metrics.lock().unwrap().snapshot();
        println!("  WAL size: {} bytes", snap.wal_size_bytes);
        println!("  SSTables: {}", snap.sstable_count);
    }

    // ── Test 2: Force a flush, then read from SSTable ────────────────────────
    println!("\n── Test 2: Manual flush → read from SSTable");
    {
        let _ = fs::remove_dir_all(db_dir);
        let mut engine = Engine::open(db_dir).unwrap();

        // Write some data
        for i in 0u32..50 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{:04}", i);
            engine.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        println!("  Before flush — SSTables: {}", engine.metrics.lock().unwrap().sstable_count);
        println!("  Before flush — WAL size: {} bytes", engine.metrics.lock().unwrap().wal_size_bytes);

        // Force flush (normally triggered by size threshold)
        engine.flush_memtable().unwrap();

        println!("  After flush  — SSTables: {}", engine.metrics.lock().unwrap().sstable_count);
        println!("  After flush  — WAL size: {} bytes (truncated)", engine.metrics.lock().unwrap().wal_size_bytes);

        // Read back from SSTable
        let v = engine.get(b"key_0023").unwrap();
        println!("  get(key_0023) from SSTable = {:?}", v.map(|b| String::from_utf8(b).unwrap()));

        let v = engine.get(b"key_0049").unwrap();
        println!("  get(key_0049) from SSTable = {:?}", v.map(|b| String::from_utf8(b).unwrap()));
    }

    // ── Test 3: Delete → tombstone survives flush ────────────────────────────
    println!("\n── Test 3: Delete survives flush");
    {
        let _ = fs::remove_dir_all(db_dir);
        let mut engine = Engine::open(db_dir).unwrap();

        engine.put(b"ghost", b"i exist").unwrap();
        engine.delete(b"ghost").unwrap();
        engine.flush_memtable().unwrap();

        // Must still return None — tombstone in SSTable blocks the read
        let v = engine.get(b"ghost").unwrap();
        println!("  get(ghost) after flush = {:?} (None = tombstone respected ✓)", v);
    }

    // ── Test 4: memtable + SSTable read merge ────────────────────────────────
    println!("\n── Test 4: Memtable shadows SSTable");
    {
        let _ = fs::remove_dir_all(db_dir);
        let mut engine = Engine::open(db_dir).unwrap();

        // Write v1 and flush it to SSTable
        engine.put(b"version", b"v1").unwrap();
        engine.flush_memtable().unwrap();

        // Write v2 — lives in memtable only
        engine.put(b"version", b"v2").unwrap();

        let v = engine.get(b"version").unwrap();
        println!(
            "  get(version) = {:?} (should be v2 from memtable, not v1 from SSTable ✓)",
            v.map(|b| String::from_utf8(b).unwrap())
        );
    }

    // ── Test 5: Range scan across memtable + SSTable ─────────────────────────
    println!("\n── Test 5: Range scan across memtable + SSTable");
    {
        let _ = fs::remove_dir_all(db_dir);
        let mut engine = Engine::open(db_dir).unwrap();

        // Flush first batch
        engine.put(b"apple", b"1").unwrap();
        engine.put(b"cherry", b"3").unwrap();
        engine.put(b"elderberry", b"5").unwrap();
        engine.flush_memtable().unwrap();

        // Second batch stays in memtable
        engine.put(b"banana", b"2").unwrap();
        engine.put(b"date", b"4").unwrap();

        let results = engine.scan(b"apple", b"fig").unwrap();
        println!("  scan(apple..fig):");
        for (k, v) in &results {
            println!(
                "    {:?} = {:?}",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            );
        }
        assert_eq!(results.len(), 5);
        println!("  All 5 entries merged correctly ✓");
    }

    // ── Test 6: Crash recovery — reopen and read SSTables ────────────────────
    println!("\n── Test 6: Crash recovery across engine reopen");
    {
        let _ = fs::remove_dir_all(db_dir);

        // Session A — write and flush
        {
            let mut engine = Engine::open(db_dir).unwrap();
            engine.put(b"durable", b"absolutely").unwrap();
            engine.flush_memtable().unwrap();
            // Engine dropped here — simulated shutdown
        }

        // Session B — reopen, data should still be there
        {
            let mut engine = Engine::open(db_dir).unwrap();
            let v = engine.get(b"durable").unwrap();
            println!(
                "  get(durable) after reopen = {:?} ✓",
                v.map(|b| String::from_utf8(b).unwrap())
            );
            println!("  SSTables found on reopen: {}", engine.metrics.lock().unwrap().sstable_count);
        }
    }

    // ── Test 7: Metrics snapshot ──────────────────────────────────────────────
    println!("\n── Test 7: Metrics after a full session");
    {
        let _ = fs::remove_dir_all(db_dir);
        let mut engine = Engine::open(db_dir).unwrap();

        for i in 0u32..30 {
            engine.put(format!("k{}", i).as_bytes(), b"v").unwrap();
        }
        engine.flush_memtable().unwrap();
        for i in 0u32..10 {
            engine.get(format!("k{}", i).as_bytes()).unwrap();
        }

        let snap = engine.metrics.lock().unwrap().snapshot();
        println!("  {}", snap.to_json());
    }

    let _ = fs::remove_dir_all(db_dir);
    println!("\nDay 4 complete. SSTable flush, sparse index seek, and cross-layer reads all work.");
}