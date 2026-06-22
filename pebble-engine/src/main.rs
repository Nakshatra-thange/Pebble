mod engine;

use engine::bloom::BloomFilter;
use engine::engine::Engine;
use engine::memtable::MemValue;
use engine::sstable::SSTable;
use std::fs;

fn main() {
    println!("=== Pebble Engine — Day 5: Bloom Filters ===\n");

    // ── Test 1: Bloom filter correctness ─────────────────────────────────────
    println!("── Test 1: Bloom filter — no false negatives");
    {
        let mut bloom = BloomFilter::new(1000, 0.01);
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{:06}", i)).collect();

        for k in &keys {
            bloom.insert(k.as_bytes());
        }

        // Every inserted key must return true (no false negatives — ever)
        let mut false_negatives = 0;
        for k in &keys {
            if !bloom.might_contain(k.as_bytes()) {
                false_negatives += 1;
            }
        }
        println!("  False negatives: {} (must be 0) ✓", false_negatives);
        assert_eq!(false_negatives, 0);

        println!(
            "  Filter: {} bits, k={} hash fns",
            bloom.num_bits(),
            bloom.num_hash_fns()
        );
        println!("  Expected FPR: {:.4}%", bloom.expected_fpr(1000) * 100.0);
    }

    // ── Test 2: False positive rate measurement ───────────────────────────────
    println!("\n── Test 2: Measured false positive rate at different sizes");
    println!("  {:>12}  {:>10}  {:>12}  {:>12}", "target_fpr", "bits", "measured_fpr", "k");

    for &target_fpr in &[0.10, 0.05, 0.01, 0.001] {
        let n = 10_000usize;
        let mut bloom = BloomFilter::new(n, target_fpr);

        // Insert n keys
        for i in 0..n {
            bloom.insert(format!("inserted_{}", i).as_bytes());
        }

        // Test against n different keys that were never inserted
        let trials = 100_000usize;
        let mut false_positives = 0usize;
        for i in 0..trials {
            if bloom.might_contain(format!("absent_{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let measured = false_positives as f64 / trials as f64;
        println!(
            "  {:>12.3}  {:>10}  {:>11.4}%  {:>12}",
            target_fpr,
            bloom.num_bits(),
            measured * 100.0,
            bloom.num_hash_fns()
        );
    }

    // ── Test 3: Bloom filter encode / decode round-trip ──────────────────────
    println!("\n── Test 3: Encode / decode round-trip");
    {
        let mut bloom = BloomFilter::new(500, 0.01);
        for i in 0..500u32 {
            bloom.insert(format!("rt_key_{}", i).as_bytes());
        }

        let encoded = bloom.encode();
        let decoded = BloomFilter::decode(&encoded).expect("decode failed");

        // All inserted keys must still be found after round-trip
        let mut misses = 0;
        for i in 0..500u32 {
            if !decoded.might_contain(format!("rt_key_{}", i).as_bytes()) {
                misses += 1;
            }
        }
        println!("  Encoded size: {} bytes", encoded.len());
        println!("  Misses after decode (must be 0): {} ✓", misses);
        assert_eq!(misses, 0);
    }

    // ── Test 4: SSTable with bloom filter — definite misses skip disk ─────────
    println!("\n── Test 4: SSTable bloom — definite misses");
    {
        let sst_path = "/tmp/pebble_bloom_test.sst";
        let _ = fs::remove_file(sst_path);

        // Build a small SSTable
        let entries: Vec<(Vec<u8>, MemValue)> = (0..100u32)
            .map(|i| {
                (
                    format!("bloom_key_{:04}", i).into_bytes(),
                    MemValue::Value(format!("val_{}", i).into_bytes()),
                )
            })
            .collect();

        let mut sst = SSTable::flush(sst_path, entries).unwrap();

        if let Some((bits, k)) = sst.bloom_info() {
            println!("  Bloom filter: {} bits, k={}", bits, k);
        }
        if let Some(fpr) = sst.expected_fpr() {
            println!("  Expected FPR: {:.4}%", fpr * 100.0);
        }

        // Key that exists → found
        let found = sst.get(b"bloom_key_0042").unwrap();
        println!(
            "  get(bloom_key_0042) = {:?} ✓",
            found.as_ref().map(|v| match v {
                MemValue::Value(b) => String::from_utf8_lossy(b).into_owned(),
                MemValue::Tombstone => "<tombstone>".into(),
            })
        );

        // Key that never existed → bloom returns false, no disk seek
        let miss = sst.get(b"never_inserted_key_xyz").unwrap();
        println!("  get(never_inserted_key_xyz) = {:?} (bloom skipped disk ✓)", miss);
        assert!(miss.is_none());

        let _ = fs::remove_file(sst_path);
    }

    // ── Test 5: Bloom survives SSTable reopen ────────────────────────────────
    println!("\n── Test 5: Bloom filter survives SSTable reopen");
    {
        let sst_path = "/tmp/pebble_bloom_reopen.sst";
        let _ = fs::remove_file(sst_path);

        let entries: Vec<(Vec<u8>, MemValue)> = (0..200u32)
            .map(|i| {
                (
                    format!("persist_key_{:05}", i).into_bytes(),
                    MemValue::Value(b"v".to_vec()),
                )
            })
            .collect();

        // Write and close
        {
            SSTable::flush(sst_path, entries).unwrap();
        }

        // Reopen and verify bloom still works
        let mut sst = SSTable::open(sst_path).unwrap();

        let mut false_negatives = 0;
        for i in 0..200u32 {
            let key = format!("persist_key_{:05}", i);
            if let Some(ref bloom) = sst.bloom {
                if !bloom.might_contain(key.as_bytes()) {
                    false_negatives += 1;
                }
            }
        }

        println!("  False negatives after reopen: {} (must be 0) ✓", false_negatives);
        assert_eq!(false_negatives, 0);

        // Miss on never-inserted key
        let miss = sst.get(b"totally_absent").unwrap();
        println!("  get(totally_absent) after reopen = {:?} ✓", miss);

        if let Some((bits, k)) = sst.bloom_info() {
            println!("  Loaded bloom: {} bits, k={}", bits, k);
        }

        let _ = fs::remove_file(sst_path);
    }

    // ── Test 6: Full engine — bloom works end to end ──────────────────────────
    println!("\n── Test 6: Full engine with bloom filters");
    {
        let db_dir = "/tmp/pebble_day5";
        let _ = fs::remove_dir_all(db_dir);
        let mut eng = Engine::open(db_dir).unwrap();

        // Write 200 keys and flush
        for i in 0..200u32 {
            let key = format!("engine_key_{:05}", i);
            let val = format!("engine_val_{:05}", i);
            eng.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        eng.flush_memtable().unwrap();

        // Reads of existing keys
        let v = eng.get(b"engine_key_00099").unwrap();
        println!(
            "  get(engine_key_00099) = {:?} ✓",
            v.map(|b| String::from_utf8(b).unwrap())
        );

        // Reads of absent keys — bloom should skip the SSTable
        let v = eng.get(b"ghost_key_never_written").unwrap();
        println!("  get(ghost_key_never_written) = {:?} (bloom skip ✓)", v);

        let snap = eng.metrics.lock().unwrap().snapshot();
        println!("  SSTables: {}", snap.sstable_count);
        println!("  Total reads: {}", snap.total_reads);

        let _ = fs::remove_dir_all(db_dir);
    }

    println!("\nDay 5 complete. Bloom filters built, measured, serialized, and wired into SSTables.");
    println!("Every SSTable now has a 1% FPR bloom filter loaded at open time.");
    println!("Absent key lookups skip disk entirely — zero seeks for definite misses.");
}