mod engine;

use engine::metrics::new_shared_metrics;
use engine::wal::{Wal, WalEntry};
use std::fs;
use std::io::Write;

fn main() {
    println!("=== Pebble Engine — Day 2: WAL Tests ===\n");

    let wal_path = "/tmp/pebble_test.wal";

    // Clean slate
    let _ = fs::remove_file(wal_path);

    // ── Test 1: Normal append + replay ──────────────────────────────────────
    println!("── Test 1: Normal append + replay");
    {
        let metrics = new_shared_metrics();
        let mut wal = Wal::open(wal_path, metrics.clone()).unwrap();

        wal.append_put(b"name", b"alice").unwrap();
        wal.append_put(b"age", b"30").unwrap();
        wal.append_put(b"city", b"delhi").unwrap();
        wal.append_delete(b"age").unwrap();

        println!("  WAL size after 4 records: {} bytes", metrics.lock().unwrap().wal_size_bytes);

        let entries = wal.replay().unwrap();
        println!("  Replayed {} entries:", entries.len());
        for e in &entries {
            match e {
                WalEntry::Put { key, value } => println!(
                    "    PUT  {:?} = {:?}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                ),
                WalEntry::Delete { key } => println!(
                    "    DEL  {:?}",
                    String::from_utf8_lossy(key)
                ),
            }
        }

        let recovery_ms = metrics.lock().unwrap().recovery_time_ms;
        println!("  Recovery time: {}ms", recovery_ms);
    }

    // ── Test 2: Torn-write detection ─────────────────────────────────────────
    println!("\n── Test 2: Torn-write detection");
    {
        let _ = fs::remove_file(wal_path);
        let metrics = new_shared_metrics();
        let mut wal = Wal::open(wal_path, metrics.clone()).unwrap();

        wal.append_put(b"good_key_1", b"good_val_1").unwrap();
        wal.append_put(b"good_key_2", b"good_val_2").unwrap();

        // Simulate a torn write: manually append garbage bytes
        // (as if the process died mid-write)
        {
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(wal_path)
                .unwrap();
            f.write_all(b"\x00\xFF\xAB\x12\xDE\xAD\xBE\xEF partial record").unwrap();
            // No fsync — the process "died" here
        }

        let size_with_garbage = fs::metadata(wal_path).unwrap().len();
        println!("  File size with torn write: {} bytes", size_with_garbage);

        let entries = wal.replay().unwrap();
        let size_after_replay = fs::metadata(wal_path).unwrap().len();

        println!("  Entries recovered (only valid ones): {}", entries.len());
        println!("  File size after truncation: {} bytes", size_after_replay);

        assert_eq!(entries.len(), 2, "Should recover exactly 2 good entries");
        assert!(
            size_after_replay < size_with_garbage,
            "Truncation should have removed the garbage tail"
        );
        println!("  Torn-write detection ✓");
    }

    // ── Test 3: WAL survives a simulated crash + reopen ──────────────────────
    println!("\n── Test 3: Crash simulation (close + reopen)");
    {
        let _ = fs::remove_file(wal_path);
        let metrics = new_shared_metrics();

        // Write phase — "process A"
        {
            let mut wal = Wal::open(wal_path, metrics.clone()).unwrap();
            wal.append_put(b"survivor", b"yes").unwrap();
            wal.append_put(b"also_survivor", b"definitely").unwrap();
            // WAL is dropped here — file is closed (simulating crash)
        }

        // Recovery phase — "process B" restarts
        let mut wal = Wal::open(wal_path, metrics.clone()).unwrap();
        let entries = wal.replay().unwrap();

        println!("  Entries recovered after reopen: {}", entries.len());
        for e in &entries {
            if let WalEntry::Put { key, value } = e {
                println!(
                    "    PUT {:?} = {:?}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                );
            }
        }
        assert_eq!(entries.len(), 2);
        println!("  Crash + reopen ✓");
    }

    // ── Test 4: Recovery timing benchmark ────────────────────────────────────
    println!("\n── Test 4: Recovery timing (1000 records)");
    {
        let _ = fs::remove_file(wal_path);
        let metrics = new_shared_metrics();
        let mut wal = Wal::open(wal_path, metrics.clone()).unwrap();

        for i in 0u32..1000 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{:06}_padding_to_make_it_realistic", i);
            wal.append_put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let wal_size = metrics.lock().unwrap().wal_size_bytes;
        println!("  WAL size (1000 records): {} bytes ({:.1} KB)", wal_size, wal_size as f64 / 1024.0);

        let entries = wal.replay().unwrap();
        let recovery_ms = metrics.lock().unwrap().recovery_time_ms;

        println!("  Replayed: {} entries", entries.len());
        println!("  Recovery time: {}ms", recovery_ms);
        println!("  → This number goes in your benchmark table on Day 7");
    }

    // Cleanup
    let _ = fs::remove_file(wal_path);
    println!("\nDay 2 complete. WAL is durable, torn-write-safe, and timed.");
}