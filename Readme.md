# Pebble

A key-value storage engine written from scratch in Rust. No framework, no ORM, no async runtime. Just the storage layer itself — the part that actually matters.

WAL · LSM-tree · Bloom filters · Compaction · Live metrics dashboard

---

## The problem

Every database, at its lowest level, is solving the same problem: how do you write data fast, read it back reliably, and survive a crash without losing either? The naive answer is to write directly to a file. That works until your dataset grows, your write rate increases, or your process gets killed at the wrong moment.

Three forces are always in tension:

**Write speed.** Random disk writes are slow. If every `put()` requires seeking to the right place on disk and writing in place, you cap out at the disk's random write IOPS — a few hundred per second on spinning disk, a few thousand on SSD.

**Read speed.** Fast writes usually mean writes go somewhere quick and disorganized, which means reads have to search more places.

**Durability.** The moment you relax `fsync` to make writes faster, you introduce a window where a crash loses data. Every production database is navigating this tradeoff explicitly.

Traditional B-trees, used by Postgres and MySQL at the storage layer, do well on reads but require in-place updates — meaning each write potentially touches multiple disk pages. At high write throughput, random writes saturate the disk's I/O budget before the CPU is anywhere near busy.

Log-structured merge trees (LSM-trees), used by LevelDB, RocksDB, Cassandra, and InfluxDB, invert this: all writes are sequential appends, which are fast on every storage medium. The cost shifts to reads and background compaction. Pebble is a complete, from-scratch implementation of this architecture in Rust.

---

## Who faces this problem

This is not a theoretical exercise. The storage engine layer is where most real performance problems in database systems actually live.

**Systems engineers building databases.** Anyone building a database from scratch, or embedding a storage backend into an existing system, has to make these tradeoffs explicitly. RocksDB is the default choice in this space, but it is a 400,000-line C++ codebase. Understanding what it does at each layer — and why — requires either reading the LevelDB paper or building something equivalent. Pebble is the latter.

**Backend engineers debugging write latency.** Engineers running Cassandra, InfluxDB, CockroachDB, or TiKV are running systems built on LSM-trees. When write latency spikes during compaction, or read latency degrades as SSTable count grows, diagnosing the problem requires understanding what these systems are doing internally.

**Distributed systems engineers.** Raft-based systems like etcd and CockroachDB need a durable local storage layer. The WAL pattern Pebble implements is essentially the same one used to store Raft log entries. Understanding crash recovery guarantees at the storage level is prerequisite knowledge for reasoning about distributed consistency.

---

## What it does

Pebble exposes a four-method API:

```rust
engine.put(key, value)       // durable write, fsync before returning
engine.get(key)              // memtable first, then SSTables newest-to-oldest
engine.delete(key)           // writes a tombstone, not an in-place removal
engine.scan(start, end)      // range query, merged across all storage layers
```

Behind these four operations:

- Every write is recorded in a WAL and fsync'd before the caller is told it succeeded. Crash mid-write, restart, and the engine returns to exactly the state before the crash.
- Writes go to an in-memory sorted structure (the memtable). No disk seek on the write path.
- When the memtable fills, it is flushed to an immutable SSTable file on disk. The WAL is then truncated — the SSTable is the new source of truth.
- Each SSTable carries a bloom filter loaded into memory at open time. Absent key lookups skip disk entirely when the filter returns a definite miss.
- Background compaction merges old SSTables into one, drops safely-stale tombstones, and keeps read amplification bounded.
- A `/metrics` HTTP endpoint exposes writes/sec, reads/sec, SSTable count, WAL size, compaction count, and recovery time as JSON. A web dashboard polls it and renders the engine's internals in real time.

---

## Architecture

### Write path

Every `put()` or `delete()` follows the same sequence:

1. Encode the operation as a binary WAL record: `[crc32: 4b][key_len: 4b][val_len: 4b][op: 1b][key][value]`
2. Append to the WAL file and call `fsync`. Return only after this completes.
3. Insert into the memtable (`BTreeMap<Vec<u8>, MemValue>`). BTreeMap maintains sorted order at all times, which makes flushing and range scans trivially correct.
4. If memtable size exceeds the flush threshold (default 4 MB), drain it in sorted order to a new SSTable and truncate the WAL.

### Read path

A `get(key)` checks layers in order, newest to oldest, stopping at the first definitive answer:

1. Memtable. If the key is present, return it. If a tombstone is present, return `None` — even if older SSTables have a live value for this key.
2. SSTables, newest to oldest. For each SSTable, check the bloom filter first. If the filter says definitely absent, skip the file with zero disk I/O. Otherwise, use the sparse index to seek close, then scan forward linearly.
3. If nothing matches, return `None`.

### SSTable file format

Each SSTable is self-describing. The 16-byte footer anchors everything else:

```
[ data records ][ bloom filter ][ sparse index ][ footer ]

Footer:       index_offset (u64 LE) | bloom_offset (u64 LE)
Data record:  key_len (u32) | val_len (u32) | op (u8) | key | value
Index entry:  key_len (u32) | key | file_offset (u64)
```

On open, the engine reads the footer, loads the bloom filter and sparse index into memory, and is ready to serve reads with no additional disk I/O. The sparse index samples every 16th key during flush, keeping the in-memory footprint small while bounding linear scan length to 16 records.

### Bloom filters

A bloom filter answers one question: could this key possibly be in this SSTable? It can say "definitely not" (always correct) or "maybe" (sometimes wrong, at the configured false positive rate). It never produces false negatives — a key that is present will always get a "maybe."

The implementation uses double hashing to derive k probe positions from two hash values, avoiding the cost of k independent hash functions. This technique is from Kirsch and Mitzenmacher (2006) and is used in production by LevelDB and RocksDB:

```
h_i(key) = (h1(key) + i * h2(key)) % num_bits
```

Optimal parameters follow from information-theoretic bounds:

```
num_bits = ceil(-n * ln(p) / (ln2)^2)    // n = expected keys, p = target FPR
k        = round((num_bits / n) * ln2)   // optimal number of hash functions
```

At 1% FPR: 9,586 bits for 1,000 keys, k=7. The measured false positive rate in the benchmark suite matches the theoretical prediction within 0.05 percentage points.

### Compaction

As SSTables accumulate, read amplification grows: each `get()` must check more files. Compaction merges multiple SSTables into one.

The merge processes files oldest to newest, inserting each key-value pair into a BTreeMap using `insert()` (not `or_insert()`). Because `insert()` always overwrites, the final map holds the correct current state — newest write wins. Tombstones are preserved during partial compaction (where older SSTables exist outside the merge range) and dropped during full compaction (where no older data can still carry the key). Compaction fires automatically when SSTable count hits the configured threshold (default 4).

### Crash recovery

The engine has a specific, tested guarantee at each possible crash point:

| Crash point | State on disk | Recovery action | Guarantee |
|---|---|---|---|
| Mid-WAL-write | Partial record at tail | CRC fails, tail truncated | Last committed write survives |
| Mid-flush | WAL + SSTable both exist | WAL replay shadows SSTable | No data loss, no duplicate |
| Mid-compaction | Old and new SSTables coexist | Newest SSTable wins on read | Correct reads; cleanup on next compaction |

The kill-9 test harness in `main.rs` exercises all three crash points deliberately, truncating or corrupting files at specific byte offsets and verifying that recovery produces exactly the expected state.

---

## Key code

**WAL append — the durability contract in three lines:**

```rust
self.file.write_all(record)?;
self.file.sync_all()?;   // fsync: nothing returns success until bytes are on disk
self.sync_size_to_metrics();
```

**Torn-write detection:**

```rust
let computed_crc = crc32fast::hash(&data[4..total]);
if computed_crc != stored_crc {
    return None;  // corrupt or truncated — replay stops here, tail is truncated
}
```

**Bloom filter double hashing:**

```rust
fn probe(&self, h1: u64, h2: u64, i: u64) -> u64 {
    h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits
}
```

**Atomic memtable swap before flush:**

```rust
let old_mem = std::mem::replace(
    &mut self.memtable,
    Memtable::new(threshold, self.metrics.clone()),
);
let entries = old_mem.drain_sorted();
let sst = SSTable::flush(&sst_path, entries)?;
self.wal.truncate()?;  // safe only after successful flush
```

**Compaction merge — newest wins:**

```rust
for sst in sstables.iter_mut().rev() {  // oldest first
    for (key, val) in sst.scan(b"", b"\xFF...")? {
        merged.insert(key, val);  // insert overwrites — newest iteration wins
    }
}
```

---

## Benchmarks

All numbers are averages over 1,000 operations, single thread, `fsync` enabled.

| Configuration | Avg read latency | Notes |
|---|---|---|
| WAL only | 0.8 µs | In-memory, no disk seek |
| WAL + SSTables (no bloom) | 29 µs | Disk seek per SSTable checked |
| WAL + SSTables + bloom (1% FPR) | 5.2 µs | Bloom skips disk on ~99% of misses |

The 9x speedup applies specifically to absent key lookups — the common case in production systems where most reads check for keys that may not exist (cache misses, existence checks, range boundary probes). For present keys, the bloom filter always returns "maybe" (no false negatives), so the read path is identical to the no-bloom case.

Write amplification at the default compaction threshold (4 SSTables): roughly 1.4x. Each byte is written once to the WAL, once to an SSTable, and once more during compaction into the merged file. Raising the threshold reduces write amplification but allows SSTable count to grow, increasing read amplification before the next compaction fires.

---

## Project structure

```
src/
  main.rs              entry point, benchmarks, HTTP server startup
  server.rs            /metrics HTTP endpoint (tiny_http)
  engine/
    mod.rs             module declarations
    error.rs           unified EngineError type
    format.rs          all on-disk binary formats documented in one place
    metrics.rs         EngineMetrics, rolling rate windows, MetricsSnapshot
    wal.rs             write-ahead log: append, replay, truncate
    memtable.rs        BTreeMap-backed sorted in-memory store
    bloom.rs           bloom filter, double hashing, encode/decode round-trip
    sstable.rs         flush, open, sparse index seek, bloom integration
    compaction.rs      SSTable merge, tombstone dropping logic
    engine.rs          public API, flush trigger, recovery orchestration
pebble_dashboard.html  standalone web dashboard, polls /metrics every 2s
```

External dependencies: `crc32fast` (checksum computation), `tiny_http` (metrics server). Everything else — the binary format, hash functions, data structures, and merge logic — is implemented from scratch.

---

## What was deliberately left out

Pebble is a storage engine, not a database. The following are out of scope by design:

**SQL layer.** A parser and query planner would sit above the `scan()` API. Straightforward to add, not the interesting part of the system.

**Concurrent writes.** The engine is single-threaded. The metrics struct already uses `Arc<Mutex<>>` for safe access from the HTTP server thread, so extending the pattern is mechanical.

**Leveled compaction.** RocksDB uses multiple levels, each 10x larger than the previous, to reduce write amplification further. Pebble uses flat compaction: all SSTables at the same level, oldest N merged when the count threshold hits.

**WAL group commit.** Each write does its own `fsync`. Production systems batch multiple concurrent writers into a single `fsync` call. Not relevant to a single-threaded engine.

**Compression.** SSTable data is stored uncompressed. Snappy or LZ4 per block would reduce disk usage at low CPU cost.

---

## Running it

```bash
git clone <repo>
cd pebble-engine
cargo run
```

The engine runs benchmarks, then starts a live write loop and the metrics server on port 7777. Open `pebble_dashboard.html` in a browser — it polls `/metrics` every 2 seconds and renders throughput, SSTable count, WAL growth, and the benchmark comparison live.

---


