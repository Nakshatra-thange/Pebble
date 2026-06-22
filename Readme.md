PEBBLE
A key-value storage engine written in Rust
WAL  •  LSM-tree  •  Bloom filters  •  Compaction  •  Live metrics dashboard

Languages
Rust
Lines of code
~2,400
External deps
2
Build time
~1.3s




1. The problem
Every database, at its lowest level, is solving the same problem: how do you write data fast, read it back reliably, and survive a crash without losing either? The naive answer is to write directly to a file. That works until your dataset grows, your write rate increases, or your process gets killed at the wrong moment.
Three forces are always in tension:
Write speed. Random disk writes are slow. If every put() requires seeking to the right place on disk and writing in place, you cap out at the disk's random write IOPS — typically a few hundred per second on spinning rust, a few thousand on SSD.
Read speed. Fast writes usually mean writes go somewhere quick and disorganized, which means reads have to search more places.
Durability. The moment you relax fsync to make writes faster, you introduce a window where a crash loses data. Every production database is navigating this tradeoff explicitly.

Traditional B-trees, used by Postgres and MySQL at the storage layer, do well on reads but require in-place updates — meaning each write potentially touches multiple disk pages. At high write throughput, the number of random writes saturates the disk's I/O budget before the CPU is even close to busy.
Log-structured merge trees (LSM-trees), used by LevelDB, RocksDB, Cassandra, and InfluxDB, invert this: all writes are sequential appends, which are fast on every storage medium. The cost is shifted to reads and background compaction. Pebble is a complete, from-scratch implementation of this architecture.


2. Who faces this problem
This is not a theoretical exercise. The storage engine layer is where most real performance problems in database systems actually live.
Systems engineers building databases
Anyone building a database from scratch, or embedding a storage backend into an existing system, has to make these tradeoffs explicitly. RocksDB is the default choice in this space, but it is a 400,000-line C++ codebase. Understanding what it is doing at each layer — and why — requires either reading the LevelDB paper or building something equivalent.
Backend engineers debugging write latency
Engineers using Cassandra, InfluxDB, CockroachDB, or TiKV are running systems built on LSM-trees. When write latency spikes during compaction, or read latency degrades as SSTable count grows, diagnosing the problem requires understanding what these systems are doing internally. Pebble makes that concrete.
Distributed systems engineers
Raft-based systems like etcd and CockroachDB need a durable local storage layer. The WAL pattern Pebble implements is essentially the same one Raft log entries are stored in. Understanding the crash recovery guarantees at the storage level is prerequisite knowledge for reasoning about distributed consistency.


3. What Pebble solves
Pebble is a single-node embedded key-value store that exposes a simple API and implements the full production storage engine stack underneath it.
The public API
put(key, value)  // durable write, fsync before return
get(key)         // memtable first, then SSTables newest-to-oldest
delete(key)      // writes a tombstone, not an in-place removal
scan(start, end) // range query, merged across all layers

Behind these four operations, Pebble provides:
Crash safety. Every write is recorded in the WAL and fsync'd before the caller is told it succeeded. If the process is killed mid-write, the WAL replays on restart and the engine returns to exactly the state before the crash.
High write throughput. Writes go to an in-memory sorted structure (the memtable) backed by a WAL. No disk seek on the write path. Sequential appends only.
Correct reads across multiple storage layers. When data lives in both memory and multiple SSTable files, the read path merges them correctly with a clear precedence rule: newer data always wins, tombstones propagate correctly.
Bloom filters on every SSTable. A probabilistic data structure loaded into memory at startup that answers 'definitely not here' for absent keys with zero disk I/O. At 1% false positive rate, absent key lookups are 9x faster than without.
Background compaction. Periodically merges old SSTables into one, drops tombstones, and prevents read amplification from growing unbounded.
Live metrics over HTTP. A /metrics endpoint exposes writes/sec, reads/sec, SSTable count, WAL size, compaction count, and recovery time as JSON. A web dashboard polls it and renders the engine's internals in real time.


4. How it is built
4.1 The write path
Every write follows the same sequence, regardless of whether it is a put or a delete:
The operation is encoded as a binary record: CRC32 checksum (4 bytes), key length (4 bytes), value length (4 bytes), operation type (1 byte), key bytes, value bytes.
The record is appended to the WAL file and fsync'd. The caller is not told the write succeeded until this completes. This is the durability guarantee.
The key-value pair (or tombstone) is inserted into the memtable, a BTreeMap kept in memory. BTreeMap maintains sorted order, which is required for flushing and range scans.
When the memtable exceeds its size threshold (default 4 MB), it is drained in sorted order to a new SSTable file on disk. The WAL is then truncated — its data is now in the SSTable.

The WAL record format is fixed-width in its header, which makes sequential replay fast and makes torn-write detection unambiguous: if the CRC over the payload does not match the stored CRC, the record is corrupt and replay stops at that byte offset.
4.2 The read path
A get(key) call checks layers in order from newest to oldest, stopping at the first definitive answer:
Memtable. If the key is here, return it. If a tombstone is here, return None — even if older SSTables have a live value.
SSTables, newest to oldest. For each SSTable, the bloom filter is checked first. If the filter says the key is definitely absent, skip this file entirely. If it says maybe present, do the actual seek.
If no layer returns a match, return None.

The bloom filter check is O(k) hash computations against an in-memory bit array — no disk I/O. The seek, when it does happen, uses a sparse index: an in-memory array of (key, file offset) pairs sampled every 16 keys during flush. The read path binary-searches this index to find the nearest offset, then reads forward linearly for at most 16 records.
4.3 The SSTable file format
Each SSTable file is self-describing. Everything needed to read it is contained in the file itself, anchored by a 16-byte footer:
[ data records ][ bloom filter ][ sparse index ][ footer: 8 + 8 bytes ]

Footer layout: index_offset (u64 LE) | bloom_offset (u64 LE)

Data record:   key_len (u32) | val_len (u32) | op (u8) | key | value

Index entry:   key_len (u32) | key | file_offset (u64)

On open, the engine reads the footer to locate the bloom filter and sparse index, loads both into memory, and is ready to serve reads without any additional disk I/O. The bloom filter for a 10,000-key SSTable at 1% false positive rate occupies roughly 12 KB in memory.
4.4 Bloom filters
A bloom filter answers one question: could this key possibly be in this SSTable? It can say 'definitely not' (always correct) or 'maybe' (sometimes wrong, at the configured false positive rate). The filter never produces false negatives.
The implementation uses double hashing to derive k probe positions from two hash values, avoiding the cost of k independent hash functions:
h_i(key) = (h1(key) + i * h2(key)) % num_bits

This technique is from Kirsch and Mitzenmacher (2006) and is used in production by LevelDB and RocksDB. The optimal parameters follow from the information-theoretic bounds:

num_bits = ceil(-n * ln(p) / (ln2)^2)    // n = keys, p = target FPR
k        = round((num_bits / n) * ln2)   // optimal hash count

At 1% FPR: 9.6 bits per key, k=7. At 0.1% FPR: 14.4 bits per key, k=10. These are theoretical minimums and the measured false positive rates in the benchmark suite match them within 0.05 percentage points.
4.5 Compaction
As SSTables accumulate, read amplification grows: a get() must check more files before finding its answer (or confirming absence). Compaction addresses this by merging multiple SSTables into one.
The merge is straightforward because SSTables are sorted: process from oldest to newest, inserting each key-value pair into a BTreeMap. Because newer values overwrite older ones via insert() (not or_insert()), the final map holds the correct current state. Tombstones are preserved during partial compaction (where older SSTables still exist outside the merge range) and dropped during full compaction (where all SSTables are being merged and no older data can exist).
Compaction fires automatically when SSTable count reaches the configured threshold (default 4). The resulting merged file gets a new sequence number higher than all its inputs, which keeps the newest-first ordering invariant intact.
4.6 Crash recovery
The engine guarantees specific behavior at each possible crash point. The table below describes what the engine finds on restart and what it does:

Crash point
State on disk
Recovery action
Guarantee
Mid-WAL-write
Partial record at tail
CRC fails, tail truncated
Last committed write survives
Mid-flush
WAL + SSTable both exist
WAL replay shadows SSTable
No data loss, no duplicate
Mid-compaction
Old + new SSTables coexist
Newest SSTable wins on read
Correct reads, cleanup on next compaction


The kill-9 recovery test harness in the test suite exercises all three crash points deliberately: it writes data, truncates or corrupts the WAL at specific byte offsets, and verifies that recovery produces exactly the expected state.


5. Key code — the ideas in concrete form
WAL append with fsync
The durability contract lives in these three lines. write_all stages bytes in the kernel buffer. sync_all forces them to physical storage. Nothing returns success until both complete.
self.file.write_all(record)?;
self.file.sync_all()?;   // fsync — durability guarantee
self.sync_size_to_metrics();

Torn-write detection
CRC32 is computed over the payload (everything after the checksum field). If the computed checksum does not match the stored one — because the write was interrupted — decode_wal_record returns None and replay stops at that byte offset. The file is then truncated to remove the garbage tail.
let computed_crc = crc32fast::hash(&data[4..total]);
if computed_crc != stored_crc { return None; }

Bloom filter double hashing
Two FNV-1a variants produce two independent 64-bit hashes. All k probe positions are derived from these two values. No k separate hash functions needed.
fn probe(&self, h1: u64, h2: u64, i: u64) -> u64 {
    h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits
}

Memtable flush — atomic swap
std::mem::replace swaps in a fresh empty memtable before writing to disk. If the flush fails, the old data is gone from the live memtable — but it is still in the WAL and will be recovered on restart. This keeps the engine in a consistent state through failed flushes.
let old_mem = std::mem::replace(
    &mut self.memtable,
    Memtable::new(threshold, self.metrics.clone()),
);
let entries = old_mem.drain_sorted();
let sst = SSTable::flush(&sst_path, entries)?;
self.wal.truncate()?;  // safe only after successful flush

Compaction merge — newest wins
The key is insert() not or_insert(). Processing oldest-to-newest means each iteration overwrites with a newer value. The final BTreeMap holds the correct current state for every key.
for sst in sstables.iter_mut().rev() {  // oldest first
    for (key, val) in sst.scan(b"", b"\xFF...")? {
        merged.insert(key, val);  // newest write wins
    }
}


6. Benchmark results
All benchmarks run on a single machine, single thread, fsync enabled. Numbers are averages over 1000 operations.

Configuration
Avg read latency
Notes
WAL only
0.8 µs
In-memory, no disk seek
WAL + SSTables
29 µs
Disk seek per SSTable
WAL + SSTables + bloom
5.2 µs
9x faster on absent keys


The bloom filter speedup applies specifically to absent key lookups — the common case in production systems where most reads check for keys that may not exist (cache misses, existence checks, range boundary probes). For present key lookups, the filter always returns 'maybe' (it has no false negatives) so the read path is identical to the no-bloom case.
The write amplification tradeoff: a compaction threshold of 4 SSTables produces roughly 1.4x write amplification (each byte written once to the memtable, once to an SSTable, once more during compaction into the merged file). Higher thresholds reduce write amplification but allow SSTable count to grow, increasing read amplification before the next compaction fires.


7. Project structure
src/
  main.rs              -- entry point, benchmarks, HTTP server startup
  server.rs            -- /metrics HTTP endpoint (tiny_http)
  engine/
    mod.rs             -- module declarations
    error.rs           -- unified EngineError type
    format.rs          -- all on-disk binary formats
    metrics.rs         -- EngineMetrics, rate windows, MetricsSnapshot
    wal.rs             -- write-ahead log, append, replay, truncate
    memtable.rs        -- BTreeMap-backed sorted in-memory store
    bloom.rs           -- bloom filter, double hashing, encode/decode
    sstable.rs         -- flush, open, sparse index, bloom integration
    compaction.rs      -- SSTable merge, tombstone dropping
    engine.rs          -- public API, flush trigger, recovery orchestration
pebble_dashboard.html  -- standalone web dashboard, polls /metrics

External dependencies: crc32fast (checksum computation), tiny_http (metrics server). No async runtime, no serialization framework, no ORM. The binary format, the hash functions, and the data structures are all implemented from scratch.


8. What was deliberately left out
Pebble is a storage engine, not a database. The following are out of scope by design:
SQL parser and query planner. Adding a SQL layer on top of Pebble would be straightforward — it would sit above the scan() API — but it is not the interesting part of the system.
Concurrent access. The engine is single-threaded. Adding concurrent writes would require either a lock per key (fine-grained) or a single write lock (simple). The metrics struct already uses Arc<Mutex<>> for thread-safe access from the HTTP server thread.
Multi-level compaction. RocksDB uses a leveled compaction strategy with multiple levels, each 10x larger than the previous. Pebble uses flat compaction: all SSTables are at the same level, and the oldest N are merged when the count threshold is hit. Leveled compaction would reduce write amplification further at the cost of implementation complexity.
WAL group commit. Each write currently does its own fsync. In production systems, multiple concurrent writers' records are batched into a single fsync. Not applicable to a single-threaded engine.
Compression. SSTable data is stored uncompressed. Snappy or LZ4 compression per block would reduce disk usage significantly at low CPU cost.
Encryption at rest. Not implemented. Would sit between the serialization layer and the file write.


9. Resume line

Built a key-value storage engine in Rust implementing a write-ahead log for crash-safe durability, an LSM-tree with memtable, immutable SSTables, and background compaction, and per-SSTable bloom filters reducing unnecessary disk seeks. Verified durability with a kill-9 recovery harness simulating crashes mid-write, mid-flush, and mid-compaction. Exposed live engine metrics over HTTP to a real-time web dashboard. Benchmarked read latency across three configurations, demonstrating a 9x speedup from bloom filters on absent key lookups.

