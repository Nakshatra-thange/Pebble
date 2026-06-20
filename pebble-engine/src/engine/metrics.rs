use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
struct RateWindow {
    timestamps: VecDeque<Instant>,
    window: Duration,
}

impl RateWindow {
    fn new(window_secs: u64) -> Self {
        Self {
            timestamps: VecDeque::new(),
            window: Duration::from_secs(window_secs),
        }
    }

    fn record(&mut self) {
        let now = Instant::now();
        self.timestamps.push_back(now);
        // Evict entries older than the window
        while let Some(&front) = self.timestamps.front() {
            if now.duration_since(front) > self.window {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }
    }

    /// Returns events per second over the last window
    fn rate(&mut self) -> f64 {
        let now = Instant::now();
        // Evict stale entries
        while let Some(&front) = self.timestamps.front() {
            if now.duration_since(front) > self.window {
                self.timestamps.pop_front();
            } else {
                break;
            }
        }
        self.timestamps.len() as f64 / self.window.as_secs_f64()
    }
}

pub struct EngineMetrics {
    write_window: RateWindow,
    read_window: RateWindow,

    pub compaction_count: u64,
    pub wal_size_bytes: u64,
    pub sstable_count: u64,
    pub recovery_time_ms: u64,

    // Lifetime totals (useful for debugging)
    pub total_writes: u64,
    pub total_reads: u64,
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self {
            write_window: RateWindow::new(1),
            read_window: RateWindow::new(1),
            compaction_count: 0,
            wal_size_bytes: 0,
            sstable_count: 0,
            recovery_time_ms: 0,
            total_writes: 0,
            total_reads: 0,
        }
    }

    pub fn record_write(&mut self) {
        self.write_window.record();
        self.total_writes += 1;
    }

    pub fn record_read(&mut self) {
        self.read_window.record();
        self.total_reads += 1;
    }

    pub fn writes_per_sec(&mut self) -> f64 {
        self.write_window.rate()
    }

    pub fn reads_per_sec(&mut self) -> f64 {
        self.read_window.rate()
    }

    pub fn snapshot(&mut self) -> MetricsSnapshot {
        MetricsSnapshot {
            writes_per_sec: self.writes_per_sec(),
            reads_per_sec: self.reads_per_sec(),
            compaction_count: self.compaction_count,
            wal_size_bytes: self.wal_size_bytes,
            sstable_count: self.sstable_count,
            recovery_time_ms: self.recovery_time_ms,
            total_writes: self.total_writes,
            total_reads: self.total_reads,
        }
    }
}

/// A plain, cloneable snapshot safe to serialize/send over HTTP
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub writes_per_sec: f64,
    pub reads_per_sec: f64,
    pub compaction_count: u64,
    pub wal_size_bytes: u64,
    pub sstable_count: u64,
    pub recovery_time_ms: u64,
    pub total_writes: u64,
    pub total_reads: u64,
}

impl MetricsSnapshot {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"writes_per_sec":{:.2},"reads_per_sec":{:.2},"compaction_count":{},"wal_size_bytes":{},"sstable_count":{},"recovery_time_ms":{},"total_writes":{},"total_reads":{}}}"#,
            self.writes_per_sec,
            self.reads_per_sec,
            self.compaction_count,
            self.wal_size_bytes,
            self.sstable_count,
            self.recovery_time_ms,
            self.total_writes,
            self.total_reads,
        )
    }
}

/// Shared metrics handle — pass this Arc clone to every component
pub type SharedMetrics = Arc<Mutex<EngineMetrics>>;

pub fn new_shared_metrics() -> SharedMetrics {
    Arc::new(Mutex::new(EngineMetrics::new()))
}