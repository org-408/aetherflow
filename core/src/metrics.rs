//! Opt-in processing-latency histogram.
//!
//! Records the time taken by a single `handle` call into log2(ns) buckets.
//! **Only a single consumer (the owning core's thread) writes**, so even
//! `fetch_add(Relaxed)` has zero contention. Readers (any thread) use a relaxed
//! load.
//!
//! `Instant::now()` has a hot cost (~tens of ns), so it is **OFF by default**.
//! Measurement happens only when explicitly enabled via
//! `SpawnBuilder::instrumented()` (zero-overhead by default).

use std::sync::atomic::{AtomicU64, Ordering};

const BUCKETS: usize = 64;

/// Latency histogram with log2(ns) buckets.
pub struct LatencyHistogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl LatencyHistogram {
    pub(crate) fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }

    /// Record one sample (called only by the owning core's thread = single writer).
    pub(crate) fn record(&self, ns: u64) {
        let idx = if ns == 0 {
            0
        } else {
            (63 - ns.leading_zeros()) as usize
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        // Single writer, so load→max→store is sufficient.
        if ns > self.max_ns.load(Ordering::Relaxed) {
            self.max_ns.store(ns, Ordering::Relaxed);
        }
    }

    /// Current snapshot (percentiles are approximated at bucket granularity).
    pub fn snapshot(&self) -> LatencySnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let counts: [u64; BUCKETS] =
            std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed));
        let pct = |q: f64| -> u64 {
            if count == 0 {
                return 0;
            }
            let target = (count as f64 * q) as u64;
            let mut acc = 0u64;
            for (i, &c) in counts.iter().enumerate() {
                acc += c;
                if acc >= target {
                    // Bucket i = [2^i, 2^(i+1)) ns. Midpoint approximation.
                    return (1u64 << i).saturating_add(1u64 << i.saturating_sub(1));
                }
            }
            self.max_ns.load(Ordering::Relaxed)
        };
        LatencySnapshot {
            count,
            mean_ns: self
                .sum_ns
                .load(Ordering::Relaxed)
                .checked_div(count)
                .unwrap_or(0),
            p50_ns: pct(0.50),
            p99_ns: pct(0.99),
            p999_ns: pct(0.999),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}

/// Result of [`LatencyHistogram::snapshot`] (values in ns, percentiles approximated at bucket granularity).
#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub count: u64,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}
