//! opt-in の processing-latency ヒストグラム。
//!
//! handle 1 回の所要時間を log2(ns) バケットに記録する。**単一消費者(所有コアスレッド)だけが
//! 書く**ので、`fetch_add(Relaxed)` でも競合ゼロ。読み手(任意スレッド)は relaxed load。
//!
//! `Instant::now()` のホットコスト(~数十 ns)があるため **既定 OFF**。`SpawnBuilder::instrumented()`
//! で明示的に有効化したときだけ計測する(zero-overhead by default)。

use std::sync::atomic::{AtomicU64, Ordering};

const BUCKETS: usize = 64;

/// log2(ns) バケットの遅延ヒストグラム。
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

    /// 1 サンプルを記録(所有コアスレッドのみが呼ぶ = 単一ライター)。
    pub(crate) fn record(&self, ns: u64) {
        let idx = if ns == 0 {
            0
        } else {
            (63 - ns.leading_zeros()) as usize
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        // 単一ライターなので load→max→store で十分。
        if ns > self.max_ns.load(Ordering::Relaxed) {
            self.max_ns.store(ns, Ordering::Relaxed);
        }
    }

    /// 現在のスナップショット(分位はバケット粒度の近似)。
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
                    // バケット i = [2^i, 2^(i+1)) ns。中点近似。
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

/// [`LatencyHistogram::snapshot`] の結果(単位は ns、分位はバケット粒度の近似)。
#[derive(Debug, Clone, Copy)]
pub struct LatencySnapshot {
    pub count: u64,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}
