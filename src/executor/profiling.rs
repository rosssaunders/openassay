use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default)]
struct Metric {
    count: u64,
    nanos: u128,
}

static ENABLED: OnceLock<bool> = OnceLock::new();
static METRICS: OnceLock<Mutex<HashMap<&'static str, Metric>>> = OnceLock::new();

pub struct SpanGuard {
    name: &'static str,
    start: Option<Instant>,
}

pub fn is_enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("OPENASSAY_EXEC_PROFILE").is_some())
}

pub fn span(name: &'static str) -> SpanGuard {
    if is_enabled() {
        SpanGuard {
            name,
            start: Some(Instant::now()),
        }
    } else {
        SpanGuard { name, start: None }
    }
}

pub fn record_duration(name: &'static str, nanos: u128) {
    if !is_enabled() {
        return;
    }
    let metrics = METRICS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut metrics = metrics.lock().expect("profiling metrics mutex poisoned");
    let metric = metrics.entry(name).or_default();
    metric.count += 1;
    metric.nanos += nanos;
}

pub fn reset() {
    if let Some(metrics) = METRICS.get() {
        metrics
            .lock()
            .expect("profiling metrics mutex poisoned")
            .clear();
    }
}

pub fn report() -> String {
    let Some(metrics) = METRICS.get() else {
        return "profiling disabled or no metrics recorded".to_string();
    };
    let metrics = metrics.lock().expect("profiling metrics mutex poisoned");
    if metrics.is_empty() {
        return "no profiling samples recorded".to_string();
    }

    let total_nanos = metrics.values().map(|metric| metric.nanos).sum::<u128>();
    let mut rows = metrics
        .iter()
        .map(|(name, metric)| (*name, *metric))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| right.1.nanos.cmp(&left.1.nanos).then(left.0.cmp(right.0)));

    let mut output = String::new();
    let _ = writeln!(
        output,
        "total_profiled_ms={:.3}",
        total_nanos as f64 / 1_000_000.0
    );
    for (name, metric) in rows {
        let total_ms = metric.nanos as f64 / 1_000_000.0;
        let avg_us = if metric.count == 0 {
            0.0
        } else {
            metric.nanos as f64 / metric.count as f64 / 1_000.0
        };
        let pct = if total_nanos == 0 {
            0.0
        } else {
            metric.nanos as f64 * 100.0 / total_nanos as f64
        };
        let _ = writeln!(
            output,
            "{name}: total_ms={total_ms:.3} pct={pct:.2} count={} avg_us={avg_us:.3}",
            metric.count
        );
    }
    output
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            record_duration(self.name, start.elapsed().as_nanos());
        }
    }
}
