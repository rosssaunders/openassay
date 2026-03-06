use std::sync::{Mutex, OnceLock};

#[path = "benchmark/tpch_queries.rs"]
mod tpch_queries;

#[path = "benchmark/clickbench_queries.rs"]
mod clickbench_queries;

#[path = "benchmark/clickbench_realdata.rs"]
mod clickbench_realdata;

fn benchmark_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
