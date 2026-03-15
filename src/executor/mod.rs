use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

pub mod bytecode_expr;
pub mod column_batch;
pub mod column_filter;
pub mod columnar_agg;
pub mod exec_expr;
pub mod exec_grouping;
pub mod exec_main;
pub mod exec_scan;
pub mod exec_srf;
pub mod hash_join;
pub mod node_agg;
pub mod node_append;
pub mod node_cte;
pub mod node_hash_join;
pub mod node_limit;
pub mod node_merge_join;
pub mod node_modify_table;
pub mod node_nested_loop;
pub mod node_result;
pub mod node_set_op;
pub mod node_sort;
pub mod node_subquery;
pub mod node_window_agg;
pub mod pipeline;
pub mod profiling;
pub mod window_eval;

static COLUMNAR_EXECUTION_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn columnar_execution_enabled() -> bool {
    COLUMNAR_EXECUTION_ENABLED.load(AtomicOrdering::Relaxed)
}

pub fn set_columnar_execution_enabled(enabled: bool) {
    COLUMNAR_EXECUTION_ENABLED.store(enabled, AtomicOrdering::Relaxed);
}
