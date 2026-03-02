use std::sync::{Mutex, OnceLock};

/// Global mutex to serialize ALL regression tests in this binary.
/// All test modules share global mutable state (catalog, storage, extensions)
/// so they must not run concurrently.
fn regression_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[path = "regression/basic_sql.rs"]
mod basic_sql;

#[path = "regression/pg_compat.rs"]
mod pg_compat;

#[path = "regression/pg_compat_simplified.rs"]
mod pg_compat_simplified;
