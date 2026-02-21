use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::parser::ast::{SetStatement, ShowStatement};
use crate::tcop::engine::{EngineError, QueryResult, ScalarValue};

static GLOBAL_GUC: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn global_guc() -> &'static RwLock<HashMap<String, String>> {
    GLOBAL_GUC.get_or_init(|| RwLock::new(default_gucs()))
}

fn default_gucs() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("server_version".to_string(), "16.0".to_string());
    m.insert("server_encoding".to_string(), "UTF8".to_string());
    m.insert("client_encoding".to_string(), "UTF8".to_string());
    m.insert("is_superuser".to_string(), "on".to_string());
    m.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    m.insert("IntervalStyle".to_string(), "postgres".to_string());
    m.insert("TimeZone".to_string(), "UTC".to_string());
    m.insert("integer_datetimes".to_string(), "on".to_string());
    m.insert("standard_conforming_strings".to_string(), "on".to_string());
    m.insert("search_path".to_string(), "\"$user\", public".to_string());
    m.insert("application_name".to_string(), String::new());
    m
}

/// Resets all GUC variables to their default values.
pub fn reset_all_gucs() {
    let mut guc = global_guc().write().expect("guc lock poisoned");
    *guc = default_gucs();
}

pub(crate) fn with_guc_read<T>(f: impl FnOnce(&HashMap<String, String>) -> T) -> T {
    let guc = global_guc().read().expect("guc lock poisoned");
    f(&guc)
}

pub async fn execute_set(set_stmt: &SetStatement) -> Result<QueryResult, EngineError> {
    let mut guc = global_guc().write().expect("guc lock poisoned");
    guc.insert(set_stmt.name.clone(), set_stmt.value.clone());
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "SET".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_show(show_stmt: &ShowStatement) -> Result<QueryResult, EngineError> {
    let guc = global_guc().read().expect("guc lock poisoned");
    let value = guc
        .get(&show_stmt.name)
        .or_else(|| {
            guc.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&show_stmt.name))
                .map(|(_, v)| v)
        })
        .cloned()
        .unwrap_or_default();
    Ok(QueryResult {
        columns: vec![show_stmt.name.clone()],
        rows: vec![vec![ScalarValue::Text(value)]],
        command_tag: "SHOW".to_string(),
        rows_affected: 0,
    })
}
