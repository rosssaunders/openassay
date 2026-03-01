use std::cell::RefCell;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::json;

use crate::arrow::record_batch::query_result_to_arrow_ipc;
use crate::parser::sql_parser::parse_statement;
use crate::tcop::engine::{
    EngineStateSnapshot, execute_planned_query, plan_statement, restore_state, snapshot_state,
};
use crate::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[cfg(target_arch = "wasm32")]
use crate::executor::exec_expr::process_pending_ws_callbacks;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

const SNAPSHOT_HEADER: &str = "POSTGRUST_BROWSER_SNAPSHOT_V1";

thread_local! {
    static BASELINE_SNAPSHOT: RefCell<Option<EngineStateSnapshot>> = const { RefCell::new(None) };
    static SNAPSHOT_REPLAY_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserQueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    command_tag: String,
    rows_affected: u64,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub async fn execute_sql(sql: &str) -> String {
    execute_sql_internal(sql, true).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = execute_sql_json))]
pub async fn execute_sql_json(sql: &str) -> String {
    execute_sql_json_internal(sql, true).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = run_sql_json))]
pub async fn run_sql_json(sql: &str) -> String {
    execute_sql_json(sql).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = exec_sql))]
pub async fn exec_sql(sql: &str) -> String {
    execute_sql(sql).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = run_sql))]
pub async fn run_sql(sql: &str) -> String {
    execute_sql(sql).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = execute_sql_http))]
pub async fn execute_sql_http(sql: &str) -> String {
    execute_sql(sql).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = run_sql_http))]
pub async fn run_sql_http(sql: &str) -> String {
    execute_sql_http(sql).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = execute_sql_http_json))]
pub async fn execute_sql_http_json(sql: &str) -> String {
    execute_sql_json_internal(sql, true).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = run_sql_http_json))]
pub async fn run_sql_http_json(sql: &str) -> String {
    execute_sql_json_internal(sql, true).await
}

/// Execute a SQL statement and return the result as Arrow IPC file format bytes.
///
/// The first statement that produces a result set is converted to an Arrow
/// [`RecordBatch`](arrow::record_batch::RecordBatch) and serialised to the
/// Arrow IPC file format.  Returns an empty `Vec` if no result set was
/// produced or if an error occurred.
///
/// # JavaScript usage (Apache Arrow JS)
///
/// ```javascript
/// const bytes = await wasm.execute_sql_arrow("SELECT * FROM data");
/// const table = arrow.tableFromIPC(new Uint8Array(bytes));
/// ```
#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = execute_sql_arrow))]
pub async fn execute_sql_arrow(sql: &str) -> Vec<u8> {
    execute_sql_arrow_internal(sql).await.unwrap_or_default()
}

async fn execute_sql_arrow_internal(sql: &str) -> Result<Vec<u8>, String> {
    ensure_baseline_snapshot();

    #[cfg(target_arch = "wasm32")]
    process_pending_ws_callbacks().await;

    let statement = parse_statement(sql).map_err(|e| e.to_string())?;
    let planned = plan_statement(statement).map_err(|e| e.to_string())?;
    let result = execute_planned_query(&planned, &[])
        .await
        .map_err(|e| e.to_string())?;

    let trimmed = sql.trim();
    if !trimmed.is_empty() {
        push_snapshot_log(trimmed.to_string());
    }

    query_result_to_arrow_ipc(&result).map_err(|e| e.to_string())
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = export_state_snapshot))]
pub fn export_state_snapshot() -> String {
    ensure_baseline_snapshot();
    let mut out = String::from(SNAPSHOT_HEADER);
    for entry in snapshot_log_entries() {
        out.push('\n');
        out.push_str(&BASE64_STANDARD.encode(entry.as_bytes()));
    }
    out
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = export_state))]
pub fn export_state() -> String {
    export_state_snapshot()
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = import_state_snapshot))]
pub async fn import_state_snapshot(snapshot: &str) -> String {
    ensure_baseline_snapshot();
    match import_state_snapshot_impl(snapshot).await {
        Ok(message) => message,
        Err(message) => format!("Execution error: {message}"),
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = import_state))]
pub async fn import_state(snapshot: &str) -> String {
    import_state_snapshot(snapshot).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = reset_state_snapshot))]
pub fn reset_state_snapshot() -> String {
    ensure_baseline_snapshot();
    restore_state(baseline_snapshot_clone());
    clear_snapshot_log();
    "OK".to_string()
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = reset_state))]
pub fn reset_state() -> String {
    reset_state_snapshot()
}

async fn execute_sql_internal(sql: &str, record_snapshot: bool) -> String {
    match execute_sql_results_internal(sql, record_snapshot).await {
        Ok(results) => render_results(&results),
        Err(message) => format!("Execution error: {message}"),
    }
}

async fn execute_sql_json_internal(sql: &str, record_snapshot: bool) -> String {
    match execute_sql_results_internal(sql, record_snapshot).await {
        Ok(results) => render_results_json_payload(&results),
        Err(message) => json!({
            "ok": false,
            "error": message,
            "rendered": format!("Execution error: {message}"),
            "results": [],
        })
        .to_string(),
    }
}

async fn execute_sql_results_internal(
    sql: &str,
    record_snapshot: bool,
) -> Result<Vec<BrowserQueryResult>, String> {
    ensure_baseline_snapshot();
    let results = execute_simple_query(sql).await?;
    if record_snapshot {
        let trimmed = sql.trim();
        if !trimmed.is_empty() {
            push_snapshot_log(trimmed.to_string());
        }
    }
    Ok(results)
}

async fn execute_simple_query(sql: &str) -> Result<Vec<BrowserQueryResult>, String> {
    // Process any pending WebSocket on_message callbacks before running user SQL,
    // so that incoming data is available (e.g. INSERTed into tables) by query time.
    #[cfg(target_arch = "wasm32")]
    process_pending_ws_callbacks().await;

    let mut session = PostgresSession::new();
    let messages = session
        .run([FrontendMessage::Query {
            sql: sql.to_string(),
        }])
        .await;

    let mut results = Vec::new();
    let mut current_columns: Option<Vec<String>> = None;
    let mut current_rows: Vec<Vec<String>> = Vec::new();

    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                current_columns = Some(fields.into_iter().map(|field| field.name).collect());
                current_rows.clear();
            }
            BackendMessage::DataRow { values } => {
                current_rows.push(values);
            }
            BackendMessage::DataRowBinary { values } => {
                current_rows.push(
                    values
                        .into_iter()
                        .map(|value| match value {
                            None => "NULL".to_string(),
                            Some(bytes) => String::from_utf8(bytes.clone()).unwrap_or_else(|_| {
                                format!(
                                    "\\x{}",
                                    bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
                                )
                            }),
                        })
                        .collect(),
                );
            }
            BackendMessage::CommandComplete { tag, rows } => {
                results.push(BrowserQueryResult {
                    columns: current_columns.take().unwrap_or_default(),
                    rows: std::mem::take(&mut current_rows),
                    command_tag: tag,
                    rows_affected: rows,
                });
            }
            BackendMessage::ErrorResponse { message, .. } => return Err(message),
            _ => {}
        }
    }

    if results.is_empty() {
        return Ok(vec![BrowserQueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "EMPTY".to_string(),
            rows_affected: 0,
        }]);
    }

    Ok(results)
}

async fn import_state_snapshot_impl(snapshot: &str) -> Result<String, String> {
    let trimmed = snapshot.trim();
    if trimmed.is_empty() {
        restore_state(baseline_snapshot_clone());
        clear_snapshot_log();
        return Ok("OK (empty snapshot)".to_string());
    }

    let mut lines = trimmed.lines();
    let header = lines.next().unwrap_or_default().trim();
    if header != SNAPSHOT_HEADER {
        return Err("invalid snapshot header".to_string());
    }

    let mut replay_entries: Vec<(usize, String)> = Vec::new();
    for (idx, raw_line) in lines.enumerate() {
        let line_no = idx + 2;
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let decoded = BASE64_STANDARD
            .decode(line)
            .map_err(|_| format!("invalid base64 payload at snapshot line {line_no}"))?;
        let statement = String::from_utf8(decoded)
            .map_err(|_| format!("invalid utf8 payload at snapshot line {line_no}"))?;
        replay_entries.push((line_no, statement));
    }

    restore_state(baseline_snapshot_clone());
    clear_snapshot_log();

    let mut replayed = Vec::with_capacity(replay_entries.len());
    for (line_no, statement) in replay_entries {
        let out = execute_sql_internal(&statement, false).await;
        if let Some(error) = out.strip_prefix("Execution error:") {
            restore_state(baseline_snapshot_clone());
            clear_snapshot_log();
            return Err(format!(
                "snapshot replay failed at line {}: {}",
                line_no,
                error.trim()
            ));
        }
        replayed.push(statement);
    }
    replace_snapshot_log(replayed.clone());

    Ok(format!("OK (replayed {} statements)", replayed.len()))
}

fn ensure_baseline_snapshot() {
    BASELINE_SNAPSHOT.with(|slot: &RefCell<Option<EngineStateSnapshot>>| {
        if slot.borrow().is_none() {
            slot.replace(Some(snapshot_state()));
        }
    });
}

fn baseline_snapshot_clone() -> EngineStateSnapshot {
    ensure_baseline_snapshot();
    BASELINE_SNAPSHOT.with(|slot: &RefCell<Option<EngineStateSnapshot>>| {
        match slot.borrow().as_ref() {
            Some(snapshot) => snapshot.clone(),
            None => unreachable!("baseline snapshot should be initialized"),
        }
    })
}

fn push_snapshot_log(entry: String) {
    SNAPSHOT_REPLAY_LOG.with(|log| {
        log.borrow_mut().push(entry);
    });
}

fn replace_snapshot_log(entries: Vec<String>) {
    SNAPSHOT_REPLAY_LOG.with(|log| {
        log.replace(entries);
    });
}

fn clear_snapshot_log() {
    SNAPSHOT_REPLAY_LOG.with(|log| {
        log.borrow_mut().clear();
    });
}

fn snapshot_log_entries() -> Vec<String> {
    SNAPSHOT_REPLAY_LOG.with(|log| log.borrow().clone())
}

fn render_results(results: &[BrowserQueryResult]) -> String {
    let mut out = String::new();
    for (idx, result) in results.iter().enumerate() {
        if idx > 0 {
            out.push_str("\n\n");
        }
        out.push_str(&render_query_result(result));
    }
    out
}

fn render_query_result(result: &BrowserQueryResult) -> String {
    if result.columns.is_empty() && result.rows.is_empty() {
        return format!("{} {}", result.command_tag, result.rows_affected);
    }

    if result.rows.is_empty() {
        return format!("{} {}", result.command_tag, result.rows_affected);
    }

    result
        .rows
        .iter()
        .map(|row| {
            if row.len() <= 1 {
                row.first().cloned().unwrap_or_default()
            } else {
                row.join("\t")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_results_json_payload(results: &[BrowserQueryResult]) -> String {
    let result_rows = results
        .iter()
        .map(|result| {
            json!({
                "columns": result.columns.clone(),
                "rows": result.rows.clone(),
                "command_tag": result.command_tag.clone(),
                "rows_affected": result.rows_affected,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "ok": true,
        "rendered": render_results(results),
        "results": result_rows,
    })
    .to_string()
}

#[cfg(test)]
fn reset_browser_snapshot_state_for_tests() {
    crate::catalog::reset_global_catalog_for_tests();
    crate::tcop::engine::reset_global_storage_for_tests();
    BASELINE_SNAPSHOT.with(|slot: &RefCell<Option<EngineStateSnapshot>>| {
        slot.replace(Some(snapshot_state()));
    });
    clear_snapshot_log();
}

#[cfg(test)]
mod tests {
    use super::{
        execute_sql, execute_sql_json, export_state_snapshot, import_state_snapshot,
        reset_browser_snapshot_state_for_tests, reset_state_snapshot,
    };
    use std::future::Future;

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should start")
            .block_on(future)
    }

    #[test]
    fn browser_api_executes_multi_statement_sql() {
        crate::catalog::with_global_state_lock(|| {
            reset_browser_snapshot_state_for_tests();

            let out = block_on(execute_sql(
                "create table browser_users (id int8, name text);
                 insert into browser_users values (1, 'Ada');
                 select * from browser_users;",
            ));
            assert!(out.contains("Ada"));
            assert!(out.contains("1\tAda"));
            assert!(!out.contains("-+-"));
        });
    }

    #[test]
    fn snapshot_export_import_roundtrip_restores_state() {
        crate::catalog::with_global_state_lock(|| {
            reset_browser_snapshot_state_for_tests();

            let create_out = block_on(execute_sql(
                "create table browser_snap_users (id int8, name text);
                 insert into browser_snap_users values (1, 'Ada'), (2, 'Linus');",
            ));
            assert!(!create_out.starts_with("Execution error:"));

            let snapshot = export_state_snapshot();
            assert!(snapshot.starts_with("POSTGRUST_BROWSER_SNAPSHOT_V1"));

            let reset_out = reset_state_snapshot();
            assert_eq!(reset_out, "OK");

            let import_out = block_on(import_state_snapshot(&snapshot));
            assert!(!import_out.starts_with("Execution error:"));

            let select_out = block_on(execute_sql("select * from browser_snap_users order by id;"));
            assert!(select_out.contains("Ada"));
            assert!(select_out.contains("Linus"));
        });
    }

    #[test]
    fn execute_sql_json_returns_structured_results() {
        crate::catalog::with_global_state_lock(|| {
            reset_browser_snapshot_state_for_tests();
            let out = block_on(execute_sql_json("select 1 as one, 2 as two;"));
            let parsed: serde_json::Value =
                serde_json::from_str(&out).expect("json payload should parse");

            assert_eq!(parsed.get("ok"), Some(&serde_json::Value::Bool(true)));
            assert_eq!(
                parsed["results"][0]["columns"],
                serde_json::json!(["one", "two"])
            );
            assert_eq!(
                parsed["results"][0]["rows"],
                serde_json::json!([["1", "2"]])
            );
            assert_eq!(parsed["rendered"], serde_json::json!("1\t2"));
        });
    }
}
