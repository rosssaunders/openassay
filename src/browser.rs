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
    rows: Vec<Vec<Option<String>>>,
    command_tag: String,
    rows_affected: u64,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = browse_catalog_json))]
pub async fn browse_catalog_json(path: &str) -> String {
    browse_catalog_json_internal(path).await
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = browse_catalog))]
pub async fn browse_catalog(path: &str) -> String {
    browse_catalog_json(path).await
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
    let mut current_rows: Vec<Vec<Option<String>>> = Vec::new();

    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                current_columns = Some(fields.into_iter().map(|field| field.name).collect());
                current_rows.clear();
            }
            BackendMessage::DataRow { values } => {
                current_rows.push(values.into_iter().map(Some).collect());
            }
            BackendMessage::DataRowBinary { values } => {
                current_rows.push(
                    values
                        .into_iter()
                        .map(|value| match value {
                            None => None,
                            Some(bytes) => {
                                Some(String::from_utf8(bytes.clone()).unwrap_or_else(|_| {
                                    format!(
                                        "\\x{}",
                                        bytes
                                            .iter()
                                            .map(|b| format!("{b:02x}"))
                                            .collect::<String>()
                                    )
                                }))
                            }
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
                row.first().and_then(|v| v.clone()).unwrap_or_default()
            } else {
                row.iter()
                    .map(|v| v.as_deref().unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("\t")
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

#[cfg(not(target_arch = "wasm32"))]
async fn browse_catalog_json_internal(path: &str) -> String {
    match crate::catalog::iceberg::browse_iceberg_catalogs(path).await {
        Ok(catalogs) => {
            let payload = catalogs
                .into_iter()
                .map(|catalog| {
                    json!({
                        "name": catalog.name,
                        "kind": "iceberg",
                        "location": catalog.location,
                        "namespaces": catalog.namespaces.into_iter().map(|namespace| {
                            json!({
                                "name": namespace.schema.name(),
                                "namespace_path": namespace.namespace_path,
                                "tables": namespace.tables.into_iter().map(|table| {
                                    json!({
                                        "name": table.table.name(),
                                        "schema_name": table.table.schema_name(),
                                        "kind": "BASE TABLE",
                                        "location": table.location,
                                        "columns": table.table.columns().iter().map(|column| {
                                            json!({
                                                "name": column.name(),
                                                "ordinal": column.ordinal() + 1,
                                                "nullable": column.nullable(),
                                                "data_type": browser_type_name(column.type_signature()),
                                            })
                                        }).collect::<Vec<_>>(),
                                        "metadata": {
                                            "table_root": table.metadata.table_root,
                                            "metadata_file": table.metadata.metadata_file,
                                            "table_uuid": table.metadata.table_uuid,
                                            "format_version": table.metadata.format_version,
                                            "last_updated_ms": table.metadata.last_updated_ms,
                                            "current_schema_id": table.metadata.current_schema_id,
                                            "partition_spec": table.metadata.partition_spec_json,
                                            "snapshot_count": table.metadata.snapshot_count,
                                            "total_data_files": table.metadata.total_data_files,
                                            "partition_columns": table.metadata.partition_columns,
                                            "column_aliases": table.metadata.column_aliases,
                                        }
                                    })
                                }).collect::<Vec<_>>(),
                            })
                        }).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "ok": true,
                "catalogs": payload,
            })
            .to_string()
        }
        Err(error) => json!({
            "ok": false,
            "error": error.message,
            "catalogs": [],
        })
        .to_string(),
    }
}

#[cfg(target_arch = "wasm32")]
async fn browse_catalog_json_internal(_path: &str) -> String {
    json!({
        "ok": false,
        "error": "catalog browsing is not supported on wasm targets",
        "catalogs": [],
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// FDW WASM bindings
// ---------------------------------------------------------------------------

/// Register a foreign data wrapper by name.
///
/// In WASM builds the `callback` parameter is a JavaScript function with
/// the signature `(tableName: string, options: string) => string[][]` which
/// returns an array of rows (each row is an array of string column values).
///
/// In native builds, `callback` is ignored and only the wrapper name is
/// registered as a no-op placeholder.
///
/// # JavaScript usage
///
/// ```javascript
/// register_fdw("my_fdw", (table, opts) => [["1","hello"],["2","world"]]);
/// await execute_sql(`
///   CREATE SERVER myserver FOREIGN DATA WRAPPER my_fdw;
///   CREATE FOREIGN TABLE ft (id int8, name text) SERVER myserver;
///   SELECT * FROM ft;
/// `);
/// ```
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = register_fdw)]
pub fn register_fdw(name: &str, callback: js_sys::Function) -> String {
    use std::sync::Arc;

    use crate::foreign::{
        FdwError, ForeignDataWrapper, ForeignScan, ForeignTableDef, Qual, with_fdw_write,
    };
    use crate::storage::tuple::ScalarValue;

    struct JsForeignScan {
        rows: std::vec::IntoIter<Vec<ScalarValue>>,
    }

    impl ForeignScan for JsForeignScan {
        fn iterate(&mut self) -> Option<Vec<ScalarValue>> {
            self.rows.next()
        }
        fn end(&mut self) {}
    }

    struct JsFdw {
        name: String,
        callback: js_sys::Function,
    }

    // Safety: WASM is single-threaded.
    unsafe impl Send for JsFdw {}
    unsafe impl Sync for JsFdw {}

    impl ForeignDataWrapper for JsFdw {
        fn name(&self) -> &str {
            &self.name
        }
        fn begin_scan(
            &self,
            table: &ForeignTableDef,
            _quals: &[Qual],
        ) -> Result<Box<dyn ForeignScan>, FdwError> {
            let options_json = serde_json::to_string(&table.options).unwrap_or_default();
            let this = wasm_bindgen::JsValue::null();
            let table_name = wasm_bindgen::JsValue::from_str(&format!("{}", table.table_oid));
            let opts = wasm_bindgen::JsValue::from_str(&options_json);
            let result = self
                .callback
                .call2(&this, &table_name, &opts)
                .map_err(|e| FdwError {
                    message: format!("JS FDW callback error: {:?}", e),
                })?;

            // Expect result to be an array of arrays of strings.
            // Values are returned as ScalarValue::Text — the executor handles
            // type coercion (implicit casts) when evaluating expressions, similar
            // to how PostgreSQL's file_fdw reads everything as text and relies
            // on input functions for conversion.
            let outer = js_sys::Array::from(&result);
            let mut rows = Vec::new();
            for i in 0..outer.length() {
                let inner = js_sys::Array::from(&outer.get(i));
                let mut row = Vec::new();
                for j in 0..inner.length() {
                    let val = inner.get(j);
                    if val.is_null() || val.is_undefined() {
                        row.push(ScalarValue::Null);
                    } else {
                        row.push(ScalarValue::Text(
                            val.as_string().unwrap_or_else(|| format!("{:?}", val)),
                        ));
                    }
                }
                rows.push(row);
            }

            Ok(Box::new(JsForeignScan {
                rows: rows.into_iter(),
            }))
        }
    }

    let wrapper = Arc::new(JsFdw {
        name: name.to_string(),
        callback,
    });

    with_fdw_write(|reg| {
        reg.register_wrapper(wrapper);
    });

    "OK".to_string()
}

/// Register a foreign data wrapper by name (native stub).
///
/// On native targets, FDW implementations are registered via the Rust API
/// directly using `foreign::with_fdw_write`.  This WASM-only stub prevents
/// import errors in dual-target builds.
#[cfg(not(target_arch = "wasm32"))]
pub fn register_fdw(_name: &str) -> String {
    "OK".to_string()
}

fn browser_type_name(signature: crate::catalog::TypeSignature) -> &'static str {
    match signature {
        crate::catalog::TypeSignature::Bool => "boolean",
        crate::catalog::TypeSignature::Int8 => "bigint",
        crate::catalog::TypeSignature::Float8 => "double precision",
        crate::catalog::TypeSignature::Numeric => "numeric",
        crate::catalog::TypeSignature::Text => "text",
        crate::catalog::TypeSignature::Date => "date",
        crate::catalog::TypeSignature::Timestamp => "timestamp without time zone",
        crate::catalog::TypeSignature::Vector(_) => "vector",
    }
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
