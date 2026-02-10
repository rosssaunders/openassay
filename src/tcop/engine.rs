use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{OnceLock, RwLock};

use crate::commands::sequence::{with_sequences_read, with_sequences_write, SequenceState};
pub(crate) use crate::storage::heap::{with_storage_read, with_storage_write};
use crate::utils::adt::datetime::{
    datetime_from_epoch_seconds, format_date, format_timestamp, parse_datetime_text,
};
use crate::executor::exec_main::{
    combine_scopes, evaluate_from_clause, evaluate_table_expression, scope_for_table_row,
    scope_for_table_row_with_qualifiers,
};
pub(crate) use crate::executor::exec_expr::{eval_expr, EvalScope};
pub(crate) use crate::utils::adt::misc::truthy;
pub(crate) use crate::executor::exec_main::execute_query;
use crate::catalog::oid::Oid;
use crate::catalog::with_catalog_write;
use crate::catalog::{
    Column, ColumnSpec, SearchPath, TableKind, TypeSignature, with_catalog_read,
};
use crate::parser::ast::{
    BinaryOp, ConflictTarget, DeleteStatement, Expr, ForeignKeyAction, FunctionParam,
    FunctionReturnType, GroupByExpr, InsertSource, InsertStatement, JoinCondition,
    MergeStatement, MergeWhenClause, OnConflictClause, OrderByExpr, Query, QueryExpr, SelectItem,
    SelectStatement, SetOperator, Statement, TableConstraint, TableExpression, TableFunctionRef,
    UnaryOp, UpdateStatement,
    // DiscardStatement, DoStatement, ListenStatement, NotifyStatement, UnlistenStatement used in pattern matching
};
use crate::security::{self, RlsCommand, TablePrivilege};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError {
    pub message: String,
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for EngineError {}

pub use crate::storage::tuple::{CopyBinaryColumn, CopyBinarySnapshot, ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
    pub command_tag: String,
    pub rows_affected: u64,
}

const PG_BOOL_OID: u32 = 16;
const PG_INT8_OID: u32 = 20;
const PG_TEXT_OID: u32 = 25;
const PG_FLOAT8_OID: u32 = 701;
const PG_DATE_OID: u32 = 1082;
const PG_TIMESTAMP_OID: u32 = 1114;

#[derive(Debug, Clone)]
pub struct PlannedQuery {
    statement: Statement,
    columns: Vec<String>,
    column_type_oids: Vec<u32>,
    returns_data: bool,
    command_tag: String,
}

impl PlannedQuery {
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn column_type_oids(&self) -> &[u32] {
        &self.column_type_oids
    }

    pub fn returns_data(&self) -> bool {
        self.returns_data
    }

    pub fn command_tag(&self) -> &str {
        &self.command_tag
    }
}

pub fn plan_statement(statement: Statement) -> Result<PlannedQuery, EngineError> {
    let (columns, column_type_oids, returns_data, command_tag) = match &statement {
        Statement::Query(query) => {
            let output = derive_query_output_columns(query)?;
            (
                output.iter().map(|col| col.name.clone()).collect(),
                output.iter().map(|col| col.type_oid).collect(),
                true,
                "SELECT".to_string(),
            )
        }
        Statement::CreateTable(_) => (Vec::new(), Vec::new(), false, "CREATE TABLE".to_string()),
        Statement::CreateSchema(_) => (Vec::new(), Vec::new(), false, "CREATE SCHEMA".to_string()),
        Statement::CreateIndex(_) => (Vec::new(), Vec::new(), false, "CREATE INDEX".to_string()),
        Statement::CreateSequence(_) => {
            (Vec::new(), Vec::new(), false, "CREATE SEQUENCE".to_string())
        }
        Statement::CreateView(create) => (
            Vec::new(),
            Vec::new(),
            false,
            if create.materialized {
                "CREATE MATERIALIZED VIEW".to_string()
            } else {
                "CREATE VIEW".to_string()
            },
        ),
        Statement::RefreshMaterializedView(_) => (
            Vec::new(),
            Vec::new(),
            false,
            "REFRESH MATERIALIZED VIEW".to_string(),
        ),
        Statement::AlterSequence(_) => {
            (Vec::new(), Vec::new(), false, "ALTER SEQUENCE".to_string())
        }
        Statement::AlterView(alter) => (
            Vec::new(),
            Vec::new(),
            false,
            if alter.materialized {
                "ALTER MATERIALIZED VIEW".to_string()
            } else {
                "ALTER VIEW".to_string()
            },
        ),
        Statement::Insert(insert) => {
            let columns = derive_dml_returning_columns(&insert.table_name, &insert.returning)?;
            let oids =
                derive_dml_returning_column_type_oids(&insert.table_name, &insert.returning)?;
            (
                columns,
                oids,
                !insert.returning.is_empty(),
                "INSERT".to_string(),
            )
        }
        Statement::Update(update) => {
            let columns = derive_dml_returning_columns(&update.table_name, &update.returning)?;
            let oids =
                derive_dml_returning_column_type_oids(&update.table_name, &update.returning)?;
            (
                columns,
                oids,
                !update.returning.is_empty(),
                "UPDATE".to_string(),
            )
        }
        Statement::Delete(delete) => {
            let columns = derive_dml_returning_columns(&delete.table_name, &delete.returning)?;
            let oids =
                derive_dml_returning_column_type_oids(&delete.table_name, &delete.returning)?;
            (
                columns,
                oids,
                !delete.returning.is_empty(),
                "DELETE".to_string(),
            )
        }
        Statement::Merge(merge) => {
            let columns = derive_dml_returning_columns(&merge.target_table, &merge.returning)?;
            let oids =
                derive_dml_returning_column_type_oids(&merge.target_table, &merge.returning)?;
            (
                columns,
                oids,
                !merge.returning.is_empty(),
                "MERGE".to_string(),
            )
        }
        Statement::DropTable(_) => (Vec::new(), Vec::new(), false, "DROP TABLE".to_string()),
        Statement::DropSchema(_) => (Vec::new(), Vec::new(), false, "DROP SCHEMA".to_string()),
        Statement::DropIndex(_) => (Vec::new(), Vec::new(), false, "DROP INDEX".to_string()),
        Statement::DropSequence(_) => (Vec::new(), Vec::new(), false, "DROP SEQUENCE".to_string()),
        Statement::DropView(drop) => (
            Vec::new(),
            Vec::new(),
            false,
            if drop.materialized {
                "DROP MATERIALIZED VIEW".to_string()
            } else {
                "DROP VIEW".to_string()
            },
        ),
        Statement::Truncate(_) => (Vec::new(), Vec::new(), false, "TRUNCATE".to_string()),
        Statement::AlterTable(_) => (Vec::new(), Vec::new(), false, "ALTER TABLE".to_string()),
        Statement::Explain(_) => {
            let cols = vec!["QUERY PLAN".to_string()];
            (cols, vec![PG_TEXT_OID], true, "EXPLAIN".to_string())
        }
        Statement::Set(_) => (Vec::new(), Vec::new(), false, "SET".to_string()),
        Statement::Show(show) => {
            let col_name = show.name.clone();
            (vec![col_name], vec![PG_TEXT_OID], true, "SHOW".to_string())
        }
        Statement::Discard(_) => (Vec::new(), Vec::new(), false, "DISCARD".to_string()),
        Statement::Do(_) => (Vec::new(), Vec::new(), false, "DO".to_string()),
        Statement::Listen(_) => (Vec::new(), Vec::new(), false, "LISTEN".to_string()),
        Statement::Notify(_) => (Vec::new(), Vec::new(), false, "NOTIFY".to_string()),
        Statement::Unlisten(_) => (Vec::new(), Vec::new(), false, "UNLISTEN".to_string()),
        Statement::CreateExtension(_) => (Vec::new(), Vec::new(), false, "CREATE EXTENSION".to_string()),
        Statement::DropExtension(_) => (Vec::new(), Vec::new(), false, "DROP EXTENSION".to_string()),
        Statement::CreateFunction(_) => (Vec::new(), Vec::new(), false, "CREATE FUNCTION".to_string()),
        Statement::Transaction(_) => {
            return Err(EngineError {
                message: "transaction statements must be executed via the session protocol"
                    .to_string(),
            });
        }
    };
    Ok(PlannedQuery {
        statement,
        columns,
        column_type_oids,
        returns_data,
        command_tag,
    })
}

pub fn execute_planned_query<'a>(
    plan: &'a PlannedQuery,
    params: &'a [Option<String>],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, EngineError>> + 'a>> {
    Box::pin(async move {
    let result = match &plan.statement {
        Statement::Query(query) => execute_query(query, params).await?,
        Statement::Insert(insert) => execute_insert(insert, params).await?,
        Statement::Update(update) => execute_update(update, params).await?,
        Statement::Delete(delete) => execute_delete(delete, params).await?,
        Statement::Merge(merge) => execute_merge(merge, params).await?,
        Statement::Transaction(_) => {
            return Err(EngineError {
                message: "transaction statements must be executed via the session protocol"
                    .to_string(),
            });
        }
        _ => crate::tcop::utility::execute_utility_statement(&plan.statement, params).await?,
    };
    Ok(result)
    })
}

// ── Extension & User Function Registry ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UserFunction {
    pub name: Vec<String>,
    pub params: Vec<FunctionParam>,
    pub return_type: Option<FunctionReturnType>,
    pub body: String,
    pub language: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ExtensionRecord {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: String,
}

/// WebSocket connection state for the ws extension
#[derive(Debug, Clone)]
pub struct WsConnection {
    pub id: i64,
    pub url: String,
    pub state: String,
    pub opened_at: String,
    pub messages_in: i64,
    pub messages_out: i64,
    pub on_open: Option<String>,
    pub on_message: Option<String>,
    pub on_close: Option<String>,
    pub inbound_queue: Vec<String>,
    /// Whether this connection uses real I/O (false in tests / simulate mode)
    pub real_io: bool,
}

/// Handle for a real native WebSocket connection (non-wasm32, non-test).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod ws_native {
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;

    /// A native WebSocket handle using tungstenite with a background reader thread.
    pub struct NativeWsHandle {
        pub writer: Arc<Mutex<Option<tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>>>>,
        pub incoming: mpsc::Receiver<String>,
        pub _reader_thread: thread::JoinHandle<()>,
    }

    /// Open a real WebSocket connection. Returns a handle for sending/receiving.
    pub fn open_connection(url: &str) -> Result<NativeWsHandle, String> {
        let (socket, _response) = tungstenite::connect(url)
            .map_err(|e| format!("WebSocket connect failed: {}", e))?;

        let writer = Arc::new(Mutex::new(Some(socket)));
        let reader_writer = Arc::clone(&writer);
        let (tx, rx) = mpsc::channel();

        let reader_thread = thread::spawn(move || {
            loop {
                // We need to read from the socket, but the writer lock holds it.
                // We'll use a pattern where the reader briefly locks to read one message.
                let msg = {
                    let mut guard = reader_writer.lock().unwrap();
                    if let Some(ref mut ws) = *guard {
                        match ws.read() {
                            Ok(tungstenite::Message::Text(t)) => Some(t.to_string()),
                            Ok(tungstenite::Message::Binary(b)) => {
                                Some(String::from_utf8_lossy(&b).to_string())
                            }
                            Ok(tungstenite::Message::Close(_)) => None,
                            Ok(_) => Some(String::new()), // ping/pong/frame - skip
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                };
                match msg {
                    Some(s) if !s.is_empty() => {
                        if tx.send(s).is_err() {
                            break;
                        }
                    }
                    Some(_) => continue, // empty = ping/pong
                    None => break,       // closed or error
                }
            }
        });

        Ok(NativeWsHandle {
            writer,
            incoming: rx,
            _reader_thread: reader_thread,
        })
    }

    pub fn send_message(handle: &NativeWsHandle, msg: &str) -> Result<(), String> {
        let mut guard = handle.writer.lock().unwrap();
        if let Some(ref mut ws) = *guard {
            ws.write(tungstenite::Message::Text(msg.to_string()))
                .map_err(|e| format!("WebSocket send failed: {}", e))?;
            ws.flush().map_err(|e| format!("WebSocket flush failed: {}", e))?;
            Ok(())
        } else {
            Err("connection already closed".to_string())
        }
    }

    pub fn close_connection(handle: &NativeWsHandle) -> Result<(), String> {
        let mut guard = handle.writer.lock().unwrap();
        if let Some(ref mut ws) = *guard {
            let _ = ws.close(None);
            let _ = ws.flush();
        }
        *guard = None;
        Ok(())
    }

    pub fn drain_incoming(handle: &NativeWsHandle) -> Vec<String> {
        let mut msgs = Vec::new();
        while let Ok(m) = handle.incoming.try_recv() {
            msgs.push(m);
        }
        msgs
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Global map of connection id -> native WS handle (non-wasm only)
#[cfg(not(target_arch = "wasm32"))]
static NATIVE_WS_HANDLES: std::sync::OnceLock<std::sync::Mutex<HashMap<i64, ws_native::NativeWsHandle>>> = std::sync::OnceLock::new();

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_ws_handles(
) -> &'static std::sync::Mutex<HashMap<i64, ws_native::NativeWsHandle>> {
    NATIVE_WS_HANDLES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Drain incoming messages from real connections into the inbound_queue
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn drain_native_ws_messages(conn_id: i64) {
    let msgs = {
        let handles = native_ws_handles().lock().unwrap();
        if let Some(handle) = handles.get(&conn_id) {
            ws_native::drain_incoming(handle)
        } else {
            return;
        }
    };
    if !msgs.is_empty() {
        with_ext_write(|ext| {
            if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
                conn.messages_in += msgs.len() as i64;
                conn.inbound_queue.extend(msgs);
            }
        });
    }
}

/// WebSocket implementation for wasm32 (browser) using web_sys::WebSocket.
///
/// # How it works
/// Browser WebSockets are callback-based (onmessage, onopen, etc.). The SQL engine
/// is synchronous. We bridge this by using shared buffers (`Rc<RefCell<...>>`) that
/// callbacks write into, and the engine reads from on the next SQL call.
///
/// The JS event loop runs between WASM calls, so callbacks fire between SQL statements.
/// This means: connect → return to JS → onopen fires → next SQL call sees state="open".
///
/// # Limitation
/// The on_message SQL callback is NOT automatically invoked in WASM. Messages are
/// buffered and must be polled via ws.recv() or ws.messages(). This is because we
/// cannot re-enter the SQL engine from a JS closure callback.
#[cfg(target_arch = "wasm32")]
mod ws_wasm {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use web_sys::{WebSocket, MessageEvent, CloseEvent, ErrorEvent};

    /// Stored closures must be kept alive for the lifetime of the WebSocket connection.
    struct WasmWsClosures {
        _on_open: Closure<dyn FnMut(JsValue)>,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
        _on_close: Closure<dyn FnMut(CloseEvent)>,
        _on_error: Closure<dyn FnMut(ErrorEvent)>,
    }

    pub struct WasmWsHandle {
        pub socket: WebSocket,
        pub messages: Rc<RefCell<Vec<String>>>,
        pub state: Rc<RefCell<String>>,
        _closures: WasmWsClosures,
    }

    pub fn open_connection(url: &str) -> Result<WasmWsHandle, String> {
        let ws = WebSocket::new(url).map_err(|e| format!("WebSocket creation failed: {:?}", e))?;

        let messages: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let state: Rc<RefCell<String>> = Rc::new(RefCell::new("connecting".to_string()));

        // onopen
        let state_clone = Rc::clone(&state);
        let on_open = Closure::<dyn FnMut(JsValue)>::wrap(Box::new(move |_| {
            *state_clone.borrow_mut() = "open".to_string();
        }));
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        // onmessage
        let msgs_clone = Rc::clone(&messages);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
                msgs_clone.borrow_mut().push(String::from(txt));
            }
        }));
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        // onclose
        let state_clone = Rc::clone(&state);
        let on_close = Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |_| {
            *state_clone.borrow_mut() = "closed".to_string();
        }));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        // onerror
        let state_clone = Rc::clone(&state);
        let on_error = Closure::<dyn FnMut(ErrorEvent)>::wrap(Box::new(move |_| {
            *state_clone.borrow_mut() = "error".to_string();
        }));
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        Ok(WasmWsHandle {
            socket: ws,
            messages,
            state,
            _closures: WasmWsClosures {
                _on_open: on_open,
                _on_message: on_message,
                _on_close: on_close,
                _on_error: on_error,
            },
        })
    }

    pub fn send_message(handle: &WasmWsHandle, msg: &str) -> Result<(), String> {
        handle.socket.send_with_str(msg)
            .map_err(|e| format!("WebSocket send failed: {:?}", e))
    }

    pub fn close_connection(handle: &WasmWsHandle) -> Result<(), String> {
        handle.socket.close()
            .map_err(|e| format!("WebSocket close failed: {:?}", e))
    }

    pub fn drain_incoming(handle: &WasmWsHandle) -> Vec<String> {
        handle.messages.borrow_mut().drain(..).collect()
    }

    pub fn get_state(handle: &WasmWsHandle) -> String {
        handle.state.borrow().clone()
    }

    // Thread-local storage for WASM handles (no Send/Sync needed, single-threaded)
    thread_local! {
        static WASM_WS_HANDLES: RefCell<HashMap<i64, WasmWsHandle>> = RefCell::new(HashMap::new());
    }

    pub fn store_handle(id: i64, handle: WasmWsHandle) {
        WASM_WS_HANDLES.with(|h| h.borrow_mut().insert(id, handle));
    }

    pub fn remove_handle(id: i64) {
        WASM_WS_HANDLES.with(|h| h.borrow_mut().remove(&id));
    }

    pub fn with_handle<T>(id: i64, f: impl FnOnce(&WasmWsHandle) -> T) -> Option<T> {
        WASM_WS_HANDLES.with(|h| {
            let handles = h.borrow();
            handles.get(&id).map(f)
        })
    }
}

/// Drain incoming messages from WASM WebSocket connections into the inbound_queue
#[cfg(target_arch = "wasm32")]
fn drain_wasm_ws_messages(conn_id: i64) {
    let msgs = ws_wasm::with_handle(conn_id, |handle| {
        ws_wasm::drain_incoming(handle)
    });
    if let Some(msgs) = msgs {
        if !msgs.is_empty() {
            with_ext_write(|ext| {
                if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
                    conn.messages_in += msgs.len() as i64;
                    conn.inbound_queue.extend(msgs);
                }
            });
        }
    }
}

/// Update connection state from WASM WebSocket handle
#[cfg(target_arch = "wasm32")]
fn sync_wasm_ws_state(conn_id: i64) {
    let new_state = ws_wasm::with_handle(conn_id, |handle| {
        ws_wasm::get_state(handle)
    });
    if let Some(state) = new_state {
        with_ext_write(|ext| {
            if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
                conn.state = state;
            }
        });
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ExtensionState {
    pub(crate) extensions: Vec<ExtensionRecord>,
    pub(crate) user_functions: Vec<UserFunction>,
    pub(crate) ws_connections: HashMap<i64, WsConnection>,
    pub(crate) ws_next_id: i64,
}

static GLOBAL_EXTENSION_STATE: OnceLock<RwLock<ExtensionState>> = OnceLock::new();

fn global_extension_state() -> &'static RwLock<ExtensionState> {
    GLOBAL_EXTENSION_STATE.get_or_init(|| RwLock::new(ExtensionState::default()))
}

pub(crate) fn with_ext_read<T>(f: impl FnOnce(&ExtensionState) -> T) -> T {
    let state = global_extension_state().read().expect("ext state lock poisoned");
    f(&state)
}

pub(crate) fn with_ext_write<T>(f: impl FnOnce(&mut ExtensionState) -> T) -> T {
    let mut state = global_extension_state().write().expect("ext state lock poisoned");
    f(&mut state)
}

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EngineStateSnapshot {
    catalog: crate::catalog::Catalog,
    rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
    pub(crate) sequences: HashMap<String, SequenceState>,
    security: crate::security::SecurityState,
}

pub fn snapshot_state() -> EngineStateSnapshot {
    EngineStateSnapshot {
        catalog: with_catalog_read(|catalog| catalog.clone()),
        rows_by_table: with_storage_read(|storage| storage.rows_by_table.clone()),
        sequences: with_sequences_read(|sequences| sequences.clone()),
        security: security::snapshot_state(),
    }
}

pub fn restore_state(snapshot: EngineStateSnapshot) {
    let EngineStateSnapshot {
        catalog: next_catalog,
        rows_by_table: next_rows,
        sequences: next_sequences,
        security: next_security,
    } = snapshot;
    with_catalog_write(|catalog| {
        *catalog = next_catalog;
    });
    with_storage_write(|storage| {
        storage.rows_by_table = next_rows;
    });
    with_sequences_write(|sequences| {
        *sequences = next_sequences;
    });
    security::restore_state(next_security);
}

#[cfg(test)]
pub fn reset_global_storage_for_tests() {
    with_storage_write(|storage| {
        storage.rows_by_table.clear();
    });
    with_sequences_write(|sequences| {
        sequences.clear();
    });
    crate::commands::matview::reset_refresh_scheduler_for_tests();
    with_ext_write(|ext| {
        *ext = ExtensionState::default();
    });
    security::reset_global_security_for_tests();
}

pub async fn copy_table_binary_snapshot(
    table_name: &[String],
) -> Result<CopyBinarySnapshot, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot COPY relation \"{}\" because it is not a heap table",
                table.qualified_name()
            ),
        });
    }
    require_relation_privilege(&table, TablePrivilege::Select)?;

    let mut rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    let mut visible_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if relation_row_visible_for_command(&table, &row, RlsCommand::Select, &[])
            .await
            .unwrap_or(false)
        {
            visible_rows.push(row);
        }
    }
    rows = visible_rows;

    let columns = table
        .columns()
        .iter()
        .map(|column| CopyBinaryColumn {
            name: column.name().to_string(),
            type_oid: type_signature_to_oid(column.type_signature()),
        })
        .collect();

    Ok(CopyBinarySnapshot {
        qualified_name: table.qualified_name(),
        columns,
        rows,
    })
}

pub fn copy_table_column_oids(table_name: &[String]) -> Result<Vec<u32>, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot COPY relation \"{}\" because it is not a heap table",
                table.qualified_name()
            ),
        });
    }
    Ok(table
        .columns()
        .iter()
        .map(|column| type_signature_to_oid(column.type_signature()))
        .collect())
}

pub async fn copy_insert_rows(
    table_name: &[String],
    rows: Vec<Vec<ScalarValue>>,
) -> Result<u64, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot COPY into relation \"{}\" because it is not a heap table",
                table.qualified_name()
            ),
        });
    }
    require_relation_privilege(&table, TablePrivilege::Insert)?;

    let mut candidate_rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    let mut accepted_rows = Vec::with_capacity(rows.len());

    for source_row in rows {
        if source_row.len() != table.columns().len() {
            return Err(EngineError {
                message: format!(
                    "COPY row has {} columns but relation \"{}\" expects {}",
                    source_row.len(),
                    table.qualified_name(),
                    table.columns().len()
                ),
            });
        }

        let mut row = vec![ScalarValue::Null; table.columns().len()];
        for (idx, (raw, column)) in source_row
            .into_iter()
            .zip(table.columns().iter())
            .enumerate()
        {
            row[idx] = coerce_value_for_column(raw, column)?;
        }
        for (idx, column) in table.columns().iter().enumerate() {
            if matches!(row[idx], ScalarValue::Null) && !column.nullable() {
                return Err(EngineError {
                    message: format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name(),
                        table.qualified_name()
                    ),
                });
            }
        }
        if !relation_row_passes_check_for_command(&table, &row, RlsCommand::Insert, &[])
            .await?
        {
            return Err(EngineError {
                message: format!(
                    "new row violates row-level security policy for relation \"{}\"",
                    table.qualified_name()
                ),
            });
        }
        candidate_rows.push(row.clone());
        accepted_rows.push(row);
    }

    validate_table_constraints(&table, &candidate_rows).await?;
    with_storage_write(|storage| {
        storage.rows_by_table.insert(table.oid(), candidate_rows);
    });

    Ok(accepted_rows.len() as u64)
}

pub(crate) fn require_relation_owner(table: &crate::catalog::Table) -> Result<(), EngineError> {
    let role = security::current_role();
    security::require_manage_relation(&role, table.oid(), &table.qualified_name())
        .map_err(|message| EngineError { message })
}

pub(crate) fn require_relation_privilege(
    table: &crate::catalog::Table,
    privilege: TablePrivilege,
) -> Result<(), EngineError> {
    let role = security::current_role();
    security::require_table_privilege(&role, table.oid(), &table.qualified_name(), privilege)
        .map_err(|message| EngineError { message })
}

pub(crate) async fn relation_row_visible_for_command(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    command: RlsCommand,
    params: &[Option<String>],
) -> Result<bool, EngineError> {
    let role = security::current_role();
    let eval = security::rls_evaluation_for_role(&role, table.oid(), command);
    if !eval.enabled || eval.bypass {
        return Ok(true);
    }
    if eval.policies.is_empty() {
        return Ok(false);
    }

    let scope = scope_for_table_row(table, row);
    for policy in &eval.policies {
        let Some(predicate) = policy.using_expr.as_ref() else {
            return Ok(true);
        };
        if truthy(&eval_expr(predicate, &scope, params).await?) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn relation_row_passes_check_for_command(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    command: RlsCommand,
    params: &[Option<String>],
) -> Result<bool, EngineError> {
    let role = security::current_role();
    let eval = security::rls_evaluation_for_role(&role, table.oid(), command);
    if !eval.enabled || eval.bypass {
        return Ok(true);
    }
    if eval.policies.is_empty() {
        return Ok(false);
    }

    let scope = scope_for_table_row(table, row);
    for policy in &eval.policies {
        let predicate = policy.check_expr.as_ref().or(policy.using_expr.as_ref());
        let Some(predicate) = predicate else {
            return Ok(true);
        };
        if truthy(&eval_expr(predicate, &scope, params).await?) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn execute_insert(
    insert: &InsertStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&insert.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot INSERT into non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    require_relation_privilege(&table, TablePrivilege::Insert)?;

    let target_indexes = resolve_insert_target_indexes(&table, &insert.columns)?;
    let source_rows = match &insert.source {
        InsertSource::Values(values_rows) => {
            let mut rows = Vec::with_capacity(values_rows.len());
            for row_exprs in values_rows {
                let mut row = Vec::with_capacity(row_exprs.len());
                for expr in row_exprs {
                    row.push(eval_expr(expr, &EvalScope::default(), params).await?);
                }
                rows.push(row);
            }
            rows
        }
        InsertSource::Query(query) => execute_query(query, params).await?.rows,
    };

    let mut materialized = Vec::with_capacity(source_rows.len());
    for source_row in &source_rows {
        if source_row.len() != target_indexes.len() {
            return Err(EngineError {
                message: format!(
                    "INSERT has {} expressions but {} target columns",
                    source_row.len(),
                    target_indexes.len()
                ),
            });
        }

        let mut row = vec![ScalarValue::Null; table.columns().len()];
        let mut provided = vec![false; table.columns().len()];
        for (raw, col_idx) in source_row.iter().zip(target_indexes.iter()) {
            let column = &table.columns()[*col_idx];
            row[*col_idx] = coerce_value_for_column(raw.clone(), column)?;
            provided[*col_idx] = true;
        }

        for (idx, column) in table.columns().iter().enumerate() {
            if !provided[idx]
                && let Some(default_expr) = column.default()
            {
                let raw = eval_expr(default_expr, &EvalScope::default(), params).await?;
                row[idx] = coerce_value_for_column(raw, column)?;
            }
        }

        for (idx, column) in table.columns().iter().enumerate() {
            if matches!(row[idx], ScalarValue::Null) && !column.nullable() {
                return Err(EngineError {
                    message: format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name(),
                        table.qualified_name()
                    ),
                });
            }
        }
        if !relation_row_passes_check_for_command(&table, &row, RlsCommand::Insert, params).await? {
            return Err(EngineError {
                message: format!(
                    "new row violates row-level security policy for relation \"{}\"",
                    table.qualified_name()
                ),
            });
        }

        materialized.push(row);
    }

    let mut candidate_rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    let mut accepted_rows = Vec::new();
    match &insert.on_conflict {
        None => {
            candidate_rows.extend(materialized.iter().cloned());
            validate_table_constraints(&table, &candidate_rows).await?;
            accepted_rows = materialized.clone();
        }
        Some(OnConflictClause::DoNothing { .. }) => {
            let conflict_target_indexes = match &insert.on_conflict {
                Some(OnConflictClause::DoNothing {
                    conflict_target: Some(target),
                }) => Some(resolve_on_conflict_target_indexes(&table, target)?),
                _ => None,
            };
            for row in &materialized {
                if let Some(target_indexes) = conflict_target_indexes.as_ref()
                    && row_conflicts_on_columns(&candidate_rows, row, target_indexes)
                {
                    continue;
                }
                let mut trial = candidate_rows.clone();
                trial.push(row.clone());
                match validate_table_constraints(&table, &trial).await {
                    Ok(()) => {
                        candidate_rows = trial;
                        accepted_rows.push(row.clone());
                    }
                    Err(err) => {
                        if conflict_target_indexes.is_some() {
                            return Err(err);
                        }
                        if is_conflict_violation(&err) {
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
        }
        Some(OnConflictClause::DoUpdate {
            conflict_target,
            assignments,
            where_clause,
        }) => {
            let Some(target) = conflict_target.as_ref() else {
                return Err(EngineError {
                    message:
                        "ON CONFLICT DO UPDATE requires an inference specification or constraint"
                            .to_string(),
                });
            };
            let conflict_target_indexes = resolve_on_conflict_target_indexes(&table, target)?;
            let assignment_targets = resolve_update_assignment_targets(&table, assignments)?;
            let conflict_scope_qualifiers = insert
                .table_alias
                .as_ref()
                .map(|alias| vec![alias.to_ascii_lowercase()])
                .unwrap_or_else(|| vec![table.name().to_string(), table.qualified_name()]);

            for row in &materialized {
                let Some(conflicting_row_idx) =
                    find_conflict_row_index(&candidate_rows, row, &conflict_target_indexes)
                else {
                    let mut trial = candidate_rows.clone();
                    trial.push(row.clone());
                    validate_table_constraints(&table, &trial).await?;
                    candidate_rows = trial;
                    accepted_rows.push(row.clone());
                    continue;
                };

                let existing_row = candidate_rows[conflicting_row_idx].clone();
                if !relation_row_visible_for_command(
                    &table,
                    &existing_row,
                    RlsCommand::Update,
                    params,
                )
                .await?
                {
                    continue;
                }
                let mut scope = scope_for_table_row_with_qualifiers(
                    &table,
                    &existing_row,
                    &conflict_scope_qualifiers,
                );
                add_excluded_row_to_scope(&mut scope, &table, row);
                if let Some(predicate) = where_clause
                    && !truthy(&eval_expr(predicate, &scope, params).await?)
                {
                    continue;
                }

                let mut updated_row = existing_row.clone();
                for (col_idx, column, expr) in &assignment_targets {
                    let raw = eval_expr(expr, &scope, params).await?;
                    updated_row[*col_idx] = coerce_value_for_column(raw, column)?;
                }
                for (idx, column) in table.columns().iter().enumerate() {
                    if matches!(updated_row[idx], ScalarValue::Null) && !column.nullable() {
                        return Err(EngineError {
                            message: format!(
                                "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                                column.name(),
                                table.qualified_name()
                            ),
                        });
                    }
                }
                if !relation_row_passes_check_for_command(
                    &table,
                    &updated_row,
                    RlsCommand::Update,
                    params,
                )
                .await?
                {
                    return Err(EngineError {
                        message: format!(
                            "new row violates row-level security policy for relation \"{}\"",
                            table.qualified_name()
                        ),
                    });
                }

                let mut trial = candidate_rows.clone();
                trial[conflicting_row_idx] = updated_row.clone();
                validate_table_constraints(&table, &trial).await?;
                candidate_rows = trial;
                accepted_rows.push(updated_row);
            }
        }
    }

    let inserted = accepted_rows.len() as u64;
    with_storage_write(|storage| {
        storage
            .rows_by_table
            .insert(table.oid(), candidate_rows.clone());
    });
    let returning_columns = if insert.returning.is_empty() {
        Vec::new()
    } else {
        derive_returning_columns_from_table(&table, &insert.returning)?
    };
    let returning_rows = if insert.returning.is_empty() {
        Vec::new()
    } else {
        {
            let mut out = Vec::with_capacity(accepted_rows.len());
            for row in &accepted_rows {
                out.push(project_returning_row(&insert.returning, &table, row, params).await?);
            }
            out
        }
    };

    Ok(QueryResult {
        columns: returning_columns,
        rows: returning_rows,
        command_tag: "INSERT".to_string(),
        rows_affected: inserted,
    })
}

async fn execute_update(
    update: &UpdateStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&update.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot UPDATE non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    require_relation_privilege(&table, TablePrivilege::Update)?;
    if update.assignments.is_empty() {
        return Err(EngineError {
            message: "UPDATE requires at least one assignment".to_string(),
        });
    }

    let mut assignment_targets = Vec::with_capacity(update.assignments.len());
    let mut seen = HashSet::new();
    for assignment in &update.assignments {
        let normalized = assignment.column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(EngineError {
                message: format!("column \"{}\" specified more than once", assignment.column),
            });
        }
        let Some((idx, column)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == normalized)
        else {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    assignment.column,
                    table.qualified_name()
                ),
            });
        };
        assignment_targets.push((idx, column, &assignment.value));
    }

    let current_rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    let from_rows = if update.from.is_empty() {
        Vec::new()
    } else {
        evaluate_from_clause(&update.from, params, None).await?
    };
    let mut next_rows = current_rows.clone();
    let mut returning_base_rows = Vec::new();
    let mut updated = 0u64;

    for (row_idx, row) in current_rows.iter().enumerate() {
        if !relation_row_visible_for_command(&table, row, RlsCommand::Update, params).await? {
            continue;
        }
        let base_scope = scope_for_table_row(&table, row);
        let mut matched_scope = None;
        if update.from.is_empty() {
            let matches = if let Some(predicate) = &update.where_clause {
                truthy(&eval_expr(predicate, &base_scope, params).await?)
            } else {
                true
            };
            if matches {
                matched_scope = Some(base_scope.clone());
            }
        } else {
            for from_scope in &from_rows {
                let combined = combine_scopes(&base_scope, from_scope, &HashSet::new());
                let matches = if let Some(predicate) = &update.where_clause {
                    truthy(&eval_expr(predicate, &combined, params).await?)
                } else {
                    true
                };
                if matches {
                    matched_scope = Some(combined);
                    break;
                }
            }
        }
        let Some(scope) = matched_scope else {
            continue;
        };

        let mut new_row = row.clone();
        for (col_idx, column, expr) in &assignment_targets {
            let raw = eval_expr(expr, &scope, params).await?;
            new_row[*col_idx] = coerce_value_for_column(raw, column)?;
        }
        for (idx, column) in table.columns().iter().enumerate() {
            if matches!(new_row[idx], ScalarValue::Null) && !column.nullable() {
                return Err(EngineError {
                    message: format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name(),
                        table.qualified_name()
                    ),
                });
            }
        }
        if !relation_row_passes_check_for_command(&table, &new_row, RlsCommand::Update, params)
            .await?
        {
            return Err(EngineError {
                message: format!(
                    "new row violates row-level security policy for relation \"{}\"",
                    table.qualified_name()
                ),
            });
        }
        next_rows[row_idx] = new_row;
        returning_base_rows.push(next_rows[row_idx].clone());
        updated += 1;
    }

    validate_table_constraints(&table, &next_rows).await?;
    let staged_updates =
        apply_on_update_actions(&table, &current_rows, next_rows.clone()).await?;

    with_storage_write(|storage| {
        for (table_oid, rows) in staged_updates {
            storage.rows_by_table.insert(table_oid, rows);
        }
    });
    let returning_columns = if update.returning.is_empty() {
        Vec::new()
    } else {
        derive_returning_columns_from_table(&table, &update.returning)?
    };
    let returning_rows = if update.returning.is_empty() {
        Vec::new()
    } else {
        {
            let mut out = Vec::with_capacity(returning_base_rows.len());
            for row in &returning_base_rows {
                out.push(project_returning_row(&update.returning, &table, row, params).await?);
            }
            out
        }
    };

    Ok(QueryResult {
        columns: returning_columns,
        rows: returning_rows,
        command_tag: "UPDATE".to_string(),
        rows_affected: updated,
    })
}

async fn execute_delete(
    delete: &DeleteStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&delete.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot DELETE from non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    require_relation_privilege(&table, TablePrivilege::Delete)?;

    let current_rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    let using_rows = if delete.using.is_empty() {
        Vec::new()
    } else {
        evaluate_from_clause(&delete.using, params, None).await?
    };
    let mut retained = Vec::with_capacity(current_rows.len());
    let mut removed_rows = Vec::new();
    let mut deleted = 0u64;
    for row in &current_rows {
        if !relation_row_visible_for_command(&table, row, RlsCommand::Delete, params).await? {
            retained.push(row.clone());
            continue;
        }
        let base_scope = scope_for_table_row(&table, row);
        let matches = if delete.using.is_empty() {
            if let Some(predicate) = &delete.where_clause {
                truthy(&eval_expr(predicate, &base_scope, params).await?)
            } else {
                true
            }
        } else {
            let mut any = false;
            for using_scope in &using_rows {
                let combined = combine_scopes(&base_scope, using_scope, &HashSet::new());
                let passes = if let Some(predicate) = &delete.where_clause {
                    truthy(&eval_expr(predicate, &combined, params).await?)
                } else {
                    true
                };
                if passes {
                    any = true;
                    break;
                }
            }
            any
        };
        if matches {
            deleted += 1;
            removed_rows.push(row.clone());
        } else {
            retained.push(row.clone());
        }
    }

    let staged_updates =
        apply_on_delete_actions(&table, retained, removed_rows.clone()).await?;

    with_storage_write(|storage| {
        for (table_oid, rows) in staged_updates {
            storage.rows_by_table.insert(table_oid, rows);
        }
    });
    let returning_columns = if delete.returning.is_empty() {
        Vec::new()
    } else {
        derive_returning_columns_from_table(&table, &delete.returning)?
    };
    let returning_rows = if delete.returning.is_empty() {
        Vec::new()
    } else {
        {
            let mut out = Vec::with_capacity(removed_rows.len());
            for row in &removed_rows {
                out.push(project_returning_row(&delete.returning, &table, row, params).await?);
            }
            out
        }
    };

    Ok(QueryResult {
        columns: returning_columns,
        rows: returning_rows,
        command_tag: "DELETE".to_string(),
        rows_affected: deleted,
    })
}

async fn execute_merge(
    merge: &MergeStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&merge.target_table, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot MERGE into non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }

    let mut need_insert = false;
    let mut need_update = false;
    let mut need_delete = false;
    for clause in &merge.when_clauses {
        match clause {
            MergeWhenClause::MatchedUpdate { .. }
            | MergeWhenClause::NotMatchedBySourceUpdate { .. } => need_update = true,
            MergeWhenClause::MatchedDelete { .. }
            | MergeWhenClause::NotMatchedBySourceDelete { .. } => need_delete = true,
            MergeWhenClause::NotMatchedInsert { .. } => need_insert = true,
            _ => {}
        }
    }
    if need_insert {
        require_relation_privilege(&table, TablePrivilege::Insert)?;
    }
    if need_update {
        require_relation_privilege(&table, TablePrivilege::Update)?;
    }
    if need_delete {
        require_relation_privilege(&table, TablePrivilege::Delete)?;
    }

    let source_eval = evaluate_table_expression(&merge.source, params, None).await?;
    let current_rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&table.oid())
            .cloned()
            .unwrap_or_default()
    });
    #[derive(Clone)]
    struct MergeCandidateRow {
        source_row_index: Option<usize>,
        values: Vec<ScalarValue>,
    }

    let target_qualifiers = merge
        .target_alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .unwrap_or_else(|| vec![table.name().to_string(), table.qualified_name()]);
    let returning_columns = if merge.returning.is_empty() {
        Vec::new()
    } else {
        derive_returning_columns_from_table(&table, &merge.returning)?
    };
    let mut returning_rows = Vec::new();

    let mut candidate_rows = current_rows
        .iter()
        .enumerate()
        .map(|(idx, row)| MergeCandidateRow {
            source_row_index: Some(idx),
            values: row.clone(),
        })
        .collect::<Vec<_>>();
    let mut matched_target_source_rows = HashSet::new();
    let mut modified_target_source_rows = HashSet::new();
    let mut deleted_rows = Vec::new();
    let mut changed = 0u64;

    for source_scope in source_eval.rows {
        let mut matched_index = None;
        for (row_idx, target_row) in candidate_rows.iter().enumerate() {
            if target_row.source_row_index.is_none() {
                continue;
            }
            let target_scope =
                scope_for_table_row_with_qualifiers(&table, &target_row.values, &target_qualifiers);
            let combined = combine_scopes(&target_scope, &source_scope, &HashSet::new());
            if truthy(&eval_expr(&merge.on, &combined, params).await?) {
                if matched_index.is_some() {
                    return Err(EngineError {
                        message: "MERGE matched more than one target row for a source row"
                            .to_string(),
                    });
                }
                matched_index = Some(row_idx);
            }
        }

        if let Some(target_idx) = matched_index {
            if let Some(source_idx) = candidate_rows[target_idx].source_row_index {
                matched_target_source_rows.insert(source_idx);
            }
            let mut clause_applied = false;
            for clause in &merge.when_clauses {
                match clause {
                    MergeWhenClause::MatchedUpdate {
                        condition,
                        assignments,
                    } => {
                        if !relation_row_visible_for_command(
                            &table,
                            &candidate_rows[target_idx].values,
                            RlsCommand::Update,
                            params,
                        )
                        .await?
                        {
                            continue;
                        }
                        let target_scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[target_idx].values,
                            &target_qualifiers,
                        );
                        let combined =
                            combine_scopes(&target_scope, &source_scope, &HashSet::new());
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &combined, params).await?)
                        {
                            continue;
                        }
                        let source_idx = candidate_rows[target_idx]
                            .source_row_index
                            .expect("matched rows originate from base relation");
                        if !modified_target_source_rows.insert(source_idx) {
                            return Err(EngineError {
                                message: "MERGE cannot affect the same target row more than once"
                                    .to_string(),
                            });
                        }
                        let assignment_targets =
                            resolve_update_assignment_targets(&table, assignments)?;
                        let mut new_row = candidate_rows[target_idx].values.clone();
                        for (col_idx, column, expr) in &assignment_targets {
                            let raw = eval_expr(expr, &combined, params).await?;
                            new_row[*col_idx] = coerce_value_for_column(raw, column)?;
                        }
                        for (idx, column) in table.columns().iter().enumerate() {
                            if matches!(new_row[idx], ScalarValue::Null) && !column.nullable() {
                                return Err(EngineError {
                                    message: format!(
                                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                                        column.name(),
                                        table.qualified_name()
                                    ),
                                });
                            }
                        }
                        if !relation_row_passes_check_for_command(
                            &table,
                            &new_row,
                            RlsCommand::Update,
                            params,
                        )
                        .await?
                        {
                            return Err(EngineError {
                                message: format!(
                                    "new row violates row-level security policy for relation \"{}\"",
                                    table.qualified_name()
                                ),
                            });
                        }
                        candidate_rows[target_idx].values = new_row;
                        if !merge.returning.is_empty() {
                            returning_rows.push(project_returning_row_with_qualifiers(
                                &merge.returning,
                                &table,
                                &candidate_rows[target_idx].values,
                                &target_qualifiers,
                                params,
                            )
                            .await?);
                        }
                        changed += 1;
                        clause_applied = true;
                        break;
                    }
                    MergeWhenClause::MatchedDelete { condition } => {
                        if !relation_row_visible_for_command(
                            &table,
                            &candidate_rows[target_idx].values,
                            RlsCommand::Delete,
                            params,
                        )
                        .await?
                        {
                            continue;
                        }
                        let target_scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[target_idx].values,
                            &target_qualifiers,
                        );
                        let combined =
                            combine_scopes(&target_scope, &source_scope, &HashSet::new());
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &combined, params).await?)
                        {
                            continue;
                        }
                        let source_idx = candidate_rows[target_idx]
                            .source_row_index
                            .expect("matched rows originate from base relation");
                        if !modified_target_source_rows.insert(source_idx) {
                            return Err(EngineError {
                                message: "MERGE cannot affect the same target row more than once"
                                    .to_string(),
                            });
                        }
                        let removed = candidate_rows.remove(target_idx);
                        if !merge.returning.is_empty() {
                            returning_rows.push(project_returning_row_with_qualifiers(
                                &merge.returning,
                                &table,
                                &removed.values,
                                &target_qualifiers,
                                params,
                            )
                            .await?);
                        }
                        if removed.source_row_index.is_some() {
                            deleted_rows.push(removed.values);
                        }
                        changed += 1;
                        clause_applied = true;
                        break;
                    }
                    MergeWhenClause::MatchedDoNothing { condition } => {
                        let target_scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[target_idx].values,
                            &target_qualifiers,
                        );
                        let combined =
                            combine_scopes(&target_scope, &source_scope, &HashSet::new());
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &combined, params).await?)
                        {
                            continue;
                        }
                        clause_applied = true;
                        break;
                    }
                    _ => {}
                }
            }
            if clause_applied {
                continue;
            }
        } else {
            for clause in &merge.when_clauses {
                match clause {
                    MergeWhenClause::NotMatchedInsert {
                        condition,
                        columns,
                        values,
                    } => {
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &source_scope, params).await?)
                        {
                            continue;
                        }
                        let target_indexes = resolve_insert_target_indexes(&table, columns)?;
                        if values.len() != target_indexes.len() {
                            return Err(EngineError {
                                message: format!(
                                    "MERGE INSERT has {} expressions but {} target columns",
                                    values.len(),
                                    target_indexes.len()
                                ),
                            });
                        }
                        let mut row = vec![ScalarValue::Null; table.columns().len()];
                        let mut provided = vec![false; table.columns().len()];
                        for (expr, col_idx) in values.iter().zip(target_indexes.iter()) {
                            let raw = eval_expr(expr, &source_scope, params).await?;
                            let column = &table.columns()[*col_idx];
                            row[*col_idx] = coerce_value_for_column(raw, column)?;
                            provided[*col_idx] = true;
                        }
                        for (idx, column) in table.columns().iter().enumerate() {
                            if !provided[idx]
                                && let Some(default_expr) = column.default()
                            {
                                let raw =
                                    eval_expr(default_expr, &source_scope, params).await?;
                                row[idx] = coerce_value_for_column(raw, column)?;
                            }
                            if matches!(row[idx], ScalarValue::Null) && !column.nullable() {
                                return Err(EngineError {
                                    message: format!(
                                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                                        column.name(),
                                        table.qualified_name()
                                    ),
                                });
                            }
                        }
                        if !relation_row_passes_check_for_command(
                            &table,
                            &row,
                            RlsCommand::Insert,
                            params,
                        )
                        .await?
                        {
                            return Err(EngineError {
                                message: format!(
                                    "new row violates row-level security policy for relation \"{}\"",
                                    table.qualified_name()
                                ),
                            });
                        }
                        if !merge.returning.is_empty() {
                            returning_rows.push(project_returning_row_with_qualifiers(
                                &merge.returning,
                                &table,
                                &row,
                                &target_qualifiers,
                                params,
                            )
                            .await?);
                        }
                        candidate_rows.push(MergeCandidateRow {
                            source_row_index: None,
                            values: row,
                        });
                        changed += 1;
                        break;
                    }
                    MergeWhenClause::NotMatchedDoNothing { condition } => {
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &source_scope, params).await?)
                        {
                            continue;
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    let has_not_matched_by_source_clauses = merge.when_clauses.iter().any(|clause| {
        matches!(
            clause,
            MergeWhenClause::NotMatchedBySourceUpdate { .. }
                | MergeWhenClause::NotMatchedBySourceDelete { .. }
                | MergeWhenClause::NotMatchedBySourceDoNothing { .. }
        )
    });
    if has_not_matched_by_source_clauses {
        let mut row_idx = 0usize;
        while row_idx < candidate_rows.len() {
            let Some(source_idx) = candidate_rows[row_idx].source_row_index else {
                row_idx += 1;
                continue;
            };
            if matched_target_source_rows.contains(&source_idx) {
                row_idx += 1;
                continue;
            }

            let mut clause_applied = false;
            let mut removed_current_row = false;
            for clause in &merge.when_clauses {
                match clause {
                    MergeWhenClause::NotMatchedBySourceUpdate {
                        condition,
                        assignments,
                    } => {
                        if !relation_row_visible_for_command(
                            &table,
                            &candidate_rows[row_idx].values,
                            RlsCommand::Update,
                            params,
                        )
                        .await?
                        {
                            continue;
                        }
                        let scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[row_idx].values,
                            &target_qualifiers,
                        );
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &scope, params).await?)
                        {
                            continue;
                        }
                        let assignment_targets =
                            resolve_update_assignment_targets(&table, assignments)?;
                        let mut new_row = candidate_rows[row_idx].values.clone();
                        for (col_idx, column, expr) in &assignment_targets {
                            let raw = eval_expr(expr, &scope, params).await?;
                            new_row[*col_idx] = coerce_value_for_column(raw, column)?;
                        }
                        for (idx, column) in table.columns().iter().enumerate() {
                            if matches!(new_row[idx], ScalarValue::Null) && !column.nullable() {
                                return Err(EngineError {
                                    message: format!(
                                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                                        column.name(),
                                        table.qualified_name()
                                    ),
                                });
                            }
                        }
                        if !relation_row_passes_check_for_command(
                            &table,
                            &new_row,
                            RlsCommand::Update,
                            params,
                        )
                        .await?
                        {
                            return Err(EngineError {
                                message: format!(
                                    "new row violates row-level security policy for relation \"{}\"",
                                    table.qualified_name()
                                ),
                            });
                        }
                        candidate_rows[row_idx].values = new_row;
                        if !merge.returning.is_empty() {
                            returning_rows.push(project_returning_row_with_qualifiers(
                                &merge.returning,
                                &table,
                                &candidate_rows[row_idx].values,
                                &target_qualifiers,
                                params,
                            )
                            .await?);
                        }
                        changed += 1;
                        clause_applied = true;
                        break;
                    }
                    MergeWhenClause::NotMatchedBySourceDelete { condition } => {
                        if !relation_row_visible_for_command(
                            &table,
                            &candidate_rows[row_idx].values,
                            RlsCommand::Delete,
                            params,
                        )
                        .await?
                        {
                            continue;
                        }
                        let scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[row_idx].values,
                            &target_qualifiers,
                        );
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &scope, params).await?)
                        {
                            continue;
                        }
                        let removed = candidate_rows.remove(row_idx);
                        if !merge.returning.is_empty() {
                            returning_rows.push(project_returning_row_with_qualifiers(
                                &merge.returning,
                                &table,
                                &removed.values,
                                &target_qualifiers,
                                params,
                            )
                            .await?);
                        }
                        if removed.source_row_index.is_some() {
                            deleted_rows.push(removed.values);
                        }
                        changed += 1;
                        clause_applied = true;
                        removed_current_row = true;
                        break;
                    }
                    MergeWhenClause::NotMatchedBySourceDoNothing { condition } => {
                        let scope = scope_for_table_row_with_qualifiers(
                            &table,
                            &candidate_rows[row_idx].values,
                            &target_qualifiers,
                        );
                        if let Some(cond) = condition
                            && !truthy(&eval_expr(cond, &scope, params).await?)
                        {
                            continue;
                        }
                        clause_applied = true;
                        break;
                    }
                    _ => {}
                }
            }

            if !removed_current_row {
                row_idx += 1;
            }
            if !clause_applied {
                continue;
            }
        }
    }

    let final_rows = candidate_rows
        .iter()
        .map(|candidate| candidate.values.clone())
        .collect::<Vec<_>>();
    validate_table_constraints(&table, &final_rows).await?;

    let mut update_changes = Vec::new();
    for candidate in &candidate_rows {
        let Some(source_idx) = candidate.source_row_index else {
            continue;
        };
        let old_row = &current_rows[source_idx];
        if old_row != &candidate.values {
            update_changes.push((old_row.clone(), candidate.values.clone()));
        }
    }

    let staged_seed = with_storage_read(|storage| storage.rows_by_table.clone());
    let mut staged_updates = apply_on_update_actions_with_staged(
        &table,
        final_rows.clone(),
        update_changes,
        staged_seed,
    )
    .await?;
    if !deleted_rows.is_empty() {
        staged_updates = apply_on_delete_actions_with_staged(
            &table,
            final_rows.clone(),
            deleted_rows,
            staged_updates,
        )
        .await?;
    }
    with_storage_write(|storage| {
        for (table_oid, rows) in staged_updates {
            storage.rows_by_table.insert(table_oid, rows);
        }
    });

    Ok(QueryResult {
        columns: returning_columns,
        rows: returning_rows,
        command_tag: "MERGE".to_string(),
        rows_affected: changed,
    })
}

pub(crate) fn type_signature_to_oid(ty: TypeSignature) -> u32 {
    match ty {
        TypeSignature::Bool => PG_BOOL_OID,
        TypeSignature::Int8 => PG_INT8_OID,
        TypeSignature::Float8 => PG_FLOAT8_OID,
        TypeSignature::Text => PG_TEXT_OID,
        TypeSignature::Date => PG_DATE_OID,
        TypeSignature::Timestamp => PG_TIMESTAMP_OID,
    }
}

pub fn type_oid_size(type_oid: u32) -> i16 {
    match type_oid {
        PG_BOOL_OID => 1,
        PG_INT8_OID => 8,
        PG_FLOAT8_OID => 8,
        PG_DATE_OID => 4,
        PG_TIMESTAMP_OID => 8,
        _ => -1,
    }
}

fn resolve_insert_target_indexes(
    table: &crate::catalog::Table,
    target_columns: &[String],
) -> Result<Vec<usize>, EngineError> {
    if target_columns.is_empty() {
        return Ok((0..table.columns().len()).collect());
    }

    let mut indexes = Vec::with_capacity(target_columns.len());
    let mut seen = HashSet::new();
    for column_name in target_columns {
        let normalized = column_name.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(EngineError {
                message: format!("column \"{}\" specified more than once", column_name),
            });
        }
        let Some((idx, _)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == normalized)
        else {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    column_name,
                    table.qualified_name()
                ),
            });
        };
        indexes.push(idx);
    }
    Ok(indexes)
}

fn resolve_update_assignment_targets<'a>(
    table: &'a crate::catalog::Table,
    assignments: &'a [crate::parser::ast::Assignment],
) -> Result<Vec<(usize, &'a Column, &'a Expr)>, EngineError> {
    if assignments.is_empty() {
        return Err(EngineError {
            message: "ON CONFLICT DO UPDATE requires at least one assignment".to_string(),
        });
    }
    let mut out = Vec::with_capacity(assignments.len());
    let mut seen = HashSet::new();
    for assignment in assignments {
        let normalized = assignment.column.to_ascii_lowercase();
        if !seen.insert(normalized.clone()) {
            return Err(EngineError {
                message: format!("column \"{}\" specified more than once", assignment.column),
            });
        }
        let Some((idx, column)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == normalized)
        else {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    assignment.column,
                    table.qualified_name()
                ),
            });
        };
        out.push((idx, column, &assignment.value));
    }
    Ok(out)
}

pub(crate) fn find_column_index(
    table: &crate::catalog::Table,
    column_name: &str,
) -> Result<usize, EngineError> {
    let normalized = column_name.to_ascii_lowercase();
    table
        .columns()
        .iter()
        .position(|column| column.name() == normalized)
        .ok_or_else(|| EngineError {
            message: format!(
                "column \"{}\" of relation \"{}\" does not exist",
                column_name,
                table.qualified_name()
            ),
        })
}

fn resolve_on_conflict_target_indexes(
    table: &crate::catalog::Table,
    conflict_target: &ConflictTarget,
) -> Result<Vec<usize>, EngineError> {
    let normalized_target = match conflict_target {
        ConflictTarget::Columns(columns) => {
            let mut normalized_target = Vec::with_capacity(columns.len());
            let mut seen = HashSet::new();
            for column in columns {
                let normalized = column.to_ascii_lowercase();
                if !seen.insert(normalized.clone()) {
                    return Err(EngineError {
                        message: format!(
                            "ON CONFLICT target column \"{}\" specified more than once",
                            column
                        ),
                    });
                }
                normalized_target.push(normalized);
            }
            if normalized_target.is_empty() {
                return Err(EngineError {
                    message: "ON CONFLICT target must reference at least one column".to_string(),
                });
            }
            if !table
                .key_constraints()
                .iter()
                .any(|constraint| constraint.columns == normalized_target)
            {
                return Err(EngineError {
                    message: "there is no unique or primary key constraint matching the ON CONFLICT specification".to_string(),
                });
            }
            normalized_target
        }
        ConflictTarget::Constraint(name) => {
            let normalized_name = name.to_ascii_lowercase();
            let Some(constraint) = table
                .key_constraints()
                .iter()
                .find(|constraint| constraint.name.as_deref() == Some(normalized_name.as_str()))
            else {
                return Err(EngineError {
                    message: format!(
                        "constraint \"{}\" for relation \"{}\" does not exist",
                        name,
                        table.qualified_name()
                    ),
                });
            };
            constraint.columns.clone()
        }
    };

    normalized_target
        .iter()
        .map(|column| find_column_index(table, column))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) async fn validate_table_constraints(
    table: &crate::catalog::Table,
    candidate_rows: &[Vec<ScalarValue>],
) -> Result<(), EngineError> {
    validate_table_constraints_with_overrides(table, candidate_rows, None).await
}

async fn validate_table_constraints_with_overrides(
    table: &crate::catalog::Table,
    candidate_rows: &[Vec<ScalarValue>],
    row_overrides: Option<&HashMap<Oid, Vec<Vec<ScalarValue>>>>,
) -> Result<(), EngineError> {
    for row in candidate_rows {
        for (idx, column) in table.columns().iter().enumerate() {
            if matches!(row.get(idx), Some(ScalarValue::Null) | None) && !column.nullable() {
                return Err(EngineError {
                    message: format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        column.name(),
                        table.qualified_name()
                    ),
                });
            }
        }
    }

    for row in candidate_rows {
        let scope = scope_for_table_row(table, row);
        for column in table.columns() {
            let Some(check_expr) = column.check() else {
                continue;
            };
            let check_value = eval_expr(check_expr, &scope, &[]).await?;
            let check_passed = match check_value {
                ScalarValue::Bool(true) | ScalarValue::Null => true,
                ScalarValue::Bool(false) => false,
                _ => {
                    return Err(EngineError {
                        message: format!(
                            "CHECK constraint on column \"{}\" of relation \"{}\" must evaluate to boolean",
                            column.name(),
                            table.qualified_name()
                        ),
                    });
                }
            };
            if !check_passed {
                return Err(EngineError {
                    message: format!(
                        "row for relation \"{}\" violates CHECK constraint on column \"{}\"",
                        table.qualified_name(),
                        column.name()
                    ),
                });
            }
        }
    }

    for constraint in table.key_constraints() {
        let column_indexes = constraint
            .columns
            .iter()
            .map(|column| find_column_index(table, column))
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = HashSet::new();

        for row in candidate_rows {
            let values = column_indexes
                .iter()
                .map(|idx| row.get(*idx).cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();

            if values
                .iter()
                .any(|value| matches!(value, ScalarValue::Null))
            {
                if constraint.primary {
                    return Err(EngineError {
                        message: format!(
                            "null value in key columns ({}) of relation \"{}\" violates not-null constraint",
                            constraint.columns.join(", "),
                            table.qualified_name()
                        ),
                    });
                }
                continue;
            }

            let key = values.iter().map(scalar_key).collect::<Vec<_>>().join("|");
            if !seen.insert(key) {
                let kind = if constraint.primary {
                    "primary key"
                } else {
                    "unique constraint"
                };
                return Err(EngineError {
                    message: format!(
                        "duplicate value for key columns ({}) of relation \"{}\" violates {}",
                        constraint.columns.join(", "),
                        table.qualified_name(),
                        kind
                    ),
                });
            }
        }
    }

    for constraint in table.foreign_key_constraints() {
        let (referenced_table, child_column_indexes, parent_column_indexes) =
            resolve_foreign_key_indexes(table, constraint)?;
        let referenced_rows = if let Some(rows) = row_overrides
            .and_then(|overrides| overrides.get(&referenced_table.oid()))
            .cloned()
        {
            rows
        } else if referenced_table.oid() == table.oid() {
            candidate_rows.to_vec()
        } else {
            with_storage_read(|storage| {
                storage
                    .rows_by_table
                    .get(&referenced_table.oid())
                    .cloned()
                    .unwrap_or_default()
            })
        };
        let referenced_keys: HashSet<String> = referenced_rows
            .iter()
            .filter_map(|row| composite_non_null_key(row, &parent_column_indexes))
            .collect();

        for row in candidate_rows {
            let Some(child_key) = composite_non_null_key(row, &child_column_indexes) else {
                continue;
            };
            if !referenced_keys.contains(&child_key) {
                return Err(EngineError {
                    message: format!(
                        "insert or update on relation \"{}\" violates foreign key{}",
                        table.qualified_name(),
                        constraint
                            .name
                            .as_ref()
                            .map(|name| format!(" \"{name}\""))
                            .unwrap_or_default()
                    ),
                });
            }
        }
    }

    Ok(())
}

fn is_conflict_violation(err: &EngineError) -> bool {
    err.message.contains("primary key") || err.message.contains("unique constraint")
}

fn row_conflicts_on_columns(
    existing_rows: &[Vec<ScalarValue>],
    candidate_row: &[ScalarValue],
    column_indexes: &[usize],
) -> bool {
    let Some(candidate_key) = composite_non_null_key(candidate_row, column_indexes) else {
        return false;
    };
    existing_rows
        .iter()
        .filter_map(|row| composite_non_null_key(row, column_indexes))
        .any(|existing_key| existing_key == candidate_key)
}

fn find_conflict_row_index(
    existing_rows: &[Vec<ScalarValue>],
    candidate_row: &[ScalarValue],
    column_indexes: &[usize],
) -> Option<usize> {
    let candidate_key = composite_non_null_key(candidate_row, column_indexes)?;
    existing_rows.iter().position(|row| {
        composite_non_null_key(row, column_indexes)
            .is_some_and(|existing_key| existing_key == candidate_key)
    })
}

fn add_excluded_row_to_scope(
    scope: &mut EvalScope,
    table: &crate::catalog::Table,
    excluded_row: &[ScalarValue],
) {
    for (column, value) in table.columns().iter().zip(excluded_row.iter()) {
        scope.insert_qualified(&format!("excluded.{}", column.name()), value.clone());
    }
}

#[derive(Debug, Clone)]
struct ReferencingForeignKey {
    child_table: crate::catalog::Table,
    child_column_indexes: Vec<usize>,
    parent_column_indexes: Vec<usize>,
    on_delete: ForeignKeyAction,
    on_update: ForeignKeyAction,
}

async fn apply_on_delete_actions(
    parent_table: &crate::catalog::Table,
    parent_rows_after: Vec<Vec<ScalarValue>>,
    deleted_parent_rows: Vec<Vec<ScalarValue>>,
) -> Result<HashMap<Oid, Vec<Vec<ScalarValue>>>, EngineError> {
    let staged_rows = with_storage_read(|storage| storage.rows_by_table.clone());
    apply_on_delete_actions_with_staged(
        parent_table,
        parent_rows_after,
        deleted_parent_rows,
        staged_rows,
    )
    .await
}

async fn apply_on_delete_actions_with_staged(
    parent_table: &crate::catalog::Table,
    parent_rows_after: Vec<Vec<ScalarValue>>,
    deleted_parent_rows: Vec<Vec<ScalarValue>>,
    mut staged_rows: HashMap<Oid, Vec<Vec<ScalarValue>>>,
) -> Result<HashMap<Oid, Vec<Vec<ScalarValue>>>, EngineError> {
    staged_rows.insert(parent_table.oid(), parent_rows_after);

    let mut queue: VecDeque<(crate::catalog::Table, Vec<Vec<ScalarValue>>)> = VecDeque::new();
    if !deleted_parent_rows.is_empty() {
        queue.push_back((parent_table.clone(), deleted_parent_rows));
    }

    while let Some((current_parent_table, current_deleted_rows)) = queue.pop_front() {
        let references = collect_referencing_foreign_keys(&current_parent_table)?;
        for reference in references {
            let parent_deleted_keys: HashSet<String> = current_deleted_rows
                .iter()
                .filter_map(|row| composite_non_null_key(row, &reference.parent_column_indexes))
                .collect();
            if parent_deleted_keys.is_empty() {
                continue;
            }

            let child_table_oid = reference.child_table.oid();
            let child_rows = staged_rows
                .get(&child_table_oid)
                .cloned()
                .unwrap_or_default();

            match reference.on_delete {
                ForeignKeyAction::Restrict => {
                    let violates = child_rows.iter().any(|row| {
                        composite_non_null_key(row, &reference.child_column_indexes)
                            .is_some_and(|key| parent_deleted_keys.contains(&key))
                    });
                    if violates {
                        return Err(EngineError {
                            message: format!(
                                "update or delete on relation \"{}\" violates foreign key from relation \"{}\"",
                                current_parent_table.qualified_name(),
                                reference.child_table.qualified_name()
                            ),
                        });
                    }
                }
                ForeignKeyAction::SetNull => {
                    let mut updated_rows = child_rows;
                    for row in &mut updated_rows {
                        let Some(key) =
                            composite_non_null_key(row, &reference.child_column_indexes)
                        else {
                            continue;
                        };
                        if !parent_deleted_keys.contains(&key) {
                            continue;
                        }
                        for idx in &reference.child_column_indexes {
                            if !reference.child_table.columns()[*idx].nullable() {
                                return Err(EngineError {
                                    message: format!(
                                        "ON DELETE SET NULL would violate not-null column \"{}\" in relation \"{}\"",
                                        reference.child_table.columns()[*idx].name(),
                                        reference.child_table.qualified_name()
                                    ),
                                });
                            }
                            row[*idx] = ScalarValue::Null;
                        }
                    }
                    staged_rows.insert(child_table_oid, updated_rows);
                }
                ForeignKeyAction::Cascade => {
                    let mut retained_rows = Vec::new();
                    let mut cascaded_rows = Vec::new();
                    for row in child_rows {
                        let should_delete =
                            composite_non_null_key(&row, &reference.child_column_indexes)
                                .is_some_and(|key| parent_deleted_keys.contains(&key));
                        if should_delete {
                            cascaded_rows.push(row);
                        } else {
                            retained_rows.push(row);
                        }
                    }
                    if !cascaded_rows.is_empty() {
                        staged_rows.insert(child_table_oid, retained_rows);
                        queue.push_back((reference.child_table, cascaded_rows));
                    }
                }
            }
        }
    }

    validate_staged_rows(&staged_rows).await?;
    Ok(staged_rows)
}

async fn apply_on_update_actions(
    parent_table: &crate::catalog::Table,
    parent_rows_before: &[Vec<ScalarValue>],
    parent_rows_after: Vec<Vec<ScalarValue>>,
) -> Result<HashMap<Oid, Vec<Vec<ScalarValue>>>, EngineError> {
    let mut initial_changes = Vec::new();
    for (old_row, new_row) in parent_rows_before.iter().zip(parent_rows_after.iter()) {
        if old_row != new_row {
            initial_changes.push((old_row.clone(), new_row.clone()));
        }
    }
    let staged_rows = with_storage_read(|storage| storage.rows_by_table.clone());
    apply_on_update_actions_with_staged(
        parent_table,
        parent_rows_after,
        initial_changes,
        staged_rows,
    )
    .await
}

async fn apply_on_update_actions_with_staged(
    parent_table: &crate::catalog::Table,
    parent_rows_after: Vec<Vec<ScalarValue>>,
    initial_changes: Vec<(Vec<ScalarValue>, Vec<ScalarValue>)>,
    mut staged_rows: HashMap<Oid, Vec<Vec<ScalarValue>>>,
) -> Result<HashMap<Oid, Vec<Vec<ScalarValue>>>, EngineError> {
    staged_rows.insert(parent_table.oid(), parent_rows_after);

    if initial_changes.is_empty() {
        return Ok(staged_rows);
    }

    #[allow(clippy::type_complexity)]
    let mut queue: VecDeque<(
        crate::catalog::Table,
        Vec<(Vec<ScalarValue>, Vec<ScalarValue>)>,
    )> = VecDeque::new();
    queue.push_back((parent_table.clone(), initial_changes));

    while let Some((current_parent_table, changed_rows)) = queue.pop_front() {
        let references = collect_referencing_foreign_keys(&current_parent_table)?;
        for reference in references {
            let mut key_updates: HashMap<String, Vec<ScalarValue>> = HashMap::new();
            for (old_row, new_row) in &changed_rows {
                let Some(old_key) =
                    composite_non_null_key(old_row, &reference.parent_column_indexes)
                else {
                    continue;
                };
                let Some(old_values) = values_at_indexes(old_row, &reference.parent_column_indexes)
                else {
                    continue;
                };
                let Some(new_values) = values_at_indexes(new_row, &reference.parent_column_indexes)
                else {
                    continue;
                };
                if old_values != new_values {
                    key_updates.insert(old_key, new_values);
                }
            }
            if key_updates.is_empty() {
                continue;
            }

            let child_table_oid = reference.child_table.oid();
            let child_rows = staged_rows
                .get(&child_table_oid)
                .cloned()
                .unwrap_or_default();

            match reference.on_update {
                ForeignKeyAction::Restrict => {
                    let violates = child_rows.iter().any(|row| {
                        composite_non_null_key(row, &reference.child_column_indexes)
                            .is_some_and(|key| key_updates.contains_key(&key))
                    });
                    if violates {
                        return Err(EngineError {
                            message: format!(
                                "update on relation \"{}\" violates foreign key from relation \"{}\"",
                                current_parent_table.qualified_name(),
                                reference.child_table.qualified_name()
                            ),
                        });
                    }
                }
                ForeignKeyAction::SetNull => {
                    let mut updated_rows = child_rows;
                    let mut changed_child_rows = Vec::new();
                    for row in &mut updated_rows {
                        let Some(key) =
                            composite_non_null_key(row, &reference.child_column_indexes)
                        else {
                            continue;
                        };
                        if !key_updates.contains_key(&key) {
                            continue;
                        }
                        let old_row = row.clone();
                        for idx in &reference.child_column_indexes {
                            if !reference.child_table.columns()[*idx].nullable() {
                                return Err(EngineError {
                                    message: format!(
                                        "ON UPDATE SET NULL would violate not-null column \"{}\" in relation \"{}\"",
                                        reference.child_table.columns()[*idx].name(),
                                        reference.child_table.qualified_name()
                                    ),
                                });
                            }
                            row[*idx] = ScalarValue::Null;
                        }
                        if *row != old_row {
                            changed_child_rows.push((old_row, row.clone()));
                        }
                    }
                    if !changed_child_rows.is_empty() {
                        staged_rows.insert(child_table_oid, updated_rows);
                        queue.push_back((reference.child_table.clone(), changed_child_rows));
                    }
                }
                ForeignKeyAction::Cascade => {
                    let mut updated_rows = child_rows;
                    let mut changed_child_rows = Vec::new();
                    for row in &mut updated_rows {
                        let Some(key) =
                            composite_non_null_key(row, &reference.child_column_indexes)
                        else {
                            continue;
                        };
                        let Some(new_values) = key_updates.get(&key) else {
                            continue;
                        };
                        let old_row = row.clone();
                        for (idx, new_value) in
                            reference.child_column_indexes.iter().zip(new_values.iter())
                        {
                            if matches!(new_value, ScalarValue::Null)
                                && !reference.child_table.columns()[*idx].nullable()
                            {
                                return Err(EngineError {
                                    message: format!(
                                        "ON UPDATE CASCADE would violate not-null column \"{}\" in relation \"{}\"",
                                        reference.child_table.columns()[*idx].name(),
                                        reference.child_table.qualified_name()
                                    ),
                                });
                            }
                            row[*idx] = new_value.clone();
                        }
                        if *row != old_row {
                            changed_child_rows.push((old_row, row.clone()));
                        }
                    }
                    if !changed_child_rows.is_empty() {
                        staged_rows.insert(child_table_oid, updated_rows);
                        queue.push_back((reference.child_table.clone(), changed_child_rows));
                    }
                }
            }
        }
    }

    validate_staged_rows(&staged_rows).await?;
    Ok(staged_rows)
}

async fn validate_staged_rows(
    staged_rows: &HashMap<Oid, Vec<Vec<ScalarValue>>>,
) -> Result<(), EngineError> {
    let tables = with_catalog_read(|catalog| {
        let mut out = Vec::new();
        for schema in catalog.schemas() {
            out.extend(schema.tables().cloned());
        }
        out
    });

    for table in tables {
        if table.kind() != TableKind::Heap {
            continue;
        }
        let rows = staged_rows.get(&table.oid()).cloned().unwrap_or_default();
        validate_table_constraints_with_overrides(&table, &rows, Some(staged_rows)).await?;
    }
    Ok(())
}

fn collect_referencing_foreign_keys(
    parent_table: &crate::catalog::Table,
) -> Result<Vec<ReferencingForeignKey>, EngineError> {
    with_catalog_read(
        |catalog| -> Result<Vec<ReferencingForeignKey>, EngineError> {
            let mut out = Vec::new();
            for schema in catalog.schemas() {
                for child_table in schema.tables() {
                    for constraint in child_table.foreign_key_constraints() {
                        let referenced_table = match catalog
                            .resolve_table(&constraint.referenced_table, &SearchPath::default())
                        {
                            Ok(table) => table,
                            Err(_) => continue,
                        };
                        if referenced_table.oid() != parent_table.oid() {
                            continue;
                        }

                        let child_column_indexes = constraint
                            .columns
                            .iter()
                            .map(|column| find_column_index(child_table, column))
                            .collect::<Result<Vec<_>, _>>()?;
                        let referenced_columns =
                            resolve_referenced_columns(child_table, constraint, referenced_table)?;
                        let parent_column_indexes = referenced_columns
                            .iter()
                            .map(|column| find_column_index(parent_table, column))
                            .collect::<Result<Vec<_>, _>>()?;

                        out.push(ReferencingForeignKey {
                            child_table: child_table.clone(),
                            child_column_indexes,
                            parent_column_indexes,
                            on_delete: constraint.on_delete,
                            on_update: constraint.on_update,
                        });
                    }
                }
            }
            Ok(out)
        },
    )
}

fn resolve_foreign_key_indexes(
    child_table: &crate::catalog::Table,
    constraint: &crate::catalog::ForeignKeyConstraint,
) -> Result<(crate::catalog::Table, Vec<usize>, Vec<usize>), EngineError> {
    let referenced_table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&constraint.referenced_table, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    let child_column_indexes = constraint
        .columns
        .iter()
        .map(|column| find_column_index(child_table, column))
        .collect::<Result<Vec<_>, _>>()?;
    let referenced_columns =
        resolve_referenced_columns(child_table, constraint, &referenced_table)?;
    let parent_column_indexes = referenced_columns
        .iter()
        .map(|column| find_column_index(&referenced_table, column))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        referenced_table,
        child_column_indexes,
        parent_column_indexes,
    ))
}

fn resolve_referenced_columns(
    child_table: &crate::catalog::Table,
    constraint: &crate::catalog::ForeignKeyConstraint,
    referenced_table: &crate::catalog::Table,
) -> Result<Vec<String>, EngineError> {
    let referenced_columns = if !constraint.referenced_columns.is_empty() {
        constraint.referenced_columns.clone()
    } else if constraint.columns.len() == 1
        && referenced_table
            .columns()
            .iter()
            .any(|column| column.name() == constraint.columns[0])
    {
        vec![constraint.columns[0].clone()]
    } else {
        primary_key_columns(referenced_table)?
    };
    if referenced_columns.len() != constraint.columns.len() {
        return Err(EngineError {
            message: format!(
                "foreign key on relation \"{}\" has {} referencing columns but {} referenced columns on \"{}\"",
                child_table.qualified_name(),
                constraint.columns.len(),
                referenced_columns.len(),
                referenced_table.qualified_name()
            ),
        });
    }
    Ok(referenced_columns)
}

fn primary_key_columns(table: &crate::catalog::Table) -> Result<Vec<String>, EngineError> {
    table
        .key_constraints()
        .iter()
        .find(|constraint| constraint.primary)
        .map(|constraint| constraint.columns.clone())
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}\" referenced by foreign key does not have a primary key",
                table.qualified_name()
            ),
        })
}

pub(crate) fn preview_table_with_added_constraint(
    table: &crate::catalog::Table,
    constraint: &TableConstraint,
) -> Result<crate::catalog::Table, EngineError> {
    let mut preview = table.clone();
    match constraint {
        TableConstraint::PrimaryKey { .. } | TableConstraint::Unique { .. } => {
            let mut specs = crate::commands::create_table::key_constraint_specs_from_ast(
                std::slice::from_ref(constraint),
            )?;
            let spec = specs.pop().expect("one key constraint spec");
            if let Some(name) = &spec.name
                && table_constraint_name_exists(&preview, name)
            {
                return Err(EngineError {
                    message: format!(
                        "constraint \"{}\" already exists for relation \"{}\"",
                        name,
                        preview.qualified_name()
                    ),
                });
            }
            if spec.primary
                && preview
                    .key_constraints()
                    .iter()
                    .any(|existing| existing.primary)
            {
                return Err(EngineError {
                    message: format!(
                        "relation \"{}\" already has a primary key",
                        preview.qualified_name()
                    ),
                });
            }
            for column in &spec.columns {
                find_column_index(&preview, column)?;
            }
            preview
                .key_constraints_mut()
                .push(crate::catalog::KeyConstraint {
                    name: spec.name,
                    columns: spec.columns.clone(),
                    primary: spec.primary,
                });
            if spec.primary {
                for key_col in spec.columns {
                    if let Some(column) = preview
                        .columns_mut()
                        .iter_mut()
                        .find(|column| column.name() == key_col)
                    {
                        column.set_nullable(false);
                    }
                }
            }
        }
        TableConstraint::ForeignKey { .. } => {
            let mut specs = crate::commands::create_table::foreign_key_constraint_specs_from_ast(
                std::slice::from_ref(constraint),
            )?;
            let spec = specs.pop().expect("one foreign key constraint spec");
            if let Some(name) = &spec.name
                && table_constraint_name_exists(&preview, name)
            {
                return Err(EngineError {
                    message: format!(
                        "constraint \"{}\" already exists for relation \"{}\"",
                        name,
                        preview.qualified_name()
                    ),
                });
            }
            for column in &spec.columns {
                find_column_index(&preview, column)?;
            }
            preview
                .foreign_key_constraints_mut()
                .push(crate::catalog::ForeignKeyConstraint {
                    name: spec.name,
                    columns: spec.columns,
                    referenced_table: spec.referenced_table,
                    referenced_columns: spec.referenced_columns,
                    on_delete: spec.on_delete,
                    on_update: spec.on_update,
                });
        }
    }
    Ok(preview)
}

fn table_constraint_name_exists(table: &crate::catalog::Table, name: &str) -> bool {
    table
        .key_constraints()
        .iter()
        .any(|constraint| constraint.name.as_deref() == Some(name))
        || table
            .foreign_key_constraints()
            .iter()
            .any(|constraint| constraint.name.as_deref() == Some(name))
}

fn values_at_indexes(row: &[ScalarValue], indexes: &[usize]) -> Option<Vec<ScalarValue>> {
    let mut out = Vec::with_capacity(indexes.len());
    for idx in indexes {
        out.push(row.get(*idx)?.clone());
    }
    Some(out)
}

fn composite_non_null_key(row: &[ScalarValue], indexes: &[usize]) -> Option<String> {
    let mut out = Vec::with_capacity(indexes.len());
    for idx in indexes {
        let value = row.get(*idx)?;
        if matches!(value, ScalarValue::Null) {
            return None;
        }
        out.push(scalar_key(value));
    }
    Some(out.join("|"))
}

fn scalar_key(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "N".to_string(),
        ScalarValue::Bool(v) => format!("B:{v}"),
        ScalarValue::Int(v) => format!("I:{v}"),
        ScalarValue::Float(v) => format!("F:{v}"),
        ScalarValue::Text(v) => format!("T:{v}"),
        ScalarValue::Array(_) => format!("A:{}", value.render()),
    }
}

pub(crate) fn coerce_value_for_column_spec(
    value: ScalarValue,
    spec: &ColumnSpec,
) -> Result<ScalarValue, EngineError> {
    let temp_column = Column::new(
        0,
        spec.name.clone(),
        spec.type_signature,
        0,
        spec.nullable,
        spec.unique,
        spec.primary_key,
        spec.references.clone(),
        spec.check.clone(),
        spec.default.clone(),
    );
    coerce_value_for_column(value, &temp_column)
}

fn coerce_value_for_column(
    value: ScalarValue,
    column: &Column,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        if !column.nullable() {
            return Err(EngineError {
                message: format!("column \"{}\" does not allow null values", column.name()),
            });
        }
        return Ok(ScalarValue::Null);
    }

    match (column.type_signature(), value) {
        (TypeSignature::Bool, ScalarValue::Bool(v)) => Ok(ScalarValue::Bool(v)),
        (TypeSignature::Bool, ScalarValue::Text(v)) => match v.trim().to_ascii_lowercase().as_str()
        {
            "true" | "t" | "1" => Ok(ScalarValue::Bool(true)),
            "false" | "f" | "0" => Ok(ScalarValue::Bool(false)),
            _ => Err(EngineError {
                message: format!("invalid boolean literal for column \"{}\"", column.name()),
            }),
        },
        (TypeSignature::Int8, ScalarValue::Int(v)) => Ok(ScalarValue::Int(v)),
        (TypeSignature::Int8, ScalarValue::Text(v)) => {
            let parsed = v.trim().parse::<i64>().map_err(|_| EngineError {
                message: format!("invalid integer literal for column \"{}\"", column.name()),
            })?;
            Ok(ScalarValue::Int(parsed))
        }
        (TypeSignature::Float8, ScalarValue::Int(v)) => Ok(ScalarValue::Float(v as f64)),
        (TypeSignature::Float8, ScalarValue::Float(v)) => Ok(ScalarValue::Float(v)),
        (TypeSignature::Float8, ScalarValue::Text(v)) => {
            let parsed = v.trim().parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid float literal for column \"{}\"", column.name()),
            })?;
            Ok(ScalarValue::Float(parsed))
        }
        (TypeSignature::Text, ScalarValue::Text(v)) => Ok(ScalarValue::Text(v)),
        (TypeSignature::Text, v) => Ok(ScalarValue::Text(v.render())),
        (TypeSignature::Date, ScalarValue::Text(v)) => {
            let dt = parse_datetime_text(&v)?;
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        (TypeSignature::Date, ScalarValue::Int(v)) => {
            let dt = datetime_from_epoch_seconds(v);
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        (TypeSignature::Date, ScalarValue::Float(v)) => {
            let dt = datetime_from_epoch_seconds(v as i64);
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        (TypeSignature::Timestamp, ScalarValue::Text(v)) => {
            let dt = parse_datetime_text(&v)?;
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        (TypeSignature::Timestamp, ScalarValue::Int(v)) => {
            let dt = datetime_from_epoch_seconds(v);
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        (TypeSignature::Timestamp, ScalarValue::Float(v)) => {
            let dt = datetime_from_epoch_seconds(v as i64);
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        _ => Err(EngineError {
            message: format!("type mismatch for column \"{}\"", column.name()),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedOutputColumn {
    name: String,
    type_oid: u32,
}

#[derive(Debug, Clone, Default)]
struct TypeScope {
    unqualified: HashMap<String, u32>,
    qualified: HashMap<String, u32>,
    ambiguous: HashSet<String>,
}

impl TypeScope {
    fn insert_unqualified(&mut self, key: &str, type_oid: u32) {
        let key = key.to_ascii_lowercase();
        if self.ambiguous.contains(&key) {
            return;
        }
        #[allow(clippy::map_entry)]
        if self.unqualified.contains_key(&key) {
            self.unqualified.remove(&key);
            self.ambiguous.insert(key);
        } else {
            self.unqualified.insert(key, type_oid);
        }
    }

    fn insert_qualified(&mut self, parts: &[String], type_oid: u32) {
        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.insert(key, type_oid);
    }

    fn lookup_identifier(&self, parts: &[String]) -> Option<u32> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            let key = parts[0].to_ascii_lowercase();
            if self.ambiguous.contains(&key) {
                return None;
            }
            return self.unqualified.get(&key).copied();
        }

        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.get(&key).copied()
    }
}

#[derive(Debug, Clone)]
struct ExpandedFromTypeColumn {
    label: String,
    lookup_parts: Vec<String>,
    type_oid: u32,
}

fn cast_type_name_to_oid(type_name: &str) -> u32 {
    match type_name.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => PG_BOOL_OID,
        "int8" | "int4" | "int2" | "bigint" | "integer" | "smallint" => PG_INT8_OID,
        "float8" | "float4" | "numeric" | "decimal" | "real" => PG_FLOAT8_OID,
        "date" => PG_DATE_OID,
        "timestamp" | "timestamptz" => PG_TIMESTAMP_OID,
        _ => PG_TEXT_OID,
    }
}

fn infer_numeric_result_oid(left: u32, right: u32) -> u32 {
    if left == PG_FLOAT8_OID || right == PG_FLOAT8_OID {
        PG_FLOAT8_OID
    } else {
        PG_INT8_OID
    }
}

fn infer_common_type_oid(
    exprs: &[Expr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    let mut oid = PG_TEXT_OID;
    for expr in exprs {
        let next = infer_expr_type_oid(expr, scope, ctes);
        if next == PG_TEXT_OID {
            continue;
        }
        if oid == PG_TEXT_OID {
            oid = next;
            continue;
        }
        if oid == next {
            continue;
        }
        if (oid == PG_INT8_OID || oid == PG_FLOAT8_OID)
            && (next == PG_INT8_OID || next == PG_FLOAT8_OID)
        {
            oid = infer_numeric_result_oid(oid, next);
            continue;
        }
        oid = PG_TEXT_OID;
    }
    oid
}

fn infer_function_return_oid(
    name: &[String],
    args: &[Expr],
    within_group: &[OrderByExpr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    let fn_name = name
        .last()
        .map(|part| part.to_ascii_lowercase())
        .unwrap_or_default();
    match fn_name.as_str() {
        "count" | "char_length" | "length" | "nextval" | "currval" | "setval"
        | "strpos" | "position" | "ascii" | "pg_backend_pid" | "width_bucket"
        | "scale" | "factorial" | "num_nulls" | "num_nonnulls" => PG_INT8_OID,
        "extract" | "date_part" => PG_INT8_OID,
        "avg" | "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop"
        | "corr" | "covar_pop" | "covar_samp" | "regr_slope" | "regr_intercept" | "regr_r2"
        | "regr_avgx" | "regr_avgy" | "regr_sxx" | "regr_sxy" | "regr_syy"
        | "percentile_cont" => PG_FLOAT8_OID,
        "regr_count" => PG_INT8_OID,
        "bool_and" | "bool_or" | "every" | "has_table_privilege" | "has_column_privilege"
        | "has_schema_privilege" | "pg_table_is_visible" | "pg_type_is_visible"
        | "isfinite" => PG_BOOL_OID,
        "abs" | "ceil" | "ceiling" | "floor" | "round" | "trunc" | "sign" | "mod" => args
            .first()
            .map(|expr| infer_expr_type_oid(expr, scope, ctes))
            .unwrap_or(PG_FLOAT8_OID),
        "power" | "pow" | "sqrt" | "cbrt" | "exp" | "ln" | "log" | "sin" | "cos" | "tan"
        | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "pi" | "random"
        | "to_number" => PG_FLOAT8_OID,
        "div" | "gcd" | "lcm" | "ntile" | "row_number" | "rank" | "dense_rank" => PG_INT8_OID,
        "percent_rank" | "cume_dist" => PG_FLOAT8_OID,
        "sum" => args
            .first()
            .map(|expr| {
                let oid = infer_expr_type_oid(expr, scope, ctes);
                if oid == PG_FLOAT8_OID {
                    PG_FLOAT8_OID
                } else {
                    PG_INT8_OID
                }
            })
            .unwrap_or(PG_INT8_OID),
        "percentile_disc" | "mode" => within_group
            .first()
            .map(|entry| infer_expr_type_oid(&entry.expr, scope, ctes))
            .unwrap_or(PG_TEXT_OID),
        "min" | "max" | "nullif" => args
            .first()
            .map(|expr| infer_expr_type_oid(expr, scope, ctes))
            .unwrap_or(PG_TEXT_OID),
        "coalesce" | "greatest" | "least" => infer_common_type_oid(args, scope, ctes),
        "date" | "current_date" | "to_date" => PG_DATE_OID,
        "timestamp" | "current_timestamp" | "now" | "date_trunc" | "to_timestamp"
        | "clock_timestamp" => PG_TIMESTAMP_OID,
        "date_add" | "date_sub" => PG_DATE_OID,
        "jsonb_path_exists" | "jsonb_path_match" | "jsonb_exists" | "jsonb_exists_any"
        | "jsonb_exists_all" => PG_BOOL_OID,
        "connect" if name.len() == 2 && name[0].eq_ignore_ascii_case("ws") => PG_INT8_OID,
        "send" | "close" if name.len() == 2 && name[0].eq_ignore_ascii_case("ws") => PG_BOOL_OID,
        _ => PG_TEXT_OID,
    }
}

fn infer_expr_type_oid(
    expr: &Expr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    match expr {
        Expr::Identifier(parts) => scope.lookup_identifier(parts).unwrap_or(PG_TEXT_OID),
        Expr::String(_) => PG_TEXT_OID,
        Expr::Integer(_) => PG_INT8_OID,
        Expr::Float(_) => PG_FLOAT8_OID,
        Expr::Boolean(_) => PG_BOOL_OID,
        Expr::Null => PG_TEXT_OID,
        Expr::Parameter(_) => PG_TEXT_OID,
        Expr::FunctionCall {
            name,
            args,
            within_group,
            ..
        } => infer_function_return_oid(name, args, within_group, scope, ctes),
        Expr::Cast { type_name, .. } => cast_type_name_to_oid(type_name),
        Expr::Wildcard => PG_TEXT_OID,
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => PG_BOOL_OID,
            UnaryOp::Plus | UnaryOp::Minus => infer_expr_type_oid(expr, scope, ctes),
        },
        Expr::Binary { left, op, right } => {
            let left_oid = infer_expr_type_oid(left, scope, ctes);
            let right_oid = infer_expr_type_oid(right, scope, ctes);
            match op {
                BinaryOp::Or
                | BinaryOp::And
                | BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::Gt
                | BinaryOp::Gte
                | BinaryOp::JsonContains
                | BinaryOp::JsonContainedBy
                | BinaryOp::JsonPathExists
                | BinaryOp::JsonPathMatch
                | BinaryOp::JsonHasKey
                | BinaryOp::JsonHasAny
                | BinaryOp::JsonHasAll => PG_BOOL_OID,
                BinaryOp::JsonGet
                | BinaryOp::JsonGetText
                | BinaryOp::JsonPath
                | BinaryOp::JsonPathText
                | BinaryOp::JsonConcat
                | BinaryOp::JsonDeletePath => PG_TEXT_OID,
                BinaryOp::Add => {
                    if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && right_oid == PG_INT8_OID
                    {
                        left_oid
                    } else if (right_oid == PG_DATE_OID || right_oid == PG_TIMESTAMP_OID)
                        && left_oid == PG_INT8_OID
                    {
                        right_oid
                    } else {
                        infer_numeric_result_oid(left_oid, right_oid)
                    }
                }
                BinaryOp::Sub => {
                    if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && (right_oid == PG_DATE_OID || right_oid == PG_TIMESTAMP_OID)
                    {
                        PG_INT8_OID
                    } else if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && right_oid == PG_INT8_OID
                    {
                        left_oid
                    } else if left_oid == PG_TEXT_OID && right_oid == PG_TEXT_OID {
                        PG_TEXT_OID
                    } else {
                        infer_numeric_result_oid(left_oid, right_oid)
                    }
                }
                BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    infer_numeric_result_oid(left_oid, right_oid)
                }
            }
        }
        Expr::AnyAll { .. } => PG_BOOL_OID,
        Expr::Exists(_)
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. } => PG_BOOL_OID,
        Expr::CaseSimple {
            when_then,
            else_expr,
            ..
        }
        | Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            let mut result_exprs = when_then
                .iter()
                .map(|(_, then_expr)| then_expr.clone())
                .collect::<Vec<_>>();
            if let Some(else_expr) = else_expr {
                result_exprs.push((**else_expr).clone());
            }
            infer_common_type_oid(&result_exprs, scope, ctes)
        }
        Expr::ScalarSubquery(query) => {
            let mut nested = ctes.clone();
            derive_query_output_columns_with_ctes(query, &mut nested)
                .ok()
                .and_then(|cols| cols.first().map(|col| col.type_oid))
                .unwrap_or(PG_TEXT_OID)
        }
        Expr::ArrayConstructor(_) | Expr::ArraySubquery(_) => PG_TEXT_OID,
    }
}

fn infer_select_target_name(target: &SelectItem) -> Result<String, EngineError> {
    if let Some(alias) = &target.alias {
        return Ok(alias.clone());
    }
    let name = match &target.expr {
        Expr::Identifier(parts) => parts
            .last()
            .cloned()
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::FunctionCall { name, .. } => name
            .last()
            .cloned()
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::Wildcard => {
            return Err(EngineError {
                message: "wildcard target requires FROM support".to_string(),
            });
        }
        _ => "?column?".to_string(),
    };
    Ok(name)
}

fn derive_query_output_columns(query: &Query) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let mut ctes = HashMap::new();
    derive_query_output_columns_with_ctes(query, &mut ctes)
}

fn derive_query_output_columns_with_ctes(
    query: &Query,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let mut local_ctes = ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            let cte_name = cte.name.to_ascii_lowercase();
            let cols = if with.recursive && query_references_relation(&cte.query, &cte_name) {
                derive_recursive_cte_output_columns(cte, &local_ctes)?
            } else {
                derive_query_output_columns_with_ctes(&cte.query, &mut local_ctes)?
            };
            local_ctes.insert(cte_name, cols);
        }
    }
    derive_query_expr_output_columns(&query.body, &local_ctes)
}

fn derive_recursive_cte_output_columns(
    cte: &crate::parser::ast::CommonTableExpr,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier: _,
        right,
    } = &cte.query.body
    else {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must be of the form non-recursive-term UNION [ALL] recursive-term",
                cte.name
            ),
        });
    };
    if *op != SetOperator::Union {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must use UNION or UNION ALL",
                cte.name
            ),
        });
    }
    validate_recursive_cte_terms(&cte.name, &cte_name, left, right)?;

    let left_cols = derive_query_expr_output_columns(left, ctes)?;
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_output_columns(right, &recursive_ctes)?;
    if left_cols.len() != right_cols.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }
    Ok(left_cols)
}

fn derive_query_expr_output_columns(
    expr: &QueryExpr,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    match expr {
        QueryExpr::Select(select) => derive_select_output_columns(select, ctes),
        QueryExpr::SetOperation { left, right, .. } => {
            let left_cols = derive_query_expr_output_columns(left, ctes)?;
            let right_cols = derive_query_expr_output_columns(right, ctes)?;
            if left_cols.len() != right_cols.len() {
                return Err(EngineError {
                    message: "set-operation inputs must have matching column counts".to_string(),
                });
            }
            Ok(left_cols)
        }
        QueryExpr::Nested(query) => {
            let mut nested = ctes.clone();
            derive_query_output_columns_with_ctes(query, &mut nested)
        }
    }
}

fn derive_select_output_columns(
    select: &SelectStatement,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let from_columns = if select.from.is_empty() {
        Vec::new()
    } else {
        expand_from_columns_typed(&select.from, ctes).unwrap_or_default()
    };
    let wildcard_columns = if select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard))
    {
        Some(expand_from_columns_typed(&select.from, ctes)?)
    } else {
        None
    };

    let mut type_scope = TypeScope::default();
    for col in from_columns {
        type_scope.insert_unqualified(&col.label, col.type_oid);
        type_scope.insert_qualified(&col.lookup_parts, col.type_oid);
    }

    let mut columns = Vec::new();
    for target in &select.targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            columns.extend(expanded.iter().map(|col| PlannedOutputColumn {
                name: col.label.clone(),
                type_oid: col.type_oid,
            }));
            continue;
        }

        columns.push(PlannedOutputColumn {
            name: infer_select_target_name(target)?,
            type_oid: infer_expr_type_oid(&target.expr, &type_scope, ctes),
        });
    }
    Ok(columns)
}

pub(crate) fn derive_query_columns(query: &Query) -> Result<Vec<String>, EngineError> {
    let mut ctes = HashMap::new();
    derive_query_columns_with_ctes(query, &mut ctes)
}

fn derive_query_columns_with_ctes(
    query: &Query,
    ctes: &mut HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let mut local_ctes = ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            let cte_name = cte.name.to_ascii_lowercase();
            let cols = if with.recursive && query_references_relation(&cte.query, &cte_name) {
                derive_recursive_cte_columns(cte, &local_ctes)?
            } else {
                derive_query_columns_with_ctes(&cte.query, &mut local_ctes)?
            };
            local_ctes.insert(cte_name, cols);
        }
    }
    derive_query_expr_columns(&query.body, &local_ctes)
}

fn derive_recursive_cte_columns(
    cte: &crate::parser::ast::CommonTableExpr,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier: _,
        right,
    } = &cte.query.body
    else {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must be of the form non-recursive-term UNION [ALL] recursive-term",
                cte.name
            ),
        });
    };
    if *op != SetOperator::Union {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must use UNION or UNION ALL",
                cte.name
            ),
        });
    }
    validate_recursive_cte_terms(&cte.name, &cte_name, left, right)?;

    let left_cols = derive_query_expr_columns(left, ctes)?;
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_columns(right, &recursive_ctes)?;
    if left_cols.len() != right_cols.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }
    Ok(left_cols)
}

pub(crate) fn query_references_relation(query: &Query, relation_name: &str) -> bool {
    if query_expr_references_relation(&query.body, relation_name) {
        return true;
    }
    if query
        .order_by
        .iter()
        .any(|order| expr_references_relation(&order.expr, relation_name))
    {
        return true;
    }
    if query
        .limit
        .as_ref()
        .is_some_and(|expr| expr_references_relation(expr, relation_name))
    {
        return true;
    }
    if query
        .offset
        .as_ref()
        .is_some_and(|expr| expr_references_relation(expr, relation_name))
    {
        return true;
    }
    query.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| query_references_relation(&cte.query, relation_name))
    })
}

fn query_expr_references_relation(expr: &QueryExpr, relation_name: &str) -> bool {
    match expr {
        QueryExpr::Select(select) => {
            select
                .from
                .iter()
                .any(|from| table_expression_references_relation(from, relation_name))
                || select
                    .targets
                    .iter()
                    .any(|target| expr_references_relation(&target.expr, relation_name))
                || select
                    .where_clause
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
                || select.group_by.iter().any(|group_expr| match group_expr {
                    GroupByExpr::Expr(expr) => expr_references_relation(expr, relation_name),
                    GroupByExpr::GroupingSets(sets) => sets.iter().any(|set| {
                        set.iter()
                            .any(|expr| expr_references_relation(expr, relation_name))
                    }),
                    GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => exprs
                        .iter()
                        .any(|expr| expr_references_relation(expr, relation_name)),
                })
                || select
                    .having
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        QueryExpr::SetOperation { left, right, .. } => {
            query_expr_references_relation(left, relation_name)
                || query_expr_references_relation(right, relation_name)
        }
        QueryExpr::Nested(query) => query_references_relation(query, relation_name),
    }
}

fn table_expression_references_relation(table: &TableExpression, relation_name: &str) -> bool {
    match table {
        TableExpression::Relation(rel) => {
            rel.name.len() == 1 && rel.name[0].eq_ignore_ascii_case(relation_name)
        }
        TableExpression::Function(function) => function
            .args
            .iter()
            .any(|arg| expr_references_relation(arg, relation_name)),
        TableExpression::Subquery(sub) => query_references_relation(&sub.query, relation_name),
        TableExpression::Join(join) => {
            table_expression_references_relation(&join.left, relation_name)
                || table_expression_references_relation(&join.right, relation_name)
                || join
                    .condition
                    .as_ref()
                    .is_some_and(|condition| match condition {
                        JoinCondition::On(expr) => expr_references_relation(expr, relation_name),
                        JoinCondition::Using(_) => false,
                    })
        }
    }
}

fn expr_references_relation(expr: &Expr, relation_name: &str) -> bool {
    match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.iter()
                .any(|arg| expr_references_relation(arg, relation_name))
                || order_by
                    .iter()
                    .any(|entry| expr_references_relation(&entry.expr, relation_name))
                || within_group
                    .iter()
                    .any(|entry| expr_references_relation(&entry.expr, relation_name))
                || filter
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::Cast { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::Unary { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::Binary { left, right, .. } => {
            expr_references_relation(left, relation_name)
                || expr_references_relation(right, relation_name)
        }
        Expr::Exists(query) | Expr::ScalarSubquery(query) => {
            query_references_relation(query, relation_name)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_relation(expr, relation_name)
                || list
                    .iter()
                    .any(|item| expr_references_relation(item, relation_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_references_relation(expr, relation_name)
                || query_references_relation(subquery, relation_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_relation(expr, relation_name)
                || expr_references_relation(low, relation_name)
                || expr_references_relation(high, relation_name)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_references_relation(expr, relation_name)
                || expr_references_relation(pattern, relation_name)
        }
        Expr::IsNull { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::IsDistinctFrom { left, right, .. } => {
            expr_references_relation(left, relation_name)
                || expr_references_relation(right, relation_name)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            expr_references_relation(operand, relation_name)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_references_relation(when_expr, relation_name)
                        || expr_references_relation(then_expr, relation_name)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                expr_references_relation(when_expr, relation_name)
                    || expr_references_relation(then_expr, relation_name)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::ArrayConstructor(items) => items
            .iter()
            .any(|item| expr_references_relation(item, relation_name)),
        Expr::ArraySubquery(query) => query_references_relation(query, relation_name),
        _ => false,
    }
}

pub(crate) fn validate_recursive_cte_terms(
    cte_display_name: &str,
    cte_name: &str,
    left: &QueryExpr,
    right: &QueryExpr,
) -> Result<(), EngineError> {
    if query_expr_references_relation(left, cte_name) {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" has self-reference in non-recursive term",
                cte_display_name
            ),
        });
    }
    if !query_expr_references_relation(right, cte_name) {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must reference itself in recursive term",
                cte_display_name
            ),
        });
    }
    Ok(())
}

fn derive_query_expr_columns(
    expr: &QueryExpr,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    match expr {
        QueryExpr::Select(select) => derive_select_columns(select, ctes),
        QueryExpr::SetOperation { left, right, .. } => {
            let left_cols = derive_query_expr_columns(left, ctes)?;
            let right_cols = derive_query_expr_columns(right, ctes)?;
            if left_cols.len() != right_cols.len() {
                return Err(EngineError {
                    message: "set-operation inputs must have matching column counts".to_string(),
                });
            }
            Ok(left_cols)
        }
        QueryExpr::Nested(query) => {
            let mut nested = ctes.clone();
            derive_query_columns_with_ctes(query, &mut nested)
        }
    }
}

pub(crate) fn derive_select_columns(
    select: &SelectStatement,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let wildcard_columns = if select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard))
    {
        Some(expand_from_columns(&select.from, ctes)?)
    } else {
        None
    };
    let mut columns = Vec::with_capacity(select.targets.len());
    for target in &select.targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                columns.push(col.label.clone());
            }
            continue;
        }

        if let Some(alias) = &target.alias {
            columns.push(alias.clone());
            continue;
        }
        let name = match &target.expr {
            Expr::Identifier(parts) => parts
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            Expr::FunctionCall { name, .. } => name
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            Expr::Wildcard => {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            }
            _ => "?column?".to_string(),
        };
        columns.push(name);
    }
    Ok(columns)
}

fn derive_dml_returning_columns(
    table_name: &[String],
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<String>, EngineError> {
    if returning.is_empty() {
        return Ok(Vec::new());
    }
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    derive_returning_columns_from_table(&table, returning)
}

fn derive_dml_returning_column_type_oids(
    table_name: &[String],
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<u32>, EngineError> {
    if returning.is_empty() {
        return Ok(Vec::new());
    }
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    derive_returning_column_type_oids_from_table(&table, returning)
}

fn derive_returning_column_type_oids_from_table(
    table: &crate::catalog::Table,
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<u32>, EngineError> {
    let mut scope = TypeScope::default();
    for column in table.columns() {
        let oid = type_signature_to_oid(column.type_signature());
        scope.insert_unqualified(column.name(), oid);
        scope.insert_qualified(&[table.name().to_string(), column.name().to_string()], oid);
        scope.insert_qualified(&[table.qualified_name(), column.name().to_string()], oid);
    }

    let ctes = HashMap::new();
    let mut out = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            out.extend(
                table
                    .columns()
                    .iter()
                    .map(|column| type_signature_to_oid(column.type_signature())),
            );
            continue;
        }
        out.push(infer_expr_type_oid(&target.expr, &scope, &ctes));
    }
    Ok(out)
}

fn derive_returning_columns_from_table(
    table: &crate::catalog::Table,
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<String>, EngineError> {
    let mut columns = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            columns.extend(
                table
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string()),
            );
            continue;
        }
        if let Some(alias) = &target.alias {
            columns.push(alias.clone());
            continue;
        }
        let name = match &target.expr {
            Expr::Identifier(parts) => parts
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            Expr::FunctionCall { name, .. } => name
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            _ => "?column?".to_string(),
        };
        columns.push(name);
    }
    Ok(columns)
}

async fn project_returning_row(
    returning: &[crate::parser::ast::SelectItem],
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let scope = scope_for_table_row(table, row);
    project_returning_row_from_scope(returning, row, &scope, params).await
}

async fn project_returning_row_with_qualifiers(
    returning: &[crate::parser::ast::SelectItem],
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    qualifiers: &[String],
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let scope = scope_for_table_row_with_qualifiers(table, row, qualifiers);
    project_returning_row_from_scope(returning, row, &scope, params).await
}

async fn project_returning_row_from_scope(
    returning: &[crate::parser::ast::SelectItem],
    row: &[ScalarValue],
    scope: &EvalScope,
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut out = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            out.extend(row.iter().cloned());
            continue;
        }
        out.push(eval_expr(&target.expr, scope, params).await?);
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub(crate) struct ExpandedFromColumn {
    pub(crate) label: String,
    pub(crate) lookup_parts: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CteBinding {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<ScalarValue>>,
}

thread_local! {
    static ACTIVE_CTE_STACK: RefCell<Vec<HashMap<String, CteBinding>>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn current_cte_binding(name: &str) -> Option<CteBinding> {
    ACTIVE_CTE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|ctes| ctes.get(&name.to_ascii_lowercase()).cloned())
    })
}

#[allow(dead_code)]
fn with_cte_context<T>(ctes: HashMap<String, CteBinding>, f: impl FnOnce() -> T) -> T {
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().push(ctes);
        let out = f();
        stack.borrow_mut().pop();
        out
    })
}

pub(crate) async fn with_cte_context_async<T, F, Fut>(
    ctes: HashMap<String, CteBinding>,
    f: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().push(ctes);
    });
    let out = f().await;
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().pop();
    });
    out
}

pub(crate) fn active_cte_context() -> HashMap<String, CteBinding> {
    ACTIVE_CTE_STACK.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}

#[derive(Debug, Clone)]
pub(crate) struct VirtualRelationColumnDef {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
}

pub(crate) fn lookup_virtual_relation(
    name: &[String],
) -> Option<(String, String, Vec<VirtualRelationColumnDef>)> {
    let (schema, relation) = resolve_virtual_relation_name(name)?;
    let columns = virtual_relation_column_defs(&schema, &relation)?;
    Some((schema, relation, columns))
}

fn resolve_virtual_relation_name(name: &[String]) -> Option<(String, String)> {
    let normalized = name
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    match normalized.as_slice() {
        [relation] if is_pg_catalog_virtual_relation(relation) => {
            Some(("pg_catalog".to_string(), relation.to_string()))
        }
        [schema, relation]
            if schema == "pg_catalog" && is_pg_catalog_virtual_relation(relation) =>
        {
            Some((schema.to_string(), relation.to_string()))
        }
        [schema, relation]
            if schema == "information_schema"
                && is_information_schema_virtual_relation(relation) =>
        {
            Some((schema.to_string(), relation.to_string()))
        }
        [schema, relation] if schema == "ws" && relation == "connections" => {
            Some(("ws".to_string(), "connections".to_string()))
        }
        _ => None,
    }
}

fn is_pg_catalog_virtual_relation(relation: &str) -> bool {
    matches!(
        relation,
        "pg_namespace" | "pg_class" | "pg_attribute" | "pg_type"
            | "pg_database" | "pg_roles" | "pg_settings"
            | "pg_tables" | "pg_views" | "pg_indexes"
            | "pg_proc" | "pg_constraint" | "pg_extension"
    )
}

fn is_information_schema_virtual_relation(relation: &str) -> bool {
    matches!(relation, "tables" | "columns" | "schemata" | "key_column_usage" | "table_constraints")
}

fn virtual_relation_column_defs(
    schema: &str,
    relation: &str,
) -> Option<Vec<VirtualRelationColumnDef>> {
    let cols = match (schema, relation) {
        ("pg_catalog", "pg_namespace") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "nspname".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("pg_catalog", "pg_class") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "relname".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "relnamespace".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "relkind".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("pg_catalog", "pg_attribute") => vec![
            VirtualRelationColumnDef {
                name: "attrelid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attname".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "atttypid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attnum".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "attnotnull".to_string(),
                type_oid: PG_BOOL_OID,
            },
        ],
        ("pg_catalog", "pg_type") => vec![
            VirtualRelationColumnDef {
                name: "oid".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "typname".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "tables") => vec![
            VirtualRelationColumnDef {
                name: "table_schema".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_type".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "columns") => vec![
            VirtualRelationColumnDef {
                name: "table_schema".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "table_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "column_name".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "ordinal_position".to_string(),
                type_oid: PG_INT8_OID,
            },
            VirtualRelationColumnDef {
                name: "data_type".to_string(),
                type_oid: PG_TEXT_OID,
            },
            VirtualRelationColumnDef {
                name: "is_nullable".to_string(),
                type_oid: PG_TEXT_OID,
            },
        ],
        ("information_schema", "schemata") => vec![
            VirtualRelationColumnDef { name: "catalog_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "schema_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "schema_owner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("information_schema", "key_column_usage") => vec![
            VirtualRelationColumnDef { name: "constraint_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_schema".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "column_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "ordinal_position".to_string(), type_oid: PG_INT8_OID },
        ],
        ("information_schema", "table_constraints") => vec![
            VirtualRelationColumnDef { name: "constraint_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_schema".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "table_name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "constraint_type".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_database") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "datname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "datdba".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "encoding".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "datcollate".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_roles") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "rolname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "rolsuper".to_string(), type_oid: PG_BOOL_OID },
            VirtualRelationColumnDef { name: "rolcanlogin".to_string(), type_oid: PG_BOOL_OID },
        ],
        ("pg_catalog", "pg_settings") => vec![
            VirtualRelationColumnDef { name: "name".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "setting".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "category".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "short_desc".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_tables") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tablename".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tableowner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_views") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "viewname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "viewowner".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_indexes") => vec![
            VirtualRelationColumnDef { name: "schemaname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "tablename".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "indexname".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("pg_catalog", "pg_proc") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "proname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "pronamespace".to_string(), type_oid: PG_INT8_OID },
        ],
        ("pg_catalog", "pg_constraint") => vec![
            VirtualRelationColumnDef { name: "oid".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "conname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "connamespace".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "contype".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "conrelid".to_string(), type_oid: PG_INT8_OID },
        ],
        ("pg_catalog", "pg_extension") => vec![
            VirtualRelationColumnDef { name: "extname".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "extversion".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "extdescription".to_string(), type_oid: PG_TEXT_OID },
        ],
        ("ws", "connections") => vec![
            VirtualRelationColumnDef { name: "id".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "url".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "state".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "opened_at".to_string(), type_oid: PG_TEXT_OID },
            VirtualRelationColumnDef { name: "messages_in".to_string(), type_oid: PG_INT8_OID },
            VirtualRelationColumnDef { name: "messages_out".to_string(), type_oid: PG_INT8_OID },
        ],
        _ => return None,
    };
    Some(cols)
}

pub(crate) fn expand_from_columns(
    from: &[TableExpression],
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<ExpandedFromColumn>, EngineError> {
    if from.is_empty() {
        return Err(EngineError {
            message: "wildcard target requires FROM support".to_string(),
        });
    }

    let mut out = Vec::new();
    for table in from {
        out.extend(expand_table_expression_columns(table, ctes)?);
    }
    Ok(out)
}

fn expand_table_expression_columns(
    table: &TableExpression,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<ExpandedFromColumn>, EngineError> {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1 {
                let key = rel.name[0].to_ascii_lowercase();
                if let Some(columns) = ctes.get(&key) {
                    let qualifier = rel
                        .alias
                        .as_ref()
                        .map(|alias| alias.to_ascii_lowercase())
                        .unwrap_or(key);
                    return Ok(columns
                        .iter()
                        .map(|column| ExpandedFromColumn {
                            label: column.to_string(),
                            lookup_parts: vec![qualifier.clone(), column.to_string()],
                        })
                        .collect());
                }
            }
            if let Some((_, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
                let qualifier = rel
                    .alias
                    .as_ref()
                    .map(|alias| alias.to_ascii_lowercase())
                    .unwrap_or(relation_name.clone());
                return Ok(columns
                    .iter()
                    .map(|column| ExpandedFromColumn {
                        label: column.name.clone(),
                        lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                    })
                    .collect());
            }
            let table = with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&rel.name, &SearchPath::default())
                    .cloned()
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
            let qualifier = rel
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .unwrap_or_else(|| table.name().to_string());
            Ok(table
                .columns()
                .iter()
                .map(|column| ExpandedFromColumn {
                    label: column.name().to_string(),
                    lookup_parts: vec![qualifier.clone(), column.name().to_string()],
                })
                .collect())
        }
        TableExpression::Function(function) => {
            let column_names = if function.column_aliases.is_empty() {
                table_function_output_columns(function)
            } else {
                function.column_aliases.clone()
            };
            let qualifier = function
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .or_else(|| function.name.last().map(|name| name.to_ascii_lowercase()));
            Ok(column_names
                .into_iter()
                .map(|column_name| {
                    let lookup_parts = if let Some(qualifier) = &qualifier {
                        vec![qualifier.clone(), column_name.clone()]
                    } else {
                        vec![column_name.clone()]
                    };
                    ExpandedFromColumn {
                        label: column_name,
                        lookup_parts,
                    }
                })
                .collect())
        }
        TableExpression::Subquery(sub) => {
            let mut nested = ctes.clone();
            let cols = derive_query_columns_with_ctes(&sub.query, &mut nested)?;
            if let Some(alias) = &sub.alias {
                let qualifier = alias.to_ascii_lowercase();
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromColumn {
                        label: col.clone(),
                        lookup_parts: vec![qualifier.clone(), col],
                    })
                    .collect())
            } else {
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromColumn {
                        label: col.clone(),
                        lookup_parts: vec![col],
                    })
                    .collect())
            }
        }
        TableExpression::Join(join) => {
            let left_cols = expand_table_expression_columns(&join.left, ctes)?;
            let right_cols = expand_table_expression_columns(&join.right, ctes)?;
            let using_columns = if join.natural {
                left_cols
                    .iter()
                    .filter(|c| {
                        right_cols
                            .iter()
                            .any(|r| r.label.eq_ignore_ascii_case(&c.label))
                    })
                    .map(|c| c.label.clone())
                    .collect::<Vec<_>>()
            } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                cols.clone()
            } else {
                Vec::new()
            };
            let using_set: HashSet<String> = using_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect();

            let mut out = left_cols;
            for col in right_cols {
                if using_set.contains(&col.label.to_ascii_lowercase()) {
                    continue;
                }
                out.push(col);
            }
            Ok(out)
        }
    }
}

fn expand_from_columns_typed(
    from: &[TableExpression],
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<ExpandedFromTypeColumn>, EngineError> {
    if from.is_empty() {
        return Err(EngineError {
            message: "wildcard target requires FROM support".to_string(),
        });
    }

    let mut out = Vec::new();
    for table in from {
        out.extend(expand_table_expression_columns_typed(table, ctes)?);
    }
    Ok(out)
}

fn expand_table_expression_columns_typed(
    table: &TableExpression,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<ExpandedFromTypeColumn>, EngineError> {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1 {
                let key = rel.name[0].to_ascii_lowercase();
                if let Some(columns) = ctes.get(&key) {
                    let qualifier = rel
                        .alias
                        .as_ref()
                        .map(|alias| alias.to_ascii_lowercase())
                        .unwrap_or(key);
                    return Ok(columns
                        .iter()
                        .map(|column| ExpandedFromTypeColumn {
                            label: column.name.to_string(),
                            lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                            type_oid: column.type_oid,
                        })
                        .collect());
                }
            }
            if let Some((_, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
                let qualifier = rel
                    .alias
                    .as_ref()
                    .map(|alias| alias.to_ascii_lowercase())
                    .unwrap_or(relation_name.clone());
                return Ok(columns
                    .iter()
                    .map(|column| ExpandedFromTypeColumn {
                        label: column.name.clone(),
                        lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                        type_oid: column.type_oid,
                    })
                    .collect());
            }
            let table = with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&rel.name, &SearchPath::default())
                    .cloned()
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
            let qualifier = rel
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .unwrap_or_else(|| table.name().to_string());
            Ok(table
                .columns()
                .iter()
                .map(|column| ExpandedFromTypeColumn {
                    label: column.name().to_string(),
                    lookup_parts: vec![qualifier.clone(), column.name().to_string()],
                    type_oid: type_signature_to_oid(column.type_signature()),
                })
                .collect())
        }
        TableExpression::Function(function) => {
            let column_names = if function.column_aliases.is_empty() {
                table_function_output_columns(function)
            } else {
                function.column_aliases.clone()
            };
            let mut column_type_oids =
                table_function_output_type_oids(function, column_names.len());
            if column_type_oids.len() < column_names.len() {
                column_type_oids.resize(column_names.len(), PG_TEXT_OID);
            }
            let qualifier = function
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .or_else(|| function.name.last().map(|name| name.to_ascii_lowercase()));
            Ok(column_names
                .into_iter()
                .enumerate()
                .map(|(idx, column_name)| {
                    let lookup_parts = if let Some(qualifier) = &qualifier {
                        vec![qualifier.clone(), column_name.clone()]
                    } else {
                        vec![column_name.clone()]
                    };
                    ExpandedFromTypeColumn {
                        label: column_name,
                        lookup_parts,
                        type_oid: *column_type_oids.get(idx).unwrap_or(&PG_TEXT_OID),
                    }
                })
                .collect())
        }
        TableExpression::Subquery(sub) => {
            let mut nested = ctes.clone();
            let cols = derive_query_output_columns_with_ctes(&sub.query, &mut nested)?;
            if let Some(alias) = &sub.alias {
                let qualifier = alias.to_ascii_lowercase();
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![qualifier.clone(), col.name],
                        type_oid: col.type_oid,
                    })
                    .collect())
            } else {
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![col.name],
                        type_oid: col.type_oid,
                    })
                    .collect())
            }
        }
        TableExpression::Join(join) => {
            let left_cols = expand_table_expression_columns_typed(&join.left, ctes)?;
            let right_cols = expand_table_expression_columns_typed(&join.right, ctes)?;
            let using_columns = if join.natural {
                left_cols
                    .iter()
                    .filter(|c| {
                        right_cols
                            .iter()
                            .any(|r| r.label.eq_ignore_ascii_case(&c.label))
                    })
                    .map(|c| c.label.clone())
                    .collect::<Vec<_>>()
            } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                cols.clone()
            } else {
                Vec::new()
            };
            let using_set: HashSet<String> = using_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect();

            let mut out = left_cols;
            for col in right_cols {
                if using_set.contains(&col.label.to_ascii_lowercase()) {
                    continue;
                }
                out.push(col);
            }
            Ok(out)
        }
    }
}

fn table_function_output_columns(function: &TableFunctionRef) -> Vec<String> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();
    match fn_name.as_str() {
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" => {
            vec!["key".to_string(), "value".to_string()]
        }
        "json_object_keys" | "jsonb_object_keys" => vec!["key".to_string()],
        "generate_series" => vec!["generate_series".to_string()],
        "unnest" => vec!["unnest".to_string()],
        "regexp_matches" => vec!["regexp_matches".to_string()],
        "regexp_split_to_table" => vec!["regexp_split_to_table".to_string()],
        "pg_get_keywords" => vec!["word".to_string(), "catcode".to_string(), "catdesc".to_string()],
        _ => vec!["value".to_string()],
    }
}

fn table_function_output_type_oids(function: &TableFunctionRef, count: usize) -> Vec<u32> {
    if !function.column_alias_types.is_empty() {
        return function
            .column_alias_types
            .iter()
            .take(count)
            .map(|entry| {
                entry
                    .as_deref()
                    .map(cast_type_name_to_oid)
                    .unwrap_or(PG_TEXT_OID)
            })
            .collect();
    }

    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();
    let mut oids = match fn_name.as_str() {
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" => {
            vec![PG_TEXT_OID, PG_TEXT_OID]
        }
        "json_object_keys" | "jsonb_object_keys" => vec![PG_TEXT_OID],
        _ => vec![PG_TEXT_OID],
    };
    if oids.len() < count {
        oids.resize(count, PG_TEXT_OID);
    } else if oids.len() > count {
        oids.truncate(count);
    }
    oids
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
    use crate::parser::sql_parser::parse_statement;
    use crate::executor::exec_expr::ws_simulate_message;
    use serde_json::{Number as JsonNumber, Value as JsonValue};
    use std::future::Future;

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread().enable_all().build()
            .expect("tokio runtime should start")
            .block_on(future)
    }

    fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
        with_global_state_lock(|| {
            reset_global_catalog_for_tests();
            reset_global_storage_for_tests();
            f()
        })
    }

    fn run_statement(sql: &str, params: &[Option<String>]) -> QueryResult {
        let statement = parse_statement(sql).expect("statement should parse");
        let planned = plan_statement(statement).expect("statement should plan");
        block_on(execute_planned_query(&planned, params)).expect("query should execute")
    }

    fn run(sql: &str) -> QueryResult {
        with_isolated_state(|| run_statement(sql, &[]))
    }

    fn run_batch(statements: &[&str]) -> Vec<QueryResult> {
        with_isolated_state(|| {
            statements
                .iter()
                .map(|statement| run_statement(statement, &[]))
                .collect()
        })
    }

    #[test]
    fn executes_scalar_select() {
        let result = run("SELECT 1 + 2 * 3 AS n");
        assert_eq!(result.columns, vec!["n".to_string()]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0], vec![ScalarValue::Int(7)]);
    }

    #[test]
    fn executes_where_filter() {
        let result = run("SELECT 1 WHERE false");
        assert!(result.rows.is_empty());
    }

    #[test]
    fn executes_set_operations_and_order_limit() {
        let result = run("SELECT 3 UNION SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 2");
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn executes_parameterized_expression() {
        let result =
            with_isolated_state(|| run_statement("SELECT $1 + 5", &[Some("7".to_string())]));
        assert_eq!(result.rows[0], vec![ScalarValue::Int(12)]);
    }

    #[test]
    fn exposes_pg_catalog_virtual_relations_for_introspection() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8)",
            "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'users'",
            "SELECT nspname FROM pg_namespace WHERE nspname = 'public'",
        ]);
        assert_eq!(
            results[1].rows,
            vec![vec![ScalarValue::Text("users".to_string())]]
        );
        assert_eq!(
            results[2].rows,
            vec![vec![ScalarValue::Text("public".to_string())]]
        );
    }

    #[test]
    fn exposes_information_schema_tables_and_columns() {
        let results = run_batch(&[
            "CREATE TABLE events (event_day date, created_at timestamp)",
            "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_name = 'events'",
            "SELECT ordinal_position, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'events' ORDER BY 1",
        ]);
        assert_eq!(
            results[1].rows,
            vec![vec![
                ScalarValue::Text("public".to_string()),
                ScalarValue::Text("events".to_string()),
                ScalarValue::Text("BASE TABLE".to_string())
            ]]
        );
        assert_eq!(
            results[2].rows,
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("event_day".to_string()),
                    ScalarValue::Text("date".to_string()),
                    ScalarValue::Text("YES".to_string())
                ],
                vec![
                    ScalarValue::Int(2),
                    ScalarValue::Text("created_at".to_string()),
                    ScalarValue::Text("timestamp without time zone".to_string()),
                    ScalarValue::Text("YES".to_string())
                ]
            ]
        );
    }

    #[test]
    fn supports_date_and_timestamp_column_types() {
        let results = run_batch(&[
            "CREATE TABLE events (event_day date, created_at timestamp)",
            "INSERT INTO events VALUES ('2024-01-02', '2024-01-02 03:04:05')",
            "SELECT event_day, created_at FROM events",
        ]);
        assert_eq!(
            results[2].rows,
            vec![vec![
                ScalarValue::Text("2024-01-02".to_string()),
                ScalarValue::Text("2024-01-02 03:04:05".to_string())
            ]]
        );
    }

    #[test]
    fn evaluates_null_comparison_and_three_valued_logic() {
        let result = run(
            "SELECT NULL = NULL, 1 = NULL, NULL <> 1, true OR NULL, false OR NULL, true AND NULL, false AND NULL",
        );
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Bool(false),
            ]]
        );
    }

    #[test]
    fn evaluates_is_null_predicates() {
        let result = run("SELECT NULL IS NULL, 1 IS NULL, NULL IS NOT NULL, 1 IS NOT NULL");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Bool(true),
                ScalarValue::Bool(false),
                ScalarValue::Bool(false),
                ScalarValue::Bool(true),
            ]]
        );
    }

    #[test]
    fn evaluates_is_distinct_from_predicates() {
        let result = run("SELECT \
                1 IS DISTINCT FROM 1, \
                1 IS DISTINCT FROM 2, \
                NULL IS DISTINCT FROM NULL, \
                NULL IS DISTINCT FROM 1, \
                NULL IS NOT DISTINCT FROM NULL, \
                1 IS NOT DISTINCT FROM 1, \
                1 IS NOT DISTINCT FROM '1'");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Bool(false),
                ScalarValue::Bool(true),
                ScalarValue::Bool(false),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
            ]]
        );
    }

    #[test]
    fn evaluates_between_and_like_predicates() {
        let result = run("SELECT \
                5 BETWEEN 1 AND 10, \
                5 NOT BETWEEN 1 AND 4, \
                NULL BETWEEN 1 AND 2, \
                'abc' LIKE 'a%', \
                'ABC' ILIKE 'a%', \
                'abc' NOT LIKE 'a%', \
                'a_c' LIKE 'a\\_c', \
                'axc' LIKE 'a_c'");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(false),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
            ]]
        );
    }

    #[test]
    fn evaluates_case_expressions() {
        let result = run("SELECT \
                CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END, \
                CASE WHEN 1 = 0 THEN 'no' WHEN 3 > 1 THEN 'yes' END, \
                CASE NULL WHEN NULL THEN 1 ELSE 2 END, \
                CASE WHEN NULL THEN 1 ELSE 2 END, \
                CASE WHEN true THEN CASE 1 WHEN 1 THEN 'nested' ELSE 'x' END ELSE 'y' END");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Text("two".to_string()),
                ScalarValue::Text("yes".to_string()),
                ScalarValue::Int(2),
                ScalarValue::Int(2),
                ScalarValue::Text("nested".to_string()),
            ]]
        );
    }

    #[test]
    fn supports_case_in_where_order_by_and_group_by() {
        let results = run_batch(&[
            "CREATE TABLE scores (id int8, score int8, active boolean)",
            "INSERT INTO scores VALUES (1, 95, true), (2, 75, true), (3, NULL, false), (4, 82, true)",
            "SELECT id FROM scores WHERE CASE WHEN active THEN score >= 80 ELSE false END ORDER BY CASE id WHEN 1 THEN 0 ELSE 1 END, id",
            "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade, count(*) FROM scores GROUP BY CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END ORDER BY grade",
        ]);

        assert_eq!(
            results[2].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(4)],]
        );
        assert_eq!(
            results[3].rows,
            vec![
                vec![ScalarValue::Text("A".to_string()), ScalarValue::Int(1)],
                vec![ScalarValue::Text("B".to_string()), ScalarValue::Int(1)],
                vec![ScalarValue::Text("C".to_string()), ScalarValue::Int(2)],
            ]
        );
    }

    #[test]
    fn rejects_non_boolean_operands_for_logical_operators() {
        with_isolated_state(|| {
            let statement = parse_statement("SELECT 1 AND true").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("logical type mismatch expected");
            assert!(
                err.message
                    .contains("argument of AND must be type boolean or null")
            );
        });
    }

    #[test]
    fn handles_arithmetic_nulls_and_division_by_zero() {
        let result = run("SELECT 1 + NULL, 10 / NULL, 10 % NULL");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null
            ]]
        );

        with_isolated_state(|| {
            let statement = parse_statement("SELECT 10 / 0").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("division by zero should error");
            assert!(err.message.contains("division by zero"));
        });

        with_isolated_state(|| {
            let statement = parse_statement("SELECT 10 % 0").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("modulo by zero should error");
            assert!(err.message.contains("division by zero"));
        });
    }

    #[test]
    fn supports_mixed_type_numeric_and_comparison_coercion() {
        let result = run("SELECT 1 + '2', '3' * 4, '10' / '2', '10' % '3', 1 = '1', '2.5' > 2");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int(3),
                ScalarValue::Int(12),
                ScalarValue::Int(5),
                ScalarValue::Int(1),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
            ]]
        );

        with_isolated_state(|| {
            let statement = parse_statement("SELECT 'x' + 1").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("non-numeric coercion should fail");
            assert!(
                err.message
                    .contains("numeric operation expects numeric values")
            );
        });
    }

    #[test]
    fn evaluates_date_time_builtins() {
        let result = run("SELECT \
                date('2024-02-29 10:30:40'), \
                timestamp('2024-02-29'), \
                extract('year', timestamp('2024-02-29 10:30:40')), \
                date_part('dow', date('2024-03-03')), \
                date_trunc('hour', timestamp('2024-02-29 10:30:40')), \
                date_add(date('2024-02-28'), 2), \
                date_sub(date('2024-03-01'), 1)");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Text("2024-02-29".to_string()),
                ScalarValue::Text("2024-02-29 00:00:00".to_string()),
                ScalarValue::Int(2024),
                ScalarValue::Int(0),
                ScalarValue::Text("2024-02-29 10:00:00".to_string()),
                ScalarValue::Text("2024-03-01".to_string()),
                ScalarValue::Text("2024-02-29".to_string()),
            ]]
        );
    }

    #[test]
    fn evaluates_cast_and_typecast_expressions() {
        let result = run("SELECT \
                CAST('1' AS int8), \
                '2'::int8 + 3, \
                CAST('true' AS boolean), \
                CAST('2024-02-29' AS date), \
                CAST('2024-02-29' AS timestamp), \
                CAST('42' AS double precision)");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Int(5),
                ScalarValue::Bool(true),
                ScalarValue::Text("2024-02-29".to_string()),
                ScalarValue::Text("2024-02-29 00:00:00".to_string()),
                ScalarValue::Float(42.0),
            ]]
        );
    }

    #[test]
    fn evaluates_date_time_operator_arithmetic() {
        let result = run("SELECT \
                date('2024-02-28') + 2, \
                date('2024-03-01') - 1, \
                date('2024-03-01') - date('2024-02-28'), \
                timestamp('2024-03-01 10:30:40') + 1, \
                timestamp('2024-03-01 10:30:40') - timestamp('2024-03-01 10:30:00')");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Text("2024-03-01".to_string()),
                ScalarValue::Text("2024-02-29".to_string()),
                ScalarValue::Int(2),
                ScalarValue::Text("2024-03-02 10:30:40".to_string()),
                ScalarValue::Int(40),
            ]]
        );
    }

    #[test]
    fn evaluates_current_date_and_timestamp_builtins() {
        let result = run("SELECT current_date(), current_timestamp(), now()");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].len(), 3);
        for value in &result.rows[0] {
            let ScalarValue::Text(text) = value else {
                panic!("expected text timestamp/date value");
            };
            assert!(!text.is_empty());
        }
    }

    #[test]
    fn evaluates_extended_scalar_functions() {
        let result = run("SELECT \
                nullif(1, 1), \
                nullif(1, 2), \
                greatest(1, NULL, 5, 3), \
                least(8, NULL, 2, 4), \
                concat('a', NULL, 'b', 1), \
                concat_ws('-', 'a', NULL, 'b', 1), \
                substring('abcdef', 2, 3), \
                substr('abcdef', 4), \
                left('abcdef', -2), \
                right('abcdef', -2), \
                btrim('***abc***', '*'), \
                ltrim('***abc***', '*'), \
                rtrim('***abc***', '*'), \
                replace('abcabc', 'ab', 'x'), \
                lower(NULL), \
                upper(NULL), \
                length(NULL), \
                abs(NULL)");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Null,
                ScalarValue::Int(1),
                ScalarValue::Int(5),
                ScalarValue::Int(2),
                ScalarValue::Text("ab1".to_string()),
                ScalarValue::Text("a-b-1".to_string()),
                ScalarValue::Text("bcd".to_string()),
                ScalarValue::Text("def".to_string()),
                ScalarValue::Text("abcd".to_string()),
                ScalarValue::Text("cdef".to_string()),
                ScalarValue::Text("abc".to_string()),
                ScalarValue::Text("abc***".to_string()),
                ScalarValue::Text("***abc".to_string()),
                ScalarValue::Text("xcxc".to_string()),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
            ]]
        );
    }

    #[test]
    fn evaluates_additional_string_functions() {
        let result = run("SELECT \
                position('bar' IN 'foobar'), \
                overlay('abcdef' PLACING 'zz' FROM 3 FOR 2), \
                ascii('A'), \
                chr(65), \
                encode('foo', 'hex'), \
                decode('666f6f', 'hex'), \
                encode('foo', 'base64'), \
                decode('Zm9v', 'base64'), \
                md5('abc'), \
                quote_literal('O''Reilly'), \
                quote_ident('some\"ident'), \
                quote_nullable(NULL), \
                quote_nullable('hi'), \
                regexp_match('abc123', '([a-z]+)([0-9]+)'), \
                regexp_split_to_array('a-b-c', '-')");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int(4),
                ScalarValue::Text("abzzef".to_string()),
                ScalarValue::Int(65),
                ScalarValue::Text("A".to_string()),
                ScalarValue::Text("666f6f".to_string()),
                ScalarValue::Text("foo".to_string()),
                ScalarValue::Text("Zm9v".to_string()),
                ScalarValue::Text("foo".to_string()),
                ScalarValue::Text("900150983cd24fb0d6963f7d28e17f72".to_string()),
                ScalarValue::Text("'O''Reilly'".to_string()),
                ScalarValue::Text("\"some\"\"ident\"".to_string()),
                ScalarValue::Text("NULL".to_string()),
                ScalarValue::Text("'hi'".to_string()),
                ScalarValue::Text("{abc,123}".to_string()),
                ScalarValue::Text("{a,b,c}".to_string()),
            ]]
        );
    }

    #[test]
    fn evaluates_regexp_set_returning_functions() {
        let result = run("SELECT * FROM regexp_matches('abc123 abc456', '([a-z]+)([0-9]+)', 'g')");
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("{abc,123}".to_string())],
                vec![ScalarValue::Text("{abc,456}".to_string())],
            ]
        );

        let result = run("SELECT * FROM regexp_split_to_table('a,b,c', ',')");
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Text("b".to_string())],
                vec![ScalarValue::Text("c".to_string())],
            ]
        );
    }

    #[test]
    fn evaluates_additional_date_time_functions() {
        let result = run("SELECT \
                age(timestamp('2024-03-02 00:00:00'), timestamp('2024-03-01 00:00:00')), \
                to_timestamp('2024-03-01 12:34:56', 'YYYY-MM-DD HH24:MI:SS'), \
                to_timestamp(0), \
                to_date('2024-03-01', 'YYYY-MM-DD'), \
                make_interval(1, 2, 0, 3, 4, 5, 6), \
                justify_hours(make_interval(0, 0, 0, 0, 25, 0, 0)), \
                justify_days(make_interval(0, 0, 0, 45, 0, 0, 0)), \
                justify_interval(make_interval(0, 0, 0, 35, 25, 0, 0)), \
                isfinite('infinity'), \
                isfinite(date('2024-03-01'))");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Text("0 mons 1 days 00:00:00".to_string()),
                ScalarValue::Text("2024-03-01 12:34:56".to_string()),
                ScalarValue::Text("1970-01-01 00:00:00".to_string()),
                ScalarValue::Text("2024-03-01".to_string()),
                ScalarValue::Text("14 mons 3 days 04:05:06".to_string()),
                ScalarValue::Text("0 mons 1 days 01:00:00".to_string()),
                ScalarValue::Text("1 mons 15 days 00:00:00".to_string()),
                ScalarValue::Text("1 mons 6 days 01:00:00".to_string()),
                ScalarValue::Bool(false),
                ScalarValue::Bool(true),
            ]]
        );

        let result = run("SELECT age(timestamp('2024-03-01 00:00:00')), clock_timestamp()");
        assert_eq!(result.rows.len(), 1);
        for value in &result.rows[0] {
            let ScalarValue::Text(text) = value else {
                panic!("expected text interval/timestamp value");
            };
            assert!(!text.is_empty());
        }
    }

    #[test]
    fn evaluates_math_and_conditional_functions() {
        let result = run("SELECT \
                trunc(1.2345, 2), \
                width_bucket(5, 0, 10, 5), \
                scale('123.4500'), \
                factorial(5), \
                num_nulls(1, NULL, 'x', NULL), \
                num_nonnulls(1, NULL, 'x', NULL)");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Float(1.23),
                ScalarValue::Int(3),
                ScalarValue::Int(4),
                ScalarValue::Int(120),
                ScalarValue::Int(2),
                ScalarValue::Int(2),
            ]]
        );
    }

    #[test]
    fn evaluates_json_jsonb_scalar_functions() {
        let result = run("SELECT \
                to_json(1), \
                to_json('x'), \
                json_build_object('a', 1, 'b', true, 'c', NULL), \
                json_build_array(1, 'x', NULL), \
                json_array_length('[1,2,3]'), \
                jsonb_array_length('[]'), \
                jsonb_exists('{\"a\":1,\"b\":2}', 'b'), \
                jsonb_exists_any('{\"a\":1,\"b\":2}', '{x,b}'), \
                jsonb_exists_all('{\"a\":1,\"b\":2}', '{a,b}'), \
                json_typeof('{\"a\":1}'), \
                json_extract_path('{\"a\":{\"b\":[10,true,null]}}', 'a', 'b', 1), \
                json_extract_path_text('{\"a\":{\"b\":[10,true,null]}}', 'a', 'b', 0), \
                json_strip_nulls('{\"a\":1,\"b\":null,\"c\":{\"d\":null,\"e\":2}}'), \
                row_to_json(row(1, 'x')), \
                array_to_json(json_build_array(1, 'x')), \
                json_object('[[\"a\",\"1\"],[\"b\",\"2\"]]'), \
                json_object('[\"a\",\"1\",\"b\",\"2\"]'), \
                json_object('[\"k1\",\"k2\"]', '[\"v1\",\"v2\"]')");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], ScalarValue::Text("1".to_string()));
        assert_eq!(result.rows[0][1], ScalarValue::Text("\"x\"".to_string()));
        assert_eq!(result.rows[0][4], ScalarValue::Int(3));
        assert_eq!(result.rows[0][5], ScalarValue::Int(0));
        assert_eq!(result.rows[0][6], ScalarValue::Bool(true));
        assert_eq!(result.rows[0][7], ScalarValue::Bool(true));
        assert_eq!(result.rows[0][8], ScalarValue::Bool(true));
        assert_eq!(result.rows[0][9], ScalarValue::Text("object".to_string()));
        assert_eq!(result.rows[0][10], ScalarValue::Text("true".to_string()));
        assert_eq!(result.rows[0][11], ScalarValue::Text("10".to_string()));
        assert_eq!(
            result.rows[0][13],
            ScalarValue::Text("{\"f1\":1,\"f2\":\"x\"}".to_string())
        );
        assert_eq!(
            result.rows[0][14],
            ScalarValue::Text("[1,\"x\"]".to_string())
        );
        assert_eq!(
            result.rows[0][15],
            ScalarValue::Text("{\"a\":\"1\",\"b\":\"2\"}".to_string())
        );
        assert_eq!(
            result.rows[0][16],
            ScalarValue::Text("{\"a\":\"1\",\"b\":\"2\"}".to_string())
        );
        assert_eq!(
            result.rows[0][17],
            ScalarValue::Text("{\"k1\":\"v1\",\"k2\":\"v2\"}".to_string())
        );

        let ScalarValue::Text(obj_text) = &result.rows[0][2] else {
            panic!("json_build_object should return text");
        };
        let obj: JsonValue = serde_json::from_str(obj_text).expect("object text should parse");
        assert_eq!(obj.get("a"), Some(&JsonValue::Number(JsonNumber::from(1))));
        assert_eq!(obj.get("b"), Some(&JsonValue::Bool(true)));
        assert_eq!(obj.get("c"), Some(&JsonValue::Null));

        let ScalarValue::Text(arr_text) = &result.rows[0][3] else {
            panic!("json_build_array should return text");
        };
        let arr: JsonValue = serde_json::from_str(arr_text).expect("array text should parse");
        assert_eq!(
            arr,
            JsonValue::Array(vec![
                JsonValue::Number(JsonNumber::from(1)),
                JsonValue::String("x".to_string()),
                JsonValue::Null,
            ])
        );

        let ScalarValue::Text(stripped_text) = &result.rows[0][12] else {
            panic!("json_strip_nulls should return text");
        };
        let stripped: JsonValue =
            serde_json::from_str(stripped_text).expect("stripped json should parse");
        assert_eq!(
            stripped.get("a"),
            Some(&JsonValue::Number(JsonNumber::from(1)))
        );
        assert!(stripped.get("b").is_none());
        assert_eq!(
            stripped.pointer("/c/e"),
            Some(&JsonValue::Number(JsonNumber::from(2)))
        );
        assert_eq!(stripped.pointer("/c/d"), None);
    }

    #[test]
    fn evaluates_jsonb_set() {
        let result = run("SELECT \
                jsonb_set('{\"a\":{\"b\":1}}', '{a,b}', '2'), \
                jsonb_set('{\"a\":[1,2]}', '{a,1}', '9'), \
                jsonb_set('{\"a\":{}}', '{a,c}', to_jsonb('x'), true), \
                jsonb_set('{\"a\":{}}', '{a,c}', to_jsonb('x'), false)");
        assert_eq!(result.rows.len(), 1);

        let ScalarValue::Text(set1_text) = &result.rows[0][0] else {
            panic!("jsonb_set output 1 should be text");
        };
        let set1: JsonValue = serde_json::from_str(set1_text).expect("json should parse");
        assert_eq!(
            set1.pointer("/a/b"),
            Some(&JsonValue::Number(JsonNumber::from(2)))
        );

        let ScalarValue::Text(set2_text) = &result.rows[0][1] else {
            panic!("jsonb_set output 2 should be text");
        };
        let set2: JsonValue = serde_json::from_str(set2_text).expect("json should parse");
        assert_eq!(
            set2.pointer("/a/1"),
            Some(&JsonValue::Number(JsonNumber::from(9)))
        );

        let ScalarValue::Text(set3_text) = &result.rows[0][2] else {
            panic!("jsonb_set output 3 should be text");
        };
        let set3: JsonValue = serde_json::from_str(set3_text).expect("json should parse");
        assert_eq!(
            set3.pointer("/a/c"),
            Some(&JsonValue::String("x".to_string()))
        );

        let ScalarValue::Text(set4_text) = &result.rows[0][3] else {
            panic!("jsonb_set output 4 should be text");
        };
        let set4: JsonValue = serde_json::from_str(set4_text).expect("json should parse");
        assert_eq!(set4.pointer("/a/c"), None);
    }

    #[test]
    fn evaluates_jsonb_insert_and_set_lax() {
        let result = run("SELECT \
                jsonb_insert('{\"a\":[1,2]}', '{a,1}', '9'), \
                jsonb_insert('{\"a\":[1,2]}', '{a,1}', '9', true), \
                jsonb_insert('{\"a\":{\"b\":1}}', '{a,c}', '2'), \
                jsonb_set_lax('{\"a\":{\"b\":1}}', '{a,b}', NULL, true, 'use_json_null'), \
                jsonb_set_lax('{\"a\":{\"b\":1}}', '{a,b}', NULL, true, 'delete_key'), \
                jsonb_set_lax('{\"a\":{\"b\":1}}', '{a,b}', NULL, true, 'return_target')");
        assert_eq!(result.rows.len(), 1);

        let ScalarValue::Text(insert1_text) = &result.rows[0][0] else {
            panic!("jsonb_insert output 1 should be text");
        };
        let insert1: JsonValue = serde_json::from_str(insert1_text).expect("json should parse");
        assert_eq!(insert1, serde_json::json!({"a":[1,9,2]}));

        let ScalarValue::Text(insert2_text) = &result.rows[0][1] else {
            panic!("jsonb_insert output 2 should be text");
        };
        let insert2: JsonValue = serde_json::from_str(insert2_text).expect("json should parse");
        assert_eq!(insert2, serde_json::json!({"a":[1,2,9]}));

        let ScalarValue::Text(insert3_text) = &result.rows[0][2] else {
            panic!("jsonb_insert output 3 should be text");
        };
        let insert3: JsonValue = serde_json::from_str(insert3_text).expect("json should parse");
        assert_eq!(insert3, serde_json::json!({"a":{"b":1,"c":2}}));

        let ScalarValue::Text(set_lax_1_text) = &result.rows[0][3] else {
            panic!("jsonb_set_lax output 1 should be text");
        };
        let set_lax_1: JsonValue = serde_json::from_str(set_lax_1_text).expect("json should parse");
        assert_eq!(set_lax_1, serde_json::json!({"a":{"b":null}}));

        let ScalarValue::Text(set_lax_2_text) = &result.rows[0][4] else {
            panic!("jsonb_set_lax output 2 should be text");
        };
        let set_lax_2: JsonValue = serde_json::from_str(set_lax_2_text).expect("json should parse");
        assert_eq!(set_lax_2, serde_json::json!({"a":{}}));

        let ScalarValue::Text(set_lax_3_text) = &result.rows[0][5] else {
            panic!("jsonb_set_lax output 3 should be text");
        };
        let set_lax_3: JsonValue = serde_json::from_str(set_lax_3_text).expect("json should parse");
        assert_eq!(set_lax_3, serde_json::json!({"a":{"b":1}}));
    }

    #[test]
    fn evaluates_json_binary_operators() {
        let result = run("SELECT \
                '{\"a\":{\"b\":[10,true,null]},\"arr\":[\"k1\",\"k2\"]}' -> 'a', \
                '{\"a\":{\"b\":[10,true,null]},\"arr\":[\"k1\",\"k2\"]}' ->> 'arr', \
                '{\"a\":{\"b\":[10,true,null]}}' #> '{a,b,1}', \
                '{\"a\":{\"b\":[10,true,null]}}' #>> '{a,b,0}', \
                '{\"a\":1}' || '{\"b\":2}', \
                '[1,2]' || '[3,4]', \
                '[1,2]' || '3', \
                '{\"a\":1,\"b\":2}' @> '{\"a\":1}', \
                '{\"a\":1}' <@ '{\"a\":1,\"b\":2}', \
                '{\"a\":[{\"id\":1}],\"flag\":true}' @? '$.a[*].id', \
                '{\"a\":[{\"id\":1}],\"flag\":true}' @@ '$.flag', \
                '{\"a\":1,\"b\":2}' ? 'b', \
                '{\"a\":1,\"b\":2}' ?| '{x,b}', \
                '{\"a\":1,\"b\":2}' ?| array['a','c'], \
                '{\"a\":1,\"b\":2}' ?& '{a,b}', \
                '{\"a\":1,\"b\":2}' ?& array['a','b'], \
                '[\"a\",\"b\"]' ? 'a', \
                '{\"a\":1,\"b\":2}' - 'a', \
                '[\"x\",\"y\",\"z\"]' - 1, \
                '{\"a\":{\"b\":1,\"c\":2}}' #- '{a,b}'");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                ScalarValue::Text("{\"b\":[10,true,null]}".to_string()),
                ScalarValue::Text("[\"k1\",\"k2\"]".to_string()),
                ScalarValue::Text("true".to_string()),
                ScalarValue::Text("10".to_string()),
                ScalarValue::Text("{\"a\":1,\"b\":2}".to_string()),
                ScalarValue::Text("[1,2,3,4]".to_string()),
                ScalarValue::Text("[1,2,3]".to_string()),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
                ScalarValue::Text("{\"b\":2}".to_string()),
                ScalarValue::Text("[\"x\",\"z\"]".to_string()),
                ScalarValue::Text("{\"a\":{\"c\":2}}".to_string()),
            ]
        );
    }

    #[test]
    fn json_operators_null_and_missing_behave_like_sql_null() {
        let result = run("SELECT \
                '{\"a\":1}' -> 'missing', \
                '{\"a\":1}' ->> 'missing', \
                '{\"a\":1}' #> '{missing}', \
                '{\"a\":1}' #>> '{missing}', \
                '{\"a\":1}' || NULL::text, \
                NULL::text <@ '{\"a\":1}', \
                '{\"a\":1}' @? NULL::text, \
                NULL::text @@ '$.a', \
                NULL::text -> 'a', \
                '{\"a\":1}' @> NULL::text, \
                '{\"a\":1}' ? NULL::text, \
                '{\"a\":1}' #- NULL::text, \
                '{\"a\":1}' - NULL::text");
        assert_eq!(
            result.rows[0],
            vec![
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
            ]
        );
    }

    #[test]
    fn evaluates_jsonb_path_functions() {
        let result = run("SELECT \
                jsonb_path_exists('{\"a\":[{\"id\":1},{\"id\":2}],\"flag\":true}', '$.a[*].id'), \
                jsonb_path_query_first('{\"a\":[{\"id\":1},{\"id\":2}],\"flag\":true}', '$.a[1].id'), \
                jsonb_path_query_array('{\"a\":[{\"id\":1},{\"id\":2}],\"flag\":true}', '$.a[*].id'), \
                jsonb_path_exists('{\"a\":[1,2,3]}', '$.a[*] ? (@ >= $min)', '{\"min\":2}'), \
                jsonb_path_query_array('{\"a\":[1,2,3]}', '$.a[*] ? (@ >= $min)', '{\"min\":2}'), \
                jsonb_path_match('{\"flag\":true}', '$.flag'), \
                jsonb_path_exists('{\"a\":[1]}', '$.a[', NULL, true)");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Bool(true),
                ScalarValue::Text("2".to_string()),
                ScalarValue::Text("[1,2]".to_string()),
                ScalarValue::Bool(true),
                ScalarValue::Text("[2,3]".to_string()),
                ScalarValue::Bool(true),
                ScalarValue::Bool(false),
            ]]
        );
    }

    #[test]
    fn expands_jsonb_path_query_in_from_clause() {
        let result = run(
            "SELECT * FROM jsonb_path_query('{\"a\":[{\"id\":2},{\"id\":1}]}', '$.a[*].id') ORDER BY 1",
        );
        assert_eq!(result.columns, vec!["value".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("1".to_string())],
                vec![ScalarValue::Text("2".to_string())],
            ]
        );
    }

    #[test]
    fn expands_json_record_functions_in_from_clause() {
        let results = run_batch(&[
            "SELECT a, b FROM json_to_record('{\"a\":1,\"b\":\"x\"}') AS r(a int8, b text)",
            "SELECT a, b FROM json_to_recordset('[{\"a\":2,\"b\":\"y\"},{\"a\":3}]') AS r(a int8, b text) ORDER BY 1",
            "SELECT a, b FROM json_populate_record('{\"a\":0,\"b\":\"base\"}', '{\"a\":5}') AS r(a int8, b text)",
            "SELECT a, b FROM json_populate_recordset('{\"b\":\"base\"}', '[{\"a\":7},{\"a\":8,\"b\":\"z\"}]') AS r(a int8, b text) ORDER BY 1",
        ]);

        assert_eq!(
            results[0].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("x".to_string())
            ]]
        );
        assert_eq!(
            results[1].rows,
            vec![
                vec![ScalarValue::Int(2), ScalarValue::Text("y".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Null],
            ]
        );
        assert_eq!(
            results[2].rows,
            vec![vec![
                ScalarValue::Int(5),
                ScalarValue::Text("base".to_string())
            ]]
        );
        assert_eq!(
            results[3].rows,
            vec![
                vec![ScalarValue::Int(7), ScalarValue::Text("base".to_string())],
                vec![ScalarValue::Int(8), ScalarValue::Text("z".to_string())],
            ]
        );
    }

    #[test]
    fn json_functions_validate_json_input() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT json_extract_path('not-json', 'a')").expect("parses");
            let planned = plan_statement(statement).expect("plans");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("invalid json should fail");
            assert!(err.message.contains("not valid JSON"));
        });
    }

    #[test]
    fn expands_json_array_elements_in_from_clause() {
        let result = run(
            "SELECT json_extract_path_text(elem, 'currency') AS currency \
             FROM json_array_elements(json_extract_path('{\"result\":[{\"currency\":\"XRP\"},{\"currency\":\"USDC\"}]}', 'result')) AS src(elem) \
             ORDER BY currency",
        );
        assert_eq!(result.columns, vec!["currency".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("USDC".to_string())],
                vec![ScalarValue::Text("XRP".to_string())],
            ]
        );
    }

    #[test]
    fn expands_json_array_elements_text_in_from_clause() {
        let result = run("SELECT * FROM json_array_elements_text('[1,\"x\",null]')");
        assert_eq!(result.columns, vec!["value".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("1".to_string())],
                vec![ScalarValue::Text("x".to_string())],
                vec![ScalarValue::Null],
            ]
        );
    }

    #[test]
    fn expands_json_each_in_from_clause() {
        let result = run(
            "SELECT k, json_extract_path_text(v, 'currency') AS currency \
             FROM json_each('{\"first\":{\"currency\":\"XRP\"},\"second\":{\"currency\":\"USDC\"}}') AS src(k, v) \
             ORDER BY k",
        );
        assert_eq!(
            result.columns,
            vec!["k".to_string(), "currency".to_string()]
        );
        assert_eq!(
            result.rows,
            vec![
                vec![
                    ScalarValue::Text("first".to_string()),
                    ScalarValue::Text("XRP".to_string()),
                ],
                vec![
                    ScalarValue::Text("second".to_string()),
                    ScalarValue::Text("USDC".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn expands_json_each_text_in_from_clause() {
        let result = run("SELECT k, v \
             FROM json_each_text('{\"a\":1,\"b\":null,\"c\":{\"x\":1}}') AS src(k, v) \
             ORDER BY k");
        assert_eq!(result.columns, vec!["k".to_string(), "v".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Text("1".to_string()),
                ],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Null],
                vec![
                    ScalarValue::Text("c".to_string()),
                    ScalarValue::Text("{\"x\":1}".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn expands_json_object_keys_in_from_clause() {
        let result = run("SELECT * FROM json_object_keys('{\"z\":1,\"a\":2}') ORDER BY 1");
        assert_eq!(result.columns, vec!["key".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Text("z".to_string())],
            ]
        );
    }

    #[test]
    fn supports_column_aliases_for_table_functions() {
        let result = run("SELECT item FROM json_array_elements_text('[1,2]') AS t(item)");
        assert_eq!(result.columns, vec!["item".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("1".to_string())],
                vec![ScalarValue::Text("2".to_string())],
            ]
        );
    }

    #[test]
    fn validates_column_alias_count_for_multi_column_table_function() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT * FROM json_each('{\"a\":1}') AS t(k)").expect("parses");
            let planned = plan_statement(statement).expect("plans");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("alias count mismatch should be rejected");
            assert!(err.message.contains("expects 2 column aliases"));
        });
    }

    #[test]
    fn allows_table_function_arguments_to_reference_prior_from_items() {
        let result = run(
            "WITH payload AS (SELECT '{\"result\":[{\"currency\":\"XRP\"},{\"currency\":\"USDC\"}]}' AS body) \
             SELECT json_extract_path_text(item, 'currency') AS currency \
             FROM payload, json_array_elements(json_extract_path(body, 'result')) AS src(item) \
             ORDER BY currency",
        );
        assert_eq!(result.columns, vec!["currency".to_string()]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("USDC".to_string())],
                vec![ScalarValue::Text("XRP".to_string())],
            ]
        );
    }

    #[test]
    fn rejects_non_array_json_for_json_array_elements() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT * FROM json_array_elements('{\"a\":1}')").expect("parses");
            let planned = plan_statement(statement).expect("plans");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("non-array JSON input should fail");
            assert!(err.message.contains("must be a JSON array"));
        });
    }

    #[test]
    fn rejects_non_object_json_for_json_each() {
        with_isolated_state(|| {
            let statement = parse_statement("SELECT * FROM json_each('[1,2,3]')").expect("parses");
            let planned = plan_statement(statement).expect("plans");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("non-object JSON input should fail");
            assert!(err.message.contains("must be a JSON object"));
        });
    }

    #[test]
    fn rejects_missing_column_aliases_for_json_to_record() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT * FROM json_to_record('{\"a\":1}')").expect("parses");
            let planned = plan_statement(statement).expect("plans");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("json_to_record should require aliases");
            assert!(err.message.contains("requires column aliases"));
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn evaluates_http_get_builtin_function() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        with_isolated_state(|| {
            let listener = match TcpListener::bind("127.0.0.1:0") {
                Ok(listener) => listener,
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
                Err(err) => panic!("listener should bind: {err}"),
            };
            let addr = listener.local_addr().expect("listener local addr");
            let handle = thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("should accept connection");
                let mut request_buf = [0u8; 1024];
                let _ = stream.read(&mut request_buf);
                let body = "hello-from-http-get";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("response write should succeed");
            });

            let sql = format!("SELECT http_get('http://{}/data')", addr);
            let result = run_statement(&sql, &[]);
            assert_eq!(
                result.rows,
                vec![vec![ScalarValue::Text("hello-from-http-get".to_string())]]
            );

            handle.join().expect("http server thread should finish");
        });
    }

    #[test]
    fn substring_rejects_negative_length() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT substring('abcdef', 2, -1)").expect("statement parses");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("negative length should fail");
            assert!(err.message.contains("negative substring length"));
        });
    }

    #[test]
    fn executes_from_subquery() {
        let result = run("SELECT u.id \
             FROM (SELECT 1 AS id UNION SELECT 2 AS id) u \
             WHERE u.id > 1 \
             ORDER BY 1");
        assert_eq!(result.rows, vec![vec![ScalarValue::Int(2)]]);
    }

    #[test]
    fn executes_lateral_subquery_basic() {
        let results = run_batch(&[
            "CREATE TABLE test_table (id int8)",
            "INSERT INTO test_table VALUES (1), (2)",
            "SELECT t.id, l.val FROM test_table t, LATERAL (SELECT t.id * 2 AS val) l ORDER BY 1",
        ]);
        assert_eq!(
            results[2].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Int(2)],
                vec![ScalarValue::Int(2), ScalarValue::Int(4)],
            ]
        );
    }

    #[test]
    fn executes_lateral_generate_series() {
        let result = run(
            "SELECT g.n, l.tens \
             FROM generate_series(1,3) g(n), LATERAL (SELECT n * 10 AS tens) l \
             ORDER BY 1",
        );
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Int(10)],
                vec![ScalarValue::Int(2), ScalarValue::Int(20)],
                vec![ScalarValue::Int(3), ScalarValue::Int(30)],
            ]
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn executes_lateral_http_get() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        with_isolated_state(|| {
            let listener = match TcpListener::bind("127.0.0.1:0") {
                Ok(listener) => listener,
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
                Err(err) => panic!("listener should bind: {err}"),
            };
            let addr = listener.local_addr().expect("listener local addr");
            let handle = thread::spawn(move || {
                for _ in 0..2 {
                    let (mut stream, _) = listener.accept().expect("should accept connection");
                    let mut request_buf = [0u8; 1024];
                    let _ = stream.read(&mut request_buf);
                    let body = "lateral-http-get";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("response write should succeed");
                }
            });

            let url1 = format!("http://{}/one", addr);
            let url2 = format!("http://{}/two", addr);
            run_statement("CREATE TABLE urls (url text)", &[]);
            let insert_sql = format!(
                "INSERT INTO urls VALUES ('{}'), ('{}')",
                url1, url2
            );
            run_statement(&insert_sql, &[]);
            let result = run_statement(
                "SELECT u.url, length(body) \
                 FROM urls u, LATERAL (SELECT http_get(u.url) AS body) l \
                 ORDER BY 1",
                &[],
            );
            assert_eq!(
                result.rows,
                vec![
                    vec![ScalarValue::Text(url1), ScalarValue::Int(16)],
                    vec![ScalarValue::Text(url2), ScalarValue::Int(16)],
                ]
            );

            handle.join().expect("http server thread should finish");
        });
    }

    #[test]
    fn executes_array_constructors() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE test_table (id int8)", &[]);
            run_statement("INSERT INTO test_table VALUES (1), (2)", &[]);

            let r1 = run_statement("SELECT ARRAY[1, 2, 3]", &[]);
            assert_eq!(
                r1.rows[0][0],
                ScalarValue::Array(vec![
                    ScalarValue::Int(1),
                    ScalarValue::Int(2),
                    ScalarValue::Int(3),
                ])
            );

            let r2 = run_statement("SELECT ARRAY['a', 'b', 'c']", &[]);
            assert_eq!(
                r2.rows[0][0],
                ScalarValue::Array(vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Text("b".to_string()),
                    ScalarValue::Text("c".to_string()),
                ])
            );

            let r3 = run_statement(
                "SELECT ARRAY(SELECT id FROM test_table ORDER BY id)",
                &[],
            );
            assert_eq!(
                r3.rows[0][0],
                ScalarValue::Array(vec![ScalarValue::Int(1), ScalarValue::Int(2)])
            );

            let r4 = run_statement("SELECT ARRAY[1, 2] || ARRAY[3, 4]", &[]);
            assert_eq!(
                r4.rows[0][0],
                ScalarValue::Array(vec![
                    ScalarValue::Int(1),
                    ScalarValue::Int(2),
                    ScalarValue::Int(3),
                    ScalarValue::Int(4),
                ])
            );
        });
    }

    #[test]
    fn executes_with_cte_query() {
        let result =
            run("WITH t AS (SELECT 1 AS id UNION ALL SELECT 2 AS id) SELECT id FROM t ORDER BY 1");
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn executes_recursive_cte_fixpoint() {
        let result = run(
            "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) SELECT id FROM t ORDER BY 1",
        );
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Int(1)],
                vec![ScalarValue::Int(2)],
                vec![ScalarValue::Int(3)]
            ]
        );
    }

    #[test]
    fn executes_recursive_cte_union_distinct_deduplicates_fixpoint_rows() {
        let result = run(
            "WITH RECURSIVE t AS (SELECT 1 AS id UNION SELECT id FROM t WHERE id < 3) SELECT id FROM t ORDER BY 1",
        );
        assert_eq!(result.rows, vec![vec![ScalarValue::Int(1)]]);
    }

    #[test]
    fn executes_recursive_cte_with_multiple_ctes() {
        let result = run(
            "WITH RECURSIVE seed AS (SELECT 1 AS id), t AS (SELECT id FROM seed UNION ALL SELECT id + 1 FROM t WHERE id < 3), u AS (SELECT id FROM t WHERE id > 1) SELECT id FROM u ORDER BY 1",
        );
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(2)], vec![ScalarValue::Int(3)]]
        );
    }

    #[test]
    fn recursive_cte_rejects_self_reference_in_non_recursive_term() {
        with_isolated_state(|| {
            let statement = parse_statement(
                "WITH RECURSIVE t AS (SELECT id FROM t UNION ALL SELECT 1) SELECT id FROM t",
            )
            .expect("statement should parse");
            let err =
                plan_statement(statement).expect_err("planning should reject invalid recursion");
            assert!(err.message.contains("self-reference in non-recursive term"));
        });
    }

    #[test]
    fn non_self_referencing_cte_in_recursive_with_executes() {
        with_isolated_state(|| {
            let statement = parse_statement(
                "WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM t",
            )
            .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let result = block_on(execute_planned_query(&planned, &[])).expect("query should execute");
            assert_eq!(
                result.rows,
                vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
            );
        });
    }

    #[test]
    fn create_view_reads_live_underlying_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE VIEW v_users AS SELECT id, name FROM users",
            "INSERT INTO users VALUES (2, 'b')",
            "SELECT id, name FROM v_users ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())]
            ]
        );
    }

    #[test]
    fn create_or_replace_view_updates_definition() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE VIEW v_users AS SELECT id FROM users",
            "CREATE OR REPLACE VIEW v_users AS SELECT name FROM users",
            "SELECT * FROM v_users",
        ]);
        assert_eq!(results[3].command_tag, "CREATE VIEW");
        assert_eq!(
            results[4].rows,
            vec![vec![ScalarValue::Text("a".to_string())]]
        );
    }

    #[test]
    fn create_or_replace_view_rejects_non_view_relation() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            let statement = parse_statement("CREATE OR REPLACE VIEW users AS SELECT 1")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("create or replace should fail for table relation");
            assert!(err.message.contains("not a view"));
        });
    }

    #[test]
    fn create_or_replace_materialized_view_updates_definition_and_snapshot() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users",
            "INSERT INTO users VALUES (2, 'b')",
            "CREATE OR REPLACE MATERIALIZED VIEW mv_users AS SELECT name FROM users",
            "SELECT * FROM mv_users ORDER BY 1",
        ]);
        assert_eq!(results[4].command_tag, "CREATE MATERIALIZED VIEW");
        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Text("b".to_string())]
            ]
        );
    }

    #[test]
    fn create_or_replace_materialized_view_with_no_data_keeps_relation_empty() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users",
            "CREATE OR REPLACE MATERIALIZED VIEW mv_users AS SELECT name FROM users WITH NO DATA",
            "SELECT * FROM mv_users",
        ]);
        assert_eq!(results[3].command_tag, "CREATE MATERIALIZED VIEW");
        assert!(results[4].rows.is_empty());
    }

    #[test]
    fn alter_view_rename_changes_relation_name() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_users AS SELECT id FROM users", &[]);
            let altered = run_statement("ALTER VIEW v_users RENAME TO users_view", &[]);
            assert_eq!(altered.command_tag, "ALTER VIEW");

            let old_select =
                parse_statement("SELECT * FROM v_users").expect("statement should parse");
            let old_err = plan_statement(old_select).expect_err("old view name should not resolve");
            assert!(old_err.message.contains("does not exist"));

            let new_rows = run_statement("SELECT id FROM users_view", &[]);
            assert!(new_rows.rows.is_empty());
        });
    }

    #[test]
    fn alter_view_rename_column_changes_visible_name() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY, name text)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'a')", &[]);
            run_statement("CREATE VIEW v_users AS SELECT id, name FROM users", &[]);

            let altered = run_statement("ALTER VIEW v_users RENAME COLUMN name TO username", &[]);
            assert_eq!(altered.command_tag, "ALTER VIEW");

            let renamed = run_statement("SELECT username FROM v_users ORDER BY 1", &[]);
            assert_eq!(renamed.rows, vec![vec![ScalarValue::Text("a".to_string())]]);

            let statement = parse_statement("SELECT name FROM v_users").expect("statement parses");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("old column name should not resolve");
            assert!(
                err.message.contains("unknown column") || err.message.contains("does not exist")
            );
        });
    }

    #[test]
    fn alter_materialized_view_set_schema_moves_relation() {
        with_isolated_state(|| {
            run_statement("CREATE SCHEMA app", &[]);
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("INSERT INTO users VALUES (1)", &[]);
            run_statement(
                "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users",
                &[],
            );
            let altered = run_statement("ALTER MATERIALIZED VIEW mv_users SET SCHEMA app", &[]);
            assert_eq!(altered.command_tag, "ALTER MATERIALIZED VIEW");

            let old_select =
                parse_statement("SELECT * FROM mv_users").expect("statement should parse");
            let old_err = plan_statement(old_select).expect_err("old name should not resolve");
            assert!(old_err.message.contains("does not exist"));

            let new_select = run_statement("SELECT id FROM app.mv_users", &[]);
            assert_eq!(new_select.rows, vec![vec![ScalarValue::Int(1)]]);
        });
    }

    #[test]
    fn create_materialized_view_stores_snapshot() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users",
            "INSERT INTO users VALUES (2, 'b')",
            "SELECT id, name FROM mv_users ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a".to_string())
            ]]
        );
    }

    #[test]
    fn create_materialized_view_with_no_data_starts_empty() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users WITH NO DATA",
            "SELECT id, name FROM mv_users ORDER BY 1",
            "REFRESH MATERIALIZED VIEW mv_users",
            "SELECT id, name FROM mv_users ORDER BY 1",
        ]);
        assert!(results[3].rows.is_empty());
        assert_eq!(
            results[5].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a".to_string())
            ]]
        );
    }

    #[test]
    fn refresh_materialized_view_recomputes_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users",
            "INSERT INTO users VALUES (2, 'b')",
            "SELECT id, name FROM mv_users ORDER BY 1",
            "REFRESH MATERIALIZED VIEW mv_users",
            "SELECT id, name FROM mv_users ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a".to_string())
            ]]
        );
        assert_eq!(results[5].command_tag, "REFRESH MATERIALIZED VIEW");
        assert_eq!(
            results[6].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())]
            ]
        );
    }

    #[test]
    fn refresh_materialized_view_with_no_data_clears_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users",
            "REFRESH MATERIALIZED VIEW mv_users WITH NO DATA",
            "SELECT id, name FROM mv_users ORDER BY 1",
        ]);
        assert_eq!(results[3].command_tag, "REFRESH MATERIALIZED VIEW");
        assert!(results[4].rows.is_empty());
    }

    #[test]
    fn refresh_materialized_view_rejects_plain_view() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_users AS SELECT id FROM users", &[]);
            let statement = parse_statement("REFRESH MATERIALIZED VIEW v_users")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("refresh should fail for non-materialized view");
            assert!(err.message.contains("is not a materialized view"));
        });
    }

    #[test]
    fn refresh_materialized_view_concurrently_requires_unique_index() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users",
                &[],
            );
            let statement = parse_statement("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("refresh concurrently should enforce unique index");
            assert!(err.message.contains("unique index"));
        });
    }

    #[test]
    fn refresh_materialized_view_concurrently_recomputes_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'a')",
            "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users",
            "CREATE UNIQUE INDEX uq_mv_users_id ON mv_users (id)",
            "INSERT INTO users VALUES (2, 'b')",
            "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users",
            "SELECT id, name FROM mv_users ORDER BY 1",
        ]);
        assert_eq!(results[5].command_tag, "REFRESH MATERIALIZED VIEW");
        assert_eq!(
            results[6].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())]
            ]
        );
    }

    #[test]
    fn refresh_materialized_view_concurrently_rejects_with_no_data() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users",
                &[],
            );
            run_statement("CREATE UNIQUE INDEX uq_mv_users_id ON mv_users (id)", &[]);
            let statement =
                parse_statement("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users WITH NO DATA")
                    .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("refresh concurrently should reject WITH NO DATA");
            assert!(err.message.contains("WITH NO DATA"));
        });
    }

    #[test]
    fn drop_table_restrict_rejects_dependent_view() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_users AS SELECT id FROM users", &[]);
            let statement = parse_statement("DROP TABLE users").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("drop should fail");
            assert!(err.message.contains("depends on it"));
        });
    }

    #[test]
    fn drop_table_cascade_drops_dependent_view() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_users AS SELECT id FROM users", &[]);
            run_statement("DROP TABLE users CASCADE", &[]);
            let statement =
                parse_statement("SELECT * FROM v_users").expect("statement should parse");
            let err = plan_statement(statement).expect_err("view should be dropped");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_view_cascade_drops_dependent_views() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_base AS SELECT id FROM users", &[]);
            run_statement("CREATE VIEW v_child AS SELECT id FROM v_base", &[]);

            let restrict =
                parse_statement("DROP VIEW v_base RESTRICT").expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop restrict should fail for dependent view");
            assert!(err.message.contains("depends on it"));

            run_statement("DROP VIEW v_base CASCADE", &[]);
            let child_select =
                parse_statement("SELECT * FROM v_child").expect("statement should parse");
            let err = plan_statement(child_select).expect_err("dependent view should be dropped");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_view_cascade_drops_dependent_materialized_views() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v_base AS SELECT id FROM users", &[]);
            run_statement(
                "CREATE MATERIALIZED VIEW mv_child AS SELECT id FROM v_base",
                &[],
            );

            let restrict =
                parse_statement("DROP VIEW v_base RESTRICT").expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop restrict should fail for dependent materialized view");
            assert!(err.message.contains("depends on it"));

            run_statement("DROP VIEW v_base CASCADE", &[]);
            let child_select =
                parse_statement("SELECT * FROM mv_child").expect("statement should parse");
            let err = plan_statement(child_select).expect_err("dependent view should be dropped");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_table_restrict_rejects_view_dependency_via_scalar_subquery() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE VIEW v_dep AS SELECT (SELECT count(*) FROM users) AS c",
                &[],
            );

            let statement = parse_statement("DROP TABLE users").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("drop should fail when view depends via scalar subquery");
            assert!(err.message.contains("depends on it"));
        });
    }

    #[test]
    fn drop_table_cascade_drops_view_dependency_via_scalar_subquery() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE VIEW v_dep AS SELECT (SELECT count(*) FROM users) AS c",
                &[],
            );
            run_statement("DROP TABLE users CASCADE", &[]);

            let statement = parse_statement("SELECT * FROM v_dep").expect("statement should parse");
            let err = plan_statement(statement).expect_err("view should be dropped by cascade");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_view_with_multiple_names_drops_all_targets() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY)", &[]);
            run_statement("CREATE VIEW v1 AS SELECT id FROM users", &[]);
            run_statement("CREATE VIEW v2 AS SELECT id FROM users", &[]);
            run_statement("DROP VIEW v1, v2", &[]);

            let select_v1 = parse_statement("SELECT * FROM v1").expect("statement should parse");
            let err_v1 = plan_statement(select_v1).expect_err("v1 should be dropped");
            assert!(err_v1.message.contains("does not exist"));

            let select_v2 = parse_statement("SELECT * FROM v2").expect("statement should parse");
            let err_v2 = plan_statement(select_v2).expect_err("v2 should be dropped");
            assert!(err_v2.message.contains("does not exist"));
        });
    }

    #[test]
    fn merge_updates_matches_and_inserts_non_matches() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'old'), (2, 'two')",
            "INSERT INTO staging VALUES (1, 'new'), (3, 'three')",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].command_tag, "MERGE");
        assert_eq!(results[4].rows_affected, 2);
        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("new".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("two".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Text("three".to_string())]
            ]
        );
    }

    #[test]
    fn merge_not_matched_by_target_inserts_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'old')",
            "INSERT INTO staging VALUES (1, 'old'), (2, 'two')",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, s.name)",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].command_tag, "MERGE");
        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("old".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("two".to_string())],
            ]
        );
    }

    #[test]
    fn merge_matched_delete_removes_matching_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY)",
            "INSERT INTO users VALUES (1, 'one'), (2, 'two')",
            "INSERT INTO staging VALUES (2)",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN DELETE",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].command_tag, "MERGE");
        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("one".to_string())
            ]]
        );
    }

    #[test]
    fn merge_matched_do_nothing_skips_matched_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'old')",
            "INSERT INTO staging VALUES (1, 'new'), (2, 'two')",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("old".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("two".to_string())],
            ]
        );
    }

    #[test]
    fn merge_not_matched_by_source_delete_removes_unmatched_target_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            "INSERT INTO staging VALUES (2, 'two-new')",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED BY SOURCE THEN DELETE",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].command_tag, "MERGE");
        assert_eq!(results[4].rows_affected, 3);
        assert_eq!(
            results[5].rows,
            vec![vec![
                ScalarValue::Int(2),
                ScalarValue::Text("two-new".to_string())
            ]]
        );
    }

    #[test]
    fn merge_not_matched_by_source_update_applies_to_unmatched_target_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY)",
            "INSERT INTO users VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            "INSERT INTO staging VALUES (2)",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET name = 'inactive'",
            "SELECT id, name FROM users ORDER BY 1",
        ]);
        assert_eq!(results[4].rows_affected, 2);
        assert_eq!(
            results[5].rows,
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("inactive".to_string())
                ],
                vec![ScalarValue::Int(2), ScalarValue::Text("two".to_string())],
                vec![
                    ScalarValue::Int(3),
                    ScalarValue::Text("inactive".to_string())
                ],
            ]
        );
    }

    #[test]
    fn merge_returning_emits_modified_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE staging (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'old')",
            "INSERT INTO staging VALUES (1, 'new'), (2, 'two')",
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) RETURNING u.id, u.name",
        ]);
        assert_eq!(results[4].command_tag, "MERGE");
        assert_eq!(
            results[4].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("new".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("two".to_string())],
            ]
        );
    }

    #[test]
    fn merge_rejects_multiple_source_rows_modifying_same_target_row() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY, name text)", &[]);
            run_statement("CREATE TABLE staging (id int8, name text)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'old')", &[]);
            run_statement(
                "INSERT INTO staging VALUES (1, 'first'), (1, 'second')",
                &[],
            );

            let statement = parse_statement(
                "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
            )
            .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[]))
                .expect_err("merge should fail when same target row is modified twice");
            assert!(err.message.contains("same target row"));
        });
    }

    #[test]
    fn executes_exists_correlated_subquery_predicate() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE orders (id int8 PRIMARY KEY, user_id int8)",
            "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO orders VALUES (10, 1), (11, 1), (12, 3)",
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = users.id) ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(3)]]
        );
    }

    #[test]
    fn executes_in_subquery_predicate() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "CREATE TABLE orders (id int8 PRIMARY KEY, user_id int8)",
            "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO orders VALUES (10, 1), (11, 3)",
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY 1",
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders) ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(3)]]
        );
        assert_eq!(results[5].rows, vec![vec![ScalarValue::Int(2)]]);
    }

    #[test]
    fn executes_scalar_subquery_expression_with_correlation() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY)",
            "CREATE TABLE orders (id int8 PRIMARY KEY, user_id int8)",
            "INSERT INTO users VALUES (1), (2), (3)",
            "INSERT INTO orders VALUES (10, 1), (11, 1), (12, 3)",
            "SELECT id, (SELECT count(*) FROM orders o WHERE o.user_id = users.id) AS order_count FROM users ORDER BY 1",
        ]);
        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Int(2)],
                vec![ScalarValue::Int(2), ScalarValue::Int(0)],
                vec![ScalarValue::Int(3), ScalarValue::Int(1)]
            ]
        );
    }

    #[test]
    fn executes_inner_join_on_subqueries() {
        let result = run("SELECT l.id \
             FROM (SELECT 1 AS id UNION SELECT 2 AS id) l \
             INNER JOIN (SELECT 2 AS id UNION SELECT 3 AS id) r \
             ON l.id = r.id");
        assert_eq!(result.rows, vec![vec![ScalarValue::Int(2)]]);
    }

    #[test]
    fn executes_left_join_using() {
        let result = run("SELECT l.id \
             FROM (SELECT 1 AS id UNION SELECT 2 AS id) l \
             LEFT JOIN (SELECT 2 AS id) r USING (id) \
             ORDER BY 1");
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn executes_from_dual_relation() {
        let result = run("SELECT 42 FROM dual");
        assert_eq!(result.rows, vec![vec![ScalarValue::Int(42)]]);
    }

    #[test]
    fn executes_group_by_with_having_aggregate() {
        let result = run("SELECT t.id, count(*) AS c \
             FROM (SELECT 1 AS id UNION ALL SELECT 1 AS id UNION ALL SELECT 2 AS id) t \
             GROUP BY t.id \
             HAVING count(*) > 1 \
             ORDER BY 1");
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(1), ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn executes_filtered_aggregates() {
        let results = run_batch(&[
            "CREATE TABLE sales (region text, amount int, year int)",
            "INSERT INTO sales VALUES ('East', 100, 2024), ('East', 200, 2025), ('West', 150, 2024), ('West', 300, 2025)",
            "SELECT region, \
             count(*) AS total, \
             count(*) FILTER (WHERE year = 2025) AS count_2025, \
             sum(amount) FILTER (WHERE year = 2024) AS sum_2024 \
             FROM sales \
             GROUP BY region \
             ORDER BY region",
        ]);

        assert_eq!(
            results[2].rows,
            vec![
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Int(2),
                    ScalarValue::Int(1),
                    ScalarValue::Int(100),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Int(2),
                    ScalarValue::Int(1),
                    ScalarValue::Int(150),
                ],
            ]
        );
    }

    #[test]
    fn executes_grouping_sets_rollup_and_cube() {
        let results = run_batch(&[
            "CREATE TABLE sales (region text, amount int, year int)",
            "INSERT INTO sales VALUES ('East', 100, 2024), ('East', 200, 2025), ('West', 150, 2024), ('West', 300, 2025)",
            "SELECT region, year, sum(amount) \
             FROM sales \
             GROUP BY GROUPING SETS ((region, year), (region), ()) \
             ORDER BY 1, 2",
            "SELECT region, year, sum(amount) \
             FROM sales \
             GROUP BY ROLLUP (region, year) \
             ORDER BY 1, 2",
            "SELECT region, year, sum(amount) \
             FROM sales \
             GROUP BY CUBE (region, year) \
             ORDER BY 1, 2",
            "SELECT region, year, sum(amount), grouping(region) AS gr, grouping(year) AS gy \
             FROM sales \
             GROUP BY ROLLUP (region, year) \
             ORDER BY 1, 2",
        ]);

        let rollup_rows = vec![
            vec![ScalarValue::Null, ScalarValue::Null, ScalarValue::Int(750)],
            vec![
                ScalarValue::Text("East".to_string()),
                ScalarValue::Null,
                ScalarValue::Int(300),
            ],
            vec![
                ScalarValue::Text("East".to_string()),
                ScalarValue::Int(2024),
                ScalarValue::Int(100),
            ],
            vec![
                ScalarValue::Text("East".to_string()),
                ScalarValue::Int(2025),
                ScalarValue::Int(200),
            ],
            vec![
                ScalarValue::Text("West".to_string()),
                ScalarValue::Null,
                ScalarValue::Int(450),
            ],
            vec![
                ScalarValue::Text("West".to_string()),
                ScalarValue::Int(2024),
                ScalarValue::Int(150),
            ],
            vec![
                ScalarValue::Text("West".to_string()),
                ScalarValue::Int(2025),
                ScalarValue::Int(300),
            ],
        ];

        assert_eq!(results[2].rows, rollup_rows);
        assert_eq!(results[3].rows, rollup_rows);

        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Null, ScalarValue::Null, ScalarValue::Int(750)],
                vec![ScalarValue::Null, ScalarValue::Int(2024), ScalarValue::Int(250)],
                vec![ScalarValue::Null, ScalarValue::Int(2025), ScalarValue::Int(500)],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Null,
                    ScalarValue::Int(300),
                ],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Int(2024),
                    ScalarValue::Int(100),
                ],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Int(2025),
                    ScalarValue::Int(200),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Null,
                    ScalarValue::Int(450),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Int(2024),
                    ScalarValue::Int(150),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Int(2025),
                    ScalarValue::Int(300),
                ],
            ]
        );

        assert_eq!(
            results[5].rows,
            vec![
                vec![
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Int(750),
                    ScalarValue::Int(1),
                    ScalarValue::Int(1),
                ],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Null,
                    ScalarValue::Int(300),
                    ScalarValue::Int(0),
                    ScalarValue::Int(1),
                ],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Int(2024),
                    ScalarValue::Int(100),
                    ScalarValue::Int(0),
                    ScalarValue::Int(0),
                ],
                vec![
                    ScalarValue::Text("East".to_string()),
                    ScalarValue::Int(2025),
                    ScalarValue::Int(200),
                    ScalarValue::Int(0),
                    ScalarValue::Int(0),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Null,
                    ScalarValue::Int(450),
                    ScalarValue::Int(0),
                    ScalarValue::Int(1),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Int(2024),
                    ScalarValue::Int(150),
                    ScalarValue::Int(0),
                    ScalarValue::Int(0),
                ],
                vec![
                    ScalarValue::Text("West".to_string()),
                    ScalarValue::Int(2025),
                    ScalarValue::Int(300),
                    ScalarValue::Int(0),
                    ScalarValue::Int(0),
                ],
            ]
        );
    }

    #[test]
    fn executes_ranking_and_offset_window_functions() {
        let results = run_batch(&[
            "CREATE TABLE wf (dept text, id int8, score int8)",
            "INSERT INTO wf VALUES ('a', 1, 10), ('a', 2, 10), ('a', 3, 20), ('b', 4, 5), ('b', 5, 7)",
            "SELECT id, \
             row_number() OVER (PARTITION BY dept ORDER BY score DESC), \
             rank() OVER (PARTITION BY dept ORDER BY score DESC), \
             dense_rank() OVER (PARTITION BY dept ORDER BY score DESC), \
             lag(score, 1, 0) OVER (PARTITION BY dept ORDER BY id), \
             lead(score, 2, -1) OVER (PARTITION BY dept ORDER BY id) \
             FROM wf ORDER BY id",
        ]);
        assert_eq!(
            results[2].rows,
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Int(2),
                    ScalarValue::Int(2),
                    ScalarValue::Int(2),
                    ScalarValue::Int(0),
                    ScalarValue::Int(20),
                ],
                vec![
                    ScalarValue::Int(2),
                    ScalarValue::Int(3),
                    ScalarValue::Int(2),
                    ScalarValue::Int(2),
                    ScalarValue::Int(10),
                    ScalarValue::Int(-1),
                ],
                vec![
                    ScalarValue::Int(3),
                    ScalarValue::Int(1),
                    ScalarValue::Int(1),
                    ScalarValue::Int(1),
                    ScalarValue::Int(10),
                    ScalarValue::Int(-1),
                ],
                vec![
                    ScalarValue::Int(4),
                    ScalarValue::Int(2),
                    ScalarValue::Int(2),
                    ScalarValue::Int(2),
                    ScalarValue::Int(0),
                    ScalarValue::Int(-1),
                ],
                vec![
                    ScalarValue::Int(5),
                    ScalarValue::Int(1),
                    ScalarValue::Int(1),
                    ScalarValue::Int(1),
                    ScalarValue::Int(5),
                    ScalarValue::Int(-1),
                ],
            ]
        );
    }

    #[test]
    fn executes_window_aggregates_with_rows_and_range_frames() {
        let results = run_batch(&[
            "CREATE TABLE wf (dept text, id int8, score int8)",
            "INSERT INTO wf VALUES ('a', 1, 10), ('a', 2, 10), ('a', 3, 20), ('b', 4, 5), ('b', 5, 7)",
            "SELECT id, \
             sum(score) OVER (PARTITION BY dept ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
             count(*) OVER (PARTITION BY dept), \
             avg(score) OVER (PARTITION BY dept), \
             min(score) OVER (PARTITION BY dept), \
             max(score) OVER (PARTITION BY dept) \
             FROM wf ORDER BY id",
            "SELECT id, \
             sum(score) OVER (PARTITION BY dept ORDER BY score RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) \
             FROM wf WHERE dept = 'a' ORDER BY id",
        ]);
        assert_eq!(
            results[2].rows,
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Int(10),
                    ScalarValue::Int(3),
                    ScalarValue::Float(13.333333333333334),
                    ScalarValue::Int(10),
                    ScalarValue::Int(20),
                ],
                vec![
                    ScalarValue::Int(2),
                    ScalarValue::Int(20),
                    ScalarValue::Int(3),
                    ScalarValue::Float(13.333333333333334),
                    ScalarValue::Int(10),
                    ScalarValue::Int(20),
                ],
                vec![
                    ScalarValue::Int(3),
                    ScalarValue::Int(30),
                    ScalarValue::Int(3),
                    ScalarValue::Float(13.333333333333334),
                    ScalarValue::Int(10),
                    ScalarValue::Int(20),
                ],
                vec![
                    ScalarValue::Int(4),
                    ScalarValue::Int(5),
                    ScalarValue::Int(2),
                    ScalarValue::Float(6.0),
                    ScalarValue::Int(5),
                    ScalarValue::Int(7),
                ],
                vec![
                    ScalarValue::Int(5),
                    ScalarValue::Int(12),
                    ScalarValue::Int(2),
                    ScalarValue::Float(6.0),
                    ScalarValue::Int(5),
                    ScalarValue::Int(7),
                ],
            ]
        );
        assert_eq!(
            results[3].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Int(20)],
                vec![ScalarValue::Int(2), ScalarValue::Int(20)],
                vec![ScalarValue::Int(3), ScalarValue::Int(20)],
            ]
        );
    }

    #[test]
    fn executes_json_aggregate_functions() {
        let result = run("SELECT \
             json_agg(v), \
             jsonb_agg(v), \
             json_object_agg(k, v), \
             jsonb_object_agg(k, v) \
             FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b' AS k, 2 AS v) t");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0],
            vec![
                ScalarValue::Text("[1,2]".to_string()),
                ScalarValue::Text("[1,2]".to_string()),
                ScalarValue::Text("{\"a\":1,\"b\":2}".to_string()),
                ScalarValue::Text("{\"a\":1,\"b\":2}".to_string()),
            ]
        );
    }

    #[test]
    fn executes_json_aggregate_modifiers() {
        let results = run_batch(&[
            "SELECT \
             json_agg(DISTINCT v ORDER BY v DESC) FILTER (WHERE keep), \
             count(DISTINCT v) FILTER (WHERE keep) \
             FROM (SELECT 1 AS v, true AS keep UNION ALL SELECT 2 AS v, true AS keep UNION ALL SELECT 1 AS v, false AS keep) t",
            "SELECT \
             json_object_agg(k, v ORDER BY v DESC), \
             json_object_agg(k, v ORDER BY v ASC) \
             FROM (SELECT 'x' AS k, 1 AS v UNION ALL SELECT 'x' AS k, 2 AS v) t",
        ]);

        assert_eq!(
            results[0].rows,
            vec![vec![
                ScalarValue::Text("[2,1]".to_string()),
                ScalarValue::Int(2),
            ]]
        );
        assert_eq!(
            results[1].rows,
            vec![vec![
                ScalarValue::Text("{\"x\":1}".to_string()),
                ScalarValue::Text("{\"x\":2}".to_string()),
            ]]
        );
    }

    #[test]
    fn json_object_agg_rejects_null_keys() {
        with_isolated_state(|| {
            let statement =
                parse_statement("SELECT json_object_agg(k, v) FROM (SELECT NULL AS k, 1 AS v) t")
                    .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("null key should fail");
            assert!(err.message.contains("key cannot be null"));
        });
    }

    #[test]
    fn executes_global_aggregate_over_empty_input() {
        let result = run("SELECT count(*), json_agg(v), json_object_agg(k, v) \
             FROM (SELECT 1 AS v, 'a' AS k WHERE false) t");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int(0),
                ScalarValue::Null,
                ScalarValue::Null
            ]]
        );
    }

    #[test]
    fn creates_heap_table_inserts_and_scans_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users (id, name) VALUES (2, 'bravo'), (1, 'alpha')",
            "SELECT u.id, u.name FROM users u ORDER BY 1",
        ]);

        assert_eq!(results[0].command_tag, "CREATE TABLE");
        assert_eq!(results[0].rows_affected, 0);
        assert_eq!(results[1].command_tag, "INSERT");
        assert_eq!(results[1].rows_affected, 2);
        assert_eq!(
            results[2].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("bravo".to_string())]
            ]
        );
    }

    #[test]
    fn creates_sequence_and_evaluates_nextval_currval() {
        let results = run_batch(&[
            "CREATE SEQUENCE user_id_seq",
            "SELECT nextval('user_id_seq'), nextval('user_id_seq')",
            "SELECT currval('user_id_seq')",
        ]);

        assert_eq!(
            results[1].rows,
            vec![vec![ScalarValue::Int(1), ScalarValue::Int(2)]]
        );
        assert_eq!(results[2].rows, vec![vec![ScalarValue::Int(2)]]);
    }

    #[test]
    fn create_sequence_supports_start_and_increment_options() {
        let results = run_batch(&[
            "CREATE SEQUENCE seq1 START WITH 10 INCREMENT BY 5 MINVALUE 0 MAXVALUE 100 CACHE 8",
            "SELECT nextval('seq1'), nextval('seq1'), currval('seq1')",
        ]);
        assert_eq!(
            results[1].rows,
            vec![vec![
                ScalarValue::Int(10),
                ScalarValue::Int(15),
                ScalarValue::Int(15)
            ]]
        );
    }

    #[test]
    fn alter_sequence_restart_and_increment_work() {
        let results = run_batch(&[
            "CREATE SEQUENCE seq2 START WITH 3 INCREMENT BY 2",
            "SELECT nextval('seq2')",
            "ALTER SEQUENCE seq2 RESTART WITH 20",
            "SELECT nextval('seq2')",
            "ALTER SEQUENCE seq2 INCREMENT BY 7",
            "SELECT nextval('seq2')",
            "ALTER SEQUENCE seq2 START WITH 100",
            "ALTER SEQUENCE seq2 RESTART",
            "SELECT nextval('seq2')",
        ]);
        assert_eq!(results[1].rows, vec![vec![ScalarValue::Int(3)]]);
        assert_eq!(results[3].rows, vec![vec![ScalarValue::Int(20)]]);
        assert_eq!(results[5].rows, vec![vec![ScalarValue::Int(27)]]);
        assert_eq!(results[8].rows, vec![vec![ScalarValue::Int(100)]]);
    }

    #[test]
    fn sequence_cycle_wraps_at_bounds() {
        let results = run_batch(&[
            "CREATE SEQUENCE seq_cycle START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 2 CYCLE",
            "SELECT nextval('seq_cycle'), nextval('seq_cycle'), nextval('seq_cycle'), nextval('seq_cycle')",
        ]);
        assert_eq!(
            results[1].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Int(2),
                ScalarValue::Int(1),
                ScalarValue::Int(2)
            ]]
        );
    }

    #[test]
    fn setval_controls_nextval_behavior() {
        let results = run_batch(&[
            "CREATE SEQUENCE seq_setval START 5 INCREMENT 2",
            "SELECT setval('seq_setval', 20), nextval('seq_setval')",
            "SELECT setval('seq_setval', 30, false), nextval('seq_setval')",
        ]);
        assert_eq!(
            results[1].rows,
            vec![vec![ScalarValue::Int(20), ScalarValue::Int(22)]]
        );
        assert_eq!(
            results[2].rows,
            vec![vec![ScalarValue::Int(30), ScalarValue::Int(30)]]
        );
    }

    #[test]
    fn create_unique_index_enforces_uniqueness() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8, email text)", &[]);
            run_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'a@example.com')", &[]);

            let duplicate = parse_statement("INSERT INTO users VALUES (2, 'a@example.com')")
                .expect("statement should parse");
            let plan = plan_statement(duplicate).expect("statement should plan");
            let err = block_on(execute_planned_query(&plan, &[])).expect_err("duplicate should fail");
            assert!(err.message.contains("unique constraint"));
        });
    }

    #[test]
    fn create_unique_index_failure_is_atomic() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8, email text)", &[]);
            run_statement(
                "INSERT INTO users VALUES (1, 'a@example.com'), (2, 'a@example.com')",
                &[],
            );

            let duplicate_index =
                parse_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)")
                    .expect("statement should parse");
            let duplicate_index_plan =
                plan_statement(duplicate_index).expect("statement should plan");
            let err = block_on(execute_planned_query(&duplicate_index_plan, &[]))
                .expect_err("unique index build should fail over duplicate rows");
            assert!(err.message.contains("unique constraint"));

            let table = crate::catalog::with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&["users".to_string()], &SearchPath::default())
                    .cloned()
            })
            .expect("table should resolve");
            assert!(
                !table
                    .key_constraints()
                    .iter()
                    .any(|constraint| constraint.name.as_deref() == Some("uq_users_email"))
            );
            assert!(
                !table
                    .indexes()
                    .iter()
                    .any(|index| index.name == "uq_users_email")
            );
        });
    }

    #[test]
    fn insert_on_conflict_do_nothing_skips_conflicting_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, email text)",
            "INSERT INTO users VALUES (1, 'a')",
            "INSERT INTO users VALUES (1, 'dup'), (2, 'b') ON CONFLICT DO NOTHING RETURNING id",
            "SELECT id FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].rows_affected, 1);
        assert_eq!(results[2].rows, vec![vec![ScalarValue::Int(2)]]);
        assert_eq!(
            results[3].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn insert_on_conflict_target_matches_constraint() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, email text UNIQUE)",
            "INSERT INTO users VALUES (1, 'a')",
            "INSERT INTO users VALUES (1, 'dup'), (2, 'b') ON CONFLICT (id) DO NOTHING RETURNING id",
            "SELECT id FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].rows_affected, 1);
        assert_eq!(results[2].rows, vec![vec![ScalarValue::Int(2)]]);
        assert_eq!(
            results[3].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn insert_on_conflict_target_does_not_hide_other_unique_violations() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE users (id int8 PRIMARY KEY, email text UNIQUE)",
                &[],
            );
            run_statement("INSERT INTO users VALUES (1, 'a')", &[]);

            let statement =
                parse_statement("INSERT INTO users VALUES (2, 'a') ON CONFLICT (id) DO NOTHING")
                    .expect("statement should parse");
            let plan = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&plan, &[]))
                .expect_err("non-target unique conflict should fail");
            assert!(err.message.contains("unique constraint"));
        });
    }

    #[test]
    fn insert_on_conflict_do_update_updates_conflicting_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, email text UNIQUE, name text)",
            "INSERT INTO users VALUES (1, 'a@example.com', 'old')",
            "INSERT INTO users VALUES (1, 'a@example.com', 'new') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id, name",
            "SELECT id, name FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].rows_affected, 1);
        assert_eq!(
            results[2].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("new".to_string())
            ]]
        );
        assert_eq!(
            results[3].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("new".to_string())
            ]]
        );
    }

    #[test]
    fn insert_on_conflict_do_update_honors_where_clause() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, name text)",
            "INSERT INTO users VALUES (1, 'old')",
            "INSERT INTO users VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.name = 'missing' RETURNING id",
            "SELECT id, name FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].rows_affected, 0);
        assert!(results[2].rows.is_empty());
        assert_eq!(
            results[3].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("old".to_string())
            ]]
        );
    }

    #[test]
    fn insert_on_conflict_on_constraint_and_alias_where_works() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8, email text, name text, CONSTRAINT users_pkey PRIMARY KEY (id), CONSTRAINT users_email_key UNIQUE (email))",
            "INSERT INTO users VALUES (1, 'a@example.com', 'old')",
            "INSERT INTO users AS u VALUES (1, 'a@example.com', 'new') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE u.id = 1 RETURNING name",
            "SELECT id, email, name FROM users ORDER BY 1",
        ]);

        assert_eq!(
            results[2].rows,
            vec![vec![ScalarValue::Text("new".to_string())]]
        );
        assert_eq!(
            results[3].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a@example.com".to_string()),
                ScalarValue::Text("new".to_string())
            ]]
        );
    }

    #[test]
    fn insert_enforces_not_null_columns() {
        let results = run_batch(&["CREATE TABLE users (id int8 NOT NULL, name text)"]);
        assert_eq!(results[0].command_tag, "CREATE TABLE");

        let err = with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 NOT NULL, name text)", &[]);
            let statement = parse_statement("INSERT INTO users (name) VALUES ('missing id')")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            block_on(execute_planned_query(&planned, &[])).expect_err("insert should fail")
        });
        assert!(err.message.contains("not-null"));
    }

    #[test]
    fn enforces_primary_key_and_unique_constraints() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE users (id int8 PRIMARY KEY, email text UNIQUE)",
                &[],
            );
            run_statement("INSERT INTO users VALUES (1, 'a@example.com')", &[]);

            let dup_pk = parse_statement("INSERT INTO users VALUES (1, 'b@example.com')")
                .expect("statement should parse");
            let dup_pk_plan = plan_statement(dup_pk).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&dup_pk_plan, &[])).expect_err("duplicate pk should fail");
            assert!(err.message.contains("primary key"));

            let dup_unique = parse_statement("INSERT INTO users VALUES (2, 'a@example.com')")
                .expect("statement should parse");
            let dup_unique_plan = plan_statement(dup_unique).expect("statement should plan");
            let err = block_on(execute_planned_query(&dup_unique_plan, &[]))
                .expect_err("duplicate unique should fail");
            assert!(err.message.contains("unique constraint"));
        });
    }

    #[test]
    fn enforces_multi_column_table_constraints() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE membership (user_id int8, org_id int8, email text, PRIMARY KEY (user_id, org_id), UNIQUE (email))",
                &[],
            );
            run_statement(
                "INSERT INTO membership VALUES (1, 10, 'a@example.com')",
                &[],
            );

            let dup_pk = parse_statement("INSERT INTO membership VALUES (1, 10, 'b@example.com')")
                .expect("statement should parse");
            let dup_pk_plan = plan_statement(dup_pk).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&dup_pk_plan, &[])).expect_err("duplicate pk should fail");
            assert!(err.message.contains("primary key"));

            let dup_unique =
                parse_statement("INSERT INTO membership VALUES (1, 11, 'a@example.com')")
                    .expect("statement should parse");
            let dup_unique_plan = plan_statement(dup_unique).expect("statement should plan");
            let err = block_on(execute_planned_query(&dup_unique_plan, &[]))
                .expect_err("duplicate unique should fail");
            assert!(err.message.contains("unique constraint"));
        });
    }

    #[test]
    fn enforces_foreign_key_on_insert_and_update() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id))",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1)", &[]);
            run_statement("INSERT INTO children VALUES (10, 1)", &[]);

            let bad_insert = parse_statement("INSERT INTO children VALUES (11, 999)")
                .expect("statement should parse");
            let bad_insert_plan = plan_statement(bad_insert).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&bad_insert_plan, &[])).expect_err("fk insert should fail");
            assert!(err.message.contains("foreign key"));

            let bad_update = parse_statement("UPDATE children SET parent_id = 999 WHERE id = 10")
                .expect("statement should parse");
            let bad_update_plan = plan_statement(bad_update).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&bad_update_plan, &[])).expect_err("fk update should fail");
            assert!(err.message.contains("foreign key"));
        });
    }

    #[test]
    fn rejects_delete_of_referenced_parent_rows() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id))",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1)", &[]);
            run_statement("INSERT INTO children VALUES (10, 1)", &[]);

            let statement = parse_statement("DELETE FROM parents WHERE id = 1")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("delete should fail");
            assert!(err.message.contains("violates foreign key"));
        });
    }

    #[test]
    fn enforces_composite_foreign_key() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE parents (a int8, b int8, PRIMARY KEY (a, b))",
                &[],
            );
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, a int8, b int8, CONSTRAINT fk_ab FOREIGN KEY (a, b) REFERENCES parents(a, b))",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1, 10)", &[]);
            run_statement("INSERT INTO children VALUES (100, 1, 10)", &[]);

            let bad_insert = parse_statement("INSERT INTO children VALUES (101, 1, 11)")
                .expect("statement should parse");
            let bad_insert_plan = plan_statement(bad_insert).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&bad_insert_plan, &[])).expect_err("fk insert should fail");
            assert!(err.message.contains("foreign key"));
        });
    }

    #[test]
    fn cascades_delete_to_referencing_rows() {
        let results = run_batch(&[
            "CREATE TABLE parents (id int8 PRIMARY KEY)",
            "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id) ON DELETE CASCADE)",
            "INSERT INTO parents VALUES (1), (2)",
            "INSERT INTO children VALUES (10, 1), (11, 1), (12, 2)",
            "DELETE FROM parents WHERE id = 1",
            "SELECT id, parent_id FROM children ORDER BY 1",
        ]);

        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![vec![ScalarValue::Int(12), ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn sets_referencing_columns_to_null_on_delete_set_null() {
        let results = run_batch(&[
            "CREATE TABLE parents (id int8 PRIMARY KEY)",
            "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id) ON DELETE SET NULL)",
            "INSERT INTO parents VALUES (1)",
            "INSERT INTO children VALUES (10, 1)",
            "DELETE FROM parents WHERE id = 1",
            "SELECT id, parent_id FROM children",
        ]);

        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![vec![ScalarValue::Int(10), ScalarValue::Null]]
        );
    }

    #[test]
    fn cascades_update_to_referencing_rows() {
        let results = run_batch(&[
            "CREATE TABLE parents (id int8 PRIMARY KEY)",
            "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id) ON UPDATE CASCADE)",
            "INSERT INTO parents VALUES (1)",
            "INSERT INTO children VALUES (10, 1)",
            "UPDATE parents SET id = 2 WHERE id = 1",
            "SELECT * FROM children",
        ]);

        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![vec![ScalarValue::Int(10), ScalarValue::Int(2)]]
        );
    }

    #[test]
    fn sets_referencing_columns_to_null_on_update_set_null() {
        let results = run_batch(&[
            "CREATE TABLE parents (id int8 PRIMARY KEY)",
            "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id) ON UPDATE SET NULL)",
            "INSERT INTO parents VALUES (1)",
            "INSERT INTO children VALUES (10, 1)",
            "UPDATE parents SET id = 2 WHERE id = 1",
            "SELECT * FROM children",
        ]);

        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(
            results[5].rows,
            vec![vec![ScalarValue::Int(10), ScalarValue::Null]]
        );
    }

    #[test]
    fn rejects_update_of_referenced_parent_rows_by_default() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id))",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1)", &[]);
            run_statement("INSERT INTO children VALUES (10, 1)", &[]);

            let statement = parse_statement("UPDATE parents SET id = 2 WHERE id = 1")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("update should fail");
            assert!(err.message.contains("violates foreign key"));
        });
    }

    #[test]
    fn alter_table_drop_constraint_removes_enforcement() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE users (id int8 PRIMARY KEY, email text, CONSTRAINT uq_email UNIQUE (email))",
                &[],
            );
            run_statement("INSERT INTO users VALUES (1, 'a@example.com')", &[]);

            let duplicate = parse_statement("INSERT INTO users VALUES (2, 'a@example.com')")
                .expect("statement should parse");
            let duplicate_plan = plan_statement(duplicate).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&duplicate_plan, &[])).expect_err("duplicate should fail");
            assert!(err.message.contains("unique constraint"));

            run_statement("ALTER TABLE users DROP CONSTRAINT uq_email", &[]);
            let ok = run_statement("INSERT INTO users VALUES (2, 'a@example.com')", &[]);
            assert_eq!(ok.command_tag, "INSERT");
            assert_eq!(ok.rows_affected, 1);
        });
    }

    #[test]
    fn alter_table_add_unique_constraint_enforces_values() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY, email text)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'a@example.com')", &[]);
            run_statement(
                "ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)",
                &[],
            );

            let duplicate = parse_statement("INSERT INTO users VALUES (2, 'a@example.com')")
                .expect("statement should parse");
            let duplicate_plan = plan_statement(duplicate).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&duplicate_plan, &[])).expect_err("duplicate should fail");
            assert!(err.message.contains("unique constraint"));
        });
    }

    #[test]
    fn alter_table_add_foreign_key_constraint_validates_existing_rows() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8)",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1)", &[]);
            run_statement("INSERT INTO children VALUES (10, 999)", &[]);

            let statement = parse_statement(
                "ALTER TABLE children ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parents(id)",
            )
            .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("alter should fail");
            assert!(err.message.contains("foreign key"));
        });
    }

    #[test]
    fn enforces_check_constraints() {
        with_isolated_state(|| {
            run_statement(
                "CREATE TABLE metrics (id int8 PRIMARY KEY, score int8 CHECK (score >= 0))",
                &[],
            );
            run_statement("INSERT INTO metrics VALUES (1, 5)", &[]);

            let bad_insert = parse_statement("INSERT INTO metrics VALUES (2, -1)")
                .expect("statement should parse");
            let bad_insert_plan = plan_statement(bad_insert).expect("statement should plan");
            let err = block_on(execute_planned_query(&bad_insert_plan, &[])).expect_err("check should fail");
            assert!(err.message.contains("CHECK constraint"));

            let bad_update = parse_statement("UPDATE metrics SET score = -2 WHERE id = 1")
                .expect("statement should parse");
            let bad_update_plan = plan_statement(bad_update).expect("statement should plan");
            let err = block_on(execute_planned_query(&bad_update_plan, &[])).expect_err("check should fail");
            assert!(err.message.contains("CHECK constraint"));
        });
    }

    #[test]
    fn updates_rows_with_where_clause() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users (id, name) VALUES (1, 'alpha'), (2, 'bravo')",
            "UPDATE users SET name = upper(name) WHERE id = 2",
            "SELECT id, name FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].command_tag, "UPDATE");
        assert_eq!(results[2].rows_affected, 1);
        assert_eq!(
            results[3].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("BRAVO".to_string())]
            ]
        );
    }

    #[test]
    fn deletes_rows_with_where_clause() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, active boolean NOT NULL)",
            "INSERT INTO users VALUES (1, true), (2, false), (3, false)",
            "DELETE FROM users WHERE active = false",
            "SELECT count(*) FROM users",
        ]);

        assert_eq!(results[2].command_tag, "DELETE");
        assert_eq!(results[2].rows_affected, 2);
        assert_eq!(results[3].rows, vec![vec![ScalarValue::Int(1)]]);
    }

    #[test]
    fn insert_select_materializes_query_rows() {
        let results = run_batch(&[
            "CREATE TABLE staging (id int8 NOT NULL, name text)",
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO staging VALUES (1, 'alpha'), (2, 'bravo')",
            "INSERT INTO users (id, name) SELECT id, upper(name) FROM staging WHERE id > 1",
            "SELECT id, name FROM users ORDER BY 1",
        ]);

        assert_eq!(results[3].command_tag, "INSERT");
        assert_eq!(results[3].rows_affected, 1);
        assert_eq!(
            results[4].rows,
            vec![vec![
                ScalarValue::Int(2),
                ScalarValue::Text("BRAVO".to_string())
            ]]
        );
    }

    #[test]
    fn update_from_applies_joined_source_values() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, team_id int8, label text)",
            "CREATE TABLE teams (id int8 PRIMARY KEY, label text)",
            "INSERT INTO users VALUES (1, 10, 'u1'), (2, 20, 'u2'), (3, 30, 'u3')",
            "INSERT INTO teams VALUES (10, 'red'), (20, 'blue')",
            "UPDATE users SET label = t.label FROM teams t WHERE users.team_id = t.id RETURNING id, label",
            "SELECT id, label FROM users ORDER BY 1",
        ]);

        assert_eq!(results[4].rows_affected, 2);
        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("red".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("blue".to_string())]
            ]
        );
        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("red".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("blue".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Text("u3".to_string())]
            ]
        );
    }

    #[test]
    fn delete_using_applies_join_predicates() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, team_id int8)",
            "CREATE TABLE teams (id int8 PRIMARY KEY, active boolean)",
            "INSERT INTO users VALUES (1, 10), (2, 20), (3, 30)",
            "INSERT INTO teams VALUES (10, true), (20, false)",
            "DELETE FROM users USING teams t WHERE users.team_id = t.id AND t.active = false RETURNING id",
            "SELECT id FROM users ORDER BY 1",
        ]);

        assert_eq!(results[4].rows_affected, 1);
        assert_eq!(results[4].rows, vec![vec![ScalarValue::Int(2)]]);
        assert_eq!(
            results[5].rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(3)]]
        );
    }

    #[test]
    fn insert_returning_projects_inserted_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users VALUES (1, 'a'), (2, 'b') RETURNING id, upper(name) AS u",
        ]);

        assert_eq!(results[1].columns, vec!["id".to_string(), "u".to_string()]);
        assert_eq!(
            results[1].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("A".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("B".to_string())]
            ]
        );
    }

    #[test]
    fn update_returning_projects_updated_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users VALUES (1, 'a'), (2, 'b')",
            "UPDATE users SET name = upper(name) WHERE id = 2 RETURNING *",
        ]);

        assert_eq!(
            results[2].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            results[2].rows,
            vec![vec![
                ScalarValue::Int(2),
                ScalarValue::Text("B".to_string())
            ]]
        );
    }

    #[test]
    fn delete_returning_projects_deleted_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, active boolean NOT NULL)",
            "INSERT INTO users VALUES (1, true), (2, false)",
            "DELETE FROM users WHERE active = false RETURNING id",
        ]);

        assert_eq!(results[2].columns, vec!["id".to_string()]);
        assert_eq!(results[2].rows, vec![vec![ScalarValue::Int(2)]]);
        assert_eq!(results[2].rows_affected, 1);
    }

    #[test]
    fn drop_table_removes_relation() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 NOT NULL)", &[]);
            run_statement("DROP TABLE users", &[]);

            let statement =
                parse_statement("SELECT id FROM users").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("select should fail");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_table_rejects_if_referenced_by_foreign_key() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id))",
                &[],
            );

            let statement = parse_statement("DROP TABLE parents").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("drop should fail");
            assert!(err.message.contains("depends on it"));
        });
    }

    #[test]
    fn drop_index_respects_restrict_and_cascade() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 PRIMARY KEY, email text)", &[]);
            run_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'x@example.com')", &[]);

            let restrict = parse_statement("DROP INDEX uq_users_email RESTRICT")
                .expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop restrict should fail for constraint-backed index");
            assert!(err.message.contains("depends on it"));

            run_statement("DROP INDEX uq_users_email CASCADE", &[]);
            let inserted = run_statement("INSERT INTO users VALUES (2, 'x@example.com')", &[]);
            assert_eq!(inserted.rows_affected, 1);
        });
    }

    #[test]
    fn drop_sequence_respects_default_dependencies() {
        with_isolated_state(|| {
            run_statement("CREATE SEQUENCE seq_users_id", &[]);
            run_statement(
                "CREATE TABLE users (id int8 DEFAULT nextval('seq_users_id'))",
                &[],
            );

            let restrict = parse_statement("DROP SEQUENCE seq_users_id RESTRICT")
                .expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop sequence restrict should fail for dependent defaults");
            assert!(err.message.contains("depends on it"));

            run_statement("DROP SEQUENCE seq_users_id CASCADE", &[]);

            let table = with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&["users".to_string()], &SearchPath::default())
                    .cloned()
            })
            .expect("table should resolve");
            let id_column = table
                .columns()
                .iter()
                .find(|column| column.name() == "id")
                .expect("id column should exist");
            assert!(id_column.default().is_none());

            let query =
                parse_statement("SELECT nextval('seq_users_id')").expect("statement should parse");
            let plan = plan_statement(query).expect("statement should plan");
            let err = block_on(execute_planned_query(&plan, &[])).expect_err("sequence should be dropped");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_sequence_respects_view_dependencies() {
        with_isolated_state(|| {
            run_statement("CREATE SEQUENCE seq_view_id", &[]);
            run_statement(
                "CREATE VIEW v_seq AS SELECT nextval('seq_view_id') AS id",
                &[],
            );

            let restrict = parse_statement("DROP SEQUENCE seq_view_id RESTRICT")
                .expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop sequence restrict should fail for dependent view");
            assert!(err.message.contains("depends on it"));

            run_statement("DROP SEQUENCE seq_view_id CASCADE", &[]);
            let statement = parse_statement("SELECT * FROM v_seq").expect("statement should parse");
            let err = plan_statement(statement).expect_err("view should be dropped by cascade");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_sequence_cascade_drops_transitive_view_dependencies() {
        with_isolated_state(|| {
            run_statement("CREATE SEQUENCE seq_view_id", &[]);
            run_statement(
                "CREATE VIEW v_base AS SELECT nextval('seq_view_id') AS id",
                &[],
            );
            run_statement("CREATE VIEW v_child AS SELECT id FROM v_base", &[]);

            run_statement("DROP SEQUENCE seq_view_id CASCADE", &[]);

            let base = parse_statement("SELECT * FROM v_base").expect("statement should parse");
            let base_err = plan_statement(base).expect_err("base view should be dropped");
            assert!(base_err.message.contains("does not exist"));

            let child = parse_statement("SELECT * FROM v_child").expect("statement should parse");
            let child_err = plan_statement(child).expect_err("child view should be dropped");
            assert!(child_err.message.contains("does not exist"));
        });
    }

    #[test]
    fn truncate_restrict_and_cascade_follow_fk_dependencies() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_id int8 REFERENCES parents(id))",
                &[],
            );
            run_statement("INSERT INTO parents VALUES (1), (2)", &[]);
            run_statement("INSERT INTO children VALUES (10, 1), (11, 2)", &[]);

            let restrict = parse_statement("TRUNCATE parents").expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("truncate restrict should fail on referenced relation");
            assert!(err.message.contains("depends on it"));

            run_statement("TRUNCATE parents CASCADE", &[]);
            let parents = run_statement("SELECT count(*) FROM parents", &[]);
            let children = run_statement("SELECT count(*) FROM children", &[]);
            assert_eq!(parents.rows, vec![vec![ScalarValue::Int(0)]]);
            assert_eq!(children.rows, vec![vec![ScalarValue::Int(0)]]);
        });
    }

    #[test]
    fn drop_schema_considers_sequences_for_restrict_and_cascade() {
        with_isolated_state(|| {
            run_statement("CREATE SCHEMA app", &[]);
            run_statement("CREATE SEQUENCE app.seq1", &[]);

            let restrict =
                parse_statement("DROP SCHEMA app RESTRICT").expect("statement should parse");
            let restrict_plan = plan_statement(restrict).expect("statement should plan");
            let err = block_on(execute_planned_query(&restrict_plan, &[]))
                .expect_err("drop schema restrict should fail when sequence exists");
            assert!(err.message.contains("is not empty"));

            run_statement("DROP SCHEMA app CASCADE", &[]);
            let lookup = with_catalog_read(|catalog| catalog.schema("app").is_some());
            assert!(!lookup);

            let query =
                parse_statement("SELECT nextval('app.seq1')").expect("statement should parse");
            let plan = plan_statement(query).expect("statement should plan");
            let err = block_on(execute_planned_query(&plan, &[])).expect_err("sequence should be removed");
            assert!(err.message.contains("does not exist"));
        });
    }

    #[test]
    fn drop_schema_cascade_drops_views_depending_on_schema_sequence() {
        with_isolated_state(|| {
            run_statement("CREATE SCHEMA app", &[]);
            run_statement("CREATE SEQUENCE app.seq1", &[]);
            run_statement(
                "CREATE TABLE users (id int8 DEFAULT nextval('app.seq1'))",
                &[],
            );
            run_statement("CREATE VIEW v_seq AS SELECT nextval('app.seq1') AS id", &[]);

            run_statement("DROP SCHEMA app CASCADE", &[]);

            let view_query = parse_statement("SELECT * FROM v_seq").expect("statement parses");
            let view_err = plan_statement(view_query).expect_err("view should be dropped");
            assert!(view_err.message.contains("does not exist"));

            let default_cleared = with_catalog_read(|catalog| {
                catalog
                    .table("public", "users")
                    .and_then(|table| {
                        table
                            .columns()
                            .iter()
                            .find(|column| column.name() == "id")
                            .and_then(|column| column.default())
                    })
                    .is_none()
            });
            assert!(default_cleared);
        });
    }

    #[test]
    fn drop_column_rejects_if_referenced_by_foreign_key() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE parents (id int8 PRIMARY KEY, code text)", &[]);
            run_statement(
                "CREATE TABLE children (id int8 PRIMARY KEY, parent_code text REFERENCES parents(code))",
                &[],
            );

            let statement = parse_statement("ALTER TABLE parents DROP COLUMN code")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("alter should fail");
            assert!(err.message.contains("referenced by foreign key"));
        });
    }

    #[test]
    fn alter_table_add_column_updates_existing_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL)",
            "INSERT INTO users VALUES (1), (2)",
            "ALTER TABLE users ADD COLUMN name text",
            "UPDATE users SET name = 'x' WHERE id = 1",
            "SELECT id, name FROM users ORDER BY 1",
        ]);

        assert_eq!(results[2].command_tag, "ALTER TABLE");
        assert_eq!(
            results[4].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("x".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Null]
            ]
        );
    }

    #[test]
    fn insert_uses_column_default_for_missing_values() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 PRIMARY KEY, tag text DEFAULT 'new')",
            "INSERT INTO users (id) VALUES (1), (2)",
            "SELECT * FROM users ORDER BY 1",
        ]);

        assert_eq!(
            results[2].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("new".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("new".to_string())]
            ]
        );
    }

    #[test]
    fn alter_table_add_not_null_column_rejects_non_empty_table() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 NOT NULL)", &[]);
            run_statement("INSERT INTO users VALUES (1)", &[]);

            let statement = parse_statement("ALTER TABLE users ADD COLUMN name text NOT NULL")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("alter should fail");
            assert!(err.message.contains("NOT NULL"));
        });
    }

    #[test]
    fn alter_table_add_not_null_column_with_default_backfills_rows() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL)",
            "INSERT INTO users VALUES (1), (2)",
            "ALTER TABLE users ADD COLUMN tag text NOT NULL DEFAULT 'active'",
            "SELECT * FROM users ORDER BY 1",
        ]);

        assert_eq!(
            results[3].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("active".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("active".to_string())]
            ]
        );
    }

    #[test]
    fn alter_table_drop_column_removes_data_slot() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text, age int8)",
            "INSERT INTO users VALUES (1, 'a', 42)",
            "ALTER TABLE users DROP COLUMN age",
            "SELECT * FROM users",
        ]);

        assert_eq!(results[2].command_tag, "ALTER TABLE");
        assert_eq!(
            results[3].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            results[3].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a".to_string())
            ]]
        );
    }

    #[test]
    fn alter_table_rename_column_changes_lookup_name() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 NOT NULL, note text)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'x')", &[]);
            run_statement("ALTER TABLE users RENAME COLUMN note TO details", &[]);
            let result = run_statement("SELECT details FROM users", &[]);
            assert_eq!(result.rows, vec![vec![ScalarValue::Text("x".to_string())]]);

            let statement =
                parse_statement("SELECT note FROM users").expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err =
                block_on(execute_planned_query(&planned, &[])).expect_err("old column name should fail");
            assert!(err.message.contains("unknown column"));
        });
    }

    #[test]
    fn alter_table_set_and_drop_not_null() {
        with_isolated_state(|| {
            run_statement("CREATE TABLE users (id int8 NOT NULL, note text)", &[]);
            run_statement("INSERT INTO users VALUES (1, 'a')", &[]);
            run_statement("ALTER TABLE users ALTER COLUMN note SET NOT NULL", &[]);

            let statement = parse_statement("INSERT INTO users VALUES (2, NULL)")
                .expect("statement should parse");
            let planned = plan_statement(statement).expect("statement should plan");
            let err = block_on(execute_planned_query(&planned, &[])).expect_err("insert should fail");
            assert!(err.message.contains("does not allow null values"));

            run_statement("ALTER TABLE users ALTER COLUMN note DROP NOT NULL", &[]);
            let ok = run_statement("INSERT INTO users VALUES (2, NULL)", &[]);
            assert_eq!(ok.command_tag, "INSERT");
            assert_eq!(ok.rows_affected, 1);
        });
    }

    #[test]
    fn alter_table_set_and_drop_default() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, note text)",
            "ALTER TABLE users ALTER COLUMN note SET DEFAULT 'x'",
            "INSERT INTO users (id) VALUES (1)",
            "ALTER TABLE users ALTER COLUMN note DROP DEFAULT",
            "INSERT INTO users (id) VALUES (2)",
            "SELECT id, note FROM users ORDER BY 1",
        ]);

        assert_eq!(
            results[5].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("x".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Null]
            ]
        );
    }

    #[test]
    fn selects_all_columns_with_wildcard() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users VALUES (2, 'b'), (1, 'a')",
            "SELECT * FROM users ORDER BY 1",
        ]);

        assert_eq!(
            results[2].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            results[2].rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())]
            ]
        );
    }

    #[test]
    fn selects_all_columns_from_subquery_with_wildcard() {
        let result = run("SELECT * FROM (SELECT 1 AS id, 'x' AS tag) s");
        assert_eq!(result.columns, vec!["id".to_string(), "tag".to_string()]);
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("x".to_string())
            ]]
        );
    }

    #[test]
    fn selects_wildcard_over_join_with_using() {
        let results = run_batch(&[
            "CREATE TABLE a (id int8 NOT NULL, v text)",
            "CREATE TABLE b (id int8 NOT NULL, w text)",
            "INSERT INTO a VALUES (1, 'av'), (2, 'ax')",
            "INSERT INTO b VALUES (1, 'bw'), (3, 'bx')",
            "SELECT * FROM a INNER JOIN b USING (id) ORDER BY 1",
        ]);

        assert_eq!(
            results[4].columns,
            vec!["id".to_string(), "v".to_string(), "w".to_string()]
        );
        assert_eq!(
            results[4].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("av".to_string()),
                ScalarValue::Text("bw".to_string())
            ]]
        );
    }

    #[test]
    fn supports_mixed_wildcard_and_expression_targets() {
        let results = run_batch(&[
            "CREATE TABLE users (id int8 NOT NULL, name text)",
            "INSERT INTO users VALUES (1, 'alpha')",
            "SELECT *, upper(name) AS name_upper FROM users",
        ]);

        assert_eq!(
            results[2].columns,
            vec![
                "id".to_string(),
                "name".to_string(),
                "name_upper".to_string()
            ]
        );
        assert_eq!(
            results[2].rows,
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("alpha".to_string()),
                ScalarValue::Text("ALPHA".to_string())
            ]]
        );
    }

    // === Phase 1 roadmap tests ===

    // 1.6.2 Math functions
    #[test]
    fn math_functions_ceil_floor_round() {
        let r = run("SELECT ceil(4.3), floor(4.7), round(4.567, 2)");
        assert_eq!(r.rows[0][0], ScalarValue::Float(5.0));
        assert_eq!(r.rows[0][1], ScalarValue::Float(4.0));
        assert_eq!(r.rows[0][2], ScalarValue::Float(4.57));
    }

    #[test]
    fn math_functions_power_sqrt_exp_ln() {
        let r = run("SELECT power(2, 10), sqrt(144.0), exp(0), ln(1)");
        assert_eq!(r.rows[0][0], ScalarValue::Float(1024.0));
        assert_eq!(r.rows[0][1], ScalarValue::Float(12.0));
        assert_eq!(r.rows[0][2], ScalarValue::Float(1.0));
        assert_eq!(r.rows[0][3], ScalarValue::Float(0.0));
    }

    #[test]
    fn math_functions_trig() {
        let r = run("SELECT sin(0), cos(0), pi()");
        assert_eq!(r.rows[0][0], ScalarValue::Float(0.0));
        assert_eq!(r.rows[0][1], ScalarValue::Float(1.0));
        assert_eq!(r.rows[0][2], ScalarValue::Float(std::f64::consts::PI));
    }

    #[test]
    fn math_functions_sign_abs_mod() {
        let r = run("SELECT sign(-5), sign(3), abs(-7), mod(17, 5)");
        assert_eq!(r.rows[0][0], ScalarValue::Int(-1));
        assert_eq!(r.rows[0][1], ScalarValue::Int(1));
        assert_eq!(r.rows[0][2], ScalarValue::Int(7));
        assert_eq!(r.rows[0][3], ScalarValue::Int(2));
    }

    #[test]
    fn math_functions_gcd_lcm_div() {
        let r = run("SELECT gcd(12, 8), lcm(4, 6), div(17, 5)");
        assert_eq!(r.rows[0][0], ScalarValue::Int(4));
        assert_eq!(r.rows[0][1], ScalarValue::Int(12));
        assert_eq!(r.rows[0][2], ScalarValue::Int(3));
    }

    // 1.6.1 String functions
    #[test]
    fn string_functions_initcap_repeat_reverse() {
        let r = run("SELECT initcap('hello world'), repeat('ab', 3), reverse('abc')");
        assert_eq!(r.rows[0][0], ScalarValue::Text("Hello World".to_string()));
        assert_eq!(r.rows[0][1], ScalarValue::Text("ababab".to_string()));
        assert_eq!(r.rows[0][2], ScalarValue::Text("cba".to_string()));
    }

    #[test]
    fn string_functions_translate_split_part_strpos() {
        let r = run("SELECT translate('hello', 'helo', 'HELO'), split_part('a.b.c', '.', 2), strpos('hello', 'llo')");
        assert_eq!(r.rows[0][0], ScalarValue::Text("HELLO".to_string()));
        assert_eq!(r.rows[0][1], ScalarValue::Text("b".to_string()));
        assert_eq!(r.rows[0][2], ScalarValue::Int(3));
    }

    #[test]
    fn string_functions_lpad_rpad() {
        let r = run("SELECT lpad('hi', 5, 'xy'), rpad('hi', 5, 'xy')");
        assert_eq!(r.rows[0][0], ScalarValue::Text("xyxhi".to_string()));
        assert_eq!(r.rows[0][1], ScalarValue::Text("hixyx".to_string()));
    }

    // 1.6.5 Aggregate functions
    #[test]
    fn aggregate_string_agg() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8, name text)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "SELECT string_agg(name, ', ') FROM t",
        ]);
        assert_eq!(results[2].rows[0][0], ScalarValue::Text("a, b, c".to_string()));
    }

    #[test]
    fn aggregate_array_agg() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8, val int8)",
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
            "SELECT array_agg(val) FROM t",
        ]);
        assert_eq!(results[2].rows[0][0], ScalarValue::Text("{10,20,30}".to_string()));
    }

    #[test]
    fn aggregate_bool_and_or() {
        let results = run_batch(&[
            "CREATE TABLE t (v bool)",
            "INSERT INTO t VALUES (true), (true), (false)",
            "SELECT bool_and(v), bool_or(v) FROM t",
        ]);
        assert_eq!(results[2].rows[0][0], ScalarValue::Bool(false));
        assert_eq!(results[2].rows[0][1], ScalarValue::Bool(true));
    }

    #[test]
    fn aggregate_stddev_variance() {
        let results = run_batch(&[
            "CREATE TABLE t (v float8)",
            "INSERT INTO t VALUES (2.0), (4.0), (4.0), (4.0), (5.0), (5.0), (7.0), (9.0)",
            "SELECT variance(v), stddev(v) FROM t",
        ]);
        // variance and stddev should be non-null floats
        match &results[2].rows[0][0] {
            ScalarValue::Float(v) => assert!(*v > 0.0),
            other => panic!("expected numeric, got {:?}", other),
        }
    }

    #[test]
    fn aggregate_statistical_and_ordered_set() {
        let results = run_batch(&[
            "CREATE TABLE stats_test (x FLOAT, y FLOAT)",
            "INSERT INTO stats_test VALUES (1,2),(2,4),(3,5),(4,4),(5,5)",
            "SELECT corr(y,x), covar_pop(y,x), covar_samp(y,x), regr_slope(y,x), \
             regr_intercept(y,x), regr_r2(y,x), regr_count(y,x) FROM stats_test",
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM stats_test",
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM stats_test",
            "CREATE TABLE mode_test (x INT)",
            "INSERT INTO mode_test VALUES (1),(1),(2),(3)",
            "SELECT mode() WITHIN GROUP (ORDER BY x) FROM mode_test",
        ]);
        let row = &results[2].rows[0];
        let assert_float = |value: &ScalarValue, expected: f64| match value {
            ScalarValue::Float(v) => assert!((v - expected).abs() < 1e-9),
            other => panic!("expected float, got {:?}", other),
        };
        assert_float(&row[0], 0.7745966692414834);
        assert_float(&row[1], 1.2);
        assert_float(&row[2], 1.5);
        assert_float(&row[3], 0.6);
        assert_float(&row[4], 2.2);
        assert_float(&row[5], 0.6);
        assert_eq!(row[6], ScalarValue::Int(5));
        match &results[3].rows[0][0] {
            ScalarValue::Float(v) => assert!((*v - 3.0).abs() < 1e-9),
            other => panic!("expected float, got {:?}", other),
        }
        match &results[4].rows[0][0] {
            ScalarValue::Float(v) => assert!((*v - 3.0).abs() < 1e-9),
            ScalarValue::Int(v) => assert_eq!(*v, 3),
            other => panic!("expected float, got {:?}", other),
        }
        assert_eq!(results[7].rows[0][0], ScalarValue::Int(1));
    }

    // 1.3 Window functions
    #[test]
    fn window_function_ntile() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8)",
            "INSERT INTO t VALUES (1), (2), (3), (4)",
            "SELECT id, ntile(2) OVER (ORDER BY id) FROM t",
        ]);
        assert_eq!(results[2].rows.len(), 4);
        assert_eq!(results[2].rows[0][1], ScalarValue::Int(1));
        assert_eq!(results[2].rows[2][1], ScalarValue::Int(2));
    }

    #[test]
    fn window_function_first_last_value() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8, val text)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "SELECT id, first_value(val) OVER (ORDER BY id), last_value(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        ]);
        assert_eq!(results[2].rows[0][1], ScalarValue::Text("a".to_string()));
        assert_eq!(results[2].rows[0][2], ScalarValue::Text("c".to_string()));
    }

    #[test]
    fn window_function_percent_rank_cume_dist() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8)",
            "INSERT INTO t VALUES (1), (2), (3), (4)",
            "SELECT id, percent_rank() OVER (ORDER BY id), cume_dist() OVER (ORDER BY id) FROM t",
        ]);
        assert_eq!(results[2].rows[0][1], ScalarValue::Float(0.0));
        assert_eq!(results[2].rows[0][2], ScalarValue::Float(0.25));
    }

    // 1.7 generate_series
    #[test]
    fn generate_series_basic() {
        let r = run("SELECT * FROM generate_series(1, 5)");
        assert_eq!(r.rows.len(), 5);
        assert_eq!(r.rows[0][0], ScalarValue::Int(1));
        assert_eq!(r.rows[4][0], ScalarValue::Int(5));
    }

    #[test]
    fn generate_series_with_step() {
        let r = run("SELECT * FROM generate_series(0, 10, 3)");
        assert_eq!(r.rows.len(), 4);
        assert_eq!(r.rows[0][0], ScalarValue::Int(0));
        assert_eq!(r.rows[3][0], ScalarValue::Int(9));
    }

    // 1.6.8 System info functions
    #[test]
    fn system_info_functions() {
        let r = run("SELECT version(), current_database(), current_schema()");
        match &r.rows[0][0] {
            ScalarValue::Text(v) => assert!(v.contains("Postrust")),
            other => panic!("expected text, got {:?}", other),
        }
        assert_eq!(r.rows[0][1], ScalarValue::Text("postrust".to_string()));
        assert_eq!(r.rows[0][2], ScalarValue::Text("public".to_string()));
    }

    #[test]
    fn make_date_function() {
        let r = run("SELECT make_date(2024, 1, 15)");
        assert_eq!(r.rows[0][0], ScalarValue::Text("2024-01-15".to_string()));
    }

    #[test]
    fn string_to_array_and_array_to_string() {
        let r = run("SELECT string_to_array('a,b,c', ',')");
        assert_eq!(
            r.rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Text("a".to_string()),
                ScalarValue::Text("b".to_string()),
                ScalarValue::Text("c".to_string())
            ])
        );
        let r2 = run("SELECT array_to_string(string_to_array('a,b,c', ','), '|')");
        assert_eq!(r2.rows[0][0], ScalarValue::Text("a|b|c".to_string()));
    }

    #[test]
    fn array_functions_basic() {
        let results = run_batch(&[
            "SELECT array_append(ARRAY[1,2], 3)",
            "SELECT array_prepend(0, ARRAY[1,2])",
            "SELECT array_cat(ARRAY[1], ARRAY[2,3])",
            "SELECT array_remove(ARRAY[1,2,2,3], 2)",
            "SELECT array_replace(ARRAY[1,2,2,3], 2, 9)",
            "SELECT array_position(ARRAY[1,2,3], 2)",
            "SELECT array_position(ARRAY[1,2,3], 5)",
            "SELECT array_positions(ARRAY[1,2,2,3], 2)",
            "SELECT array_positions(ARRAY[1,2,3], 9)",
            "SELECT array_length(ARRAY[1,2,3], 1)",
            "SELECT array_dims(ARRAY[1,2,3])",
            "SELECT array_ndims(ARRAY[1,2,3])",
            "SELECT array_fill(5, ARRAY[3])",
            "SELECT array_upper(ARRAY[1,2,3], 1)",
            "SELECT array_lower(ARRAY[1,2,3], 1)",
            "SELECT cardinality(ARRAY[1,2,3])",
            "SELECT array_to_string(ARRAY[1,NULL,3], ',', 'x')",
            "SELECT string_to_array('a,NULL,b', ',', 'NULL')",
            "SELECT array_dims(ARRAY[])",
            "SELECT array_upper(ARRAY[], 1)",
            "SELECT array_lower(ARRAY[], 1)",
        ]);

        assert_eq!(
            results[0].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Int(1),
                ScalarValue::Int(2),
                ScalarValue::Int(3)
            ])
        );
        assert_eq!(
            results[1].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Int(0),
                ScalarValue::Int(1),
                ScalarValue::Int(2)
            ])
        );
        assert_eq!(
            results[2].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Int(1),
                ScalarValue::Int(2),
                ScalarValue::Int(3)
            ])
        );
        assert_eq!(
            results[3].rows[0][0],
            ScalarValue::Array(vec![ScalarValue::Int(1), ScalarValue::Int(3)])
        );
        assert_eq!(
            results[4].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Int(1),
                ScalarValue::Int(9),
                ScalarValue::Int(9),
                ScalarValue::Int(3)
            ])
        );
        assert_eq!(results[5].rows[0][0], ScalarValue::Int(2));
        assert_eq!(results[6].rows[0][0], ScalarValue::Null);
        assert_eq!(
            results[7].rows[0][0],
            ScalarValue::Array(vec![ScalarValue::Int(2), ScalarValue::Int(3)])
        );
        assert_eq!(results[8].rows[0][0], ScalarValue::Array(Vec::new()));
        assert_eq!(results[9].rows[0][0], ScalarValue::Int(3));
        assert_eq!(
            results[10].rows[0][0],
            ScalarValue::Text("[1:3]".to_string())
        );
        assert_eq!(results[11].rows[0][0], ScalarValue::Int(1));
        assert_eq!(
            results[12].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Int(5),
                ScalarValue::Int(5),
                ScalarValue::Int(5)
            ])
        );
        assert_eq!(results[13].rows[0][0], ScalarValue::Int(3));
        assert_eq!(results[14].rows[0][0], ScalarValue::Int(1));
        assert_eq!(results[15].rows[0][0], ScalarValue::Int(3));
        assert_eq!(
            results[16].rows[0][0],
            ScalarValue::Text("1,x,3".to_string())
        );
        assert_eq!(
            results[17].rows[0][0],
            ScalarValue::Array(vec![
                ScalarValue::Text("a".to_string()),
                ScalarValue::Null,
                ScalarValue::Text("b".to_string())
            ])
        );
        assert_eq!(results[18].rows[0][0], ScalarValue::Null);
        assert_eq!(results[19].rows[0][0], ScalarValue::Null);
        assert_eq!(results[20].rows[0][0], ScalarValue::Null);
    }

    #[test]
    fn array_any_all_predicates() {
        let results = run_batch(&[
            "SELECT 2 = ANY(ARRAY[1,2,3])",
            "SELECT 4 = ANY(ARRAY[1,2,3])",
            "SELECT 4 = ALL(ARRAY[4,4])",
            "SELECT 4 = ALL(ARRAY[4,5])",
            "SELECT 2 < ALL(ARRAY[3,4])",
            "SELECT 2 < ANY(ARRAY[1,3])",
            "SELECT 2 <> ANY(ARRAY[2,2,2])",
            "SELECT 2 <> ALL(ARRAY[3,4])",
            "SELECT 1 = ANY(ARRAY[NULL])",
            "SELECT 1 = ALL(ARRAY[NULL])",
        ]);
        assert_eq!(results[0].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[1].rows[0][0], ScalarValue::Bool(false));
        assert_eq!(results[2].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[3].rows[0][0], ScalarValue::Bool(false));
        assert_eq!(results[4].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[5].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[6].rows[0][0], ScalarValue::Bool(false));
        assert_eq!(results[7].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[8].rows[0][0], ScalarValue::Null);
        assert_eq!(results[9].rows[0][0], ScalarValue::Null);
    }

    // 1.6.9 Type conversion
    #[test]
    fn to_number_function() {
        let r = run("SELECT to_number('$1,234.56', '9999.99')");
        match &r.rows[0][0] {
            ScalarValue::Float(v) => assert!((*v - 1234.56).abs() < 0.01),
            other => panic!("expected float, got {:?}", other),
        }
    }

    // 1.8 EXPLAIN
    #[test]
    fn explain_basic_query() {
        let r = run("EXPLAIN SELECT 1");
        assert_eq!(r.columns, vec!["QUERY PLAN".to_string()]);
        assert!(!r.rows.is_empty());
    }

    #[test]
    fn explain_analyze_query() {
        let results = run_batch(&[
            "CREATE TABLE t (id int8)",
            "INSERT INTO t VALUES (1), (2), (3)",
            "EXPLAIN ANALYZE SELECT * FROM t",
        ]);
        assert!(results[2].rows.len() >= 2); // should have plan + timing
    }

    // 1.10 SET/SHOW
    #[test]
    fn set_and_show_variable() {
        let results = run_batch(&[
            "SET search_path = 'myschema'",
            "SHOW search_path",
        ]);
        assert_eq!(results[1].rows[0][0], ScalarValue::Text("myschema".to_string()));
    }

    // 1.13 LISTEN/NOTIFY/UNLISTEN
    #[test]
    fn listen_notify_unlisten_parse_and_execute() {
        let results = run_batch(&[
            "LISTEN my_channel",
            "NOTIFY my_channel, 'hello'",
            "UNLISTEN my_channel",
        ]);
        assert_eq!(results[0].command_tag, "LISTEN");
        assert_eq!(results[1].command_tag, "NOTIFY");
        assert_eq!(results[2].command_tag, "UNLISTEN");
    }

    // 1.12 DO blocks
    #[test]
    fn do_block_parses_and_executes() {
        let r = run("DO 'BEGIN NULL; END'");
        assert_eq!(r.command_tag, "DO");
    }

    // 1.11 System catalogs
    #[test]
    fn pg_settings_returns_guc_variables() {
        let r = run("SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'");
        assert!(!r.rows.is_empty());
        assert_eq!(r.rows[0][0], ScalarValue::Text("server_version".to_string()));
    }

    #[test]
    fn pg_database_returns_current_database() {
        let r = run("SELECT datname FROM pg_catalog.pg_database");
        assert_eq!(r.rows[0][0], ScalarValue::Text("postrust".to_string()));
    }

    #[test]
    fn pg_tables_lists_user_tables() {
        let results = run_batch(&[
            "CREATE TABLE catalog_test (id int8)",
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'catalog_test'",
        ]);
        assert_eq!(results[1].rows.len(), 1);
        assert_eq!(results[1].rows[0][0], ScalarValue::Text("catalog_test".to_string()));
    }

    #[test]
    fn information_schema_schemata_lists_schemas() {
        let r = run("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public'");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], ScalarValue::Text("public".to_string()));
    }

    // 1.1 Additional type system: VARCHAR/CHAR parsing
    #[test]
    fn cast_to_integer_and_varchar() {
        let r = run("SELECT CAST(42 AS text), CAST('123' AS int8)");
        assert_eq!(r.rows[0][0], ScalarValue::Text("42".to_string()));
        assert_eq!(r.rows[0][1], ScalarValue::Int(123));
    }

    // 1.1 Extended type parsing
    #[test]
    fn parses_extended_types_in_create_table() {
        let results = run_batch(&[
            "CREATE TABLE typed (a smallint, b integer, c bigint, d real, e varchar, f bytea, g uuid, h json, i jsonb, j numeric, k serial)",
            "INSERT INTO typed VALUES (1, 2, 3, 4.5, 'hello', 'binary', '550e8400-e29b-41d4-a716-446655440000', '{\"a\":1}', '{\"b\":2}', 3.14, 1)",
            "SELECT * FROM typed",
        ]);
        assert_eq!(results[2].rows.len(), 1);
    }

    // 1.7 DISTINCT ON
    #[test]
    fn distinct_on_keeps_first_per_group() {
        let results = run_batch(&[
            "CREATE TABLE t (cat text, val int8)",
            "INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)",
            "SELECT DISTINCT ON (cat) cat, val FROM t ORDER BY cat, val",
        ]);
        assert_eq!(results[2].rows.len(), 2);
        assert_eq!(results[2].rows[0][0], ScalarValue::Text("a".to_string()));
        assert_eq!(results[2].rows[0][1], ScalarValue::Int(1));
        assert_eq!(results[2].rows[1][0], ScalarValue::Text("b".to_string()));
        assert_eq!(results[2].rows[1][1], ScalarValue::Int(3));
    }

    // 1.7 VALUES as standalone query
    #[test]
    fn values_as_standalone_query() {
        let r = run("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], ScalarValue::Int(1));
        assert_eq!(r.rows[2][1], ScalarValue::Text("c".to_string()));
    }

    // === Extension system tests ===

    #[test]
    fn create_and_drop_extension() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT extname, extversion FROM pg_extension",
            "DROP EXTENSION ws",
            "SELECT extname FROM pg_extension",
        ]);
        assert_eq!(results[0].command_tag, "CREATE EXTENSION");
        assert_eq!(results[1].rows.len(), 1);
        assert_eq!(results[1].rows[0][0], ScalarValue::Text("ws".to_string()));
        assert_eq!(results[1].rows[0][1], ScalarValue::Text("1.0".to_string()));
        assert_eq!(results[2].command_tag, "DROP EXTENSION");
        assert_eq!(results[3].rows.len(), 0);
    }

    #[test]
    fn create_extension_if_not_exists() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "CREATE EXTENSION IF NOT EXISTS ws",
        ]);
        assert_eq!(results[0].command_tag, "CREATE EXTENSION");
        assert_eq!(results[1].command_tag, "CREATE EXTENSION");
    }

    #[test]
    fn create_extension_duplicate_errors() {
        with_isolated_state(|| {
            run_statement("CREATE EXTENSION ws", &[]);
            let result = parse_statement("CREATE EXTENSION ws")
                .and_then(|s| plan_statement(s).map_err(|e| crate::parser::sql_parser::ParseError { message: e.message, position: 0 }))
                .and_then(|p| block_on(execute_planned_query(&p, &[])).map_err(|e| crate::parser::sql_parser::ParseError { message: e.message, position: 0 }));
            assert!(result.is_err());
        });
    }

    #[test]
    fn drop_extension_if_exists() {
        run("DROP EXTENSION IF EXISTS ws");
    }

    #[test]
    fn drop_extension_nonexistent_errors() {
        with_isolated_state(|| {
            let stmt = parse_statement("DROP EXTENSION ws").unwrap();
            let planned = plan_statement(stmt).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
        });
    }

    #[test]
    fn create_extension_unknown_errors() {
        with_isolated_state(|| {
            let stmt = parse_statement("CREATE EXTENSION foobar").unwrap();
            let planned = plan_statement(stmt).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
        });
    }

    // === CREATE FUNCTION tests ===

    #[test]
    fn create_function_basic() {
        let results = run_batch(&[
            "CREATE FUNCTION add_one(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql",
            "SELECT proname FROM pg_proc WHERE proname = 'add_one'",
        ]);
        assert_eq!(results[0].command_tag, "CREATE FUNCTION");
        assert_eq!(results[1].rows.len(), 1);
        assert_eq!(results[1].rows[0][0], ScalarValue::Text("add_one".to_string()));
    }

    #[test]
    fn create_or_replace_function() {
        let results = run_batch(&[
            "CREATE FUNCTION my_fn(x TEXT) RETURNS TEXT AS $$ SELECT x $$ LANGUAGE sql",
            "CREATE OR REPLACE FUNCTION my_fn(x TEXT) RETURNS TEXT AS $$ SELECT x $$ LANGUAGE sql",
        ]);
        assert_eq!(results[0].command_tag, "CREATE FUNCTION");
        assert_eq!(results[1].command_tag, "CREATE FUNCTION");
    }

    #[test]
    fn create_function_returns_table() {
        run("CREATE FUNCTION my_tbl(msg JSONB) RETURNS TABLE(price TEXT, qty TEXT) AS $$ SELECT msg->>'p', msg->>'q' $$ LANGUAGE sql");
    }

    // === WebSocket extension tests ===

    #[test]
    fn ws_connect_returns_id() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://example.com')",
        ]);
        assert_eq!(results[1].rows.len(), 1);
        assert_eq!(results[1].rows[0][0], ScalarValue::Int(1));
    }

    #[test]
    fn ws_connections_virtual_table() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://example.com')",
            "SELECT id, url, state FROM ws.connections",
        ]);
        assert_eq!(results[2].rows.len(), 1);
        assert_eq!(results[2].rows[0][0], ScalarValue::Int(1));
        assert_eq!(results[2].rows[0][1], ScalarValue::Text("wss://example.com".to_string()));
        assert_eq!(results[2].rows[0][2], ScalarValue::Text("connecting".to_string()));
    }

    #[test]
    fn ws_send_on_valid_connection() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://example.com')",
            "SELECT ws.send(1, 'hello')",
        ]);
        assert_eq!(results[2].rows[0][0], ScalarValue::Bool(true));
    }

    #[test]
    fn ws_close_marks_connection_closed() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://example.com')",
            "SELECT ws.close(1)",
            "SELECT state FROM ws.connections WHERE id = 1",
        ]);
        assert_eq!(results[2].rows[0][0], ScalarValue::Bool(true));
        assert_eq!(results[3].rows[0][0], ScalarValue::Text("closed".to_string()));
    }

    #[test]
    fn ws_send_on_invalid_id_errors() {
        with_isolated_state(|| {
            run_statement("CREATE EXTENSION ws", &[]);
            let stmt = parse_statement("SELECT ws.send(999, 'hello')").unwrap();
            let planned = plan_statement(stmt).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
        });
    }

    #[test]
    fn ws_connect_without_extension_errors() {
        with_isolated_state(|| {
            let stmt = parse_statement("SELECT ws.connect('wss://example.com')").unwrap();
            let planned = plan_statement(stmt).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
        });
    }

    #[test]
    fn ws_multiple_connections() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://a.com')",
            "SELECT ws.connect('wss://b.com')",
            "SELECT count(*) FROM ws.connections",
        ]);
        assert_eq!(results[1].rows[0][0], ScalarValue::Int(1));
        assert_eq!(results[2].rows[0][0], ScalarValue::Int(2));
        assert_eq!(results[3].rows[0][0], ScalarValue::Int(2));
    }

    #[test]
    fn ws_drop_extension_clears_connections() {
        let results = run_batch(&[
            "CREATE EXTENSION ws",
            "SELECT ws.connect('wss://example.com')",
            "DROP EXTENSION ws",
            "CREATE EXTENSION ws",
            "SELECT count(*) FROM ws.connections",
        ]);
        assert_eq!(results[4].rows[0][0], ScalarValue::Int(0));
    }

    #[test]
    fn ws_callback_on_message() {
        with_isolated_state(|| {
            run_statement("CREATE EXTENSION ws", &[]);
            run_statement(
                "CREATE FUNCTION handle_msg(msg JSONB) RETURNS TABLE(price TEXT) AS $$ SELECT msg->>'p' $$ LANGUAGE sql",
                &[],
            );
            run_statement(
                "SELECT ws.connect('wss://example.com', NULL, 'handle_msg', NULL)",
                &[],
            );
            // Simulate a message arriving
            let results = block_on(ws_simulate_message(1, r#"{"p":"100.5","q":"2.0"}"#)).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].rows[0][0], ScalarValue::Text("100.5".to_string()));
        });
    }

    #[test]
    fn ws_send_on_closed_connection_errors() {
        with_isolated_state(|| {
            run_statement("CREATE EXTENSION ws", &[]);
            run_statement("SELECT ws.connect('wss://example.com')", &[]);
            run_statement("SELECT ws.close(1)", &[]);
            let stmt = parse_statement("SELECT ws.send(1, 'hello')").unwrap();
            let planned = plan_statement(stmt).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
        });
    }
}
