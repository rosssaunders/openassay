use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{OnceLock, RwLock};

use crate::catalog::oid::Oid;
pub(crate) use crate::catalog::system_catalogs::lookup_virtual_relation;
use crate::catalog::with_catalog_write;
use crate::catalog::{Column, ColumnSpec, SearchPath, TableKind, TypeSignature, with_catalog_read};
use crate::commands::sequence::{SequenceState, with_sequences_read, with_sequences_write};
pub(crate) use crate::executor::exec_expr::{EvalScope, eval_expr};
pub(crate) use crate::executor::exec_main::execute_query;
use crate::executor::exec_main::{
    combine_scopes, evaluate_from_clause, evaluate_table_expression, scope_for_table_row,
    scope_for_table_row_with_qualifiers,
};
use crate::parser::ast::{
    ConflictTarget, CreateFunctionStatement, DeleteStatement, Expr, ForeignKeyAction,
    FunctionParam, FunctionParamMode, FunctionReturnType, InsertSource, InsertStatement,
    MergeStatement, MergeWhenClause, OnConflictClause, Statement, TableConstraint, TriggerEvent,
    TriggerTiming, UpdateStatement,
};
use crate::planner::{self, PlanNode};
use crate::security::{self, RlsCommand, TablePrivilege};
pub(crate) use crate::storage::heap::{with_storage_read, with_storage_write};
pub(crate) use crate::tcop::pquery::{
    CteBinding, ExpandedFromColumn, active_cte_context, current_cte_binding, derive_query_columns,
    derive_select_columns, expand_from_columns, query_references_relation, type_oid_size,
    type_signature_to_oid, validate_recursive_cte_terms, with_cte_context_async,
};
use crate::tcop::pquery::{
    derive_dml_returning_column_type_oids, derive_dml_returning_columns,
    derive_query_output_columns, derive_returning_columns_from_table, project_returning_row,
    project_returning_row_with_qualifiers,
};
use crate::utils::adt::datetime::{
    datetime_from_epoch_seconds, format_date, format_timestamp, parse_datetime_text,
};
pub(crate) use crate::utils::adt::misc::truthy;
use crate::utils::adt::vector::coerce_scalar_to_vector;

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

const PG_TEXT_OID: u32 = 25;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
    pub command_tag: String,
    pub rows_affected: u64,
}

#[derive(Debug, Clone)]
pub struct PlannedQuery {
    plan: PlanNode,
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
    // Run semantic analysis before planning
    let search_path = crate::catalog::SearchPath::default();
    crate::analyzer::analyze(&statement, &search_path)?;
    let plan = planner::plan(&statement);

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
        Statement::NoOp(noop) => (Vec::new(), Vec::new(), false, noop.command_tag.clone()),
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
        Statement::CreateRole(_) => (Vec::new(), Vec::new(), false, "CREATE ROLE".to_string()),
        Statement::AlterRole(_) => (Vec::new(), Vec::new(), false, "ALTER ROLE".to_string()),
        Statement::DropRole(_) => (Vec::new(), Vec::new(), false, "DROP ROLE".to_string()),
        Statement::Grant(_) => (Vec::new(), Vec::new(), false, "GRANT".to_string()),
        Statement::Revoke(_) => (Vec::new(), Vec::new(), false, "REVOKE".to_string()),
        Statement::Copy(_) => (Vec::new(), Vec::new(), false, "COPY".to_string()),
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
        Statement::CreateExtension(_) => (
            Vec::new(),
            Vec::new(),
            false,
            "CREATE EXTENSION".to_string(),
        ),
        Statement::DropExtension(_) => {
            (Vec::new(), Vec::new(), false, "DROP EXTENSION".to_string())
        }
        Statement::CreateFunction(_) => {
            (Vec::new(), Vec::new(), false, "CREATE FUNCTION".to_string())
        }
        Statement::DropFunction(_) => (Vec::new(), Vec::new(), false, "DROP FUNCTION".to_string()),
        Statement::CreateTrigger(_) => {
            (Vec::new(), Vec::new(), false, "CREATE TRIGGER".to_string())
        }
        Statement::DropTrigger(_) => (Vec::new(), Vec::new(), false, "DROP TRIGGER".to_string()),
        Statement::CreateSubscription(_) => (
            Vec::new(),
            Vec::new(),
            false,
            "CREATE SUBSCRIPTION".to_string(),
        ),
        Statement::DropSubscription(_) => (
            Vec::new(),
            Vec::new(),
            false,
            "DROP SUBSCRIPTION".to_string(),
        ),
        Statement::Transaction(statement) => {
            let tag = match statement {
                crate::parser::ast::TransactionStatement::Begin => "BEGIN",
                crate::parser::ast::TransactionStatement::Commit => "COMMIT",
                crate::parser::ast::TransactionStatement::Rollback => "ROLLBACK",
                crate::parser::ast::TransactionStatement::Savepoint(_) => "SAVEPOINT",
                crate::parser::ast::TransactionStatement::ReleaseSavepoint(_) => "RELEASE",
                crate::parser::ast::TransactionStatement::RollbackToSavepoint(_) => "ROLLBACK",
            };
            (Vec::new(), Vec::new(), false, tag.to_string())
        }
        Statement::CreateType(_) => (Vec::new(), Vec::new(), false, "CREATE TYPE".to_string()),
        Statement::CreateCast(_) => (Vec::new(), Vec::new(), false, "CREATE CAST".to_string()),
        Statement::CreateDomain(_) => (Vec::new(), Vec::new(), false, "CREATE DOMAIN".to_string()),
        Statement::DropType(_) => (Vec::new(), Vec::new(), false, "DROP TYPE".to_string()),
        Statement::DropDomain(_) => (Vec::new(), Vec::new(), false, "DROP DOMAIN".to_string()),
    };
    Ok(PlannedQuery {
        plan,
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
        let result = match &plan.plan {
            PlanNode::Query(query_plan) => execute_query(&query_plan.query, params).await?,
            PlanNode::Insert(insert_plan) => execute_insert(&insert_plan.statement, params).await?,
            PlanNode::Update(update_plan) => execute_update(&update_plan.statement, params).await?,
            PlanNode::Delete(delete_plan) => execute_delete(&delete_plan.statement, params).await?,
            PlanNode::Merge(merge_plan) => execute_merge(&merge_plan.statement, params).await?,
            PlanNode::PassThrough(statement) => match statement {
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
                _ => crate::tcop::utility::execute_utility_statement(statement, params).await?,
            },
        };
        Ok(result)
    })
}

fn triggers_for_table_event(
    table: &crate::catalog::Table,
    timing: TriggerTiming,
    event: TriggerEvent,
) -> Vec<UserTrigger> {
    with_ext_read(|ext| {
        ext.triggers
            .iter()
            .filter(|trigger| {
                trigger.timing == timing
                    && trigger.events.contains(&event)
                    && trigger.table_name.len() == 2
                    && trigger.table_name[0].eq_ignore_ascii_case(table.schema_name())
                    && trigger.table_name[1].eq_ignore_ascii_case(table.name())
            })
            .cloned()
            .collect()
    })
}

fn execute_row_trigger(
    trigger: &UserTrigger,
    table: &crate::catalog::Table,
    tg_op: &str,
    old_row: Option<&[ScalarValue]>,
    new_row: Option<&[ScalarValue]>,
) -> Result<Option<ScalarValue>, EngineError> {
    let Some(function) = lookup_user_function(&trigger.function_name, 0) else {
        return Ok(None);
    };
    if !function.language.eq_ignore_ascii_case("plpgsql") {
        return Ok(None);
    }
    let create_stmt = CreateFunctionStatement {
        name: function.name.clone(),
        params: function.params.clone(),
        return_type: function.return_type.clone(),
        is_trigger: true,
        body: function.body.clone(),
        language: function.language.clone(),
        or_replace: true,
    };
    let compiled = match crate::plpgsql::compile_create_function_statement(&create_stmt) {
        Ok(compiled) => compiled,
        Err(_) => return Ok(None),
    };

    let field_names = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    let context = crate::plpgsql::PLpgSQLTriggerContext {
        tg_op: tg_op.to_string(),
        tg_table_name: table.name().to_string(),
        new_row: new_row.map(|row| (field_names.clone(), row.to_vec())),
        old_row: old_row.map(|row| (field_names.clone(), row.to_vec())),
    };

    match crate::plpgsql::plpgsql_exec_function_with_trigger(&compiled, &[], Some(&context)) {
        Ok(result) => Ok(result),
        Err(_) => Ok(None),
    }
}

fn apply_before_trigger_result(
    table: &crate::catalog::Table,
    result: Option<ScalarValue>,
    current_row: Vec<ScalarValue>,
) -> Result<Option<Vec<ScalarValue>>, EngineError> {
    match result {
        None => Ok(Some(current_row)),
        Some(ScalarValue::Null) => Ok(None),
        Some(ScalarValue::Record(values)) => {
            if values.len() != table.columns().len() {
                return Err(EngineError {
                    message: format!(
                        "trigger returned record with {} columns for relation \"{}\" (expected {})",
                        values.len(),
                        table.qualified_name(),
                        table.columns().len()
                    ),
                });
            }
            Ok(Some(values))
        }
        Some(_) => Ok(Some(current_row)),
    }
}

// ── Extension & User Function Registry ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UserFunction {
    pub name: Vec<String>,
    pub params: Vec<FunctionParam>,
    pub return_type: Option<FunctionReturnType>,
    pub is_trigger: bool,
    pub body: String,
    pub language: String,
}

#[derive(Debug, Clone)]
pub struct UserTrigger {
    pub name: String,
    pub table_name: Vec<String>,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub function_name: Vec<String>,
}

pub(crate) fn function_input_arity(params: &[FunctionParam]) -> usize {
    params
        .iter()
        .filter(|param| matches!(param.mode, FunctionParamMode::In | FunctionParamMode::InOut))
        .count()
}

pub(crate) fn same_function_identity(left: &UserFunction, right: &UserFunction) -> bool {
    left.name == right.name
        && left
            .params
            .iter()
            .filter(|param| matches!(param.mode, FunctionParamMode::In | FunctionParamMode::InOut))
            .map(|param| &param.data_type)
            .eq(right
                .params
                .iter()
                .filter(|param| {
                    matches!(param.mode, FunctionParamMode::In | FunctionParamMode::InOut)
                })
                .map(|param| &param.data_type))
}

pub(crate) fn lookup_user_function(name: &[String], arg_count: usize) -> Option<UserFunction> {
    let lowered = name
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    with_ext_read(|ext| {
        let mut candidates = ext
            .user_functions
            .iter()
            .filter(|func| {
                if func.name == lowered {
                    return true;
                }
                func.name.last() == lowered.last()
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|func| function_input_arity(&func.params));
        candidates
            .iter()
            .find(|func| function_input_arity(&func.params) == arg_count)
            .cloned()
            .or_else(|| candidates.into_iter().next())
    })
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
        pub writer: Arc<
            Mutex<
                Option<
                    tungstenite::WebSocket<
                        tungstenite::stream::MaybeTlsStream<std::net::TcpStream>,
                    >,
                >,
            >,
        >,
        pub incoming: mpsc::Receiver<String>,
        pub _reader_thread: thread::JoinHandle<()>,
    }

    /// Open a real WebSocket connection. Returns a handle for sending/receiving.
    pub fn open_connection(url: &str) -> Result<NativeWsHandle, String> {
        let (socket, _response) =
            tungstenite::connect(url).map_err(|e| format!("WebSocket connect failed: {e}"))?;

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
            ws.write(tungstenite::Message::Text(msg.to_string().into()))
                .map_err(|e| format!("WebSocket send failed: {e}"))?;
            ws.flush()
                .map_err(|e| format!("WebSocket flush failed: {e}"))?;
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
static NATIVE_WS_HANDLES: std::sync::OnceLock<
    std::sync::Mutex<HashMap<i64, ws_native::NativeWsHandle>>,
> = std::sync::OnceLock::new();

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_ws_handles()
-> &'static std::sync::Mutex<HashMap<i64, ws_native::NativeWsHandle>> {
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
pub(crate) mod ws_wasm {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::prelude::*;
    use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

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

        // onmessage — handle text, Blob (via toString), and ArrayBuffer data
        let msgs_clone = Rc::clone(&messages);
        let on_message =
            Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |e: MessageEvent| {
                let data = e.data();
                if let Ok(txt) = data.dyn_into::<js_sys::JsString>() {
                    msgs_clone.borrow_mut().push(String::from(txt));
                } else if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let arr = js_sys::Uint8Array::new(&buf);
                    let mut bytes = vec![0u8; arr.length() as usize];
                    arr.copy_to(&mut bytes);
                    if let Ok(s) = String::from_utf8(bytes) {
                        msgs_clone.borrow_mut().push(s);
                    }
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
        handle
            .socket
            .send_with_str(msg)
            .map_err(|e| format!("WebSocket send failed: {:?}", e))
    }

    pub fn close_connection(handle: &WasmWsHandle) -> Result<(), String> {
        handle
            .socket
            .close()
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
pub(crate) fn drain_wasm_ws_messages(conn_id: i64) {
    let msgs = ws_wasm::with_handle(conn_id, |handle| ws_wasm::drain_incoming(handle));
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
pub(crate) fn sync_wasm_ws_state(conn_id: i64) {
    let new_state = ws_wasm::with_handle(conn_id, |handle| ws_wasm::get_state(handle));
    if let Some(state) = new_state {
        with_ext_write(|ext| {
            if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
                conn.state = state;
            }
        });
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct UserEnumType {
    pub(crate) name: Vec<String>,
    pub(crate) labels: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct UserDomain {
    pub(crate) name: Vec<String>,
    pub(crate) base_type: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ExtensionState {
    pub(crate) extensions: Vec<ExtensionRecord>,
    pub(crate) user_functions: Vec<UserFunction>,
    pub(crate) triggers: Vec<UserTrigger>,
    pub(crate) user_types: Vec<UserEnumType>,
    pub(crate) user_domains: Vec<UserDomain>,
    pub(crate) ws_connections: HashMap<i64, WsConnection>,
    pub(crate) ws_next_id: i64,
}

static GLOBAL_EXTENSION_STATE: OnceLock<RwLock<ExtensionState>> = OnceLock::new();

fn global_extension_state() -> &'static RwLock<ExtensionState> {
    GLOBAL_EXTENSION_STATE.get_or_init(|| RwLock::new(ExtensionState::default()))
}

pub(crate) fn with_ext_read<T>(f: impl FnOnce(&ExtensionState) -> T) -> T {
    let state = global_extension_state()
        .read()
        .expect("ext state lock poisoned");
    f(&state)
}

pub(crate) fn with_ext_write<T>(f: impl FnOnce(&mut ExtensionState) -> T) -> T {
    let mut state = global_extension_state()
        .write()
        .expect("ext state lock poisoned");
    f(&mut state)
}

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EngineStateSnapshot {
    catalog: crate::catalog::Catalog,
    rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
    indexes_by_table: HashMap<(Oid, String), crate::storage::heap::StoredIndex>,
    pub(crate) sequences: HashMap<String, SequenceState>,
    security: crate::security::SecurityState,
}

pub fn snapshot_state() -> EngineStateSnapshot {
    EngineStateSnapshot {
        catalog: with_catalog_read(|catalog| catalog.clone()),
        rows_by_table: with_storage_read(|storage| storage.rows_by_table.clone()),
        indexes_by_table: with_storage_read(|storage| storage.indexes_by_table.clone()),
        sequences: with_sequences_read(|sequences| sequences.clone()),
        security: security::snapshot_state(),
    }
}

pub fn restore_state(snapshot: EngineStateSnapshot) {
    let EngineStateSnapshot {
        catalog: next_catalog,
        rows_by_table: next_rows,
        indexes_by_table: next_indexes,
        sequences: next_sequences,
        security: next_security,
    } = snapshot;
    with_catalog_write(|catalog| {
        *catalog = next_catalog;
    });
    with_storage_write(|storage| {
        storage.rows_by_table = next_rows;
        storage.indexes_by_table = next_indexes;
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
        storage.indexes_by_table.clear();
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

pub fn copy_table_column_names(table_name: &[String]) -> Result<Vec<String>, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    Ok(table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
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
        if !relation_row_passes_check_for_command(&table, &row, RlsCommand::Insert, &[]).await? {
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
        let _ = storage.replace_rows_for_table(table.oid(), candidate_rows);
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

#[derive(Debug, Clone)]
enum IndexMutationAction {
    Insert {
        row: Vec<ScalarValue>,
    },
    Update {
        offset: usize,
        row: Vec<ScalarValue>,
    },
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
    let before_insert_triggers =
        triggers_for_table_event(&table, TriggerTiming::Before, TriggerEvent::Insert);
    let after_insert_triggers =
        triggers_for_table_event(&table, TriggerTiming::After, TriggerEvent::Insert);
    let source_rows = match &insert.source {
        InsertSource::Values(values_rows) => {
            let mut rows = Vec::with_capacity(values_rows.len());
            for row_exprs in values_rows {
                if row_exprs.len() > target_indexes.len() {
                    return Err(EngineError {
                        message: format!(
                            "INSERT has {} expressions but {} target columns",
                            row_exprs.len(),
                            target_indexes.len()
                        ),
                    });
                }
                // Build a full row, handling DEFAULT by applying column defaults
                let mut row = vec![ScalarValue::Null; table.columns().len()];
                let mut provided = vec![false; table.columns().len()];
                for (expr, col_idx) in row_exprs.iter().zip(target_indexes.iter()) {
                    if matches!(expr, Expr::Default) {
                        // Leave as not provided; default fill below
                    } else {
                        let val = eval_expr(expr, &EvalScope::default(), params).await?;
                        let column = &table.columns()[*col_idx];
                        row[*col_idx] = coerce_value_for_column(val, column)?;
                        provided[*col_idx] = true;
                    }
                }
                for (idx, column) in table.columns().iter().enumerate() {
                    if !provided[idx]
                        && let Some(default_expr) = column.default()
                    {
                        let raw = eval_expr(default_expr, &EvalScope::default(), params).await?;
                        row[idx] = coerce_value_for_column(raw, column)?;
                    }
                }
                rows.push(row);
            }
            rows
        }
        InsertSource::Query(query) => {
            let query_rows = execute_query(query, params).await?.rows;
            let mut rows = Vec::with_capacity(query_rows.len());
            for source_row in &query_rows {
                if source_row.len() > target_indexes.len() {
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
                rows.push(row);
            }
            rows
        }
    };

    let mut materialized = Vec::with_capacity(source_rows.len());
    'source_rows: for row in &source_rows {
        let mut row = row.clone();
        for trigger in &before_insert_triggers {
            let result = execute_row_trigger(trigger, &table, "INSERT", None, Some(&row))?;
            let Some(next_row) = apply_before_trigger_result(&table, result, row)? else {
                continue 'source_rows;
            };
            row = next_row;
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
    let mut index_mutations = Vec::new();
    let mut pending_unique_keys: HashMap<String, HashSet<String>> = HashMap::new();
    match &insert.on_conflict {
        None => {
            for row in &materialized {
                ensure_unique_index_preinsert(&table, row, &mut pending_unique_keys, None)?;
                index_mutations.push(IndexMutationAction::Insert { row: row.clone() });
            }
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
                    && row_conflicts_on_columns_with_index(
                        &table,
                        &candidate_rows,
                        row,
                        target_indexes,
                    )
                {
                    continue;
                }
                let mut trial = candidate_rows.clone();
                trial.push(row.clone());
                match validate_table_constraints(&table, &trial).await {
                    Ok(()) => {
                        candidate_rows = trial;
                        ensure_unique_index_preinsert(&table, row, &mut pending_unique_keys, None)?;
                        accepted_rows.push(row.clone());
                        index_mutations.push(IndexMutationAction::Insert { row: row.clone() });
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
                let Some(conflicting_row_idx) = find_conflict_row_index_with_index(
                    &table,
                    &candidate_rows,
                    row,
                    &conflict_target_indexes,
                ) else {
                    let mut trial = candidate_rows.clone();
                    trial.push(row.clone());
                    validate_table_constraints(&table, &trial).await?;
                    candidate_rows = trial;
                    ensure_unique_index_preinsert(&table, row, &mut pending_unique_keys, None)?;
                    accepted_rows.push(row.clone());
                    index_mutations.push(IndexMutationAction::Insert { row: row.clone() });
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
                    updated_row[*col_idx] =
                        eval_update_assignment_value(expr, column, &scope, params).await?;
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
                index_mutations.push(IndexMutationAction::Update {
                    offset: conflicting_row_idx,
                    row: updated_row.clone(),
                });
                accepted_rows.push(updated_row);
            }
        }
    }

    let inserted = accepted_rows.len() as u64;
    let write_result = with_storage_write(|storage| {
        for mutation in &index_mutations {
            match mutation {
                IndexMutationAction::Insert { row } => {
                    storage.append_row(table.oid(), row.clone())?;
                }
                IndexMutationAction::Update { offset, row } => {
                    storage.update_row(table.oid(), *offset, row.clone())?;
                }
            }
        }
        Ok::<(), String>(())
    });
    if let Err(err) = write_result {
        with_storage_write(|storage| {
            let _ = storage.replace_rows_for_table(table.oid(), candidate_rows.clone());
        });
        return Err(EngineError { message: err });
    }
    for row in &accepted_rows {
        for trigger in &after_insert_triggers {
            let _ = execute_row_trigger(trigger, &table, "INSERT", None, Some(row))?;
        }
    }
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
        let assignment_key = format!("{normalized}:{:?}", assignment.subscripts);
        // Check for qualified column name (e.g. SET t.b = ...) — PostgreSQL rejects this
        // The column name should not contain dots; if someone writes "t.b", the parser
        // produces column="t" which won't match, giving a good error. But let's also
        // detect if the column name looks qualified.
        if !seen.insert(assignment_key) {
            return Err(EngineError {
                message: format!("column \"{}\" specified more than once", assignment.column),
            });
        }
        // Check for generated columns
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
        if column.is_generated() && !matches!(assignment.value, Expr::Default) {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" can only be updated to DEFAULT",
                    column.name()
                ),
            });
        }
        assignment_targets.push((idx, column, &assignment.value));
    }
    let before_update_triggers =
        triggers_for_table_event(&table, TriggerTiming::Before, TriggerEvent::Update);
    let after_update_triggers =
        triggers_for_table_event(&table, TriggerTiming::After, TriggerEvent::Update);

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
    let mut after_trigger_rows = Vec::new();
    let mut updated_changes = Vec::new();
    let mut updated = 0u64;
    let target_qualifiers = update
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .unwrap_or_else(|| vec![table.name().to_string(), table.qualified_name()]);

    'row_updates: for (row_idx, row) in current_rows.iter().enumerate() {
        if !relation_row_visible_for_command(&table, row, RlsCommand::Update, params).await? {
            continue;
        }
        let base_scope = scope_for_table_row_with_qualifiers(&table, row, &target_qualifiers);
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
            new_row[*col_idx] = eval_update_assignment_value(expr, column, &scope, params).await?;
        }
        for trigger in &before_update_triggers {
            let result = execute_row_trigger(trigger, &table, "UPDATE", Some(row), Some(&new_row))?;
            let Some(next_row) = apply_before_trigger_result(&table, result, new_row)? else {
                continue 'row_updates;
            };
            new_row = next_row;
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
        updated_changes.push((row_idx, next_rows[row_idx].clone()));
        returning_base_rows.push(next_rows[row_idx].clone());
        after_trigger_rows.push((row.clone(), next_rows[row_idx].clone()));
        updated += 1;
    }

    validate_table_constraints(&table, &next_rows).await?;
    let mut staged_updates =
        apply_on_update_actions(&table, &current_rows, next_rows.clone()).await?;
    let parent_rows_after = staged_updates
        .remove(&table.oid())
        .unwrap_or_else(|| next_rows.clone());
    let write_result = with_storage_write(|storage| {
        for (offset, row) in &updated_changes {
            storage.update_row(table.oid(), *offset, row.clone())?;
        }
        for (table_oid, rows) in &staged_updates {
            storage.replace_rows_for_table(*table_oid, rows.clone())?;
        }
        Ok::<(), String>(())
    });
    if let Err(err) = write_result {
        with_storage_write(|storage| {
            let _ = storage.replace_rows_for_table(table.oid(), parent_rows_after.clone());
            for (table_oid, rows) in &staged_updates {
                let _ = storage.replace_rows_for_table(*table_oid, rows.clone());
            }
        });
        return Err(EngineError { message: err });
    }
    for (old_row, new_row) in &after_trigger_rows {
        for trigger in &after_update_triggers {
            let _ = execute_row_trigger(trigger, &table, "UPDATE", Some(old_row), Some(new_row))?;
        }
    }
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
    let before_delete_triggers =
        triggers_for_table_event(&table, TriggerTiming::Before, TriggerEvent::Delete);
    let after_delete_triggers =
        triggers_for_table_event(&table, TriggerTiming::After, TriggerEvent::Delete);

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
    let mut removed_row_offsets = Vec::new();
    let mut deleted = 0u64;
    for (row_idx, row) in current_rows.iter().enumerate() {
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
            let mut allow_delete = true;
            for trigger in &before_delete_triggers {
                let result = execute_row_trigger(trigger, &table, "DELETE", Some(row), None)?;
                if matches!(result, Some(ScalarValue::Null)) {
                    allow_delete = false;
                    break;
                }
            }
            if !allow_delete {
                retained.push(row.clone());
                continue;
            }
            deleted += 1;
            removed_rows.push(row.clone());
            removed_row_offsets.push(row_idx);
        } else {
            retained.push(row.clone());
        }
    }

    let mut staged_updates =
        apply_on_delete_actions(&table, retained, removed_rows.clone()).await?;
    let parent_rows_after = staged_updates.remove(&table.oid()).unwrap_or_else(Vec::new);
    let write_result = with_storage_write(|storage| {
        storage.delete_rows_by_offsets(table.oid(), &removed_row_offsets)?;
        for (table_oid, rows) in &staged_updates {
            storage.replace_rows_for_table(*table_oid, rows.clone())?;
        }
        Ok::<(), String>(())
    });
    if let Err(err) = write_result {
        with_storage_write(|storage| {
            let _ = storage.replace_rows_for_table(table.oid(), parent_rows_after.clone());
            for (table_oid, rows) in &staged_updates {
                let _ = storage.replace_rows_for_table(*table_oid, rows.clone());
            }
        });
        return Err(EngineError { message: err });
    }
    for row in &removed_rows {
        for trigger in &after_delete_triggers {
            let _ = execute_row_trigger(trigger, &table, "DELETE", Some(row), None)?;
        }
    }
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
                            new_row[*col_idx] =
                                eval_update_assignment_value(expr, column, &combined, params)
                                    .await?;
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
                            returning_rows.push(
                                project_returning_row_with_qualifiers(
                                    &merge.returning,
                                    &table,
                                    &candidate_rows[target_idx].values,
                                    &target_qualifiers,
                                    params,
                                )
                                .await?,
                            );
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
                            returning_rows.push(
                                project_returning_row_with_qualifiers(
                                    &merge.returning,
                                    &table,
                                    &removed.values,
                                    &target_qualifiers,
                                    params,
                                )
                                .await?,
                            );
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
                        let mut row = vec![ScalarValue::Null; table.columns().len()];
                        let mut provided = vec![false; table.columns().len()];
                        if columns.is_empty() && values.is_empty() {
                            // INSERT DEFAULT VALUES — all columns use defaults
                        } else {
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
                            for (expr, col_idx) in values.iter().zip(target_indexes.iter()) {
                                let column = &table.columns()[*col_idx];
                                if matches!(expr, Expr::Default) {
                                    continue;
                                }
                                let raw = eval_expr(expr, &source_scope, params).await?;
                                row[*col_idx] = coerce_value_for_column(raw, column)?;
                                provided[*col_idx] = true;
                            }
                        }
                        for (idx, column) in table.columns().iter().enumerate() {
                            if !provided[idx]
                                && let Some(default_expr) = column.default()
                            {
                                let raw = eval_expr(default_expr, &source_scope, params).await?;
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
                            returning_rows.push(
                                project_returning_row_with_qualifiers(
                                    &merge.returning,
                                    &table,
                                    &row,
                                    &target_qualifiers,
                                    params,
                                )
                                .await?,
                            );
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
                            new_row[*col_idx] =
                                eval_update_assignment_value(expr, column, &scope, params).await?;
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
                            returning_rows.push(
                                project_returning_row_with_qualifiers(
                                    &merge.returning,
                                    &table,
                                    &candidate_rows[row_idx].values,
                                    &target_qualifiers,
                                    params,
                                )
                                .await?,
                            );
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
                            returning_rows.push(
                                project_returning_row_with_qualifiers(
                                    &merge.returning,
                                    &table,
                                    &removed.values,
                                    &target_qualifiers,
                                    params,
                                )
                                .await?,
                            );
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
            let _ = storage.replace_rows_for_table(table_oid, rows);
        }
    });

    Ok(QueryResult {
        columns: returning_columns,
        rows: returning_rows,
        command_tag: "MERGE".to_string(),
        rows_affected: changed,
    })
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
        let has_path_suffix = normalized.contains('.') || normalized.contains('[');
        if !has_path_suffix && !seen.insert(normalized.clone()) {
            return Err(EngineError {
                message: format!("column \"{column_name}\" specified more than once"),
            });
        }
        let base_column = normalized
            .split(['.', '['])
            .next()
            .unwrap_or(normalized.as_str());
        let Some((idx, _)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == base_column)
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
        let assignment_key = format!("{normalized}:{:?}", assignment.subscripts);
        if !seen.insert(assignment_key) {
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

async fn eval_update_assignment_value(
    expr: &Expr,
    column: &Column,
    scope: &EvalScope,
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let raw = if matches!(expr, Expr::Default) {
        if let Some(default_expr) = column.default() {
            eval_expr(default_expr, scope, params).await?
        } else {
            ScalarValue::Null
        }
    } else {
        eval_expr(expr, scope, params).await?
    };
    coerce_value_for_column(raw, column)
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
                if seen.insert(normalized.clone()) {
                    normalized_target.push(normalized);
                }
            }
            if normalized_target.is_empty() {
                return Err(EngineError {
                    message: "ON CONFLICT target must reference at least one column".to_string(),
                });
            }

            let target_set = normalized_target
                .iter()
                .cloned()
                .collect::<HashSet<String>>();
            let matches_unique_constraint = table.key_constraints().iter().any(|constraint| {
                constraint
                    .columns
                    .iter()
                    .cloned()
                    .collect::<HashSet<String>>()
                    == target_set
            });
            let matches_unique_index = table.indexes().iter().any(|index| {
                index.unique
                    && index.columns.iter().cloned().collect::<HashSet<String>>() == target_set
            });
            if !matches_unique_constraint && !matches_unique_index {
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

fn row_conflicts_on_columns_with_index(
    table: &crate::catalog::Table,
    existing_rows: &[Vec<ScalarValue>],
    candidate_row: &[ScalarValue],
    column_indexes: &[usize],
) -> bool {
    find_conflict_row_index_with_index(table, existing_rows, candidate_row, column_indexes)
        .is_some()
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

fn find_conflict_row_index_with_index(
    table: &crate::catalog::Table,
    existing_rows: &[Vec<ScalarValue>],
    candidate_row: &[ScalarValue],
    column_indexes: &[usize],
) -> Option<usize> {
    let candidate_values = values_at_indexes(candidate_row, column_indexes)?;
    if candidate_values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null))
    {
        return None;
    }
    let matching_index = with_storage_read(|storage| {
        storage
            .index_descriptors_for_table(table.oid())
            .into_iter()
            .find(|descriptor| descriptor.unique && descriptor.column_indexes == column_indexes)
    });
    if let Some(descriptor) = matching_index {
        let offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(table.oid(), &descriptor.name, &candidate_values)
        });
        if let Some(offset) = offsets.first()
            && *offset < existing_rows.len()
        {
            return Some(*offset);
        }
    }
    find_conflict_row_index(existing_rows, candidate_row, column_indexes)
}

fn ensure_unique_index_preinsert(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    pending_keys: &mut HashMap<String, HashSet<String>>,
    ignore_offset: Option<usize>,
) -> Result<(), EngineError> {
    let unique_indexes = with_storage_read(|storage| {
        storage
            .index_descriptors_for_table(table.oid())
            .into_iter()
            .filter(|descriptor| descriptor.unique)
            .collect::<Vec<_>>()
    });
    for descriptor in unique_indexes {
        let Some(values) = values_at_indexes(row, &descriptor.column_indexes) else {
            continue;
        };
        if values
            .iter()
            .any(|value| matches!(value, ScalarValue::Null))
        {
            continue;
        }
        let key = values.iter().map(scalar_key).collect::<Vec<_>>().join("|");
        let existing_offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(table.oid(), &descriptor.name, &values)
        });
        if existing_offsets
            .iter()
            .any(|offset| Some(*offset) != ignore_offset)
        {
            return Err(EngineError {
                message: format!(
                    "duplicate value for key columns ({}) of relation \"{}\" violates unique constraint",
                    descriptor.column_names.join(", "),
                    table.qualified_name()
                ),
            });
        }
        let seen = pending_keys.entry(descriptor.name).or_default();
        if !seen.insert(key) {
            return Err(EngineError {
                message: format!(
                    "duplicate value for key columns ({}) of relation \"{}\" violates unique constraint",
                    descriptor.column_names.join(", "),
                    table.qualified_name()
                ),
            });
        }
    }
    Ok(())
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
        ScalarValue::Numeric(v) => format!("N:{v}"),
        ScalarValue::Text(v) => format!("T:{v}"),
        ScalarValue::Array(_) => format!("A:{}", value.render()),
        ScalarValue::Record(_) => format!("R:{}", value.render()),
        ScalarValue::Vector(_) => format!("V:{}", value.render()),
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
            let parsed =
                crate::utils::adt::misc::parse_pg_int_literal(&v).map_err(|_| EngineError {
                    message: format!("invalid integer literal for column \"{}\"", column.name()),
                })?;
            Ok(ScalarValue::Int(parsed))
        }
        (TypeSignature::Float8, ScalarValue::Int(v)) => Ok(ScalarValue::Float(v as f64)),
        (TypeSignature::Float8, ScalarValue::Float(v)) => Ok(ScalarValue::Float(v)),
        (TypeSignature::Float8, ScalarValue::Numeric(d)) => {
            use rust_decimal::prelude::ToPrimitive;
            Ok(ScalarValue::Float(d.to_f64().unwrap_or(f64::NAN)))
        }
        (TypeSignature::Float8, ScalarValue::Text(v)) => {
            let parsed = crate::utils::adt::float::float8in(&v)?;
            Ok(ScalarValue::Float(parsed))
        }
        (TypeSignature::Numeric, ScalarValue::Numeric(d)) => Ok(ScalarValue::Numeric(d)),
        (TypeSignature::Numeric, ScalarValue::Int(v)) => {
            Ok(ScalarValue::Numeric(rust_decimal::Decimal::from(v)))
        }
        (TypeSignature::Numeric, ScalarValue::Float(v)) => {
            use rust_decimal::prelude::FromPrimitive;
            Ok(ScalarValue::Numeric(
                rust_decimal::Decimal::from_f64(v).unwrap_or_default(),
            ))
        }
        (TypeSignature::Numeric, ScalarValue::Text(v)) => {
            let normalized = v.trim().to_ascii_lowercase();
            if normalized == "nan" {
                return Ok(ScalarValue::Float(f64::NAN));
            }
            if normalized == "inf" || normalized == "infinity" {
                return Ok(ScalarValue::Float(f64::INFINITY));
            }
            if normalized == "-inf" || normalized == "-infinity" {
                return Ok(ScalarValue::Float(f64::NEG_INFINITY));
            }
            let parsed = v
                .trim()
                .parse::<rust_decimal::Decimal>()
                .map_err(|_| EngineError {
                    message: format!("invalid numeric literal for column \"{}\"", column.name()),
                })?;
            Ok(ScalarValue::Numeric(parsed))
        }
        (TypeSignature::Text, ScalarValue::Text(v)) => Ok(ScalarValue::Text(v)),
        (TypeSignature::Text, v) => Ok(ScalarValue::Text(v.render())),
        (TypeSignature::Date, ScalarValue::Text(v)) => {
            let lower = v.trim().to_ascii_lowercase();
            if lower == "infinity" || lower == "inf" {
                Ok(ScalarValue::Text("infinity".to_string()))
            } else if lower == "-infinity" || lower == "-inf" {
                Ok(ScalarValue::Text("-infinity".to_string()))
            } else {
                let dt = parse_datetime_text(&v)?;
                Ok(ScalarValue::Text(format_date(dt.date)))
            }
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
            let lower = v.trim().to_ascii_lowercase();
            if lower == "infinity" || lower == "inf" {
                Ok(ScalarValue::Text("infinity".to_string()))
            } else if lower == "-infinity" || lower == "-inf" {
                Ok(ScalarValue::Text("-infinity".to_string()))
            } else {
                let dt = parse_datetime_text(&v)?;
                Ok(ScalarValue::Text(format_timestamp(dt)))
            }
        }
        (TypeSignature::Timestamp, ScalarValue::Int(v)) => {
            let dt = datetime_from_epoch_seconds(v);
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        (TypeSignature::Timestamp, ScalarValue::Float(v)) => {
            let dt = datetime_from_epoch_seconds(v as i64);
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        (TypeSignature::Vector(expected_dim), v) => {
            let parsed = coerce_scalar_to_vector(
                &v,
                expected_dim,
                &format!("column \"{}\"", column.name()),
            )?;
            Ok(ScalarValue::Vector(parsed))
        }
        _ => Err(EngineError {
            message: format!("type mismatch for column \"{}\"", column.name()),
        }),
    }
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
