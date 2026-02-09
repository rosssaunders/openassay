use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{OnceLock, RwLock};

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::catalog::dependency::{
    describe_table as describe_table_from_catalog,
    expand_and_order_relation_drop as expand_and_order_relation_drop_in_catalog,
    expand_table_dependency_set as expand_table_dependencies_in_catalog,
    index_backing_constraint_name as index_backing_constraint_name_in_catalog,
    plan_sequence_drop as plan_sequence_drop_in_catalog,
};
use crate::catalog::oid::Oid;
use crate::catalog::with_catalog_write;
use crate::catalog::{
    Column, ColumnSpec, IndexSpec, SearchPath, TableKind, TypeSignature, with_catalog_read,
};
use crate::parser::ast::{
    AlterSequenceAction, AlterSequenceStatement, AlterTableAction, AlterTableStatement,
    AlterViewAction, AlterViewStatement, BinaryOp, ComparisonQuantifier, ConflictTarget,
    CreateIndexStatement,
    CreateSchemaStatement, CreateSequenceStatement, CreateTableStatement, CreateViewStatement,
    DeleteStatement, DropBehavior, DropIndexStatement, DropSchemaStatement, DropSequenceStatement,
    DropTableStatement, DropViewStatement, Expr, ForeignKeyAction, InsertSource, InsertStatement,
    JoinCondition, JoinExpr, JoinType, MergeStatement, MergeWhenClause, OnConflictClause, OrderByExpr, Query,
    QueryExpr, RefreshMaterializedViewStatement, SelectItem, SelectQuantifier, SelectStatement,
    SetOperator, SetQuantifier, Statement, TableConstraint, TableExpression, TableFunctionRef,
    TableRef, TruncateStatement, TypeName, UnaryOp, UpdateStatement, WindowFrameBound,
    WindowFrameUnits, WindowSpec, ExplainStatement, SetStatement, ShowStatement,
    CreateExtensionStatement, DropExtensionStatement, CreateFunctionStatement,
    FunctionParam, FunctionReturnType, SubqueryRef, GroupByExpr,
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

type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Array(Vec<ScalarValue>),
}

impl ScalarValue {
    pub fn render(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(v) => v.to_string(),
            Self::Int(v) => v.to_string(),
            Self::Float(v) => {
                let mut text = v.to_string();
                if !text.contains('.') && !text.contains('e') && !text.contains('E') {
                    text.push_str(".0");
                }
                text
            }
            Self::Text(v) => v.clone(),
            Self::Array(values) => render_array_literal(values),
        }
    }
}

fn render_array_literal(values: &[ScalarValue]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|value| match value {
            ScalarValue::Null => "NULL".to_string(),
            _ => value.render(),
        })
        .collect();
    format!("{{{}}}", parts.join(","))
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBinaryColumn {
    pub name: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyBinarySnapshot {
    pub qualified_name: String,
    pub columns: Vec<CopyBinaryColumn>,
    pub rows: Vec<Vec<ScalarValue>>,
}

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
        Statement::CreateTable(create) => execute_create_table(create).await?,
        Statement::CreateSchema(create) => execute_create_schema(create).await?,
        Statement::CreateIndex(create) => execute_create_index(create).await?,
        Statement::CreateSequence(create) => execute_create_sequence(create).await?,
        Statement::CreateView(create) => execute_create_view(create, params).await?,
        Statement::RefreshMaterializedView(refresh) => {
            execute_refresh_materialized_view(refresh, params).await?
        }
        Statement::AlterSequence(alter) => execute_alter_sequence(alter).await?,
        Statement::AlterView(alter) => execute_alter_view(alter).await?,
        Statement::Insert(insert) => execute_insert(insert, params).await?,
        Statement::Update(update) => execute_update(update, params).await?,
        Statement::Delete(delete) => execute_delete(delete, params).await?,
        Statement::Merge(merge) => execute_merge(merge, params).await?,
        Statement::DropTable(drop_table) => execute_drop_table(drop_table).await?,
        Statement::DropSchema(drop_schema) => execute_drop_schema(drop_schema).await?,
        Statement::DropIndex(drop_index) => execute_drop_index(drop_index).await?,
        Statement::DropSequence(drop_sequence) => execute_drop_sequence(drop_sequence).await?,
        Statement::DropView(drop_view) => execute_drop_view(drop_view).await?,
        Statement::Truncate(truncate) => execute_truncate(truncate).await?,
        Statement::AlterTable(alter_table) => execute_alter_table(alter_table).await?,
        Statement::Explain(explain) => execute_explain(explain, params).await?,
        Statement::Set(set_stmt) => execute_set(set_stmt).await?,
        Statement::Show(show_stmt) => execute_show(show_stmt).await?,
        Statement::Discard(_) => QueryResult {
            columns: Vec::new(), rows: Vec::new(),
            command_tag: "DISCARD".to_string(), rows_affected: 0,
        },
        Statement::Do(_) => QueryResult {
            columns: Vec::new(), rows: Vec::new(),
            command_tag: "DO".to_string(), rows_affected: 0,
        },
        Statement::Listen(_) => QueryResult {
            columns: Vec::new(), rows: Vec::new(),
            command_tag: "LISTEN".to_string(), rows_affected: 0,
        },
        Statement::Notify(_) => QueryResult {
            columns: Vec::new(), rows: Vec::new(),
            command_tag: "NOTIFY".to_string(), rows_affected: 0,
        },
        Statement::Unlisten(_) => QueryResult {
            columns: Vec::new(), rows: Vec::new(),
            command_tag: "UNLISTEN".to_string(), rows_affected: 0,
        },
        Statement::CreateExtension(create) => execute_create_extension(create).await?,
        Statement::DropExtension(drop_ext) => execute_drop_extension(drop_ext).await?,
        Statement::CreateFunction(create) => execute_create_function(create).await?,
    };
    Ok(result)
    })
}

#[derive(Debug, Default)]
struct InMemoryStorage {
    rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
}

static GLOBAL_GUC: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn global_guc() -> &'static RwLock<HashMap<String, String>> {
    GLOBAL_GUC.get_or_init(|| {
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
        m.insert("application_name".to_string(), "".to_string());
        RwLock::new(m)
    })
}

async fn execute_set(set_stmt: &SetStatement) -> Result<QueryResult, EngineError> {
    let mut guc = global_guc().write().expect("guc lock poisoned");
    guc.insert(set_stmt.name.clone(), set_stmt.value.clone());
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "SET".to_string(),
        rows_affected: 0,
    })
}

async fn execute_show(show_stmt: &ShowStatement) -> Result<QueryResult, EngineError> {
    let guc = global_guc().read().expect("guc lock poisoned");
    let value = guc.get(&show_stmt.name)
        .or_else(|| guc.iter().find(|(k, _)| k.eq_ignore_ascii_case(&show_stmt.name)).map(|(_, v)| v))
        .cloned()
        .unwrap_or_default();
    Ok(QueryResult {
        columns: vec![show_stmt.name.clone()],
        rows: vec![vec![ScalarValue::Text(value)]],
        command_tag: "SHOW".to_string(),
        rows_affected: 0,
    })
}

async fn execute_explain(
    explain: &ExplainStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let mut plan_lines = Vec::new();
    describe_statement_plan(&explain.statement, &mut plan_lines, 0);

    if explain.analyze {
        // Actually execute and add timing
        let start = std::time::Instant::now();
        let inner_result = Box::pin(execute_planned_query(
            &plan_statement((*explain.statement).clone())?,
            params,
        ))
        .await?;
        let elapsed = start.elapsed();
        plan_lines.push("Planning Time: 0.001 ms".to_string());
        plan_lines.push(format!(
            "Execution Time: {:.3} ms",
            elapsed.as_secs_f64() * 1000.0
        ));
        plan_lines.push(format!(
            "  (actual rows={})",
            inner_result.rows.len()
        ));
    }

    let rows = plan_lines.into_iter().map(|line| vec![ScalarValue::Text(line)]).collect();
    Ok(QueryResult {
        columns: vec!["QUERY PLAN".to_string()],
        rows,
        command_tag: "EXPLAIN".to_string(),
        rows_affected: 0,
    })
}

fn describe_statement_plan(stmt: &Statement, lines: &mut Vec<String>, indent: usize) {
    let prefix = " ".repeat(indent);
    match stmt {
        Statement::Query(query) => {
            match &query.body {
                QueryExpr::Select(select) => {
                    if select.from.is_empty() {
                        lines.push(format!("{}Result  (cost=0.00..0.01 rows=1 width=0)", prefix));
                    } else {
                        lines.push(format!("{}Seq Scan  (cost=0.00..1.00 rows=1 width=0)", prefix));
                        for table_expr in &select.from {
                            if let TableExpression::Relation(rel) = table_expr {
                                lines.push(format!("{}  on {}", prefix, rel.name.join(".")));
                            }
                        }
                    }
                    if select.where_clause.is_some() {
                        lines.push(format!("{}  Filter: <condition>", prefix));
                    }
                }
                QueryExpr::SetOperation { op, .. } => {
                    lines.push(format!("{}{:?}  (cost=0.00..1.00 rows=1 width=0)", prefix, op));
                }
                QueryExpr::Nested(inner) => {
                    describe_statement_plan(&Statement::Query((**inner).clone()), lines, indent);
                }
            }
        }
        Statement::Insert(_) => lines.push(format!("{}Insert  (cost=0.00..1.00 rows=0 width=0)", prefix)),
        Statement::Update(_) => lines.push(format!("{}Update  (cost=0.00..1.00 rows=0 width=0)", prefix)),
        Statement::Delete(_) => lines.push(format!("{}Delete  (cost=0.00..1.00 rows=0 width=0)", prefix)),
        _ => lines.push(format!("{}Utility Statement", prefix)),
    }
}

static GLOBAL_STORAGE: OnceLock<RwLock<InMemoryStorage>> = OnceLock::new();

fn global_storage() -> &'static RwLock<InMemoryStorage> {
    GLOBAL_STORAGE.get_or_init(|| RwLock::new(InMemoryStorage::default()))
}

fn with_storage_read<T>(f: impl FnOnce(&InMemoryStorage) -> T) -> T {
    let storage = global_storage()
        .read()
        .expect("global storage lock poisoned for read");
    f(&storage)
}

fn with_storage_write<T>(f: impl FnOnce(&mut InMemoryStorage) -> T) -> T {
    let mut storage = global_storage()
        .write()
        .expect("global storage lock poisoned for write");
    f(&mut storage)
}

#[derive(Debug, Clone)]
struct SequenceState {
    start: i64,
    current: i64,
    increment: i64,
    min_value: i64,
    max_value: i64,
    cycle: bool,
    cache: i64,
    called: bool,
}

static GLOBAL_SEQUENCES: OnceLock<RwLock<HashMap<String, SequenceState>>> = OnceLock::new();

fn global_sequences() -> &'static RwLock<HashMap<String, SequenceState>> {
    GLOBAL_SEQUENCES.get_or_init(|| RwLock::new(HashMap::new()))
}

fn with_sequences_read<T>(f: impl FnOnce(&HashMap<String, SequenceState>) -> T) -> T {
    let sequences = global_sequences()
        .read()
        .expect("global sequences lock poisoned for read");
    f(&sequences)
}

fn with_sequences_write<T>(f: impl FnOnce(&mut HashMap<String, SequenceState>) -> T) -> T {
    let mut sequences = global_sequences()
        .write()
        .expect("global sequences lock poisoned for write");
    f(&mut sequences)
}

#[derive(Debug, Default)]
struct RefreshScheduler {
    active_relation_oids: HashSet<Oid>,
}

static GLOBAL_REFRESH_SCHEDULER: OnceLock<RwLock<RefreshScheduler>> = OnceLock::new();

fn global_refresh_scheduler() -> &'static RwLock<RefreshScheduler> {
    GLOBAL_REFRESH_SCHEDULER.get_or_init(|| RwLock::new(RefreshScheduler::default()))
}

fn with_refresh_scheduler_write<T>(f: impl FnOnce(&mut RefreshScheduler) -> T) -> T {
    let mut scheduler = global_refresh_scheduler()
        .write()
        .expect("global refresh scheduler lock poisoned for write");
    f(&mut scheduler)
}

#[derive(Debug)]
struct RefreshExecutionGuard {
    relation_oid: Oid,
}

impl Drop for RefreshExecutionGuard {
    fn drop(&mut self) {
        with_refresh_scheduler_write(|scheduler| {
            scheduler.active_relation_oids.remove(&self.relation_oid);
        });
    }
}

fn acquire_refresh_execution_guard(
    relation_oid: Oid,
    qualified_name: &str,
) -> Result<RefreshExecutionGuard, EngineError> {
    let inserted = with_refresh_scheduler_write(|scheduler| {
        scheduler.active_relation_oids.insert(relation_oid)
    });
    if !inserted {
        return Err(EngineError {
            message: format!(
                "cannot refresh materialized view \"{}\" because it is already being refreshed",
                qualified_name
            ),
        });
    }
    Ok(RefreshExecutionGuard { relation_oid })
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
struct ExtensionRecord {
    name: String,
    version: String,
    description: String,
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
mod ws_native {
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
fn native_ws_handles() -> &'static std::sync::Mutex<HashMap<i64, ws_native::NativeWsHandle>> {
    NATIVE_WS_HANDLES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Drain incoming messages from real connections into the inbound_queue
#[cfg(not(target_arch = "wasm32"))]
fn drain_native_ws_messages(conn_id: i64) {
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
struct ExtensionState {
    extensions: Vec<ExtensionRecord>,
    user_functions: Vec<UserFunction>,
    ws_connections: HashMap<i64, WsConnection>,
    ws_next_id: i64,
}

static GLOBAL_EXTENSION_STATE: OnceLock<RwLock<ExtensionState>> = OnceLock::new();

fn global_extension_state() -> &'static RwLock<ExtensionState> {
    GLOBAL_EXTENSION_STATE.get_or_init(|| RwLock::new(ExtensionState::default()))
}

fn with_ext_read<T>(f: impl FnOnce(&ExtensionState) -> T) -> T {
    let state = global_extension_state().read().expect("ext state lock poisoned");
    f(&state)
}

fn with_ext_write<T>(f: impl FnOnce(&mut ExtensionState) -> T) -> T {
    let mut state = global_extension_state().write().expect("ext state lock poisoned");
    f(&mut state)
}

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EngineStateSnapshot {
    catalog: crate::catalog::Catalog,
    rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
    sequences: HashMap<String, SequenceState>,
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
    with_refresh_scheduler_write(|scheduler| {
        scheduler.active_relation_oids.clear();
    });
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

fn require_relation_owner(table: &crate::catalog::Table) -> Result<(), EngineError> {
    let role = security::current_role();
    security::require_manage_relation(&role, table.oid(), &table.qualified_name())
        .map_err(|message| EngineError { message })
}

fn require_relation_privilege(
    table: &crate::catalog::Table,
    privilege: TablePrivilege,
) -> Result<(), EngineError> {
    let role = security::current_role();
    security::require_table_privilege(&role, table.oid(), &table.qualified_name(), privilege)
        .map_err(|message| EngineError { message })
}

async fn relation_row_visible_for_command(
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

async fn execute_create_table(
    create: &CreateTableStatement,
) -> Result<QueryResult, EngineError> {
    if create.columns.is_empty() {
        return Err(EngineError {
            message: "CREATE TABLE requires at least one column".to_string(),
        });
    }

    let (schema_name, table_name) = relation_name_for_create(&create.name)?;
    let mut transformed_columns = create.columns.clone();
    let mut identity_sequence_names = Vec::new();
    for column in &mut transformed_columns {
        if !column.identity {
            continue;
        }
        if column.default.is_some() {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" cannot specify both IDENTITY and DEFAULT",
                    column.name
                ),
            });
        }
        let sequence_name = format!(
            "{}.{}_{}_seq",
            schema_name,
            table_name,
            column.name.to_ascii_lowercase()
        );
        column.default = Some(Expr::FunctionCall {
            name: vec!["nextval".to_string()],
            args: vec![Expr::String(sequence_name.clone())],
            distinct: false,
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter: None,
            over: None,
        });
        column.nullable = false;
        identity_sequence_names.push(sequence_name);
    }
    if !identity_sequence_names.is_empty() {
        let existing = with_sequences_read(|sequences| {
            identity_sequence_names
                .iter()
                .find(|name| sequences.contains_key(*name))
                .cloned()
        });
        if let Some(name) = existing {
            return Err(EngineError {
                message: format!("sequence \"{}\" already exists", name),
            });
        }
    }

    let column_specs = transformed_columns
        .iter()
        .map(column_spec_from_ast)
        .collect::<Result<Vec<_>, _>>()?;
    let key_specs = key_constraint_specs_from_ast(&create.constraints)?;
    let foreign_key_specs = foreign_key_constraint_specs_from_ast(&create.constraints)?;

    let table_oid = with_catalog_write(|catalog| {
        catalog.create_table(
            &schema_name,
            &table_name,
            TableKind::Heap,
            column_specs,
            key_specs,
            foreign_key_specs,
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    with_storage_write(|storage| {
        storage.rows_by_table.entry(table_oid).or_default();
    });
    security::set_relation_owner(table_oid, &security::current_role());
    if !identity_sequence_names.is_empty() {
        with_sequences_write(|sequences| {
            for name in identity_sequence_names {
                sequences.insert(
                    name,
                    SequenceState {
                        start: 1,
                        current: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: i64::MAX,
                        cycle: false,
                        cache: 1,
                        called: false,
                    },
                );
            }
        });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE TABLE".to_string(),
        rows_affected: 0,
    })
}

async fn execute_create_schema(
    create: &CreateSchemaStatement,
) -> Result<QueryResult, EngineError> {
    let created = with_catalog_write(|catalog| catalog.create_schema(&create.name));
    match created {
        Ok(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE SCHEMA".to_string(),
            rows_affected: 0,
        }),
        Err(err) if create.if_not_exists && err.message.contains("already exists") => {
            Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "CREATE SCHEMA".to_string(),
                rows_affected: 0,
            })
        }
        Err(err) => Err(EngineError {
            message: err.message,
        }),
    }
}

async fn execute_create_index(
    create: &CreateIndexStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&create.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView) {
        return Err(EngineError {
            message: format!(
                "cannot CREATE INDEX on non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    require_relation_owner(&table)?;

    let index_name = create.name.to_ascii_lowercase();
    let index_columns = create
        .columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if create.unique {
        let key_spec = crate::catalog::KeyConstraintSpec {
            name: Some(index_name.clone()),
            columns: index_columns.clone(),
            primary: false,
        };
        let preview = {
            let mut preview = table.clone();
            preview
                .key_constraints_mut()
                .push(crate::catalog::KeyConstraint {
                    name: key_spec.name.clone(),
                    columns: key_spec.columns.clone(),
                    primary: false,
                });
            preview
        };
        let rows = with_storage_read(|storage| {
            storage
                .rows_by_table
                .get(&table.oid())
                .cloned()
                .unwrap_or_default()
        });
        validate_table_constraints(&preview, &rows).await?;

        with_catalog_write(|catalog| {
            catalog.add_key_constraint(table.schema_name(), table.name(), key_spec)?;
            let index = IndexSpec {
                name: index_name.clone(),
                columns: index_columns.clone(),
                unique: true,
            };
            if let Err(err) = catalog.add_index(table.schema_name(), table.name(), index) {
                let _ = catalog.drop_constraint(table.schema_name(), table.name(), &index_name);
                return Err(err);
            }
            Ok(())
        })
        .map_err(|err| EngineError {
            message: err.message,
        })?;
    } else {
        let index = IndexSpec {
            name: index_name,
            columns: index_columns,
            unique: false,
        };
        with_catalog_write(|catalog| catalog.add_index(table.schema_name(), table.name(), index))
            .map_err(|err| EngineError {
            message: err.message,
        })?;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE INDEX".to_string(),
        rows_affected: 0,
    })
}

async fn execute_create_sequence(
    create: &CreateSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&create.name)?;
    let increment = create.increment.unwrap_or(1);
    if increment == 0 {
        return Err(EngineError {
            message: "INCREMENT must not be zero".to_string(),
        });
    }
    let mut min_value = create
        .min_value
        .unwrap_or(Some(default_sequence_min_value(increment)))
        .unwrap_or_else(|| default_sequence_min_value(increment));
    let mut max_value = create
        .max_value
        .unwrap_or(Some(default_sequence_max_value(increment)))
        .unwrap_or_else(|| default_sequence_max_value(increment));
    if min_value > max_value {
        return Err(EngineError {
            message: "MINVALUE must be less than or equal to MAXVALUE".to_string(),
        });
    }
    let start = create
        .start
        .unwrap_or_else(|| default_sequence_start(increment, min_value, max_value));
    if start < min_value || start > max_value {
        return Err(EngineError {
            message: "START value is out of sequence bounds".to_string(),
        });
    }
    let cycle = create.cycle.unwrap_or(false);
    let cache = create.cache.unwrap_or(1);
    if cache <= 0 {
        return Err(EngineError {
            message: "CACHE must be greater than zero".to_string(),
        });
    }
    // Preserve explicit defaults after bounds checks in case caller provided NO MIN/MAX.
    if create.min_value == Some(None) {
        min_value = default_sequence_min_value(increment);
    }
    if create.max_value == Some(None) {
        max_value = default_sequence_max_value(increment);
    }

    let inserted = with_sequences_write(|sequences| {
        if sequences.contains_key(&key) {
            return false;
        }
        sequences.insert(
            key,
            SequenceState {
                start,
                current: start,
                increment,
                min_value,
                max_value,
                cycle,
                cache,
                called: false,
            },
        );
        true
    });
    if !inserted {
        return Err(EngineError {
            message: format!("sequence \"{}\" already exists", create.name.join(".")),
        });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE SEQUENCE".to_string(),
        rows_affected: 0,
    })
}

async fn execute_create_view(
    create: &CreateViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let (schema_name, view_name) = relation_name_for_create(&create.name)?;
    if create.or_replace {
        let existing =
            with_catalog_read(|catalog| catalog.table(&schema_name, &view_name).cloned());
        if let Some(existing) = existing {
            require_relation_owner(&existing)?;
        }
    }
    let columns = derive_query_columns(&create.query)?;
    let rows = if create.materialized && create.with_data {
        execute_query(&create.query, params).await?.rows
    } else {
        Vec::new()
    };
    let oid = with_catalog_write(|catalog| {
        if create.or_replace {
            if catalog.table(&schema_name, &view_name).is_some() {
                catalog.replace_view(
                    &schema_name,
                    &view_name,
                    create.materialized,
                    columns,
                    create.query.clone(),
                )
            } else {
                catalog.create_view(
                    &schema_name,
                    &view_name,
                    create.materialized,
                    columns,
                    create.query.clone(),
                )
            }
        } else {
            catalog.create_view(
                &schema_name,
                &view_name,
                create.materialized,
                columns,
                create.query.clone(),
            )
        }
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if create.or_replace {
        with_storage_write(|storage| {
            storage.rows_by_table.remove(&oid);
        });
    }
    if create.materialized {
        with_storage_write(|storage| {
            storage.rows_by_table.insert(oid, rows);
        });
    }
    security::set_relation_owner(oid, &security::current_role());

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if create.materialized {
            "CREATE MATERIALIZED VIEW".to_string()
        } else {
            "CREATE VIEW".to_string()
        },
        rows_affected: 0,
    })
}

async fn execute_refresh_materialized_view(
    refresh: &RefreshMaterializedViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let relation = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&refresh.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if relation.kind() != TableKind::MaterializedView {
        return Err(EngineError {
            message: format!(
                "\"{}\" is not a materialized view",
                relation.qualified_name()
            ),
        });
    }
    require_relation_owner(&relation)?;
    if refresh.concurrently && !refresh.with_data {
        return Err(EngineError {
            message: "REFRESH MATERIALIZED VIEW CONCURRENTLY does not support WITH NO DATA"
                .to_string(),
        });
    }
    if refresh.concurrently && !relation.indexes().iter().any(|index| index.unique) {
        return Err(EngineError {
            message: format!(
                "cannot refresh materialized view \"{}\" concurrently because it does not have a unique index",
                relation.qualified_name()
            ),
        });
    }
    let _refresh_guard =
        acquire_refresh_execution_guard(relation.oid(), &relation.qualified_name())?;
    let rows = if refresh.concurrently {
        evaluate_materialized_view_rows_concurrently(&relation, refresh.with_data, params).await?
    } else {
        evaluate_materialized_view_rows_live(&relation, refresh.with_data, params).await?
    };
    if refresh.concurrently {
        let still_valid = with_catalog_read(|catalog| {
            catalog
                .table(relation.schema_name(), relation.name())
                .is_some_and(|current| {
                    current.oid() == relation.oid()
                        && current.kind() == TableKind::MaterializedView
                        && current.indexes().iter().any(|index| index.unique)
                })
        });
        if !still_valid {
            return Err(EngineError {
                message: format!(
                    "cannot refresh materialized view \"{}\" concurrently because it changed during refresh",
                    relation.qualified_name()
                ),
            });
        }
    };
    with_storage_write(|storage| {
        storage.rows_by_table.insert(relation.oid(), rows);
    });
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "REFRESH MATERIALIZED VIEW".to_string(),
        rows_affected: 0,
    })
}

async fn evaluate_materialized_view_rows_live(
    relation: &crate::catalog::Table,
    with_data: bool,
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    if !with_data {
        return Ok(Vec::new());
    }
    let definition = relation.view_definition().ok_or_else(|| EngineError {
        message: format!(
            "view definition for relation \"{}\" is missing",
            relation.qualified_name()
        ),
    })?;
    Ok(execute_query(definition, params).await?.rows)
}

async fn evaluate_materialized_view_rows_concurrently(
    relation: &crate::catalog::Table,
    with_data: bool,
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    let baseline = snapshot_state();
    let evaluated = evaluate_materialized_view_rows_live(relation, with_data, params).await;
    let post_eval = snapshot_state();
    restore_state(baseline);
    with_sequences_write(|sequences| {
        *sequences = post_eval.sequences;
    });
    evaluated
}

async fn execute_alter_view(alter: &AlterViewStatement) -> Result<QueryResult, EngineError> {
    let relation = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&alter.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    let expected_kind = if alter.materialized {
        TableKind::MaterializedView
    } else {
        TableKind::View
    };
    if relation.kind() != expected_kind {
        return Err(EngineError {
            message: if alter.materialized {
                format!(
                    "\"{}\" is not a materialized view",
                    relation.qualified_name()
                )
            } else {
                format!("\"{}\" is not a view", relation.qualified_name())
            },
        });
    }
    require_relation_owner(&relation)?;

    match &alter.action {
        AlterViewAction::RenameTo { new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_relation(relation.schema_name(), relation.name(), new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterViewAction::RenameColumn { old_name, new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_column(relation.schema_name(), relation.name(), old_name, new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterViewAction::SetSchema { schema_name } => {
            with_catalog_write(|catalog| {
                catalog.move_relation_to_schema(
                    relation.schema_name(),
                    relation.name(),
                    schema_name,
                )
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if alter.materialized {
            "ALTER MATERIALIZED VIEW".to_string()
        } else {
            "ALTER VIEW".to_string()
        },
        rows_affected: 0,
    })
}

async fn execute_alter_sequence(
    alter: &AlterSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&alter.name)?;
    with_sequences_write(|sequences| {
        let Some(state) = sequences.get_mut(&key) else {
            return Err(EngineError {
                message: format!("sequence \"{}\" does not exist", key),
            });
        };
        for action in &alter.actions {
            match action {
                AlterSequenceAction::Restart { with } => {
                    state.current = with.unwrap_or(state.start);
                    state.called = false;
                }
                AlterSequenceAction::SetStart { start } => {
                    state.start = *start;
                    if !state.called {
                        state.current = *start;
                    }
                }
                AlterSequenceAction::SetIncrement { increment } => {
                    if *increment == 0 {
                        return Err(EngineError {
                            message: "INCREMENT must not be zero".to_string(),
                        });
                    }
                    state.increment = *increment;
                }
                AlterSequenceAction::SetMinValue { min } => {
                    state.min_value =
                        min.unwrap_or_else(|| default_sequence_min_value(state.increment));
                }
                AlterSequenceAction::SetMaxValue { max } => {
                    state.max_value =
                        max.unwrap_or_else(|| default_sequence_max_value(state.increment));
                }
                AlterSequenceAction::SetCycle { cycle } => {
                    state.cycle = *cycle;
                }
                AlterSequenceAction::SetCache { cache } => {
                    if *cache <= 0 {
                        return Err(EngineError {
                            message: "CACHE must be greater than zero".to_string(),
                        });
                    }
                    state.cache = *cache;
                }
            }
        }

        if state.min_value > state.max_value {
            return Err(EngineError {
                message: "MINVALUE must be less than or equal to MAXVALUE".to_string(),
            });
        }
        if state.start < state.min_value || state.start > state.max_value {
            return Err(EngineError {
                message: "START value is out of sequence bounds".to_string(),
            });
        }
        if !state.called {
            if state.current < state.min_value || state.current > state.max_value {
                return Err(EngineError {
                    message: "RESTART value is out of sequence bounds".to_string(),
                });
            }
        } else if state.current < state.min_value || state.current > state.max_value {
            return Err(EngineError {
                message: "sequence current value is out of bounds after ALTER SEQUENCE".to_string(),
            });
        }
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "ALTER SEQUENCE".to_string(),
        rows_affected: 0,
    })
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

fn drop_relations_by_oid_order(drop_order: &[Oid]) -> Result<(), EngineError> {
    for table_oid in drop_order {
        let ident = with_catalog_read(|catalog| describe_table_from_catalog(catalog, *table_oid))
            .map_err(|err| EngineError {
            message: err.message,
        })?;
        with_catalog_write(|catalog| catalog.drop_table(&ident.schema_name, &ident.table_name))
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        with_storage_write(|storage| {
            storage.rows_by_table.remove(table_oid);
        });
        security::clear_relation_security(*table_oid);
    }
    Ok(())
}

async fn execute_drop_table(
    drop_table: &DropTableStatement,
) -> Result<QueryResult, EngineError> {
    let resolved = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&drop_table.name, &SearchPath::default())
            .cloned()
    });

    let table = match resolved {
        Ok(table) => table,
        Err(_err) if drop_table.if_exists => {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP TABLE".to_string(),
                rows_affected: 0,
            });
        }
        Err(err) => {
            return Err(EngineError {
                message: err.message,
            });
        }
    };

    let drop_order = with_catalog_read(|catalog| {
        expand_and_order_relation_drop_in_catalog(
            catalog,
            &[table.oid()],
            matches!(drop_table.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        require_relation_owner(&relation_table)?;
    }
    drop_relations_by_oid_order(&drop_order)?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP TABLE".to_string(),
        rows_affected: drop_order.len() as u64,
    })
}

async fn execute_drop_schema(
    drop_schema: &DropSchemaStatement,
) -> Result<QueryResult, EngineError> {
    let schema_name = drop_schema.name.to_ascii_lowercase();
    let schema_tables = with_catalog_read(|catalog| {
        catalog
            .schema(&schema_name)
            .map(|schema| schema.tables().map(|table| table.oid()).collect::<Vec<_>>())
    });
    let Some(initial_tables) = schema_tables else {
        if drop_schema.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP SCHEMA".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("schema \"{}\" does not exist", drop_schema.name),
        });
    };

    let schema_sequences = with_sequences_read(|sequences| {
        let prefix = format!("{schema_name}.");
        sequences
            .keys()
            .filter(|name| name.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>()
    });

    if matches!(drop_schema.behavior, DropBehavior::Restrict)
        && (!initial_tables.is_empty() || !schema_sequences.is_empty())
    {
        return Err(EngineError {
            message: format!("schema \"{}\" is not empty", drop_schema.name),
        });
    }

    let mut relation_oids = initial_tables;
    if matches!(drop_schema.behavior, DropBehavior::Cascade) && !schema_sequences.is_empty() {
        let sequence_plans = with_catalog_read(|catalog| {
            schema_sequences
                .iter()
                .map(|sequence| plan_sequence_drop_in_catalog(catalog, sequence, true))
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|err| EngineError {
            message: err.message,
        })?;
        for plan in sequence_plans {
            for oid in plan.relation_drop_order {
                if !relation_oids.contains(&oid) {
                    relation_oids.push(oid);
                }
            }
        }
    }

    let drop_order = with_catalog_read(|catalog| {
        expand_and_order_relation_drop_in_catalog(
            catalog,
            &relation_oids,
            matches!(drop_schema.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        require_relation_owner(&relation_table)?;
    }
    drop_relations_by_oid_order(&drop_order)?;

    if !schema_sequences.is_empty() {
        for sequence_name in &schema_sequences {
            with_catalog_write(|catalog| {
                catalog.clear_sequence_defaults(sequence_name);
            });
        }
        with_sequences_write(|sequences| {
            for sequence_name in &schema_sequences {
                sequences.remove(sequence_name);
            }
        });
    }

    with_catalog_write(|catalog| catalog.drop_schema(&schema_name)).map_err(|err| EngineError {
        message: err.message,
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP SCHEMA".to_string(),
        rows_affected: (drop_order.len() + schema_sequences.len()) as u64,
    })
}

async fn execute_drop_index(
    drop_index: &DropIndexStatement,
) -> Result<QueryResult, EngineError> {
    let resolved = resolve_index_target(&drop_index.name)?;
    let Some(target) = resolved else {
        if drop_index.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP INDEX".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("index \"{}\" does not exist", drop_index.name.join(".")),
        });
    };
    let relation = with_catalog_read(|catalog| {
        catalog
            .table(&target.schema_name, &target.table_name)
            .cloned()
    })
    .ok_or_else(|| EngineError {
        message: format!(
            "relation \"{}.{}\" does not exist",
            target.schema_name, target.table_name
        ),
    })?;
    require_relation_owner(&relation)?;

    let backing_constraint = with_catalog_read(|catalog| {
        index_backing_constraint_name_in_catalog(
            catalog,
            &target.schema_name,
            &target.table_name,
            &target.index_name,
        )
    });
    if let Some(constraint_name) = backing_constraint
        && matches!(drop_index.behavior, DropBehavior::Restrict)
    {
        return Err(EngineError {
            message: format!(
                "cannot drop index \"{}\" because constraint \"{}\" on relation \"{}.{}\" depends on it",
                target.index_name, constraint_name, target.schema_name, target.table_name
            ),
        });
    }

    with_catalog_write(|catalog| {
        catalog.drop_index(
            &target.schema_name,
            &target.table_name,
            &target.index_name,
            matches!(drop_index.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP INDEX".to_string(),
        rows_affected: 0,
    })
}

async fn execute_drop_sequence(
    drop_sequence: &DropSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&drop_sequence.name)?;
    let exists = with_sequences_read(|sequences| sequences.contains_key(&key));
    if !exists {
        if drop_sequence.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP SEQUENCE".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("sequence \"{}\" does not exist", key),
        });
    }

    let dependency_plan = with_catalog_read(|catalog| {
        plan_sequence_drop_in_catalog(
            catalog,
            &key,
            matches!(drop_sequence.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if !dependency_plan.default_dependents.is_empty() {
        with_catalog_write(|catalog| {
            catalog.clear_sequence_defaults(&key);
        });
    }
    if !dependency_plan.relation_drop_order.is_empty() {
        drop_relations_by_oid_order(&dependency_plan.relation_drop_order)?;
    }

    with_sequences_write(|sequences| {
        sequences.remove(&key);
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP SEQUENCE".to_string(),
        rows_affected: 0,
    })
}

async fn execute_drop_view(
    drop_view: &DropViewStatement,
) -> Result<QueryResult, EngineError> {
    let expected_kind = if drop_view.materialized {
        TableKind::MaterializedView
    } else {
        TableKind::View
    };

    let mut base_oids = Vec::new();
    for name in &drop_view.names {
        let resolved = with_catalog_read(|catalog| {
            catalog.resolve_table(name, &SearchPath::default()).cloned()
        });
        let relation = match resolved {
            Ok(table) => table,
            Err(_err) if drop_view.if_exists => continue,
            Err(err) => {
                return Err(EngineError {
                    message: err.message,
                });
            }
        };
        if relation.kind() != expected_kind {
            return Err(EngineError {
                message: if drop_view.materialized {
                    format!(
                        "\"{}\" is not a materialized view",
                        relation.qualified_name()
                    )
                } else {
                    format!("\"{}\" is not a view", relation.qualified_name())
                },
            });
        }
        require_relation_owner(&relation)?;
        if !base_oids.iter().any(|oid| oid == &relation.oid()) {
            base_oids.push(relation.oid());
        }
    }
    if base_oids.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: if drop_view.materialized {
                "DROP MATERIALIZED VIEW".to_string()
            } else {
                "DROP VIEW".to_string()
            },
            rows_affected: 0,
        });
    }

    let drop_order = with_catalog_read(|catalog| {
        expand_and_order_relation_drop_in_catalog(
            catalog,
            &base_oids,
            matches!(drop_view.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        require_relation_owner(&relation_table)?;
    }
    drop_relations_by_oid_order(&drop_order)?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if drop_view.materialized {
            "DROP MATERIALIZED VIEW".to_string()
        } else {
            "DROP VIEW".to_string()
        },
        rows_affected: drop_order.len() as u64,
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

async fn execute_truncate(
    truncate: &TruncateStatement,
) -> Result<QueryResult, EngineError> {
    let mut base_table_oids = Vec::with_capacity(truncate.table_names.len());
    for table_name in &truncate.table_names {
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
                    "cannot truncate system relation \"{}\"",
                    table.qualified_name()
                ),
            });
        }
        require_relation_privilege(&table, TablePrivilege::Truncate)?;
        if !base_table_oids.iter().any(|oid| oid == &table.oid()) {
            base_table_oids.push(table.oid());
        }
    }

    let targets = with_catalog_read(|catalog| {
        expand_table_dependencies_in_catalog(
            catalog,
            &base_table_oids,
            matches!(truncate.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &targets {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        require_relation_privilege(&relation_table, TablePrivilege::Truncate)?;
    }
    with_storage_write(|storage| {
        for table_oid in &targets {
            storage.rows_by_table.insert(*table_oid, Vec::new());
        }
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "TRUNCATE".to_string(),
        rows_affected: targets.len() as u64,
    })
}

async fn execute_alter_table(
    alter_table: &AlterTableStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&alter_table.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot ALTER non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    require_relation_owner(&table)?;

    match &alter_table.action {
        AlterTableAction::AddColumn(column_def) => {
            let column_spec = column_spec_from_ast(column_def)?;
            let default_value = if let Some(default_expr) = &column_spec.default {
                let raw = eval_expr(default_expr, &EvalScope::default(), &[]).await?;
                Some(coerce_value_for_column_spec(raw, &column_spec)?)
            } else {
                None
            };
            if !column_spec.nullable && default_value.is_none() {
                let has_rows = with_storage_read(|storage| {
                    storage
                        .rows_by_table
                        .get(&table.oid())
                        .is_some_and(|rows| !rows.is_empty())
                });
                if has_rows {
                    return Err(EngineError {
                        message:
                            "ALTER TABLE ADD COLUMN with NOT NULL requires empty table or DEFAULT"
                                .to_string(),
                    });
                }
            }

            with_catalog_write(|catalog| {
                catalog.add_column(table.schema_name(), table.name(), column_spec)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;

            with_storage_write(|storage| {
                let rows = storage.rows_by_table.entry(table.oid()).or_default();
                for row in rows.iter_mut() {
                    row.push(default_value.clone().unwrap_or(ScalarValue::Null));
                }
            });
        }
        AlterTableAction::AddConstraint(constraint) => {
            let current_rows = with_storage_read(|storage| {
                storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default()
            });
            let preview = preview_table_with_added_constraint(&table, constraint)?;
            validate_table_constraints(&preview, &current_rows).await?;

            match constraint {
                TableConstraint::PrimaryKey { .. } | TableConstraint::Unique { .. } => {
                    let mut specs =
                        key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
                    let spec = specs.pop().expect("one key spec for add constraint");
                    with_catalog_write(|catalog| {
                        catalog.add_key_constraint(table.schema_name(), table.name(), spec)
                    })
                    .map_err(|err| EngineError {
                        message: err.message,
                    })?;
                }
                TableConstraint::ForeignKey { .. } => {
                    let mut specs =
                        foreign_key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
                    let spec = specs.pop().expect("one fk spec for add constraint");
                    with_catalog_write(|catalog| {
                        catalog.add_foreign_key_constraint(table.schema_name(), table.name(), spec)
                    })
                    .map_err(|err| EngineError {
                        message: err.message,
                    })?;
                }
            }
        }
        AlterTableAction::DropColumn { name } => {
            let dropped_index = with_catalog_write(|catalog| {
                catalog.drop_column(table.schema_name(), table.name(), name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;

            with_storage_write(|storage| {
                let rows = storage.rows_by_table.entry(table.oid()).or_default();
                for row in rows.iter_mut() {
                    if dropped_index < row.len() {
                        row.remove(dropped_index);
                    }
                }
            });
        }
        AlterTableAction::DropConstraint { name } => {
            with_catalog_write(|catalog| {
                catalog.drop_constraint(table.schema_name(), table.name(), name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_column(table.schema_name(), table.name(), old_name, new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::SetColumnNullable { name, nullable } => {
            if !*nullable {
                let index = find_column_index(&table, name)?;
                let has_nulls = with_storage_read(|storage| {
                    storage.rows_by_table.get(&table.oid()).is_some_and(|rows| {
                        rows.iter()
                            .any(|row| matches!(row.get(index), Some(ScalarValue::Null)))
                    })
                });
                if has_nulls {
                    return Err(EngineError {
                        message: format!(
                            "column \"{}\" of relation \"{}\" contains null values",
                            name,
                            table.qualified_name()
                        ),
                    });
                }
            }

            with_catalog_write(|catalog| {
                catalog.set_column_nullable(table.schema_name(), table.name(), name, *nullable)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::SetColumnDefault { name, default } => {
            with_catalog_write(|catalog| {
                catalog.set_column_default(table.schema_name(), table.name(), name, default.clone())
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "ALTER TABLE".to_string(),
        rows_affected: 0,
    })
}

fn relation_name_for_create(name: &[String]) -> Result<(String, String), EngineError> {
    match name {
        [table_name] => Ok(("public".to_string(), table_name.to_ascii_lowercase())),
        [schema_name, table_name] => Ok((
            schema_name.to_ascii_lowercase(),
            table_name.to_ascii_lowercase(),
        )),
        _ => Err(EngineError {
            message: format!("invalid relation name \"{}\"", name.join(".")),
        }),
    }
}

fn normalize_sequence_name(name: &[String]) -> Result<String, EngineError> {
    match name {
        [seq_name] => Ok(format!("public.{}", seq_name.to_ascii_lowercase())),
        [schema_name, seq_name] => Ok(format!(
            "{}.{}",
            schema_name.to_ascii_lowercase(),
            seq_name.to_ascii_lowercase()
        )),
        _ => Err(EngineError {
            message: format!("invalid sequence name \"{}\"", name.join(".")),
        }),
    }
}

fn normalize_sequence_name_from_text(raw: &str) -> Result<String, EngineError> {
    let parts = raw
        .split('.')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [seq_name] => Ok(format!("public.{seq_name}")),
        [schema_name, seq_name] => Ok(format!("{schema_name}.{seq_name}")),
        _ => Err(EngineError {
            message: format!("invalid sequence name \"{}\"", raw),
        }),
    }
}

#[derive(Debug, Clone)]
struct IndexTarget {
    schema_name: String,
    table_name: String,
    index_name: String,
}

fn resolve_index_target(index_name: &[String]) -> Result<Option<IndexTarget>, EngineError> {
    match index_name {
        [name] => {
            let normalized_name = name.to_ascii_lowercase();
            let mut matches = Vec::new();
            with_catalog_read(|catalog| {
                for schema_name in SearchPath::default().schemas() {
                    let Some(schema) = catalog.schema(schema_name) else {
                        continue;
                    };
                    for table in schema.tables() {
                        if table
                            .indexes()
                            .iter()
                            .any(|index| index.name == normalized_name)
                        {
                            matches.push(IndexTarget {
                                schema_name: schema.name().to_string(),
                                table_name: table.name().to_string(),
                                index_name: normalized_name.clone(),
                            });
                        }
                    }
                }
            });
            if matches.len() > 1 {
                return Err(EngineError {
                    message: format!("index reference \"{}\" is ambiguous", name),
                });
            }
            Ok(matches.pop())
        }
        [schema_name, name] => {
            let normalized_schema = schema_name.to_ascii_lowercase();
            let normalized_name = name.to_ascii_lowercase();
            let mut found = None;
            with_catalog_read(|catalog| {
                if let Some(schema) = catalog.schema(&normalized_schema) {
                    for table in schema.tables() {
                        if table
                            .indexes()
                            .iter()
                            .any(|index| index.name == normalized_name)
                        {
                            found = Some(IndexTarget {
                                schema_name: normalized_schema.clone(),
                                table_name: table.name().to_string(),
                                index_name: normalized_name.clone(),
                            });
                            break;
                        }
                    }
                }
            });
            Ok(found)
        }
        _ => Err(EngineError {
            message: format!("invalid index name \"{}\"", index_name.join(".")),
        }),
    }
}

fn default_sequence_min_value(increment: i64) -> i64 {
    if increment > 0 { 1 } else { i64::MIN }
}

fn default_sequence_max_value(increment: i64) -> i64 {
    if increment > 0 { i64::MAX } else { -1 }
}

fn default_sequence_start(increment: i64, min_value: i64, max_value: i64) -> i64 {
    if increment > 0 { min_value } else { max_value }
}

fn column_spec_from_ast(
    column: &crate::parser::ast::ColumnDefinition,
) -> Result<ColumnSpec, EngineError> {
    if column.name.trim().is_empty() {
        return Err(EngineError {
            message: "column name cannot be empty".to_string(),
        });
    }

    let references = if let Some(reference) = &column.references {
        if reference.table_name.is_empty() {
            return Err(EngineError {
                message: format!("column \"{}\" has invalid REFERENCES target", column.name),
            });
        }
        Some(crate::catalog::ForeignKeySpec {
            table_name: reference
                .table_name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect(),
            column_name: reference
                .column_name
                .as_ref()
                .map(|name| name.to_ascii_lowercase()),
            on_delete: reference.on_delete,
            on_update: reference.on_update,
        })
    } else {
        None
    };

    Ok(ColumnSpec {
        name: column.name.to_ascii_lowercase(),
        type_signature: type_signature_from_ast(column.data_type.clone()),
        nullable: column.nullable && !column.primary_key,
        unique: column.unique || column.primary_key,
        primary_key: column.primary_key,
        references,
        check: column.check.clone(),
        default: column.default.clone(),
    })
}

fn key_constraint_specs_from_ast(
    constraints: &[TableConstraint],
) -> Result<Vec<crate::catalog::KeyConstraintSpec>, EngineError> {
    let mut out = Vec::new();
    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey { name, columns } => {
                if columns.is_empty() {
                    return Err(EngineError {
                        message: "PRIMARY KEY requires at least one column".to_string(),
                    });
                }
                out.push(crate::catalog::KeyConstraintSpec {
                    name: name.as_ref().map(|name| name.to_ascii_lowercase()),
                    columns: columns
                        .iter()
                        .map(|column| column.to_ascii_lowercase())
                        .collect(),
                    primary: true,
                });
            }
            TableConstraint::Unique { name, columns } => {
                if columns.is_empty() {
                    return Err(EngineError {
                        message: "UNIQUE requires at least one column".to_string(),
                    });
                }
                out.push(crate::catalog::KeyConstraintSpec {
                    name: name.as_ref().map(|name| name.to_ascii_lowercase()),
                    columns: columns
                        .iter()
                        .map(|column| column.to_ascii_lowercase())
                        .collect(),
                    primary: false,
                });
            }
            TableConstraint::ForeignKey { .. } => {}
        }
    }
    Ok(out)
}

fn foreign_key_constraint_specs_from_ast(
    constraints: &[TableConstraint],
) -> Result<Vec<crate::catalog::ForeignKeyConstraintSpec>, EngineError> {
    let mut out = Vec::new();
    for constraint in constraints {
        let TableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } = constraint
        else {
            continue;
        };
        if columns.is_empty() {
            return Err(EngineError {
                message: "FOREIGN KEY requires at least one referencing column".to_string(),
            });
        }
        if referenced_table.is_empty() {
            return Err(EngineError {
                message: "FOREIGN KEY REFERENCES target cannot be empty".to_string(),
            });
        }
        if !referenced_columns.is_empty() && columns.len() != referenced_columns.len() {
            return Err(EngineError {
                message: format!(
                    "FOREIGN KEY has {} referencing columns but {} referenced columns",
                    columns.len(),
                    referenced_columns.len()
                ),
            });
        }

        out.push(crate::catalog::ForeignKeyConstraintSpec {
            name: name.as_ref().map(|name| name.to_ascii_lowercase()),
            columns: columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect(),
            referenced_table: referenced_table
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect(),
            referenced_columns: referenced_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect(),
            on_delete: *on_delete,
            on_update: *on_update,
        });
    }
    Ok(out)
}

fn type_signature_from_ast(ty: TypeName) -> TypeSignature {
    match ty {
        TypeName::Bool => TypeSignature::Bool,
        TypeName::Int2 | TypeName::Int4 | TypeName::Int8 | TypeName::Serial | TypeName::BigSerial => TypeSignature::Int8,
        TypeName::Float4 | TypeName::Float8 | TypeName::Numeric => TypeSignature::Float8,
        TypeName::Text | TypeName::Varchar | TypeName::Char | TypeName::Bytea | TypeName::Uuid
        | TypeName::Json | TypeName::Jsonb | TypeName::Interval => TypeSignature::Text,
        TypeName::Date => TypeSignature::Date,
        TypeName::Timestamp | TypeName::TimestampTz => TypeSignature::Timestamp,
    }
}

fn type_signature_to_oid(ty: TypeSignature) -> u32 {
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

fn find_column_index(
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

async fn validate_table_constraints(
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

fn preview_table_with_added_constraint(
    table: &crate::catalog::Table,
    constraint: &TableConstraint,
) -> Result<crate::catalog::Table, EngineError> {
    let mut preview = table.clone();
    match constraint {
        TableConstraint::PrimaryKey { .. } | TableConstraint::Unique { .. } => {
            let mut specs = key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
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
            let mut specs =
                foreign_key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
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

fn coerce_value_for_column_spec(
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

fn derive_query_columns(query: &Query) -> Result<Vec<String>, EngineError> {
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

fn query_references_relation(query: &Query, relation_name: &str) -> bool {
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

fn validate_recursive_cte_terms(
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

fn derive_select_columns(
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
struct ExpandedFromColumn {
    label: String,
    lookup_parts: Vec<String>,
}

#[derive(Debug, Clone)]
struct CteBinding {
    columns: Vec<String>,
    rows: Vec<Vec<ScalarValue>>,
}

thread_local! {
    static ACTIVE_CTE_STACK: RefCell<Vec<HashMap<String, CteBinding>>> = const { RefCell::new(Vec::new()) };
}

fn current_cte_binding(name: &str) -> Option<CteBinding> {
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

async fn with_cte_context_async<T, F, Fut>(ctes: HashMap<String, CteBinding>, f: F) -> T
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

fn active_cte_context() -> HashMap<String, CteBinding> {
    ACTIVE_CTE_STACK.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}

#[derive(Debug, Clone)]
struct VirtualRelationColumnDef {
    name: String,
    type_oid: u32,
}

fn lookup_virtual_relation(
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

fn expand_from_columns(
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

async fn execute_query(
    query: &Query,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    execute_query_with_outer(query, params, None).await
}

fn execute_query_with_outer<'a>(
    query: &'a Query,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<QueryResult, EngineError>> {
    Box::pin(async move {
        let inherited_ctes = active_cte_context();
        let mut local_ctes = inherited_ctes.clone();

        if let Some(with) = &query.with {
            for cte in &with.ctes {
                let cte_name = cte.name.to_ascii_lowercase();
                let binding =
                    if with.recursive && query_references_relation(&cte.query, &cte_name) {
                        evaluate_recursive_cte_binding(cte, params, outer_scope, &local_ctes)
                            .await?
                    } else {
                        let cte_result =
                            with_cte_context_async(local_ctes.clone(), || async {
                                execute_query_with_outer(&cte.query, params, outer_scope).await
                            })
                            .await?;
                        CteBinding {
                            columns: cte_result.columns.clone(),
                            rows: cte_result.rows.clone(),
                        }
                    };
                local_ctes.insert(cte_name, binding);
            }
        }

        with_cte_context_async(local_ctes, || async {
            let mut result = execute_query_expr_with_outer(&query.body, params, outer_scope).await?;
            apply_order_by(&mut result, query, params).await?;
            apply_offset_limit(&mut result, query, params).await?;
            Ok(result)
        })
        .await
    })
}

async fn evaluate_recursive_cte_binding(
    cte: &crate::parser::ast::CommonTableExpr,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    inherited_ctes: &HashMap<String, CteBinding>,
) -> Result<CteBinding, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier,
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

    let seed = with_cte_context_async(inherited_ctes.clone(), || async {
        execute_query_expr_with_outer(left, params, outer_scope).await
    })
    .await?;
    let columns = seed.columns.clone();
    let mut all_rows = if matches!(quantifier, SetQuantifier::Distinct) {
        dedupe_rows(seed.rows.clone())
    } else {
        seed.rows.clone()
    };
    let mut working_rows = all_rows.clone();

    let max_iterations = 10_000usize;
    let mut iterations = 0usize;
    while !working_rows.is_empty() {
        if iterations >= max_iterations {
            return Err(EngineError {
                message: format!(
                    "recursive query \"{}\" exceeded {} iterations",
                    cte.name, max_iterations
                ),
            });
        }
        iterations += 1;

        let mut context = inherited_ctes.clone();
        context.insert(
            cte_name.clone(),
            CteBinding {
                columns: columns.clone(),
                rows: working_rows.clone(),
            },
        );
        let recursive_term = with_cte_context_async(context, || async {
            execute_query_expr_with_outer(right, params, outer_scope).await
        })
        .await?;

        if recursive_term.columns.len() != columns.len() {
            return Err(EngineError {
                message: "set-operation inputs must have matching column counts".to_string(),
            });
        }

        let mut next_rows = recursive_term.rows;
        if matches!(quantifier, SetQuantifier::Distinct) {
            let mut seen = all_rows
                .iter()
                .map(|row| row_key(row))
                .collect::<HashSet<_>>();
            let mut filtered = Vec::new();
            for row in next_rows {
                let key = row_key(&row);
                if seen.insert(key) {
                    filtered.push(row);
                }
            }
            next_rows = filtered;
        }

        if next_rows.is_empty() {
            break;
        }
        all_rows.extend(next_rows.iter().cloned());
        working_rows = next_rows;
    }

    Ok(CteBinding {
        columns,
        rows: all_rows,
    })
}

fn execute_query_expr_with_outer<'a>(
    expr: &'a QueryExpr,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<QueryResult, EngineError>> {
    Box::pin(async move {
        match expr {
            QueryExpr::Select(select) => execute_select(select, params, outer_scope).await,
            QueryExpr::Nested(query) => execute_query_with_outer(query, params, outer_scope).await,
            QueryExpr::SetOperation {
                left,
                op,
                quantifier,
                right,
            } => execute_set_operation(left, *op, *quantifier, right, params, outer_scope).await,
        }
    })
}

async fn execute_select(
    select: &SelectStatement,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let cte_columns = active_cte_context()
        .into_iter()
        .map(|(name, binding)| (name, binding.columns))
        .collect::<HashMap<_, _>>();
    let has_wildcard = select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard));
    let wildcard_columns = if has_wildcard {
        Some(expand_from_columns(&select.from, &cte_columns)?)
    } else {
        None
    };
    let columns = derive_select_columns(select, &cte_columns)?;
    let mut rows = Vec::new();
    let mut source_rows = if select.from.is_empty() {
        vec![outer_scope.cloned().unwrap_or_default()]
    } else {
        evaluate_from_clause(&select.from, params, outer_scope).await?
    };
    if let Some(outer) = outer_scope
        && !select.from.is_empty()
    {
        for scope in &mut source_rows {
            scope.inherit_outer(outer);
        }
    }

    let has_aggregate = select
        .targets
        .iter()
        .any(|target| contains_aggregate_expr(&target.expr))
        || select.having.as_ref().is_some_and(contains_aggregate_expr);
    let has_window = select
        .targets
        .iter()
        .any(|target| contains_window_expr(&target.expr));

    if select.where_clause.as_ref().is_some_and(contains_window_expr) {
        return Err(EngineError {
            message: "window functions are not allowed in WHERE".to_string(),
        });
    }
    if group_by_contains_window_expr(&select.group_by) {
        return Err(EngineError {
            message: "window functions are not allowed in GROUP BY".to_string(),
        });
    }
    if select.having.as_ref().is_some_and(contains_window_expr) {
        return Err(EngineError {
            message: "window functions are not allowed in HAVING".to_string(),
        });
    }

    let mut filtered_rows = Vec::new();
    for scope in source_rows {
        if let Some(predicate) = &select.where_clause
            && !truthy(&eval_expr(predicate, &scope, params).await?)
        {
            continue;
        }
        filtered_rows.push(scope);
    }

    if !select.group_by.is_empty() || has_aggregate {
        if has_wildcard {
            return Err(EngineError {
                message: "wildcard target with grouped/aggregate projection is not implemented"
                    .to_string(),
            });
        }

        for expr in group_by_exprs(&select.group_by) {
            if contains_aggregate_expr(expr) {
                return Err(EngineError {
                    message: "aggregate functions are not allowed in GROUP BY".to_string(),
                });
            }
        }
        let grouping_sets = expand_grouping_sets(&select.group_by);
        let all_grouping = collect_grouping_identifiers(&select.group_by);

        for grouping_set in grouping_sets {
            let current_grouping: HashSet<String> = grouping_set
                .iter()
                .filter_map(|expr| identifier_key(expr))
                .collect();
            let grouping_context = GroupingContext {
                current_grouping,
                all_grouping: all_grouping.clone(),
            };

            let mut groups: Vec<Vec<EvalScope>> = Vec::new();
            if grouping_set.is_empty() {
                groups.push(filtered_rows.clone());
            } else {
                let mut index_by_key: HashMap<String, usize> = HashMap::new();
                for scope in &filtered_rows {
                    let key_values = grouping_set
                        .iter()
                        .map(|expr| eval_expr(expr, scope, params))
                        .collect::<Vec<_>>();
                    let key_values = {
                        let mut values = Vec::with_capacity(key_values.len());
                        for value in key_values {
                            values.push(value.await?);
                        }
                        values
                    };
                    let key = row_key(&key_values);
                    let idx = if let Some(existing) = index_by_key.get(&key) {
                        *existing
                    } else {
                        let idx = groups.len();
                        groups.push(Vec::new());
                        index_by_key.insert(key, idx);
                        idx
                    };
                    groups[idx].push(scope.clone());
                }
            }

            for group_rows in groups {
                let representative = group_rows.first().cloned().unwrap_or_default();
                if let Some(having) = &select.having {
                    let having_value = eval_group_expr(
                        having,
                        &group_rows,
                        &representative,
                        params,
                        &grouping_context,
                    )
                    .await?;
                    if !truthy(&having_value) {
                        continue;
                    }
                }

                let mut row = Vec::new();
                for target in &select.targets {
                    if matches!(target.expr, Expr::Wildcard) {
                        return Err(EngineError {
                            message: "wildcard target is not yet implemented in executor"
                                .to_string(),
                        });
                    }
                    row.push(
                        eval_group_expr(
                            &target.expr,
                            &group_rows,
                            &representative,
                            params,
                            &grouping_context,
                        )
                        .await?,
                    );
                }
                rows.push(row);
            }
        }
    } else if has_window {
        for (row_idx, scope) in filtered_rows.iter().enumerate() {
            let row = project_select_row_with_window(
                &select.targets,
                scope,
                row_idx,
                &filtered_rows,
                params,
                wildcard_columns.as_deref(),
            )
            .await?;
            rows.push(row);
        }
    } else {
        for scope in filtered_rows {
            let row = project_select_row(
                &select.targets,
                &scope,
                params,
                wildcard_columns.as_deref(),
            )
            .await?;
            rows.push(row);
        }
    }

    if matches!(select.quantifier, Some(SelectQuantifier::Distinct)) {
        if !select.distinct_on.is_empty() {
            // DISTINCT ON: keep first row for each distinct value of the ON expressions
            // Evaluate the exprs against a scope built from the projected columns
            let mut seen = HashSet::new();
            let mut deduped = Vec::new();
            for row in &rows {
                let scope = scope_from_row(&columns, row, &[], &columns);
                let mut key_parts = Vec::new();
                for expr in &select.distinct_on {
                    let val = eval_expr(expr, &scope, params).await?;
                    key_parts.push(val.render());
                }
                let key_str = key_parts.join("\0");
                if seen.insert(key_str) {
                    deduped.push(row.clone());
                }
            }
            rows = deduped;
        } else {
            rows = dedupe_rows(rows);
        }
    }

    Ok(QueryResult {
        columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}

#[derive(Debug, Clone)]
struct TableEval {
    rows: Vec<EvalScope>,
    columns: Vec<String>,
    null_scope: EvalScope,
}

async fn evaluate_from_clause(
    from: &[TableExpression],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<Vec<EvalScope>, EngineError> {
    let mut current = vec![EvalScope::default()];
    for item in from {
        let mut next = Vec::new();
        match item {
            TableExpression::Function(_) | TableExpression::Subquery(SubqueryRef { lateral: true, .. }) => {
                // Table functions in FROM may reference prior FROM bindings; evaluate per lhs scope.
                for lhs_scope in &current {
                    let mut merged_outer = lhs_scope.clone();
                    if let Some(outer) = outer_scope {
                        merged_outer.inherit_outer(outer);
                    }
                    let rhs =
                        evaluate_table_expression(item, params, Some(&merged_outer)).await?;
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
            _ => {
                let rhs = evaluate_table_expression(item, params, outer_scope).await?;
                for lhs_scope in &current {
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
        }
        current = next;
    }
    Ok(current)
}

fn evaluate_table_expression<'a>(
    table: &'a TableExpression,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<TableEval, EngineError>> {
    Box::pin(async move {
        match table {
            TableExpression::Relation(rel) => evaluate_relation(rel, params, outer_scope).await,
            TableExpression::Function(function) => {
                evaluate_table_function(function, params, outer_scope).await
            }
            TableExpression::Subquery(sub) => {
                let result = execute_query_with_outer(&sub.query, params, outer_scope).await?;
                let qualifiers = sub
                    .alias
                    .as_ref()
                    .map(|alias| vec![alias.to_ascii_lowercase()])
                    .unwrap_or_default();
                let mut rows = Vec::with_capacity(result.rows.len());
                for row in &result.rows {
                    rows.push(scope_from_row(
                        &result.columns,
                        row,
                        &qualifiers,
                        &result.columns,
                    ));
                }
                let null_values = vec![ScalarValue::Null; result.columns.len()];
                let null_scope =
                    scope_from_row(&result.columns, &null_values, &qualifiers, &result.columns);

                Ok(TableEval {
                    rows,
                    columns: result.columns,
                    null_scope,
                })
            }
            TableExpression::Join(join) => {
                let left = evaluate_table_expression(&join.left, params, outer_scope).await?;
                if is_lateral_table_expression(&join.right) {
                    evaluate_lateral_join(join, &left, params, outer_scope).await
                } else {
                    let right = evaluate_table_expression(&join.right, params, outer_scope).await?;
                    evaluate_join(
                        join.kind,
                        join.condition.as_ref(),
                        join.natural,
                        &left,
                        &right,
                        params,
                    )
                    .await
                }
            }
        }
    })
}

fn is_lateral_table_expression(table: &TableExpression) -> bool {
    matches!(table, TableExpression::Subquery(SubqueryRef { lateral: true, .. }))
}

async fn evaluate_lateral_join(
    join: &JoinExpr,
    left: &TableEval,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    if matches!(join.kind, JoinType::Right | JoinType::Full) {
        return Err(EngineError {
            message: "RIGHT/FULL JOIN with LATERAL is not supported".to_string(),
        });
    }

    let mut right_columns: Option<Vec<String>> = None;
    let mut right_null_scope: Option<EvalScope> = None;
    let mut using_columns: Option<Vec<String>> = None;
    let mut using_set: Option<HashSet<String>> = None;
    let empty_set: HashSet<String> = HashSet::new();
    let mut output_rows = Vec::new();

    if left.rows.is_empty() {
        let mut merged_outer = left.null_scope.clone();
        if let Some(outer) = outer_scope {
            merged_outer.inherit_outer(outer);
        }
        let right_eval = evaluate_table_expression(&join.right, params, Some(&merged_outer)).await?;
        right_columns = Some(right_eval.columns.clone());
        right_null_scope = Some(right_eval.null_scope.clone());
    } else {
        for left_row in &left.rows {
            let mut merged_outer = left_row.clone();
            if let Some(outer) = outer_scope {
                merged_outer.inherit_outer(outer);
            }
            let right_eval =
                evaluate_table_expression(&join.right, params, Some(&merged_outer)).await?;

            if right_columns.is_none() {
                right_columns = Some(right_eval.columns.clone());
                right_null_scope = Some(right_eval.null_scope.clone());
                let cols = if join.natural {
                    left.columns
                        .iter()
                        .filter(|c| right_eval.columns.iter().any(|r| r.eq_ignore_ascii_case(c)))
                        .cloned()
                        .collect::<Vec<_>>()
                } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                    cols.clone()
                } else {
                    Vec::new()
                };
                let set = cols
                    .iter()
                    .map(|c| c.to_ascii_lowercase())
                    .collect::<HashSet<_>>();
                using_columns = Some(cols);
                using_set = Some(set);
            } else if right_columns.as_ref() != Some(&right_eval.columns) {
                return Err(EngineError {
                    message: "LATERAL subquery returned inconsistent columns".to_string(),
                });
            }

            let mut left_matched = false;
            let cols = using_columns.as_deref().unwrap_or(&[]);
            let set = using_set.as_ref().unwrap_or(&empty_set);
            for right_row in &right_eval.rows {
                let matches = match join.kind {
                    JoinType::Cross => true,
                    _ => join_condition_matches(
                        join.condition.as_ref(),
                        cols,
                        left_row,
                        right_row,
                        params,
                    )
                    .await?,
                };
                if matches {
                    left_matched = true;
                    output_rows.push(combine_scopes(left_row, right_row, set));
                }
            }

            if !left_matched && matches!(join.kind, JoinType::Left)
                && let Some(null_scope) = right_null_scope.as_ref()
            {
                output_rows.push(combine_scopes(left_row, null_scope, set));
            }
        }
    }

    let right_columns = right_columns.unwrap_or_default();
    let using_set = using_set.unwrap_or_default();
    let mut output_columns = left.columns.clone();
    for col in &right_columns {
        if using_set.contains(&col.to_ascii_lowercase()) {
            continue;
        }
        output_columns.push(col.clone());
    }
    let null_scope = combine_scopes(
        &left.null_scope,
        &right_null_scope.unwrap_or_default(),
        &using_set,
    );

    Ok(TableEval {
        rows: output_rows,
        columns: output_columns,
        null_scope,
    })
}

async fn evaluate_table_function(
    function: &TableFunctionRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    let mut scope = EvalScope::default();
    if let Some(outer) = outer_scope {
        scope.inherit_outer(outer);
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        args.push(eval_expr(arg, &scope, params).await?);
    }

    let (mut columns, rows) = evaluate_set_returning_function(function, &args).await?;
    if !function.column_aliases.is_empty() {
        if function.column_aliases.len() != columns.len() {
            return Err(EngineError {
                message: format!(
                    "table function {} expects {} column aliases, got {}",
                    function
                        .name
                        .last()
                        .map(String::as_str)
                        .unwrap_or("function"),
                    columns.len(),
                    function.column_aliases.len()
                ),
            });
        }
        columns = function.column_aliases.clone();
    }

    let qualifiers = function
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .or_else(|| {
            function
                .name
                .last()
                .map(|name| vec![name.to_ascii_lowercase()])
        })
        .unwrap_or_default();
    let mut scoped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
    })
}

async fn evaluate_set_returning_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();

    match fn_name.as_str() {
        "json_array_elements" | "jsonb_array_elements" => {
            eval_json_array_elements_set_function(args, false, &fn_name)
        }
        "json_array_elements_text" | "jsonb_array_elements_text" => {
            eval_json_array_elements_set_function(args, true, &fn_name)
        }
        "json_each" | "jsonb_each" => eval_json_each_set_function(args, false, &fn_name),
        "json_each_text" | "jsonb_each_text" => eval_json_each_set_function(args, true, &fn_name),
        "json_object_keys" | "jsonb_object_keys" => {
            eval_json_object_keys_set_function(args, &fn_name)
        }
        "jsonb_path_query" => eval_jsonb_path_query_set_function(args, &fn_name),
        "json_to_record" | "jsonb_to_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, false)
        }
        "json_to_recordset" | "jsonb_to_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, false)
        }
        "json_populate_record" | "jsonb_populate_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, true)
        }
        "json_populate_recordset" | "jsonb_populate_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, true)
        }
        "regexp_matches" => eval_regexp_matches_set_function(args, &fn_name),
        "regexp_split_to_table" => eval_regexp_split_to_table_set_function(args, &fn_name),
        "generate_series" => eval_generate_series(args, &fn_name),
        "unnest" => eval_unnest_set_function(args, &fn_name),
        "pg_get_keywords" => eval_pg_get_keywords(),
        "messages" if function.name.len() == 2 && function.name[0].eq_ignore_ascii_case("ws") => {
            execute_ws_messages(args).await
        }
        _ => Err(EngineError {
            message: format!(
                "unsupported set-returning table function {}",
                function
                    .name
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
        }),
    }
}

fn eval_json_array_elements_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };

    let rows = items
        .iter()
        .map(|item| {
            let value = if text_mode {
                json_value_text_output(item)
            } else {
                ScalarValue::Text(item.to_string())
            };
            vec![value]
        })
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

fn eval_json_each_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string(), "value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .iter()
        .map(|(key, value)| {
            let value_col = if text_mode {
                json_value_text_output(value)
            } else {
                ScalarValue::Text(value.to_string())
            };
            vec![ScalarValue::Text(key.clone()), value_col]
        })
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string(), "value".to_string()], rows))
}

fn eval_json_object_keys_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .keys()
        .map(|key| vec![ScalarValue::Text(key.clone())])
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string()], rows))
}

fn json_value_to_scalar(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(v) => ScalarValue::Bool(*v),
        JsonValue::Number(n) => {
            if let Some(int) = n.as_i64() {
                ScalarValue::Int(int)
            } else if let Some(float) = n.as_f64() {
                ScalarValue::Float(float)
            } else {
                ScalarValue::Text(n.to_string())
            }
        }
        JsonValue::String(v) => ScalarValue::Text(v.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

fn eval_jsonb_path_query_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let values = jsonb_path_query_values(args, fn_name)?;
    let rows = values
        .into_iter()
        .map(|value| vec![ScalarValue::Text(value.to_string())])
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

fn eval_json_record_table_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
    fn_name: &str,
    recordset: bool,
    populate: bool,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if function.column_aliases.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires column aliases (for example AS t(col1, col2))"),
        });
    }
    let expected_args = if populate { 2 } else { 1 };
    if args.len() != expected_args {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly {expected_args} argument(s)"),
        });
    }

    let mut base = JsonMap::new();
    if populate && !matches!(args[0], ScalarValue::Null) {
        let base_value = parse_json_document_arg(&args[0], fn_name, 1)?;
        let JsonValue::Object(base_obj) = base_value else {
            return Err(EngineError {
                message: format!("{fn_name}() base argument must be a JSON object"),
            });
        };
        base = base_obj;
    }

    let json_arg_idx = if populate { 2 } else { 1 };
    if matches!(args[json_arg_idx - 1], ScalarValue::Null) {
        if recordset {
            return Ok((function.column_aliases.clone(), Vec::new()));
        }
        return Ok((
            function.column_aliases.clone(),
            vec![vec![ScalarValue::Null; function.column_aliases.len()]],
        ));
    }

    let source = parse_json_document_arg(&args[json_arg_idx - 1], fn_name, json_arg_idx)?;
    let objects = if recordset {
        let JsonValue::Array(items) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an array of objects"),
            });
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Object(map) = item else {
                return Err(EngineError {
                    message: format!("{fn_name}() JSON input must be an array of objects"),
                });
            };
            out.push(map);
        }
        out
    } else {
        let JsonValue::Object(map) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an object"),
            });
        };
        vec![map]
    };

    let mut rows = Vec::with_capacity(objects.len());
    for object in objects {
        let mut merged = base.clone();
        for (key, value) in object {
            merged.insert(key, value);
        }
        let mut row = Vec::with_capacity(function.column_aliases.len());
        for (idx, column) in function.column_aliases.iter().enumerate() {
            let mut value = merged
                .get(column)
                .map(json_value_to_scalar)
                .unwrap_or(ScalarValue::Null);
            if let Some(type_name) = function
                .column_alias_types
                .get(idx)
                .and_then(|entry| entry.as_deref())
            {
                value = eval_cast_scalar(value, type_name)?;
            }
            row.push(value);
        }
        rows.push(row);
    }

    Ok((function.column_aliases.clone(), rows))
}

fn eval_generate_series(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok((vec!["generate_series".to_string()], Vec::new()));
    }
    // Try integer series first
    let start = match &args[0] {
        ScalarValue::Int(i) => *i as f64,
        ScalarValue::Float(f) => *f,
        _ => return Err(EngineError { message: format!("{fn_name}() expects numeric arguments") }),
    };
    let stop = match &args[1] {
        ScalarValue::Int(i) => *i as f64,
        ScalarValue::Float(f) => *f,
        _ => return Err(EngineError { message: format!("{fn_name}() expects numeric arguments") }),
    };
    let step = if args.len() == 3 {
        match &args[2] {
            ScalarValue::Int(i) => *i as f64,
            ScalarValue::Float(f) => *f,
            _ => return Err(EngineError { message: format!("{fn_name}() expects numeric step") }),
        }
    } else {
        if start <= stop { 1.0 } else { -1.0 }
    };
    if step == 0.0 {
        return Err(EngineError { message: "step size cannot be zero".to_string() });
    }
    let use_int = matches!((&args[0], &args[1]), (ScalarValue::Int(_), ScalarValue::Int(_)))
        && (args.len() < 3 || matches!(&args[2], ScalarValue::Int(_)));
    let mut rows = Vec::new();
    let mut current = start;
    let max_rows = 1_000_000;
    loop {
        if rows.len() >= max_rows { break; }
        if step > 0.0 && current > stop { break; }
        if step < 0.0 && current < stop { break; }
        if use_int {
            rows.push(vec![ScalarValue::Int(current as i64)]);
        } else {
            rows.push(vec![ScalarValue::Float(current)]);
        }
        current += step;
    }
    Ok((vec!["generate_series".to_string()], rows))
}

fn eval_unnest_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError { message: format!("{fn_name}() expects one argument") });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let text = args[0].render();
    let inner = text.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let rows: Vec<Vec<ScalarValue>> = inner.split(',').map(|p| {
        let p = p.trim();
        if p == "NULL" {
            vec![ScalarValue::Null]
        } else {
            vec![ScalarValue::Text(p.to_string())]
        }
    }).collect();
    Ok((vec!["unnest".to_string()], rows))
}

fn eval_pg_get_keywords() -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let keywords = vec![
        ("select", "R", "reserved"),
        ("from", "R", "reserved"),
        ("where", "R", "reserved"),
        ("insert", "U", "unreserved"),
        ("update", "U", "unreserved"),
        ("delete", "U", "unreserved"),
        ("create", "U", "unreserved"),
        ("drop", "U", "unreserved"),
        ("alter", "U", "unreserved"),
        ("table", "U", "unreserved"),
    ];
    let columns = vec!["word".to_string(), "catcode".to_string(), "catdesc".to_string()];
    let rows = keywords.into_iter().map(|(w, c, d)| {
        vec![ScalarValue::Text(w.to_string()), ScalarValue::Text(c.to_string()), ScalarValue::Text(d.to_string())]
    }).collect();
    Ok((columns, rows))
}

async fn evaluate_relation(
    rel: &TableRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    if rel.name.len() == 1
        && let Some(cte) = current_cte_binding(&rel.name[0])
    {
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![rel.name[0].to_ascii_lowercase()]
        };

        let mut scoped_rows = Vec::with_capacity(cte.rows.len());
        for row in &cte.rows {
            scoped_rows.push(scope_from_row(&cte.columns, row, &qualifiers, &cte.columns));
        }
        let null_values = vec![ScalarValue::Null; cte.columns.len()];
        let null_scope = scope_from_row(&cte.columns, &null_values, &qualifiers, &cte.columns);

        return Ok(TableEval {
            rows: scoped_rows,
            columns: cte.columns,
            null_scope,
        });
    }

    if let Some((schema_name, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
        let rows = virtual_relation_rows(&schema_name, &relation_name)?;
        let column_names = columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<Vec<_>>();
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![
                relation_name.to_ascii_lowercase(),
                format!("{}.{}", schema_name, relation_name),
            ]
        };
        let mut scoped_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            scoped_rows.push(scope_from_row(
                &column_names,
                row,
                &qualifiers,
                &column_names,
            ));
        }
        let null_values = vec![ScalarValue::Null; column_names.len()];
        let null_scope = scope_from_row(&column_names, &null_values, &qualifiers, &column_names);
        return Ok(TableEval {
            rows: scoped_rows,
            columns: column_names,
            null_scope,
        });
    }

    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    let (columns, mut rows) = match table.kind() {
        TableKind::VirtualDual => (Vec::new(), vec![Vec::new()]),
        TableKind::Heap | TableKind::MaterializedView => {
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            let rows = with_storage_read(|storage| {
                storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default()
            });
            (columns, rows)
        }
        TableKind::View => {
            let definition = table.view_definition().ok_or_else(|| EngineError {
                message: format!(
                    "view definition for relation \"{}\" is missing",
                    table.qualified_name()
                ),
            })?;
            let result = execute_query_with_outer(definition, params, outer_scope).await?;
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            if result.columns.len() != columns.len() {
                return Err(EngineError {
                    message: format!(
                        "view \"{}\" has invalid column definition",
                        table.qualified_name()
                    ),
                });
            }
            (columns, result.rows)
        }
    };
    if table.kind() != TableKind::VirtualDual {
        require_relation_privilege(&table, TablePrivilege::Select)?;
        let mut visible_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if relation_row_visible_for_command(&table, &row, RlsCommand::Select, params)
                .await?
            {
                visible_rows.push(row);
            }
        }
        rows = visible_rows;
    }

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };

    let mut scoped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
    })
}

fn virtual_relation_rows(
    schema: &str,
    relation: &str,
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    match (schema, relation) {
        ("pg_catalog", "pg_namespace") => {
            let mut entries = with_catalog_read(|catalog| {
                catalog
                    .schemas()
                    .map(|schema| (schema.oid(), schema.name().to_string()))
                    .collect::<Vec<_>>()
            });
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(|(oid, name)| vec![ScalarValue::Int(oid as i64), ScalarValue::Text(name)])
                .collect())
        }
        ("pg_catalog", "pg_class") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        out.push((
                            table.oid(),
                            table.name().to_string(),
                            schema.oid(),
                            pg_relkind_for_table(table.kind()).to_string(),
                        ));
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(oid, relname, relnamespace, relkind)| {
                    vec![
                        ScalarValue::Int(oid as i64),
                        ScalarValue::Text(relname),
                        ScalarValue::Int(relnamespace as i64),
                        ScalarValue::Text(relkind),
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_attribute") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            out.push((
                                table.oid(),
                                column.ordinal(),
                                column.name().to_string(),
                                type_signature_to_oid(column.type_signature()),
                                !column.nullable(),
                            ));
                        }
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(attrelid, attnum, attname, atttypid, attnotnull)| {
                    vec![
                        ScalarValue::Int(attrelid as i64),
                        ScalarValue::Text(attname),
                        ScalarValue::Int(atttypid as i64),
                        ScalarValue::Int(attnum as i64 + 1),
                        ScalarValue::Bool(attnotnull),
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_type") => {
            let mut entries = vec![
                (16u32, "bool".to_string()),
                (20u32, "int8".to_string()),
                (25u32, "text".to_string()),
                (701u32, "float8".to_string()),
                (1082u32, "date".to_string()),
                (1114u32, "timestamp".to_string()),
            ];
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(|(oid, typname)| {
                    vec![ScalarValue::Int(oid as i64), ScalarValue::Text(typname)]
                })
                .collect())
        }
        ("information_schema", "tables") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        out.push((
                            schema.name().to_string(),
                            table.name().to_string(),
                            information_schema_table_type(table.kind()).to_string(),
                        ));
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(table_schema, table_name, table_type)| {
                    vec![
                        ScalarValue::Text(table_schema),
                        ScalarValue::Text(table_name),
                        ScalarValue::Text(table_type),
                    ]
                })
                .collect())
        }
        ("information_schema", "columns") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            out.push((
                                schema.name().to_string(),
                                table.name().to_string(),
                                column.ordinal(),
                                column.name().to_string(),
                                information_schema_data_type(column.type_signature()).to_string(),
                                if column.nullable() {
                                    "YES".to_string()
                                } else {
                                    "NO".to_string()
                                },
                            ));
                        }
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            Ok(entries
                .into_iter()
                .map(
                    |(table_schema, table_name, ordinal, column_name, data_type, is_nullable)| {
                        vec![
                            ScalarValue::Text(table_schema),
                            ScalarValue::Text(table_name),
                            ScalarValue::Text(column_name),
                            ScalarValue::Int(ordinal as i64 + 1),
                            ScalarValue::Text(data_type),
                            ScalarValue::Text(is_nullable),
                        ]
                    },
                )
                .collect())
        }
        ("information_schema", "schemata") => {
            let schemas = with_catalog_read(|catalog| {
                catalog.schemas().map(|s| s.name().to_string()).collect::<Vec<_>>()
            });
            Ok(schemas.into_iter().map(|name| {
                vec![
                    ScalarValue::Text("postrust".to_string()),
                    ScalarValue::Text(name),
                    ScalarValue::Text("postrust".to_string()),
                ]
            }).collect())
        }
        ("information_schema", "key_column_usage") => {
            // Return empty for now - would need constraint introspection
            Ok(Vec::new())
        }
        ("information_schema", "table_constraints") => {
            // Return empty for now
            Ok(Vec::new())
        }
        ("pg_catalog", "pg_database") => {
            Ok(vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("postrust".to_string()),
                ScalarValue::Int(10),
                ScalarValue::Int(6), // UTF8
                ScalarValue::Text("en_US.UTF-8".to_string()),
            ]])
        }
        ("pg_catalog", "pg_roles") => {
            let role = security::current_role();
            Ok(vec![vec![
                ScalarValue::Int(10),
                ScalarValue::Text(role),
                ScalarValue::Bool(true),
                ScalarValue::Bool(true),
            ]])
        }
        ("pg_catalog", "pg_settings") => {
            let guc = global_guc().read().expect("guc lock");
            Ok(guc.iter().map(|(name, value)| {
                vec![
                    ScalarValue::Text(name.clone()),
                    ScalarValue::Text(value.clone()),
                    ScalarValue::Text("Ungrouped".to_string()),
                    ScalarValue::Text(String::new()),
                ]
            }).collect())
        }
        ("pg_catalog", "pg_tables") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        if matches!(table.kind(), TableKind::Heap | TableKind::VirtualDual) {
                            rows.push(vec![
                                ScalarValue::Text(schema.name().to_string()),
                                ScalarValue::Text(table.name().to_string()),
                                ScalarValue::Text("postrust".to_string()),
                            ]);
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_views") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        if matches!(table.kind(), TableKind::View | TableKind::MaterializedView) {
                            rows.push(vec![
                                ScalarValue::Text(schema.name().to_string()),
                                ScalarValue::Text(table.name().to_string()),
                                ScalarValue::Text("postrust".to_string()),
                            ]);
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_indexes") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for index in table.indexes() {
                            rows.push(vec![
                                ScalarValue::Text(schema.name().to_string()),
                                ScalarValue::Text(table.name().to_string()),
                                ScalarValue::Text(index.name.as_str().to_string()),
                            ]);
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_proc") => {
            // Return user-defined functions
            Ok(with_ext_read(|ext| {
                ext.user_functions.iter().enumerate().map(|(i, f)| {
                    vec![
                        ScalarValue::Int(90000 + i as i64),
                        ScalarValue::Text(f.name.last().cloned().unwrap_or_default()),
                        ScalarValue::Int(0), // pronamespace placeholder
                    ]
                }).collect()
            }))
        }
        ("pg_catalog", "pg_constraint") => {
            // Return empty for now
            Ok(Vec::new())
        }
        ("pg_catalog", "pg_extension") => {
            Ok(with_ext_read(|ext| {
                ext.extensions.iter().map(|e| {
                    vec![
                        ScalarValue::Text(e.name.clone()),
                        ScalarValue::Text(e.version.clone()),
                        ScalarValue::Text(e.description.clone()),
                    ]
                }).collect()
            }))
        }
        ("ws", "connections") => {
            if !is_ws_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"ws\" is not loaded".to_string(),
                });
            }
            Ok(with_ext_read(|ext| {
                let mut conns: Vec<_> = ext.ws_connections.values().collect();
                conns.sort_by_key(|c| c.id);
                conns.iter().map(|c| {
                    vec![
                        ScalarValue::Int(c.id),
                        ScalarValue::Text(c.url.clone()),
                        ScalarValue::Text(c.state.clone()),
                        ScalarValue::Text(c.opened_at.clone()),
                        ScalarValue::Int(c.messages_in),
                        ScalarValue::Int(c.messages_out),
                    ]
                }).collect()
            }))
        }
        _ => Err(EngineError {
            message: format!("relation \"{}.{}\" does not exist", schema, relation),
        }),
    }
}

fn pg_relkind_for_table(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "r",
        TableKind::View => "v",
        TableKind::MaterializedView => "m",
    }
}

fn information_schema_table_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "BASE TABLE",
        TableKind::View => "VIEW",
        TableKind::MaterializedView => "MATERIALIZED VIEW",
    }
}

fn information_schema_data_type(signature: TypeSignature) -> &'static str {
    match signature {
        TypeSignature::Bool => "boolean",
        TypeSignature::Int8 => "bigint",
        TypeSignature::Float8 => "double precision",
        TypeSignature::Text => "text",
        TypeSignature::Date => "date",
        TypeSignature::Timestamp => "timestamp without time zone",
    }
}

async fn evaluate_join(
    join_type: JoinType,
    condition: Option<&JoinCondition>,
    natural: bool,
    left: &TableEval,
    right: &TableEval,
    params: &[Option<String>],
) -> Result<TableEval, EngineError> {
    let using_columns = if natural {
        left.columns
            .iter()
            .filter(|c| right.columns.iter().any(|r| r.eq_ignore_ascii_case(c)))
            .cloned()
            .collect::<Vec<_>>()
    } else if let Some(JoinCondition::Using(cols)) = condition {
        cols.clone()
    } else {
        Vec::new()
    };
    let using_set: HashSet<String> = using_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();

    let mut output_rows = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];

    for left_row in &left.rows {
        let mut left_matched = false;
        for (right_idx, right_row) in right.rows.iter().enumerate() {
            let matches = match join_type {
                JoinType::Cross => true,
                _ => {
                    join_condition_matches(condition, &using_columns, left_row, right_row, params)
                        .await?
                }
            };

            if matches {
                left_matched = true;
                right_matched[right_idx] = true;
                output_rows.push(combine_scopes(left_row, right_row, &using_set));
            }
        }

        if !left_matched && matches!(join_type, JoinType::Left | JoinType::Full) {
            output_rows.push(combine_scopes(left_row, &right.null_scope, &using_set));
        }
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (right_idx, right_row) in right.rows.iter().enumerate() {
            if !right_matched[right_idx] {
                output_rows.push(combine_scopes(&left.null_scope, right_row, &using_set));
            }
        }
    }

    let mut output_columns = left.columns.clone();
    for col in &right.columns {
        if using_set.contains(&col.to_ascii_lowercase()) {
            continue;
        }
        output_columns.push(col.clone());
    }

    let null_scope = combine_scopes(&left.null_scope, &right.null_scope, &using_set);
    Ok(TableEval {
        rows: output_rows,
        columns: output_columns,
        null_scope,
    })
}

async fn project_select_row(
    targets: &[crate::parser::ast::SelectItem],
    scope: &EvalScope,
    params: &[Option<String>],
    wildcard_columns: Option<&[ExpandedFromColumn]>,
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut row = Vec::new();
    for target in targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                row.push(scope.lookup_identifier(&col.lookup_parts)?);
            }
            continue;
        }
        row.push(eval_expr(&target.expr, scope, params).await?);
    }
    Ok(row)
}

async fn project_select_row_with_window(
    targets: &[crate::parser::ast::SelectItem],
    scope: &EvalScope,
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
    wildcard_columns: Option<&[ExpandedFromColumn]>,
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut row = Vec::new();
    for target in targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                row.push(scope.lookup_identifier(&col.lookup_parts)?);
            }
            continue;
        }
        row.push(
            eval_expr_with_window(&target.expr, scope, row_idx, all_rows, params).await?,
        );
    }
    Ok(row)
}

fn contains_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            if over.is_none()
                && let Some(fn_name) = name.last()
                && is_aggregate_function(fn_name)
            {
                return true;
            }
            args.iter().any(contains_aggregate_expr)
                || order_by
                    .iter()
                    .any(|entry| contains_aggregate_expr(&entry.expr))
                || within_group
                    .iter()
                    .any(|entry| contains_aggregate_expr(&entry.expr))
                || filter
                    .as_ref()
                    .is_some_and(|entry| contains_aggregate_expr(entry))
        }
        Expr::Cast { expr, .. } => contains_aggregate_expr(expr),
        Expr::Unary { expr, .. } => contains_aggregate_expr(expr),
        Expr::Binary { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::AnyAll { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::InList { expr, list, .. } => {
            contains_aggregate_expr(expr) || list.iter().any(contains_aggregate_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_aggregate_expr(expr)
                || contains_aggregate_expr(low)
                || contains_aggregate_expr(high)
        }
        Expr::Like { expr, pattern, .. } => {
            contains_aggregate_expr(expr) || contains_aggregate_expr(pattern)
        }
        Expr::IsNull { expr, .. } => contains_aggregate_expr(expr),
        Expr::IsDistinctFrom { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            contains_aggregate_expr(operand)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    contains_aggregate_expr(when_expr) || contains_aggregate_expr(then_expr)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate_expr(expr))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                contains_aggregate_expr(when_expr) || contains_aggregate_expr(then_expr)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| contains_aggregate_expr(expr))
        }
        Expr::ArrayConstructor(items) => items.iter().any(contains_aggregate_expr),
        Expr::ArraySubquery(_) => false,
        Expr::Exists(_) | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => false,
        _ => false,
    }
}

fn contains_window_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            over.is_some()
                || args.iter().any(contains_window_expr)
                || order_by.iter().any(|entry| contains_window_expr(&entry.expr))
                || within_group
                    .iter()
                    .any(|entry| contains_window_expr(&entry.expr))
                || filter.as_ref().is_some_and(|entry| contains_window_expr(entry))
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(contains_window_expr)
                        || window.order_by.iter().any(|entry| contains_window_expr(&entry.expr))
                        || window.frame.as_ref().is_some_and(|frame| {
                            contains_window_bound_expr(&frame.start)
                                || contains_window_bound_expr(&frame.end)
                        })
                })
        }
        Expr::Cast { expr, .. } => contains_window_expr(expr),
        Expr::Unary { expr, .. } => contains_window_expr(expr),
        Expr::Binary { left, right, .. } => contains_window_expr(left) || contains_window_expr(right),
        Expr::AnyAll { left, right, .. } => {
            contains_window_expr(left) || contains_window_expr(right)
        }
        Expr::InList { expr, list, .. } => {
            contains_window_expr(expr) || list.iter().any(contains_window_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_window_expr(expr) || contains_window_expr(low) || contains_window_expr(high),
        Expr::Like { expr, pattern, .. } => contains_window_expr(expr) || contains_window_expr(pattern),
        Expr::IsNull { expr, .. } => contains_window_expr(expr),
        Expr::IsDistinctFrom { left, right, .. } => {
            contains_window_expr(left) || contains_window_expr(right)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            contains_window_expr(operand)
                || when_then
                    .iter()
                    .any(|(when_expr, then_expr)| {
                        contains_window_expr(when_expr) || contains_window_expr(then_expr)
                    })
                || else_expr.as_ref().is_some_and(|expr| contains_window_expr(expr))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then
                .iter()
                .any(|(when_expr, then_expr)| {
                    contains_window_expr(when_expr) || contains_window_expr(then_expr)
                })
                || else_expr.as_ref().is_some_and(|expr| contains_window_expr(expr))
        }
        Expr::ArrayConstructor(items) => items.iter().any(contains_window_expr),
        Expr::ArraySubquery(_) => false,
        Expr::Exists(_) | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => false,
        _ => false,
    }
}

fn contains_window_bound_expr(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::OffsetPreceding(expr) | WindowFrameBound::OffsetFollowing(expr) => {
            contains_window_expr(expr)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => false,
    }
}

fn group_by_contains_window_expr(group_by: &[GroupByExpr]) -> bool {
    group_by.iter().any(|expr| match expr {
        GroupByExpr::Expr(expr) => contains_window_expr(expr),
        GroupByExpr::GroupingSets(sets) => sets
            .iter()
            .any(|set| set.iter().any(contains_window_expr)),
        GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
            exprs.iter().any(contains_window_expr)
        }
    })
}

fn group_by_exprs(group_by: &[GroupByExpr]) -> Vec<&Expr> {
    let mut out = Vec::new();
    for entry in group_by {
        match entry {
            GroupByExpr::Expr(expr) => out.push(expr),
            GroupByExpr::GroupingSets(sets) => {
                for set in sets {
                    for expr in set {
                        out.push(expr);
                    }
                }
            }
            GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
                for expr in exprs {
                    out.push(expr);
                }
            }
        }
    }
    out
}

fn identifier_key(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(parts) => Some(parts.join(".").to_ascii_lowercase()),
        _ => None,
    }
}

fn collect_grouping_identifiers(group_by: &[GroupByExpr]) -> HashSet<String> {
    let mut out = HashSet::new();
    for expr in group_by_exprs(group_by) {
        if let Some(key) = identifier_key(expr) {
            out.insert(key);
        }
    }
    out
}

fn expand_grouping_sets<'a>(group_by: &'a [GroupByExpr]) -> Vec<Vec<&'a Expr>> {
    if group_by.is_empty() {
        return vec![Vec::new()];
    }
    let mut sets: Vec<Vec<&'a Expr>> = vec![Vec::new()];
    for entry in group_by {
        let element_sets: Vec<Vec<&'a Expr>> = match entry {
            GroupByExpr::Expr(expr) => vec![vec![expr]],
            GroupByExpr::GroupingSets(sets) => {
                sets.iter().map(|set| set.iter().collect()).collect()
            }
            GroupByExpr::Rollup(exprs) => {
                let mut rollup_sets = Vec::new();
                for len in (0..=exprs.len()).rev() {
                    rollup_sets.push(exprs[..len].iter().collect());
                }
                rollup_sets
            }
            GroupByExpr::Cube(exprs) => {
                let mut cube_sets = Vec::new();
                let count = exprs.len();
                let max_mask = 1usize << count;
                for mask in (0..max_mask).rev() {
                    let mut set = Vec::new();
                    for (idx, expr) in exprs.iter().enumerate() {
                        if (mask & (1 << idx)) != 0 {
                            set.push(expr);
                        }
                    }
                    cube_sets.push(set);
                }
                cube_sets
            }
        };

        let mut combined_sets = Vec::new();
        for existing in &sets {
            for element in &element_sets {
                let mut merged = Vec::with_capacity(existing.len() + element.len());
                merged.extend(existing.iter().copied());
                merged.extend(element.iter().copied());
                combined_sets.push(merged);
            }
        }
        sets = combined_sets;
    }
    sets
}

struct GroupingContext {
    current_grouping: HashSet<String>,
    all_grouping: HashSet<String>,
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "string_agg"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "json_object_agg"
            | "jsonb_object_agg"
            | "bool_and"
            | "bool_or"
            | "every"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "variance"
            | "var_samp"
            | "var_pop"
            | "corr"
            | "covar_pop"
            | "covar_samp"
            | "regr_slope"
            | "regr_intercept"
            | "regr_count"
            | "regr_r2"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_sxy"
            | "regr_syy"
            | "percentile_cont"
            | "percentile_disc"
            | "mode"
    )
}

fn eval_group_expr<'a>(
    expr: &'a Expr,
    group_rows: &'a [EvalScope],
    representative: &'a EvalScope,
    params: &'a [Option<String>],
    grouping: &'a GroupingContext,
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => {
            if over.is_some() {
                return Err(EngineError {
                    message: "window functions are not allowed in grouped aggregate expressions"
                        .to_string(),
                });
            }
            let fn_name = name
                .last()
                .map(|n| n.to_ascii_lowercase())
                .unwrap_or_default();
            if fn_name == "grouping" {
                if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
                    return Err(EngineError {
                        message: "grouping() does not accept aggregate modifiers".to_string(),
                    });
                }
                if args.len() != 1 {
                    return Err(EngineError {
                        message: "grouping() expects exactly one argument".to_string(),
                    });
                }
                let Some(key) = identifier_key(&args[0]) else {
                    return Err(EngineError {
                        message: "grouping() expects a column reference".to_string(),
                    });
                };
                let value = if grouping.current_grouping.contains(&key) {
                    0
                } else {
                    1
                };
                return Ok(ScalarValue::Int(value));
            }
            if is_aggregate_function(&fn_name) {
                return eval_aggregate_function(
                    &fn_name,
                    args,
                    *distinct,
                    order_by,
                    within_group,
                    filter.as_deref(),
                    group_rows,
                    params,
                )
                .await;
            }
            if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
                return Err(EngineError {
                    message: format!(
                        "{}() aggregate modifiers require grouped aggregate evaluation",
                        fn_name
                    ),
                });
            }

            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(
                    eval_group_expr(arg, group_rows, representative, params, grouping).await?,
                );
            }
            eval_scalar_function(&fn_name, &values).await
        }
        Expr::Unary { op, expr } => {
            let value = eval_group_expr(expr, group_rows, representative, params, grouping).await?;
            eval_unary(op.clone(), value)
        }
        Expr::Binary { left, op, right } => {
            let lhs = eval_group_expr(left, group_rows, representative, params, grouping).await?;
            let rhs = eval_group_expr(right, group_rows, representative, params, grouping).await?;
            eval_binary(op.clone(), lhs, rhs)
        }
        Expr::AnyAll {
            left,
            op,
            right,
            quantifier,
        } => {
            let lhs = eval_group_expr(left, group_rows, representative, params, grouping).await?;
            let rhs = eval_group_expr(right, group_rows, representative, params, grouping).await?;
            eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
        }
        Expr::Cast { expr, type_name } => {
            let value = eval_group_expr(expr, group_rows, representative, params, grouping).await?;
            eval_cast_scalar(value, type_name)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_group_expr(expr, group_rows, representative, params, grouping).await?;
            let low_value = eval_group_expr(low, group_rows, representative, params, grouping).await?;
            let high_value = eval_group_expr(high, group_rows, representative, params, grouping).await?;
            eval_between_predicate(value, low_value, high_value, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let value = eval_group_expr(expr, group_rows, representative, params, grouping).await?;
            let pattern_value = eval_group_expr(pattern, group_rows, representative, params, grouping).await?;
            eval_like_predicate(value, pattern_value, *case_insensitive, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_group_expr(expr, group_rows, representative, params, grouping).await?;
            let is_null = matches!(value, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_value = eval_group_expr(left, group_rows, representative, params, grouping).await?;
            let right_value = eval_group_expr(right, group_rows, representative, params, grouping).await?;
            eval_is_distinct_from(left_value, right_value, *negated)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let operand_value =
                eval_group_expr(operand, group_rows, representative, params, grouping).await?;
            for (when_expr, then_expr) in when_then {
                let when_value =
                    eval_group_expr(when_expr, group_rows, representative, params, grouping).await?;
                if matches!(operand_value, ScalarValue::Null)
                    || matches!(when_value, ScalarValue::Null)
                {
                    continue;
                }
                if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal {
                    return eval_group_expr(then_expr, group_rows, representative, params, grouping)
                        .await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_group_expr(else_expr, group_rows, representative, params, grouping).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when_expr, then_expr) in when_then {
                let condition =
                    eval_group_expr(when_expr, group_rows, representative, params, grouping).await?;
                if truthy(&condition) {
                    return eval_group_expr(then_expr, group_rows, representative, params, grouping)
                        .await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_group_expr(else_expr, group_rows, representative, params, grouping).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::ArrayConstructor(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_group_expr(item, group_rows, representative, params, grouping).await?);
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::ArraySubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(representative)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let mut values = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                values.push(row.first().cloned().unwrap_or(ScalarValue::Null));
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::Identifier(_) => {
            if let Some(key) = identifier_key(expr)
                && grouping.all_grouping.contains(&key)
                && !grouping.current_grouping.contains(&key)
            {
                return Ok(ScalarValue::Null);
            }
            if group_rows.is_empty() {
                eval_expr(expr, &EvalScope::default(), params).await
            } else {
                eval_expr(expr, representative, params).await
            }
        }
        _ => {
            if group_rows.is_empty() {
                eval_expr(expr, &EvalScope::default(), params).await
            } else {
                eval_expr(expr, representative, params).await
            }
        }
    }
    })
}

#[derive(Debug, Clone)]
struct AggregateInputRow {
    args: Vec<ScalarValue>,
    order_keys: Vec<ScalarValue>,
}

#[derive(Debug, Clone, Copy)]
struct RegrStats {
    count: i64,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

fn compute_regr_stats(rows: &[AggregateInputRow], message: &str) -> Result<RegrStats, EngineError> {
    let mut stats = RegrStats {
        count: 0,
        sum_x: 0.0,
        sum_y: 0.0,
        sum_xx: 0.0,
        sum_yy: 0.0,
        sum_xy: 0.0,
    };
    for row in rows {
        let Some(y_value) = row.args.first() else {
            continue;
        };
        let Some(x_value) = row.args.get(1) else {
            continue;
        };
        if matches!(y_value, ScalarValue::Null) || matches!(x_value, ScalarValue::Null) {
            continue;
        }
        let y = parse_f64_numeric_scalar(y_value, message)?;
        let x = parse_f64_numeric_scalar(x_value, message)?;
        stats.count += 1;
        stats.sum_x += x;
        stats.sum_y += y;
        stats.sum_xx += x * x;
        stats.sum_yy += y * y;
        stats.sum_xy += x * y;
    }
    Ok(stats)
}

async fn build_aggregate_input_rows(
    args: &[Expr],
    order_by: &[OrderByExpr],
    filter: Option<&Expr>,
    group_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<AggregateInputRow>, EngineError> {
    let mut out = Vec::with_capacity(group_rows.len());
    for scope in group_rows {
        if let Some(predicate) = filter
            && !truthy(&eval_expr(predicate, scope, params).await?)
        {
            continue;
        }

        let mut arg_values = Vec::with_capacity(args.len());
        for arg in args {
            arg_values.push(eval_expr(arg, scope, params).await?);
        }
        let mut order_values = Vec::with_capacity(order_by.len());
        for order_expr in order_by {
            order_values.push(eval_expr(&order_expr.expr, scope, params).await?);
        }
        out.push(AggregateInputRow {
            args: arg_values,
            order_keys: order_values,
        });
    }
    Ok(out)
}

fn apply_aggregate_distinct(rows: &mut Vec<AggregateInputRow>) {
    let mut seen = HashSet::new();
    rows.retain(|row| seen.insert(row_key(&row.args)));
}

fn sort_aggregate_rows(rows: &mut [AggregateInputRow], order_by: &[OrderByExpr]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|left, right| compare_order_keys(&left.order_keys, &right.order_keys, order_by));
}

#[allow(clippy::too_many_arguments)]
async fn eval_aggregate_function(
    fn_name: &str,
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    within_group: &[OrderByExpr],
    filter: Option<&Expr>,
    group_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let is_ordered_set = matches!(fn_name, "percentile_cont" | "percentile_disc" | "mode");
    if !within_group.is_empty() && !is_ordered_set {
        return Err(EngineError {
            message: format!("{fn_name}() does not support WITHIN GROUP"),
        });
    }
    if is_ordered_set && within_group.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires WITHIN GROUP (ORDER BY ...)"),
        });
    }

    match fn_name {
        "count" => {
            if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
                if distinct {
                    return Err(EngineError {
                        message: "count(DISTINCT *) is not supported".to_string(),
                    });
                }
                if !order_by.is_empty() {
                    return Err(EngineError {
                        message: "count(*) does not accept aggregate ORDER BY".to_string(),
                    });
                }
                let mut count = 0i64;
                for scope in group_rows {
                    if let Some(predicate) = filter
                        && !truthy(&eval_expr(predicate, scope, params).await?)
                    {
                        continue;
                    }
                    count += 1;
                }
                return Ok(ScalarValue::Int(count));
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "count() expects exactly one argument".to_string(),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let count = rows
                .iter()
                .filter(|row| !matches!(row.args[0], ScalarValue::Null))
                .count() as i64;
            Ok(ScalarValue::Int(count))
        }
        "sum" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "sum() expects exactly one argument".to_string(),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            let mut int_sum: i64 = 0;
            let mut float_sum: f64 = 0.0;
            let mut saw_float = false;
            let mut saw_any = false;
            for row in rows {
                match row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Int(v) => {
                        int_sum += v;
                        float_sum += v as f64;
                        saw_any = true;
                    }
                    ScalarValue::Float(v) => {
                        float_sum += v;
                        saw_float = true;
                        saw_any = true;
                    }
                    _ => {
                        return Err(EngineError {
                            message: "sum() expects numeric values".to_string(),
                        });
                    }
                }
            }
            if !saw_any {
                return Ok(ScalarValue::Null);
            }
            if saw_float {
                Ok(ScalarValue::Float(float_sum))
            } else {
                Ok(ScalarValue::Int(int_sum))
            }
        }
        "avg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "avg() expects exactly one argument".to_string(),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            let mut total = 0.0f64;
            let mut count = 0u64;
            for row in rows {
                match row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Int(v) => {
                        total += v as f64;
                        count += 1;
                    }
                    ScalarValue::Float(v) => {
                        total += v;
                        count += 1;
                    }
                    _ => {
                        return Err(EngineError {
                            message: "avg() expects numeric values".to_string(),
                        });
                    }
                }
            }
            if count == 0 {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Float(total / count as f64))
            }
        }
        "min" | "max" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly one argument"),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            let mut current: Option<ScalarValue> = None;
            for row in rows {
                let value = row.args[0].clone();
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                match &current {
                    None => current = Some(value),
                    Some(existing) => {
                        let cmp = scalar_cmp(&value, existing);
                        let take = if fn_name == "min" {
                            cmp == Ordering::Less
                        } else {
                            cmp == Ordering::Greater
                        };
                        if take {
                            current = Some(value);
                        }
                    }
                }
            }
            Ok(current.unwrap_or(ScalarValue::Null))
        }
        "json_agg" | "jsonb_agg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly one argument"),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                out.push(scalar_to_json_value(&row.args[0])?);
            }
            Ok(ScalarValue::Text(JsonValue::Array(out).to_string()))
        }
        "string_agg" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: "string_agg() expects exactly two arguments".to_string(),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let delimiter = match &rows.first().map(|r| &r.args[1]) {
                Some(ScalarValue::Text(s)) => s.clone(),
                Some(ScalarValue::Null) => return Ok(ScalarValue::Null),
                Some(other) => other.render(),
                None => return Ok(ScalarValue::Null),
            };
            let parts: Vec<String> = rows.iter()
                .filter(|r| !matches!(r.args[0], ScalarValue::Null))
                .map(|r| r.args[0].render())
                .collect();
            if parts.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Text(parts.join(&delimiter)))
            }
        }
        "array_agg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "array_agg() expects exactly one argument".to_string(),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let parts: Vec<String> = rows.iter().map(|r| {
                if matches!(r.args[0], ScalarValue::Null) { "NULL".to_string() } else { r.args[0].render() }
            }).collect();
            Ok(ScalarValue::Text(format!("{{{}}}", parts.join(","))))
        }
        "bool_and" | "every" => {
            if args.len() != 1 {
                return Err(EngineError { message: format!("{fn_name}() expects one argument") });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let mut result = true;
            let mut saw_any = false;
            for row in &rows {
                match &row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Bool(b) => { saw_any = true; if !b { result = false; } }
                    _ => return Err(EngineError { message: format!("{fn_name}() expects boolean") }),
                }
            }
            if !saw_any { Ok(ScalarValue::Null) } else { Ok(ScalarValue::Bool(result)) }
        }
        "bool_or" => {
            if args.len() != 1 {
                return Err(EngineError { message: "bool_or() expects one argument".to_string() });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let mut result = false;
            let mut saw_any = false;
            for row in &rows {
                match &row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Bool(b) => { saw_any = true; if *b { result = true; } }
                    _ => return Err(EngineError { message: "bool_or() expects boolean".to_string() }),
                }
            }
            if !saw_any { Ok(ScalarValue::Null) } else { Ok(ScalarValue::Bool(result)) }
        }
        "stddev" | "stddev_samp" => {
            if args.len() != 1 {
                return Err(EngineError { message: format!("{fn_name}() expects one argument") });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows.iter().filter_map(|r| match &r.args[0] {
                ScalarValue::Int(i) => Some(*i as f64),
                ScalarValue::Float(f) => Some(*f),
                _ => None,
            }).collect();
            if values.len() < 2 { return Ok(ScalarValue::Null); }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(ScalarValue::Float(variance.sqrt()))
        }
        "stddev_pop" => {
            if args.len() != 1 {
                return Err(EngineError { message: "stddev_pop() expects one argument".to_string() });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows.iter().filter_map(|r| match &r.args[0] {
                ScalarValue::Int(i) => Some(*i as f64),
                ScalarValue::Float(f) => Some(*f),
                _ => None,
            }).collect();
            if values.is_empty() { return Ok(ScalarValue::Null); }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(variance.sqrt()))
        }
        "variance" | "var_samp" => {
            if args.len() != 1 {
                return Err(EngineError { message: format!("{fn_name}() expects one argument") });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows.iter().filter_map(|r| match &r.args[0] {
                ScalarValue::Int(i) => Some(*i as f64),
                ScalarValue::Float(f) => Some(*f),
                _ => None,
            }).collect();
            if values.len() < 2 { return Ok(ScalarValue::Null); }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64))
        }
        "var_pop" => {
            if args.len() != 1 {
                return Err(EngineError { message: "var_pop() expects one argument".to_string() });
            }
            let rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows.iter().filter_map(|r| match &r.args[0] {
                ScalarValue::Int(i) => Some(*i as f64),
                ScalarValue::Float(f) => Some(*f),
                _ => None,
            }).collect();
            if values.is_empty() { return Ok(ScalarValue::Null); }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64))
        }
        "corr"
        | "covar_pop"
        | "covar_samp"
        | "regr_slope"
        | "regr_intercept"
        | "regr_count"
        | "regr_r2"
        | "regr_avgx"
        | "regr_avgy"
        | "regr_sxx"
        | "regr_sxy"
        | "regr_syy" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly two arguments"),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let stats = compute_regr_stats(&rows, &format!("{fn_name}() expects numeric values"))?;
            let count = stats.count;
            if fn_name == "regr_count" {
                return Ok(ScalarValue::Int(count));
            }
            if count == 0 {
                return Ok(ScalarValue::Null);
            }
            let n = count as f64;
            let avgx = stats.sum_x / n;
            let avgy = stats.sum_y / n;
            let sxx = stats.sum_xx - stats.sum_x * stats.sum_x / n;
            let syy = stats.sum_yy - stats.sum_y * stats.sum_y / n;
            let sxy = stats.sum_xy - stats.sum_x * stats.sum_y / n;
            match fn_name {
                "corr" => {
                    if count < 2 || sxx <= 0.0 || syy <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / (sxx * syy).sqrt()))
                    }
                }
                "covar_pop" => Ok(ScalarValue::Float(sxy / n)),
                "covar_samp" => {
                    if count < 2 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / (n - 1.0)))
                    }
                }
                "regr_slope" => {
                    if count < 2 || sxx <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / sxx))
                    }
                }
                "regr_intercept" => {
                    if count < 2 || sxx <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        let slope = sxy / sxx;
                        Ok(ScalarValue::Float(avgy - slope * avgx))
                    }
                }
                "regr_r2" => {
                    if count < 2 || sxx <= 0.0 || syy <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float((sxy * sxy) / (sxx * syy)))
                    }
                }
                "regr_avgx" => Ok(ScalarValue::Float(avgx)),
                "regr_avgy" => Ok(ScalarValue::Float(avgy)),
                "regr_sxx" => Ok(ScalarValue::Float(sxx)),
                "regr_sxy" => Ok(ScalarValue::Float(sxy)),
                "regr_syy" => Ok(ScalarValue::Float(syy)),
                _ => Err(EngineError {
                    message: format!("unsupported aggregate function {}", fn_name),
                }),
            }
        }
        "percentile_cont" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "percentile_cont() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "percentile_cont() does not accept DISTINCT".to_string(),
                });
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "percentile_cont() expects exactly one argument".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "percentile_cont() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let fraction_value = rows.iter().find_map(|row| match &row.args[0] {
                ScalarValue::Null => None,
                value => Some(value.clone()),
            });
            let Some(fraction_value) = fraction_value else {
                return Ok(ScalarValue::Null);
            };
            let fraction = parse_f64_numeric_scalar(
                &fraction_value,
                "percentile_cont() expects numeric fraction",
            )?;
            if !(0.0..=1.0).contains(&fraction) {
                return Err(EngineError {
                    message: "percentile_cont() fraction must be between 0 and 1".to_string(),
                });
            }
            let mut values = Vec::new();
            for row in &rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                let parsed = parse_f64_numeric_scalar(
                    value,
                    "percentile_cont() expects numeric values",
                )?;
                values.push(parsed);
            }
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if values.len() == 1 {
                return Ok(ScalarValue::Float(values[0]));
            }
            let pos = fraction * (values.len() - 1) as f64;
            let lower_idx = pos.floor() as usize;
            let upper_idx = pos.ceil() as usize;
            let lower = values[lower_idx];
            let upper = values[upper_idx];
            if lower_idx == upper_idx {
                Ok(ScalarValue::Float(lower))
            } else {
                let weight = pos - lower_idx as f64;
                Ok(ScalarValue::Float(lower + (upper - lower) * weight))
            }
        }
        "percentile_disc" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "percentile_disc() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "percentile_disc() does not accept DISTINCT".to_string(),
                });
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "percentile_disc() expects exactly one argument".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "percentile_disc() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let fraction_value = rows.iter().find_map(|row| match &row.args[0] {
                ScalarValue::Null => None,
                value => Some(value.clone()),
            });
            let Some(fraction_value) = fraction_value else {
                return Ok(ScalarValue::Null);
            };
            let fraction = parse_f64_numeric_scalar(
                &fraction_value,
                "percentile_disc() expects numeric fraction",
            )?;
            if !(0.0..=1.0).contains(&fraction) {
                return Err(EngineError {
                    message: "percentile_disc() fraction must be between 0 and 1".to_string(),
                });
            }
            let mut values = Vec::new();
            for row in &rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                values.push(value.clone());
            }
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut pos = (fraction * values.len() as f64).ceil() as usize;
            if pos == 0 {
                pos = 1;
            }
            Ok(values[pos - 1].clone())
        }
        "mode" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "mode() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "mode() does not accept DISTINCT".to_string(),
                });
            }
            if !args.is_empty() {
                return Err(EngineError {
                    message: "mode() expects no arguments".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "mode() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let mut best_value: Option<ScalarValue> = None;
            let mut best_count = 0usize;
            let mut current_value: Option<ScalarValue> = None;
            let mut current_count = 0usize;
            for row in rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                match &current_value {
                    Some(existing) if scalar_cmp(existing, value) == Ordering::Equal => {
                        current_count += 1;
                    }
                    _ => {
                        if current_count > best_count {
                            best_count = current_count;
                            best_value = current_value.clone();
                        }
                        current_value = Some(value.clone());
                        current_count = 1;
                    }
                }
            }
            if current_count > best_count {
                best_value = current_value;
            }
            Ok(best_value.unwrap_or(ScalarValue::Null))
        }
        "json_object_agg" | "jsonb_object_agg" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly two arguments"),
                });
            }
            let mut rows = build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut out = JsonMap::new();
            for row in rows {
                if matches!(row.args[0], ScalarValue::Null) {
                    return Err(EngineError {
                        message: format!("{fn_name}() key cannot be null"),
                    });
                }
                out.insert(row.args[0].render(), scalar_to_json_value(&row.args[1])?);
            }
            Ok(ScalarValue::Text(JsonValue::Object(out).to_string()))
        }
        _ => Err(EngineError {
            message: format!("unsupported aggregate function {}", fn_name),
        }),
    }
}

async fn join_condition_matches(
    condition: Option<&JoinCondition>,
    using_columns: &[String],
    left_row: &EvalScope,
    right_row: &EvalScope,
    params: &[Option<String>],
) -> Result<bool, EngineError> {
    if let Some(JoinCondition::On(expr)) = condition {
        let scope = combine_scopes(left_row, right_row, &HashSet::new());
        return Ok(truthy(&eval_expr(expr, &scope, params).await?));
    }

    if !using_columns.is_empty() {
        for col in using_columns {
            let left_value = left_row
                .lookup_join_column(col)
                .ok_or_else(|| EngineError {
                    message: format!("column \"{}\" does not exist in left side of JOIN", col),
                })?;
            let right_value = right_row
                .lookup_join_column(col)
                .ok_or_else(|| EngineError {
                    message: format!("column \"{}\" does not exist in right side of JOIN", col),
                })?;

            if matches!(left_value, ScalarValue::Null) || matches!(right_value, ScalarValue::Null) {
                return Ok(false);
            }
            if scalar_cmp(&left_value, &right_value) != Ordering::Equal {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

fn scope_from_row(
    columns: &[String],
    row: &[ScalarValue],
    qualifiers: &[String],
    visible_columns: &[String],
) -> EvalScope {
    let mut scope = EvalScope::default();
    for (col, value) in columns.iter().zip(row.iter()) {
        scope.insert_unqualified(col, value.clone());
        for qualifier in qualifiers {
            scope.insert_qualified(&format!("{}.{}", qualifier, col), value.clone());
        }
    }

    // Ensure all visible columns exist even if row data is empty (e.g. relation with no rows).
    for col in visible_columns {
        if scope.lookup_join_column(col).is_none() {
            scope.insert_unqualified(col, ScalarValue::Null);
            for qualifier in qualifiers {
                scope.insert_qualified(&format!("{}.{}", qualifier, col), ScalarValue::Null);
            }
        }
    }
    scope
}

fn scope_for_table_row(table: &crate::catalog::Table, row: &[ScalarValue]) -> EvalScope {
    let qualifiers = vec![table.name().to_string(), table.qualified_name()];
    scope_for_table_row_with_qualifiers(table, row, &qualifiers)
}

fn scope_for_table_row_with_qualifiers(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    qualifiers: &[String],
) -> EvalScope {
    let columns = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    scope_from_row(&columns, row, qualifiers, &columns)
}

fn combine_scopes(
    left: &EvalScope,
    right: &EvalScope,
    using_columns: &HashSet<String>,
) -> EvalScope {
    let mut out = left.clone();
    out.merge(right);

    for col in using_columns {
        if let Some(value) = left
            .lookup_join_column(col)
            .or_else(|| right.lookup_join_column(col))
        {
            out.force_unqualified(col, value);
        }
    }

    out
}

async fn execute_set_operation(
    left: &QueryExpr,
    op: SetOperator,
    quantifier: SetQuantifier,
    right: &QueryExpr,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let left_res = execute_query_expr_with_outer(left, params, outer_scope).await?;
    let right_res = execute_query_expr_with_outer(right, params, outer_scope).await?;
    if left_res.columns.len() != right_res.columns.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }

    let rows = match (op, quantifier) {
        (SetOperator::Union, SetQuantifier::All) => {
            let mut out = left_res.rows.clone();
            out.extend(right_res.rows.iter().cloned());
            out
        }
        (SetOperator::Union, SetQuantifier::Distinct) => dedupe_rows(
            left_res
                .rows
                .iter()
                .cloned()
                .chain(right_res.rows.iter().cloned())
                .collect(),
        ),
        (SetOperator::Intersect, SetQuantifier::Distinct) => {
            intersect_rows(&left_res.rows, &right_res.rows, false)
        }
        (SetOperator::Intersect, SetQuantifier::All) => {
            intersect_rows(&left_res.rows, &right_res.rows, true)
        }
        (SetOperator::Except, SetQuantifier::Distinct) => {
            except_rows(&left_res.rows, &right_res.rows, false)
        }
        (SetOperator::Except, SetQuantifier::All) => {
            except_rows(&left_res.rows, &right_res.rows, true)
        }
    };

    Ok(QueryResult {
        columns: left_res.columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}

fn dedupe_rows(rows: Vec<Vec<ScalarValue>>) -> Vec<Vec<ScalarValue>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        let key = row_key(&row);
        if seen.insert(key) {
            out.push(row);
        }
    }
    out
}

fn intersect_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                out.push(row.clone());
            }
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

fn except_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                continue;
            }
            out.push(row.clone());
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if !right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

fn count_rows(rows: &[Vec<ScalarValue>]) -> std::collections::HashMap<String, usize> {
    let mut counts = std::collections::HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)).or_insert(0) += 1;
    }
    counts
}

fn row_key(row: &[ScalarValue]) -> String {
    row.iter()
        .map(|v| match v {
            ScalarValue::Null => "N".to_string(),
            ScalarValue::Bool(b) => format!("B:{b}"),
            ScalarValue::Int(i) => format!("I:{i}"),
            ScalarValue::Float(f) => format!("F:{f}"),
            ScalarValue::Text(t) => format!("T:{t}"),
            ScalarValue::Array(_) => format!("A:{}", v.render()),
        })
        .collect::<Vec<_>>()
        .join("|")
}

async fn apply_order_by(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    if query.order_by.is_empty() || result.rows.is_empty() {
        return Ok(());
    }

    let columns = result.columns.clone();
    let mut decorated = Vec::with_capacity(result.rows.len());
    for row in result.rows.drain(..) {
        let scope = EvalScope::from_output_row(&columns, &row);
        let mut keys = Vec::with_capacity(query.order_by.len());
        for spec in &query.order_by {
            keys.push(resolve_order_key(
                &spec.expr, &scope, &columns, &row, params,
            )
            .await?);
        }
        decorated.push((keys, row));
    }

    decorated.sort_by(|(ka, _), (kb, _)| compare_order_keys(ka, kb, &query.order_by));
    result.rows = decorated.into_iter().map(|(_, row)| row).collect();
    Ok(())
}

fn compare_order_keys(
    left: &[ScalarValue],
    right: &[ScalarValue],
    specs: &[crate::parser::ast::OrderByExpr],
) -> Ordering {
    for (idx, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        let ord = scalar_cmp(l, r);
        if ord != Ordering::Equal {
            if specs[idx].ascending == Some(false) {
                return ord.reverse();
            }
            return ord;
        }
    }
    Ordering::Equal
}

fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use ScalarValue::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Text(x), Text(y)) => x.cmp(y),
        (Int(x), Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Float(x), Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        _ => a.render().cmp(&b.render()),
    }
}

async fn resolve_order_key(
    expr: &Expr,
    scope: &EvalScope,
    columns: &[String],
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    if let Expr::Integer(pos) = expr
        && *pos > 0
    {
        let idx = (*pos as usize).saturating_sub(1);
        if idx < row.len() {
            return Ok(row[idx].clone());
        }
    }

    if let Expr::Identifier(parts) = expr
        && parts.len() == 1
    {
        let want = parts[0].to_ascii_lowercase();
        if let Some((idx, _)) = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.to_ascii_lowercase() == want)
        {
            return Ok(row[idx].clone());
        }
    }

    eval_expr(expr, scope, params).await
}

async fn apply_offset_limit(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    let offset = if let Some(expr) = &query.offset {
        parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "OFFSET",
        )?
    } else {
        0usize
    };

    let limit = if let Some(expr) = &query.limit {
        Some(parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "LIMIT",
        )?)
    } else {
        None
    };

    if offset > 0 {
        if offset >= result.rows.len() {
            result.rows.clear();
            return Ok(());
        }
        result.rows = result.rows[offset..].to_vec();
    }

    if let Some(limit) = limit
        && limit < result.rows.len()
    {
        result.rows.truncate(limit);
    }

    Ok(())
}

fn parse_non_negative_int(value: &ScalarValue, what: &str) -> Result<usize, EngineError> {
    match value {
        ScalarValue::Int(v) if *v >= 0 => Ok(*v as usize),
        ScalarValue::Text(v) => {
            let parsed = v.parse::<usize>().map_err(|_| EngineError {
                message: format!("{what} must be a non-negative integer"),
            })?;
            Ok(parsed)
        }
        _ => Err(EngineError {
            message: format!("{what} must be a non-negative integer"),
        }),
    }
}

#[derive(Debug, Clone, Default)]
struct EvalScope {
    unqualified: HashMap<String, ScalarValue>,
    qualified: HashMap<String, ScalarValue>,
    ambiguous: HashSet<String>,
}

impl EvalScope {
    fn from_output_row(columns: &[String], row: &[ScalarValue]) -> Self {
        let mut scope = Self::default();
        for (col, value) in columns.iter().zip(row.iter()) {
            scope.insert_unqualified(col, value.clone());
        }
        scope
    }

    fn insert_unqualified(&mut self, key: &str, value: ScalarValue) {
        let key = key.to_ascii_lowercase();
        if self.ambiguous.contains(&key) {
            return;
        }
        #[allow(clippy::map_entry)]
        if self.unqualified.contains_key(&key) {
            self.unqualified.remove(&key);
            self.ambiguous.insert(key);
        } else {
            self.unqualified.insert(key, value);
        }
    }

    fn insert_qualified(&mut self, key: &str, value: ScalarValue) {
        self.qualified.insert(key.to_ascii_lowercase(), value);
    }

    fn lookup_identifier(&self, parts: &[String]) -> Result<ScalarValue, EngineError> {
        if parts.is_empty() {
            return Err(EngineError {
                message: "empty identifier".to_string(),
            });
        }

        if parts.len() == 1 {
            let key = parts[0].to_ascii_lowercase();
            if self.ambiguous.contains(&key) {
                return Err(EngineError {
                    message: format!("column reference \"{}\" is ambiguous", parts[0]),
                });
            }
            return self
                .unqualified
                .get(&key)
                .cloned()
                .ok_or_else(|| EngineError {
                    message: format!("unknown column \"{}\"", parts[0]),
                });
        }

        let key = parts
            .iter()
            .map(|p| p.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified
            .get(&key)
            .cloned()
            .ok_or_else(|| EngineError {
                message: format!("unknown column \"{}\"", parts.join(".")),
            })
    }

    fn lookup_join_column(&self, column: &str) -> Option<ScalarValue> {
        let key = column.to_ascii_lowercase();
        if !self.ambiguous.contains(&key)
            && let Some(value) = self.unqualified.get(&key)
        {
            return Some(value.clone());
        }

        let suffix = format!(".{}", key);
        let mut matches = self
            .qualified
            .iter()
            .filter_map(|(name, value)| name.ends_with(&suffix).then_some(value.clone()));
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn force_unqualified(&mut self, key: &str, value: ScalarValue) {
        let key = key.to_ascii_lowercase();
        self.ambiguous.remove(&key);
        self.unqualified.insert(key, value);
    }

    fn merge(&mut self, other: &Self) {
        for key in &other.ambiguous {
            self.unqualified.remove(key);
            self.ambiguous.insert(key.clone());
        }

        for (key, value) in &other.unqualified {
            self.insert_unqualified(key, value.clone());
        }
        for (key, value) in &other.qualified {
            self.qualified.insert(key.clone(), value.clone());
        }
    }

    fn inherit_outer(&mut self, outer: &Self) {
        for (key, value) in &outer.unqualified {
            if !self.unqualified.contains_key(key) && !self.ambiguous.contains(key) {
                self.unqualified.insert(key.clone(), value.clone());
            }
        }
        for (key, value) in &outer.qualified {
            self.qualified
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

fn eval_expr<'a>(
    expr: &'a Expr,
    scope: &'a EvalScope,
    params: &'a [Option<String>],
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
        Expr::Null => Ok(ScalarValue::Null),
        Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
        Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
        Expr::Float(v) => {
            let parsed = v.parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid float literal \"{v}\""),
            })?;
            Ok(ScalarValue::Float(parsed))
        }
        Expr::String(v) => Ok(ScalarValue::Text(v.clone())),
        Expr::Parameter(idx) => parse_param(*idx, params),
        Expr::Identifier(parts) => scope.lookup_identifier(parts),
        Expr::Unary { op, expr } => {
            let value = eval_expr(expr, scope, params).await?;
            eval_unary(op.clone(), value)
        }
        Expr::Binary { left, op, right } => {
            let lhs = eval_expr(left, scope, params).await?;
            let rhs = eval_expr(right, scope, params).await?;
            eval_binary(op.clone(), lhs, rhs)
        }
        Expr::AnyAll {
            left,
            op,
            right,
            quantifier,
        } => {
            let lhs = eval_expr(left, scope, params).await?;
            let rhs = eval_expr(right, scope, params).await?;
            eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
        }
        Expr::Exists(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            Ok(ScalarValue::Bool(!result.rows.is_empty()))
        }
        Expr::ScalarSubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if result.rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if result.rows.len() > 1 {
                return Err(EngineError {
                    message: "more than one row returned by a subquery used as an expression"
                        .to_string(),
                });
            }
            if result.rows[0].len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            Ok(result.rows[0][0].clone())
        }
        Expr::ArrayConstructor(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_expr(item, scope, params).await?);
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::ArraySubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let mut values = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                values.push(row.first().cloned().unwrap_or(ScalarValue::Null));
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lhs = eval_expr(expr, scope, params).await?;
            let mut rhs_values = Vec::with_capacity(list.len());
            for item in list {
                rhs_values.push(eval_expr(item, scope, params).await?);
            }
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let lhs = eval_expr(expr, scope, params).await?;
            let result = execute_query_with_outer(subquery, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let rhs_values = result
                .rows
                .iter()
                .map(|row| row.first().cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr(expr, scope, params).await?;
            let low_value = eval_expr(low, scope, params).await?;
            let high_value = eval_expr(high, scope, params).await?;
            eval_between_predicate(value, low_value, high_value, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let value = eval_expr(expr, scope, params).await?;
            let pattern_value = eval_expr(pattern, scope, params).await?;
            eval_like_predicate(value, pattern_value, *case_insensitive, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, scope, params).await?;
            let is_null = matches!(value, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_value = eval_expr(left, scope, params).await?;
            let right_value = eval_expr(right, scope, params).await?;
            eval_is_distinct_from(left_value, right_value, *negated)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let operand_value = eval_expr(operand, scope, params).await?;
            for (when_expr, then_expr) in when_then {
                let when_value = eval_expr(when_expr, scope, params).await?;
                if matches!(operand_value, ScalarValue::Null)
                    || matches!(when_value, ScalarValue::Null)
                {
                    continue;
                }
                if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal {
                    return eval_expr(then_expr, scope, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, scope, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when_expr, then_expr) in when_then {
                let condition = eval_expr(when_expr, scope, params).await?;
                if truthy(&condition) {
                    return eval_expr(then_expr, scope, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, scope, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::Cast { expr, type_name } => {
            let value = eval_expr(expr, scope, params).await?;
            eval_cast_scalar(value, type_name)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => eval_function(
            name,
            args,
            *distinct,
            order_by,
            within_group,
            filter.as_deref(),
            over.as_deref(),
            scope,
            params,
        )
        .await,
        Expr::Wildcard => Err(EngineError {
            message: "wildcard expression requires FROM support".to_string(),
        }),
    }
    })
}

fn eval_expr_with_window<'a>(
    expr: &'a Expr,
    scope: &'a EvalScope,
    row_idx: usize,
    all_rows: &'a [EvalScope],
    params: &'a [Option<String>],
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
        Expr::Null => Ok(ScalarValue::Null),
        Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
        Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
        Expr::Float(v) => {
            let parsed = v.parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid float literal \"{v}\""),
            })?;
            Ok(ScalarValue::Float(parsed))
        }
        Expr::String(v) => Ok(ScalarValue::Text(v.clone())),
        Expr::Parameter(idx) => parse_param(*idx, params),
        Expr::Identifier(parts) => scope.lookup_identifier(parts),
        Expr::Unary { op, expr } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            eval_unary(op.clone(), value)
        }
        Expr::Binary { left, op, right } => {
            let lhs = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let rhs = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_binary(op.clone(), lhs, rhs)
        }
        Expr::AnyAll {
            left,
            op,
            right,
            quantifier,
        } => {
            let lhs = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let rhs = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
        }
        Expr::Exists(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            Ok(ScalarValue::Bool(!result.rows.is_empty()))
        }
        Expr::ScalarSubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if result.rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if result.rows.len() > 1 {
                return Err(EngineError {
                    message: "more than one row returned by a subquery used as an expression"
                        .to_string(),
                });
            }
            if result.rows[0].len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            Ok(result.rows[0][0].clone())
        }
        Expr::ArrayConstructor(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_expr_with_window(item, scope, row_idx, all_rows, params).await?);
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::ArraySubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let mut values = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                values.push(row.first().cloned().unwrap_or(ScalarValue::Null));
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lhs = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let mut rhs = Vec::with_capacity(list.len());
            for item in list {
                rhs.push(eval_expr_with_window(item, scope, row_idx, all_rows, params).await?);
            }
            eval_in_membership(lhs, rhs, *negated)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let lhs = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let result = execute_query_with_outer(subquery, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let rhs_values = result
                .rows
                .iter()
                .map(|row| row.first().cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let low_value = eval_expr_with_window(low, scope, row_idx, all_rows, params).await?;
            let high_value = eval_expr_with_window(high, scope, row_idx, all_rows, params).await?;
            eval_between_predicate(value, low_value, high_value, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let pattern_value =
                eval_expr_with_window(pattern, scope, row_idx, all_rows, params).await?;
            eval_like_predicate(value, pattern_value, *case_insensitive, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let is_null = matches!(value, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_value = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let right_value = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_is_distinct_from(left_value, right_value, *negated)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let operand_value =
                eval_expr_with_window(operand, scope, row_idx, all_rows, params).await?;
            for (when_expr, then_expr) in when_then {
                let when_value =
                    eval_expr_with_window(when_expr, scope, row_idx, all_rows, params).await?;
                if matches!(operand_value, ScalarValue::Null)
                    || matches!(when_value, ScalarValue::Null)
                {
                    continue;
                }
                if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal {
                    return eval_expr_with_window(then_expr, scope, row_idx, all_rows, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr_with_window(else_expr, scope, row_idx, all_rows, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when_expr, then_expr) in when_then {
                let condition =
                    eval_expr_with_window(when_expr, scope, row_idx, all_rows, params).await?;
                if truthy(&condition) {
                    return eval_expr_with_window(then_expr, scope, row_idx, all_rows, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr_with_window(else_expr, scope, row_idx, all_rows, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::Cast { expr, type_name } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            eval_cast_scalar(value, type_name)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => {
            if let Some(window) = over.as_deref() {
                if !within_group.is_empty() {
                    return Err(EngineError {
                        message: "WITHIN GROUP is not allowed for window functions".to_string(),
                    });
                }
                eval_window_function(
                    name,
                    args,
                    *distinct,
                    order_by,
                filter.as_deref(),
                window,
                row_idx,
                all_rows,
                params,
            )
            .await
            } else {
                let fn_name = name
                    .last()
                    .map(|n| n.to_ascii_lowercase())
                    .unwrap_or_default();
                if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
                    return Err(EngineError {
                        message: format!(
                            "{}() aggregate modifiers require grouped aggregate evaluation",
                            fn_name
                        ),
                    });
                }
                if is_aggregate_function(&fn_name) {
                    return Err(EngineError {
                        message: format!(
                            "aggregate function {}() must be used with grouped evaluation",
                            fn_name
                        ),
                    });
                }

                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(
                        eval_expr_with_window(arg, scope, row_idx, all_rows, params).await?,
                    );
                }
                eval_scalar_function(&fn_name, &values).await
            }
        }
        Expr::Wildcard => Err(EngineError {
            message: "wildcard expression requires FROM support".to_string(),
        }),
    }
    })
}

#[allow(clippy::too_many_arguments)]
async fn eval_window_function(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    filter: Option<&Expr>,
    window: &WindowSpec,
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let fn_name = name
        .last()
        .map(|n| n.to_ascii_lowercase())
        .unwrap_or_default();
    let mut partition = window_partition_rows(window, row_idx, all_rows, params).await?;
    let order_keys = window_order_keys(window, &mut partition, all_rows, params).await?;
    let current_pos = partition
        .iter()
        .position(|entry| *entry == row_idx)
        .ok_or_else(|| EngineError {
            message: "window row not found in partition".to_string(),
        })?;

    match fn_name.as_str() {
        "row_number" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "row_number() does not accept arguments".to_string(),
                });
            }
            Ok(ScalarValue::Int((current_pos + 1) as i64))
        }
        "rank" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "rank() does not accept arguments".to_string(),
                });
            }
            if order_keys.is_empty() {
                return Ok(ScalarValue::Int(1));
            }
            let mut rank = 1usize;
            for idx in 1..=current_pos {
                if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by)
                    != Ordering::Equal
                {
                    rank = idx + 1;
                }
            }
            Ok(ScalarValue::Int(rank as i64))
        }
        "dense_rank" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "dense_rank() does not accept arguments".to_string(),
                });
            }
            if order_keys.is_empty() {
                return Ok(ScalarValue::Int(1));
            }
            let mut rank = 1i64;
            for idx in 1..=current_pos {
                if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by)
                    != Ordering::Equal
                {
                    rank += 1;
                }
            }
            Ok(ScalarValue::Int(rank))
        }
        "lag" | "lead" => {
            if args.is_empty() || args.len() > 3 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects 1 to 3 arguments"),
                });
            }
            let offset = if let Some(offset_expr) = args.get(1) {
                let offset_value = eval_expr(offset_expr, &all_rows[row_idx], params).await?;
                if matches!(offset_value, ScalarValue::Null) {
                    return Ok(ScalarValue::Null);
                }
                parse_non_negative_int(&offset_value, "window offset")?
            } else {
                1usize
            };
            let target_pos = if fn_name == "lag" {
                current_pos.checked_sub(offset)
            } else {
                current_pos.checked_add(offset)
            };
            let Some(target_pos) = target_pos else {
                return Ok(if let Some(default_expr) = args.get(2) {
                    eval_expr(default_expr, &all_rows[row_idx], params).await?
                } else {
                    ScalarValue::Null
                });
            };
            if target_pos >= partition.len() {
                return Ok(if let Some(default_expr) = args.get(2) {
                    eval_expr(default_expr, &all_rows[row_idx], params).await?
                } else {
                    ScalarValue::Null
                });
            }
            let target_row = partition[target_pos];
            eval_expr(&args[0], &all_rows[target_row], params).await
        }
        "ntile" => {
            if args.len() != 1 {
                return Err(EngineError { message: "ntile() expects exactly one argument".to_string() });
            }
            let n = {
                let v = eval_expr(&args[0], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "ntile() expects integer")? as usize
            };
            if n == 0 { return Err(EngineError { message: "ntile() argument must be positive".to_string() }); }
            let total = partition.len();
            let bucket = (current_pos * n / total) + 1;
            Ok(ScalarValue::Int(bucket as i64))
        }
        "percent_rank" => {
            if !args.is_empty() { return Err(EngineError { message: "percent_rank() takes no arguments".to_string() }); }
            if partition.len() <= 1 { return Ok(ScalarValue::Float(0.0)); }
            // rank - 1 / count - 1
            let mut rank = 1usize;
            if !order_keys.is_empty() {
                for idx in 1..=current_pos {
                    if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by) != Ordering::Equal {
                        rank = idx + 1;
                    }
                }
            }
            Ok(ScalarValue::Float((rank - 1) as f64 / (partition.len() - 1) as f64))
        }
        "cume_dist" => {
            if !args.is_empty() { return Err(EngineError { message: "cume_dist() takes no arguments".to_string() }); }
            if order_keys.is_empty() { return Ok(ScalarValue::Float(1.0)); }
            // Number of rows <= current row / total rows
            let current_key = &order_keys[current_pos];
            let count_le = order_keys.iter().filter(|k| {
                compare_order_keys(k, current_key, &window.order_by) != Ordering::Greater
            }).count();
            Ok(ScalarValue::Float(count_le as f64 / partition.len() as f64))
        }
        "first_value" => {
            if args.len() != 1 { return Err(EngineError { message: "first_value() expects one argument".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&first) = frame_rows.first() {
                eval_expr(&args[0], &all_rows[first], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "last_value" => {
            if args.len() != 1 { return Err(EngineError { message: "last_value() expects one argument".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&last) = frame_rows.last() {
                eval_expr(&args[0], &all_rows[last], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "nth_value" => {
            if args.len() != 2 { return Err(EngineError { message: "nth_value() expects two arguments".to_string() }); }
            let n = {
                let v = eval_expr(&args[1], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "nth_value() expects integer")? as usize
            };
            if n == 0 { return Err(EngineError { message: "nth_value() argument must be positive".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&target) = frame_rows.get(n - 1) {
                eval_expr(&args[0], &all_rows[target], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "sum" | "count" | "avg" | "min" | "max" | "string_agg" | "array_agg" => {
            let frame_rows = window_frame_rows(
                window,
                &partition,
                &order_keys,
                current_pos,
                all_rows,
                params,
            )
            .await?;
            let scoped_rows = frame_rows
                .iter()
                .map(|idx| all_rows[*idx].clone())
                .collect::<Vec<_>>();
            eval_aggregate_function(
                &fn_name,
                args,
                distinct,
                order_by,
                &[],
                filter,
                &scoped_rows,
                params,
            )
            .await
        }
        _ => Err(EngineError {
            message: format!("unsupported window function {}", fn_name),
        }),
    }
}

async fn window_partition_rows(
    window: &WindowSpec,
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<usize>, EngineError> {
    if window.partition_by.is_empty() {
        return Ok((0..all_rows.len()).collect());
    }
    let current_scope = &all_rows[row_idx];
    let mut current_key = Vec::with_capacity(window.partition_by.len());
    for expr in &window.partition_by {
        current_key.push(eval_expr(expr, current_scope, params).await?);
    }
    let current_key = row_key(&current_key);
    let mut out = Vec::new();
    for (idx, scope) in all_rows.iter().enumerate() {
        let mut key_values = Vec::with_capacity(window.partition_by.len());
        for expr in &window.partition_by {
            key_values.push(eval_expr(expr, scope, params).await?);
        }
        if row_key(&key_values) == current_key {
            out.push(idx);
        }
    }
    Ok(out)
}

async fn window_order_keys(
    window: &WindowSpec,
    partition: &mut [usize],
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    if window.order_by.is_empty() {
        return Ok(Vec::new());
    }
    let mut decorated = Vec::with_capacity(partition.len());
    for idx in partition.iter().copied() {
        let mut keys = Vec::with_capacity(window.order_by.len());
        for order in &window.order_by {
            keys.push(eval_expr(&order.expr, &all_rows[idx], params).await?);
        }
        decorated.push((idx, keys));
    }
    decorated.sort_by(|left, right| compare_order_keys(&left.1, &right.1, &window.order_by));
    for (slot, (idx, _)) in partition.iter_mut().zip(decorated.iter()) {
        *slot = *idx;
    }
    Ok(decorated.into_iter().map(|(_, keys)| keys).collect())
}

async fn window_frame_rows(
    window: &WindowSpec,
    partition: &[usize],
    order_keys: &[Vec<ScalarValue>],
    current_pos: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<usize>, EngineError> {
    let Some(frame) = window.frame.as_ref() else {
        return Ok(partition.to_vec());
    };

    match frame.units {
        WindowFrameUnits::Rows => {
            let start = frame_row_position(
                &frame.start,
                current_pos,
                partition.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            let end = frame_row_position(
                &frame.end,
                current_pos,
                partition.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            if start > end {
                return Ok(Vec::new());
            }
            Ok(partition[start..=end].to_vec())
        }
        WindowFrameUnits::Range => {
            if window.order_by.is_empty() {
                return Ok(partition.to_vec());
            }
            let ascending = window.order_by[0].ascending != Some(false);
            let current_value = window_first_order_numeric_key(
                order_keys,
                current_pos,
                partition[current_pos],
                all_rows,
                window,
                params,
            )
            .await?;
            let effective_current = if ascending { current_value } else { -current_value };
            let start = range_bound_threshold(
                &frame.start,
                effective_current,
                true,
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            let end = range_bound_threshold(
                &frame.end,
                effective_current,
                false,
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            if let (Some(start), Some(end)) = (start, end)
                && start > end
            {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            for (pos, row_idx) in partition.iter().enumerate() {
                let value = window_first_order_numeric_key(
                    order_keys,
                    pos,
                    *row_idx,
                    all_rows,
                    window,
                    params,
                )
                .await?;
                let effective = if ascending { value } else { -value };
                if let Some(start) = start
                    && effective < start
                {
                    continue;
                }
                if let Some(end) = end
                    && effective > end
                {
                    continue;
                }
                out.push(*row_idx);
            }
            Ok(out)
        }
    }
}

async fn frame_row_position(
    bound: &WindowFrameBound,
    current_pos: usize,
    partition_len: usize,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<usize, EngineError> {
    let max_pos = partition_len.saturating_sub(1);
    Ok(match bound {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::UnboundedFollowing => max_pos,
        WindowFrameBound::CurrentRow => current_pos,
        WindowFrameBound::OffsetPreceding(expr) => {
            let offset = frame_bound_offset_usize(expr, current_scope, params).await?;
            current_pos.saturating_sub(offset)
        }
        WindowFrameBound::OffsetFollowing(expr) => {
            let offset = frame_bound_offset_usize(expr, current_scope, params).await?;
            current_pos.saturating_add(offset).min(max_pos)
        }
    })
}

async fn frame_bound_offset_usize(
    expr: &Expr,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<usize, EngineError> {
    let value = eval_expr(expr, current_scope, params).await?;
    parse_non_negative_int(&value, "window frame offset")
}

async fn frame_bound_offset_f64(
    expr: &Expr,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<f64, EngineError> {
    let value = eval_expr(expr, current_scope, params).await?;
    let parsed = parse_f64_scalar(&value, "window frame offset must be numeric")?;
    if parsed < 0.0 {
        return Err(EngineError {
            message: "window frame offset must be a non-negative number".to_string(),
        });
    }
    Ok(parsed)
}

async fn range_bound_threshold(
    bound: &WindowFrameBound,
    current: f64,
    is_start: bool,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<Option<f64>, EngineError> {
    match bound {
        WindowFrameBound::UnboundedPreceding if is_start => Ok(None),
        WindowFrameBound::UnboundedFollowing if !is_start => Ok(None),
        WindowFrameBound::UnboundedFollowing if is_start => Ok(Some(f64::INFINITY)),
        WindowFrameBound::UnboundedPreceding if !is_start => Ok(Some(f64::NEG_INFINITY)),
        WindowFrameBound::CurrentRow => Ok(Some(current)),
        WindowFrameBound::OffsetPreceding(expr) => Ok(Some(
            current - frame_bound_offset_f64(expr, current_scope, params).await?,
        )),
        WindowFrameBound::OffsetFollowing(expr) => Ok(Some(
            current + frame_bound_offset_f64(expr, current_scope, params).await?,
        )),
        WindowFrameBound::UnboundedPreceding | WindowFrameBound::UnboundedFollowing => {
            Err(EngineError {
                message: "invalid RANGE frame bound configuration".to_string(),
            })
        }
    }
}

async fn window_first_order_numeric_key(
    order_keys: &[Vec<ScalarValue>],
    pos: usize,
    row_idx: usize,
    all_rows: &[EvalScope],
    window: &WindowSpec,
    params: &[Option<String>],
) -> Result<f64, EngineError> {
    if let Some(value) = order_keys.get(pos).and_then(|keys| keys.first()) {
        return parse_f64_scalar(value, "RANGE frame ORDER BY key must be numeric");
    }
    let expr = &window.order_by[0].expr;
    let value = eval_expr(expr, &all_rows[row_idx], params).await?;
    parse_f64_scalar(&value, "RANGE frame ORDER BY key must be numeric")
}

fn eval_in_membership(
    lhs: ScalarValue,
    rhs_values: Vec<ScalarValue>,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(lhs, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let mut saw_null = false;
    for rhs in rhs_values {
        if matches!(rhs, ScalarValue::Null) {
            saw_null = true;
            continue;
        }
        if compare_values_for_predicate(&lhs, &rhs)? == Ordering::Equal {
            return Ok(ScalarValue::Bool(!negated));
        }
    }

    if saw_null {
        Ok(ScalarValue::Null)
    } else {
        Ok(ScalarValue::Bool(negated))
    }
}

fn eval_between_predicate(
    value: ScalarValue,
    low: ScalarValue,
    high: ScalarValue,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null)
        || matches!(low, ScalarValue::Null)
        || matches!(high, ScalarValue::Null)
    {
        return Ok(ScalarValue::Null);
    }
    let in_range = compare_values_for_predicate(&value, &low)? != Ordering::Less
        && compare_values_for_predicate(&value, &high)? != Ordering::Greater;
    Ok(ScalarValue::Bool(if negated {
        !in_range
    } else {
        in_range
    }))
}

fn eval_like_predicate(
    value: ScalarValue,
    pattern: ScalarValue,
    case_insensitive: bool,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) || matches!(pattern, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut text = value.render();
    let mut pattern_text = pattern.render();
    if case_insensitive {
        text = text.to_ascii_lowercase();
        pattern_text = pattern_text.to_ascii_lowercase();
    }
    let matched = like_match(&text, &pattern_text);
    Ok(ScalarValue::Bool(if negated { !matched } else { matched }))
}

fn eval_is_distinct_from(
    left: ScalarValue,
    right: ScalarValue,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    let distinct = match (&left, &right) {
        (ScalarValue::Null, ScalarValue::Null) => false,
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => true,
        _ => compare_values_for_predicate(&left, &right)? != Ordering::Equal,
    };
    Ok(ScalarValue::Bool(if negated {
        !distinct
    } else {
        distinct
    }))
}

fn eval_cast_scalar(value: ScalarValue, type_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match type_name {
        "boolean" => Ok(ScalarValue::Bool(parse_bool_scalar(
            &value,
            "cannot cast value to boolean",
        )?)),
        "int8" => Ok(ScalarValue::Int(parse_i64_scalar(
            &value,
            "cannot cast value to bigint",
        )?)),
        "float8" => Ok(ScalarValue::Float(parse_f64_scalar(
            &value,
            "cannot cast value to double precision",
        )?)),
        "text" => Ok(ScalarValue::Text(value.render())),
        "date" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        "timestamp" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        other => Err(EngineError {
            message: format!("unsupported cast type {}", other),
        }),
    }
}

fn like_match(value: &str, pattern: &str) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let mut memo = HashMap::new();
    like_match_recursive(&value_chars, &pattern_chars, 0, 0, &mut memo)
}

fn like_match_recursive(
    value: &[char],
    pattern: &[char],
    vi: usize,
    pi: usize,
    memo: &mut HashMap<(usize, usize), bool>,
) -> bool {
    if let Some(cached) = memo.get(&(vi, pi)) {
        return *cached;
    }

    let result = if pi >= pattern.len() {
        vi >= value.len()
    } else {
        match pattern[pi] {
            '%' => {
                let mut i = vi;
                let mut matched = false;
                while i <= value.len() {
                    if like_match_recursive(value, pattern, i, pi + 1, memo) {
                        matched = true;
                        break;
                    }
                    i += 1;
                }
                matched
            }
            '_' => vi < value.len() && like_match_recursive(value, pattern, vi + 1, pi + 1, memo),
            '\\' => {
                if pi + 1 >= pattern.len() {
                    vi < value.len()
                        && value[vi] == '\\'
                        && like_match_recursive(value, pattern, vi + 1, pi + 1, memo)
                } else {
                    vi < value.len()
                        && value[vi] == pattern[pi + 1]
                        && like_match_recursive(value, pattern, vi + 1, pi + 2, memo)
                }
            }
            ch => {
                vi < value.len()
                    && value[vi] == ch
                    && like_match_recursive(value, pattern, vi + 1, pi + 1, memo)
            }
        }
    };

    memo.insert((vi, pi), result);
    result
}

fn parse_param(index: i32, params: &[Option<String>]) -> Result<ScalarValue, EngineError> {
    if index <= 0 {
        return Err(EngineError {
            message: format!("invalid parameter reference ${index}"),
        });
    }
    let idx = (index - 1) as usize;
    let value = params.get(idx).ok_or_else(|| EngineError {
        message: format!("missing value for parameter ${index}"),
    })?;

    let Some(raw) = value else {
        return Ok(ScalarValue::Null);
    };

    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(ScalarValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(ScalarValue::Bool(false));
    }
    if let Ok(v) = trimmed.parse::<i64>() {
        return Ok(ScalarValue::Int(v));
    }
    if let Ok(v) = trimmed.parse::<f64>() {
        return Ok(ScalarValue::Float(v));
    }
    Ok(ScalarValue::Text(raw.clone()))
}

fn eval_unary(op: UnaryOp, value: ScalarValue) -> Result<ScalarValue, EngineError> {
    match (op, value) {
        (UnaryOp::Not, ScalarValue::Bool(v)) => Ok(ScalarValue::Bool(!v)),
        (UnaryOp::Not, ScalarValue::Null) => Ok(ScalarValue::Null),
        (UnaryOp::Plus, ScalarValue::Int(v)) => Ok(ScalarValue::Int(v)),
        (UnaryOp::Plus, ScalarValue::Float(v)) => Ok(ScalarValue::Float(v)),
        (UnaryOp::Minus, ScalarValue::Int(v)) => Ok(ScalarValue::Int(-v)),
        (UnaryOp::Minus, ScalarValue::Float(v)) => Ok(ScalarValue::Float(-v)),
        _ => Err(EngineError {
            message: "invalid unary operation".to_string(),
        }),
    }
}

fn eval_binary(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    use BinaryOp::*;
    match op {
        Or => eval_logical_or(left, right),
        And => eval_logical_and(left, right),
        Eq => eval_comparison(left, right, |ord| ord == Ordering::Equal),
        NotEq => eval_comparison(left, right, |ord| ord != Ordering::Equal),
        Lt => eval_comparison(left, right, |ord| ord == Ordering::Less),
        Lte => eval_comparison(left, right, |ord| {
            matches!(ord, Ordering::Less | Ordering::Equal)
        }),
        Gt => eval_comparison(left, right, |ord| ord == Ordering::Greater),
        Gte => eval_comparison(left, right, |ord| {
            matches!(ord, Ordering::Greater | Ordering::Equal)
        }),
        Add => eval_add(left, right),
        Sub => eval_sub(left, right),
        Mul => numeric_bin(left, right, |a, b| a * b, |a, b| a * b),
        Div => numeric_div(left, right),
        Mod => numeric_mod(left, right),
        JsonGet => eval_json_get_operator(left, right, false),
        JsonGetText => eval_json_get_operator(left, right, true),
        JsonPath => eval_json_path_operator(left, right, false),
        JsonPathText => eval_json_path_operator(left, right, true),
        JsonPathExists => eval_json_path_predicate_operator(left, right, false),
        JsonPathMatch => eval_json_path_predicate_operator(left, right, true),
        JsonConcat => eval_json_concat_operator(left, right),
        JsonContains => eval_json_contains_operator(left, right),
        JsonContainedBy => eval_json_contained_by_operator(left, right),
        JsonHasKey => eval_json_has_key_operator(left, right),
        JsonHasAny => eval_json_has_any_all_operator(left, right, true),
        JsonHasAll => eval_json_has_any_all_operator(left, right, false),
        JsonDeletePath => eval_json_delete_path_operator(left, right),
    }
}

fn eval_add(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    if parse_numeric_operand(&left).is_ok() && parse_numeric_operand(&right).is_ok() {
        return numeric_bin(left, right, |a, b| a + b, |a, b| a + b);
    }

    if let Some(lhs) = parse_temporal_operand(&left) {
        let days = parse_i64_scalar(&right, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(lhs, days));
    }
    if let Some(rhs) = parse_temporal_operand(&right) {
        let days = parse_i64_scalar(&left, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(rhs, days));
    }

    Err(EngineError {
        message: "numeric operation expects numeric values".to_string(),
    })
}

fn eval_sub(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    if parse_numeric_operand(&left).is_ok() && parse_numeric_operand(&right).is_ok() {
        return numeric_bin(left, right, |a, b| a - b, |a, b| a - b);
    }

    if let Some(lhs) = parse_temporal_operand(&left) {
        if let Some(rhs) = parse_temporal_operand(&right) {
            if lhs.date_only && rhs.date_only {
                let left_days = days_from_civil(
                    lhs.datetime.date.year,
                    lhs.datetime.date.month,
                    lhs.datetime.date.day,
                );
                let right_days = days_from_civil(
                    rhs.datetime.date.year,
                    rhs.datetime.date.month,
                    rhs.datetime.date.day,
                );
                return Ok(ScalarValue::Int(left_days - right_days));
            }
            let left_epoch = datetime_to_epoch_seconds(lhs.datetime);
            let right_epoch = datetime_to_epoch_seconds(rhs.datetime);
            return Ok(ScalarValue::Int(left_epoch - right_epoch));
        }
        let days = parse_i64_scalar(&right, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(lhs, -days));
    }

    if matches!(left, ScalarValue::Text(_))
        && parse_json_document_arg(&left, "json operator -", 1).is_ok()
    {
        return eval_json_delete_operator(left, right);
    }

    Err(EngineError {
        message: "numeric operation expects numeric values".to_string(),
    })
}

fn eval_logical_or(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let lhs = parse_nullable_bool(&left, "argument of OR must be type boolean or null")?;
    let rhs = parse_nullable_bool(&right, "argument of OR must be type boolean or null")?;
    Ok(match (lhs, rhs) {
        (Some(true), _) | (_, Some(true)) => ScalarValue::Bool(true),
        (Some(false), Some(false)) => ScalarValue::Bool(false),
        _ => ScalarValue::Null,
    })
}

fn eval_logical_and(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let lhs = parse_nullable_bool(&left, "argument of AND must be type boolean or null")?;
    let rhs = parse_nullable_bool(&right, "argument of AND must be type boolean or null")?;
    Ok(match (lhs, rhs) {
        (Some(false), _) | (_, Some(false)) => ScalarValue::Bool(false),
        (Some(true), Some(true)) => ScalarValue::Bool(true),
        _ => ScalarValue::Null,
    })
}

fn eval_comparison(
    left: ScalarValue,
    right: ScalarValue,
    predicate: impl Fn(Ordering) -> bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    Ok(ScalarValue::Bool(predicate(compare_values_for_predicate(
        &left, &right,
    )?)))
}

fn compare_any_all_predicate(
    op: BinaryOp,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Option<bool>, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(None);
    }
    let ord = compare_values_for_predicate(left, right)?;
    let result = match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::NotEq => ord != Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::Lte => matches!(ord, Ordering::Less | Ordering::Equal),
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::Gte => matches!(ord, Ordering::Greater | Ordering::Equal),
        _ => {
            return Err(EngineError {
                message: "ANY/ALL expects comparison operator".to_string(),
            })
        }
    };
    Ok(Some(result))
}

fn eval_any_all(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
    quantifier: ComparisonQuantifier,
) -> Result<ScalarValue, EngineError> {
    if matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let items = match right {
        ScalarValue::Array(values) => values,
        _ => {
            return Err(EngineError {
                message: "ANY/ALL expects array argument".to_string(),
            })
        }
    };
    if items.is_empty() {
        return Ok(ScalarValue::Bool(matches!(
            quantifier,
            ComparisonQuantifier::All
        )));
    }
    let mut saw_null = false;
    for item in items {
        match compare_any_all_predicate(op.clone(), &left, &item)? {
            Some(true) => {
                if matches!(quantifier, ComparisonQuantifier::Any) {
                    return Ok(ScalarValue::Bool(true));
                }
            }
            Some(false) => {
                if matches!(quantifier, ComparisonQuantifier::All) {
                    return Ok(ScalarValue::Bool(false));
                }
            }
            None => {
                saw_null = true;
            }
        }
    }
    match quantifier {
        ComparisonQuantifier::Any => {
            if saw_null {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(false))
            }
        }
        ComparisonQuantifier::All => {
            if saw_null {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(true))
            }
        }
    }
}

fn numeric_bin(
    left: ScalarValue,
    right: ScalarValue,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;
    match (left_num, right_num) {
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(int_op(a, b))),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a as f64, b)))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(float_op(a, b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a, b)))
        }
    }
}

fn numeric_div(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;
    match (left_num, right_num) {
        (NumericOperand::Int(_), NumericOperand::Int(0))
        | (NumericOperand::Int(_), NumericOperand::Float(0.0))
        | (NumericOperand::Float(_), NumericOperand::Int(0))
        | (NumericOperand::Float(_), NumericOperand::Float(0.0)) => Err(EngineError {
            message: "division by zero".to_string(),
        }),
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(a / b)),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float((a as f64) / b))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(a / (b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => Ok(ScalarValue::Float(a / b)),
    }
}

fn numeric_mod(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_int = parse_i64_scalar(&left, "operator % expects integer values")?;
    let right_int = parse_i64_scalar(&right, "operator % expects integer values")?;
    match (left_int, right_int) {
        (_, 0) => Err(EngineError {
            message: "division by zero".to_string(),
        }),
        (a, b) => Ok(ScalarValue::Int(a % b)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn eval_function(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    within_group: &[OrderByExpr],
    filter: Option<&Expr>,
    over: Option<&crate::parser::ast::WindowSpec>,
    scope: &EvalScope,
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let fn_name = name
        .last()
        .map(|n| n.to_ascii_lowercase())
        .unwrap_or_default();
    if distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
        return Err(EngineError {
            message: format!(
                "{}() aggregate modifiers require grouped aggregate evaluation",
                fn_name
            ),
        });
    }
    if over.is_some() {
        return Err(EngineError {
            message: "window functions require SELECT window evaluation context".to_string(),
        });
    }
    if is_aggregate_function(&fn_name) {
        return Err(EngineError {
            message: format!(
                "aggregate function {}() must be used with grouped evaluation",
                fn_name
            ),
        });
    }

    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_expr(arg, scope, params).await?);
    }

    // Handle schema-qualified extension functions (ws.connect, ws.send, ws.close)
    if name.len() == 2 {
        let schema = name[0].to_ascii_lowercase();
        if schema == "ws" {
            match fn_name.as_str() {
                "connect" => return execute_ws_connect(&values).await,
                "send" => return execute_ws_send(&values).await,
                "close" => return execute_ws_close(&values).await,
                "recv" => return execute_ws_recv(&values).await,
                _ => {
                    return Err(EngineError {
                        message: format!("function ws.{}() does not exist", fn_name),
                    });
                }
            }
        }
    }

    eval_scalar_function(&fn_name, &values).await
}

async fn eval_scalar_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "http_get" if args.len() == 1 => eval_http_get_builtin(&args[0]).await,
        "row" => Ok(ScalarValue::Text(
            JsonValue::Array(
                args.iter()
                    .map(scalar_to_json_value)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .to_string(),
        )),
        "to_json" | "to_jsonb" if args.len() == 1 => Ok(ScalarValue::Text(
            scalar_to_json_value(&args[0])?.to_string(),
        )),
        "row_to_json" if args.len() == 1 || args.len() == 2 => eval_row_to_json(args, fn_name),
        "array_to_json" if args.len() == 1 || args.len() == 2 => eval_array_to_json(args, fn_name),
        "json_object" if args.len() == 1 || args.len() == 2 => eval_json_object(args, fn_name),
        "json_build_object" | "jsonb_build_object" if args.len().is_multiple_of(2) => Ok(ScalarValue::Text(
            json_build_object_value(args)?.to_string(),
        )),
        "json_build_array" | "jsonb_build_array" => {
            Ok(ScalarValue::Text(json_build_array_value(args)?.to_string()))
        }
        "json_extract_path" | "jsonb_extract_path" if args.len() >= 2 => {
            eval_json_extract_path(args, false, fn_name)
        }
        "json_extract_path_text" | "jsonb_extract_path_text" if args.len() >= 2 => {
            eval_json_extract_path(args, true, fn_name)
        }
        "json_array_length" | "jsonb_array_length" if args.len() == 1 => {
            eval_json_array_length(&args[0], fn_name)
        }
        "json_typeof" | "jsonb_typeof" if args.len() == 1 => eval_json_typeof(&args[0], fn_name),
        "json_strip_nulls" | "jsonb_strip_nulls" if args.len() == 1 => {
            eval_json_strip_nulls(&args[0], fn_name)
        }
        "json_pretty" | "jsonb_pretty" if args.len() == 1 => eval_json_pretty(&args[0], fn_name),
        "jsonb_exists" if args.len() == 2 => eval_jsonb_exists(&args[0], &args[1]),
        "jsonb_exists_any" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], true, fn_name)
        }
        "jsonb_exists_all" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], false, fn_name)
        }
        "jsonb_path_exists" if args.len() >= 2 => eval_jsonb_path_exists(args, fn_name),
        "jsonb_path_match" if args.len() >= 2 => eval_jsonb_path_match(args, fn_name),
        "jsonb_path_query" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_path_query_array" if args.len() >= 2 => eval_jsonb_path_query_array(args, fn_name),
        "jsonb_path_query_first" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_set" if args.len() == 3 || args.len() == 4 => eval_jsonb_set(args),
        "jsonb_insert" if args.len() == 3 || args.len() == 4 => eval_jsonb_insert(args),
        "jsonb_set_lax" if args.len() >= 3 && args.len() <= 5 => eval_jsonb_set_lax(args),
        "nextval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "nextval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                let value = sequence_next_value(state, &sequence_name)?;
                Ok(ScalarValue::Int(value))
            })
        }
        "currval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "currval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_read(|sequences| {
                let Some(state) = sequences.get(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                if !state.called {
                    return Err(EngineError {
                        message: format!(
                            "currval of sequence \"{}\" is not yet defined",
                            sequence_name
                        ),
                    });
                }
                Ok(ScalarValue::Int(state.current))
            })
        }
        "setval" if args.len() == 2 || args.len() == 3 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "setval() expects text sequence name".to_string(),
                    });
                }
            };
            let value = parse_i64_scalar(&args[1], "setval() expects integer value")?;
            let is_called = if args.len() == 3 {
                parse_bool_scalar(&args[2], "setval() expects boolean third argument")?
            } else {
                true
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                set_sequence_value(state, &sequence_name, value, is_called)?;
                Ok(ScalarValue::Int(value))
            })
        }
        "lower" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_lowercase()))
        }
        "upper" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_uppercase()))
        }
        "length" | "char_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().chars().count() as i64))
        }
        "abs" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.abs())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.abs())),
            _ => Err(EngineError {
                message: "abs() expects numeric argument".to_string(),
            }),
        },
        "nullif" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            if matches!(args[1], ScalarValue::Null) {
                return Ok(args[0].clone());
            }
            if compare_values_for_predicate(&args[0], &args[1])? == Ordering::Equal {
                Ok(ScalarValue::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "greatest" if !args.is_empty() => eval_extremum(args, true),
        "least" if !args.is_empty() => eval_extremum(args, false),
        "concat" => {
            let mut out = String::new();
            for arg in args {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                out.push_str(&arg.render());
            }
            Ok(ScalarValue::Text(out))
        }
        "concat_ws" if !args.is_empty() => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let separator = args[0].render();
            let mut parts = Vec::new();
            for arg in &args[1..] {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                parts.push(arg.render());
            }
            Ok(ScalarValue::Text(parts.join(&separator)))
        }
        "substring" | "substr" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let start = parse_i64_scalar(&args[1], "substring() expects integer start index")?;
            let length = if args.len() == 3 {
                Some(parse_i64_scalar(
                    &args[2],
                    "substring() expects integer length",
                )?)
            } else {
                None
            };
            Ok(ScalarValue::Text(substring_chars(&input, start, length)?))
        }
        "position" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let needle = args[0].render();
            let haystack = args[1].render();
            Ok(ScalarValue::Int(find_substring_position(&haystack, &needle)))
        }
        "overlay" if args.len() == 3 || args.len() == 4 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let replacement = args[1].render();
            let start = parse_i64_scalar(&args[2], "overlay() expects integer start")?;
            let count = if args.len() == 4 {
                Some(parse_i64_scalar(&args[3], "overlay() expects integer count")?)
            } else {
                None
            };
            Ok(ScalarValue::Text(overlay_text(
                &input,
                &replacement,
                start,
                count,
            )?))
        }
        "left" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "left() expects integer length")?;
            Ok(ScalarValue::Text(left_chars(&input, count)))
        }
        "right" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "right() expects integer length")?;
            Ok(ScalarValue::Text(right_chars(&input, count)))
        }
        "btrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Both,
            )))
        }
        "ltrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Left,
            )))
        }
        "rtrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Right,
            )))
        }
        "replace" if args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let from = args[1].render();
            let to = args[2].render();
            Ok(ScalarValue::Text(input.replace(&from, &to)))
        }
        "ascii" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(ascii_code(&args[0].render())))
        }
        "chr" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let code = parse_i64_scalar(&args[0], "chr() expects integer")?;
            Ok(ScalarValue::Text(chr_from_code(code)?))
        }
        "encode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let data = args[0].render();
            let format = args[1].render();
            Ok(ScalarValue::Text(encode_bytes(data.as_bytes(), &format)?))
        }
        "decode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let format = args[1].render();
            let decoded = decode_bytes(&input, &format)?;
            Ok(ScalarValue::Text(String::from_utf8_lossy(&decoded).to_string()))
        }
        "date" if args.len() == 1 => eval_date_function(&args[0]),
        "timestamp" if args.len() == 1 => eval_timestamp_function(&args[0]),
        "now" | "current_timestamp" if args.is_empty() => {
            Ok(ScalarValue::Text(current_timestamp_string()?))
        }
        "clock_timestamp" if args.is_empty() => Ok(ScalarValue::Text(current_timestamp_string()?)),
        "current_date" if args.is_empty() => Ok(ScalarValue::Text(current_date_string()?)),
        "age" if args.len() == 1 || args.len() == 2 => eval_age(args),
        "extract" | "date_part" if args.len() == 2 => eval_extract_or_date_part(&args[0], &args[1]),
        "date_trunc" if args.len() == 2 => eval_date_trunc(&args[0], &args[1]),
        "date_add" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], true),
        "date_sub" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], false),
        "to_timestamp" if args.len() == 1 => eval_to_timestamp(&args[0]),
        "to_timestamp" if args.len() == 2 => eval_to_timestamp_with_format(&args[0], &args[1]),
        "to_date" if args.len() == 2 => eval_to_date_with_format(&args[0], &args[1]),
        "make_interval" if args.len() == 7 => eval_make_interval(args),
        "justify_hours" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Hours),
        "justify_days" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Days),
        "justify_interval" if args.len() == 1 => {
            eval_justify_interval(&args[0], JustifyMode::Full)
        }
        "isfinite" if args.len() == 1 => eval_isfinite(&args[0]),
        "coalesce" if !args.is_empty() => {
            for value in args {
                if !matches!(value, ScalarValue::Null) {
                    return Ok(value.clone());
                }
            }
            Ok(ScalarValue::Null)
        }
        // --- Math functions ---
        "ceil" | "ceiling" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.ceil())),
            _ => Err(EngineError { message: "ceil() expects numeric argument".to_string() }),
        },
        "floor" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.floor())),
            _ => Err(EngineError { message: "floor() expects numeric argument".to_string() }),
        },
        "round" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "round() expects integer scale")?
            } else { 0 };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).round() / factor))
                }
                _ => Err(EngineError { message: "round() expects numeric argument".to_string() }),
            }
        },
        "trunc" | "truncate" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "trunc() expects integer scale")?
            } else { 0 };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).trunc() / factor))
                }
                _ => Err(EngineError { message: "trunc() expects numeric argument".to_string() }),
            }
        },
        "power" | "pow" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let base = coerce_to_f64(&args[0], "power()")?;
            let exp = coerce_to_f64(&args[1], "power()")?;
            Ok(ScalarValue::Float(base.powf(exp)))
        },
        "sqrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "sqrt()")?;
            Ok(ScalarValue::Float(v.sqrt()))
        },
        "cbrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "cbrt()")?;
            Ok(ScalarValue::Float(v.cbrt()))
        },
        "exp" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "exp()")?;
            Ok(ScalarValue::Float(v.exp()))
        },
        "ln" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "ln()")?;
            Ok(ScalarValue::Float(v.ln()))
        },
        "log" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "log()")?;
            Ok(ScalarValue::Float(v.log10()))
        },
        "log" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let base = coerce_to_f64(&args[0], "log()")?;
            let v = coerce_to_f64(&args[1], "log()")?;
            Ok(ScalarValue::Float(v.log(base)))
        },
        "sin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "sin()")?.sin()))
        },
        "cos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "cos()")?.cos()))
        },
        "tan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "tan()")?.tan()))
        },
        "asin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "asin()")?.asin()))
        },
        "acos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "acos()")?.acos()))
        },
        "atan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "atan()")?.atan()))
        },
        "atan2" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = coerce_to_f64(&args[0], "atan2()")?;
            let x = coerce_to_f64(&args[1], "atan2()")?;
            Ok(ScalarValue::Float(y.atan2(x)))
        },
        "degrees" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "degrees()")?.to_degrees()))
        },
        "radians" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "radians()")?.to_radians()))
        },
        "sign" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.signum())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(if *f > 0.0 { 1.0 } else if *f < 0.0 { -1.0 } else { 0.0 })),
            _ => Err(EngineError { message: "sign() expects numeric argument".to_string() }),
        },
        "width_bucket" if args.len() == 4 => eval_width_bucket(args),
        "scale" if args.len() == 1 => eval_scale(&args[0]),
        "factorial" if args.len() == 1 => eval_factorial(&args[0]),
        "pi" if args.is_empty() => Ok(ScalarValue::Float(std::f64::consts::PI)),
        "random" if args.is_empty() => Ok(ScalarValue::Float(rand_f64())),
        "mod" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_binary(BinaryOp::Mod, args[0].clone(), args[1].clone())
        },
        "div" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = coerce_to_f64(&args[0], "div()")?;
            let b = coerce_to_f64(&args[1], "div()")?;
            if b == 0.0 { return Err(EngineError { message: "division by zero".to_string() }); }
            Ok(ScalarValue::Int((a / b).trunc() as i64))
        },
        "gcd" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = parse_i64_scalar(&args[0], "gcd() expects integer")?;
            let b = parse_i64_scalar(&args[1], "gcd() expects integer")?;
            Ok(ScalarValue::Int(gcd_i64(a, b)))
        },
        "lcm" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = parse_i64_scalar(&args[0], "lcm() expects integer")?;
            let b = parse_i64_scalar(&args[1], "lcm() expects integer")?;
            let g = gcd_i64(a, b);
            Ok(ScalarValue::Int(if g == 0 { 0 } else { (a / g * b).abs() }))
        },
        // --- Additional string functions ---
        "initcap" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(initcap_string(&args[0].render())))
        },
        "repeat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let s = args[0].render();
            let n = parse_i64_scalar(&args[1], "repeat() expects integer count")?;
            if n < 0 { return Ok(ScalarValue::Text(String::new())); }
            Ok(ScalarValue::Text(s.repeat(n as usize)))
        },
        "reverse" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(args[0].render().chars().rev().collect()))
        },
        "translate" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let from: Vec<char> = args[1].render().chars().collect();
            let to: Vec<char> = args[2].render().chars().collect();
            let result: String = input.chars().filter_map(|c| {
                if let Some(pos) = from.iter().position(|f| *f == c) {
                    to.get(pos).copied()
                } else {
                    Some(c)
                }
            }).collect();
            Ok(ScalarValue::Text(result))
        },
        "split_part" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let delimiter = args[1].render();
            let field = parse_i64_scalar(&args[2], "split_part() expects integer field")?;
            if field <= 0 { return Err(EngineError { message: "field position must be greater than zero".to_string() }); }
            let parts: Vec<&str> = input.split(&delimiter).collect();
            Ok(ScalarValue::Text(parts.get((field - 1) as usize).unwrap_or(&"").to_string()))
        },
        "strpos" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let haystack = args[0].render();
            let needle = args[1].render();
            Ok(ScalarValue::Int(haystack.find(&needle).map(|i| i as i64 + 1).unwrap_or(0)))
        },
        "lpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "lpad() expects integer length")? as usize;
            let fill = if args.len() == 3 { args[2].render() } else { " ".to_string() };
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, true)))
        },
        "rpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "rpad() expects integer length")? as usize;
            let fill = if args.len() == 3 { args[2].render() } else { " ".to_string() };
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, false)))
        },
        "quote_literal" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(quote_literal(&args[0].render())))
        },
        "quote_ident" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(quote_ident(&args[0].render())))
        },
        "quote_nullable" if args.len() == 1 => Ok(ScalarValue::Text(quote_nullable(&args[0]))),
        "md5" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(md5_hex(&args[0].render())))
        },
        "regexp_match" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_match(&args[0].render(), &args[1].render(), "")
        },
        "regexp_match" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_match(&args[0].render(), &args[1].render(), &args[2].render())
        },
        "regexp_replace" if args.len() == 3 || args.len() == 4 => {
            if args.iter().take(3).any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let source = args[0].render();
            let pattern = args[1].render();
            let replacement = args[2].render();
            let flags = if args.len() == 4 { args[3].render() } else { String::new() };
            eval_regexp_replace(&source, &pattern, &replacement, &flags)
        },
        "regexp_split_to_array" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_split_to_array(&args[0].render(), &args[1].render())
        },
        "num_nulls" => Ok(ScalarValue::Int(count_nulls(args) as i64)),
        "num_nonnulls" => Ok(ScalarValue::Int(count_nonnulls(args) as i64)),
        // --- System info functions ---
        "version" if args.is_empty() => {
            Ok(ScalarValue::Text("Postrust 0.1.0 on Rust".to_string()))
        },
        "current_database" if args.is_empty() => {
            Ok(ScalarValue::Text("postrust".to_string()))
        },
        "current_schema" if args.is_empty() => {
            Ok(ScalarValue::Text("public".to_string()))
        },
        "current_user" | "session_user" | "user" if args.is_empty() => {
            let role = security::current_role();
            Ok(ScalarValue::Text(role))
        },
        "pg_backend_pid" if args.is_empty() => {
            Ok(ScalarValue::Int(std::process::id() as i64))
        },
        "pg_get_userbyid" if args.len() == 1 => {
            Ok(ScalarValue::Text("postrust".to_string()))
        },
        "has_table_privilege" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Bool(true))
        },
        "has_column_privilege" if args.len() == 3 || args.len() == 4 => {
            Ok(ScalarValue::Bool(true))
        },
        "has_schema_privilege" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Bool(true))
        },
        "pg_get_expr" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Null)
        },
        "pg_table_is_visible" if args.len() == 1 => {
            Ok(ScalarValue::Bool(true))
        },
        "pg_type_is_visible" if args.len() == 1 => {
            Ok(ScalarValue::Bool(true))
        },
        "obj_description" | "col_description" | "shobj_description" if !args.is_empty() => {
            Ok(ScalarValue::Null)
        },
        "format_type" if args.len() == 2 => {
            Ok(ScalarValue::Text("unknown".to_string()))
        },
        "pg_catalog.format_type" if args.len() == 2 => {
            Ok(ScalarValue::Text("unknown".to_string()))
        },
        // --- Make date/time ---
        "make_date" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = parse_i64_scalar(&args[0], "make_date() year")? as i32;
            let m = parse_i64_scalar(&args[1], "make_date() month")? as u32;
            let d = parse_i64_scalar(&args[2], "make_date() day")? as u32;
            Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02}", y, m, d)))
        },
        "make_timestamp" if args.len() == 6 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = parse_i64_scalar(&args[0], "year")? as i32;
            let mo = parse_i64_scalar(&args[1], "month")? as u32;
            let d = parse_i64_scalar(&args[2], "day")? as u32;
            let h = parse_i64_scalar(&args[3], "hour")? as u32;
            let mi = parse_i64_scalar(&args[4], "min")? as u32;
            let s = coerce_to_f64(&args[5], "sec")?;
            let sec = s.trunc() as u32;
            let frac = ((s - s.trunc()) * 1_000_000.0).round() as u32;
            if frac == 0 {
                Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, mo, d, h, mi, sec)))
            } else {
                Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, mo, d, h, mi, sec, frac)))
            }
        },
        "to_char" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            // Simplified: just return the first arg rendered
            Ok(ScalarValue::Text(args[0].render()))
        },
        "to_number" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let s = args[0].render();
            let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-').collect();
            match cleaned.parse::<f64>() {
                Ok(v) => Ok(ScalarValue::Float(v)),
                Err(_) => Err(EngineError { message: format!("invalid input for to_number: {}", s) }),
            }
        },
        "array_append" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let mut values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_append() expects array as first argument".to_string(),
                    })
                }
            };
            values.push(args[1].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_prepend" if args.len() == 2 => {
            if matches!(args[1], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let mut values = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_prepend() expects array as second argument".to_string(),
                    })
                }
            };
            values.insert(0, args[0].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_cat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let mut left = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    })
                }
            };
            let right = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    })
                }
            };
            left.extend(right);
            Ok(ScalarValue::Array(left))
        }
        "array_remove" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_remove() expects array as first argument".to_string(),
                    })
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if !array_value_matches(&args[1], value)? {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_replace" if args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_replace() expects array as first argument".to_string(),
                    })
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if array_value_matches(&args[1], value)? {
                    out.push(args[2].clone());
                } else {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_position" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_position() expects array as first argument".to_string(),
                    })
                }
            };
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    return Ok(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Null)
        }
        "array_positions" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_positions() expects array as first argument".to_string(),
                    })
                }
            };
            let mut positions = Vec::new();
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    positions.push(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Array(positions))
        }
        "array_length" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_length() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_length() expects integer dimension")?;
            if dim != 1 {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_dims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_dims() expects array argument".to_string(),
                    })
                }
            };
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(format!("[1:{}]", values.len())))
        }
        "array_ndims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            match &args[0] {
                ScalarValue::Array(_) => Ok(ScalarValue::Int(1)),
                _ => Err(EngineError {
                    message: "array_ndims() expects array argument".to_string(),
                }),
            }
        }
        "array_fill" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let lengths = match &args[1] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_fill() expects array of lengths".to_string(),
                    })
                }
            };
            if lengths.len() != 1 {
                return Err(EngineError {
                    message: "array_fill() currently supports one-dimensional arrays".to_string(),
                });
            }
            let length = parse_i64_scalar(&lengths[0], "array_fill() expects integer length")?;
            if length < 0 {
                return Err(EngineError {
                    message: "array_fill() length must be non-negative".to_string(),
                });
            }
            let mut out = Vec::with_capacity(length as usize);
            for _ in 0..length {
                out.push(args[0].clone());
            }
            Ok(ScalarValue::Array(out))
        }
        "array_upper" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_upper() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_upper() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_lower" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_lower() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_lower() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(1))
        }
        "cardinality" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            match &args[0] {
                ScalarValue::Array(values) => Ok(ScalarValue::Int(values.len() as i64)),
                _ => Err(EngineError {
                    message: "cardinality() expects array argument".to_string(),
                }),
            }
        }
        "string_to_array" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let delimiter = if matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Array(vec![ScalarValue::Text(input)]));
            } else { args[1].render() };
            let null_str = args.get(2).and_then(|a| if matches!(a, ScalarValue::Null) { None } else { Some(a.render()) });
            let parts = if delimiter.is_empty() {
                input.chars().map(|c| c.to_string()).collect::<Vec<_>>()
            } else {
                input.split(&delimiter).map(|p| p.to_string()).collect::<Vec<_>>()
            };
            let values = parts.into_iter().map(|part| {
                if null_str.as_deref() == Some(part.as_str()) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Text(part)
                }
            }).collect();
            Ok(ScalarValue::Array(values))
        },
        "array_to_string" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let delimiter = args[1].render();
            let null_replacement = args.get(2).map(|a| a.render());
            let values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                ScalarValue::Text(text) => {
                    let inner = text.trim_start_matches('{').trim_end_matches('}');
                    if inner.is_empty() {
                        return Ok(ScalarValue::Text(String::new()));
                    }
                    inner.split(',').map(|part| {
                        let trimmed = part.trim();
                        if trimmed == "NULL" {
                            ScalarValue::Null
                        } else {
                            ScalarValue::Text(trimmed.to_string())
                        }
                    }).collect::<Vec<_>>()
                }
                _ => {
                    return Err(EngineError {
                        message: "array_to_string() expects array argument".to_string(),
                    })
                }
            };
            let result: Vec<String> = values.iter().filter_map(|value| {
                match value {
                    ScalarValue::Null => null_replacement.clone(),
                    _ => Some(value.render()),
                }
            }).collect();
            Ok(ScalarValue::Text(result.join(&delimiter)))
        },
        _ => Err(EngineError {
            message: format!("unsupported function call {}", fn_name),
        }),
    }
}

fn array_value_matches(target: &ScalarValue, candidate: &ScalarValue) -> Result<bool, EngineError> {
    match (target, candidate) {
        (ScalarValue::Null, ScalarValue::Null) => Ok(true),
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => Ok(false),
        _ => Ok(compare_values_for_predicate(target, candidate)? == Ordering::Equal),
    }
}

fn coerce_to_f64(v: &ScalarValue, context: &str) -> Result<f64, EngineError> {
    match v {
        ScalarValue::Int(i) => Ok(*i as f64),
        ScalarValue::Float(f) => Ok(*f),
        _ => Err(EngineError { message: format!("{} expects numeric argument", context) }),
    }
}

fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

fn initcap_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if capitalize_next {
                result.extend(c.to_uppercase());
                capitalize_next = false;
            } else {
                result.extend(c.to_lowercase());
            }
        } else {
            result.push(c);
            capitalize_next = true;
        }
    }
    result
}

fn pad_string(input: &str, len: usize, fill: &str, left: bool) -> String {
    let input_len = input.chars().count();
    if input_len >= len {
        return input.chars().take(len).collect();
    }
    let pad_len = len - input_len;
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return input.to_string();
    }
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    if left {
        format!("{}{}", padding, input)
    } else {
        format!("{}{}", input, padding)
    }
}

fn md5_hex(input: &str) -> String {
    let digest = md5_digest(input.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn md5_digest(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9,
        14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
        4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    let mut msg = input.to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            let start = i * 4;
            *word = u32::from_le_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }

        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            let rotate = a
                .wrapping_add(f)
                .wrapping_add(K[i])
                .wrapping_add(m[g]);
            b = b.wrapping_add(rotate.rotate_left(S[i]));
            a = temp;
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

fn encode_bytes(input: &[u8], format: &str) -> Result<String, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => Ok(input.iter().map(|b| format!("{:02x}", b)).collect()),
        "base64" => {
            use base64::Engine;
            Ok(base64::engine::general_purpose::STANDARD.encode(input))
        }
        "escape" => Ok(escape_bytes(input)),
        _ => Err(EngineError {
            message: format!("encode() unsupported format {}", format),
        }),
    }
}

fn decode_bytes(input: &str, format: &str) -> Result<Vec<u8>, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => decode_hex_bytes(input),
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(input.trim())
                .map_err(|_| EngineError {
                    message: "decode() invalid base64 input".to_string(),
                })
        }
        "escape" => decode_escape_bytes(input),
        _ => Err(EngineError {
            message: format!("decode() unsupported format {}", format),
        }),
    }
}

fn escape_bytes(input: &[u8]) -> String {
    let mut out = String::new();
    for &byte in input {
        if byte == b'\\' {
            out.push_str("\\\\");
        } else if (0x20..=0x7e).contains(&byte) {
            out.push(byte as char);
        } else {
            out.push('\\');
            out.push_str(&format!("{:03o}", byte));
        }
    }
    out
}

fn decode_hex_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let trimmed = input.trim();
    let hex = trimmed.strip_prefix("\\x").or_else(|| trimmed.strip_prefix("\\X")).unwrap_or(trimmed);
    if !hex.len().is_multiple_of(2) {
        return Err(EngineError {
            message: "decode() invalid hex input length".to_string(),
        });
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for idx in (0..bytes.len()).step_by(2) {
        let part = std::str::from_utf8(&bytes[idx..idx + 2]).unwrap_or("");
        let value = u8::from_str_radix(part, 16).map_err(|_| EngineError {
            message: "decode() invalid hex input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

fn decode_escape_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch as u8);
            continue;
        }
        let Some(next) = chars.peek().copied() else {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        };
        if next == '\\' {
            chars.next();
            out.push(b'\\');
            continue;
        }
        let mut octal = String::new();
        for _ in 0..3 {
            if let Some(digit) = chars.peek().copied()
                && digit.is_ascii_digit() && digit <= '7'
            {
                octal.push(digit);
                chars.next();
            }
        }
        if octal.is_empty() {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        }
        let value = u8::from_str_radix(&octal, 8).map_err(|_| EngineError {
            message: "decode() invalid escape input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

fn build_regex(pattern: &str, flags: &str, fn_name: &str) -> Result<regex::Regex, EngineError> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'g' => {}
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() unsupported regex flag {}", flag),
                });
            }
        }
    }
    builder.build().map_err(|err| EngineError {
        message: format!("{fn_name}() invalid regex: {err}"),
    })
}

fn text_array_from_options(items: &[Option<String>]) -> String {
    let rendered = items
        .iter()
        .map(|item| item.clone().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>();
    format!("{{{}}}", rendered.join(","))
}

fn eval_regexp_match(text: &str, pattern: &str, flags: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, flags, "regexp_match")?;
    let Some(caps) = regex.captures(text) else {
        return Ok(ScalarValue::Null);
    };
    Ok(ScalarValue::Text(regex_captures_to_array(&caps)))
}

fn regex_captures_to_array(caps: &regex::Captures<'_>) -> String {
    let mut items = Vec::new();
    if caps.len() > 1 {
        for idx in 1..caps.len() {
            items.push(caps.get(idx).map(|m| m.as_str().to_string()));
        }
    } else {
        items.push(caps.get(0).map(|m| m.as_str().to_string()));
    }
    text_array_from_options(&items)
}

fn eval_regexp_matches_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 && args.len() != 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().take(2).any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() == 3 { args[2].render() } else { String::new() };
    let global = flags.contains('g');
    let regex = build_regex(&pattern, &flags, fn_name)?;
    let mut rows = Vec::new();
    if global {
        for caps in regex.captures_iter(&text) {
            rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
        }
    } else if let Some(caps) = regex.captures(&text) {
        rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
    }
    Ok((vec![fn_name.to_string()], rows))
}

fn eval_regexp_split_to_array(text: &str, pattern: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, "", "regexp_split_to_array")?;
    let parts = regex
        .split(text)
        .map(|part| Some(part.to_string()))
        .collect::<Vec<_>>();
    Ok(ScalarValue::Text(text_array_from_options(&parts)))
}

fn eval_regexp_split_to_table_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 arguments"),
        });
    }
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let regex = build_regex(&pattern, "", fn_name)?;
    let rows = regex
        .split(&text)
        .map(|part| vec![ScalarValue::Text(part.to_string())])
        .collect();
    Ok((vec![fn_name.to_string()], rows))
}

fn rand_f64() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (seed as f64) / (u32::MAX as f64)
}

fn eval_regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
    let global = flags.contains('g');
    let regex = build_regex(pattern, flags, "regexp_replace")?;
    let out = if global {
        regex.replace_all(source, replacement).to_string()
    } else {
        regex.replace(source, replacement).to_string()
    };
    Ok(ScalarValue::Text(out))
}

fn eval_extremum(args: &[ScalarValue], greatest: bool) -> Result<ScalarValue, EngineError> {
    let mut best: Option<ScalarValue> = None;
    for arg in args {
        if matches!(arg, ScalarValue::Null) {
            continue;
        }
        match &best {
            None => best = Some(arg.clone()),
            Some(current) => {
                let cmp = compare_values_for_predicate(arg, current)?;
                let should_take = if greatest {
                    cmp == Ordering::Greater
                } else {
                    cmp == Ordering::Less
                };
                if should_take {
                    best = Some(arg.clone());
                }
            }
        }
    }
    Ok(best.unwrap_or(ScalarValue::Null))
}

fn scalar_to_json_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Null => Ok(JsonValue::Null),
        ScalarValue::Bool(v) => Ok(JsonValue::Bool(*v)),
        ScalarValue::Int(v) => Ok(JsonValue::Number(JsonNumber::from(*v))),
        ScalarValue::Float(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .ok_or_else(|| EngineError {
                message: "cannot convert non-finite float to JSON value".to_string(),
            }),
        ScalarValue::Text(v) => Ok(JsonValue::String(v.clone())),
        ScalarValue::Array(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(scalar_to_json_value(value)?);
            }
            Ok(JsonValue::Array(items))
        }
    }
}

fn json_build_object_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut object = JsonMap::new();
    for idx in (0..args.len()).step_by(2) {
        let key = match &args[idx] {
            ScalarValue::Null => {
                return Err(EngineError {
                    message: "json_build_object() key cannot be null".to_string(),
                });
            }
            other => other.render(),
        };
        let value = scalar_to_json_value(&args[idx + 1])?;
        object.insert(key, value);
    }
    Ok(JsonValue::Object(object))
}

fn json_build_array_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut items = Vec::with_capacity(args.len());
    for arg in args {
        items.push(scalar_to_json_value(arg)?);
    }
    Ok(JsonValue::Array(items))
}

fn parse_json_or_scalar_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => serde_json::from_str::<JsonValue>(text)
            .or_else(|_| scalar_to_json_value(value))
            .map_err(|err| EngineError {
                message: format!("cannot convert value to JSON: {err}"),
            }),
        _ => scalar_to_json_value(value),
    }
}

fn parse_json_constructor_pretty_arg(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<bool>, EngineError> {
    if args.len() < 2 {
        return Ok(Some(false));
    }
    if matches!(args[1], ScalarValue::Null) {
        return Ok(None);
    }
    parse_bool_scalar(
        &args[1],
        &format!("{fn_name}() expects boolean pretty argument"),
    )
    .map(Some)
}

fn maybe_pretty_json(value: &JsonValue, pretty: bool) -> Result<String, EngineError> {
    if pretty {
        serde_json::to_string_pretty(value).map_err(|err| EngineError {
            message: format!("failed to pretty-print JSON: {err}"),
        })
    } else {
        Ok(value.to_string())
    }
}

fn eval_row_to_json(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };

    let input = parse_json_or_scalar_value(&args[0])?;
    let object = match input {
        JsonValue::Object(map) => JsonValue::Object(map),
        JsonValue::Array(items) => {
            let mut map = JsonMap::new();
            for (idx, value) in items.into_iter().enumerate() {
                map.insert(format!("f{}", idx + 1), value);
            }
            JsonValue::Object(map)
        }
        scalar => {
            let mut map = JsonMap::new();
            map.insert("f1".to_string(), scalar);
            JsonValue::Object(map)
        }
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&object, pretty)?))
}

fn eval_array_to_json(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let parsed = parse_json_or_scalar_value(&args[0])?;
    let JsonValue::Array(_) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&parsed, pretty)?))
}

fn json_value_text_token(
    value: &JsonValue,
    fn_name: &str,
    key_mode: bool,
) -> Result<String, EngineError> {
    match value {
        JsonValue::Null if key_mode => Err(EngineError {
            message: format!("{fn_name}() key cannot be null"),
        }),
        JsonValue::Null => Ok("null".to_string()),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(v) => Ok(v.to_string()),
        JsonValue::Number(v) => Ok(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_object_pairs_from_array(
    value: &JsonValue,
    fn_name: &str,
) -> Result<Vec<(String, String)>, EngineError> {
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument must be a JSON array"),
        });
    };

    if items
        .iter()
        .all(|item| matches!(item, JsonValue::Array(inner) if inner.len() == 2))
    {
        let mut pairs = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Array(inner) = item else {
                continue;
            };
            let key = json_value_text_token(&inner[0], fn_name, true)?;
            let value = json_value_text_token(&inner[1], fn_name, false)?;
            pairs.push((key, value));
        }
        return Ok(pairs);
    }

    if items.len() % 2 != 0 {
        return Err(EngineError {
            message: format!("{fn_name}() requires an even-length text array"),
        });
    }

    let mut pairs = Vec::with_capacity(items.len() / 2);
    for idx in (0..items.len()).step_by(2) {
        let key = json_value_text_token(&items[idx], fn_name, true)?;
        let value = json_value_text_token(&items[idx + 1], fn_name, false)?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

fn eval_json_object(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let pairs = match args.len() {
        1 => {
            let source = parse_json_document_arg(&args[0], fn_name, 1)?;
            parse_json_object_pairs_from_array(&source, fn_name)?
        }
        2 => {
            let keys = parse_json_document_arg(&args[0], fn_name, 1)?;
            let values = parse_json_document_arg(&args[1], fn_name, 2)?;
            let JsonValue::Array(key_items) = keys else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 1 must be a JSON array"),
                });
            };
            let JsonValue::Array(value_items) = values else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 2 must be a JSON array"),
                });
            };
            if key_items.len() != value_items.len() {
                return Err(EngineError {
                    message: format!("{fn_name}() key/value array lengths must match"),
                });
            }
            key_items
                .iter()
                .zip(value_items.iter())
                .map(|(key, value)| {
                    Ok((
                        json_value_text_token(key, fn_name, true)?,
                        json_value_text_token(value, fn_name, false)?,
                    ))
                })
                .collect::<Result<Vec<_>, EngineError>>()?
        }
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}() expects one or two arguments"),
            });
        }
    };

    let mut object = JsonMap::new();
    for (key, value) in pairs {
        object.insert(key, JsonValue::String(value));
    }
    Ok(ScalarValue::Text(JsonValue::Object(object).to_string()))
}

fn parse_json_document_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSON text"),
        });
    };
    serde_json::from_str::<JsonValue>(text).map_err(|err| EngineError {
        message: format!("{fn_name}() argument {arg_index} is not valid JSON: {err}"),
    })
}

fn parse_json_path_segments(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<Vec<String>>, EngineError> {
    let mut out = Vec::with_capacity(args.len().saturating_sub(1));
    for path_arg in &args[1..] {
        match path_arg {
            ScalarValue::Null => return Ok(None),
            ScalarValue::Text(text) => out.push(text.clone()),
            ScalarValue::Int(v) => out.push(v.to_string()),
            ScalarValue::Float(v) => out.push(v.to_string()),
            ScalarValue::Bool(v) => out.push(v.to_string()),
            ScalarValue::Array(_) => {
                return Err(EngineError {
                    message: format!("{fn_name}() path arguments must be scalar values"),
                });
            }
        }
    }
    if out.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires at least one path argument"),
        });
    }
    Ok(Some(out))
}

fn extract_json_path_value<'a>(root: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = root;
    for segment in path {
        current = match current {
            JsonValue::Object(map) => map.get(segment)?,
            JsonValue::Array(array) => {
                let idx = segment.parse::<usize>().ok()?;
                array.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn json_value_text_output(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::String(text) => ScalarValue::Text(text.clone()),
        JsonValue::Bool(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Number(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

fn eval_json_extract_path(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&args[0], fn_name, 1)?;
    let Some(path) = parse_json_path_segments(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

fn eval_json_array_length(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let JsonValue::Array(items) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Int(items.len() as i64))
}

fn eval_json_typeof(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let ty = match parsed {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    };
    Ok(ScalarValue::Text(ty.to_string()))
}

fn strip_null_object_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if let Some(inner) = map.get_mut(&key) {
                    strip_null_object_fields(inner);
                }
                if map.get(&key).is_some_and(|candidate| candidate.is_null()) {
                    map.remove(&key);
                }
            }
        }
        JsonValue::Array(array) => {
            for item in array {
                strip_null_object_fields(item);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn eval_json_strip_nulls(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut parsed = parse_json_document_arg(value, fn_name, 1)?;
    strip_null_object_fields(&mut parsed);
    Ok(ScalarValue::Text(parsed.to_string()))
}

fn eval_json_pretty(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let pretty = serde_json::to_string_pretty(&parsed).map_err(|err| EngineError {
        message: format!("{fn_name}() failed to pretty-print JSON: {err}"),
    })?;
    Ok(ScalarValue::Text(pretty))
}

fn eval_jsonb_exists(target: &ScalarValue, key: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(key, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, "jsonb_exists", 1)?;
    let key = scalar_to_json_path_segment(key, "jsonb_exists")?;
    Ok(ScalarValue::Bool(json_has_key(&parsed, &key)))
}

fn eval_jsonb_exists_any_all(
    target: &ScalarValue,
    keys: &ScalarValue,
    any_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(keys, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, fn_name, 1)?;
    let keys = parse_json_path_operand(keys, fn_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() key array cannot be empty"),
        });
    }
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&parsed, key))
    } else {
        keys.iter().all(|key| json_has_key(&parsed, key))
    };
    Ok(ScalarValue::Bool(matched))
}

fn parse_json_path_text_array(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);
    if inner.trim().is_empty() {
        return Vec::new();
    }
    inner
        .split(',')
        .map(str::trim)
        .map(|segment| {
            segment
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .unwrap_or(segment)
                .to_string()
        })
        .collect()
}

fn parse_json_new_value_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => match serde_json::from_str::<JsonValue>(text) {
            Ok(parsed) => Ok(parsed),
            Err(_) => Ok(JsonValue::String(text.clone())),
        },
        _ => scalar_to_json_value(value).map_err(|err| EngineError {
            message: format!(
                "{fn_name}() argument {arg_index} is invalid: {}",
                err.message
            ),
        }),
    }
}

fn json_container_for_next_segment(next: Option<&str>) -> JsonValue {
    if next.is_some_and(|segment| segment.parse::<usize>().is_ok()) {
        JsonValue::Array(Vec::new())
    } else {
        JsonValue::Object(JsonMap::new())
    }
}

fn json_set_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    create_missing: bool,
) -> bool {
    if path.is_empty() {
        *root = new_value;
        return true;
    }

    let mut current = root;
    for idx in 0..path.len() {
        let segment = &path[idx];
        let is_last = idx + 1 == path.len();
        let next_segment = path.get(idx + 1).map(String::as_str);

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(segment) || create_missing {
                        map.insert(segment.clone(), new_value);
                        return true;
                    }
                    return false;
                }
                if !map.contains_key(segment) {
                    if !create_missing {
                        return false;
                    }
                    map.insert(
                        segment.clone(),
                        json_container_for_next_segment(next_segment),
                    );
                }
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                current = next;
            }
            JsonValue::Array(array) => {
                let Ok(index) = segment.parse::<usize>() else {
                    return false;
                };
                if is_last {
                    if index < array.len() {
                        array[index] = new_value;
                        return true;
                    }
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                    array[index] = new_value;
                    return true;
                }
                if index >= array.len() {
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                }
                if array[index].is_null() {
                    array[index] = json_container_for_next_segment(next_segment);
                }
                current = &mut array[index];
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        }
    }
    false
}

fn json_insert_array_index(len: usize, segment: &str, insert_after: bool) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    let mut pos = if index >= 0 {
        index as isize
    } else {
        len as isize + index as isize
    };
    if insert_after {
        pos += 1;
    }
    if pos < 0 {
        pos = 0;
    }
    if pos > len as isize {
        pos = len as isize;
    }
    Some(pos as usize)
}

fn json_insert_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    insert_after: bool,
) -> bool {
    if path.is_empty() {
        return false;
    }

    let mut current = root;
    for segment in &path[..path.len() - 1] {
        current = match current {
            JsonValue::Object(map) => {
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                next
            }
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), segment) else {
                    return false;
                };
                &mut array[idx]
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        };
    }

    let last = &path[path.len() - 1];
    match current {
        JsonValue::Object(map) => {
            if map.contains_key(last) {
                false
            } else {
                map.insert(last.clone(), new_value);
                true
            }
        }
        JsonValue::Array(array) => {
            let Some(pos) = json_insert_array_index(array.len(), last, insert_after) else {
                return false;
            };
            array.insert(pos, new_value);
            true
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

fn eval_jsonb_set(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set", 3)?;
    let create_missing = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_set() expects boolean create_missing argument",
        )?
    } else {
        true
    };
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

fn eval_jsonb_insert(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_insert", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_insert() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_insert", 3)?;
    let insert_after = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_insert() expects boolean insert_after argument",
        )?
    } else {
        false
    };
    let _ = json_insert_path(&mut target, &path, new_value, insert_after);
    Ok(ScalarValue::Text(target.to_string()))
}

fn eval_jsonb_set_lax(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set_lax", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set_lax() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let create_missing = if args.len() >= 4 {
        if matches!(args[3], ScalarValue::Null) {
            return Ok(ScalarValue::Null);
        }
        parse_bool_scalar(
            &args[3],
            "jsonb_set_lax() expects boolean create_if_missing argument",
        )?
    } else {
        true
    };

    if matches!(args[2], ScalarValue::Null) {
        let treatment = if args.len() >= 5 {
            if matches!(args[4], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            args[4].render().trim().to_ascii_lowercase()
        } else {
            "use_json_null".to_string()
        };
        match treatment.as_str() {
            "raise_exception" => {
                return Err(EngineError {
                    message: "jsonb_set_lax() null_value_treatment requested exception".to_string(),
                });
            }
            "use_json_null" => {
                let _ = json_set_path(&mut target, &path, JsonValue::Null, create_missing);
            }
            "delete_key" => {
                if !path.is_empty() {
                    let _ = json_remove_path(&mut target, &path);
                }
            }
            "return_target" => {}
            _ => {
                return Err(EngineError {
                    message: format!("jsonb_set_lax() unknown null_value_treatment {}", treatment),
                });
            }
        }
        return Ok(ScalarValue::Text(target.to_string()));
    }

    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set_lax", 3)?;
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

fn extract_json_get_value<'a>(target: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    match target {
        JsonValue::Object(map) => map.get(path),
        JsonValue::Array(array) => {
            let idx = path.parse::<i64>().ok()?;
            if idx >= 0 {
                array.get(idx as usize)
            } else {
                let back = idx.unsigned_abs() as usize;
                if back > array.len() {
                    None
                } else {
                    array.get(array.len().saturating_sub(back))
                }
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => None,
    }
}

fn scalar_to_json_path_segment(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<String, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator does not accept NULL path/key operand"),
        }),
        ScalarValue::Text(text) => Ok(text.clone()),
        ScalarValue::Int(v) => Ok(v.to_string()),
        ScalarValue::Float(v) => Ok(v.to_string()),
        ScalarValue::Bool(v) => Ok(v.to_string()),
        ScalarValue::Array(_) => Err(EngineError {
            message: format!("{operator_name} operator path operand must be scalar"),
        }),
    }
}

fn parse_json_path_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator path operand cannot be NULL"),
        }),
        ScalarValue::Text(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('[') {
                let parsed =
                    serde_json::from_str::<JsonValue>(trimmed).map_err(|err| EngineError {
                        message: format!(
                            "{operator_name} operator path operand must be text[]/json array: {err}"
                        ),
                    })?;
                let JsonValue::Array(items) = parsed else {
                    return Err(EngineError {
                        message: format!("{operator_name} operator path operand must be array"),
                    });
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::String(s) => out.push(s),
                        JsonValue::Number(n) => out.push(n.to_string()),
                        JsonValue::Bool(v) => out.push(v.to_string()),
                        JsonValue::Null => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array cannot contain null"
                                ),
                            });
                        }
                        JsonValue::Array(_) | JsonValue::Object(_) => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array entries must be scalar"
                                ),
                            });
                        }
                    }
                }
                return Ok(out);
            }
            Ok(parse_json_path_text_array(trimmed))
        }
        ScalarValue::Int(v) => Ok(vec![v.to_string()]),
        ScalarValue::Float(v) => Ok(vec![v.to_string()]),
        ScalarValue::Bool(v) => Ok(vec![v.to_string()]),
        ScalarValue::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if matches!(value, ScalarValue::Null) {
                    return Err(EngineError {
                        message: format!(
                            "{operator_name} operator path array cannot contain null"
                        ),
                    });
                }
                out.push(value.render());
            }
            Ok(out)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathStep {
    Key(String),
    Index(i64),
    Wildcard,
    Filter(JsonPathFilterExpr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonPathFilterOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterOperand {
    Current,
    CurrentPath(Vec<JsonPathStep>),
    RootPath(Vec<JsonPathStep>),
    Variable(String),
    Literal(JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterExpr {
    Exists(JsonPathFilterOperand),
    Compare {
        left: JsonPathFilterOperand,
        op: JsonPathFilterOp,
        right: JsonPathFilterOperand,
    },
    Truthy(JsonPathFilterOperand),
}

fn parse_jsonpath_text_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<String, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSONPath text"),
        });
    };
    Ok(text.clone())
}

fn parse_jsonpath_vars_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonMap<String, JsonValue>, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(JsonMap::new());
    }
    let parsed = parse_json_document_arg(value, fn_name, arg_index)?;
    let JsonValue::Object(vars) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be a JSON object"),
        });
    };
    Ok(vars)
}

fn parse_jsonpath_silent_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<bool, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(false);
    }
    parse_bool_scalar(
        value,
        &format!("{fn_name}() argument {arg_index} must be boolean"),
    )
}

fn strip_outer_parentheses(text: &str) -> &str {
    let mut trimmed = text.trim();
    loop {
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let mut depth = 0isize;
        let mut in_quote: Option<char> = None;
        let mut encloses = true;
        for (idx, ch) in trimmed.char_indices() {
            if let Some(quote) = in_quote {
                if ch == quote {
                    in_quote = None;
                } else if ch == '\\' {
                    continue;
                }
                continue;
            }
            if ch == '\'' || ch == '"' {
                in_quote = Some(ch);
                continue;
            }
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    encloses = false;
                    break;
                }
            }
        }
        if encloses {
            trimmed = trimmed[1..trimmed.len() - 1].trim();
        } else {
            return trimmed;
        }
    }
}

fn find_jsonpath_compare_operator(expr: &str) -> Option<(usize, &'static str, JsonPathFilterOp)> {
    let bytes = expr.as_bytes();
    let mut idx = 0usize;
    let mut in_quote: Option<u8> = None;
    let mut paren_depth = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if let Some(quote) = in_quote {
            if b == quote {
                in_quote = None;
            } else if b == b'\\' {
                idx += 1;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' | b'"' => {
                in_quote = Some(b);
                idx += 1;
                continue;
            }
            b'(' => {
                paren_depth += 1;
                idx += 1;
                continue;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }
        if paren_depth == 0 {
            for (token, op) in [
                ("==", JsonPathFilterOp::Eq),
                ("!=", JsonPathFilterOp::NotEq),
                (">=", JsonPathFilterOp::Gte),
                ("<=", JsonPathFilterOp::Lte),
                (">", JsonPathFilterOp::Gt),
                ("<", JsonPathFilterOp::Lt),
            ] {
                if expr[idx..].starts_with(token) {
                    return Some((idx, token, op));
                }
            }
        }
        idx += 1;
    }
    None
}

fn parse_jsonpath_filter_operand(
    token: &str,
    context: &str,
) -> Result<JsonPathFilterOperand, EngineError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(EngineError {
            message: format!("{context} JSONPath filter operand is empty"),
        });
    }

    if token == "@" {
        return Ok(JsonPathFilterOperand::Current);
    }
    if token.starts_with("@.") || token.starts_with("@[") {
        let rooted = format!("${}", &token[1..]);
        let steps = parse_jsonpath_steps(&rooted, context)?;
        return Ok(JsonPathFilterOperand::CurrentPath(steps));
    }
    if token == "$" {
        return Ok(JsonPathFilterOperand::RootPath(Vec::new()));
    }
    if token.starts_with("$.") || token.starts_with("$[") {
        let steps = parse_jsonpath_steps(token, context)?;
        return Ok(JsonPathFilterOperand::RootPath(steps));
    }
    if let Some(name) = token.strip_prefix('$')
        && !name.is_empty()
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Ok(JsonPathFilterOperand::Variable(name.to_string()));
    }

    if (token.starts_with('"') && token.ends_with('"'))
        || (token.starts_with('\'') && token.ends_with('\''))
    {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::String(
            token[1..token.len() - 1].to_string(),
        )));
    }
    if token.eq_ignore_ascii_case("true") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(true)));
    }
    if token.eq_ignore_ascii_case("false") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(false)));
    }
    if token.eq_ignore_ascii_case("null") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Null));
    }
    if let Ok(number) = serde_json::from_str::<JsonValue>(token)
        && matches!(number, JsonValue::Number(_))
    {
        return Ok(JsonPathFilterOperand::Literal(number));
    }

    Err(EngineError {
        message: format!("{context} unsupported JSONPath filter operand {token}"),
    })
}

fn parse_jsonpath_filter_expr(
    text: &str,
    context: &str,
) -> Result<JsonPathFilterExpr, EngineError> {
    let trimmed = strip_outer_parentheses(text).trim();
    if trimmed.is_empty() {
        return Err(EngineError {
            message: format!("{context} empty JSONPath filter expression"),
        });
    }

    if trimmed.len() > 7
        && trimmed[..6].eq_ignore_ascii_case("exists")
        && trimmed[6..].trim_start().starts_with('(')
    {
        let rest = trimmed[6..].trim_start();
        if let Some(inner) = rest
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        {
            let operand = parse_jsonpath_filter_operand(inner, context)?;
            return Ok(JsonPathFilterExpr::Exists(operand));
        }
    }

    if let Some((idx, token, op)) = find_jsonpath_compare_operator(trimmed) {
        let left = parse_jsonpath_filter_operand(&trimmed[..idx], context)?;
        let right = parse_jsonpath_filter_operand(&trimmed[idx + token.len()..], context)?;
        return Ok(JsonPathFilterExpr::Compare { left, op, right });
    }

    Ok(JsonPathFilterExpr::Truthy(parse_jsonpath_filter_operand(
        trimmed, context,
    )?))
}

fn parse_jsonpath_steps(path: &str, context: &str) -> Result<Vec<JsonPathStep>, EngineError> {
    let bytes = path.trim().as_bytes();
    if bytes.is_empty() || bytes[0] != b'$' {
        return Err(EngineError {
            message: format!("{context} JSONPath must start with '$'"),
        });
    }

    let mut steps = Vec::new();
    let mut idx = 1usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b' ' | b'\t' | b'\r' | b'\n' => idx += 1,
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath"),
                    });
                }
                if bytes[idx] == b'*' {
                    steps.push(JsonPathStep::Wildcard);
                    idx += 1;
                    continue;
                }
                if bytes[idx] == b'"' {
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'"' {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath key"),
                    });
                }
                let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                steps.push(JsonPathStep::Key(key.to_string()));
            }
            b'[' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                if bytes[idx] == b'*' {
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath wildcard index"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Wildcard);
                    continue;
                }
                if bytes[idx] == b'\'' || bytes[idx] == b'"' {
                    let quote = bytes[idx];
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != quote {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath bracket expression"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b']' {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                let token = std::str::from_utf8(&bytes[start..idx])
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                idx += 1;
                if token.is_empty() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath index"),
                    });
                }
                if let Ok(index) = token.parse::<i64>() {
                    steps.push(JsonPathStep::Index(index));
                } else {
                    steps.push(JsonPathStep::Key(token));
                }
            }
            b'?' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() || bytes[idx] != b'(' {
                    return Err(EngineError {
                        message: format!("{context} expected '(' after JSONPath filter marker"),
                    });
                }
                idx += 1;
                let start = idx;
                let mut depth = 1usize;
                let mut in_quote: Option<u8> = None;
                while idx < bytes.len() {
                    let b = bytes[idx];
                    if let Some(quote) = in_quote {
                        if b == quote {
                            in_quote = None;
                        } else if b == b'\\' {
                            idx += 1;
                        }
                        idx += 1;
                        continue;
                    }
                    if b == b'\'' || b == b'"' {
                        in_quote = Some(b);
                        idx += 1;
                        continue;
                    }
                    if b == b'(' {
                        depth += 1;
                        idx += 1;
                        continue;
                    }
                    if b == b')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        idx += 1;
                        continue;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() || depth != 0 {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath filter expression"),
                    });
                }
                let expr_text = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                idx += 1;
                steps.push(JsonPathStep::Filter(parse_jsonpath_filter_expr(
                    expr_text, context,
                )?));
            }
            _ => {
                return Err(EngineError {
                    message: format!("{context} invalid JSONPath near byte {}", idx),
                });
            }
        }
    }
    Ok(steps)
}

fn jsonpath_query_values(
    root: &JsonValue,
    steps: &[JsonPathStep],
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    let mut current = vec![root.clone()];
    for step in steps {
        let mut next = Vec::new();
        for value in current {
            match step {
                JsonPathStep::Key(key) => {
                    if let JsonValue::Object(map) = value
                        && let Some(found) = map.get(key)
                    {
                        next.push(found.clone());
                    }
                }
                JsonPathStep::Index(index) => {
                    if let JsonValue::Array(items) = value
                        && let Some(idx) = json_array_index_from_i64(items.len(), *index)
                    {
                        next.push(items[idx].clone());
                    }
                }
                JsonPathStep::Wildcard => match value {
                    JsonValue::Array(items) => next.extend(items),
                    JsonValue::Object(map) => next.extend(map.values().cloned()),
                    JsonValue::Null
                    | JsonValue::Bool(_)
                    | JsonValue::Number(_)
                    | JsonValue::String(_) => {}
                },
                JsonPathStep::Filter(expr) => {
                    if eval_jsonpath_filter_expr(
                        expr,
                        &value,
                        absolute_root,
                        vars,
                        context,
                        silent,
                    )? {
                        next.push(value);
                    }
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }
    Ok(current)
}

fn jsonpath_value_truthy(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => false,
        JsonValue::Bool(v) => *v,
        JsonValue::Number(v) => v.as_f64().is_some_and(|n| n != 0.0),
        JsonValue::String(v) => !v.is_empty(),
        JsonValue::Array(v) => !v.is_empty(),
        JsonValue::Object(v) => !v.is_empty(),
    }
}

fn json_values_compare(left: &JsonValue, right: &JsonValue) -> Result<Ordering, EngineError> {
    match (left, right) {
        (JsonValue::Array(_), JsonValue::Array(_))
        | (JsonValue::Object(_), JsonValue::Object(_)) => {
            if left == right {
                Ok(Ordering::Equal)
            } else {
                Ok(left.to_string().cmp(&right.to_string()))
            }
        }
        _ => {
            compare_values_for_predicate(&json_value_to_scalar(left), &json_value_to_scalar(right))
        }
    }
}

fn eval_jsonpath_filter_operand(
    operand: &JsonPathFilterOperand,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    match operand {
        JsonPathFilterOperand::Current => Ok(vec![current.clone()]),
        JsonPathFilterOperand::CurrentPath(steps) => {
            jsonpath_query_values(current, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::RootPath(steps) => {
            jsonpath_query_values(absolute_root, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::Variable(name) => {
            if let Some(value) = vars.get(name) {
                Ok(vec![value.clone()])
            } else if silent {
                Ok(Vec::new())
            } else {
                Err(EngineError {
                    message: format!("{context} JSONPath variable ${name} is not provided"),
                })
            }
        }
        JsonPathFilterOperand::Literal(value) => Ok(vec![value.clone()]),
    }
}

fn eval_jsonpath_filter_expr(
    expr: &JsonPathFilterExpr,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<bool, EngineError> {
    match expr {
        JsonPathFilterExpr::Exists(operand) => Ok(!eval_jsonpath_filter_operand(
            operand,
            current,
            absolute_root,
            vars,
            context,
            silent,
        )?
        .is_empty()),
        JsonPathFilterExpr::Truthy(operand) => {
            let values = eval_jsonpath_filter_operand(
                operand,
                current,
                absolute_root,
                vars,
                context,
                silent,
            )?;
            Ok(values.iter().any(jsonpath_value_truthy))
        }
        JsonPathFilterExpr::Compare { left, op, right } => {
            let left_values =
                eval_jsonpath_filter_operand(left, current, absolute_root, vars, context, silent)?;
            let right_values =
                eval_jsonpath_filter_operand(right, current, absolute_root, vars, context, silent)?;
            for left in &left_values {
                for right in &right_values {
                    let ordering = json_values_compare(left, right)?;
                    let matched = match op {
                        JsonPathFilterOp::Eq => ordering == Ordering::Equal,
                        JsonPathFilterOp::NotEq => ordering != Ordering::Equal,
                        JsonPathFilterOp::Gt => ordering == Ordering::Greater,
                        JsonPathFilterOp::Gte => {
                            matches!(ordering, Ordering::Greater | Ordering::Equal)
                        }
                        JsonPathFilterOp::Lt => ordering == Ordering::Less,
                        JsonPathFilterOp::Lte => {
                            matches!(ordering, Ordering::Less | Ordering::Equal)
                        }
                    };
                    if matched {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }
}

fn eval_json_path_predicate_operator(
    left: ScalarValue,
    right: ScalarValue,
    match_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(
        &left,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        1,
    )?;
    let path_text = parse_jsonpath_text_arg(
        &right,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        2,
    )?;
    let steps = parse_jsonpath_steps(
        &path_text,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
    )?;
    let vars = JsonMap::new();
    let values = jsonpath_query_values(
        &target,
        &steps,
        &target,
        &vars,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        false,
    )?;
    if match_mode {
        if let Some(first) = values.first() {
            Ok(ScalarValue::Bool(jsonpath_value_truthy(first)))
        } else {
            Ok(ScalarValue::Bool(false))
        }
    } else {
        Ok(ScalarValue::Bool(!values.is_empty()))
    }
}

fn jsonb_path_query_values(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Vec<JsonValue>, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(Vec::new());
    }
    let silent = if args.len() >= 4 {
        parse_jsonpath_silent_arg(&args[3], fn_name, 4)?
    } else {
        false
    };
    let evaluate = || -> Result<Vec<JsonValue>, EngineError> {
        let target = parse_json_document_arg(&args[0], fn_name, 1)?;
        let path_text = parse_jsonpath_text_arg(&args[1], fn_name, 2)?;
        let vars = if args.len() >= 3 {
            parse_jsonpath_vars_arg(&args[2], fn_name, 3)?
        } else {
            JsonMap::new()
        };
        let steps = parse_jsonpath_steps(&path_text, fn_name)?;
        jsonpath_query_values(&target, &steps, &target, &vars, fn_name, silent)
    };
    if silent {
        evaluate().or(Ok(Vec::new()))
    } else {
        evaluate()
    }
}

fn eval_jsonb_path_exists(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Bool(!values.is_empty()))
}

fn eval_jsonb_path_match(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        match first {
            JsonValue::Null => Ok(ScalarValue::Null),
            JsonValue::Bool(v) => Ok(ScalarValue::Bool(*v)),
            _ => Ok(ScalarValue::Bool(jsonpath_value_truthy(first))),
        }
    } else {
        Ok(ScalarValue::Bool(false))
    }
}

fn eval_jsonb_path_query_array(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Text(JsonValue::Array(values).to_string()))
}

fn eval_jsonb_path_query_first(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        Ok(ScalarValue::Text(first.to_string()))
    } else {
        Ok(ScalarValue::Null)
    }
}

fn eval_json_get_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ->/->>", 1)?;
    let path_segment = scalar_to_json_path_segment(&right, "->")?;
    let Some(found) = extract_json_get_value(&target, &path_segment) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

fn eval_json_path_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator #>/#>>", 1)?;
    let path = parse_json_path_operand(&right, "#>")?;
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

fn json_concat(lhs: JsonValue, rhs: JsonValue) -> JsonValue {
    match (lhs, rhs) {
        (JsonValue::Object(mut left), JsonValue::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            JsonValue::Object(left)
        }
        (JsonValue::Array(mut left), JsonValue::Array(right)) => {
            left.extend(right);
            JsonValue::Array(left)
        }
        (JsonValue::Array(mut left), right) => {
            left.push(right);
            JsonValue::Array(left)
        }
        (left, JsonValue::Array(right)) => {
            let mut out = Vec::with_capacity(right.len() + 1);
            out.push(left);
            out.extend(right);
            JsonValue::Array(out)
        }
        (left, right) => JsonValue::Array(vec![left, right]),
    }
}

fn eval_json_concat_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match (left, right) {
        (ScalarValue::Array(mut left_items), ScalarValue::Array(right_items)) => {
            left_items.extend(right_items);
            Ok(ScalarValue::Array(left_items))
        }
        (ScalarValue::Array(mut left_items), other) => {
            left_items.push(other);
            Ok(ScalarValue::Array(left_items))
        }
        (other, ScalarValue::Array(mut right_items)) => {
            right_items.insert(0, other);
            Ok(ScalarValue::Array(right_items))
        }
        (left, right) => {
            let lhs = parse_json_document_arg(&left, "json operator ||", 1)?;
            let rhs = parse_json_document_arg(&right, "json operator ||", 2)?;
            Ok(ScalarValue::Text(json_concat(lhs, rhs).to_string()))
        }
    }
}

fn json_contains(lhs: &JsonValue, rhs: &JsonValue) -> bool {
    match (lhs, rhs) {
        (JsonValue::Object(lmap), JsonValue::Object(rmap)) => rmap.iter().all(|(key, rvalue)| {
            lmap.get(key)
                .is_some_and(|lvalue| json_contains(lvalue, rvalue))
        }),
        (JsonValue::Array(larr), JsonValue::Array(rarr)) => rarr
            .iter()
            .all(|rvalue| larr.iter().any(|lvalue| json_contains(lvalue, rvalue))),
        _ => lhs == rhs,
    }
}

fn eval_json_contains_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator @>", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator @>", 2)?;
    Ok(ScalarValue::Bool(json_contains(&lhs, &rhs)))
}

fn eval_json_contained_by_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator <@", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator <@", 2)?;
    Ok(ScalarValue::Bool(json_contains(&rhs, &lhs)))
}

fn json_array_index_from_i64(len: usize, index: i64) -> Option<usize> {
    if index >= 0 {
        let idx = index as usize;
        if idx < len { Some(idx) } else { None }
    } else {
        let back = index.unsigned_abs() as usize;
        if back == 0 || back > len {
            None
        } else {
            Some(len - back)
        }
    }
}

fn json_array_index_from_segment(len: usize, segment: &str) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    json_array_index_from_i64(len, index)
}

fn json_remove_path(target: &mut JsonValue, path: &[String]) -> bool {
    if path.is_empty() {
        return false;
    }
    if path.len() == 1 {
        return match target {
            JsonValue::Object(map) => map.remove(&path[0]).is_some(),
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                    return false;
                };
                array.remove(idx);
                true
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                false
            }
        };
    }

    match target {
        JsonValue::Object(map) => map
            .get_mut(&path[0])
            .is_some_and(|next| json_remove_path(next, &path[1..])),
        JsonValue::Array(array) => {
            let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                return false;
            };
            json_remove_path(&mut array[idx], &path[1..])
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

fn eval_json_delete_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator -", 1)?;
    match &mut target {
        JsonValue::Object(map) => {
            let key = scalar_to_json_path_segment(&right, "-")?;
            map.remove(&key);
        }
        JsonValue::Array(array) => match right {
            ScalarValue::Int(index) => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index) {
                    array.remove(idx);
                }
            }
            ScalarValue::Float(index) if index.fract() == 0.0 => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index as i64) {
                    array.remove(idx);
                }
            }
            ScalarValue::Text(text) => {
                array.retain(|item| !matches!(item, JsonValue::String(value) if value == &text));
            }
            _ => {
                return Err(EngineError {
                    message: "json operator - expects text key or integer array index".to_string(),
                });
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            return Err(EngineError {
                message: "json operator - expects object or array left operand".to_string(),
            });
        }
    }
    Ok(ScalarValue::Text(target.to_string()))
}

fn eval_json_delete_path_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator #-", 1)?;
    let path = parse_json_path_operand(&right, "#-")?;
    if !path.is_empty() {
        let _ = json_remove_path(&mut target, &path);
    }
    Ok(ScalarValue::Text(target.to_string()))
}

fn json_has_key(target: &JsonValue, key: &str) -> bool {
    match target {
        JsonValue::Object(map) => map.contains_key(key),
        JsonValue::Array(array) => array
            .iter()
            .any(|item| matches!(item, JsonValue::String(text) if text == key)),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

fn eval_json_has_key_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?", 1)?;
    let key = scalar_to_json_path_segment(&right, "?")?;
    Ok(ScalarValue::Bool(json_has_key(&target, &key)))
}

fn parse_json_key_list_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    let keys = parse_json_path_operand(value, operator_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{operator_name} operator key array cannot be empty"),
        });
    }
    Ok(keys)
}

fn eval_json_has_any_all_operator(
    left: ScalarValue,
    right: ScalarValue,
    any_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?|/?&", 1)?;
    let keys = parse_json_key_list_operand(&right, if any_mode { "?|" } else { "?&" })?;
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&target, key))
    } else {
        keys.iter().all(|key| json_has_key(&target, key))
    };
    Ok(ScalarValue::Bool(matched))
}

async fn eval_http_get_builtin(url_value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_get() expects a text URL argument".to_string(),
        });
    };

    #[cfg(not(target_arch = "wasm32"))]
    {
        let response = reqwest::get(url).await.map_err(|err| EngineError {
            message: format!("http_get request failed: {err}"),
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(EngineError {
                message: format!("http_get() request failed with status {status}"),
            });
        }
        let body = response.text().await.map_err(|err| EngineError {
            message: format!("http_get body read failed: {err}"),
        })?;
        Ok(ScalarValue::Text(body))
    }

    #[cfg(target_arch = "wasm32")]
    {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;

        let window = web_sys::window().ok_or_else(|| EngineError {
            message: "http_get(): window is not available".to_string(),
        })?;
        let resp_value = JsFuture::from(window.fetch_with_str(url))
            .await
            .map_err(|e| EngineError {
                message: format!(
                    "http_get() request failed: {}",
                    e.as_string().unwrap_or_else(|| "unknown error".to_string())
                ),
            })?;
        let resp: web_sys::Response = resp_value.dyn_into().map_err(|_| EngineError {
            message: "http_get(): response was not a Response".to_string(),
        })?;
        if !resp.ok() {
            return Err(EngineError {
                message: format!(
                    "http_get() request failed with status {} {}",
                    resp.status(),
                    resp.status_text()
                ),
            });
        }
        let text_promise = resp.text().map_err(|_| EngineError {
            message: "http_get(): failed to read response body".to_string(),
        })?;
        let body = JsFuture::from(text_promise)
            .await
            .map_err(|_| EngineError {
                message: "http_get(): failed to read response body".to_string(),
            })?
            .as_string()
            .unwrap_or_default();
        Ok(ScalarValue::Text(body))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum NumericOperand {
    Int(i64),
    Float(f64),
}

fn parse_numeric_operand(value: &ScalarValue) -> Result<NumericOperand, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(NumericOperand::Int(*v)),
        ScalarValue::Float(v) => Ok(NumericOperand::Float(*v)),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(NumericOperand::Int(parsed));
            }
            if let Ok(parsed) = v.parse::<f64>() {
                return Ok(NumericOperand::Float(parsed));
            }
            Err(EngineError {
                message: "numeric operation expects numeric values".to_string(),
            })
        }
        ScalarValue::Array(_) => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
        _ => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
    }
}

fn compare_values_for_predicate(
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Ordering, EngineError> {
    if let (Ok(left_num), Ok(right_num)) =
        (parse_numeric_operand(left), parse_numeric_operand(right))
    {
        let ord = match (left_num, right_num) {
            (NumericOperand::Int(a), NumericOperand::Int(b)) => a.cmp(&b),
            (NumericOperand::Int(a), NumericOperand::Float(b)) => {
                (a as f64).partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Int(b)) => {
                a.partial_cmp(&(b as f64)).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Float(b)) => {
                a.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
        };
        return Ok(ord);
    }

    if let (Some(left_bool), Some(right_bool)) = (try_parse_bool(left), try_parse_bool(right)) {
        return Ok(left_bool.cmp(&right_bool));
    }

    if let (Some(left_time), Some(right_time)) =
        (parse_temporal_operand(left), parse_temporal_operand(right))
    {
        if left_time.date_only && right_time.date_only {
            let left_days = days_from_civil(
                left_time.datetime.date.year,
                left_time.datetime.date.month,
                left_time.datetime.date.day,
            );
            let right_days = days_from_civil(
                right_time.datetime.date.year,
                right_time.datetime.date.month,
                right_time.datetime.date.day,
            );
            return Ok(left_days.cmp(&right_days));
        }
        let left_epoch = datetime_to_epoch_seconds(left_time.datetime);
        let right_epoch = datetime_to_epoch_seconds(right_time.datetime);
        return Ok(left_epoch.cmp(&right_epoch));
    }

    match (left, right) {
        (ScalarValue::Text(a), ScalarValue::Text(b)) => Ok(a.cmp(b)),
        _ => Ok(left.render().cmp(&right.render())),
    }
}

fn try_parse_bool(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Bool(v) => Some(*v),
        ScalarValue::Int(v) => Some(*v != 0),
        ScalarValue::Text(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "t" | "1" => Some(true),
                "false" | "f" | "0" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_nullable_bool(value: &ScalarValue, message: &str) -> Result<Option<bool>, EngineError> {
    match value {
        ScalarValue::Bool(v) => Ok(Some(*v)),
        ScalarValue::Null => Ok(None),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrimMode {
    Left,
    Right,
    Both,
}

fn substring_chars(input: &str, start: i64, length: Option<i64>) -> Result<String, EngineError> {
    if let Some(length) = length
        && length < 0
    {
        return Err(EngineError {
            message: "negative substring length not allowed".to_string(),
        });
    }
    let chars = input.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return Ok(String::new());
    }

    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx >= chars.len() {
        return Ok(String::new());
    }
    let end_idx = match length {
        Some(len) => start_idx.saturating_add(len as usize).min(chars.len()),
        None => chars.len(),
    };
    Ok(chars[start_idx..end_idx].iter().collect())
}

fn left_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        return chars[..(count as usize).min(chars.len())].iter().collect();
    }
    let keep = chars.len().saturating_sub(count.unsigned_abs() as usize);
    chars[..keep].iter().collect()
}

fn right_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        let keep = (count as usize).min(chars.len());
        return chars[chars.len() - keep..].iter().collect();
    }
    let drop = count.unsigned_abs() as usize;
    let start = drop.min(chars.len());
    chars[start..].iter().collect()
}

fn find_substring_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    let Some(byte_idx) = haystack.find(needle) else {
        return 0;
    };
    haystack[..byte_idx].chars().count() as i64 + 1
}

fn overlay_text(
    input: &str,
    replacement: &str,
    start: i64,
    count: Option<i64>,
) -> Result<String, EngineError> {
    let mut chars = input.chars().collect::<Vec<_>>();
    let replace_chars = replacement.chars().collect::<Vec<_>>();
    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx > chars.len() {
        return Ok(input.to_string());
    }
    let count = count.unwrap_or(replace_chars.len() as i64);
    if count < 0 {
        return Err(EngineError {
            message: "overlay() expects non-negative count".to_string(),
        });
    }
    let end_idx = start_idx.saturating_add(count as usize).min(chars.len());
    chars.splice(start_idx..end_idx, replace_chars);
    Ok(chars.iter().collect())
}

fn ascii_code(input: &str) -> i64 {
    input.chars().next().map(|c| c as i64).unwrap_or(0)
}

fn chr_from_code(code: i64) -> Result<String, EngineError> {
    if !(0..=255).contains(&code) {
        return Err(EngineError {
            message: "chr() expects value between 0 and 255".to_string(),
        });
    }
    let ch = char::from_u32(code as u32).ok_or_else(|| EngineError {
        message: "chr() expects valid Unicode code point".to_string(),
    })?;
    Ok(ch.to_string())
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn quote_ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

fn quote_nullable(value: &ScalarValue) -> String {
    if matches!(value, ScalarValue::Null) {
        "NULL".to_string()
    } else {
        quote_literal(&value.render())
    }
}

fn count_nulls(args: &[ScalarValue]) -> usize {
    args.iter().filter(|arg| matches!(arg, ScalarValue::Null)).count()
}

fn count_nonnulls(args: &[ScalarValue]) -> usize {
    args.len() - count_nulls(args)
}

fn trim_text(input: &str, trim_chars: Option<&str>, mode: TrimMode) -> String {
    match trim_chars {
        None => match mode {
            TrimMode::Left => input.trim_start().to_string(),
            TrimMode::Right => input.trim_end().to_string(),
            TrimMode::Both => input.trim().to_string(),
        },
        Some(chars) => trim_chars_from_text(input, chars, mode),
    }
}

fn trim_chars_from_text(input: &str, trim_chars: &str, mode: TrimMode) -> String {
    if trim_chars.is_empty() {
        return input.to_string();
    }
    let set: HashSet<char> = trim_chars.chars().collect();
    let chars = input.chars().collect::<Vec<_>>();

    let mut start = 0usize;
    let mut end = chars.len();
    if matches!(mode, TrimMode::Left | TrimMode::Both) {
        while start < end && set.contains(&chars[start]) {
            start += 1;
        }
    }
    if matches!(mode, TrimMode::Right | TrimMode::Both) {
        while end > start && set.contains(&chars[end - 1]) {
            end -= 1;
        }
    }
    chars[start..end].iter().collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TemporalOperand {
    datetime: DateTimeValue,
    date_only: bool,
}

fn parse_temporal_operand(value: &ScalarValue) -> Option<TemporalOperand> {
    let ScalarValue::Text(text) = value else {
        return None;
    };
    let trimmed = text.trim();
    let has_time = trimmed.contains('T') || trimmed.contains(' ');
    let datetime = parse_datetime_text(trimmed).ok()?;
    Some(TemporalOperand {
        datetime,
        date_only: !has_time,
    })
}

fn temporal_add_days(temporal: TemporalOperand, days: i64) -> ScalarValue {
    let mut datetime = temporal.datetime;
    datetime.date = add_days(datetime.date, days);
    if temporal.date_only {
        ScalarValue::Text(format_date(datetime.date))
    } else {
        ScalarValue::Text(format_timestamp(datetime))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateValue {
    year: i32,
    month: u32,
    day: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateTimeValue {
    date: DateValue,
    hour: u32,
    minute: u32,
    second: u32,
}

fn eval_date_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_date(datetime.date)))
}

fn eval_timestamp_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

fn eval_extract_or_date_part(
    field: &ScalarValue,
    source: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let datetime = parse_datetime_scalar(source)?;
    let value = match field_name.as_str() {
        "year" => ScalarValue::Int(datetime.date.year as i64),
        "month" => ScalarValue::Int(datetime.date.month as i64),
        "day" => ScalarValue::Int(datetime.date.day as i64),
        "hour" => ScalarValue::Int(datetime.hour as i64),
        "minute" => ScalarValue::Int(datetime.minute as i64),
        "second" => ScalarValue::Int(datetime.second as i64),
        "dow" => ScalarValue::Int(day_of_week(datetime.date) as i64),
        "doy" => ScalarValue::Int(day_of_year(datetime.date) as i64),
        "epoch" => ScalarValue::Float(datetime_to_epoch_seconds(datetime) as f64),
        _ => {
            return Err(EngineError {
                message: format!("unsupported date/time field {}", field_name),
            });
        }
    };
    Ok(value)
}

fn eval_date_trunc(field: &ScalarValue, source: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let mut datetime = parse_datetime_scalar(source)?;
    match field_name.as_str() {
        "year" => {
            datetime.date.month = 1;
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "month" => {
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "day" => {
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "hour" => {
            datetime.minute = 0;
            datetime.second = 0;
        }
        "minute" => {
            datetime.second = 0;
        }
        "second" => {}
        _ => {
            return Err(EngineError {
                message: format!("unsupported date_trunc field {}", field_name),
            });
        }
    }
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

fn eval_date_add_sub(
    date_value: &ScalarValue,
    day_delta: &ScalarValue,
    add: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(date_value, ScalarValue::Null) || matches!(day_delta, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(date_value)?;
    let mut days = parse_i64_scalar(day_delta, "date_add/date_sub expects integer day count")?;
    if !add {
        days = -days;
    }
    let shifted = add_days(datetime.date, days);
    Ok(ScalarValue::Text(format_date(shifted)))
}

fn current_timestamp_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_timestamp(dt))
}

fn current_date_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_date(dt.date))
}

#[derive(Debug, Clone, Copy)]
struct IntervalValue {
    months: i64,
    days: i64,
    seconds: i64,
}

#[derive(Debug, Clone, Copy)]
enum JustifyMode {
    Hours,
    Days,
    Full,
}

fn eval_age(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let (left, right) = if args.len() == 1 {
        let current = current_utc_datetime()?;
        let current_midnight = DateTimeValue {
            date: current.date,
            hour: 0,
            minute: 0,
            second: 0,
        };
        (current_midnight, parse_datetime_scalar(&args[0])?)
    } else {
        (
            parse_datetime_scalar(&args[0])?,
            parse_datetime_scalar(&args[1])?,
        )
    };
    let delta = datetime_to_epoch_seconds(left) - datetime_to_epoch_seconds(right);
    let interval = interval_from_seconds(delta);
    Ok(ScalarValue::Text(format_interval(interval)))
}

fn eval_to_timestamp(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let seconds = parse_f64_scalar(value, "to_timestamp() expects numeric input")?;
    let dt = datetime_from_epoch_seconds(seconds.trunc() as i64);
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

fn eval_to_timestamp_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let dt = parse_datetime_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

fn eval_to_date_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let date = parse_date_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_date(date)))
}

fn eval_make_interval(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let years = parse_i64_scalar(&args[0], "make_interval() expects years")?;
    let months = parse_i64_scalar(&args[1], "make_interval() expects months")?;
    let weeks = parse_i64_scalar(&args[2], "make_interval() expects weeks")?;
    let days = parse_i64_scalar(&args[3], "make_interval() expects days")?;
    let hours = parse_i64_scalar(&args[4], "make_interval() expects hours")?;
    let mins = parse_i64_scalar(&args[5], "make_interval() expects mins")?;
    let secs = parse_f64_scalar(&args[6], "make_interval() expects secs")?;

    let interval = IntervalValue {
        months: years * 12 + months,
        days: weeks * 7 + days,
        seconds: hours * 3_600 + mins * 60 + secs.trunc() as i64,
    };
    Ok(ScalarValue::Text(format_interval(interval)))
}

fn eval_justify_interval(
    value: &ScalarValue,
    mode: JustifyMode,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut interval = parse_interval_text(&value.render())?;
    if matches!(mode, JustifyMode::Hours | JustifyMode::Full) {
        let extra_days = interval.seconds.div_euclid(86_400);
        interval.days += extra_days;
        interval.seconds = interval.seconds.rem_euclid(86_400);
    }
    if matches!(mode, JustifyMode::Days | JustifyMode::Full) {
        let extra_months = interval.days.div_euclid(30);
        interval.months += extra_months;
        interval.days = interval.days.rem_euclid(30);
    }
    Ok(ScalarValue::Text(format_interval(interval)))
}

fn eval_isfinite(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let finite = match value {
        ScalarValue::Text(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            !matches!(normalized.as_str(), "infinity" | "-infinity" | "nan")
        }
        ScalarValue::Float(f) => f.is_finite(),
        _ => true,
    };
    Ok(ScalarValue::Bool(finite))
}

fn eval_width_bucket(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let value = parse_f64_scalar(&args[0], "width_bucket() expects numeric value")?;
    let min = parse_f64_scalar(&args[1], "width_bucket() expects numeric min")?;
    let max = parse_f64_scalar(&args[2], "width_bucket() expects numeric max")?;
    let count = parse_i64_scalar(&args[3], "width_bucket() expects integer count")?;
    if count <= 0 {
        return Err(EngineError {
            message: "width_bucket() expects positive count".to_string(),
        });
    }
    if min == max {
        return Err(EngineError {
            message: "width_bucket() requires min and max to differ".to_string(),
        });
    }
    let buckets = count as f64;
    let bucket = if min < max {
        if value < min {
            0
        } else if value >= max {
            count + 1
        } else {
            (((value - min) * buckets) / (max - min)).floor() as i64 + 1
        }
    } else {
        if value > min {
            0
        } else if value <= max {
            count + 1
        } else {
            (((min - value) * buckets) / (min - max)).floor() as i64 + 1
        }
    };
    Ok(ScalarValue::Int(bucket))
}

fn eval_scale(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let rendered = value.render();
    let trimmed = rendered.trim();
    let main = if let Some(idx) = trimmed.find('e').or_else(|| trimmed.find('E')) {
        &trimmed[..idx]
    } else {
        trimmed
    };
    let scale = main
        .split_once('.')
        .map(|(_, frac)| frac.len() as i64)
        .unwrap_or(0);
    Ok(ScalarValue::Int(scale))
}

fn eval_factorial(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let n = parse_i64_scalar(value, "factorial() expects integer")?;
    if n < 0 {
        return Err(EngineError {
            message: "factorial() expects non-negative integer".to_string(),
        });
    }
    let mut acc: i64 = 1;
    for i in 1..=n {
        acc = acc.checked_mul(i).ok_or_else(|| EngineError {
            message: "factorial() overflowed".to_string(),
        })?;
    }
    Ok(ScalarValue::Int(acc))
}

fn current_utc_datetime() -> Result<DateTimeValue, EngineError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let epoch_seconds = match now.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(err) => -(err.duration().as_secs() as i64),
    };
    Ok(datetime_from_epoch_seconds(epoch_seconds))
}

fn parse_datetime_scalar(value: &ScalarValue) -> Result<DateTimeValue, EngineError> {
    match value {
        ScalarValue::Text(v) => parse_datetime_text(v),
        ScalarValue::Int(v) => Ok(datetime_from_epoch_seconds(*v)),
        ScalarValue::Float(v) => Ok(datetime_from_epoch_seconds(*v as i64)),
        _ => Err(EngineError {
            message: "expected date/timestamp-compatible value".to_string(),
        }),
    }
}

fn parse_datetime_text(text: &str) -> Result<DateTimeValue, EngineError> {
    let raw = text.trim();
    if raw.is_empty() {
        return Err(EngineError {
            message: "invalid date/timestamp value".to_string(),
        });
    }

    let (date_part, time_part) = if let Some(pos) = raw.find('T') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else if let Some(pos) = raw.find(' ') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else {
        (raw, None)
    };

    let date = parse_date_text(date_part)?;
    let (hour, minute, second) = match time_part {
        None => (0, 0, 0),
        Some(time_raw) => parse_time_text(time_raw)?,
    };
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_text(text: &str) -> Result<DateValue, EngineError> {
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(EngineError {
            message: "invalid date value".to_string(),
        });
    }
    let year = parts[0].parse::<i32>().map_err(|_| EngineError {
        message: "invalid date year".to_string(),
    })?;
    let month = parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date month".to_string(),
    })?;
    let day = parts[2].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date day".to_string(),
    })?;
    if month == 0 || month > 12 {
        return Err(EngineError {
            message: "invalid date month".to_string(),
        });
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(EngineError {
            message: "invalid date day".to_string(),
        });
    }
    Ok(DateValue { year, month, day })
}

fn parse_time_text(text: &str) -> Result<(u32, u32, u32), EngineError> {
    let mut cleaned = text.trim();
    if cleaned.ends_with('Z') {
        cleaned = &cleaned[..cleaned.len() - 1];
    }
    if let Some(sign_pos) = cleaned
        .char_indices()
        .find_map(|(idx, ch)| ((ch == '+' || ch == '-') && idx > 1).then_some(idx))
    {
        cleaned = &cleaned[..sign_pos];
    }
    let time_parts = cleaned.split(':').collect::<Vec<_>>();
    if time_parts.len() < 2 || time_parts.len() > 3 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    let hour = time_parts[0].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp hour".to_string(),
    })?;
    let minute = time_parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp minute".to_string(),
    })?;
    let second = if time_parts.len() == 3 {
        time_parts[2]
            .split('.')
            .next()
            .unwrap_or("")
            .parse::<u32>()
            .map_err(|_| EngineError {
                message: "invalid timestamp second".to_string(),
            })?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    Ok((hour, minute, second))
}

fn parse_datetime_with_format(text: &str, format: &str) -> Result<DateTimeValue, EngineError> {
    let (date, hour, minute, second) = parse_datetime_parts_with_format(text, format)?;
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_with_format(text: &str, format: &str) -> Result<DateValue, EngineError> {
    let (date, _hour, _minute, _second) = parse_datetime_parts_with_format(text, format)?;
    Ok(date)
}

fn parse_datetime_parts_with_format(
    text: &str,
    format: &str,
) -> Result<(DateValue, u32, u32, u32), EngineError> {
    let input = text.trim();
    let fmt = format.trim().to_ascii_uppercase();
    let mut in_idx = 0usize;
    let mut fmt_idx = 0usize;
    let bytes = input.as_bytes();

    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    let mut day: Option<u32> = None;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;

    while fmt_idx < fmt.len() {
        let remaining = &fmt[fmt_idx..];
        if remaining.starts_with("YYYY") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 4, "year")? as i32;
            year = Some(value);
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MM") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "month")? as u32;
            month = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("DD") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "day")? as u32;
            day = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("HH24") {
            hour = parse_fixed_digits(bytes, &mut in_idx, 2, "hour")? as u32;
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MI") {
            minute = parse_fixed_digits(bytes, &mut in_idx, 2, "minute")? as u32;
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("SS") {
            second = parse_fixed_digits(bytes, &mut in_idx, 2, "second")? as u32;
            fmt_idx += 2;
            continue;
        }

        let ch = fmt[fmt_idx..].chars().next().unwrap();
        let ch_len = ch.len_utf8();
        if in_idx >= bytes.len() || bytes[in_idx] != ch as u8 {
            return Err(EngineError {
                message: "to_timestamp/to_date format mismatch".to_string(),
            });
        }
        in_idx += 1;
        fmt_idx += ch_len;
    }

    if in_idx != bytes.len() {
        return Err(EngineError {
            message: "to_timestamp/to_date format mismatch".to_string(),
        });
    }

    let date = date_from_parts(
        year.ok_or_else(|| EngineError { message: "missing year".to_string() })?,
        month.ok_or_else(|| EngineError { message: "missing month".to_string() })?,
        day.ok_or_else(|| EngineError { message: "missing day".to_string() })?,
    )?;

    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid time component".to_string(),
        });
    }

    Ok((date, hour, minute, second))
}

fn parse_fixed_digits(
    bytes: &[u8],
    idx: &mut usize,
    count: usize,
    label: &str,
) -> Result<i64, EngineError> {
    if *idx + count > bytes.len() {
        return Err(EngineError {
            message: format!("invalid {label} in format input"),
        });
    }
    let slice = &bytes[*idx..*idx + count];
    let text = std::str::from_utf8(slice).unwrap_or("");
    let value = text.parse::<i64>().map_err(|_| EngineError {
        message: format!("invalid {label} in format input"),
    })?;
    *idx += count;
    Ok(value)
}

fn date_from_parts(year: i32, month: u32, day: u32) -> Result<DateValue, EngineError> {
    if month == 0 || month > 12 {
        return Err(EngineError {
            message: "invalid date month".to_string(),
        });
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(EngineError {
            message: "invalid date day".to_string(),
        });
    }
    Ok(DateValue { year, month, day })
}

fn format_date(date: DateValue) -> String {
    format!("{:04}-{:02}-{:02}", date.year, date.month, date.day)
}

fn format_timestamp(datetime: DateTimeValue) -> String {
    format!(
        "{} {:02}:{:02}:{:02}",
        format_date(datetime.date),
        datetime.hour,
        datetime.minute,
        datetime.second
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn day_of_year(date: DateValue) -> u32 {
    let mut total = 0u32;
    for month in 1..date.month {
        total += days_in_month(date.year, month);
    }
    total + date.day
}

fn day_of_week(date: DateValue) -> u32 {
    let days = days_from_civil(date.year, date.month, date.day);
    (days + 4).rem_euclid(7) as u32
}

fn add_days(date: DateValue, days: i64) -> DateValue {
    let day_number = days_from_civil(date.year, date.month, date.day);
    civil_from_days(day_number + days)
}

fn datetime_to_epoch_seconds(datetime: DateTimeValue) -> i64 {
    let days = days_from_civil(datetime.date.year, datetime.date.month, datetime.date.day);
    days * 86_400
        + datetime.hour as i64 * 3_600
        + datetime.minute as i64 * 60
        + datetime.second as i64
}

fn datetime_from_epoch_seconds(seconds: i64) -> DateTimeValue {
    let day = seconds.div_euclid(86_400);
    let sec_of_day = seconds.rem_euclid(86_400);
    let date = civil_from_days(day);
    DateTimeValue {
        date,
        hour: (sec_of_day / 3_600) as u32,
        minute: ((sec_of_day % 3_600) / 60) as u32,
        second: (sec_of_day % 60) as u32,
    }
}

fn interval_from_seconds(seconds: i64) -> IntervalValue {
    let days = seconds.div_euclid(86_400);
    let rem = seconds.rem_euclid(86_400);
    IntervalValue {
        months: 0,
        days,
        seconds: rem,
    }
}

fn parse_interval_text(text: &str) -> Result<IntervalValue, EngineError> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 5 {
        return Err(EngineError {
            message: "invalid interval value".to_string(),
        });
    }
    let months = parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval months".to_string(),
    })?;
    let days = parts[2].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval days".to_string(),
    })?;
    let time = parts[4];
    let time_parts = time.split(':').collect::<Vec<_>>();
    if time_parts.len() != 3 {
        return Err(EngineError {
            message: "invalid interval time".to_string(),
        });
    }
    let hour = time_parts[0]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval hour".to_string(),
        })?;
    let minute = time_parts[1]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval minute".to_string(),
        })?;
    let second = time_parts[2]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval second".to_string(),
        })?;
    let total_seconds = hour * 3_600 + minute * 60 + second;
    Ok(IntervalValue {
        months,
        days,
        seconds: total_seconds,
    })
}

fn format_interval(interval: IntervalValue) -> String {
    let sign = if interval.seconds < 0 { "-" } else { "" };
    let seconds = interval.seconds.abs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!(
        "{} mons {} days {}{:02}:{:02}:{:02}",
        interval.months, interval.days, sign, hours, minutes, secs
    )
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> DateValue {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    DateValue {
        year: year as i32,
        month: month as u32,
        day: day as u32,
    }
}

fn sequence_next_value(state: &mut SequenceState, sequence_name: &str) -> Result<i64, EngineError> {
    if !state.called {
        state.called = true;
        if state.current < state.min_value || state.current > state.max_value {
            return Err(EngineError {
                message: format!("sequence \"{}\" is out of bounds", sequence_name),
            });
        }
        return Ok(state.current);
    }

    let next = state
        .current
        .checked_add(state.increment)
        .ok_or_else(|| EngineError {
            message: format!("sequence \"{}\" overflowed", sequence_name),
        })?;
    if next >= state.min_value && next <= state.max_value {
        state.current = next;
        return Ok(next);
    }
    if !state.cycle {
        return Err(EngineError {
            message: format!(
                "nextval: sequence \"{}\" has reached its limit",
                sequence_name
            ),
        });
    }
    state.current = if state.increment > 0 {
        state.min_value
    } else {
        state.max_value
    };
    Ok(state.current)
}

fn set_sequence_value(
    state: &mut SequenceState,
    sequence_name: &str,
    value: i64,
    is_called: bool,
) -> Result<(), EngineError> {
    if value < state.min_value || value > state.max_value {
        return Err(EngineError {
            message: format!(
                "setval: value {} is out of bounds for sequence \"{}\"",
                value, sequence_name
            ),
        });
    }
    state.current = value;
    state.called = is_called;
    Ok(())
}

fn parse_i64_scalar(value: &ScalarValue, message: &str) -> Result<i64, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(*v),
        ScalarValue::Float(v) if v.fract() == 0.0 => Ok(*v as i64),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(parsed);
            }
            if let Ok(parsed) = v.parse::<f64>()
                && parsed.fract() == 0.0
            {
                return Ok(parsed as i64);
            }
            Err(EngineError {
                message: message.to_string(),
            })
        }
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn parse_f64_scalar(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Text(v) => v.parse::<f64>().map_err(|_| EngineError {
            message: message.to_string(),
        }),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn parse_f64_numeric_scalar(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn parse_bool_scalar(value: &ScalarValue, message: &str) -> Result<bool, EngineError> {
    try_parse_bool(value).ok_or_else(|| EngineError {
        message: message.to_string(),
    })
}

fn truthy(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Bool(v) => *v,
        ScalarValue::Null => false,
        ScalarValue::Int(v) => *v != 0,
        ScalarValue::Float(v) => *v != 0.0,
        ScalarValue::Text(v) => !v.is_empty(),
        ScalarValue::Array(values) => !values.is_empty(),
    }
}

// ── Extension & Function execution ──────────────────────────────────────────

async fn execute_create_extension(
    create: &CreateExtensionStatement,
) -> Result<QueryResult, EngineError> {
    let name = create.name.to_ascii_lowercase();
    with_ext_write(|ext| {
        if ext.extensions.iter().any(|e| e.name == name) {
            if create.if_not_exists {
                return Ok(());
            }
            return Err(EngineError {
                message: format!("extension \"{}\" already exists", name),
            });
        }
        let (version, description) = match name.as_str() {
            "ws" => ("1.0".to_string(), "WebSocket client extension".to_string()),
            _ => {
                return Err(EngineError {
                    message: format!("extension \"{}\" is not available", name),
                });
            }
        };
        ext.extensions.push(ExtensionRecord {
            name: name.clone(),
            version,
            description,
        });
        // Register ws extension functions as built-in markers
        if name == "ws" {
            ext.ws_next_id = 1;
            ext.ws_connections.clear();
        }
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE EXTENSION".to_string(),
        rows_affected: 0,
    })
}

async fn execute_drop_extension(
    drop_ext: &DropExtensionStatement,
) -> Result<QueryResult, EngineError> {
    let name = drop_ext.name.to_ascii_lowercase();
    with_ext_write(|ext| {
        let before = ext.extensions.len();
        ext.extensions.retain(|e| e.name != name);
        if ext.extensions.len() == before && !drop_ext.if_exists {
            return Err(EngineError {
                message: format!("extension \"{}\" does not exist", name),
            });
        }
        if name == "ws" {
            ext.ws_connections.clear();
            // Remove ws extension functions
            ext.user_functions.retain(|f| {
                f.name.first().map(|s| s.as_str()) != Some("ws")
            });
        }
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP EXTENSION".to_string(),
        rows_affected: 0,
    })
}

async fn execute_create_function(
    create: &CreateFunctionStatement,
) -> Result<QueryResult, EngineError> {
    let uf = UserFunction {
        name: create.name.iter().map(|s| s.to_ascii_lowercase()).collect(),
        params: create.params.clone(),
        return_type: create.return_type.clone(),
        body: create.body.trim().to_string(),
        language: create.language.clone(),
    };
    with_ext_write(|ext| {
        if create.or_replace {
            ext.user_functions.retain(|f| f.name != uf.name);
        } else if ext.user_functions.iter().any(|f| f.name == uf.name) {
            return Err(EngineError {
                message: format!("function \"{}\" already exists", uf.name.join(".")),
            });
        }
        ext.user_functions.push(uf);
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE FUNCTION".to_string(),
        rows_affected: 0,
    })
}

fn is_ws_extension_loaded() -> bool {
    with_ext_read(|ext| ext.extensions.iter().any(|e| e.name == "ws"))
}

async fn execute_ws_connect(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let url = match args.first() {
        Some(ScalarValue::Text(u)) => u.clone(),
        _ => {
            return Err(EngineError {
                message: "ws.connect requires a URL argument".to_string(),
            });
        }
    };
    let on_open = match args.get(1) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let on_message = match args.get(2) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let on_close = match args.get(3) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    // Try real connection on native (non-wasm, non-test)
    #[cfg(not(target_arch = "wasm32"))]
    let real_result = ws_native::open_connection(&url);
    #[cfg(target_arch = "wasm32")]
    let real_result = ws_wasm::open_connection(&url);

    #[cfg(not(target_arch = "wasm32"))]
    let (real_io, initial_state) = match &real_result {
        Ok(_) => (true, "open".to_string()),
        Err(_) => (false, "connecting".to_string()),
    };
    #[cfg(target_arch = "wasm32")]
    let (real_io, initial_state) = match &real_result {
        Ok(_) => (true, "connecting".to_string()),
        Err(_) => (false, "connecting".to_string()),
    };

    let id = with_ext_write(|ext| {
        let id = ext.ws_next_id;
        ext.ws_next_id += 1;
        ext.ws_connections.insert(id, WsConnection {
            id,
            url,
            state: initial_state,
            opened_at: "2024-01-01 00:00:00".to_string(),
            messages_in: 0,
            messages_out: 0,
            on_open,
            on_message,
            on_close,
            inbound_queue: Vec::new(),
            real_io,
        });
        id
    });

    // Store the handle if real connection succeeded
    #[cfg(not(target_arch = "wasm32"))]
    if let Ok(handle) = real_result {
        native_ws_handles().lock().unwrap().insert(id, handle);
    }
    #[cfg(target_arch = "wasm32")]
    if let Ok(handle) = real_result {
        ws_wasm::store_handle(id, handle);
    }

    Ok(ScalarValue::Int(id))
}

async fn execute_ws_send(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.send requires a connection id".to_string(),
            });
        }
    };
    let _message = match args.get(1) {
        Some(ScalarValue::Text(m)) => m.clone(),
        _ => {
            return Err(EngineError {
                message: "ws.send requires a message argument".to_string(),
            });
        }
    };
    let (is_real, is_closed) = with_ext_read(|ext| {
        if let Some(conn) = ext.ws_connections.get(&conn_id) {
            Ok((conn.real_io, conn.state == "closed"))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })?;
    if is_closed {
        return Err(EngineError {
            message: format!("connection {} is closed", conn_id),
        });
    }

    // Send over real connection if available
    #[cfg(not(target_arch = "wasm32"))]
    if is_real {
        let handles = native_ws_handles().lock().unwrap();
        if let Some(handle) = handles.get(&conn_id) {
            ws_native::send_message(handle, &_message).map_err(|e| EngineError { message: e })?;
        }
    }
    #[cfg(target_arch = "wasm32")]
    if is_real {
        let send_result = ws_wasm::with_handle(conn_id, |handle| {
            ws_wasm::send_message(handle, &_message)
        });
        if let Some(Err(e)) = send_result {
            return Err(EngineError { message: e });
        }
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.messages_out += 1;
            Ok(ScalarValue::Bool(true))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

async fn execute_ws_close(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.close requires a connection id".to_string(),
            });
        }
    };
    // Close real connection if present
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut handles = native_ws_handles().lock().unwrap();
        if let Some(handle) = handles.get(&conn_id) {
            let _ = ws_native::close_connection(handle);
        }
        handles.remove(&conn_id);
    }
    #[cfg(target_arch = "wasm32")]
    {
        ws_wasm::with_handle(conn_id, |handle| {
            let _ = ws_wasm::close_connection(handle);
        });
        ws_wasm::remove_handle(conn_id);
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.state = "closed".to_string();
            Ok(ScalarValue::Bool(true))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

async fn execute_ws_recv(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.recv requires a connection id".to_string(),
            });
        }
    };

    // Drain any real incoming messages first
    #[cfg(not(target_arch = "wasm32"))]
    drain_native_ws_messages(conn_id);
    #[cfg(target_arch = "wasm32")]
    { sync_wasm_ws_state(conn_id); drain_wasm_ws_messages(conn_id); }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            if conn.inbound_queue.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Text(conn.inbound_queue.remove(0)))
            }
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

async fn execute_ws_messages(
    args: &[ScalarValue],
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.messages requires a connection id".to_string(),
            });
        }
    };

    // Drain any real incoming messages first
    #[cfg(not(target_arch = "wasm32"))]
    drain_native_ws_messages(conn_id);
    #[cfg(target_arch = "wasm32")]
    { sync_wasm_ws_state(conn_id); drain_wasm_ws_messages(conn_id); }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            let rows: Vec<Vec<ScalarValue>> = conn.inbound_queue
                .drain(..)
                .map(|m| vec![ScalarValue::Text(m)])
                .collect();
            Ok((vec!["message".to_string()], rows))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

/// Simulate receiving a message on a WebSocket connection (for testing).
/// Dispatches the on_message callback if set.
pub async fn ws_simulate_message(conn_id: i64, message: &str) -> Result<Vec<QueryResult>, EngineError> {
    let callback = with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.messages_in += 1;
            conn.inbound_queue.push(message.to_string());
            Ok(conn.on_message.clone())
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })?;

    let mut results = Vec::new();
    if let Some(func_name) = callback {
        // Look up the user function and execute it with the message as parameter
        let uf = with_ext_read(|ext| {
            ext.user_functions
                .iter()
                .find(|f| {
                    let fname = f.name.last().map(|s| s.as_str()).unwrap_or("");
                    fname == func_name.to_ascii_lowercase()
                })
                .cloned()
        });
        if let Some(uf) = uf {
            // Execute the function body with message substituted for the first parameter
            let body = uf.body.clone();
            // Simple parameter substitution: replace references to the first param with the message value
            let param_name = uf.params.first().and_then(|p| p.name.clone());
            let substituted = if let Some(pname) = param_name {
                body.replace(&pname, &format!("'{}'", message.replace('\'', "''")))
            } else {
                body.replace("$1", &format!("'{}'", message.replace('\'', "''")))
            };
            let stmt = crate::parser::sql_parser::parse_statement(&substituted)
                .map_err(|e| EngineError { message: format!("callback parse error: {}", e) })?;
            let planned = plan_statement(stmt)?;
            let result = execute_planned_query(&planned, &[]).await?;
            results.push(result);
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
    use crate::parser::sql_parser::parse_statement;
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
