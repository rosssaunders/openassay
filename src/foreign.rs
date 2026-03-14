//! Foreign Data Wrapper (FDW) support.
//!
//! Mirrors PostgreSQL's FDW architecture (src/backend/foreign/foreign.c,
//! src/include/foreign/fdwapi.h, src/backend/executor/nodeForeignscan.c).
//!
//! FDWs allow the query engine to read from external data sources (Excel
//! ranges, URLs, files) without materializing data into temporary tables.

use std::collections::HashMap;
use std::fmt;

use crate::catalog::oid::Oid;
use crate::catalog::table::TypeSignature;
use crate::storage::tuple::ScalarValue;

// ---------------------------------------------------------------------------
// Qual – predicate pushed down to the FDW
// ---------------------------------------------------------------------------

/// A simplified representation of a pushdown‐eligible qualifier.
///
/// Mirrors PostgreSQL's `RestrictInfo` in spirit: the FDW can inspect quals
/// to decide which rows to skip at the source level.
#[derive(Debug, Clone, PartialEq)]
pub struct Qual {
    /// Column name the predicate references.
    pub column: String,
    /// Comparison operator.
    pub operator: QualOperator,
    /// Right‐hand side literal value.
    pub value: ScalarValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualOperator {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl fmt::Display for QualOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::Lte => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Gte => write!(f, ">="),
        }
    }
}

// ---------------------------------------------------------------------------
// ForeignTable – metadata for a foreign table
// ---------------------------------------------------------------------------

/// Metadata for a single foreign table, analogous to PostgreSQL's
/// `ForeignTable` struct in `include/foreign/foreign.h`.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignTableDef {
    /// The catalog OID of this foreign table.
    pub table_oid: Oid,
    /// Name of the foreign server this table belongs to.
    pub server_name: String,
    /// Per-table OPTIONS (key → value).
    pub options: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// ForeignServer – metadata for a foreign server
// ---------------------------------------------------------------------------

/// Mirrors PostgreSQL's `ForeignServer` (pg_foreign_server catalog entry).
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignServer {
    /// OID of this server entry.
    pub oid: Oid,
    /// Logical server name (CREATE SERVER <name>).
    pub name: String,
    /// Name of the foreign-data wrapper used by this server.
    pub fdw_name: String,
    /// Per-server OPTIONS (key → value).
    pub options: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// ForeignColumnDef – column definition for a foreign table
// ---------------------------------------------------------------------------

/// Column definition used when creating a foreign table.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignColumnDef {
    pub name: String,
    pub type_signature: TypeSignature,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// ForeignScan trait – iterator over foreign rows
// ---------------------------------------------------------------------------

/// Per‐scan state returned by `ForeignDataWrapper::begin_scan`.
///
/// Mirrors PostgreSQL's `ForeignScanState` / the per‐tuple callbacks in
/// `nodeForeignscan.c` (`ExecForeignScan` → `IterateForeignScan`).
pub trait ForeignScan: Send {
    /// Return the next row, or `None` when the scan is exhausted.
    ///
    /// Each row is a `Vec<ScalarValue>` matching the foreign table's column
    /// order.
    fn iterate(&mut self) -> Option<Vec<ScalarValue>>;

    /// Called once at the end of the scan (cleanup resources).
    fn end(&mut self);
}

// ---------------------------------------------------------------------------
// ForeignDataWrapper trait – the main FDW entry point
// ---------------------------------------------------------------------------

/// The core FDW interface, mirroring PostgreSQL's `FdwRoutine`.
///
/// Implementations are registered in the FDW registry keyed by the wrapper
/// name supplied in `CREATE SERVER ... FOREIGN DATA WRAPPER <name>`.
pub trait ForeignDataWrapper: Send + Sync {
    /// Human‐readable wrapper name (e.g. `"file_fdw"`, `"excel_fdw"`).
    fn name(&self) -> &str;

    /// Start a scan of the foreign table.
    ///
    /// `table` carries the table metadata (OID, server options, table options).
    /// `quals` are pushdown‐eligible predicates the FDW may use to filter at
    /// source.
    fn begin_scan(
        &self,
        table: &ForeignTableDef,
        quals: &[Qual],
    ) -> Result<Box<dyn ForeignScan>, FdwError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FdwError {
    pub message: String,
}

impl fmt::Display for FdwError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for FdwError {}

// ---------------------------------------------------------------------------
// FDW Registry – thread-local store of FDW implementations & servers
// ---------------------------------------------------------------------------

use std::cell::RefCell;
use std::sync::Arc;

/// Global (thread-local) FDW registry.
///
/// Stores registered FDW implementations, foreign servers, and foreign table
/// definitions. Thread-local follows the same pattern as the catalog and
/// storage in this codebase.
pub struct FdwRegistry {
    /// Registered wrapper implementations keyed by wrapper name.
    wrappers: HashMap<String, Arc<dyn ForeignDataWrapper>>,
    /// Foreign servers keyed by server name.
    servers: HashMap<String, ForeignServer>,
    /// Foreign table definitions keyed by table OID.
    tables: HashMap<Oid, ForeignTableDef>,
}

impl Default for FdwRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FdwRegistry {
    pub fn new() -> Self {
        Self {
            wrappers: HashMap::new(),
            servers: HashMap::new(),
            tables: HashMap::new(),
        }
    }

    // -- Wrappers -----------------------------------------------------------

    /// Register a FDW implementation.
    pub fn register_wrapper(&mut self, wrapper: Arc<dyn ForeignDataWrapper>) {
        self.wrappers
            .insert(wrapper.name().to_ascii_lowercase(), wrapper);
    }

    /// Look up a wrapper by name.
    pub fn get_wrapper(&self, name: &str) -> Option<Arc<dyn ForeignDataWrapper>> {
        self.wrappers.get(&name.to_ascii_lowercase()).cloned()
    }

    // -- Servers ------------------------------------------------------------

    pub fn register_server(&mut self, server: ForeignServer) {
        self.servers
            .insert(server.name.to_ascii_lowercase(), server);
    }

    pub fn get_server(&self, name: &str) -> Option<&ForeignServer> {
        self.servers.get(&name.to_ascii_lowercase())
    }

    // -- Tables -------------------------------------------------------------

    pub fn register_table(&mut self, table_def: ForeignTableDef) {
        self.tables.insert(table_def.table_oid, table_def);
    }

    pub fn get_table(&self, oid: Oid) -> Option<&ForeignTableDef> {
        self.tables.get(&oid)
    }
}

impl Clone for FdwRegistry {
    fn clone(&self) -> Self {
        Self {
            wrappers: self.wrappers.clone(),
            servers: self.servers.clone(),
            tables: self.tables.clone(),
        }
    }
}

thread_local! {
    static FDW_REGISTRY: RefCell<FdwRegistry> = RefCell::new(FdwRegistry::new());
}

pub fn with_fdw_read<T>(f: impl FnOnce(&FdwRegistry) -> T) -> T {
    FDW_REGISTRY.with(|r| f(&r.borrow()))
}

pub fn with_fdw_write<T>(f: impl FnOnce(&mut FdwRegistry) -> T) -> T {
    FDW_REGISTRY.with(|r| f(&mut r.borrow_mut()))
}

// ---------------------------------------------------------------------------
// Executor helper – scan a foreign table
// ---------------------------------------------------------------------------

/// Execute a foreign scan for the given catalog table.
///
/// Looks up the FDW registry to find the table definition, server, and wrapper,
/// then calls `begin_scan` / `iterate` / `end` to produce rows.
pub fn execute_foreign_scan(
    table: &crate::catalog::Table,
) -> Result<Vec<Vec<ScalarValue>>, crate::tcop::engine::EngineError> {
    let table_oid = table.oid();

    // 1. Look up the ForeignTableDef in the registry.
    let (table_def, server_fdw_name) = with_fdw_read(|reg| {
        let table_def =
            reg.get_table(table_oid)
                .cloned()
                .ok_or_else(|| crate::tcop::engine::EngineError {
                    message: format!(
                        "foreign table \"{}\" has no FDW definition registered",
                        table.qualified_name()
                    ),
                })?;
        let server = reg.get_server(&table_def.server_name).ok_or_else(|| {
            crate::tcop::engine::EngineError {
                message: format!(
                    "foreign server \"{}\" not found for table \"{}\"",
                    table_def.server_name,
                    table.qualified_name()
                ),
            }
        })?;
        Ok::<_, crate::tcop::engine::EngineError>((table_def.clone(), server.fdw_name.clone()))
    })?;

    // 2. Get the wrapper implementation.
    let wrapper = with_fdw_read(|reg| reg.get_wrapper(&server_fdw_name)).ok_or_else(|| {
        crate::tcop::engine::EngineError {
            message: format!("foreign data wrapper \"{server_fdw_name}\" is not registered"),
        }
    })?;

    // 3. Begin scan (no quals pushed down for now).
    let mut scan =
        wrapper
            .begin_scan(&table_def, &[])
            .map_err(|e| crate::tcop::engine::EngineError {
                message: format!("FDW scan error: {}", e.message),
            })?;

    // 4. Iterate to collect all rows.
    let mut rows = Vec::new();
    while let Some(row) = scan.iterate() {
        rows.push(row);
    }

    // 5. End scan.
    scan.end();

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;

    use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
    use crate::parser::sql_parser::parse_statement;
    use crate::storage::tuple::ScalarValue;
    use crate::tcop::engine::{
        execute_planned_query, plan_statement, reset_global_storage_for_tests,
    };

    use super::*;

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime should start")
            .block_on(future)
    }

    fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
        with_global_state_lock(|| {
            reset_global_catalog_for_tests();
            reset_global_storage_for_tests();
            // Also reset the FDW registry.
            with_fdw_write(|reg| {
                *reg = FdwRegistry::new();
            });
            f()
        })
    }

    fn run_sql(sql: &str) -> crate::tcop::engine::QueryResult {
        let statement = parse_statement(sql).expect("statement should parse");
        let planned = plan_statement(statement).expect("statement should plan");
        block_on(execute_planned_query(&planned, &[])).expect("query should execute")
    }

    /// A trivial in-memory FDW for testing.
    struct TestFdw {
        rows: Vec<Vec<ScalarValue>>,
    }

    struct TestScan {
        rows: std::vec::IntoIter<Vec<ScalarValue>>,
    }

    impl ForeignScan for TestScan {
        fn iterate(&mut self) -> Option<Vec<ScalarValue>> {
            self.rows.next()
        }
        fn end(&mut self) {}
    }

    impl ForeignDataWrapper for TestFdw {
        fn name(&self) -> &str {
            "test_fdw"
        }
        fn begin_scan(
            &self,
            _table: &ForeignTableDef,
            _quals: &[Qual],
        ) -> Result<Box<dyn ForeignScan>, FdwError> {
            Ok(Box::new(TestScan {
                rows: self.rows.clone().into_iter(),
            }))
        }
    }

    #[test]
    fn parse_create_server() {
        let stmt = parse_statement(
            "CREATE SERVER myserver FOREIGN DATA WRAPPER test_fdw OPTIONS (host 'localhost', port '5432')",
        )
        .unwrap();
        match stmt {
            crate::parser::ast::Statement::CreateServer(s) => {
                assert_eq!(s.name, "myserver");
                assert_eq!(s.fdw_name, "test_fdw");
                assert_eq!(s.options.len(), 2);
                assert_eq!(s.options[0], ("host".to_string(), "localhost".to_string()));
                assert_eq!(s.options[1], ("port".to_string(), "5432".to_string()));
                assert!(!s.if_not_exists);
            }
            other => panic!("expected CreateServer, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_server_if_not_exists() {
        let stmt =
            parse_statement("CREATE SERVER IF NOT EXISTS myserver FOREIGN DATA WRAPPER test_fdw")
                .unwrap();
        match stmt {
            crate::parser::ast::Statement::CreateServer(s) => {
                assert!(s.if_not_exists);
            }
            other => panic!("expected CreateServer, got {:?}", other),
        }
    }

    #[test]
    fn parse_create_foreign_table() {
        let stmt = parse_statement(
            "CREATE FOREIGN TABLE ft (id int8, name text) SERVER myserver OPTIONS (filename '/tmp/data.csv')",
        )
        .unwrap();
        match stmt {
            crate::parser::ast::Statement::CreateForeignTable(s) => {
                assert_eq!(s.name, vec!["ft"]);
                assert_eq!(s.columns.len(), 2);
                assert_eq!(s.server_name, "myserver");
                assert_eq!(s.options.len(), 1);
                assert_eq!(
                    s.options[0],
                    ("filename".to_string(), "/tmp/data.csv".to_string())
                );
            }
            other => panic!("expected CreateForeignTable, got {:?}", other),
        }
    }

    #[test]
    fn create_server_and_foreign_table_execute() {
        with_isolated_state(|| {
            // Register the test FDW.
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw {
                    rows: vec![
                        vec![ScalarValue::Int(1), ScalarValue::Text("Alice".to_string())],
                        vec![ScalarValue::Int(2), ScalarValue::Text("Bob".to_string())],
                    ],
                }));
            });

            // Create server.
            let result = run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
            assert_eq!(result.command_tag, "CREATE SERVER");

            // Create foreign table.
            let result = run_sql("CREATE FOREIGN TABLE ft (id int8, name text) SERVER testserver");
            assert_eq!(result.command_tag, "CREATE FOREIGN TABLE");

            // Query the foreign table.
            let result = run_sql("SELECT * FROM ft");
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.rows[0][0], ScalarValue::Int(1));
            assert_eq!(result.rows[0][1], ScalarValue::Text("Alice".to_string()));
            assert_eq!(result.rows[1][0], ScalarValue::Int(2));
            assert_eq!(result.rows[1][1], ScalarValue::Text("Bob".to_string()));
        });
    }

    #[test]
    fn foreign_table_appears_in_information_schema() {
        with_isolated_state(|| {
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw { rows: vec![] }));
            });
            run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
            run_sql("CREATE FOREIGN TABLE ft (id int8) SERVER testserver");

            let result =
                run_sql("SELECT table_type FROM information_schema.tables WHERE table_name = 'ft'");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(
                result.rows[0][0],
                ScalarValue::Text("FOREIGN TABLE".to_string())
            );
        });
    }

    #[test]
    fn create_server_duplicate_errors() {
        with_isolated_state(|| {
            run_sql("CREATE SERVER dup_server FOREIGN DATA WRAPPER test_fdw");
            let statement =
                parse_statement("CREATE SERVER dup_server FOREIGN DATA WRAPPER test_fdw").unwrap();
            let planned = plan_statement(statement).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
            assert!(result.unwrap_err().message.contains("already exists"));
        });
    }

    #[test]
    fn create_server_if_not_exists_succeeds() {
        with_isolated_state(|| {
            run_sql("CREATE SERVER dup_server FOREIGN DATA WRAPPER test_fdw");
            let result =
                run_sql("CREATE SERVER IF NOT EXISTS dup_server FOREIGN DATA WRAPPER test_fdw");
            assert_eq!(result.command_tag, "CREATE SERVER");
        });
    }

    #[test]
    fn create_foreign_table_missing_server_errors() {
        with_isolated_state(|| {
            let statement =
                parse_statement("CREATE FOREIGN TABLE ft (id int8) SERVER nonexistent_server")
                    .unwrap();
            let planned = plan_statement(statement).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
            assert!(result.unwrap_err().message.contains("does not exist"));
        });
    }

    #[test]
    fn foreign_table_with_options() {
        with_isolated_state(|| {
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw { rows: vec![] }));
            });

            run_sql(
                "CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw OPTIONS (host 'example.com')",
            );
            run_sql(
                "CREATE FOREIGN TABLE ft (col1 text) SERVER testserver OPTIONS (filename '/data.csv')",
            );

            // Verify the table options were stored.
            let table_def = with_fdw_read(|reg| reg.tables.values().next().cloned());
            assert!(table_def.is_some());
            let table_def = table_def.unwrap();
            assert_eq!(
                table_def.options.get("filename"),
                Some(&"/data.csv".to_string())
            );
        });
    }

    #[test]
    fn create_foreign_table_duplicate_errors() {
        with_isolated_state(|| {
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw { rows: vec![] }));
            });
            run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
            run_sql("CREATE FOREIGN TABLE ft (id int8) SERVER testserver");
            let statement =
                parse_statement("CREATE FOREIGN TABLE ft (id int8) SERVER testserver").unwrap();
            let planned = plan_statement(statement).unwrap();
            let result = block_on(execute_planned_query(&planned, &[]));
            assert!(result.is_err());
            assert!(result.unwrap_err().message.contains("already exists"));
        });
    }

    #[test]
    fn create_foreign_table_if_not_exists_succeeds() {
        with_isolated_state(|| {
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw { rows: vec![] }));
            });
            run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
            run_sql("CREATE FOREIGN TABLE ft (id int8) SERVER testserver");
            let result =
                run_sql("CREATE FOREIGN TABLE IF NOT EXISTS ft (id int8) SERVER testserver");
            assert_eq!(result.command_tag, "CREATE FOREIGN TABLE");
        });
    }

    #[test]
    fn explain_foreign_scan() {
        with_isolated_state(|| {
            with_fdw_write(|reg| {
                reg.register_wrapper(Arc::new(TestFdw { rows: vec![] }));
            });
            run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
            run_sql("CREATE FOREIGN TABLE ft (id int8) SERVER testserver");

            let result = run_sql("EXPLAIN SELECT * FROM ft");
            let output = result.rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>();
            let has_foreign_scan = output.iter().any(|v| {
                if let ScalarValue::Text(s) = v {
                    s.contains("Foreign Scan") || s.contains("Seq Scan")
                } else {
                    false
                }
            });
            assert!(
                has_foreign_scan,
                "EXPLAIN should mention scan type, got {:?}",
                output
            );
        });
    }

    /// Helper: set up a foreign table with 3 rows for WHERE-filter tests.
    /// Rows: [("hello",""), (NULL,"world"), ("","test")]
    fn setup_filter_test_table() {
        with_fdw_write(|reg| {
            reg.register_wrapper(Arc::new(TestFdw {
                rows: vec![
                    vec![
                        ScalarValue::Text("hello".to_string()),
                        ScalarValue::Text(String::new()),
                    ],
                    vec![ScalarValue::Null, ScalarValue::Text("world".to_string())],
                    vec![
                        ScalarValue::Text(String::new()),
                        ScalarValue::Text("test".to_string()),
                    ],
                ],
            }));
        });
        run_sql("CREATE SERVER testserver FOREIGN DATA WRAPPER test_fdw");
        run_sql("CREATE FOREIGN TABLE ft (a text, b text) SERVER testserver");
    }

    #[test]
    fn foreign_scan_where_equality() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE a = 'hello'");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], ScalarValue::Text("hello".to_string()));
            assert_eq!(result.rows[0][1], ScalarValue::Text(String::new()));
        });
    }

    #[test]
    fn foreign_scan_where_empty_string() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE b = ''");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], ScalarValue::Text("hello".to_string()));
            assert_eq!(result.rows[0][1], ScalarValue::Text(String::new()));
        });
    }

    #[test]
    fn foreign_scan_where_inequality() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE b != ''");
            assert_eq!(result.rows.len(), 2);
        });
    }

    #[test]
    fn foreign_scan_where_is_null() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE a IS NULL");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], ScalarValue::Null);
            assert_eq!(result.rows[0][1], ScalarValue::Text("world".to_string()));
        });
    }

    #[test]
    fn foreign_scan_where_is_not_null() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE a IS NOT NULL");
            assert_eq!(result.rows.len(), 2);
        });
    }

    #[test]
    fn foreign_scan_where_compound() {
        with_isolated_state(|| {
            setup_filter_test_table();
            let result = run_sql("SELECT * FROM ft WHERE a IS NOT NULL AND b != ''");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], ScalarValue::Text(String::new()));
            assert_eq!(result.rows[0][1], ScalarValue::Text("test".to_string()));
        });
    }
}
