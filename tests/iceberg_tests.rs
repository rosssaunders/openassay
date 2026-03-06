#![cfg(not(target_arch = "wasm32"))]

mod iceberg_fixtures;

use iceberg_fixtures::IcebergFixtureSet;
use openassay::parser::ast::{QueryExpr, Statement};
use openassay::parser::sql_parser::parse_statement;
use openassay::storage::tuple::ScalarValue;
use openassay::tcop::engine::{EngineError, QueryResult, execute_planned_query, plan_statement};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

fn iceberg_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    let _guard = iceberg_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f()
}

fn run_statement_result(sql: &str) -> Result<QueryResult, EngineError> {
    block_on(async {
        let stmt = parse_statement(sql).expect("parse should succeed");
        let planned = plan_statement(stmt).expect("plan should succeed");
        execute_planned_query(&planned, &[]).await
    })
}

fn run_statement(sql: &str) -> QueryResult {
    run_statement_result(sql).expect("query should succeed")
}

fn block_on<F: Future<Output = T>, T>(f: F) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn sql_path_literal(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

struct FixtureGuard {
    fixtures: IcebergFixtureSet,
}

impl FixtureGuard {
    fn create() -> Self {
        Self {
            fixtures: IcebergFixtureSet::create().expect("fixtures should create"),
        }
    }
}

// ── Basic scan tests ──

#[test]
fn iceberg_scan_trades_returns_rows() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT * FROM iceberg_scan('{}')",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert!(
            result.rows.len() >= 100,
            "expected 100+ trade rows, got {}",
            result.rows.len()
        );
        assert_eq!(result.columns.len(), 8); // timestamp, instrument, price, amount, direction, iv, index_price, trade_id
    });
}

#[test]
fn iceberg_scan_ohlcv_returns_rows() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT * FROM iceberg_scan('{}')",
            sql_path_literal(&guard.fixtures.deribit_ohlcv)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 60, "expected 60 OHLCV rows");
        assert_eq!(result.columns.len(), 6); // tick, open, high, low, close, volume
    });
}

#[test]
fn iceberg_scan_options_returns_rows() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT * FROM iceberg_scan('{}')",
            sql_path_literal(&guard.fixtures.deribit_options)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 40, "expected 40 option rows");
        assert_eq!(result.columns.len(), 12);
    });
}

// ── WHERE / ORDER BY / LIMIT ──

#[test]
fn iceberg_scan_with_where_clause() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT instrument, price, amount FROM iceberg_scan('{}') WHERE instrument = 'BTC-PERPETUAL' AND price > 62000",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert!(result.rows.len() > 0, "expected filtered trade rows");
        for row in &result.rows {
            assert_eq!(row[0], ScalarValue::Text("BTC-PERPETUAL".to_string()));
            if let ScalarValue::Float(p) = &row[1] {
                assert!(*p > 62_000.0);
            }
        }
    });
}

#[test]
fn iceberg_scan_with_order_by_limit() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT price FROM iceberg_scan('{}') WHERE instrument = 'BTC-PERPETUAL' ORDER BY price DESC LIMIT 5",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 5);
        // Verify descending order
        for i in 0..4 {
            let a = match &result.rows[i][0] {
                ScalarValue::Float(v) => *v,
                _ => panic!("expected float"),
            };
            let b = match &result.rows[i + 1][0] {
                ScalarValue::Float(v) => *v,
                _ => panic!("expected float"),
            };
            assert!(a >= b, "expected descending order: {a} >= {b}");
        }
    });
}

// ── Aggregation ──

#[test]
fn iceberg_scan_with_count() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT count(*) FROM iceberg_scan('{}')",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 1);
        if let ScalarValue::Int(n) = &result.rows[0][0] {
            assert!(*n >= 100, "expected 100+ rows, got {n}");
        }
    });
}

#[test]
fn iceberg_scan_with_group_by() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT instrument, count(*), avg(price) FROM iceberg_scan('{}') GROUP BY instrument ORDER BY instrument",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 2); // BTC-PERPETUAL, ETH-PERPETUAL
        assert_eq!(
            result.rows[0][0],
            ScalarValue::Text("BTC-PERPETUAL".to_string())
        );
        assert_eq!(
            result.rows[1][0],
            ScalarValue::Text("ETH-PERPETUAL".to_string())
        );
    });
}

#[test]
fn iceberg_scan_vwap() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT instrument, sum(price * amount) / sum(amount) AS vwap FROM iceberg_scan('{}') GROUP BY instrument ORDER BY instrument",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 2);
        // BTC VWAP should be in 60k range
        if let ScalarValue::Float(vwap) = &result.rows[0][1] {
            assert!(
                *vwap > 55_000.0 && *vwap < 70_000.0,
                "BTC VWAP out of range: {vwap}"
            );
        }
    });
}

#[test]
fn iceberg_scan_buy_sell_ratio() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT direction, count(*), sum(amount) FROM iceberg_scan('{}') GROUP BY direction ORDER BY direction",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert!(result.rows.len() >= 2, "expected buy and sell groups");
    });
}

// ── JOIN ──

#[test]
fn iceberg_scan_cross_table_join() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT count(*) FROM iceberg_scan('{}') AS t JOIN iceberg_scan('{}') AS o ON t.price BETWEEN o.low AND o.high",
            sql_path_literal(&guard.fixtures.deribit_trades),
            sql_path_literal(&guard.fixtures.deribit_ohlcv)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 1);
    });
}

// ── Schema evolution ──

#[test]
fn iceberg_scan_multi_schema_reads_all_files() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT count(*) FROM iceberg_scan('{}')",
            sql_path_literal(&guard.fixtures.multi_schema)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 1);
        if let ScalarValue::Int(n) = &result.rows[0][0] {
            assert_eq!(*n, 8, "expected 5 + 3 = 8 rows from multi-schema");
        }
    });
}

#[test]
fn iceberg_scan_multi_schema_column_union() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT id, value FROM iceberg_scan('{}') ORDER BY id",
            sql_path_literal(&guard.fixtures.multi_schema)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 8);
        // First row should have id=1
        assert_eq!(result.rows[0][0], ScalarValue::Int(1));
        assert_eq!(result.rows[0][1], ScalarValue::Int(10));
    });
}

#[test]
fn iceberg_scan_evolved_schema_uses_current_column_mapping() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT * FROM iceberg_scan('{}') ORDER BY id",
            sql_path_literal(&guard.fixtures.evolved_schema)
        );
        let result = run_statement(&sql);
        assert_eq!(result.columns, vec!["id", "amount", "updated_at"]);
        assert_eq!(result.rows.len(), 5);
        assert_eq!(result.rows[0][0], ScalarValue::Int(1));
        assert_eq!(result.rows[0][1], ScalarValue::Float(10.0));
        assert_eq!(result.rows[0][2], ScalarValue::Null);
        assert_eq!(result.rows[3][0], ScalarValue::Int(4));
        assert_eq!(result.rows[3][1], ScalarValue::Float(40.5));
        assert_eq!(result.rows[3][2], ScalarValue::Int(1_700_000_001_000));
    });
}

#[test]
fn iceberg_partition_pruning_reduces_scanned_files() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let predicate = match parse_statement("SELECT 1 WHERE day = '2024-01-02'")
            .expect("predicate statement should parse")
        {
            Statement::Query(query) => match query.body {
                QueryExpr::Select(select) => {
                    select.where_clause.expect("where clause should exist")
                }
                other => panic!("expected select query, got {other:?}"),
            },
            other => panic!("expected query statement, got {other:?}"),
        };
        let plan = block_on(
            openassay::catalog::iceberg::scan_iceberg_table_with_predicate(
                &sql_path_literal(&guard.fixtures.partitioned_trades),
                Some(&predicate),
                &["iceberg_scan".to_string()],
                &[],
            ),
        )
        .expect("partitioned scan should succeed");
        assert_eq!(plan.scanned_files, 1);
        assert_eq!(plan.pruned_files, 1);
        assert_eq!(plan.rows.len(), 2);
    });
}

#[test]
fn browser_catalog_browse_lists_iceberg_catalogs_and_tables() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let payload = block_on(openassay::browser::browse_catalog_json(&sql_path_literal(
            &guard.fixtures.catalog_browser_root,
        )));
        let json: JsonValue = serde_json::from_str(&payload).expect("browse payload should parse");
        assert_eq!(json.get("ok"), Some(&JsonValue::Bool(true)));
        let catalogs = json["catalogs"]
            .as_array()
            .expect("catalogs array expected");
        assert_eq!(catalogs.len(), 2);
        assert_eq!(catalogs[0]["name"], JsonValue::String("alpha".to_string()));
        assert_eq!(catalogs[1]["name"], JsonValue::String("beta".to_string()));

        let alpha_namespaces = catalogs[0]["namespaces"]
            .as_array()
            .expect("alpha namespaces expected");
        assert!(alpha_namespaces.iter().any(|namespace| {
            namespace["tables"].as_array().is_some_and(|tables| {
                tables
                    .iter()
                    .any(|table| table["name"] == JsonValue::String("trades".to_string()))
            })
        }));
        assert!(alpha_namespaces.iter().any(|namespace| {
            namespace["tables"].as_array().is_some_and(|tables| {
                tables
                    .iter()
                    .any(|table| table["name"] == JsonValue::String("options".to_string()))
            })
        }));
    });
}

// ── Error cases ──

#[test]
fn iceberg_scan_nonexistent_path_errors() {
    with_isolated_state(|| {
        let missing = std::env::temp_dir().join("openassay-no-such-iceberg-table");
        let sql = format!(
            "SELECT * FROM iceberg_scan('{}')",
            sql_path_literal(&missing)
        );
        let err = run_statement_result(&sql).expect_err("query should fail");
        assert!(
            err.message.contains("no parquet files found")
                || err.message.contains("not found")
                || err.message.contains("does not exist")
                || err.message.contains("expects an existing"),
            "unexpected error: {}",
            err.message
        );
    });
}

#[test]
fn iceberg_scan_empty_directory_errors() {
    with_isolated_state(|| {
        let empty_dir =
            std::env::temp_dir().join(format!("openassay-empty-iceberg-{}", std::process::id()));
        if empty_dir.exists() {
            std::fs::remove_dir_all(&empty_dir).ok();
        }
        std::fs::create_dir_all(&empty_dir).expect("empty dir should create");

        let sql = format!(
            "SELECT * FROM iceberg_scan('{}')",
            sql_path_literal(&empty_dir)
        );
        let err = run_statement_result(&sql).expect_err("query should fail");
        assert!(
            err.message.contains("no parquet files found"),
            "unexpected error: {}",
            err.message
        );

        std::fs::remove_dir_all(&empty_dir).ok();
    });
}

// ── Options table queries ──

#[test]
fn iceberg_scan_options_greeks_query() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT instrument, strike, option_type, delta, gamma, vega, theta FROM iceberg_scan('{}') ORDER BY strike LIMIT 10",
            sql_path_literal(&guard.fixtures.deribit_options)
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.columns.len(), 7);
    });
}

// ── iceberg_metadata ──

#[test]
fn iceberg_metadata_returns_table_info() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let sql = format!(
            "SELECT * FROM iceberg_metadata('{}')",
            sql_path_literal(&guard.fixtures.deribit_trades)
        );
        let result = run_statement(&sql);
        assert_eq!(
            result.columns,
            vec![
                "table_uuid",
                "format_version",
                "last_updated",
                "current_schema_id",
                "partition_spec",
                "snapshot_count",
                "total_data_files"
            ]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], ScalarValue::Int(2));
        assert_eq!(result.rows[0][3], ScalarValue::Int(0));
        assert_eq!(result.rows[0][5], ScalarValue::Int(0));
        assert_eq!(result.rows[0][6], ScalarValue::Int(2));
    });
}

#[test]
fn iceberg_metadata_from_metadata_file() {
    with_isolated_state(|| {
        let guard = FixtureGuard::create();
        let metadata_file = guard
            .fixtures
            .deribit_trades
            .join("metadata/v1.metadata.json");
        let sql = format!(
            "SELECT format_version, total_data_files FROM iceberg_metadata('{}')",
            sql_path_literal(&metadata_file)
        );
        let result = run_statement(&sql);
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(2), ScalarValue::Int(2)]]
        );
    });
}
