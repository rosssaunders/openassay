#![cfg(not(target_arch = "wasm32"))]

mod iceberg_fixtures;

use iceberg_fixtures::IcebergFixtureSet;
use openassay::parser::sql_parser::parse_statement;
use openassay::storage::tuple::ScalarValue;
use openassay::tcop::engine::{EngineError, QueryResult, execute_planned_query, plan_statement};
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
        assert!(result.rows.len() >= 100, "expected 100+ trade rows, got {}", result.rows.len());
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
            let a = match &result.rows[i][0] { ScalarValue::Float(v) => *v, _ => panic!("expected float") };
            let b = match &result.rows[i + 1][0] { ScalarValue::Float(v) => *v, _ => panic!("expected float") };
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
        assert_eq!(result.rows[0][0], ScalarValue::Text("BTC-PERPETUAL".to_string()));
        assert_eq!(result.rows[1][0], ScalarValue::Text("ETH-PERPETUAL".to_string()));
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
            assert!(*vwap > 55_000.0 && *vwap < 70_000.0, "BTC VWAP out of range: {vwap}");
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
        let empty_dir = std::env::temp_dir().join(format!(
            "openassay-empty-iceberg-{}",
            std::process::id()
        ));
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
        let metadata_file = guard.fixtures.deribit_trades.join("metadata/v1.metadata.json");
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
