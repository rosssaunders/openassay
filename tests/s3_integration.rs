//! S3 integration tests — require a running MinIO instance.
//!
//! These tests are `#[ignore]`d by default. Run them with:
//!
//! ```sh
//! docker compose -f docker-compose.s3.yml up -d
//! scripts/setup_s3_test_bucket.sh
//! cargo test --test s3_integration -- --ignored
//! docker compose -f docker-compose.s3.yml down
//! ```
//!
//! The CI `integration` job handles this automatically.

#![cfg(not(target_arch = "wasm32"))]

mod iceberg_fixtures;

use iceberg_fixtures::IcebergFixtureSet;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use openassay::parser::sql_parser::parse_statement;
use openassay::storage::tuple::ScalarValue;
use openassay::tcop::engine::{QueryResult, execute_planned_query, plan_statement};
use std::future::Future;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

const BUCKET: &str = "openassay-test";

fn s3_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    let _guard = s3_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    f()
}

fn block_on<F: Future<Output = T>, T>(f: F) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn run_statement(sql: &str) -> QueryResult {
    block_on(async {
        let stmt = parse_statement(sql).expect("parse should succeed");
        let planned = plan_statement(stmt).expect("plan should succeed");
        execute_planned_query(&planned, &[])
            .await
            .expect("query should succeed")
    })
}

/// Build an S3 object store pointing at the local MinIO instance.
fn make_s3_store() -> Box<dyn ObjectStore> {
    Box::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(BUCKET)
            .build()
            .expect("S3 store should build from env"),
    )
}

/// Recursively upload a local directory to S3 under `prefix`.
async fn upload_dir(store: &dyn ObjectStore, local_root: &Path, prefix: &str) {
    let mut stack = vec![(local_root.to_path_buf(), prefix.to_string())];
    while let Some((dir, s3_prefix)) = stack.pop() {
        let entries = std::fs::read_dir(&dir).expect("directory should be readable");
        for entry in entries {
            let entry = entry.expect("dir entry should be readable");
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let s3_key = if s3_prefix.is_empty() {
                name.clone()
            } else {
                format!("{s3_prefix}/{name}")
            };
            if path.is_dir() {
                stack.push((path, s3_key));
            } else {
                let bytes = std::fs::read(&path).expect("file should be readable");
                store
                    .put(&ObjectPath::from(s3_key.as_str()), bytes.into())
                    .await
                    .expect("S3 put should succeed");
            }
        }
    }
}

/// Upload the fixture set to MinIO and return the S3 prefix for each table.
struct S3Fixtures {
    _local: IcebergFixtureSet,
    pub deribit_trades: String,
    pub deribit_ohlcv: String,
    pub raw_parquet_file: String,
}

impl S3Fixtures {
    fn create() -> Self {
        let local = IcebergFixtureSet::create().expect("fixtures should create");
        let store = make_s3_store();

        block_on(async {
            upload_dir(&*store, &local.deribit_trades, "deribit_trades").await;
            upload_dir(&*store, &local.deribit_ohlcv, "deribit_ohlcv").await;

            // Upload a single standalone parquet file (first trades data file)
            let data_dir = local.deribit_trades.join("data");
            let first_parquet = std::fs::read_dir(&data_dir)
                .expect("data dir should exist")
                .filter_map(Result::ok)
                .find(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
                .expect("should have at least one parquet file");
            let bytes =
                std::fs::read(first_parquet.path()).expect("parquet file should be readable");
            store
                .put(&ObjectPath::from("standalone/trades.parquet"), bytes.into())
                .await
                .expect("standalone parquet upload should succeed");
        });

        Self {
            _local: local,
            deribit_trades: format!("s3://{BUCKET}/deribit_trades"),
            deribit_ohlcv: format!("s3://{BUCKET}/deribit_ohlcv"),
            raw_parquet_file: format!("s3://{BUCKET}/standalone/trades.parquet"),
        }
    }
}

// ── iceberg_scan over S3 ──

#[test]
#[ignore]
fn s3_iceberg_scan_returns_rows() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!("SELECT * FROM iceberg_scan('{}')", fixtures.deribit_trades);
        let result = run_statement(&sql);
        assert!(
            result.rows.len() >= 100,
            "expected 100+ trade rows from S3, got {}",
            result.rows.len()
        );
        assert_eq!(result.columns.len(), 8);
    });
}

#[test]
#[ignore]
fn s3_iceberg_scan_with_where_clause() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT instrument, price FROM iceberg_scan('{}') WHERE instrument = 'BTC-PERPETUAL' AND price > 62000",
            fixtures.deribit_trades
        );
        let result = run_statement(&sql);
        assert!(!result.rows.is_empty(), "expected filtered rows from S3");
        for row in &result.rows {
            assert_eq!(row[0], ScalarValue::Text("BTC-PERPETUAL".to_string()));
            if let ScalarValue::Float(p) = &row[1] {
                assert!(*p > 62_000.0);
            }
        }
    });
}

#[test]
#[ignore]
fn s3_iceberg_scan_with_aggregation() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT instrument, count(*), avg(price) FROM iceberg_scan('{}') GROUP BY instrument ORDER BY instrument",
            fixtures.deribit_trades
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 2);
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
#[ignore]
fn s3_iceberg_scan_ohlcv() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!("SELECT * FROM iceberg_scan('{}')", fixtures.deribit_ohlcv);
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 60, "expected 60 OHLCV rows from S3");
        assert_eq!(result.columns.len(), 6);
    });
}

#[test]
#[ignore]
fn s3_iceberg_metadata() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT format_version, total_data_files FROM iceberg_metadata('{}')",
            fixtures.deribit_trades
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], ScalarValue::Int(2));
        assert_eq!(result.rows[0][1], ScalarValue::Int(2));
    });
}

// ── parquet_scan over S3 ──

#[test]
#[ignore]
fn s3_parquet_scan_single_file() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT * FROM parquet_scan('{}') LIMIT 5",
            fixtures.raw_parquet_file
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 5);
        assert_eq!(result.columns.len(), 8);
    });
}

#[test]
#[ignore]
fn s3_parquet_scan_with_filter() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT instrument, price FROM parquet_scan('{}') WHERE price > 62000 ORDER BY price LIMIT 3",
            fixtures.raw_parquet_file
        );
        let result = run_statement(&sql);
        assert!(result.rows.len() <= 3);
        for row in &result.rows {
            if let ScalarValue::Float(p) = &row[1] {
                assert!(*p > 62_000.0);
            }
        }
    });
}

// ── Cross-function join over S3 ──

#[test]
#[ignore]
fn s3_cross_table_join() {
    with_isolated_state(|| {
        let fixtures = S3Fixtures::create();
        let sql = format!(
            "SELECT count(*) FROM iceberg_scan('{}') AS t JOIN iceberg_scan('{}') AS o ON t.price BETWEEN o.low AND o.high",
            fixtures.deribit_trades, fixtures.deribit_ohlcv
        );
        let result = run_statement(&sql);
        assert_eq!(result.rows.len(), 1);
        if let ScalarValue::Int(n) = &result.rows[0][0] {
            assert!(*n > 0, "join should produce at least one row");
        }
    });
}
