use openassay::tcop::engine::{restore_state, snapshot_state};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};
use std::fs;
use std::path::Path;

use crate::regression_lock;

fn load_corpus() -> Vec<(String, String)> {
    let mut files = fs::read_dir(Path::new("tests/regression/corpus"))
        .expect("read regression corpus dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .to_string();
            let sql = fs::read_to_string(&path).expect("read regression corpus file");
            (name, sql)
        })
        .collect()
}

fn parse_expected_rows(sql: &str) -> Vec<Vec<String>> {
    sql.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            trimmed.strip_prefix("-- expect:").map(|expected| {
                let expected = expected.trim();
                if expected.is_empty() {
                    Vec::new()
                } else {
                    expected
                        .split('|')
                        .map(|value| value.trim().to_string())
                        .collect::<Vec<_>>()
                }
            })
        })
        .collect()
}

fn run_fixture(sql: &str) -> Vec<Vec<String>> {
    let mut session = PostgresSession::new();
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "regression corpus produced an error response: {out:?}"
    );
    out.iter()
        .filter_map(|msg| match msg {
            BackendMessage::DataRow { values } => Some(values.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn regression_corpus_suite() {
    let _guard = regression_lock().lock().unwrap_or_else(|e| e.into_inner());

    // Force catalog initialization with a dummy query, then snapshot.
    let mut warmup = PostgresSession::new();
    warmup.run_sync([FrontendMessage::Query {
        sql: "SELECT 1".to_string(),
    }]);
    let clean = snapshot_state();

    for (name, sql) in load_corpus() {
        // Restore to warm-but-clean state before each fixture file
        restore_state(clean.clone());

        let expected = parse_expected_rows(&sql);
        assert!(
            !expected.is_empty(),
            "regression corpus file {name} has no expected rows"
        );
        let rows = run_fixture(&sql);
        assert_eq!(rows, expected, "regression corpus mismatch in {name}");
    }

    // Restore clean state after all fixtures
    restore_state(clean);
}
