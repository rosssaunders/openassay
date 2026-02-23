use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[derive(Debug)]
struct Fixture {
    sql: &'static str,
    expected_rows: Vec<Vec<&'static str>>,
}

fn run_fixture(sql: &str) -> Vec<Vec<String>> {
    let mut session = PostgresSession::new();
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "fixture returned an error: {out:?}"
    );
    out.iter()
        .filter_map(|msg| match msg {
            BackendMessage::DataRow { values } => Some(values.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn differential_fixture_suite() {
    let fixtures = vec![
        Fixture {
            sql: "SELECT 1 + 2",
            expected_rows: vec![vec!["3"]],
        },
        Fixture {
            sql: "SELECT id FROM (SELECT 2 AS id UNION SELECT 1 AS id) t ORDER BY 1",
            expected_rows: vec![vec!["1"], vec!["2"]],
        },
        Fixture {
            sql: "CREATE TABLE t_diff (id int8, owner text); INSERT INTO t_diff VALUES (1, 'a'), (2, 'b'); SELECT owner FROM t_diff WHERE id = 2",
            expected_rows: vec![vec!["b"]],
        },
        Fixture {
            sql: "SELECT length('openassay')",
            expected_rows: vec![vec!["9"]],
        },
        Fixture {
            sql: "SELECT abs(-12)",
            expected_rows: vec![vec!["12"]],
        },
        Fixture {
            sql: "SELECT to_char(make_date(2024, 1, 2), 'YYYY-MM-DD')",
            expected_rows: vec![vec!["2024-01-02"]],
        },
        Fixture {
            sql: "SELECT jsonb_array_length(jsonb_build_array(1, 2, 3))",
            expected_rows: vec![vec!["3"]],
        },
        Fixture {
            sql: "WITH t AS (SELECT 1 AS id UNION SELECT 2) SELECT count(*) FROM t",
            expected_rows: vec![vec!["2"]],
        },
        Fixture {
            sql: "CREATE TABLE t_join_a (id int8, val text); CREATE TABLE t_join_b (id int8, name text); INSERT INTO t_join_a VALUES (1, 'x'), (2, 'y'); INSERT INTO t_join_b VALUES (1, 'alpha'), (2, 'beta'); SELECT a.val, b.name FROM t_join_a a JOIN t_join_b b ON a.id = b.id ORDER BY val",
            expected_rows: vec![vec!["x", "alpha"], vec!["y", "beta"]],
        },
        Fixture {
            sql: "CREATE TABLE t_window (id int8, grp text); INSERT INTO t_window VALUES (1, 'g1'), (2, 'g1'), (3, 'g2'); SELECT id, row_number() OVER (PARTITION BY grp ORDER BY id) FROM t_window ORDER BY id",
            expected_rows: vec![vec!["1", "1"], vec!["2", "2"], vec!["3", "1"]],
        },
        Fixture {
            sql: "CREATE TABLE t_txn (id int8); INSERT INTO t_txn VALUES (1); BEGIN; INSERT INTO t_txn VALUES (2); ROLLBACK; SELECT count(*) FROM t_txn",
            expected_rows: vec![vec!["1"]],
        },
    ];

    for fixture in fixtures {
        let rows = run_fixture(fixture.sql);
        let expected = fixture
            .expected_rows
            .iter()
            .map(|row| row.iter().map(|v| v.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "fixture SQL: {}", fixture.sql);
    }
}
