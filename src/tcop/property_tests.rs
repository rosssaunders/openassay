use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
use crate::tcop::engine::reset_global_storage_for_tests;
use crate::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn numeric_expr_strategy() -> impl Strategy<Value = String> {
    let leaf = any::<i16>().prop_map(|value| value.to_string());
    leaf.prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a} + {b})")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a} - {b})")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a} * {b})")),
            inner.clone().prop_map(|expr| format!("abs({expr})")),
            inner.clone().prop_map(|expr| format!("round({expr})")),
        ]
    })
}

fn run_query(session: &mut PostgresSession, sql: &str) -> Vec<Vec<String>> {
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "property SQL produced error: {out:?}"
    );
    out.iter()
        .filter_map(|msg| match msg {
            BackendMessage::DataRow { values } => Some(values.clone()),
            _ => None,
        })
        .collect()
}

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        reset_global_storage_for_tests();
        f()
    })
}

fn unique_table_name(prefix: &str) -> String {
    let id = TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn random_expressions_do_not_panic(expr in numeric_expr_strategy()) {
        with_isolated_state(|| {
            let mut session = PostgresSession::new();
            let sql = format!("SELECT {expr}");
            let out = session.run_sync([FrontendMessage::Query { sql }]);
            prop_assert!(
                !out.iter()
                    .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
                "unexpected error response: {out:?}"
            );
            Ok(())
        })?;
    }

    #[test]
    fn insert_then_select_returns_row(id in any::<i16>(), name in "[a-z]{1,8}") {
        with_isolated_state(|| {
            let mut session = PostgresSession::new();
            let expected_name = name.clone();
            let table_name = unique_table_name("prop_items");
            run_query(
                &mut session,
                &format!("CREATE TABLE {table_name} (id int8, name text)"),
            );
            run_query(
                &mut session,
                &format!("INSERT INTO {table_name} VALUES ({id}, '{name}')"),
            );
            let rows = run_query(&mut session, &format!("SELECT id, name FROM {table_name}"));
            prop_assert_eq!(rows, vec![vec![id.to_string(), expected_name]]);
            Ok(())
        })?;
    }

    #[test]
    fn transaction_rollback_restores_state(id in any::<i16>()) {
        with_isolated_state(|| {
            let mut session = PostgresSession::new();
            let rollback_id = id.wrapping_add(1);
            let table_name = unique_table_name("prop_txn");
            run_query(&mut session, &format!("CREATE TABLE {table_name} (id int8)"));
            run_query(&mut session, &format!("INSERT INTO {table_name} VALUES ({id})"));
            run_query(&mut session, "BEGIN");
            run_query(
                &mut session,
                &format!("INSERT INTO {table_name} VALUES ({rollback_id})"),
            );
            run_query(&mut session, "ROLLBACK");
            let rows = run_query(&mut session, &format!("SELECT count(*) FROM {table_name}"));
            prop_assert_eq!(rows, vec![vec!["1".to_string()]]);
            Ok(())
        })?;
    }
}
