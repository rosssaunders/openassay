//! Integration tests for the analyzer module.

use crate::analyzer::analyze;
use crate::catalog::{
    ColumnSpec, SearchPath, TableKind, TypeSignature, reset_global_catalog_for_tests,
    with_catalog_write, with_global_state_lock,
};
use crate::parser::ast::*;

fn default_search_path() -> SearchPath {
    SearchPath::default()
}

/// Helper: create a test table in the catalog.
fn setup_test_table(schema: &str, name: &str, columns: Vec<(&str, TypeSignature)>) {
    with_catalog_write(|catalog| {
        let col_specs: Vec<ColumnSpec> = columns
            .into_iter()
            .map(|(col_name, type_sig)| ColumnSpec::new(col_name, type_sig))
            .collect();
        let _ = catalog.create_table(schema, name, TableKind::Heap, col_specs, vec![], vec![]);
    });
}

// ── Table resolution tests ──────────────────────────────────────────────

#[test]
fn test_analyze_select_from_existing_table() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        setup_test_table(
            "public",
            "users",
            vec![("id", TypeSignature::Int8), ("name", TypeSignature::Text)],
        );

        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![TableExpression::Relation(TableRef {
                    name: vec!["users".to_string()],
                    alias: None,
                })],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        assert!(analyze(&stmt, &default_search_path()).is_ok());
    });
}

#[test]
fn test_analyze_select_from_nonexistent_table_deferred() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        // Table existence is deferred to executor, so analyze succeeds
        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![TableExpression::Relation(TableRef {
                    name: vec!["nonexistent".to_string()],
                    alias: None,
                })],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        assert!(analyze(&stmt, &default_search_path()).is_ok());
    });
}

// ── SELECT without FROM (allowed) ───────────────────────────────────────

#[test]
fn test_analyze_select_without_from() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Integer(1),
                    alias: None,
                }],
                from: vec![],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        assert!(analyze(&stmt, &default_search_path()).is_ok());
    });
}

// ── INSERT target validation ────────────────────────────────────────────

#[test]
fn test_analyze_insert_deferred_table_check() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        // Table existence for INSERT is deferred to executor
        let stmt = Statement::Insert(InsertStatement {
            table_name: vec!["ghost_table".to_string()],
            table_alias: None,
            columns: vec![],
            source: InsertSource::Values(vec![vec![Expr::Integer(1)]]),
            on_conflict: None,
            returning: vec![],
        });

        assert!(analyze(&stmt, &default_search_path()).is_ok());
    });
}

// ── WHERE clause type checking ──────────────────────────────────────────

#[test]
fn test_analyze_where_string_literal_rejected() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        setup_test_table("public", "t", vec![("x", TypeSignature::Int8)]);

        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![TableExpression::Relation(TableRef {
                    name: vec!["t".to_string()],
                    alias: None,
                })],
                where_clause: Some(Expr::String("not boolean".to_string())),
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        let result = analyze(&stmt, &default_search_path());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("must be type boolean"));
    });
}

// ── LIMIT validation ────────────────────────────────────────────────────

#[test]
fn test_analyze_limit_non_numeric_string() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Integer(1),
                    alias: None,
                }],
                from: vec![],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: Some(Expr::String("abc".to_string())),
            offset: None,
        });

        let result = analyze(&stmt, &default_search_path());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("invalid input syntax"));
    });
}

// ── CTE handling ────────────────────────────────────────────────────────

#[test]
fn test_analyze_cte_reference() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        let stmt = Statement::Query(Query {
            with: Some(WithClause {
                recursive: false,
                ctes: vec![CommonTableExpr {
                    name: "my_cte".to_string(),
                    column_names: vec![],
                    materialized: None,
                    query: Query {
                        with: None,
                        body: QueryExpr::Select(SelectStatement {
                            quantifier: None,
                            distinct_on: vec![],
                            targets: vec![SelectItem {
                                expr: Expr::Integer(1),
                                alias: Some("x".to_string()),
                            }],
                            from: vec![],
                            where_clause: None,
                            group_by: vec![],
                            having: None,
                            window_definitions: Vec::new(),
                        }),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                    search_clause: None,
                    cycle_clause: None,
                }],
            }),
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![TableExpression::Relation(TableRef {
                    name: vec!["my_cte".to_string()],
                    alias: None,
                })],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        // CTE reference should not cause "table does not exist"
        assert!(analyze(&stmt, &default_search_path()).is_ok());
    });
}

// ── DDL passes through ─────────────────────────────────────────────────

#[test]
fn test_analyze_ddl_passes_through() {
    let stmt = Statement::CreateSchema(CreateSchemaStatement {
        name: "test_schema".to_string(),
        if_not_exists: false,
    });
    assert!(analyze(&stmt, &default_search_path()).is_ok());
}

// ── Function arg count validation in SELECT ─────────────────────────────

#[test]
fn test_analyze_function_wrong_args_in_select() {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();

        let stmt = Statement::Query(Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: vec![],
                targets: vec![SelectItem {
                    expr: Expr::FunctionCall {
                        name: vec!["length".to_string()],
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                        within_group: vec![],
                        filter: None,
                        over: None,
                    },
                    alias: None,
                }],
                from: vec![],
                where_clause: None,
                group_by: vec![],
                having: None,
                window_definitions: Vec::new(),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        let result = analyze(&stmt, &default_search_path());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("requires 1 argument"));
    });
}
