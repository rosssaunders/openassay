use super::*;

fn as_select(query: &Query) -> &SelectStatement {
    match &query.body {
        QueryExpr::Select(select) => select,
        other => panic!("expected simple SELECT query body, got {other:?}"),
    }
}

#[test]
fn parses_simple_select() {
    let stmt = parse_statement("SELECT 1;").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 1);
    assert_eq!(select.targets[0].expr, Expr::Integer(1));
}

#[test]
fn parses_with_clause_query() {
    let stmt = parse_statement("WITH t AS (SELECT 1 AS id) SELECT id FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert!(!with.recursive);
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "t");
}

#[test]
fn parses_with_recursive_clause_query() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) SELECT id FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert!(with.recursive);
    assert_eq!(with.ctes.len(), 1);
}

#[test]
fn parses_with_cte_column_list() {
    let stmt = parse_statement("WITH t(a, b) AS (SELECT 1, 2) SELECT a, b FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "t");
    assert_eq!(with.ctes[0].column_names, vec!["a", "b"]);
    assert_eq!(with.ctes[0].materialized, None);
}

#[test]
fn parses_with_cte_materialized() {
    let stmt = parse_statement("WITH t AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "t");
    assert_eq!(with.ctes[0].materialized, Some(true));
}

#[test]
fn parses_with_cte_not_materialized() {
    let stmt = parse_statement("WITH t AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "t");
    assert_eq!(with.ctes[0].materialized, Some(false));
}

#[test]
fn parses_with_cte_column_list_and_materialized() {
    let stmt = parse_statement("WITH t(x, y) AS MATERIALIZED (SELECT 1, 2) SELECT x, y FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "t");
    assert_eq!(with.ctes[0].column_names, vec!["x", "y"]);
    assert_eq!(with.ctes[0].materialized, Some(true));
}

#[test]
fn parses_with_search_depth_first() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) \
         SEARCH DEPTH FIRST BY id SET ordercol \
         SELECT * FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    let search = with.ctes[0]
        .search_clause
        .as_ref()
        .expect("search clause should exist");
    assert!(search.depth_first);
    assert_eq!(search.by_columns, vec!["id"]);
    assert_eq!(search.set_column, "ordercol");
}

#[test]
fn parses_with_search_breadth_first() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id, 'a' AS name UNION ALL SELECT id + 1, name FROM t WHERE id < 3) \
         SEARCH BREADTH FIRST BY id, name SET seqcol \
         SELECT * FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    let search = with.ctes[0]
        .search_clause
        .as_ref()
        .expect("search clause should exist");
    assert!(!search.depth_first);
    assert_eq!(search.by_columns, vec!["id", "name"]);
    assert_eq!(search.set_column, "seqcol");
}

#[test]
fn parses_with_cycle_clause() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) \
         CYCLE id SET is_cycle USING path \
         SELECT * FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    let cycle = with.ctes[0]
        .cycle_clause
        .as_ref()
        .expect("cycle clause should exist");
    assert_eq!(cycle.columns, vec!["id"]);
    assert_eq!(cycle.set_column, "is_cycle");
    assert_eq!(cycle.using_column, "path");
    assert_eq!(cycle.mark_value, None);
    assert_eq!(cycle.default_value, None);
}

#[test]
fn parses_with_cycle_clause_with_values() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) \
         CYCLE id SET is_cycle TO 't' DEFAULT 'f' USING path \
         SELECT * FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    let cycle = with.ctes[0]
        .cycle_clause
        .as_ref()
        .expect("cycle clause should exist");
    assert_eq!(cycle.columns, vec!["id"]);
    assert_eq!(cycle.set_column, "is_cycle");
    assert_eq!(cycle.using_column, "path");
    assert_eq!(cycle.mark_value, Some("t".to_string()));
    assert_eq!(cycle.default_value, Some("f".to_string()));
}

#[test]
fn parses_with_search_and_cycle() {
    let stmt = parse_statement(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) \
         SEARCH DEPTH FIRST BY id SET ordercol \
         CYCLE id SET is_cycle USING path \
         SELECT * FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert!(with.ctes[0].search_clause.is_some());
    assert!(with.ctes[0].cycle_clause.is_some());
}

#[test]
fn parses_with_insert_returning() {
    let stmt = parse_statement(
        "WITH inserted AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM inserted",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert_eq!(with.ctes.len(), 1);
    assert_eq!(with.ctes[0].name, "inserted");
    assert!(matches!(&with.ctes[0].query.body, QueryExpr::Insert(_)));
}

#[test]
fn parses_with_update_returning() {
    let stmt = parse_statement(
        "WITH updated AS (UPDATE t SET x = x + 1 RETURNING *) SELECT * FROM updated",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert!(matches!(&with.ctes[0].query.body, QueryExpr::Update(_)));
}

#[test]
fn parses_with_delete_returning() {
    let stmt = parse_statement(
        "WITH deleted AS (DELETE FROM t WHERE x < 0 RETURNING *) SELECT * FROM deleted",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let with = query.with.as_ref().expect("with clause should exist");
    assert!(matches!(&with.ctes[0].query.body, QueryExpr::Delete(_)));
}

#[test]
fn parses_select_with_clauses() {
    let stmt = parse_statement(
        "SELECT DISTINCT foo AS bar, count(*) \
         FROM public.users u \
         WHERE id >= $1 AND active = true \
         GROUP BY foo \
         HAVING count(*) > 1 \
         ORDER BY foo DESC \
         LIMIT 10 OFFSET 20;",
    )
    .expect("parse should succeed");

    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.quantifier, Some(SelectQuantifier::Distinct));
    assert_eq!(select.targets.len(), 2);
    assert_eq!(select.from.len(), 1);
    assert!(select.where_clause.is_some());
    assert_eq!(select.group_by.len(), 1);
    assert!(select.having.is_some());
    assert_eq!(query.order_by.len(), 1);
    assert!(query.limit.is_some());
    assert!(query.offset.is_some());
}

#[test]
fn parses_joins_in_from_clause() {
    let stmt =
        parse_statement("SELECT u.id FROM users u INNER JOIN accounts a ON u.id = a.user_id")
            .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.from.len(), 1);
    match &select.from[0] {
        TableExpression::Join(join) => {
            assert_eq!(join.kind, JoinType::Inner);
            assert!(matches!(join.condition, Some(JoinCondition::On(_))));
        }
        other => panic!("expected join table expression, got {other:?}"),
    }
}

#[test]
fn parses_subquery_in_from_clause() {
    let stmt = parse_statement("SELECT sq.id FROM (SELECT id FROM users WHERE active = true) sq")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.from.len(), 1);
    match &select.from[0] {
        TableExpression::Subquery(sub) => {
            assert_eq!(sub.alias.as_deref(), Some("sq"));
            match &sub.query.body {
                QueryExpr::Select(inner) => assert_eq!(inner.targets.len(), 1),
                other => panic!("expected inner SELECT, got {other:?}"),
            }
        }
        other => panic!("expected subquery table expression, got {other:?}"),
    }
}

#[test]
fn parses_lateral_subquery_in_from_clause() {
    let stmt = parse_statement("SELECT t.id FROM test_table t, LATERAL (SELECT t.id AS id) l")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.from.len(), 2);
    match &select.from[1] {
        TableExpression::Subquery(sub) => {
            assert_eq!(sub.alias.as_deref(), Some("l"));
            assert!(sub.lateral);
        }
        other => panic!("expected lateral subquery table expression, got {other:?}"),
    }
}

#[test]
fn parses_function_call_in_from_clause() {
    let stmt = parse_statement("SELECT elem FROM json_array_elements('[1,2,3]') AS elem")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.from.len(), 1);
    match &select.from[0] {
        TableExpression::Function(function) => {
            assert_eq!(function.name, vec!["json_array_elements".to_string()]);
            assert_eq!(function.args.len(), 1);
            assert_eq!(function.alias.as_deref(), Some("elem"));
            assert!(function.column_aliases.is_empty());
            assert!(function.column_alias_types.is_empty());
        }
        other => panic!("expected function table expression, got {other:?}"),
    }
}

#[test]
fn parses_function_call_with_column_aliases_in_from_clause() {
    let stmt = parse_statement("SELECT x FROM json_array_elements('[1,2,3]') AS t(x)")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.from.len(), 1);
    match &select.from[0] {
        TableExpression::Function(function) => {
            assert_eq!(function.alias.as_deref(), Some("t"));
            assert_eq!(function.column_aliases, vec!["x".to_string()]);
            assert_eq!(function.column_alias_types, vec![None]);
        }
        other => panic!("expected function table expression, got {other:?}"),
    }
}

#[test]
fn parses_exists_predicate_subquery() {
    let stmt = parse_statement("SELECT 1 WHERE EXISTS (SELECT 1 FROM users)").expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    assert!(matches!(where_clause, Expr::Exists(_)));
}

#[test]
fn parses_array_constructors() {
    let stmt = parse_statement("SELECT ARRAY[1, 2, 3]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 1);
    assert!(matches!(select.targets[0].expr, Expr::ArrayConstructor(_)));

    let stmt = parse_statement("SELECT ARRAY(SELECT id FROM users)").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 1);
    assert!(matches!(select.targets[0].expr, Expr::ArraySubquery(_)));
}

#[test]
fn parses_in_subquery_predicate() {
    let stmt =
        parse_statement("SELECT 1 WHERE id NOT IN (SELECT id FROM users)").expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    match where_clause {
        Expr::InSubquery { negated, .. } => assert!(*negated),
        other => panic!("expected IN subquery predicate, got {other:?}"),
    }
}

#[test]
fn parses_scalar_subquery_expression() {
    let stmt = parse_statement("SELECT (SELECT 42)").expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(select.targets[0].expr, Expr::ScalarSubquery(_)));
}

#[test]
fn parses_is_null_predicates() {
    let stmt = parse_statement("SELECT 1 WHERE a IS NULL OR b IS NOT NULL").expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    match where_clause {
        Expr::Binary { left, op, right } => {
            assert_eq!(*op, BinaryOp::Or);
            assert!(matches!(left.as_ref(), Expr::IsNull { negated: false, .. }));
            assert!(matches!(right.as_ref(), Expr::IsNull { negated: true, .. }));
        }
        other => panic!("expected OR predicate, got {other:?}"),
    }
}

#[test]
fn parses_between_and_like_predicates() {
    let stmt = parse_statement(
        "SELECT 1 WHERE score BETWEEN 10 AND 20 AND name NOT LIKE 'a%' AND email ILIKE '%@x.com'",
    )
    .expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    let Expr::Binary { left, op, right } = where_clause else {
        panic!("expected AND tree");
    };
    assert_eq!(*op, BinaryOp::And);
    assert!(matches!(
        right.as_ref(),
        Expr::Like {
            case_insensitive: true,
            negated: false,
            ..
        }
    ));
    let Expr::Binary {
        left: inner_left,
        op: inner_op,
        right: inner_right,
    } = left.as_ref()
    else {
        panic!("expected inner AND");
    };
    assert_eq!(*inner_op, BinaryOp::And);
    assert!(matches!(
        inner_left.as_ref(),
        Expr::Between { negated: false, .. }
    ));
    assert!(matches!(
        inner_right.as_ref(),
        Expr::Like {
            case_insensitive: false,
            negated: true,
            ..
        }
    ));
}

#[test]
fn parses_operator_wrapper_regex_predicate() {
    let stmt =
        parse_statement("SELECT 1 WHERE relname OPERATOR(pg_catalog.~) '^foo$'").expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    match where_clause {
        Expr::FunctionCall { name, args, .. } => {
            assert_eq!(name, &vec!["regexp_like".to_string()]);
            assert_eq!(args.len(), 2);
        }
        other => panic!("expected regexp_like call, got {other:?}"),
    }
}

#[test]
fn parses_is_distinct_from_predicates() {
    let stmt = parse_statement("SELECT 1 WHERE a IS DISTINCT FROM b OR a IS NOT DISTINCT FROM c")
        .expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let where_clause = select
        .where_clause
        .as_ref()
        .expect("where clause should exist");
    let Expr::Binary { left, op, right } = where_clause else {
        panic!("expected OR expression");
    };
    assert_eq!(*op, BinaryOp::Or);
    assert!(matches!(
        left.as_ref(),
        Expr::IsDistinctFrom { negated: false, .. }
    ));
    assert!(matches!(
        right.as_ref(),
        Expr::IsDistinctFrom { negated: true, .. }
    ));
}

#[test]
fn parses_simple_case_expression() {
    let stmt =
        parse_statement("SELECT CASE level WHEN 1 THEN 'low' WHEN 2 THEN 'mid' ELSE 'high' END")
            .expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let Expr::CaseSimple {
        operand,
        when_then,
        else_expr,
    } = &select.targets[0].expr
    else {
        panic!("expected simple CASE expression");
    };
    assert!(
        matches!(operand.as_ref(), Expr::Identifier(parts) if parts == &vec!["level".to_string()])
    );
    assert_eq!(when_then.len(), 2);
    assert!(matches!(when_then[0].0, Expr::Integer(1)));
    assert!(matches!(when_then[0].1, Expr::String(ref value) if value == "low"));
    assert!(matches!(
        else_expr.as_deref(),
        Some(Expr::String(value)) if value == "high"
    ));
}

#[test]
fn parses_searched_and_nested_case_expression() {
    let stmt = parse_statement(
        "SELECT CASE WHEN score >= 90 THEN CASE WHEN bonus THEN 'A+' ELSE 'A' END WHEN score >= 80 THEN 'B' END",
    )
    .expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let Expr::CaseSearched {
        when_then,
        else_expr,
    } = &select.targets[0].expr
    else {
        panic!("expected searched CASE expression");
    };
    assert_eq!(when_then.len(), 2);
    assert!(matches!(
        when_then[0].1,
        Expr::CaseSearched {
            when_then: _,
            else_expr: _
        }
    ));
    assert!(else_expr.is_none());
}

#[test]
fn parses_cast_and_typecast_expressions() {
    let stmt = parse_statement(
        "SELECT CAST('1' AS int8), '2024-02-29'::date, CAST('2024-02-29 10:30:40' AS timestamp)",
    )
    .expect("parse ok");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 3);
    assert!(matches!(
        select.targets[0].expr,
        Expr::Cast { ref type_name, .. } if type_name == "int8"
    ));
    assert!(matches!(
        select.targets[1].expr,
        Expr::Cast { ref type_name, .. } if type_name == "date"
    ));
    assert!(matches!(
        select.targets[2].expr,
        Expr::Cast { ref type_name, .. } if type_name == "timestamp"
    ));
}

#[test]
fn parses_set_operations_with_precedence() {
    let stmt = parse_statement("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    match &query.body {
        QueryExpr::SetOperation { op, right, .. } => {
            assert_eq!(*op, SetOperator::Union);
            match right.as_ref() {
                QueryExpr::SetOperation { op, .. } => assert_eq!(*op, SetOperator::Intersect),
                other => panic!("expected INTERSECT on right side, got {other:?}"),
            }
        }
        other => panic!("expected set operation body, got {other:?}"),
    }
}

#[test]
fn parses_set_operation_with_query_modifiers() {
    let stmt = parse_statement("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5 OFFSET 2")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    assert_eq!(query.order_by.len(), 1);
    assert!(query.limit.is_some());
    assert!(query.offset.is_some());
}

#[test]
fn expression_precedence_matches_sql_expectation() {
    let stmt = parse_statement("SELECT 1 + 2 * 3 = 7 OR false;").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    match &select.targets[0].expr {
        Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::Or),
        other => panic!("expected OR expression, got {other:?}"),
    }
}

#[test]
fn parses_keyword_named_function_calls_in_expressions() {
    let stmt = parse_statement("SELECT left('abc', 2), right('abc', 1), replace('abc', 'a', 'x')")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 3);
    match &select.targets[0].expr {
        Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["left".to_string()]),
        other => panic!("expected function call, got {other:?}"),
    }
    match &select.targets[1].expr {
        Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["right".to_string()]),
        other => panic!("expected function call, got {other:?}"),
    }
    match &select.targets[2].expr {
        Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["replace".to_string()]),
        other => panic!("expected function call, got {other:?}"),
    }
}

#[test]
fn parse_fails_on_missing_target() {
    let err = parse_statement("SELECT FROM t").expect_err("parse should fail");
    assert!(err.message.contains("expected expression"));
}

#[test]
fn parses_create_table_statement() {
    let stmt =
        parse_statement("CREATE TABLE public.users (id int8 NOT NULL, name text, active boolean)")
            .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };

    assert_eq!(create.name, vec!["public".to_string(), "users".to_string()]);
    assert_eq!(create.columns.len(), 3);
    assert_eq!(create.columns[0].name, "id");
    assert_eq!(create.columns[0].data_type, TypeName::Int8);
    assert!(!create.columns[0].nullable);
    assert!(!create.columns[0].identity);
    assert!(!create.columns[0].primary_key);
    assert!(!create.columns[0].unique);
    assert!(create.columns[0].references.is_none());
    assert!(create.columns[0].check.is_none());
    assert!(create.columns[0].default.is_none());
    assert!(create.constraints.is_empty());
}

#[test]
fn parses_create_table_with_date_and_timestamp_types() {
    let stmt = parse_statement("CREATE TABLE events (event_day date, created_at timestamp)")
        .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };

    assert_eq!(create.columns.len(), 2);
    assert_eq!(create.columns[0].data_type, TypeName::Date);
    assert_eq!(create.columns[1].data_type, TypeName::Timestamp);
}

#[test]
fn parses_insert_values_statement() {
    let stmt = parse_statement("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')")
        .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };

    assert_eq!(insert.table_name, vec!["users".to_string()]);
    assert!(insert.table_alias.is_none());
    assert_eq!(insert.columns, vec!["id".to_string(), "name".to_string()]);
    let InsertSource::Values(values) = insert.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].len(), 2);
    assert!(insert.on_conflict.is_none());
    assert!(insert.returning.is_empty());
}

#[test]
fn parses_insert_with_returning() {
    let stmt = parse_statement("INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name")
        .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };
    assert!(insert.table_alias.is_none());
    assert_eq!(insert.returning.len(), 2);
    assert!(insert.on_conflict.is_none());
}

#[test]
fn parses_insert_with_on_conflict_do_nothing() {
    let stmt = parse_statement(
        "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id",
    )
    .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };
    assert!(matches!(
        insert.on_conflict,
        Some(OnConflictClause::DoNothing {
            conflict_target: Some(ConflictTarget::Columns(_))
        })
    ));
    assert_eq!(insert.returning.len(), 1);
}

#[test]
fn parses_insert_with_on_conflict_do_update() {
    let stmt = parse_statement(
        "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = 1 RETURNING id",
    )
    .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };
    match insert.on_conflict {
        Some(OnConflictClause::DoUpdate {
            conflict_target,
            assignments,
            where_clause,
        }) => {
            assert_eq!(
                conflict_target,
                Some(ConflictTarget::Columns(vec!["id".to_string()]))
            );
            assert_eq!(assignments.len(), 1);
            assert!(where_clause.is_some());
        }
        other => panic!("expected ON CONFLICT DO UPDATE clause, got {other:?}"),
    }
    assert_eq!(insert.returning.len(), 1);
}

#[test]
fn parses_insert_with_on_conflict_on_constraint() {
    let stmt = parse_statement(
        "INSERT INTO users AS u (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE u.id = 1 RETURNING id",
    )
    .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };
    assert_eq!(insert.table_alias.as_deref(), Some("u"));
    match insert.on_conflict {
        Some(OnConflictClause::DoUpdate {
            conflict_target,
            assignments,
            where_clause,
        }) => {
            assert_eq!(
                conflict_target,
                Some(ConflictTarget::Constraint("users_pkey".to_string()))
            );
            assert_eq!(assignments.len(), 1);
            assert!(where_clause.is_some());
        }
        other => panic!("expected ON CONFLICT DO UPDATE clause, got {other:?}"),
    }
}

#[test]
fn parses_insert_select_source() {
    let stmt = parse_statement("INSERT INTO users (id, name) SELECT id, name FROM staging")
        .expect("parse should succeed");
    let Statement::Insert(insert) = stmt else {
        panic!("expected insert statement");
    };
    match insert.source {
        InsertSource::Query(query) => match query.body {
            QueryExpr::Select(_) => {}
            other => panic!("expected select query source, got {other:?}"),
        },
        other => panic!("expected query source, got {other:?}"),
    }
}

#[test]
fn parses_column_level_constraints() {
    let stmt = parse_statement(
        "CREATE TABLE child (id int8 PRIMARY KEY, email text UNIQUE, parent_id int8 REFERENCES parent(id) ON DELETE SET NULL, score int8 CHECK (score >= 0))",
    )
    .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };

    assert!(create.columns[0].primary_key);
    assert!(create.columns[0].unique);
    assert!(!create.columns[0].nullable);
    assert!(!create.columns[0].identity);
    assert!(create.columns[1].unique);
    let references = create.columns[2]
        .references
        .as_ref()
        .expect("references should parse");
    assert_eq!(references.table_name, vec!["parent".to_string()]);
    assert_eq!(references.column_name.as_deref(), Some("id"));
    assert_eq!(references.on_delete, ForeignKeyAction::SetNull);
    assert_eq!(references.on_update, ForeignKeyAction::Restrict);
    assert!(create.columns[3].check.is_some());
    assert!(create.columns[3].default.is_none());
    assert!(create.constraints.is_empty());
}

#[test]
fn parses_column_default_expression() {
    let stmt = parse_statement(
        "CREATE TABLE t (id int8 PRIMARY KEY, score int8 DEFAULT 7, tag text DEFAULT 'x')",
    )
    .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.columns[1].default.is_some());
    assert!(create.columns[2].default.is_some());
}

#[test]
fn parses_identity_column_definition() {
    let stmt =
        parse_statement("CREATE TABLE t (id int8 GENERATED BY DEFAULT AS IDENTITY, name text)")
            .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.columns[0].identity);
    assert!(!create.columns[0].nullable);
}

#[test]
fn parses_table_level_key_constraints() {
    let stmt = parse_statement(
        "CREATE TABLE t (a int8, b int8, c text, PRIMARY KEY (a, b), CONSTRAINT uq_c UNIQUE (c))",
    )
    .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };

    assert_eq!(create.columns.len(), 3);
    assert_eq!(create.constraints.len(), 2);
    match &create.constraints[0] {
        TableConstraint::PrimaryKey { name, columns } => {
            assert!(name.is_none());
            assert_eq!(columns, &vec!["a".to_string(), "b".to_string()]);
        }
        other => panic!("expected primary key constraint, got {other:?}"),
    }
    match &create.constraints[1] {
        TableConstraint::Unique { name, columns } => {
            assert_eq!(name.as_deref(), Some("uq_c"));
            assert_eq!(columns, &vec!["c".to_string()]);
        }
        other => panic!("expected unique constraint, got {other:?}"),
    }
}

#[test]
fn parses_table_level_composite_foreign_key_with_actions() {
    let stmt = parse_statement(
        "CREATE TABLE child (a int8, b int8, CONSTRAINT fk_ab FOREIGN KEY (a, b) REFERENCES parent (x, y) ON DELETE CASCADE ON UPDATE SET NULL)",
    )
    .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };

    assert_eq!(create.constraints.len(), 1);
    match &create.constraints[0] {
        TableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            assert_eq!(name.as_deref(), Some("fk_ab"));
            assert_eq!(columns, &vec!["a".to_string(), "b".to_string()]);
            assert_eq!(referenced_table, &vec!["parent".to_string()]);
            assert_eq!(referenced_columns, &vec!["x".to_string(), "y".to_string()]);
            assert_eq!(*on_delete, ForeignKeyAction::Cascade);
            assert_eq!(*on_update, ForeignKeyAction::SetNull);
        }
        other => panic!("expected foreign key constraint, got {other:?}"),
    }
}

#[test]
fn parses_update_statement() {
    let stmt = parse_statement(
        "UPDATE users SET name = 'z', active = true FROM teams t WHERE users.id = t.id",
    )
    .expect("parse should succeed");
    let Statement::Update(update) = stmt else {
        panic!("expected update statement");
    };

    assert_eq!(update.table_name, vec!["users".to_string()]);
    assert_eq!(update.assignments.len(), 2);
    assert_eq!(update.from.len(), 1);
    assert!(update.where_clause.is_some());
    assert!(update.returning.is_empty());
}

#[test]
fn parses_update_with_returning() {
    let stmt = parse_statement("UPDATE users SET name = 'z' WHERE id = 1 RETURNING *")
        .expect("parse should succeed");
    let Statement::Update(update) = stmt else {
        panic!("expected update statement");
    };
    assert!(update.from.is_empty());
    assert_eq!(update.returning.len(), 1);
}

#[test]
fn parses_delete_statement() {
    let stmt = parse_statement("DELETE FROM public.users USING teams WHERE active = false")
        .expect("parse should succeed");
    let Statement::Delete(delete) = stmt else {
        panic!("expected delete statement");
    };

    assert_eq!(
        delete.table_name,
        vec!["public".to_string(), "users".to_string()]
    );
    assert_eq!(delete.using.len(), 1);
    assert!(delete.where_clause.is_some());
    assert!(delete.returning.is_empty());
}

#[test]
fn parses_delete_with_returning() {
    let stmt = parse_statement("DELETE FROM users WHERE active = false RETURNING id")
        .expect("parse should succeed");
    let Statement::Delete(delete) = stmt else {
        panic!("expected delete statement");
    };
    assert!(delete.using.is_empty());
    assert_eq!(delete.returning.len(), 1);
}

#[test]
fn parses_merge_statement() {
    let stmt = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .expect("parse should succeed");
    let Statement::Merge(merge) = stmt else {
        panic!("expected merge statement");
    };
    assert_eq!(merge.target_table, vec!["users".to_string()]);
    assert_eq!(merge.target_alias.as_deref(), Some("u"));
    assert_eq!(merge.when_clauses.len(), 2);
    assert!(merge.returning.is_empty());
}

#[test]
fn parses_merge_with_matched_do_nothing_clause() {
    let stmt = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN MATCHED AND s.skip = true THEN DO NOTHING \
         WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
    )
    .expect("parse should succeed");
    let Statement::Merge(merge) = stmt else {
        panic!("expected merge statement");
    };
    assert_eq!(merge.when_clauses.len(), 2);
    assert!(matches!(
        merge.when_clauses[0],
        MergeWhenClause::MatchedDoNothing { .. }
    ));
}

#[test]
fn parses_merge_with_not_matched_by_source_clauses() {
    let stmt = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN NOT MATCHED BY SOURCE AND u.active = false THEN DELETE \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET active = false",
    )
    .expect("parse should succeed");
    let Statement::Merge(merge) = stmt else {
        panic!("expected merge statement");
    };
    assert_eq!(merge.when_clauses.len(), 2);
    assert!(matches!(
        merge.when_clauses[0],
        MergeWhenClause::NotMatchedBySourceDelete { .. }
    ));
    assert!(matches!(
        merge.when_clauses[1],
        MergeWhenClause::NotMatchedBySourceUpdate { .. }
    ));
}

#[test]
fn parses_merge_with_not_matched_by_target_clause() {
    let stmt = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, s.name)",
    )
    .expect("parse should succeed");
    let Statement::Merge(merge) = stmt else {
        panic!("expected merge statement");
    };
    assert_eq!(merge.when_clauses.len(), 1);
    assert!(matches!(
        merge.when_clauses[0],
        MergeWhenClause::NotMatchedInsert { .. }
    ));
}

#[test]
fn parses_merge_with_returning() {
    let stmt = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         RETURNING u.id, u.name",
    )
    .expect("parse should succeed");
    let Statement::Merge(merge) = stmt else {
        panic!("expected merge statement");
    };
    assert_eq!(merge.returning.len(), 2);
}

#[test]
fn rejects_unreachable_merge_when_clause_after_unconditional() {
    let err = parse_statement(
        "MERGE INTO users u USING staging s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         WHEN MATCHED AND s.id > 0 THEN DELETE",
    )
    .expect_err("parse should fail");
    assert!(err.message.contains("unreachable"));
}

#[test]
fn parses_drop_table_statement() {
    let stmt = parse_statement("DROP TABLE IF EXISTS users").expect("parse should succeed");
    let Statement::DropTable(drop_table) = stmt else {
        panic!("expected drop table statement");
    };

    assert_eq!(drop_table.names, vec![vec!["users".to_string()]]);
    assert!(drop_table.if_exists);
    assert_eq!(drop_table.behavior, DropBehavior::Restrict);
}

#[test]
fn parses_drop_table_with_multiple_relations() {
    let stmt = parse_statement("DROP TABLE a, public.b CASCADE").expect("parse should succeed");
    let Statement::DropTable(drop_table) = stmt else {
        panic!("expected drop table statement");
    };
    assert_eq!(
        drop_table.names,
        vec![
            vec!["a".to_string()],
            vec!["public".to_string(), "b".to_string()]
        ]
    );
    assert!(!drop_table.if_exists);
    assert_eq!(drop_table.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_drop_function_with_cascade() {
    let stmt = parse_statement("DROP FUNCTION IF EXISTS app.f1(integer) CASCADE")
        .expect("parse should succeed");
    let Statement::DropFunction(drop_function) = stmt else {
        panic!("expected drop function statement");
    };
    assert_eq!(
        drop_function.name,
        vec!["app".to_string(), "f1".to_string()]
    );
    assert!(drop_function.if_exists);
    assert_eq!(drop_function.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_drop_function_multiple_signatures() {
    let stmt = parse_statement("DROP FUNCTION f1(), f2(integer, numeric(10,2)) CASCADE")
        .expect("parse should succeed");
    let Statement::DropFunction(drop_function) = stmt else {
        panic!("expected drop function statement");
    };
    assert_eq!(drop_function.name, vec!["f1".to_string()]);
    assert_eq!(drop_function.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_create_cast_statement() {
    let stmt = parse_statement(
        "CREATE CAST (integer AS date) WITH FUNCTION sql_to_date(integer) AS ASSIGNMENT",
    )
    .expect("parse should succeed");
    let Statement::CreateCast(create_cast) = stmt else {
        panic!("expected create cast statement");
    };
    assert!(create_cast.function_name.is_some());
    assert!(create_cast.as_assignment);
    assert!(!create_cast.as_implicit);
}

#[test]
fn parses_create_trigger_with_truncate_event() {
    let stmt = parse_statement(
        "CREATE TRIGGER t AFTER TRUNCATE OR UPDATE ON demo FOR EACH STATEMENT EXECUTE FUNCTION f()",
    )
    .expect("parse should succeed");
    let Statement::CreateTrigger(trigger) = stmt else {
        panic!("expected create trigger statement");
    };
    assert_eq!(trigger.events.len(), 2);
    assert!(trigger.events.contains(&TriggerEvent::Truncate));
    assert!(trigger.events.contains(&TriggerEvent::Update));
}

#[test]
fn parses_create_and_drop_schema_statements() {
    let create = parse_statement("CREATE SCHEMA IF NOT EXISTS app").expect("parse should succeed");
    let Statement::CreateSchema(create_schema) = create else {
        panic!("expected create schema statement");
    };
    assert_eq!(create_schema.name, "app");
    assert!(create_schema.if_not_exists);

    let drop = parse_statement("DROP SCHEMA IF EXISTS app CASCADE").expect("parse should succeed");
    let Statement::DropSchema(drop_schema) = drop else {
        panic!("expected drop schema statement");
    };
    assert_eq!(drop_schema.name, "app");
    assert!(drop_schema.if_exists);
    assert_eq!(drop_schema.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_create_and_drop_view_statements() {
    let create_view = parse_statement("CREATE VIEW app.v_users AS SELECT id FROM users")
        .expect("parse should succeed");
    let Statement::CreateView(view) = create_view else {
        panic!("expected create view statement");
    };
    assert_eq!(view.name, vec!["app".to_string(), "v_users".to_string()]);
    assert!(!view.or_replace);
    assert!(!view.materialized);
    assert!(view.with_data);

    let create_mat =
        parse_statement("CREATE MATERIALIZED VIEW app.mv_users AS SELECT id FROM users")
            .expect("parse should succeed");
    let Statement::CreateView(mat) = create_mat else {
        panic!("expected create materialized view statement");
    };
    assert!(!mat.or_replace);
    assert!(mat.materialized);
    assert!(mat.with_data);

    let drop_view =
        parse_statement("DROP VIEW IF EXISTS app.v_users CASCADE").expect("parse should succeed");
    let Statement::DropView(drop_view) = drop_view else {
        panic!("expected drop view statement");
    };
    assert_eq!(drop_view.names.len(), 1);
    assert!(!drop_view.materialized);
    assert!(drop_view.if_exists);
    assert_eq!(drop_view.behavior, DropBehavior::Cascade);

    let drop_mat = parse_statement("DROP MATERIALIZED VIEW app.mv_users RESTRICT")
        .expect("parse should succeed");
    let Statement::DropView(drop_mat) = drop_mat else {
        panic!("expected drop materialized view statement");
    };
    assert_eq!(drop_mat.names.len(), 1);
    assert!(drop_mat.materialized);
    assert_eq!(drop_mat.behavior, DropBehavior::Restrict);
}

#[test]
fn parses_drop_view_multiple_names() {
    let stmt = parse_statement("DROP VIEW v1, app.v2 CASCADE").expect("parse should succeed");
    let Statement::DropView(drop) = stmt else {
        panic!("expected drop view statement");
    };
    assert_eq!(
        drop.names,
        vec![
            vec!["v1".to_string()],
            vec!["app".to_string(), "v2".to_string()]
        ]
    );
    assert_eq!(drop.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_create_or_replace_view_statement() {
    let stmt = parse_statement("CREATE OR REPLACE VIEW app.v_users AS SELECT id FROM users")
        .expect("parse should succeed");
    let Statement::CreateView(view) = stmt else {
        panic!("expected create view statement");
    };
    assert!(view.or_replace);
    assert!(!view.materialized);
    assert!(view.with_data);
}

#[test]
fn parses_create_or_replace_materialized_view_statement() {
    let stmt = parse_statement("CREATE OR REPLACE MATERIALIZED VIEW app.mv AS SELECT 1")
        .expect("parse should succeed");
    let Statement::CreateView(view) = stmt else {
        panic!("expected create view statement");
    };
    assert!(view.or_replace);
    assert!(view.materialized);
    assert!(view.with_data);
}

#[test]
fn parses_create_materialized_view_with_no_data_option() {
    let stmt = parse_statement("CREATE MATERIALIZED VIEW app.mv AS SELECT 1 WITH NO DATA")
        .expect("parse should succeed");
    let Statement::CreateView(view) = stmt else {
        panic!("expected create view statement");
    };
    assert!(view.materialized);
    assert!(!view.with_data);
}

#[test]
fn parses_create_materialized_view_with_data_option() {
    let stmt = parse_statement("CREATE MATERIALIZED VIEW app.mv AS SELECT 1 WITH DATA")
        .expect("parse should succeed");
    let Statement::CreateView(view) = stmt else {
        panic!("expected create view statement");
    };
    assert!(view.materialized);
    assert!(view.with_data);
}

#[test]
fn parses_refresh_materialized_view_statement() {
    let stmt =
        parse_statement("REFRESH MATERIALIZED VIEW app.mv_users").expect("parse should succeed");
    let Statement::RefreshMaterializedView(refresh) = stmt else {
        panic!("expected refresh materialized view statement");
    };
    assert_eq!(
        refresh.name,
        vec!["app".to_string(), "mv_users".to_string()]
    );
    assert!(!refresh.concurrently);
    assert!(refresh.with_data);
}

#[test]
fn parses_refresh_materialized_view_options() {
    let stmt = parse_statement("REFRESH MATERIALIZED VIEW CONCURRENTLY app.mv_users WITH NO DATA")
        .expect("parse should succeed");
    let Statement::RefreshMaterializedView(refresh) = stmt else {
        panic!("expected refresh materialized view statement");
    };
    assert!(refresh.concurrently);
    assert!(!refresh.with_data);
}

#[test]
fn parses_drop_index_drop_sequence_and_truncate() {
    let drop_index = parse_statement("DROP INDEX IF EXISTS public.uq_users_email RESTRICT")
        .expect("parse should succeed");
    let Statement::DropIndex(drop_index) = drop_index else {
        panic!("expected drop index statement");
    };
    assert_eq!(
        drop_index.name,
        vec!["public".to_string(), "uq_users_email".to_string()]
    );
    assert!(drop_index.if_exists);
    assert_eq!(drop_index.behavior, DropBehavior::Restrict);

    let drop_sequence =
        parse_statement("DROP SEQUENCE user_id_seq CASCADE").expect("parse should succeed");
    let Statement::DropSequence(drop_sequence) = drop_sequence else {
        panic!("expected drop sequence statement");
    };
    assert_eq!(drop_sequence.name, vec!["user_id_seq".to_string()]);
    assert!(!drop_sequence.if_exists);
    assert_eq!(drop_sequence.behavior, DropBehavior::Cascade);

    let truncate =
        parse_statement("TRUNCATE TABLE users, sessions CASCADE").expect("parse should succeed");
    let Statement::Truncate(truncate) = truncate else {
        panic!("expected truncate statement");
    };
    assert_eq!(truncate.table_names.len(), 2);
    assert_eq!(truncate.behavior, DropBehavior::Cascade);
}

#[test]
fn parses_create_sequence_statement() {
    let stmt = parse_statement("CREATE SEQUENCE public.user_id_seq START WITH 7 INCREMENT BY 3")
        .expect("parse should succeed");
    let Statement::CreateSequence(create) = stmt else {
        panic!("expected create sequence statement");
    };
    assert_eq!(
        create.name,
        vec!["public".to_string(), "user_id_seq".to_string()]
    );
    assert_eq!(create.start, Some(7));
    assert_eq!(create.increment, Some(3));
    assert!(create.min_value.is_none());
    assert!(create.max_value.is_none());
    assert!(create.cycle.is_none());
    assert!(create.cache.is_none());
}

#[test]
fn parses_create_sequence_extended_options() {
    let stmt = parse_statement(
        "CREATE SEQUENCE s START 5 INCREMENT -2 MINVALUE -10 MAXVALUE 100 CYCLE CACHE 8",
    )
    .expect("parse should succeed");
    let Statement::CreateSequence(create) = stmt else {
        panic!("expected create sequence statement");
    };
    assert_eq!(create.start, Some(5));
    assert_eq!(create.increment, Some(-2));
    assert_eq!(create.min_value, Some(Some(-10)));
    assert_eq!(create.max_value, Some(Some(100)));
    assert_eq!(create.cycle, Some(true));
    assert_eq!(create.cache, Some(8));
}

#[test]
fn parses_alter_sequence_restart_statement() {
    let stmt = parse_statement("ALTER SEQUENCE public.user_id_seq RESTART WITH 42")
        .expect("parse should succeed");
    let Statement::AlterSequence(alter) = stmt else {
        panic!("expected alter sequence statement");
    };
    assert_eq!(
        alter.name,
        vec!["public".to_string(), "user_id_seq".to_string()]
    );
    assert_eq!(
        alter.actions,
        vec![AlterSequenceAction::Restart { with: Some(42) }]
    );
}

#[test]
fn parses_alter_sequence_multiple_options() {
    let stmt = parse_statement(
        "ALTER SEQUENCE s RESTART WITH 9 INCREMENT BY -3 NO MINVALUE MAXVALUE 30 NO CYCLE CACHE 12",
    )
    .expect("parse should succeed");
    let Statement::AlterSequence(alter) = stmt else {
        panic!("expected alter sequence statement");
    };
    assert_eq!(
        alter.actions,
        vec![
            AlterSequenceAction::Restart { with: Some(9) },
            AlterSequenceAction::SetIncrement { increment: -3 },
            AlterSequenceAction::SetMinValue { min: None },
            AlterSequenceAction::SetMaxValue { max: Some(30) },
            AlterSequenceAction::SetCycle { cycle: false },
            AlterSequenceAction::SetCache { cache: 12 }
        ]
    );
}

#[test]
fn parses_create_unique_index_statement() {
    let stmt = parse_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)")
        .expect("parse should succeed");
    let Statement::CreateIndex(create) = stmt else {
        panic!("expected create index statement");
    };
    assert_eq!(create.name, "uq_users_email");
    assert_eq!(create.table_name, vec!["users".to_string()]);
    assert_eq!(create.columns, vec!["email".to_string()]);
    assert!(create.unique);
}

#[test]
fn parses_create_index_with_operator_class() {
    let stmt = parse_statement(
        "CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops, unique2 int4_ops)",
    )
    .expect("parse should succeed");
    let Statement::CreateIndex(create) = stmt else {
        panic!("expected create index statement");
    };
    assert_eq!(create.name, "onek_unique1");
    assert_eq!(create.table_name, vec!["onek".to_string()]);
    assert_eq!(
        create.columns,
        vec!["unique1".to_string(), "unique2".to_string()]
    );
}

#[test]
fn rejects_create_index_include_clause() {
    let err = parse_statement("CREATE INDEX idx_users_email ON users (email) INCLUDE (id)")
        .expect_err("parse should fail");
    assert!(
        err.message
            .contains("CREATE INDEX INCLUDE clause is not supported")
    );
}

#[test]
fn rejects_create_index_with_storage_parameters_clause() {
    let err =
        parse_statement("CREATE INDEX idx_users_email ON users (email) WITH (fillfactor = 70)")
            .expect_err("parse should fail");
    assert!(
        err.message
            .contains("CREATE INDEX WITH (...) storage parameters are not supported")
    );
}

#[test]
fn rejects_create_index_nulls_distinct_clause() {
    let err =
        parse_statement("CREATE UNIQUE INDEX idx_users_email ON users (email) NULLS DISTINCT")
            .expect_err("parse should fail");
    assert!(
        err.message
            .contains("CREATE INDEX NULLS [NOT] DISTINCT clause is not supported")
    );
}

#[test]
fn rejects_create_index_tablespace_clause() {
    let err = parse_statement("CREATE INDEX idx_users_email ON users (email) TABLESPACE fast")
        .expect_err("parse should fail");
    assert!(
        err.message
            .contains("CREATE INDEX TABLESPACE clause is not supported")
    );
}

#[test]
fn parses_alter_table_add_column_statement() {
    let stmt =
        parse_statement("ALTER TABLE users ADD COLUMN note text").expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    assert_eq!(alter.table_name, vec!["users".to_string()]);
    match alter.action {
        AlterTableAction::AddColumn(column) => {
            assert_eq!(column.name, "note");
            assert_eq!(column.data_type, TypeName::Text);
            assert!(column.nullable);
        }
        other => panic!("expected add column action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_add_constraint_statement() {
    let stmt = parse_statement("ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::AddConstraint(TableConstraint::Unique { name, columns }) => {
            assert_eq!(name.as_deref(), Some("uq_email"));
            assert_eq!(columns, vec!["email".to_string()]);
        }
        other => panic!("expected add constraint action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_drop_column_statement() {
    let stmt = parse_statement("ALTER TABLE users DROP COLUMN note").expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::DropColumn { name } => assert_eq!(name, "note"),
        other => panic!("expected drop column action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_drop_constraint_statement() {
    let stmt = parse_statement("ALTER TABLE users DROP CONSTRAINT users_pkey")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::DropConstraint { name } => assert_eq!(name, "users_pkey"),
        other => panic!("expected drop constraint action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_rename_column_statement() {
    let stmt = parse_statement("ALTER TABLE users RENAME COLUMN note TO details")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::RenameColumn { old_name, new_name } => {
            assert_eq!(old_name, "note");
            assert_eq!(new_name, "details");
        }
        other => panic!("expected rename column action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_set_not_null_statement() {
    let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note SET NOT NULL")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::SetColumnNullable { name, nullable } => {
            assert_eq!(name, "note");
            assert!(!nullable);
        }
        other => panic!("expected set column nullable action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_drop_not_null_statement() {
    let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note DROP NOT NULL")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::SetColumnNullable { name, nullable } => {
            assert_eq!(name, "note");
            assert!(nullable);
        }
        other => panic!("expected set column nullable action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_set_default_statement() {
    let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note SET DEFAULT 'x'")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::SetColumnDefault { name, default } => {
            assert_eq!(name, "note");
            assert_eq!(default, Some(Expr::String("x".to_string())));
        }
        other => panic!("expected set column default action, got {other:?}"),
    }
}

#[test]
fn parses_alter_table_drop_default_statement() {
    let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note DROP DEFAULT")
        .expect("parse should succeed");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected alter table statement");
    };

    match alter.action {
        AlterTableAction::SetColumnDefault { name, default } => {
            assert_eq!(name, "note");
            assert_eq!(default, None);
        }
        other => panic!("expected set column default action, got {other:?}"),
    }
}

#[test]
fn parses_alter_view_rename_statement() {
    let stmt =
        parse_statement("ALTER VIEW users_v RENAME TO users_view").expect("parse should succeed");
    let Statement::AlterView(alter) = stmt else {
        panic!("expected alter view statement");
    };
    assert_eq!(alter.name, vec!["users_v".to_string()]);
    assert!(!alter.materialized);
    match alter.action {
        AlterViewAction::RenameTo { new_name } => assert_eq!(new_name, "users_view"),
        other => panic!("expected rename action, got {other:?}"),
    }
}

#[test]
fn parses_alter_view_rename_column_statement() {
    let stmt = parse_statement("ALTER VIEW users_v RENAME COLUMN old_col TO new_col")
        .expect("parse should succeed");
    let Statement::AlterView(alter) = stmt else {
        panic!("expected alter view statement");
    };
    assert_eq!(alter.name, vec!["users_v".to_string()]);
    assert!(!alter.materialized);
    match alter.action {
        AlterViewAction::RenameColumn { old_name, new_name } => {
            assert_eq!(old_name, "old_col");
            assert_eq!(new_name, "new_col");
        }
        other => panic!("expected rename column action, got {other:?}"),
    }
}

#[test]
fn parses_alter_materialized_view_set_schema_statement() {
    let stmt = parse_statement("ALTER MATERIALIZED VIEW mv_users SET SCHEMA app")
        .expect("parse should succeed");
    let Statement::AlterView(alter) = stmt else {
        panic!("expected alter view statement");
    };
    assert_eq!(alter.name, vec!["mv_users".to_string()]);
    assert!(alter.materialized);
    match alter.action {
        AlterViewAction::SetSchema { schema_name } => assert_eq!(schema_name, "app"),
        other => panic!("expected set schema action, got {other:?}"),
    }
}

#[test]
fn parses_json_binary_operators() {
    let stmt = parse_statement(
        "SELECT \
         doc -> 'a', \
         doc ->> 'a', \
         doc #> '{a,b}', \
         doc #>> '{a,b}', \
         doc || '{\"z\":1}', \
         doc @> '{\"a\":1}', \
         doc <@ '{\"a\":1,\"b\":2}', \
         doc @? '$.a', \
         doc @@ '$.a', \
         doc ? 'a', \
         doc ?| '{a,b}', \
         doc ?| array['a','c'], \
         doc ?& '{a,b}', \
         doc ?& array['a','b'], \
         doc #- '{a,b}' \
         FROM t",
    )
    .expect("parse should succeed");

    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = query.body else {
        panic!("expected select query body");
    };

    let expected = [
        BinaryOp::JsonGet,
        BinaryOp::JsonGetText,
        BinaryOp::JsonPath,
        BinaryOp::JsonPathText,
        BinaryOp::JsonConcat,
        BinaryOp::JsonContains,
        BinaryOp::JsonContainedBy,
        BinaryOp::JsonPathExists,
        BinaryOp::JsonPathMatch,
        BinaryOp::JsonHasKey,
        BinaryOp::JsonHasAny,
        BinaryOp::JsonHasAny,
        BinaryOp::JsonHasAll,
        BinaryOp::JsonHasAll,
        BinaryOp::JsonDeletePath,
    ];

    assert_eq!(select.targets.len(), expected.len());
    for (target, op) in select.targets.iter().zip(expected) {
        match &target.expr {
            Expr::Binary { op: parsed, .. } => assert_eq!(parsed, &op),
            other => panic!("expected binary expression target, got {other:?}"),
        }
    }
}

#[test]
fn parses_aggregate_function_modifiers() {
    let stmt = parse_statement(
        "SELECT json_agg(DISTINCT payload ORDER BY id DESC) FILTER (WHERE keep = true) FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = query.body else {
        panic!("expected select query body");
    };
    let Expr::FunctionCall {
        name,
        distinct,
        order_by,
        filter,
        ..
    } = &select.targets[0].expr
    else {
        panic!("expected function call");
    };
    assert_eq!(name, &vec!["json_agg".to_string()]);
    assert!(*distinct);
    assert_eq!(order_by.len(), 1);
    assert!(filter.is_some());
}

#[test]
fn parses_window_function_with_partition_and_order_by() {
    let stmt =
        parse_statement("SELECT row_number() OVER (PARTITION BY team ORDER BY score DESC) FROM t")
            .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = query.body else {
        panic!("expected select query body");
    };
    let Expr::FunctionCall {
        name, args, over, ..
    } = &select.targets[0].expr
    else {
        panic!("expected function call");
    };
    assert_eq!(name, &vec!["row_number".to_string()]);
    assert!(args.is_empty());
    let over = over.as_deref().expect("expected OVER clause");
    assert_eq!(over.partition_by.len(), 1);
    assert_eq!(over.order_by.len(), 1);
    assert!(over.frame.is_none());
}

#[test]
fn parses_window_frame_rows_and_range_between() {
    let stmt = parse_statement(
        "SELECT \
         sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
         avg(v) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) \
         FROM t",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = query.body else {
        panic!("expected select query body");
    };

    let Expr::FunctionCall {
        over: over_rows, ..
    } = &select.targets[0].expr
    else {
        panic!("expected function call");
    };
    let over_rows = over_rows.as_deref().expect("expected OVER clause");
    let frame_rows = over_rows.frame.as_ref().expect("expected frame");
    assert_eq!(frame_rows.units, WindowFrameUnits::Rows);
    assert!(matches!(
        frame_rows.start,
        WindowFrameBound::OffsetPreceding(Expr::Integer(1))
    ));
    assert!(matches!(frame_rows.end, WindowFrameBound::CurrentRow));

    let Expr::FunctionCall {
        over: over_range, ..
    } = &select.targets[1].expr
    else {
        panic!("expected function call");
    };
    let over_range = over_range.as_deref().expect("expected OVER clause");
    let frame_range = over_range.frame.as_ref().expect("expected frame");
    assert_eq!(frame_range.units, WindowFrameUnits::Range);
    assert!(matches!(
        frame_range.start,
        WindowFrameBound::OffsetPreceding(Expr::Integer(2))
    ));
    assert!(matches!(
        frame_range.end,
        WindowFrameBound::OffsetFollowing(Expr::Integer(1))
    ));
}

#[test]
fn parses_typed_column_aliases_for_table_functions() {
    let stmt = parse_statement(
        "SELECT * FROM json_to_record('{\"a\":1,\"b\":\"x\"}') AS r(a int8, b text)",
    )
    .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = query.body else {
        panic!("expected select query body");
    };
    let TableExpression::Function(function) = &select.from[0] else {
        panic!("expected function table expression");
    };
    assert_eq!(
        function.column_aliases,
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(
        function.column_alias_types,
        vec![Some("int8".to_string()), Some("text".to_string())]
    );
}

#[test]
fn parses_explain_statement() {
    let stmt = parse_statement("EXPLAIN SELECT 1").unwrap();
    assert!(matches!(stmt, Statement::Explain(_)));
}

#[test]
fn parses_explain_analyze() {
    let stmt = parse_statement("EXPLAIN ANALYZE SELECT 1").unwrap();
    match stmt {
        Statement::Explain(e) => assert!(e.analyze),
        _ => panic!("expected EXPLAIN"),
    }
}

#[test]
fn parses_set_statement() {
    let stmt = parse_statement("SET search_path = public").unwrap();
    match stmt {
        Statement::Set(s) => {
            assert_eq!(s.name, "search_path");
            assert_eq!(s.value, "public");
        }
        _ => panic!("expected SET"),
    }
}

#[test]
fn parses_set_to_syntax() {
    let stmt = parse_statement("SET timezone TO 'UTC'").unwrap();
    match stmt {
        Statement::Set(s) => {
            assert_eq!(s.name, "timezone");
            assert_eq!(s.value, "UTC");
        }
        _ => panic!("expected SET"),
    }
}

#[test]
fn parses_show_statement() {
    let stmt = parse_statement("SHOW search_path").unwrap();
    match stmt {
        Statement::Show(s) => assert_eq!(s.name, "search_path"),
        _ => panic!("expected SHOW"),
    }
}

#[test]
fn parses_listen_notify_unlisten() {
    let stmt = parse_statement("LISTEN my_channel").unwrap();
    assert!(matches!(stmt, Statement::Listen(_)));

    let stmt = parse_statement("NOTIFY my_channel, 'payload'").unwrap();
    match stmt {
        Statement::Notify(n) => {
            assert_eq!(n.channel, "my_channel");
            assert_eq!(n.payload, Some("payload".to_string()));
        }
        _ => panic!("expected NOTIFY"),
    }

    let stmt = parse_statement("UNLISTEN *").unwrap();
    match stmt {
        Statement::Unlisten(u) => assert!(u.channel.is_none()),
        _ => panic!("expected UNLISTEN"),
    }
}

#[test]
fn parses_do_block() {
    let stmt = parse_statement("DO 'BEGIN NULL; END'").unwrap();
    match stmt {
        Statement::Do(d) => assert_eq!(d.body, "BEGIN NULL; END"),
        _ => panic!("expected DO"),
    }
}

#[test]
fn parses_do_block_language_before_body() {
    let stmt = parse_statement("DO LANGUAGE plpgsql $$BEGIN NULL; END$$").unwrap();
    match stmt {
        Statement::Do(d) => {
            assert_eq!(d.language, "plpgsql");
            assert_eq!(d.body, "BEGIN NULL; END");
        }
        _ => panic!("expected DO"),
    }
}

#[test]
fn parses_discard_all() {
    let stmt = parse_statement("DISCARD ALL").unwrap();
    assert!(matches!(stmt, Statement::Discard(_)));
}

#[test]
fn parses_transaction_statements() {
    let stmt = parse_statement("BEGIN").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Begin)
    ));

    let stmt = parse_statement("START TRANSACTION").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Begin)
    ));

    let stmt = parse_statement("COMMIT").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Commit)
    ));

    let stmt = parse_statement("END").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Commit)
    ));

    let stmt = parse_statement("ROLLBACK").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Rollback)
    ));

    let stmt = parse_statement("SAVEPOINT s1").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::Savepoint(name)) if name == "s1"
    ));

    let stmt = parse_statement("RELEASE SAVEPOINT s1").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::ReleaseSavepoint(name)) if name == "s1"
    ));

    let stmt = parse_statement("ROLLBACK TO SAVEPOINT s1").unwrap();
    assert!(matches!(
        stmt,
        Statement::Transaction(TransactionStatement::RollbackToSavepoint(name)) if name == "s1"
    ));
}

#[test]
fn parses_create_and_drop_subscription() {
    let stmt = parse_statement(
        "CREATE SUBSCRIPTION sub1 CONNECTION 'host=upstream dbname=app' \
         PUBLICATION pub1 WITH (copy_data = false, slot_name = 'slot1')",
    )
    .expect("parse should succeed");
    let Statement::CreateSubscription(create) = stmt else {
        panic!("expected create subscription");
    };
    assert_eq!(create.name, "sub1");
    assert_eq!(create.publication, "pub1");
    assert!(!create.options.copy_data);
    assert_eq!(create.options.slot_name.as_deref(), Some("slot1"));

    let stmt = parse_statement("DROP SUBSCRIPTION IF EXISTS sub1").expect("parse should succeed");
    let Statement::DropSubscription(drop) = stmt else {
        panic!("expected drop subscription");
    };
    assert!(drop.if_exists);
    assert_eq!(drop.name, "sub1");
}

#[test]
fn parses_create_temp_table() {
    let stmt =
        parse_statement("CREATE TEMP TABLE foo (id INT, name TEXT)").expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.temporary);
    assert!(!create.if_not_exists);
    assert_eq!(create.name, vec!["foo".to_string()]);
    assert_eq!(create.columns.len(), 2);
}

#[test]
fn parses_create_temporary_table() {
    let stmt =
        parse_statement("CREATE TEMPORARY TABLE bar (id INT)").expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.temporary);
    assert!(!create.if_not_exists);
    assert_eq!(create.name, vec!["bar".to_string()]);
}

#[test]
fn parses_create_table_if_not_exists() {
    let stmt =
        parse_statement("CREATE TABLE IF NOT EXISTS baz (id INT)").expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(!create.temporary);
    assert!(create.if_not_exists);
    assert_eq!(create.name, vec!["baz".to_string()]);
}

#[test]
fn parses_create_temp_table_if_not_exists() {
    let stmt = parse_statement("CREATE TEMP TABLE IF NOT EXISTS qux (id INT, value NUMERIC)")
        .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.temporary);
    assert!(create.if_not_exists);
    assert_eq!(create.name, vec!["qux".to_string()]);
    assert_eq!(create.columns.len(), 2);
}

#[test]
fn rejects_create_temp_schema() {
    let result = parse_statement("CREATE TEMP SCHEMA foo");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.message.contains("unexpected"));
}

#[test]
fn parses_create_unlogged_table() {
    let stmt = parse_statement("CREATE UNLOGGED TABLE logs (id INT, message TEXT)")
        .expect("parse should succeed");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected create table statement");
    };
    assert!(create.unlogged);
    assert!(!create.temporary);
    assert_eq!(create.name, vec!["logs".to_string()]);
    assert_eq!(create.columns.len(), 2);
}

#[test]
fn parses_create_type_as_enum() {
    let stmt = parse_statement("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
        .expect("parse should succeed");
    let Statement::CreateType(create) = stmt else {
        panic!("expected create type statement");
    };
    assert_eq!(create.name, vec!["mood".to_string()]);
    assert_eq!(
        create.as_enum,
        vec![
            "happy".to_string(),
            "sad".to_string(),
            "neutral".to_string()
        ]
    );
}

#[test]
fn parses_create_type_qualified_name() {
    let stmt = parse_statement("CREATE TYPE public.status AS ENUM ('active', 'inactive')")
        .expect("parse should succeed");
    let Statement::CreateType(create) = stmt else {
        panic!("expected create type statement");
    };
    assert_eq!(
        create.name,
        vec!["public".to_string(), "status".to_string()]
    );
    assert_eq!(create.as_enum.len(), 2);
}

#[test]
fn parses_create_domain() {
    let stmt = parse_statement("CREATE DOMAIN posint AS INT").expect("parse should succeed");
    let Statement::CreateDomain(create) = stmt else {
        panic!("expected create domain statement");
    };
    assert_eq!(create.name, vec!["posint".to_string()]);
    assert!(create.check_constraint.is_none());
}

#[test]
fn parses_create_domain_with_check() {
    let stmt = parse_statement("CREATE DOMAIN posint AS INT CHECK (VALUE > 0)")
        .expect("parse should succeed");
    let Statement::CreateDomain(create) = stmt else {
        panic!("expected create domain statement");
    };
    assert_eq!(create.name, vec!["posint".to_string()]);
    assert!(create.check_constraint.is_some());
}

#[test]
fn parses_create_domain_with_named_check_constraint() {
    let stmt = parse_statement(
        "CREATE DOMAIN orderedarray AS INT[2] CONSTRAINT sorted CHECK (VALUE[1] < VALUE[2])",
    )
    .expect("parse should succeed");
    let Statement::CreateDomain(create) = stmt else {
        panic!("expected create domain statement");
    };
    assert_eq!(create.name, vec!["orderedarray".to_string()]);
    assert!(create.check_constraint.is_some());
}

#[test]
fn parses_drop_type() {
    let stmt = parse_statement("DROP TYPE mood").expect("parse should succeed");
    let Statement::DropType(drop) = stmt else {
        panic!("expected drop type statement");
    };
    assert_eq!(drop.name, vec!["mood".to_string()]);
    assert!(!drop.if_exists);
}

#[test]
fn parses_drop_type_if_exists_cascade() {
    let stmt = parse_statement("DROP TYPE IF EXISTS mood CASCADE").expect("parse should succeed");
    let Statement::DropType(drop) = stmt else {
        panic!("expected drop type statement");
    };
    assert_eq!(drop.name, vec!["mood".to_string()]);
    assert!(drop.if_exists);
}

#[test]
fn parses_drop_domain() {
    let stmt = parse_statement("DROP DOMAIN posint").expect("parse should succeed");
    let Statement::DropDomain(drop) = stmt else {
        panic!("expected drop domain statement");
    };
    assert_eq!(drop.name, vec!["posint".to_string()]);
    assert!(!drop.if_exists);
}

#[test]
fn parses_drop_domain_if_exists() {
    let stmt = parse_statement("DROP DOMAIN IF EXISTS posint").expect("parse should succeed");
    let Statement::DropDomain(drop) = stmt else {
        panic!("expected drop domain statement");
    };
    assert!(drop.if_exists);
}

#[test]
fn parses_cast_to_integer_types() {
    // int2 / smallint
    let stmt = parse_statement("SELECT 1::int2").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int2"
    ));

    let stmt = parse_statement("SELECT 1::smallint").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int2"
    ));

    // int4 / integer
    let stmt = parse_statement("SELECT 1::int4").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int4"
    ));

    let stmt = parse_statement("SELECT 1::integer").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int4"
    ));

    // int8 / bigint
    let stmt = parse_statement("SELECT 1::bigint").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int8"
    ));
}

#[test]
fn parses_cast_to_float_types() {
    // float4 / real
    let stmt = parse_statement("SELECT 1.5::float4").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "float8"
    ));

    let stmt = parse_statement("SELECT 1.5::real").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "float8"
    ));

    // numeric / decimal
    let stmt = parse_statement("SELECT 1.5::numeric").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "float8"
    ));

    let stmt = parse_statement("SELECT 1.5::decimal").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "float8"
    ));
}

#[test]
fn parses_cast_to_time_types() {
    let stmt = parse_statement("SELECT '12:00:00'::time").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "time"
    ));

    let stmt = parse_statement("SELECT '1 day'::interval").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "interval"
    ));
}

#[test]
fn parses_cast_to_binary_and_special_types() {
    let stmt = parse_statement("SELECT 'abc'::bytea").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "bytea"
    ));

    let stmt = parse_statement("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "uuid"
    ));
}

#[test]
fn parses_cast_to_json_types() {
    let stmt = parse_statement("SELECT '{}'::json").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "json"
    ));

    let stmt = parse_statement("SELECT '{}'::jsonb").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "jsonb"
    ));
}

#[test]
fn parses_cast_to_system_types() {
    let stmt = parse_statement("SELECT 'users'::regclass").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "regclass"
    ));

    let stmt = parse_statement("SELECT 123::oid").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "oid"
    ));
}

#[test]
fn parses_cast_to_array_types() {
    let stmt = parse_statement("SELECT ARRAY[1,2,3]::int[]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int4[]"
    ));

    let stmt = parse_statement("SELECT ARRAY['a','b']::text[]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "text[]"
    ));

    // Multi-dimensional arrays
    let stmt = parse_statement("SELECT '{}'::int[][]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::Cast { type_name, .. } if type_name == "int4[][]"
    ));
}

#[test]
fn parses_nested_array_constructor_literal() {
    let stmt = parse_statement("SELECT ARRAY[[1,2],[3,4]]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let Expr::ArrayConstructor(items) = &select.targets[0].expr else {
        panic!("expected array constructor");
    };
    assert_eq!(items.len(), 2);
    assert!(matches!(items[0], Expr::ArrayConstructor(_)));
    assert!(matches!(items[1], Expr::ArrayConstructor(_)));
}

#[test]
fn parses_function_call_with_nested_array_argument() {
    let stmt =
        parse_statement("SELECT foreach_test(ARRAY[[1,2],[3,4]])").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    let Expr::FunctionCall { args, .. } = &select.targets[0].expr else {
        panic!("expected function call");
    };
    assert_eq!(args.len(), 1);
    assert!(matches!(args[0], Expr::ArrayConstructor(_)));
}

#[test]
fn parses_qualified_wildcard() {
    let stmt = parse_statement("SELECT t.* FROM users t").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 1);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::QualifiedWildcard(parts) if parts == &vec!["t".to_string()]
    ));
}

#[test]
fn parses_schema_qualified_wildcard() {
    let stmt =
        parse_statement("SELECT public.users.* FROM public.users").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 1);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::QualifiedWildcard(parts) if parts == &vec!["public".to_string(), "users".to_string()]
    ));
}

#[test]
fn parses_multiple_wildcards() {
    let stmt = parse_statement("SELECT t1.*, t2.*, * FROM t1, t2").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    assert_eq!(select.targets.len(), 3);
    assert!(matches!(
        &select.targets[0].expr,
        Expr::QualifiedWildcard(_)
    ));
    assert!(matches!(
        &select.targets[1].expr,
        Expr::QualifiedWildcard(_)
    ));
    assert!(matches!(&select.targets[2].expr, Expr::Wildcard));
}

#[test]
fn parses_window_frame_with_groups() {
    let stmt = parse_statement(
        "SELECT id, sum(amount) OVER (ORDER BY date GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales"
    ).expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    if let Expr::FunctionCall {
        over: Some(window), ..
    } = &select.targets[1].expr
    {
        let frame = window.frame.as_ref().expect("should have frame");
        assert!(matches!(frame.units, WindowFrameUnits::Groups));
    } else {
        panic!("expected window function");
    }
}

#[test]
fn parses_window_frame_with_exclude() {
    let stmt = parse_statement(
        "SELECT id, sum(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM sales"
    ).expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    if let Expr::FunctionCall {
        over: Some(window), ..
    } = &select.targets[1].expr
    {
        let frame = window.frame.as_ref().expect("should have frame");
        assert!(matches!(frame.units, WindowFrameUnits::Rows));
        assert!(matches!(
            frame.exclusion,
            Some(WindowFrameExclusion::CurrentRow)
        ));
    } else {
        panic!("expected window function");
    }
}

#[test]
fn parses_window_frame_with_exclude_group() {
    let stmt = parse_statement(
        "SELECT id, rank() OVER (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM sales"
    ).expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    if let Expr::FunctionCall {
        over: Some(window), ..
    } = &select.targets[1].expr
    {
        let frame = window.frame.as_ref().expect("should have frame");
        assert!(matches!(frame.units, WindowFrameUnits::Range));
        assert!(matches!(frame.exclusion, Some(WindowFrameExclusion::Group)));
    } else {
        panic!("expected window function");
    }
}

#[test]
fn parses_window_function_with_respect_nulls_modifier() {
    let stmt = parse_statement("SELECT lag(v) RESPECT NULLS OVER (ORDER BY id) FROM t")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let select = as_select(&query);
    if let Expr::FunctionCall { over, .. } = &select.targets[0].expr {
        assert!(over.is_some());
    } else {
        panic!("expected window function call");
    }
}

#[test]
fn parses_order_by_using_operator() {
    let stmt =
        parse_statement("SELECT * FROM users ORDER BY id USING <").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(query.order_by[0].using_operator, Some("<".to_string()));
    assert_eq!(query.order_by[0].ascending, Some(true));
}

#[test]
fn parses_order_by_using_greater_operator() {
    let stmt =
        parse_statement("SELECT * FROM users ORDER BY id USING >").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(query.order_by[0].using_operator, Some(">".to_string()));
    assert_eq!(query.order_by[0].ascending, Some(false));
}

#[test]
fn parses_order_by_using_with_multiple_columns() {
    let stmt = parse_statement("SELECT * FROM users ORDER BY name USING <, age USING >")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    assert_eq!(query.order_by.len(), 2);
    assert_eq!(query.order_by[0].using_operator, Some("<".to_string()));
    assert_eq!(query.order_by[1].using_operator, Some(">".to_string()));
}

#[test]
fn parses_select_for_update_clause() {
    let stmt = parse_statement("SELECT * FROM users ORDER BY id FOR UPDATE OF users")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    assert_eq!(query.order_by.len(), 1);
}

#[test]
fn parses_extract_function() {
    let stmt = parse_statement("SELECT EXTRACT(year FROM '2023-01-01'::timestamp)")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::FunctionCall { name, args, .. } = &select.targets[0].expr else {
        panic!("expected function call");
    };
    assert_eq!(name, &vec!["extract".to_string()]);
    assert_eq!(args.len(), 2);
}

#[test]
fn parses_substring_function() {
    let stmt =
        parse_statement("SELECT SUBSTRING('hello' FROM 2 FOR 3)").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::FunctionCall { name, args, .. } = &select.targets[0].expr else {
        panic!("expected function call");
    };
    assert_eq!(name, &vec!["substring".to_string()]);
    assert_eq!(args.len(), 3);
}

#[test]
fn parses_trim_function() {
    let stmt =
        parse_statement("SELECT TRIM(BOTH 'x' FROM 'xxxhelloxxx')").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::FunctionCall { name, args, .. } = &select.targets[0].expr else {
        panic!("expected function call");
    };
    assert_eq!(name, &vec!["trim".to_string()]);
    assert_eq!(args.len(), 3); // mode, chars, string
}

#[test]
fn parses_date_literal() {
    let stmt = parse_statement("SELECT DATE '2024-01-15'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::TypedLiteral { type_name, value } = &select.targets[0].expr else {
        panic!("expected typed literal, got {:?}", select.targets[0].expr);
    };
    assert_eq!(type_name, "date");
    assert_eq!(value, "2024-01-15");
}

#[test]
fn parses_time_literal() {
    let stmt = parse_statement("SELECT TIME '12:34:56'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::TypedLiteral { type_name, value } = &select.targets[0].expr else {
        panic!("expected typed literal");
    };
    assert_eq!(type_name, "time");
    assert_eq!(value, "12:34:56");
}

#[test]
fn parses_timestamp_literal() {
    let stmt =
        parse_statement("SELECT TIMESTAMP '2024-01-15 12:34:56'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::TypedLiteral { type_name, value } = &select.targets[0].expr else {
        panic!("expected typed literal");
    };
    assert_eq!(type_name, "timestamp");
    assert_eq!(value, "2024-01-15 12:34:56");
}

#[test]
fn parses_interval_literal() {
    let stmt = parse_statement("SELECT INTERVAL '1 day'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::TypedLiteral { type_name, value } = &select.targets[0].expr else {
        panic!("expected typed literal");
    };
    assert_eq!(type_name, "interval");
    assert_eq!(value, "1 day");
}

#[test]
fn parses_array_subscript() {
    let stmt = parse_statement("SELECT arr[1]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::ArraySubscript { expr, index } = &select.targets[0].expr else {
        panic!("expected array subscript, got {:?}", select.targets[0].expr);
    };
    let Expr::Identifier(parts) = &**expr else {
        panic!("expected identifier for array");
    };
    assert_eq!(parts, &vec!["arr".to_string()]);
    let Expr::Integer(idx) = &**index else {
        panic!("expected integer index");
    };
    assert_eq!(*idx, 1);
}

#[test]
fn parses_array_slice() {
    let stmt = parse_statement("SELECT arr[1:3]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::ArraySlice { expr, start, end } = &select.targets[0].expr else {
        panic!("expected array slice, got {:?}", select.targets[0].expr);
    };
    let Expr::Identifier(parts) = &**expr else {
        panic!("expected identifier for array");
    };
    assert_eq!(parts, &vec!["arr".to_string()]);
    assert!(start.is_some());
    assert!(end.is_some());
    if let Some(start_expr) = start {
        let Expr::Integer(idx) = &**start_expr else {
            panic!("expected integer start");
        };
        assert_eq!(*idx, 1);
    }
    if let Some(end_expr) = end {
        let Expr::Integer(idx) = &**end_expr else {
            panic!("expected integer end");
        };
        assert_eq!(*idx, 3);
    }
}

#[test]
fn parses_array_slice_open_end() {
    let stmt = parse_statement("SELECT arr[2:]").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::ArraySlice {
        expr: _,
        start,
        end,
    } = &select.targets[0].expr
    else {
        panic!("expected array slice");
    };
    assert!(start.is_some());
    assert!(end.is_none());
}

#[test]
fn parses_jsonb_contains_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1}'::jsonb @> '{\"a\":1}'::jsonb")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonContains);
}

#[test]
fn parses_jsonb_contained_by_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1}'::jsonb <@ '{\"a\":1,\"b\":2}'::jsonb")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonContainedBy);
}

#[test]
fn parses_jsonb_has_key_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1}'::jsonb ? 'a'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonHasKey);
}

#[test]
fn parses_jsonb_has_any_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1}'::jsonb ?| ARRAY['a','b']")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonHasAny);
}

#[test]
fn parses_jsonb_has_all_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1,\"b\":2}'::jsonb ?& ARRAY['a','b']")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonHasAll);
}

#[test]
fn parses_jsonb_concat_operator() {
    let stmt = parse_statement("SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonConcat);
}

#[test]
fn parses_string_concat_operator() {
    let stmt = parse_statement("SELECT 'hello' || ' ' || 'world'").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary {
        op: op1,
        left,
        right: _,
    } = &select.targets[0].expr
    else {
        panic!("expected binary expression");
    };
    assert_eq!(*op1, BinaryOp::JsonConcat);
    let Expr::Binary { op: op2, .. } = &**left else {
        panic!("expected binary expression on left");
    };
    assert_eq!(*op2, BinaryOp::JsonConcat);
}

#[test]
fn parses_jsonb_delete_path_operator() {
    let stmt = parse_statement("SELECT '{\"a\":{\"b\":1}}'::jsonb #- '{a,b}'")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    let Expr::Binary { op, .. } = &select.targets[0].expr else {
        panic!("expected binary expression");
    };
    assert_eq!(*op, BinaryOp::JsonDeletePath);
}

#[test]
fn parses_standalone_values_single_row() {
    let stmt = parse_statement("VALUES (1, 'a')").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Values(rows) = &query.body else {
        panic!("expected values query");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
}

#[test]
fn parses_standalone_values_multi_row() {
    let stmt =
        parse_statement("VALUES (1, 'a'), (2, 'b'), (3, 'c')").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Values(rows) = &query.body else {
        panic!("expected values query");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[1].len(), 2);
    assert_eq!(rows[2].len(), 2);
}

#[test]
fn parses_values_with_order_by() {
    let stmt = parse_statement("VALUES (3), (1), (2) ORDER BY 1").expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Values(rows) = &query.body else {
        panic!("expected values query");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(query.order_by.len(), 1);
}

#[test]
fn parses_lateral_subquery() {
    let stmt =
        parse_statement("SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) sub")
            .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    assert_eq!(select.from.len(), 2);

    // Second table should be a lateral subquery
    if let TableExpression::Subquery(subq_ref) = &select.from[1] {
        assert!(subq_ref.lateral, "subquery should be marked as lateral");
    } else {
        panic!("expected subquery in FROM clause");
    }
}

#[test]
fn parses_lateral_function() {
    let stmt = parse_statement("SELECT * FROM t1, LATERAL unnest(t1.arr) AS elem")
        .expect("parse should succeed");
    let Statement::Query(query) = stmt else {
        panic!("expected query statement");
    };
    let QueryExpr::Select(select) = &query.body else {
        panic!("expected select");
    };
    assert_eq!(select.from.len(), 2);

    // Second table should be a lateral function
    if let TableExpression::Function(func_ref) = &select.from[1] {
        assert!(func_ref.lateral, "function should be marked as lateral");
    } else {
        panic!("expected function in FROM clause");
    }
}
