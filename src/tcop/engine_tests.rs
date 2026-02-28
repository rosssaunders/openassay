use super::*;
use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
use crate::executor::exec_expr::ws_simulate_message;
use crate::parser::sql_parser::parse_statement;
use crate::storage::tuple::ScalarValue;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use std::future::Future;

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

fn parse_http_response(value: &ScalarValue) -> JsonValue {
    let ScalarValue::Text(text) = value else {
        panic!("http response should be text");
    };
    serde_json::from_str::<JsonValue>(text).expect("http response should be JSON")
}

#[cfg(not(target_arch = "wasm32"))]
struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

#[cfg(not(target_arch = "wasm32"))]
fn read_http_request(stream: &mut std::net::TcpStream) -> HttpRequest {
    use std::io::Read;

    let mut buffer = Vec::new();
    let mut tmp = [0u8; 1024];
    let mut header_end = None;
    loop {
        let n = stream.read(&mut tmp).expect("request read should succeed");
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&tmp[..n]);
        if header_end.is_none() {
            header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
        }
        if header_end.is_some() {
            break;
        }
    }
    let header_end = header_end.unwrap_or(buffer.len());
    let header_bytes = &buffer[..header_end];
    let body_start = header_end.saturating_add(4);
    let mut body_bytes = buffer[body_start..].to_vec();
    let header_text = String::from_utf8_lossy(header_bytes);
    let mut lines = header_text.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    while body_bytes.len() < content_length {
        let n = stream.read(&mut tmp).expect("body read should succeed");
        if n == 0 {
            break;
        }
        body_bytes.extend_from_slice(&tmp[..n]);
    }
    let body = String::from_utf8_lossy(&body_bytes[..content_length]).to_string();
    HttpRequest {
        method,
        path,
        headers,
        body,
    }
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
    let result = with_isolated_state(|| run_statement("SELECT $1 + 5", &[Some("7".to_string())]));
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
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("logical type mismatch expected");
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
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("division by zero should error");
        assert!(err.message.contains("division by zero"));
    });

    with_isolated_state(|| {
        let statement = parse_statement("SELECT 10 % 0").expect("statement should parse");
        let planned = plan_statement(statement).expect("statement should plan");
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("modulo by zero should error");
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
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("non-numeric coercion should fail");
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
            ScalarValue::Numeric(rust_decimal::Decimal::new(123, 2)),
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
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("invalid json should fail");
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
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("non-array JSON input should fail");
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

        run_statement("CREATE EXTENSION http", &[]);
        let sql = format!("SELECT http_get('http://{}/data')", addr);
        let result = run_statement(&sql, &[]);
        let response = parse_http_response(&result.rows[0][0]);
        assert_eq!(response["status"], JsonValue::Number(JsonNumber::from(200)));
        assert_eq!(
            response["content"],
            JsonValue::String("hello-from-http-get".to_string())
        );
        assert_eq!(
            response["content_type"],
            JsonValue::String("text/plain".to_string())
        );
        let headers = response["headers"]
            .as_array()
            .expect("headers should be array");
        assert!(headers.iter().any(|header| {
            header
                .get("field")
                .and_then(|field| field.as_str())
                .is_some_and(|field| field.eq_ignore_ascii_case("content-type"))
        }));

        handle.join().expect("http server thread should finish");
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn http_extension_required() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT http_get('http://example.com')").expect("parses");
        let planned = plan_statement(statement).expect("plans");
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("http_get should require extension");
        assert!(err.message.contains("extension \"http\" is not loaded"));
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn executes_http_extension_functions() {
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    with_isolated_state(|| {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("listener should bind: {err}"),
        };
        let addr = listener.local_addr().expect("listener local addr");
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for _ in 0..8 {
                let (mut stream, _) = listener.accept().expect("should accept connection");
                let request = read_http_request(&mut stream);
                let method = request.method.clone();
                tx.send(request).expect("request should send");
                let body = if method == "HEAD" {
                    String::new()
                } else {
                    format!("response-{method}")
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Test: true\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("response write should succeed");
            }
        });

        run_statement("CREATE EXTENSION http", &[]);
        let base_url = format!("http://{}", addr);
        let responses = vec![
            run_statement(&format!("SELECT http_get('{}/get')", base_url), &[]),
            run_statement(
                &format!(
                    "SELECT http_get('{}/search', jsonb_build_object('search','two words','page',1))",
                    base_url
                ),
                &[],
            ),
            run_statement(
                &format!(
                    "SELECT http_post('{}/post', 'payload', 'text/plain')",
                    base_url
                ),
                &[],
            ),
            run_statement(
                &format!(
                    "SELECT http_post('{}/form', jsonb_build_object('name','Colin & James','rate','50%'))",
                    base_url
                ),
                &[],
            ),
            run_statement(
                &format!(
                    "SELECT http_put('{}/put', '{{\"ok\":true}}', 'application/json')",
                    base_url
                ),
                &[],
            ),
            run_statement(
                &format!(
                    "SELECT http_patch('{}/patch', 'patch-body', 'text/plain')",
                    base_url
                ),
                &[],
            ),
            run_statement(&format!("SELECT http_delete('{}/delete')", base_url), &[]),
            run_statement(&format!("SELECT http_head('{}/head')", base_url), &[]),
        ];

        let expected_contents = [
            "response-GET",
            "response-GET",
            "response-POST",
            "response-POST",
            "response-PUT",
            "response-PATCH",
            "response-DELETE",
            "",
        ];
        for (response, expected_content) in responses.iter().zip(expected_contents) {
            let body = parse_http_response(&response.rows[0][0]);
            assert_eq!(body["status"], JsonValue::Number(JsonNumber::from(200)));
            assert_eq!(
                body["content_type"],
                JsonValue::String("text/plain".to_string())
            );
            assert_eq!(
                body["content"],
                JsonValue::String(expected_content.to_string())
            );
            let headers = body["headers"].as_array().expect("headers should be array");
            assert!(headers.iter().any(|header| {
                header
                    .get("field")
                    .and_then(|field| field.as_str())
                    .is_some_and(|field| field.eq_ignore_ascii_case("x-test"))
            }));
        }

        let requests: Vec<_> = rx.iter().take(8).collect();
        handle.join().expect("http server thread should finish");

        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, "/get");

        assert_eq!(requests[1].method, "GET");
        assert!(requests[1].path.starts_with("/search?"));
        assert!(requests[1].path.contains("search=two+words"));
        assert!(requests[1].path.contains("page=1"));

        assert_eq!(requests[2].method, "POST");
        assert_eq!(requests[2].path, "/post");
        assert_eq!(requests[2].body, "payload");
        assert_eq!(
            requests[2].headers.get("content-type"),
            Some(&"text/plain".to_string())
        );

        assert_eq!(requests[3].method, "POST");
        assert_eq!(requests[3].path, "/form");
        assert!(requests[3].body.contains("name=Colin+%26+James"));
        assert!(requests[3].body.contains("rate=50%25"));
        assert_eq!(
            requests[3].headers.get("content-type"),
            Some(&"application/x-www-form-urlencoded".to_string())
        );

        assert_eq!(requests[4].method, "PUT");
        assert_eq!(requests[4].path, "/put");
        assert_eq!(requests[4].body, "{\"ok\":true}");
        assert_eq!(
            requests[4].headers.get("content-type"),
            Some(&"application/json".to_string())
        );

        assert_eq!(requests[5].method, "PATCH");
        assert_eq!(requests[5].path, "/patch");
        assert_eq!(requests[5].body, "patch-body");

        assert_eq!(requests[6].method, "DELETE");
        assert_eq!(requests[6].path, "/delete");

        assert_eq!(requests[7].method, "HEAD");
        assert_eq!(requests[7].path, "/head");
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn urlencode_encodes_text() {
    with_isolated_state(|| {
        run_statement("CREATE EXTENSION http", &[]);
        let result = run_statement("SELECT urlencode('my special string''s & things?')", &[]);
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Text(
                "my+special+string%27s+%26+things%3F".to_string()
            )]]
        );
    });
}

#[test]
fn substring_rejects_negative_length() {
    with_isolated_state(|| {
        let statement =
            parse_statement("SELECT substring('abcdef', 2, -1)").expect("statement parses");
        let planned = plan_statement(statement).expect("statement should plan");
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("negative length should fail");
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
    let result = run("SELECT g.n, l.tens \
             FROM generate_series(1,3) g(n), LATERAL (SELECT n * 10 AS tens) l \
             ORDER BY 1");
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
        run_statement("CREATE EXTENSION http", &[]);
        run_statement("CREATE TABLE urls (url text)", &[]);
        let insert_sql = format!("INSERT INTO urls VALUES ('{}'), ('{}')", url1, url2);
        run_statement(&insert_sql, &[]);
        let result = run_statement(
            "SELECT u.url, length(json_extract_path_text(l.response, 'content')) \
                 FROM urls u, LATERAL (SELECT http_get(u.url) AS response) l \
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

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn json_table_with_path() {
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
            let mut request_buf = [0u8; 2048];
            let _ = stream.read(&mut request_buf);
            let body =
                r#"{"result":[{"name":"BTC","type":"crypto"},{"name":"ETH","type":"crypto"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
        });

        let sql = format!(
            "SELECT * FROM json_table('http://{}/api', 'result') ORDER BY name",
            addr
        );
        let result = run_statement(&sql, &[]);
        assert_eq!(result.columns, vec!["name", "type"]);
        assert_eq!(
            result.rows,
            vec![
                vec![
                    ScalarValue::Text("BTC".to_string()),
                    ScalarValue::Text("crypto".to_string()),
                ],
                vec![
                    ScalarValue::Text("ETH".to_string()),
                    ScalarValue::Text("crypto".to_string()),
                ],
            ]
        );

        handle.join().expect("http server thread should finish");
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn json_table_top_level_array() {
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
            let mut request_buf = [0u8; 2048];
            let _ = stream.read(&mut request_buf);
            let body = r#"[{"id":1,"val":"a"},{"id":2,"val":"b"}]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
        });

        let sql = format!(
            "SELECT * FROM json_table('http://{}/api') ORDER BY id",
            addr
        );
        let result = run_statement(&sql, &[]);
        assert_eq!(result.columns, vec!["id", "val"]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
            ]
        );

        handle.join().expect("http server thread should finish");
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn json_table_specific_columns() {
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
            let mut request_buf = [0u8; 2048];
            let _ = stream.read(&mut request_buf);
            let body = r#"{"data":[{"x":10,"y":20,"z":30}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
        });

        let sql = format!("SELECT x, z FROM json_table('http://{}/api', 'data')", addr);
        let result = run_statement(&sql, &[]);
        assert_eq!(result.columns, vec!["x", "z"]);
        assert_eq!(
            result.rows,
            vec![vec![ScalarValue::Int(10), ScalarValue::Int(30)]]
        );

        handle.join().expect("http server thread should finish");
    });
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn json_table_primitive_array() {
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
            let mut request_buf = [0u8; 2048];
            let _ = stream.read(&mut request_buf);
            let body = r#"{"items":["alpha","beta","gamma"]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).expect("write");
        });

        let sql = format!(
            "SELECT * FROM json_table('http://{}/api', 'items') ORDER BY value",
            addr
        );
        let result = run_statement(&sql, &[]);
        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Text("beta".to_string())],
                vec![ScalarValue::Text("gamma".to_string())],
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

        let r3 = run_statement("SELECT ARRAY(SELECT id FROM test_table ORDER BY id)", &[]);
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
        let err = plan_statement(statement).expect_err("planning should reject invalid recursion");
        assert!(err.message.contains("self-reference in non-recursive term"));
    });
}

#[test]
fn non_self_referencing_cte_in_recursive_with_executes() {
    with_isolated_state(|| {
        let statement =
            parse_statement("WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM t")
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

        let old_select = parse_statement("SELECT * FROM v_users").expect("statement should parse");
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
        assert!(err.message.contains("unknown column") || err.message.contains("does not exist"));
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

        let old_select = parse_statement("SELECT * FROM mv_users").expect("statement should parse");
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
        let statement =
            parse_statement("REFRESH MATERIALIZED VIEW v_users").expect("statement should parse");
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
        let statement = parse_statement("SELECT * FROM v_users").expect("statement should parse");
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
            vec![
                ScalarValue::Null,
                ScalarValue::Int(2024),
                ScalarValue::Int(250)
            ],
            vec![
                ScalarValue::Null,
                ScalarValue::Int(2025),
                ScalarValue::Int(500)
            ],
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
        let duplicate_index_plan = plan_statement(duplicate_index).expect("statement should plan");
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
fn create_index_populates_storage_btree_for_existing_rows() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE users (id int8, email text)", &[]);
        run_statement(
            "INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')",
            &[],
        );
        run_statement("CREATE INDEX idx_users_email ON users (email)", &[]);

        let table_oid = crate::catalog::with_catalog_read(|catalog| {
            catalog
                .resolve_table(
                    &["users".to_string()],
                    &crate::catalog::SearchPath::default(),
                )
                .map(|table| table.oid())
        })
        .expect("table should resolve");
        let offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(
                table_oid,
                "idx_users_email",
                &[ScalarValue::Text("b@example.com".to_string())],
            )
        });
        assert_eq!(offsets, vec![1]);

        let result = run_statement("SELECT id FROM users WHERE email = 'b@example.com'", &[]);
        assert_eq!(result.rows, vec![vec![ScalarValue::Int(2)]]);
    });
}

#[test]
fn update_and_delete_maintain_index_offsets() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE users (id int8, email text)", &[]);
        run_statement(
            "INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com'), (3, 'c@example.com')",
            &[],
        );
        run_statement("CREATE INDEX idx_users_email ON users (email)", &[]);
        run_statement(
            "UPDATE users SET email = 'bb@example.com' WHERE id = 2",
            &[],
        );

        let table_oid = crate::catalog::with_catalog_read(|catalog| {
            catalog
                .resolve_table(
                    &["users".to_string()],
                    &crate::catalog::SearchPath::default(),
                )
                .map(|table| table.oid())
        })
        .expect("table should resolve");
        let old_offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(
                table_oid,
                "idx_users_email",
                &[ScalarValue::Text("b@example.com".to_string())],
            )
        });
        assert!(old_offsets.is_empty());
        let new_offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(
                table_oid,
                "idx_users_email",
                &[ScalarValue::Text("bb@example.com".to_string())],
            )
        });
        assert_eq!(new_offsets, vec![1]);

        run_statement("DELETE FROM users WHERE id = 1", &[]);
        let shifted_offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(
                table_oid,
                "idx_users_email",
                &[ScalarValue::Text("bb@example.com".to_string())],
            )
        });
        assert_eq!(shifted_offsets, vec![0]);
    });
}

#[test]
fn unique_index_detects_duplicate_before_insert() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE users (id int8, email text)", &[]);
        run_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)", &[]);
        run_statement("INSERT INTO users VALUES (1, 'dup@example.com')", &[]);

        let duplicate = parse_statement("INSERT INTO users VALUES (2, 'dup@example.com')")
            .expect("statement should parse");
        let plan = plan_statement(duplicate).expect("statement should plan");
        let err =
            block_on(execute_planned_query(&plan, &[])).expect_err("duplicate insert should fail");
        assert!(err.message.contains("unique constraint"));
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
        let err = block_on(execute_planned_query(&dup_pk_plan, &[]))
            .expect_err("duplicate pk should fail");
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
        let err = block_on(execute_planned_query(&dup_pk_plan, &[]))
            .expect_err("duplicate pk should fail");
        assert!(err.message.contains("primary key"));

        let dup_unique = parse_statement("INSERT INTO membership VALUES (1, 11, 'a@example.com')")
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
        let err = block_on(execute_planned_query(&bad_insert_plan, &[]))
            .expect_err("fk insert should fail");
        assert!(err.message.contains("foreign key"));

        let bad_update = parse_statement("UPDATE children SET parent_id = 999 WHERE id = 10")
            .expect("statement should parse");
        let bad_update_plan = plan_statement(bad_update).expect("statement should plan");
        let err = block_on(execute_planned_query(&bad_update_plan, &[]))
            .expect_err("fk update should fail");
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

        let statement =
            parse_statement("DELETE FROM parents WHERE id = 1").expect("statement should parse");
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
        let err = block_on(execute_planned_query(&bad_insert_plan, &[]))
            .expect_err("fk insert should fail");
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
        let err = block_on(execute_planned_query(&duplicate_plan, &[]))
            .expect_err("duplicate should fail");
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
        let err = block_on(execute_planned_query(&duplicate_plan, &[]))
            .expect_err("duplicate should fail");
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

        let bad_insert =
            parse_statement("INSERT INTO metrics VALUES (2, -1)").expect("statement should parse");
        let bad_insert_plan = plan_statement(bad_insert).expect("statement should plan");
        let err =
            block_on(execute_planned_query(&bad_insert_plan, &[])).expect_err("check should fail");
        assert!(err.message.contains("CHECK constraint"));

        let bad_update = parse_statement("UPDATE metrics SET score = -2 WHERE id = 1")
            .expect("statement should parse");
        let bad_update_plan = plan_statement(bad_update).expect("statement should plan");
        let err =
            block_on(execute_planned_query(&bad_update_plan, &[])).expect_err("check should fail");
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

        let statement = parse_statement("SELECT id FROM users").expect("statement should parse");
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

        let restrict =
            parse_statement("DROP INDEX uq_users_email RESTRICT").expect("statement should parse");
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

        let restrict =
            parse_statement("DROP SEQUENCE seq_users_id RESTRICT").expect("statement should parse");
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
        let err =
            block_on(execute_planned_query(&plan, &[])).expect_err("sequence should be dropped");
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

        let restrict =
            parse_statement("DROP SEQUENCE seq_view_id RESTRICT").expect("statement should parse");
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

        let restrict = parse_statement("DROP SCHEMA app RESTRICT").expect("statement should parse");
        let restrict_plan = plan_statement(restrict).expect("statement should plan");
        let err = block_on(execute_planned_query(&restrict_plan, &[]))
            .expect_err("drop schema restrict should fail when sequence exists");
        assert!(err.message.contains("is not empty"));

        run_statement("DROP SCHEMA app CASCADE", &[]);
        let lookup = with_catalog_read(|catalog| catalog.schema("app").is_some());
        assert!(!lookup);

        let query = parse_statement("SELECT nextval('app.seq1')").expect("statement should parse");
        let plan = plan_statement(query).expect("statement should plan");
        let err =
            block_on(execute_planned_query(&plan, &[])).expect_err("sequence should be removed");
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

        let statement = parse_statement("SELECT note FROM users").expect("statement should parse");
        let planned = plan_statement(statement).expect("statement should plan");
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("old column name should fail");
        assert!(err.message.contains("unknown column"));
    });
}

#[test]
fn alter_table_set_and_drop_not_null() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE users (id int8 NOT NULL, note text)", &[]);
        run_statement("INSERT INTO users VALUES (1, 'a')", &[]);
        run_statement("ALTER TABLE users ALTER COLUMN note SET NOT NULL", &[]);

        let statement =
            parse_statement("INSERT INTO users VALUES (2, NULL)").expect("statement should parse");
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
fn update_set_default_uses_column_default() {
    let results = run_batch(&[
        "CREATE TABLE users (id int8 NOT NULL, note text DEFAULT 'x')",
        "INSERT INTO users VALUES (1, 'a'), (2, 'b')",
        "UPDATE users SET note = DEFAULT WHERE id = 2",
        "SELECT id, note FROM users ORDER BY 1",
    ]);

    assert_eq!(results[2].command_tag, "UPDATE");
    assert_eq!(
        results[3].rows,
        vec![
            vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
            vec![ScalarValue::Int(2), ScalarValue::Text("x".to_string())]
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
    assert_eq!(
        r.rows[0][0],
        ScalarValue::Numeric(rust_decimal::Decimal::new(5, 0))
    );
    assert_eq!(
        r.rows[0][1],
        ScalarValue::Numeric(rust_decimal::Decimal::new(4, 0))
    );
    assert_eq!(
        r.rows[0][2],
        ScalarValue::Numeric(rust_decimal::Decimal::new(457, 2))
    );
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

#[test]
fn math_functions_abs_bigint_overflow_errors() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT abs('-9223372036854775808'::int8)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("abs(int8 min) should fail");
        assert!(err.message.contains("bigint out of range"));
    });
}

#[test]
fn math_functions_lcm_overflow_errors() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT lcm(9223372036854775807::int8, 2::int8)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("lcm overflow should fail");
        assert!(err.message.contains("out of range"));
    });
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
    let r = run(
        "SELECT translate('hello', 'helo', 'HELO'), split_part('a.b.c', '.', 2), strpos('hello', 'llo')",
    );
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
    assert_eq!(
        results[2].rows[0][0],
        ScalarValue::Text("a, b, c".to_string())
    );
}

#[test]
fn aggregate_array_agg() {
    let results = run_batch(&[
        "CREATE TABLE t (id int8, val int8)",
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        "SELECT array_agg(val) FROM t",
    ]);
    assert_eq!(
        results[2].rows[0][0],
        ScalarValue::Text("{10,20,30}".to_string())
    );
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
        ScalarValue::Text(v) => assert!(v.contains("OpenAssay")),
        other => panic!("expected text, got {:?}", other),
    }
    assert_eq!(r.rows[0][1], ScalarValue::Text("openassay".to_string()));
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
    let results = run_batch(&["SET search_path = 'myschema'", "SHOW search_path"]);
    assert_eq!(
        results[1].rows[0][0],
        ScalarValue::Text("myschema".to_string())
    );
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

#[test]
fn do_block_perform_executes() {
    let r = run("DO 'BEGIN PERFORM 1; END'");
    assert_eq!(r.command_tag, "DO");
}

#[test]
fn do_block_select_into_assigns_variables() {
    let r = run(
        "DO 'DECLARE a integer; b integer; BEGIN SELECT 10, 20 INTO a, b; IF NOT found OR a <> 10 OR b <> 20 THEN RAISE; END IF; END'",
    );
    assert_eq!(r.command_tag, "DO");
}

#[test]
fn do_block_for_integer_range_loop_executes() {
    let r = run(
        "DO 'DECLARE total integer := 0; BEGIN FOR i IN 1..10 LOOP total := total + i; END LOOP; IF total <> 55 OR NOT found THEN RAISE; END IF; END'",
    );
    assert_eq!(r.command_tag, "DO");
}

#[test]
fn do_block_for_query_loop_executes() {
    let r = run(
        "DO 'DECLARE rec integer; total integer := 0; BEGIN FOR rec IN SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 LOOP total := total + rec; END LOOP; IF total <> 6 OR NOT found THEN RAISE; END IF; END'",
    );
    assert_eq!(r.command_tag, "DO");
}

#[test]
fn do_block_execute_dynamic_sql_executes() {
    let r = run("DO 'BEGIN EXECUTE ''SELECT 1''; IF NOT found THEN RAISE; END IF; END'");
    assert_eq!(r.command_tag, "DO");
}

#[test]
fn do_block_execute_dynamic_sql_into_assigns_variable() {
    let r = run(
        "DO 'DECLARE v integer; BEGIN EXECUTE ''SELECT 42'' INTO v; IF NOT found OR v <> 42 THEN RAISE; END IF; END'",
    );
    assert_eq!(r.command_tag, "DO");
}

// 1.11 System catalogs
#[test]
fn pg_settings_returns_guc_variables() {
    let r = run("SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version'");
    assert!(!r.rows.is_empty());
    assert_eq!(
        r.rows[0][0],
        ScalarValue::Text("server_version".to_string())
    );
}

#[test]
fn pg_database_returns_current_database() {
    let r = run("SELECT datname FROM pg_catalog.pg_database");
    assert_eq!(r.rows[0][0], ScalarValue::Text("openassay".to_string()));
}

#[test]
fn pg_tables_lists_user_tables() {
    let results = run_batch(&[
        "CREATE TABLE catalog_test (id int8)",
        "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'catalog_test'",
    ]);
    assert_eq!(results[1].rows.len(), 1);
    assert_eq!(
        results[1].rows[0][0],
        ScalarValue::Text("catalog_test".to_string())
    );
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
    let results = run_batch(&["CREATE EXTENSION ws", "CREATE EXTENSION IF NOT EXISTS ws"]);
    assert_eq!(results[0].command_tag, "CREATE EXTENSION");
    assert_eq!(results[1].command_tag, "CREATE EXTENSION");
}

#[test]
fn create_extension_duplicate_errors() {
    with_isolated_state(|| {
        run_statement("CREATE EXTENSION ws", &[]);
        let result = parse_statement("CREATE EXTENSION ws")
            .and_then(|s| {
                plan_statement(s).map_err(|e| crate::parser::sql_parser::ParseError {
                    message: e.message,
                    position: 0,
                })
            })
            .and_then(|p| {
                block_on(execute_planned_query(&p, &[])).map_err(|e| {
                    crate::parser::sql_parser::ParseError {
                        message: e.message,
                        position: 0,
                    }
                })
            });
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

#[test]
fn create_function_basic() {
    let results = run_batch(&[
        "CREATE FUNCTION add_one(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql",
        "SELECT proname FROM pg_proc WHERE proname = 'add_one'",
    ]);
    assert_eq!(results[0].command_tag, "CREATE FUNCTION");
    assert_eq!(results[1].rows.len(), 1);
    assert_eq!(
        results[1].rows[0][0],
        ScalarValue::Text("add_one".to_string())
    );
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
    run(
        "CREATE FUNCTION my_tbl(msg JSONB) RETURNS TABLE(price TEXT, qty TEXT) AS $$ SELECT msg->>'p', msg->>'q' $$ LANGUAGE sql",
    );
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
    assert_eq!(
        results[2].rows[0][1],
        ScalarValue::Text("wss://example.com".to_string())
    );
    assert_eq!(
        results[2].rows[0][2],
        ScalarValue::Text("connecting".to_string())
    );
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
    assert_eq!(
        results[3].rows[0][0],
        ScalarValue::Text("closed".to_string())
    );
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
        assert_eq!(
            results[0].rows[0][0],
            ScalarValue::Text("100.5".to_string())
        );
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

#[test]
fn evaluates_sha256_function() {
    let result = run("SELECT sha256('abc')");
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        ScalarValue::Text(s) => {
            assert!(s.starts_with("\\x"));
            assert_eq!(s.len(), 66); // \\x + 64 hex chars
            assert_eq!(
                s,
                "\\xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
        }
        other => panic!("expected Text, got {:?}", other),
    }
}

#[test]
fn evaluates_pg_typeof_function() {
    let result = run(
        "SELECT pg_typeof(42), pg_typeof(3.14), pg_typeof('hello'), pg_typeof(true), pg_typeof(NULL)",
    );
    assert_eq!(
        result.rows,
        vec![vec![
            ScalarValue::Text("bigint".to_string()),
            ScalarValue::Text("numeric".to_string()),
            ScalarValue::Text("text".to_string()),
            ScalarValue::Text("boolean".to_string()),
            ScalarValue::Text("unknown".to_string()),
        ]]
    );
}

#[test]
fn evaluates_pg_column_size_function() {
    let result = run("SELECT pg_column_size(42), pg_column_size('hello')");
    assert_eq!(
        result.rows,
        vec![vec![
            ScalarValue::Int(8),
            ScalarValue::Int(9), // 5 chars + 4 byte header
        ]]
    );
}

#[test]
fn evaluates_timezone_function() {
    let result = run("SELECT timezone('UTC', '2024-01-01 12:00:00')");
    assert_eq!(
        result.rows,
        vec![vec![ScalarValue::Text("2024-01-01 12:00:00".to_string())]]
    );
}

#[test]
fn creates_temporary_table() {
    let results = run_batch(&[
        "CREATE TEMP TABLE temp_test (id INT, name TEXT)",
        "INSERT INTO temp_test VALUES (1, 'test')",
        "SELECT * FROM temp_test",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].rows_affected, 1);
    assert_eq!(results[2].rows.len(), 1);
}

#[test]
fn creates_table_if_not_exists() {
    let results = run_batch(&[
        "CREATE TABLE IF NOT EXISTS ine_test (id INT)",
        "CREATE TABLE IF NOT EXISTS ine_test (id INT)",
        "INSERT INTO ine_test VALUES (1)",
        "SELECT * FROM ine_test",
    ]);
    // Both CREATE TABLE statements should succeed
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].command_tag, "CREATE TABLE");
    assert_eq!(results[2].rows_affected, 1);
    assert_eq!(results[3].rows.len(), 1);
}

#[test]
fn creates_temp_table_if_not_exists() {
    let results = run_batch(&[
        "CREATE TEMP TABLE IF NOT EXISTS temp_ine (id INT, value TEXT)",
        "CREATE TEMPORARY TABLE IF NOT EXISTS temp_ine (id INT, value TEXT)",
        "INSERT INTO temp_ine VALUES (42, 'hello')",
        "SELECT * FROM temp_ine",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].command_tag, "CREATE TABLE");
    assert_eq!(results[2].rows_affected, 1);
    assert_eq!(results[3].rows.len(), 1);
}

#[test]
fn creates_type_as_enum() {
    let results = run_batch(&["CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')"]);
    assert_eq!(results[0].command_tag, "CREATE TYPE");
}

#[test]
fn creates_and_drops_type() {
    let results = run_batch(&[
        "CREATE TYPE status AS ENUM ('active', 'inactive')",
        "DROP TYPE status",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TYPE");
    assert_eq!(results[1].command_tag, "DROP TYPE");
}

#[test]
fn creates_domain() {
    let results = run_batch(&["CREATE DOMAIN posint AS INT"]);
    assert_eq!(results[0].command_tag, "CREATE DOMAIN");
}

#[test]
fn creates_domain_with_check() {
    let results = run_batch(&["CREATE DOMAIN posint AS INT CHECK (VALUE > 0)"]);
    assert_eq!(results[0].command_tag, "CREATE DOMAIN");
}

#[test]
fn creates_and_drops_domain() {
    let results = run_batch(&["CREATE DOMAIN posint AS INT", "DROP DOMAIN posint"]);
    assert_eq!(results[0].command_tag, "CREATE DOMAIN");
    assert_eq!(results[1].command_tag, "DROP DOMAIN");
}

#[test]
fn drops_type_if_exists() {
    let results = run_batch(&["DROP TYPE IF EXISTS nonexistent_type"]);
    assert_eq!(results[0].command_tag, "DROP TYPE");
}

#[test]
fn drops_domain_if_exists() {
    let results = run_batch(&["DROP DOMAIN IF EXISTS nonexistent_domain"]);
    assert_eq!(results[0].command_tag, "DROP DOMAIN");
}

#[test]
fn selects_qualified_wildcard() {
    let results = run_batch(&[
        "CREATE TABLE users (id INT, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "SELECT u.* FROM users u",
    ]);
    assert_eq!(results[3].rows.len(), 2);
    assert_eq!(results[3].rows[0].len(), 2);
    assert_eq!(results[3].rows[0][0], ScalarValue::Int(1));
    assert_eq!(
        results[3].rows[0][1],
        ScalarValue::Text("Alice".to_string())
    );
}

#[test]
fn selects_multiple_qualified_wildcards() {
    let results = run_batch(&[
        "CREATE TABLE t1 (a INT, b TEXT)",
        "CREATE TABLE t2 (c INT, d TEXT)",
        "INSERT INTO t1 VALUES (1, 'foo')",
        "INSERT INTO t2 VALUES (2, 'bar')",
        "SELECT t1.*, t2.* FROM t1, t2",
    ]);
    assert_eq!(results[4].rows.len(), 1);
    assert_eq!(results[4].rows[0].len(), 4);
    assert_eq!(results[4].rows[0][0], ScalarValue::Int(1));
    assert_eq!(results[4].rows[0][1], ScalarValue::Text("foo".to_string()));
    assert_eq!(results[4].rows[0][2], ScalarValue::Int(2));
    assert_eq!(results[4].rows[0][3], ScalarValue::Text("bar".to_string()));
}

#[test]
fn casts_to_json_and_jsonb() {
    let result = run("SELECT '{}'::json, '[]'::jsonb, '{\"a\":1}'::json");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Text("{}".to_string()));
    assert_eq!(result.rows[0][1], ScalarValue::Text("[]".to_string()));
    assert_eq!(
        result.rows[0][2],
        ScalarValue::Text("{\"a\":1}".to_string())
    );
}

#[test]
fn rejects_invalid_json_cast() {
    let statement = parse_statement("SELECT 'invalid'::json").expect("should parse");
    let planned = plan_statement(statement).expect("should plan");
    with_isolated_state(|| {
        let result = block_on(execute_planned_query(&planned, &[]));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("not valid JSON"));
    });
}

#[test]
fn create_table_as_select_simple() {
    let results = run_batch(&[
        "CREATE TABLE t1 (id int, name text)",
        "INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')",
        "CREATE TABLE t2 AS SELECT * FROM t1",
        "SELECT * FROM t2 ORDER BY id",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].rows_affected, 2);
    assert!(results[2].command_tag.starts_with("SELECT")); // "SELECT 2"
    assert_eq!(results[3].rows.len(), 2);
    assert_eq!(results[3].rows[0][0], ScalarValue::Int(1));
    assert_eq!(
        results[3].rows[0][1],
        ScalarValue::Text("Alice".to_string())
    );
    assert_eq!(results[3].rows[1][0], ScalarValue::Int(2));
    assert_eq!(results[3].rows[1][1], ScalarValue::Text("Bob".to_string()));
}

#[test]
fn create_table_as_select_with_projection() {
    let results = run_batch(&[
        "CREATE TABLE tbl (x int, y int, z text)",
        "INSERT INTO tbl VALUES (1, 2, 'a'), (3, 4, 'b')",
        "CREATE TABLE dest AS SELECT x, z FROM tbl",
        "SELECT * FROM dest ORDER BY x",
    ]);
    assert!(results[2].command_tag.starts_with("SELECT"));
    assert_eq!(results[3].rows.len(), 2);
    assert_eq!(results[3].rows[0][0], ScalarValue::Int(1));
    assert_eq!(results[3].rows[0][1], ScalarValue::Text("a".to_string()));
}

#[test]
fn create_temp_table_as_select() {
    let results = run_batch(&[
        "CREATE TEMP TABLE t AS SELECT 1 AS x, 'hello' AS y",
        "SELECT * FROM t",
    ]);
    assert!(results[0].command_tag.starts_with("SELECT"));
    assert_eq!(results[1].rows.len(), 1);
    assert_eq!(results[1].rows[0][0], ScalarValue::Int(1));
    assert_eq!(
        results[1].rows[0][1],
        ScalarValue::Text("hello".to_string())
    );
}

#[test]
fn create_index_if_not_exists() {
    let results = run_batch(&[
        "CREATE TABLE t (id int, name text)",
        "CREATE INDEX IF NOT EXISTS idx_id ON t(id)",
        "CREATE INDEX IF NOT EXISTS idx_id ON t(id)",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].command_tag, "CREATE INDEX");
    assert_eq!(results[2].command_tag, "CREATE INDEX"); // Should succeed silently
}

#[test]
fn create_index_accepts_operator_class() {
    let results = run_batch(&[
        "CREATE TABLE t (id int)",
        "CREATE INDEX idx_t_id ON t USING btree(id int4_ops)",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].command_tag, "CREATE INDEX");
}

#[test]
fn create_view_if_not_exists() {
    let results = run_batch(&[
        "CREATE TABLE t (x int)",
        "INSERT INTO t VALUES (1), (2)",
        "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t",
        "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t",
        "SELECT * FROM v ORDER BY x",
    ]);
    assert_eq!(results[2].command_tag, "CREATE VIEW");
    assert_eq!(results[3].command_tag, "CREATE VIEW"); // Should succeed silently
    assert_eq!(results[4].rows.len(), 2);
}

#[test]
fn create_sequence_if_not_exists() {
    let results = run_batch(&[
        "CREATE SEQUENCE IF NOT EXISTS seq",
        "CREATE SEQUENCE IF NOT EXISTS seq",
        "SELECT nextval('seq')",
    ]);
    assert_eq!(results[0].command_tag, "CREATE SEQUENCE");
    assert_eq!(results[1].command_tag, "CREATE SEQUENCE"); // Should succeed silently
    assert_eq!(results[2].rows[0][0], ScalarValue::Int(1));
}

#[test]
fn create_unlogged_table() {
    let results = run_batch(&[
        "CREATE UNLOGGED TABLE unlog_test (id int, note text)",
        "INSERT INTO unlog_test VALUES (1, 'abc')",
        "SELECT * FROM unlog_test",
    ]);
    assert_eq!(results[0].command_tag, "CREATE TABLE");
    assert_eq!(results[1].rows_affected, 1);
    assert_eq!(results[2].rows.len(), 1);
    assert_eq!(results[2].rows[0][0], ScalarValue::Int(1));
    assert_eq!(results[2].rows[0][1], ScalarValue::Text("abc".to_string()));
}

#[test]
fn drop_table_if_exists() {
    let results = run_batch(&[
        "DROP TABLE IF EXISTS nonexistent",
        "CREATE TABLE t1 (id int)",
        "DROP TABLE IF EXISTS t1",
        "DROP TABLE IF EXISTS t1", // Should succeed silently
    ]);
    assert_eq!(results[0].command_tag, "DROP TABLE");
    assert_eq!(results[1].command_tag, "CREATE TABLE");
    assert_eq!(results[2].command_tag, "DROP TABLE");
    assert_eq!(results[3].command_tag, "DROP TABLE");
}

#[test]
fn drop_view_if_exists() {
    let results = run_batch(&[
        "DROP VIEW IF EXISTS nonexistent",
        "CREATE TABLE t (x int)",
        "CREATE VIEW v AS SELECT * FROM t",
        "DROP VIEW IF EXISTS v",
        "DROP VIEW IF EXISTS v", // Should succeed silently
    ]);
    assert_eq!(results[0].command_tag, "DROP VIEW");
    assert_eq!(results[2].command_tag, "CREATE VIEW");
    assert_eq!(results[3].command_tag, "DROP VIEW");
    assert_eq!(results[4].command_tag, "DROP VIEW");
}

#[test]
fn drop_index_if_exists() {
    let results = run_batch(&[
        "DROP INDEX IF EXISTS nonexistent",
        "CREATE TABLE t (id int)",
        "CREATE INDEX idx ON t(id)",
        "DROP INDEX IF EXISTS idx",
        "DROP INDEX IF EXISTS idx", // Should succeed silently
    ]);
    assert_eq!(results[0].command_tag, "DROP INDEX");
    assert_eq!(results[2].command_tag, "CREATE INDEX");
    assert_eq!(results[3].command_tag, "DROP INDEX");
    assert_eq!(results[4].command_tag, "DROP INDEX");
}

#[test]
fn drop_sequence_if_exists() {
    let results = run_batch(&[
        "DROP SEQUENCE IF EXISTS nonexistent",
        "CREATE SEQUENCE seq",
        "DROP SEQUENCE IF EXISTS seq",
        "DROP SEQUENCE IF EXISTS seq", // Should succeed silently
    ]);
    assert_eq!(results[0].command_tag, "DROP SEQUENCE");
    assert_eq!(results[1].command_tag, "CREATE SEQUENCE");
    assert_eq!(results[2].command_tag, "DROP SEQUENCE");
    assert_eq!(results[3].command_tag, "DROP SEQUENCE");
}

#[test]
fn evaluates_typed_literals_and_array_operations() {
    // Test DATE literal - just verify it parses and executes
    let result = run("SELECT DATE '2024-01-15' AS d");
    assert_eq!(result.columns, vec!["d"]);
    assert_eq!(result.rows.len(), 1);

    // Test TIME literal
    let result = run("SELECT TIME '12:30:45' AS t");
    assert_eq!(result.columns, vec!["t"]);
    assert_eq!(result.rows.len(), 1);

    // Test TIMESTAMP literal
    let result = run("SELECT TIMESTAMP '2024-01-15 12:30:45' AS ts");
    assert_eq!(result.columns, vec!["ts"]);
    assert_eq!(result.rows.len(), 1);

    // Test array subscript (1-indexed)
    let result = run("SELECT ARRAY[10, 20, 30][2] AS elem");
    assert_eq!(result.columns, vec!["elem"]);
    assert_eq!(result.rows, vec![vec![ScalarValue::Int(20)]]);

    // Test array subscript - first element
    let result = run("SELECT ARRAY[10, 20, 30][1] AS elem");
    assert_eq!(result.rows, vec![vec![ScalarValue::Int(10)]]);

    // Test array slice
    let result = run("SELECT ARRAY[1, 2, 3, 4, 5][2:4] AS slice");
    assert_eq!(result.columns, vec!["slice"]);
    assert_eq!(result.rows.len(), 1);
    // Check the slice contains the right values
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Array(vec![
            ScalarValue::Int(2),
            ScalarValue::Int(3),
            ScalarValue::Int(4),
        ])
    );

    // Test CREATE TABLE with date/time types still works
    run("CREATE TABLE test_types (d date, t time, ts timestamp)");
    let result = run("SELECT 1");
    assert_eq!(result.rows, vec![vec![ScalarValue::Int(1)]]);
}

#[test]
fn test_string_concatenation() {
    let result = run("SELECT 'hello' || ' ' || 'world' AS greeting");
    assert_eq!(result.columns, vec!["greeting"]);
    assert_eq!(
        result.rows,
        vec![vec![ScalarValue::Text("hello world".to_string())]]
    );
}

#[test]
fn test_jsonb_concat() {
    let result = run("SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb AS merged");
    assert_eq!(result.columns, vec!["merged"]);
    assert_eq!(result.rows.len(), 1);
    // The result should be a merged JSON object
    let ScalarValue::Text(json_str) = &result.rows[0][0] else {
        panic!("expected text result");
    };
    let json: JsonValue = serde_json::from_str(json_str).expect("should parse JSON");
    assert_eq!(json["a"], JsonValue::Number(JsonNumber::from(1)));
    assert_eq!(json["b"], JsonValue::Number(JsonNumber::from(2)));
}

#[test]
fn test_jsonb_delete_key() {
    let result = run("SELECT '{\"a\":1,\"b\":2}'::jsonb - 'a' AS result");
    assert_eq!(result.columns, vec!["result"]);
    assert_eq!(result.rows.len(), 1);
    let ScalarValue::Text(json_str) = &result.rows[0][0] else {
        panic!("expected text result");
    };
    let json: JsonValue = serde_json::from_str(json_str).expect("should parse JSON");
    assert!(json.get("a").is_none(), "key 'a' should be deleted");
    assert_eq!(json["b"], JsonValue::Number(JsonNumber::from(2)));
}

#[test]
fn test_jsonb_has_key() {
    let result = run("SELECT '{\"a\":1}'::jsonb ? 'a' AS has_key");
    assert_eq!(result.columns, vec!["has_key"]);
    assert_eq!(result.rows, vec![vec![ScalarValue::Bool(true)]]);

    let result = run("SELECT '{\"a\":1}'::jsonb ? 'b' AS has_key");
    assert_eq!(result.rows, vec![vec![ScalarValue::Bool(false)]]);
}

#[test]
fn test_jsonb_contains() {
    let result = run("SELECT '{\"a\":1,\"b\":2}'::jsonb @> '{\"a\":1}'::jsonb AS contains");
    assert_eq!(result.columns, vec!["contains"]);
    assert_eq!(result.rows, vec![vec![ScalarValue::Bool(true)]]);

    let result = run("SELECT '{\"a\":1}'::jsonb @> '{\"a\":1,\"b\":2}'::jsonb AS contains");
    assert_eq!(result.rows, vec![vec![ScalarValue::Bool(false)]]);
}

#[test]
fn test_jsonb_subscript_reads() {
    let result = run(
        "SELECT ('{\"a\":[10,20,{\"k\":\"v\"}]}'::jsonb)['a'][2]['k'] AS v, ('[1, \"2\", null]'::jsonb)[1] AS arr_elem",
    );
    assert_eq!(result.columns, vec!["v", "arr_elem"]);
    assert_eq!(
        result.rows,
        vec![vec![
            ScalarValue::Text("\"v\"".to_string()),
            ScalarValue::Text("\"2\"".to_string()),
        ]]
    );

    let result = run("SELECT ('{\"a\":1}'::jsonb)['missing'] IS NULL AS is_null");
    assert_eq!(result.rows, vec![vec![ScalarValue::Bool(true)]]);
}

#[test]
fn test_jsonb_subscript_slice_errors() {
    with_isolated_state(|| {
        let statement =
            parse_statement("SELECT ('{\"a\":1}'::jsonb)['a':'b']").expect("parse should succeed");
        let planned = plan_statement(statement).expect("plan should succeed");
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("jsonb slices should be rejected");
        assert!(
            err.message
                .contains("jsonb subscript does not support slices")
        );
    });
}

#[test]
fn test_standalone_values_single_row() {
    let result = run("VALUES (1, 'a')");
    assert_eq!(result.columns, vec!["column1", "column2"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())]
    );
}

#[test]
fn test_standalone_values_multi_row() {
    let result = run("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    assert_eq!(result.columns, vec!["column1", "column2"]);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0],
        vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())]
    );
    assert_eq!(
        result.rows[1],
        vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())]
    );
    assert_eq!(
        result.rows[2],
        vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())]
    );
}

#[test]
fn test_values_with_order_by() {
    let result = run("VALUES (3), (1), (2) ORDER BY 1");
    assert_eq!(result.columns, vec!["column1"]);
    assert_eq!(result.rows.len(), 3);
    // Should be ordered: 1, 2, 3
    assert_eq!(result.rows[0], vec![ScalarValue::Int(1)]);
    assert_eq!(result.rows[1], vec![ScalarValue::Int(2)]);
    assert_eq!(result.rows[2], vec![ScalarValue::Int(3)]);
}

#[test]
fn test_values_with_limit() {
    let result = run("VALUES (1), (2), (3), (4) LIMIT 2");
    assert_eq!(result.columns, vec!["column1"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0], vec![ScalarValue::Int(1)]);
    assert_eq!(result.rows[1], vec![ScalarValue::Int(2)]);
}

#[test]
fn test_insert_returning() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE test_returning (id int, name text)", &[]);
        let result = run_statement(
            "INSERT INTO test_returning (id, name) VALUES (1, 'Alice') RETURNING *",
            &[],
        );

        // RETURNING should give us the inserted row
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], ScalarValue::Int(1));
        assert_eq!(result.rows[0][1], ScalarValue::Text("Alice".to_string()));

        // And the row should be in the table
        let result = run_statement("SELECT * FROM test_returning", &[]);
        assert_eq!(result.rows.len(), 1);
    });
}

#[test]
fn test_lateral_subquery() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE t1 (id int, val int)", &[]);
        run_statement("CREATE TABLE t2 (t1_id int, name text)", &[]);
        run_statement("INSERT INTO t1 VALUES (1, 10), (2, 20)", &[]);
        run_statement("INSERT INTO t2 VALUES (1, 'a'), (1, 'b'), (2, 'c')", &[]);

        // LATERAL allows the subquery to reference t1
        let result = run_statement(
            "SELECT t1.id, sub.name FROM t1, LATERAL (SELECT name FROM t2 WHERE t2.t1_id = t1.id) sub",
            &[],
        );
        assert_eq!(result.rows.len(), 3);
    });
}

// ── Tests for Phase 7: new built-in functions ──────────────────────────────────────────

#[test]
fn test_gen_random_uuid() {
    let result = run("SELECT gen_random_uuid()");
    assert_eq!(result.rows.len(), 1);
    let uuid_str = result.rows[0][0].render();
    // Check UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(uuid_str.chars().filter(|c| *c == '-').count(), 4);
    let parts: Vec<&str> = uuid_str.split('-').collect();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[0].len(), 8);
    assert_eq!(parts[1].len(), 4);
    assert_eq!(parts[2].len(), 4);
    assert_eq!(parts[3].len(), 4);
    assert_eq!(parts[4].len(), 12);
}

#[test]
fn test_uuid_ossp_extension() {
    with_isolated_state(|| {
        run_statement("CREATE EXTENSION \"uuid-ossp\"", &[]);
        let result = run_statement("SELECT uuid_nil(), uuid_generate_v4()", &[]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            ScalarValue::Text("00000000-0000-0000-0000-000000000000".to_string())
        );
        assert_eq!(result.rows[0][1].render().len(), 36);

        let v5 = run_statement("SELECT uuid_generate_v5('dns', 'example.com')", &[]);
        assert_eq!(
            v5.rows[0][0],
            ScalarValue::Text("cfbff0d1-9375-5685-968c-48ce8b15ae17".to_string())
        );
    });
}

#[test]
fn test_pgcrypto_functions() {
    with_isolated_state(|| {
        run_statement("CREATE EXTENSION pgcrypto", &[]);
        let digest = run_statement("SELECT digest('hello', 'sha256')", &[]);
        assert_eq!(
            digest.rows[0][0],
            ScalarValue::Text(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_string()
            )
        );

        let hmac = run_statement("SELECT hmac('data', 'key', 'sha1')", &[]);
        assert_eq!(
            hmac.rows[0][0],
            ScalarValue::Text("104152c5bfdca07bc633eebd46199f0255c9f49d".to_string())
        );

        let bytes = run_statement("SELECT gen_random_bytes(4)", &[]);
        assert_eq!(bytes.rows.len(), 1);
        assert_eq!(bytes.rows[0][0].render().len(), 8);

        let crypt = run_statement("SELECT crypt('secret', gen_salt('bf'))", &[]);
        let crypt_text = crypt.rows[0][0].render();
        assert!(crypt_text.starts_with("$2"), "expected bcrypt hash");
    });
}

#[test]
fn test_pgvector_functions_and_operators() {
    with_isolated_state(|| {
        run_statement("CREATE EXTENSION vector", &[]);
        let distances = run_statement(
            "SELECT l2_distance('[1,2]', '[4,6]'), l1_distance('[1,2]', '[4,6]'), cosine_distance('[1,0]', '[0,1]')",
            &[],
        );
        assert_eq!(distances.rows[0][0], ScalarValue::Float(5.0));
        assert_eq!(distances.rows[0][1], ScalarValue::Float(7.0));
        assert_eq!(distances.rows[0][2], ScalarValue::Float(1.0));

        let extras = run_statement(
            "SELECT inner_product('[1,2,3]', '[4,5,6]'), vector_dims('[1,2,3]'), vector_norm('[3,4]')",
            &[],
        );
        assert_eq!(extras.rows[0][0], ScalarValue::Float(32.0));
        assert_eq!(extras.rows[0][1], ScalarValue::Int(3));
        assert_eq!(extras.rows[0][2], ScalarValue::Float(5.0));

        let op_result = run_statement("SELECT '[1,2]' <-> '[4,6]' AS dist", &[]);
        assert_eq!(op_result.rows[0][0], ScalarValue::Float(5.0));
    });
}

#[test]
fn test_make_time() {
    let result = run("SELECT make_time(14, 30, 45.5)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "14:30:45.500000");
}

#[test]
fn test_make_time_without_fractional_seconds() {
    let result = run("SELECT make_time(8, 15, 30)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "08:15:30");
}

#[test]
fn test_format_basic_string() {
    let result = run("SELECT format('Hello %s', 'World')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "Hello World");
}

#[test]
fn test_format_multiple_args() {
    let result = run("SELECT format('Hello %s, you are %s', 'World', 'great')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "Hello World, you are great");
}

#[test]
fn test_format_with_identifier_quoting() {
    let result = run("SELECT format('CREATE TABLE %I (id int)', 'my_table')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "CREATE TABLE my_table (id int)");
}

#[test]
fn test_format_with_literal_quoting() {
    let result = run("SELECT format('INSERT INTO t VALUES (%L)', 'value''s')");
    assert_eq!(result.rows.len(), 1);
    // The input 'value''s' is parsed as the string "value's", then re-quoted with doubled quotes
    assert_eq!(
        result.rows[0][0].render(),
        "INSERT INTO t VALUES ('value''s')"
    );
}

#[test]
fn test_format_with_literal_percent() {
    let result = run("SELECT format('100%% complete')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "100% complete");
}

#[test]
fn test_format_with_null() {
    let result = run("SELECT format('Hello %s', NULL)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].render(), "Hello ");
}

#[test]
fn test_pg_input_is_valid_integer() {
    let result = run("SELECT pg_input_is_valid('42', 'integer')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_pg_input_is_valid_invalid_integer() {
    let result = run("SELECT pg_input_is_valid('foo', 'integer')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(false));
}

#[test]
fn test_pg_input_is_valid_float() {
    let result = run("SELECT pg_input_is_valid('3.14', 'float')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_pg_input_is_valid_boolean() {
    let result = run("SELECT pg_input_is_valid('true', 'boolean')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_pg_input_is_valid_date() {
    let result = run("SELECT pg_input_is_valid('2024-01-15', 'date')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_pg_input_is_valid_invalid_date() {
    let result = run("SELECT pg_input_is_valid('2024-13-01', 'date')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(false));
}

#[test]
fn test_pg_input_is_valid_json() {
    let result = run("SELECT pg_input_is_valid('{\"a\": 1}', 'json')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_pg_input_is_valid_invalid_json() {
    let result = run("SELECT pg_input_is_valid('{not json}', 'json')");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Bool(false));
}

#[test]
fn test_pg_get_viewdef() {
    let result = run("SELECT pg_get_viewdef('myview')");
    assert_eq!(result.rows.len(), 1);
    // For now, it returns a placeholder comment
    assert!(result.rows[0][0].render().contains("myview"));
}

// Tests for integer overflow handling

#[test]
fn test_int2_cast_valid() {
    let result = run("SELECT CAST(100 AS int2)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(100));
}

#[test]
fn test_int2_cast_max() {
    let result = run("SELECT CAST(32767 AS int2)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(32767));
}

#[test]
fn test_int2_cast_min() {
    let result = run("SELECT CAST(-32768 AS int2)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(-32768));
}

#[test]
fn test_int2_cast_overflow_positive() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT CAST(32768 AS int2)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("int2 overflow should fail");
        assert!(err.message.contains("smallint out of range"));
    });
}

#[test]
fn test_int2_cast_overflow_negative() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT CAST(-32769 AS int2)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("int2 overflow should fail");
        assert!(err.message.contains("smallint out of range"));
    });
}

#[test]
fn test_int4_cast_valid() {
    let result = run("SELECT CAST(1000000 AS int4)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(1000000));
}

#[test]
fn test_int4_cast_max() {
    let result = run("SELECT CAST(2147483647 AS int4)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(2147483647));
}

#[test]
fn test_int4_cast_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT CAST(2147483648 AS int4)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("int4 overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_int4_addition_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT 2147483647 + 1").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("int4 addition overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_int4_subtraction_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT -2147483648 - 1").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("int4 subtraction overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_int4_multiplication_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT 50000 * 50000").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("int4 multiplication overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_int4_division_by_zero() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT 100 / 0").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("division by zero should fail");
        assert!(err.message.contains("division by zero"));
    });
}

#[test]
fn test_int4_modulo_by_zero() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT 100 % 0").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("modulo by zero should fail");
        assert!(err.message.contains("division by zero"));
    });
}

#[test]
fn test_int4_division_overflow() {
    // -2147483648 / -1 causes overflow in two's complement
    with_isolated_state(|| {
        let statement = parse_statement("SELECT -2147483648 / -1").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("int4 division overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_int4_modulo_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT -2147483648 % -1").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("int4 modulo overflow should fail");
        assert!(err.message.contains("integer out of range"));
    });
}

#[test]
fn test_unary_minus_bigint_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT -('-9223372036854775808'::int8)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err = block_on(execute_planned_query(&planned, &[]))
            .expect_err("unary minus overflow should fail");
        assert!(err.message.contains("bigint out of range"));
    });
}

#[test]
fn test_timestamp_positive_infinity_literal() {
    let result = run("SELECT TIMESTAMP '+infinity'");
    assert_eq!(result.rows[0][0], ScalarValue::Text("infinity".to_string()));
}

#[test]
fn test_int4_valid_arithmetic() {
    let result = run("SELECT 100 + 200, 500 - 300, 10 * 20, 100 / 5, 17 % 5");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(300));
    assert_eq!(result.rows[0][1], ScalarValue::Int(200));
    assert_eq!(result.rows[0][2], ScalarValue::Int(200));
    assert_eq!(result.rows[0][3], ScalarValue::Int(20));
    assert_eq!(result.rows[0][4], ScalarValue::Int(2));
}

#[test]
fn test_typed_literal_int2() {
    let result = run("SELECT '100'::int2");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(100));
}

#[test]
fn test_typed_literal_int2_overflow() {
    with_isolated_state(|| {
        let statement = parse_statement("SELECT '40000'::int2").unwrap();
        let planned = plan_statement(statement).unwrap();
        let err =
            block_on(execute_planned_query(&planned, &[])).expect_err("int2 overflow should fail");
        assert!(err.message.contains("smallint out of range"));
    });
}

#[test]
fn test_hex_literal() {
    let result = run("SELECT 0x10");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(16));
}

#[test]
fn test_octal_literal() {
    let result = run("SELECT 0o10");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(8));
}

#[test]
fn test_binary_literal() {
    let result = run("SELECT 0b1010");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(10));
}

#[test]
fn test_underscore_in_literal() {
    let result = run("SELECT 1_000_000");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], ScalarValue::Int(1000000));
}

#[test]
fn test_named_window_definition() {
    with_isolated_state(|| {
        run_statement("CREATE TEMP TABLE t (x int)", &[]);
        run_statement("INSERT INTO t VALUES (1), (2), (3)", &[]);
        let result = run_statement(
            "SELECT x, sum(x) OVER w FROM t WINDOW w AS (ORDER BY x) ORDER BY x",
            &[],
        );
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], ScalarValue::Int(1));
        assert_eq!(result.rows[1][1], ScalarValue::Int(3));
        assert_eq!(result.rows[2][1], ScalarValue::Int(6));
    });
}

#[test]
fn test_groups_frame_mode() {
    with_isolated_state(|| {
        run_statement("CREATE TEMP TABLE t (x int)", &[]);
        run_statement("INSERT INTO t VALUES (1), (2), (2), (3)", &[]);
        let result = run_statement(
            "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
            &[],
        );
        assert_eq!(result.rows.len(), 4);
    });
}

#[test]
fn test_exclude_current_row() {
    with_isolated_state(|| {
        run_statement("CREATE TEMP TABLE t (x int)", &[]);
        run_statement("INSERT INTO t VALUES (1), (2), (3)", &[]);
        let result = run_statement(
            "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t ORDER BY x",
            &[],
        );
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], ScalarValue::Int(5)); // 2+3
        assert_eq!(result.rows[1][1], ScalarValue::Int(4)); // 1+3
        assert_eq!(result.rows[2][1], ScalarValue::Int(3)); // 1+2
    });
}

#[test]
fn test_like_escape() {
    let result = run("SELECT 'abc%def' LIKE 'abc!%def' ESCAPE '!'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));

    let result = run("SELECT 'abc_def' LIKE 'abc!_def' ESCAPE '!'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));

    let result = run("SELECT 'abc%def' LIKE 'abc%def' ESCAPE '!'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));

    let result = run("SELECT 'abcXdef' LIKE 'abc!%def' ESCAPE '!'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(false));

    // Test with backslash escape
    let result = run("SELECT 'abc\\def' LIKE 'abc\\\\def' ESCAPE '\\'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));

    // Test with custom escape character
    let result = run("SELECT 'test#%data' LIKE 'test##%data' ESCAPE '#'");
    assert_eq!(result.rows[0][0], ScalarValue::Bool(true));
}

#[test]
fn test_like_escape_null() {
    let result = run("SELECT 'abc' LIKE 'abc' ESCAPE NULL");
    assert_eq!(result.rows[0][0], ScalarValue::Null);
}

#[test]
fn test_chr_unicode() {
    // Test ASCII
    let result = run("SELECT chr(65)");
    assert_eq!(result.rows[0][0], ScalarValue::Text("A".to_string()));

    // Test newline
    let result = run("SELECT chr(10)");
    assert_eq!(result.rows[0][0], ScalarValue::Text("\n".to_string()));

    // Test Unicode beyond ASCII (Euro sign)
    let result = run("SELECT chr(8364)");
    assert_eq!(result.rows[0][0], ScalarValue::Text("€".to_string()));

    // Test emoji (smiley face)
    let result = run("SELECT chr(128512)");
    assert_eq!(result.rows[0][0], ScalarValue::Text("😀".to_string()));

    // Test null character
    let result = run("SELECT chr(0)");
    assert_eq!(result.rows[0][0], ScalarValue::Text("\0".to_string()));
}

#[test]
fn test_chr_invalid() {
    with_isolated_state(|| {
        // Negative numbers should fail
        let statement = parse_statement("SELECT chr(-1)").unwrap();
        let planned = plan_statement(statement).unwrap();
        let result = block_on(execute_planned_query(&planned, &[]));
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("out of range"));

        // Invalid Unicode code point (above valid range)
        let statement = parse_statement("SELECT chr(1114112)").unwrap(); // 0x110000, first invalid code point
        let planned = plan_statement(statement).unwrap();
        let result = block_on(execute_planned_query(&planned, &[]));
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("out of range"));
    });
}

// Phase 12: Date/Time/Timestamp Integration Tests

#[test]
fn test_date_parsing_formats() {
    // Test ISO format
    let result = run("SELECT date '1999-01-08'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );

    // Test month name format
    let result = run("SELECT date 'January 8, 1999'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );

    // Test compact format YYYYMMDD
    let result = run("SELECT date '19990108'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );

    // Test compact format YYMMDD
    let result = run("SELECT date '990108'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );

    // Test year.day format
    let result = run("SELECT date '1999.008'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );

    // Test Julian date
    let result = run("SELECT date 'J2451187'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1999-01-08".to_string())
    );
}

#[test]
fn test_date_bc_format() {
    let result = run("SELECT date '2040-04-10 BC'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("-2039-04-10".to_string())
    );
}

#[test]
fn test_time_with_microseconds() {
    // Test time with full microsecond precision
    let result = run("SELECT '23:59:59.999999'::time");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("23:59:59.999999".to_string())
    );

    // Test time with rounding to next second
    let result = run("SELECT '23:59:59.9999999'::time");
    assert_eq!(result.rows[0][0], ScalarValue::Text("24:00:00".to_string()));

    // Test time with microseconds that get truncated to fewer digits
    let result = run("SELECT '12:30:45.12'::time");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("12:30:45.12".to_string())
    );
}

#[test]
fn test_time_edge_cases() {
    // Test midnight as 24:00:00
    let result = run("SELECT '24:00:00'::time");
    assert_eq!(result.rows[0][0], ScalarValue::Text("24:00:00".to_string()));

    // Test leap second rounding
    let result = run("SELECT '23:59:60'::time");
    assert_eq!(result.rows[0][0], ScalarValue::Text("24:00:00".to_string()));
}

#[test]
fn test_time_am_pm() {
    let result = run("SELECT '11:59:59.99 PM'::time");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("23:59:59.99".to_string())
    );

    let result = run("SELECT '12:00:00 AM'::time");
    assert_eq!(result.rows[0][0], ScalarValue::Text("00:00:00".to_string()));

    let result = run("SELECT '12:00:00 PM'::time");
    assert_eq!(result.rows[0][0], ScalarValue::Text("12:00:00".to_string()));
}

#[test]
fn test_extract_microsecond() {
    let result = run("SELECT EXTRACT(MICROSECOND FROM TIME '13:30:25.575401')");
    assert_eq!(result.rows[0][0], ScalarValue::Int(25575401));
}

#[test]
fn test_extract_millisecond() {
    let result = run("SELECT EXTRACT(MILLISECOND FROM TIME '13:30:25.575401')");
    assert_eq!(result.rows[0][0], ScalarValue::Float(25575.401));
}

#[test]
fn test_extract_second_fractional() {
    let result = run("SELECT EXTRACT(SECOND FROM TIME '13:30:25.575401')");
    assert_eq!(result.rows[0][0], ScalarValue::Float(25.575401));
}

#[test]
fn test_special_datetime_values() {
    // Test epoch
    let result = run("SELECT date 'epoch'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1970-01-01".to_string())
    );

    // Test timestamp epoch
    let result = run("SELECT timestamp 'epoch'");
    assert_eq!(
        result.rows[0][0],
        ScalarValue::Text("1970-01-01 00:00:00".to_string())
    );
}
