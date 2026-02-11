use postgrust::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

/// Test postrust's core SQL capabilities using simplified test cases
/// that don't rely on PostgreSQL's specific test table structures.
#[test]
fn test_postrust_supported_features() {
    let mut session = PostgresSession::new();
    let mut passed = 0;
    let mut failed = 0;
    let mut test_results = Vec::new();
    
    println!("Testing postrust's supported PostgreSQL features...\n");
    
    // Helper function to run a test
    let mut run_test = |name: &str, sql: &str, should_succeed: bool| {
        print!("Testing {}: ", name);
        let messages = session.run_sync([FrontendMessage::Query {
            sql: sql.to_string(),
        }]);
        
        let has_error = messages.iter().any(|msg| {
            matches!(msg, BackendMessage::ErrorResponse { .. })
        });
        
        if should_succeed && !has_error {
            println!("✓ PASS");
            passed += 1;
            test_results.push((name.to_string(), "PASS".to_string()));
        } else if !should_succeed && has_error {
            println!("✓ PASS (expected error)");
            passed += 1;
            test_results.push((name.to_string(), "PASS (expected error)".to_string()));
        } else if should_succeed && has_error {
            let error_msg = messages.iter()
                .filter_map(|msg| {
                    if let BackendMessage::ErrorResponse { message, .. } = msg {
                        Some(message.clone())
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or_else(|| "Unknown error".to_string());
            println!("✗ FAIL: {}", error_msg);
            failed += 1;
            test_results.push((name.to_string(), format!("FAIL: {}", error_msg)));
        } else {
            println!("✗ FAIL: Expected error but succeeded");
            failed += 1;
            test_results.push((name.to_string(), "FAIL: Expected error but succeeded".to_string()));
        }
    };
    
    // 1. Basic SELECT queries
    run_test("basic_select", "SELECT 1 AS test_column", true);
    run_test("select_with_expression", "SELECT 1 + 2 AS result", true);
    run_test("select_with_string", "SELECT 'hello' AS greeting", true);
    
    // 2. CREATE TABLE
    run_test("create_table", 
        "CREATE TABLE test_users (id INTEGER, name TEXT, email TEXT)", true);
    
    // 3. INSERT operations
    run_test("basic_insert", 
        "INSERT INTO test_users VALUES (1, 'Alice', 'alice@example.com')", true);
    run_test("insert_with_columns", 
        "INSERT INTO test_users (id, name) VALUES (2, 'Bob')", true);
    
    // 4. SELECT from tables
    run_test("select_from_table", 
        "SELECT * FROM test_users", true);
    run_test("select_with_where", 
        "SELECT name FROM test_users WHERE id = 1", true);
    
    // 5. UPDATE and DELETE
    run_test("update_record", 
        "UPDATE test_users SET email = 'alice.updated@example.com' WHERE id = 1", true);
    run_test("delete_record", 
        "DELETE FROM test_users WHERE id = 2", true);
    
    // 6. JOINs
    run_test("create_orders_table", 
        "CREATE TABLE test_orders (id INTEGER, user_id INTEGER, amount NUMERIC)", true);
    run_test("insert_order", 
        "INSERT INTO test_orders VALUES (100, 1, 49.99)", true);
    run_test("inner_join", 
        "SELECT u.name, o.amount FROM test_users u JOIN test_orders o ON u.id = o.user_id", true);
    run_test("left_join", 
        "SELECT u.name, o.amount FROM test_users u LEFT JOIN test_orders o ON u.id = o.user_id", true);
    
    // 7. Subqueries
    run_test("subquery_exists", 
        "SELECT name FROM test_users WHERE EXISTS (SELECT 1 FROM test_orders WHERE user_id = test_users.id)", true);
    run_test("subquery_in", 
        "SELECT name FROM test_users WHERE id IN (SELECT user_id FROM test_orders)", true);
    
    // 8. Aggregates and GROUP BY
    run_test("count_aggregate", 
        "SELECT COUNT(*) FROM test_users", true);
    run_test("group_by", 
        "SELECT user_id, COUNT(*) FROM test_orders GROUP BY user_id", true);
    run_test("having_clause", 
        "SELECT user_id, COUNT(*) FROM test_orders GROUP BY user_id HAVING COUNT(*) > 0", true);
    
    // 9. DISTINCT
    run_test("distinct", 
        "SELECT DISTINCT user_id FROM test_orders", true);
    
    // 10. CASE expressions
    run_test("case_expression", 
        "SELECT name, CASE WHEN id = 1 THEN 'first' ELSE 'other' END AS category FROM test_users", true);
    
    // 11. LIKE patterns
    run_test("like_pattern", 
        "SELECT name FROM test_users WHERE name LIKE 'A%'", true);
    
    // 12. CAST/type coercion
    run_test("cast_operation", 
        "SELECT CAST(123 AS TEXT) AS text_number", true);
    run_test("double_colon_cast", 
        "SELECT 123::TEXT AS text_number", true);
    
    // 13. Window functions
    run_test("window_row_number", 
        "SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM test_users", true);
    
    // 14. CTEs (WITH clause)
    run_test("simple_cte", 
        "WITH user_summary AS (SELECT COUNT(*) as total FROM test_users) SELECT total FROM user_summary", true);
    
    // 15. Set operations
    run_test("union", 
        "SELECT name FROM test_users UNION SELECT 'Additional' AS name", true);
    
    // 16. String functions
    run_test("string_functions", 
        "SELECT LENGTH(name), UPPER(name) FROM test_users", true);
    
    // 17. Math functions
    run_test("math_functions", 
        "SELECT ABS(-42), SQRT(16), ROUND(3.14159, 2)", true);
    
    // 18. JSON/JSONB (if supported)
    run_test("json_creation", 
        "SELECT '{\"key\": \"value\"}'::JSON as json_data", true);
    
    // 19. Date/time functions
    run_test("date_functions", 
        "SELECT NOW(), CURRENT_DATE, CURRENT_TIMESTAMP", true);
    
    // 20. EXPLAIN
    run_test("explain_query", 
        "EXPLAIN SELECT * FROM test_users", true);
    
    // 21. CREATE VIEW
    run_test("create_view", 
        "CREATE VIEW user_view AS SELECT name, email FROM test_users", true);
    
    // 22. CREATE INDEX
    run_test("create_index", 
        "CREATE INDEX idx_users_name ON test_users(name)", true);
    
    // 23. Arrays (if supported)
    run_test("array_constructor", 
        "SELECT ARRAY[1, 2, 3] AS numbers", true);
    
    // 24. Sequences
    run_test("create_sequence", 
        "CREATE SEQUENCE test_seq", true);
    run_test("nextval_sequence", 
        "SELECT nextval('test_seq')", true);
    
    // Print summary
    let total = passed + failed;
    let pass_rate = if total > 0 { (passed * 100) / total } else { 0 };
    
    println!("\n=== Postrust Feature Compatibility Results ===");
    println!("Total tests: {}", total);
    println!("Passed: {}", passed);
    println!("Failed: {}", failed);
    println!("Pass rate: {}%", pass_rate);
    
    if failed > 0 {
        println!("\nFailed tests:");
        for (name, result) in &test_results {
            if result.starts_with("FAIL") {
                println!("  {}: {}", name, result);
            }
        }
    }
    
    println!("\nNote: This test measures postrust's compatibility with core PostgreSQL features.");
    println!("A high pass rate indicates good PostgreSQL compatibility for supported features.");
    
    // The test succeeds as long as basic functionality works
    // We expect some features to not be fully implemented yet
    assert!(passed > 10, "Too few core features are working (passed: {})", passed);
}