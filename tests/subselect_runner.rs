use postgrust::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[test]
fn run_subselect_sql() {
    let sql = std::fs::read_to_string("tests/regression/pg_compat/sql/subselect.sql").unwrap();
    let mut session = PostgresSession::new();
    
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_function = false;
    
    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("--") && current.trim().is_empty() {
            continue;
        }
        
        // Track $$ blocks (function bodies)
        let dollar_count = trimmed.matches("$$").count();
        if dollar_count % 2 == 1 {
            in_function = !in_function;
        }
        
        current.push_str(line);
        current.push('\n');
        
        if trimmed.ends_with(';') && !in_function {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() && !stmt.starts_with("--") {
                statements.push(stmt);
            }
            current.clear();
        }
    }
    
    let mut passed = 0;
    let mut failed = 0;
    let mut errors = Vec::new();
    
    for (i, stmt) in statements.iter().enumerate() {
        let msgs = session.run_sync([FrontendMessage::Query { sql: stmt.clone() }]);
        let has_error = msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
        if has_error {
            failed += 1;
            let err_msg = msgs.iter().find_map(|m| {
                if let BackendMessage::ErrorResponse { message, .. } = m { Some(message.clone()) } else { None }
            }).unwrap_or_default();
            errors.push((i+1, stmt.lines().next().unwrap_or("").to_string(), err_msg));
        } else {
            passed += 1;
        }
    }
    
    println!("\n=== Subselect Results ===");
    println!("Passed: {}/{}", passed, passed + failed);
    println!("Pass rate: {}%", (passed * 100) / (passed + failed));
    println!("\nAll {} errors:", errors.len());
    for (i, stmt, err) in errors.iter() {
        println!("  #{}: {} -> {}", i, &stmt[..stmt.len().min(70)], err);
    }
    
    // Don't fail the test - just report
    assert!(passed >= 87, "Regression: pass rate dropped below baseline of 87");
}
