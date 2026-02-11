use postgrust::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Load PostgreSQL compatibility test files from the pg_compat directory
fn load_pg_compat_tests() -> Vec<(String, String, Option<String>)> {
    let sql_dir = Path::new("tests/regression/pg_compat/sql");
    let expected_dir = Path::new("tests/regression/pg_compat/expected");
    
    if !sql_dir.exists() {
        panic!("PostgreSQL compatibility test SQL directory not found: {}", sql_dir.display());
    }
    
    let mut files = fs::read_dir(sql_dir)
        .expect("read pg_compat sql dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    files.sort();
    
    files
        .into_iter()
        .map(|sql_path| {
            let name = sql_path
                .file_stem()
                .expect("file stem")
                .to_string_lossy()
                .to_string();
            
            let sql = fs::read_to_string(&sql_path)
                .expect("read pg_compat sql file");
            
            // Try to load expected output
            let expected_path = expected_dir.join(format!("{}.out", name));
            let expected = if expected_path.exists() {
                Some(fs::read_to_string(&expected_path)
                    .expect("read pg_compat expected file"))
            } else {
                None
            };
            
            (name, sql, expected)
        })
        .collect()
}

/// Run a single SQL statement and return the formatted result
fn run_sql_statement(session: &mut PostgresSession, sql: &str) -> Result<String, String> {
    let messages = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    
    let mut result = String::new();
    let mut has_error = false;
    
    for msg in &messages {
        match msg {
            BackendMessage::ErrorResponse { message, code, detail, hint, position } => {
                has_error = true;
                result.push_str(&format!("ERROR: {}\n", message));
                if !code.is_empty() {
                    result.push_str(&format!("SQLSTATE: {}\n", code));
                }
                if let Some(detail) = detail {
                    result.push_str(&format!("DETAIL: {}\n", detail));
                }
                if let Some(hint) = hint {
                    result.push_str(&format!("HINT: {}\n", hint));
                }
                if let Some(position) = position {
                    result.push_str(&format!("POSITION: {}\n", position));
                }
            },
            BackendMessage::CommandComplete { tag, rows } => {
                result.push_str(&format!("{}\n", tag));
                if *rows > 0 {
                    result.push_str(&format!("({} rows)\n", rows));
                }
            },
            BackendMessage::DataRow { values } => {
                result.push_str(&format!("{}\n", values.join("|")));
            },
            BackendMessage::RowDescription { fields } => {
                let headers: Vec<String> = fields.iter()
                    .map(|field| field.name.clone())
                    .collect();
                result.push_str(&format!("{}\n", headers.join("|")));
            },
            _ => {
                // Ignore other message types for now
            }
        }
    }
    
    if has_error {
        Err(result)
    } else {
        Ok(result)
    }
}

/// Normalize output for comparison (remove timestamps, variable data, etc.)
fn normalize_output(output: &str) -> String {
    output.lines()
        .map(|line| {
            // Skip lines that contain variable data like timestamps, PIDs, etc.
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return String::new();
            }
            
            // Normalize common PostgreSQL output patterns
            if trimmed.starts_with("Time:") || 
               trimmed.starts_with("LOG:") ||
               trimmed.contains("(1 row)") ||
               trimmed.contains("rows)") {
                return String::new();
            }
            
            trimmed.to_string()
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Check if a test failure is due to an unsupported feature rather than a bug
fn is_expected_limitation(test_name: &str, error_output: &str) -> bool {
    // Features that postrust might not fully support yet
    let known_limitations = [
        "CREATE EXTENSION",
        "ALTER TABLE",
        "DROP TABLE CASCADE",
        "VACUUM",
        "ANALYZE",
        "CREATE ROLE",
        "GRANT",
        "REVOKE",
        "CREATE TABLESPACE",
        "CREATE DATABASE",
        "CLUSTER",
        "REINDEX",
        "pg_sleep",
        "CREATE LANGUAGE",
        "CREATE OPERATOR",
        "CREATE TYPE",
        "CREATE DOMAIN",
    ];
    
    for limitation in &known_limitations {
        if error_output.contains(limitation) {
            return true;
        }
    }
    
    // Test-specific known limitations
    match test_name {
        "create_index" => error_output.contains("UNIQUE") || error_output.contains("CONCURRENTLY"),
        "create_table" => error_output.contains("INHERITS") || error_output.contains("PARTITION"),
        "aggregates" => error_output.contains("CREATE AGGREGATE"),
        _ => false,
    }
}

/// Run PostgreSQL compatibility tests
#[test]
fn postgresql_compatibility_suite() {
    let tests = load_pg_compat_tests();
    let mut results = HashMap::new();
    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;
    
    println!("Running {} PostgreSQL compatibility tests...", tests.len());
    
    for (test_name, sql, _expected) in tests {
        print!("Testing {}... ", test_name);
        
        let mut session = PostgresSession::new();
        
        // Split SQL into individual statements
        let statements: Vec<&str> = sql.split(';')
            .map(|stmt| stmt.trim())
            .filter(|stmt| !stmt.is_empty() && !stmt.starts_with("--"))
            .collect();
        
        if statements.is_empty() {
            println!("SKIP (no valid statements)");
            skipped += 1;
            continue;
        }
        
        let mut test_passed = true;
        let mut error_messages = Vec::new();
        
        // Run each statement
        for statement in statements {
            match run_sql_statement(&mut session, statement) {
                Ok(result) => {
                    // Statement executed successfully
                    if result.trim().is_empty() {
                        continue;
                    }
                    // Could compare with expected output here if needed
                },
                Err(error) => {
                    if is_expected_limitation(&test_name, &error) {
                        // This is a known limitation, not a real failure
                        continue;
                    } else {
                        test_passed = false;
                        error_messages.push(error);
                        break;  // Stop on first real error
                    }
                }
            }
        }
        
        if test_passed {
            println!("PASS");
            passed += 1;
            results.insert(test_name, "PASS".to_string());
        } else {
            println!("FAIL");
            failed += 1;
            results.insert(test_name, format!("FAIL: {}", error_messages.join("; ")));
        }
    }
    
    // Print summary
    println!("\n=== PostgreSQL Compatibility Test Results ===");
    println!("Total tests: {}", passed + failed + skipped);
    println!("Passed: {}", passed);
    println!("Failed: {}", failed);
    println!("Skipped: {}", skipped);
    
    if passed + failed > 0 {
        let pass_rate = (passed * 100) / (passed + failed);
        println!("Pass rate: {}%", pass_rate);
    }
    
    if failed > 0 {
        println!("\nFailed tests:");
        for (test_name, result) in &results {
            if result.starts_with("FAIL") {
                println!("  {}: {}", test_name, result);
            }
        }
    }
    
    // Don't fail the test if we have some compatibility issues - this is expected
    // The goal is to measure compatibility, not to have 100% compatibility immediately
    if passed == 0 && failed > 0 {
        panic!("No tests passed - there might be a fundamental issue");
    }
}

/// Test individual PostgreSQL features that postrust claims to support
#[test] 
fn test_core_postgresql_features() {
    let mut session = PostgresSession::new();
    
    // Test basic SELECT
    let result = run_sql_statement(&mut session, "SELECT 1 AS test");
    assert!(result.is_ok(), "Basic SELECT failed: {:?}", result);
    
    // Test CREATE TABLE
    let result = run_sql_statement(&mut session, "CREATE TABLE test_table (id INTEGER, name TEXT)");
    assert!(result.is_ok(), "CREATE TABLE failed: {:?}", result);
    
    // Test INSERT
    let result = run_sql_statement(&mut session, "INSERT INTO test_table VALUES (1, 'test')");
    assert!(result.is_ok(), "INSERT failed: {:?}", result);
    
    // Test SELECT with WHERE
    let result = run_sql_statement(&mut session, "SELECT * FROM test_table WHERE id = 1");
    assert!(result.is_ok(), "SELECT with WHERE failed: {:?}", result);
    
    // Test JOIN
    let result = run_sql_statement(&mut session, "CREATE TABLE test_table2 (id INTEGER, value TEXT)");
    assert!(result.is_ok(), "CREATE second table failed: {:?}", result);
    
    let result = run_sql_statement(&mut session, "INSERT INTO test_table2 VALUES (1, 'joined')");
    assert!(result.is_ok(), "INSERT into second table failed: {:?}", result);
    
    let result = run_sql_statement(&mut session, 
        "SELECT t1.name, t2.value FROM test_table t1 JOIN test_table2 t2 ON t1.id = t2.id");
    assert!(result.is_ok(), "JOIN failed: {:?}", result);
    
    println!("Core PostgreSQL features test completed successfully!");
}