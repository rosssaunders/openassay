use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[test]
fn measure_arrays_pass_rate() {
    let sql = std::fs::read_to_string("tests/regression/pg_compat/sql/arrays.sql").unwrap();

    let statements: Vec<&str> = sql
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with("--") && !s.starts_with("\\"))
        .collect();

    println!("\nTotal statements: {}", statements.len());

    let mut session = PostgresSession::new();
    let mut passed = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for (i, stmt) in statements.iter().enumerate() {
        let messages = session.run_sync([FrontendMessage::Query {
            sql: stmt.to_string(),
        }]);
        let has_error = messages
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));

        if i < 5 {
            let short = if stmt.len() > 120 { &stmt[..120] } else { stmt };
            println!("  stmt#{}: {}", i, short);
        }
        if has_error {
            let err = messages
                .iter()
                .filter_map(|m| {
                    if let BackendMessage::ErrorResponse { message, .. } = m {
                        Some(message.clone())
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or_default();
            let short = if stmt.len() > 80 { &stmt[..80] } else { stmt };
            errors.push(format!("FAIL stmt#{}: {} => {}", i, short, err));
            failed += 1;
        } else {
            passed += 1;
        }
    }

    println!("\nFailed statements:");
    for e in &errors {
        println!("  {}", e);
    }

    println!(
        "\nPassed: {}/{} ({:.1}%)",
        passed,
        passed + failed,
        100.0 * passed as f64 / (passed + failed) as f64
    );
}
