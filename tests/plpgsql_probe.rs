use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

fn parse_dollar_tag(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'$' => return Some(i - start + 1),
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' => i += 1,
            _ => return None,
        }
    }
    None
}

fn split_sql_statements_with_line(sql: &str) -> Vec<(usize, String)> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut statement_start = 0usize;
    let mut i = 0usize;

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut block_comment_depth = 0usize;
    let mut dollar_tag: Option<Vec<u8>> = None;

    while i < bytes.len() {
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if block_comment_depth > 0 {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                block_comment_depth += 1;
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                block_comment_depth -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(tag) = dollar_tag.as_ref() {
            let tag_len = tag.len();
            let matches_tag =
                i + tag_len <= bytes.len() && &bytes[i..i + tag_len] == tag.as_slice();
            if matches_tag {
                dollar_tag = None;
                i += tag_len;
                continue;
            }
            i += 1;
            continue;
        }
        if in_single_quote {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            block_comment_depth = 1;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            in_single_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            in_double_quote = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'$' {
            if let Some(tag_len) = parse_dollar_tag(bytes, i) {
                dollar_tag = Some(bytes[i..i + tag_len].to_vec());
                i += tag_len;
                continue;
            }
        }

        if bytes[i] == b';' {
            let statement = sql[statement_start..i].trim();
            if !statement.is_empty() {
                let line = sql[..statement_start]
                    .bytes()
                    .filter(|b| *b == b'\n')
                    .count()
                    + 1;
                statements.push((line, statement.to_string()));
            }
            statement_start = i + 1;
        }
        i += 1;
    }

    let trailing = sql[statement_start..].trim();
    if !trailing.is_empty() {
        let line = sql[..statement_start]
            .bytes()
            .filter(|b| *b == b'\n')
            .count()
            + 1;
        statements.push((line, trailing.to_string()));
    }
    statements
}

#[test]
fn plpgsql_failure_probe() {
    let sql = std::fs::read_to_string("tests/regression/pg_compat/sql/plpgsql.sql").unwrap();
    let statements = split_sql_statements_with_line(&sql);
    let mut session = PostgresSession::new();

    let mut ok = 0usize;
    let mut err = 0usize;
    for (idx, (line, statement)) in statements.iter().enumerate() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            session.run_sync([FrontendMessage::Query {
                sql: statement.clone(),
            }])
        }));
        match result {
            Ok(messages) => {
                let mut first_error: Option<String> = None;
                for msg in &messages {
                    if let BackendMessage::ErrorResponse { message, .. } = msg {
                        first_error = Some(message.clone());
                        break;
                    }
                }
                if let Some(message) = first_error {
                    err += 1;
                    if err <= 220 {
                        let first_line = statement.lines().next().unwrap_or("").trim();
                        println!(
                            "FAIL #{err} stmt={} line={} msg={} sql={}",
                            idx + 1,
                            line,
                            message,
                            first_line
                        );
                    }
                } else {
                    ok += 1;
                }
            }
            Err(_) => {
                err += 1;
                if err <= 220 {
                    let first_line = statement.lines().next().unwrap_or("").trim();
                    println!(
                        "PANIC #{err} stmt={} line={} sql={}",
                        idx + 1,
                        line,
                        first_line
                    );
                }
                session = PostgresSession::new();
            }
        }
    }
    println!("SUMMARY ok={} err={} total={}", ok, err, ok + err);
}
