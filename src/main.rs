use openassay::parser::scansup::downcase_truncate_identifier;
use openassay::parser::sql_parser::parse_statement;
use openassay::tcop::engine::{QueryResult, execute_planned_query, plan_statement};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("ident") => {
            let Some(identifier) = args.next() else {
                eprintln!("usage: openassay ident <identifier>");
                std::process::exit(2);
            };
            let transformed = downcase_truncate_identifier(&identifier, false);
            println!("{}", transformed.value);
        }
        Some("parse") => {
            let sql = args.collect::<Vec<_>>().join(" ");
            if sql.is_empty() {
                eprintln!("usage: openassay parse <sql>");
                std::process::exit(2);
            }
            match parse_statement(&sql) {
                Ok(stmt) => println!("{stmt:#?}"),
                Err(err) => {
                    eprintln!("parse error: {err}");
                    std::process::exit(1);
                }
            }
        }
        Some("exec") => {
            let exec_args = args.collect::<Vec<_>>();
            let (params, sql) = match parse_exec_invocation(exec_args) {
                Ok(v) => v,
                Err(msg) => {
                    eprintln!("{msg}");
                    eprintln!(
                        "usage: openassay exec [--param <value> | --null-param]... [--] <sql>"
                    );
                    std::process::exit(2);
                }
            };
            let results = if params.is_empty() {
                match execute_simple_query_via_session(&sql).await {
                    Ok(results) => results,
                    Err(message) => {
                        eprintln!("execution error: {message}");
                        std::process::exit(1);
                    }
                }
            } else {
                let statements = split_sql_statements(&sql);
                if statements.is_empty() {
                    eprintln!("execution error: missing SQL for exec");
                    std::process::exit(1);
                }
                let mut results = Vec::new();
                for statement_sql in &statements {
                    let stmt = match parse_statement(statement_sql) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            eprintln!("execution error: {err}");
                            std::process::exit(1);
                        }
                    };
                    let plan = match plan_statement(stmt) {
                        Ok(plan) => plan,
                        Err(err) => {
                            eprintln!("execution error: {err}");
                            std::process::exit(1);
                        }
                    };
                    let result = match execute_planned_query(&plan, &params).await {
                        Ok(result) => result,
                        Err(err) => {
                            eprintln!("execution error: {err}");
                            std::process::exit(1);
                        }
                    };
                    results.push(cli_result_from_engine(&result));
                }
                results
            };

            for (idx, result) in results.iter().enumerate() {
                if idx > 0 {
                    println!();
                }
                print_query_result(result);
            }
        }
        Some(identifier) => {
            // Backward-compatible behavior: one bare arg is treated as an identifier.
            let transformed = downcase_truncate_identifier(identifier, false);
            println!("{}", transformed.value);
        }
        None => {
            eprintln!(
                "usage:\n  openassay ident <identifier>\n  openassay parse <sql>\n  openassay exec [--param <value> | --null-param]... [--] <sql>\n  openassay <identifier>"
            );
            std::process::exit(2);
        }
    }
}

fn parse_exec_invocation(raw_args: Vec<String>) -> Result<(Vec<Option<String>>, String), String> {
    let mut params: Vec<Option<String>> = Vec::new();
    let mut sql_tokens: Vec<String> = Vec::new();
    let mut passthrough = false;
    let mut i = 0usize;

    while i < raw_args.len() {
        let arg = &raw_args[i];
        if !passthrough && sql_tokens.is_empty() {
            match arg.as_str() {
                "--param" => {
                    i += 1;
                    if i >= raw_args.len() {
                        return Err("missing value for --param".to_string());
                    }
                    params.push(Some(raw_args[i].clone()));
                }
                "--null-param" => params.push(None),
                "--" => passthrough = true,
                _ => sql_tokens.push(arg.clone()),
            }
        } else {
            sql_tokens.push(arg.clone());
        }
        i += 1;
    }

    if sql_tokens.is_empty() {
        return Err("missing SQL for exec".to_string());
    }

    Ok((params, sql_tokens.join(" ")))
}

#[derive(Debug, Clone)]
struct CliQueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    command_tag: String,
    rows_affected: u64,
}

fn cli_result_from_engine(result: &QueryResult) -> CliQueryResult {
    CliQueryResult {
        columns: result.columns.clone(),
        rows: result
            .rows
            .iter()
            .map(|row| row.iter().map(|value| value.render()).collect())
            .collect(),
        command_tag: result.command_tag.clone(),
        rows_affected: result.rows_affected,
    }
}

async fn execute_simple_query_via_session(sql: &str) -> Result<Vec<CliQueryResult>, String> {
    let mut session = PostgresSession::new();
    let messages = session
        .run([FrontendMessage::Query {
            sql: sql.to_string(),
        }])
        .await;

    let mut results = Vec::new();
    let mut current_columns: Option<Vec<String>> = None;
    let mut current_rows: Vec<Vec<String>> = Vec::new();

    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                current_columns = Some(fields.into_iter().map(|field| field.name).collect());
                current_rows.clear();
            }
            BackendMessage::DataRow { values } => {
                current_rows.push(values);
            }
            BackendMessage::DataRowBinary { values } => {
                current_rows.push(
                    values
                        .into_iter()
                        .map(|value| match value {
                            None => "NULL".to_string(),
                            Some(bytes) => String::from_utf8(bytes.clone()).unwrap_or_else(|_| {
                                use std::fmt::Write;
                                let mut hex = String::with_capacity(2 + bytes.len() * 2);
                                hex.push_str("\\x");
                                for b in &bytes {
                                    let _ = write!(hex, "{b:02x}");
                                }
                                hex
                            }),
                        })
                        .collect(),
                );
            }
            BackendMessage::CommandComplete { tag, rows } => {
                results.push(CliQueryResult {
                    columns: current_columns.take().unwrap_or_default(),
                    rows: std::mem::take(&mut current_rows),
                    command_tag: tag,
                    rows_affected: rows,
                });
            }
            BackendMessage::ErrorResponse { message, .. } => return Err(message),
            BackendMessage::EmptyQueryResponse
            | BackendMessage::AuthenticationOk
            | BackendMessage::AuthenticationCleartextPassword
            | BackendMessage::AuthenticationSasl { .. }
            | BackendMessage::AuthenticationSaslContinue { .. }
            | BackendMessage::AuthenticationSaslFinal { .. }
            | BackendMessage::ParameterStatus { .. }
            | BackendMessage::BackendKeyData { .. }
            | BackendMessage::NoticeResponse { .. }
            | BackendMessage::ReadyForQuery { .. }
            | BackendMessage::ParseComplete
            | BackendMessage::BindComplete
            | BackendMessage::CloseComplete
            | BackendMessage::ParameterDescription { .. }
            | BackendMessage::NoData
            | BackendMessage::PortalSuspended
            | BackendMessage::CopyInResponse { .. }
            | BackendMessage::CopyOutResponse { .. }
            | BackendMessage::CopyData { .. }
            | BackendMessage::CopyDone
            | BackendMessage::FlushComplete
            | BackendMessage::Terminate
            | BackendMessage::NotificationResponse { .. } => {}
        }
    }

    Ok(results)
}

fn print_query_result(result: &CliQueryResult) {
    if result.columns.is_empty() && result.rows.is_empty() {
        println!("{} {}", result.command_tag, result.rows_affected);
        return;
    }

    if !result.columns.is_empty() {
        let mut widths: Vec<usize> = result.columns.iter().map(|c| c.len()).collect();
        for row in &result.rows {
            for (idx, value) in row.iter().enumerate() {
                if idx < widths.len() {
                    widths[idx] = widths[idx].max(value.len());
                }
            }
        }

        println!("{}", format_row(&result.columns, &widths));
        let separator = widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");
        println!("{separator}");

        for row in &result.rows {
            println!("{}", format_row(row, &widths));
        }
    } else {
        for row in &result.rows {
            println!("{}", row.join(" | "));
        }
    }

    let n = result.rows.len();
    if n == 1 {
        println!("(1 row)");
    } else {
        println!("({n} rows)");
    }
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;

    while i < chars.len() {
        let ch = chars[i];

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    current.push(chars[i + 1]);
                    i += 1;
                } else {
                    in_single = false;
                }
            }
            i += 1;
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] == '"' {
                    current.push(chars[i + 1]);
                    i += 1;
                } else {
                    in_double = false;
                }
            }
            i += 1;
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            ';' => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
        i += 1;
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }
    statements
}

fn format_row(values: &[String], widths: &[usize]) -> String {
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let width = *widths.get(idx).unwrap_or(&value.len());
            format!("{value:<width$}")
        })
        .collect::<Vec<_>>()
        .join(" | ")
}
