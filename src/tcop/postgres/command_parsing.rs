#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn parse_transaction_command(
    query: &str,
) -> Result<Option<TransactionCommand>, SessionError> {
    let tokens = query
        .split_whitespace()
        .map(|token| token.trim_matches(';'))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    let first_upper = first.to_ascii_uppercase();

    let parse_name = |token: Option<&&str>, command: &str| -> Result<String, SessionError> {
        token
            .map(|value| value.to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| SessionError {
                message: format!("{command} requires a savepoint name"),
            })
    };

    match first_upper.as_str() {
        "BEGIN" => Ok(Some(TransactionCommand::Begin)),
        "START" => Ok(Some(TransactionCommand::Begin)),
        "COMMIT" | "END" => Ok(Some(TransactionCommand::Commit)),
        "ROLLBACK" => {
            if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("TO") {
                let name = if tokens.len() >= 3 && tokens[2].eq_ignore_ascii_case("SAVEPOINT") {
                    parse_name(tokens.get(3), "ROLLBACK TO SAVEPOINT")?
                } else {
                    parse_name(tokens.get(2), "ROLLBACK TO SAVEPOINT")?
                };
                return Ok(Some(TransactionCommand::RollbackToSavepoint(name)));
            }
            Ok(Some(TransactionCommand::Rollback))
        }
        "SAVEPOINT" => {
            let name = parse_name(tokens.get(1), "SAVEPOINT")?;
            Ok(Some(TransactionCommand::Savepoint(name)))
        }
        "RELEASE" => {
            let name = if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("SAVEPOINT") {
                parse_name(tokens.get(2), "RELEASE SAVEPOINT")?
            } else {
                parse_name(tokens.get(1), "RELEASE SAVEPOINT")?
            };
            Ok(Some(TransactionCommand::ReleaseSavepoint(name)))
        }
        _ => Ok(None),
    }
}

pub(super) fn parse_security_command(query: &str) -> Result<Option<SecurityCommand>, SessionError> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if starts_with_keyword(trimmed, "SET ROLE ") {
        return parse_set_role_command(trimmed).map(Some);
    }
    if trimmed.eq_ignore_ascii_case("RESET ROLE") {
        return Ok(Some(SecurityCommand::ResetRole));
    }
    if starts_with_keyword(trimmed, "ALTER TABLE ")
        && contains_keyword(trimmed, "ROW LEVEL SECURITY")
    {
        return parse_alter_table_rls_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "CREATE POLICY ") {
        return parse_create_policy_command(trimmed).map(Some);
    }
    if starts_with_keyword(trimmed, "DROP POLICY ") {
        return parse_drop_policy_command(trimmed).map(Some);
    }

    Ok(None)
}

pub(super) fn is_parser_security_command(query: &str) -> bool {
    starts_with_keyword(query, "COPY")
        || starts_with_keyword(query, "GRANT")
        || starts_with_keyword(query, "REVOKE")
        || starts_with_keyword(query, "CREATE ROLE")
        || starts_with_keyword(query, "ALTER ROLE")
        || starts_with_keyword(query, "DROP ROLE")
}

pub(super) fn create_role_command(statement: CreateRoleStatement) -> SecurityCommand {
    let options = create_role_options(&statement.options);
    SecurityCommand::CreateRole {
        role_name: security::normalize_identifier(&statement.name),
        options,
    }
}

pub(super) fn alter_role_command(statement: AlterRoleStatement) -> SecurityCommand {
    let options = alter_role_options(&statement.options);
    SecurityCommand::AlterRole {
        role_name: security::normalize_identifier(&statement.name),
        options,
    }
}

pub(super) fn drop_role_command(statement: DropRoleStatement) -> SecurityCommand {
    SecurityCommand::DropRole {
        role_name: security::normalize_identifier(&statement.name),
        if_exists: statement.if_exists,
    }
}

pub(super) fn grant_command(statement: GrantStatement) -> Result<SecurityCommand, SessionError> {
    match statement {
        GrantStatement::Role(role) => Ok(SecurityCommand::GrantRole {
            role_name: security::normalize_identifier(&role.role_name),
            member: security::normalize_identifier(&role.member),
        }),
        GrantStatement::TablePrivileges(grant) => {
            let roles = normalize_role_list(grant.roles);
            if roles.is_empty() {
                return Err(SessionError {
                    message: "GRANT requires at least one target role".to_string(),
                });
            }
            Ok(SecurityCommand::GrantTablePrivileges {
                table_name: normalize_identifier_parts(grant.table_name),
                roles,
                privileges: map_table_privileges(grant.privileges)?,
            })
        }
    }
}

pub(super) fn revoke_command(statement: RevokeStatement) -> Result<SecurityCommand, SessionError> {
    match statement {
        RevokeStatement::Role(role) => Ok(SecurityCommand::RevokeRole {
            role_name: security::normalize_identifier(&role.role_name),
            member: security::normalize_identifier(&role.member),
        }),
        RevokeStatement::TablePrivileges(revoke) => {
            let roles = normalize_role_list(revoke.roles);
            if roles.is_empty() {
                return Err(SessionError {
                    message: "REVOKE requires at least one target role".to_string(),
                });
            }
            Ok(SecurityCommand::RevokeTablePrivileges {
                table_name: normalize_identifier_parts(revoke.table_name),
                roles,
                privileges: map_table_privileges(revoke.privileges)?,
            })
        }
    }
}

pub(super) fn copy_command(statement: CopyStatement) -> Result<CopyCommand, SessionError> {
    let AstCopyOptions {
        format,
        delimiter,
        null_marker,
        header,
    } = statement.options;
    let format = match format.unwrap_or(AstCopyFormat::Text) {
        AstCopyFormat::Text => CopyFormat::Text,
        AstCopyFormat::Csv => CopyFormat::Csv,
        AstCopyFormat::Binary => CopyFormat::Binary,
    };
    let delimiter = if let Some(delimiter) = delimiter {
        let mut chars = delimiter.chars();
        let ch = chars.next().ok_or_else(|| SessionError {
            message: "COPY DELIMITER cannot be empty".to_string(),
        })?;
        if chars.next().is_some() {
            return Err(SessionError {
                message: "COPY DELIMITER must be a single character".to_string(),
            });
        }
        ch
    } else {
        match format {
            CopyFormat::Csv => ',',
            _ => '\t',
        }
    };
    let null_marker = null_marker.unwrap_or(match format {
        CopyFormat::Csv => String::new(),
        _ => "\\N".to_string(),
    });
    Ok(CopyCommand {
        table_name: normalize_identifier_parts(statement.table_name),
        columns: normalize_identifier_parts(statement.columns),
        direction: match statement.direction {
            AstCopyDirection::To => CopyDirection::ToStdout,
            AstCopyDirection::From => CopyDirection::FromStdin,
        },
        format,
        delimiter,
        null_marker,
        header,
    })
}

fn create_role_options(options: &[RoleOption]) -> CreateRoleOptions {
    let mut out = CreateRoleOptions::default();
    for option in options {
        match option {
            RoleOption::Superuser(value) => out.superuser = *value,
            RoleOption::Login(value) => out.login = *value,
            RoleOption::Password(value) => out.password = Some(value.clone()),
        }
    }
    out
}

fn alter_role_options(options: &[RoleOption]) -> AlterRoleOptions {
    let mut out = AlterRoleOptions::default();
    for option in options {
        match option {
            RoleOption::Superuser(value) => out.superuser = Some(*value),
            RoleOption::Login(value) => out.login = Some(*value),
            RoleOption::Password(value) => out.password = Some(value.clone()),
        }
    }
    out
}

fn map_table_privileges(
    privileges: Vec<TablePrivilegeKind>,
) -> Result<Vec<TablePrivilege>, SessionError> {
    if privileges.is_empty() {
        return Err(SessionError {
            message: "no privileges specified".to_string(),
        });
    }
    let mut mapped = privileges
        .into_iter()
        .map(|privilege| match privilege {
            TablePrivilegeKind::Select => TablePrivilege::Select,
            TablePrivilegeKind::Insert => TablePrivilege::Insert,
            TablePrivilegeKind::Update => TablePrivilege::Update,
            TablePrivilegeKind::Delete => TablePrivilege::Delete,
            TablePrivilegeKind::Truncate => TablePrivilege::Truncate,
        })
        .collect::<Vec<_>>();
    mapped.sort_by_key(|privilege| *privilege as u8);
    mapped.dedup();
    Ok(mapped)
}

fn normalize_identifier_parts(parts: Vec<String>) -> Vec<String> {
    parts
        .into_iter()
        .map(|part| security::normalize_identifier(&part))
        .filter(|part| !part.is_empty())
        .collect()
}

fn normalize_role_list(roles: Vec<String>) -> Vec<String> {
    roles
        .into_iter()
        .map(|role| security::normalize_identifier(&role))
        .filter(|role| !role.is_empty())
        .collect()
}

fn parse_set_role_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(SessionError {
            message: "SET ROLE requires a role name".to_string(),
        });
    }
    Ok(SecurityCommand::SetRole {
        role_name: security::normalize_identifier(tokens[2]),
    })
}

fn parse_alter_table_rls_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 7 {
        return Err(SessionError {
            message: "ALTER TABLE ... ROW LEVEL SECURITY command is incomplete".to_string(),
        });
    }
    if !tokens[0].eq_ignore_ascii_case("ALTER")
        || !tokens[1].eq_ignore_ascii_case("TABLE")
        || !tokens[4].eq_ignore_ascii_case("ROW")
        || !tokens[5].eq_ignore_ascii_case("LEVEL")
        || !tokens[6].eq_ignore_ascii_case("SECURITY")
    {
        return Err(SessionError {
            message: "unsupported ALTER TABLE command".to_string(),
        });
    }
    let enabled = if tokens[3].eq_ignore_ascii_case("ENABLE") {
        true
    } else if tokens[3].eq_ignore_ascii_case("DISABLE") {
        false
    } else {
        return Err(SessionError {
            message: "ALTER TABLE expects ENABLE or DISABLE for ROW LEVEL SECURITY".to_string(),
        });
    };

    Ok(SecurityCommand::SetRowLevelSecurity {
        table_name: security::parse_qualified_name(tokens[2]),
        enabled,
    })
}

fn parse_create_policy_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let rest = strip_prefix_keyword(query, "CREATE POLICY ").ok_or_else(|| SessionError {
        message: "CREATE POLICY syntax error".to_string(),
    })?;
    let (policy_name, after_name) = split_once_whitespace(rest).ok_or_else(|| SessionError {
        message: "CREATE POLICY requires a policy name".to_string(),
    })?;
    let after_name = after_name.trim_start();
    let after_on = strip_prefix_keyword(after_name, "ON ").ok_or_else(|| SessionError {
        message: "CREATE POLICY requires ON <table>".to_string(),
    })?;
    let (table_name_text, mut tail) =
        split_until_keywords(after_on, &[" FOR ", " TO ", " USING ", " WITH CHECK "]);
    if table_name_text.is_empty() {
        return Err(SessionError {
            message: "CREATE POLICY requires a table name".to_string(),
        });
    }

    let mut command = RlsCommand::All;
    let mut roles = vec!["public".to_string()];
    let mut using_expr = None;
    let mut check_expr = None;

    while !tail.trim().is_empty() {
        if let Some(after_for) = strip_prefix_keyword(tail, "FOR ") {
            let (cmd, next) = split_once_whitespace(after_for).unwrap_or((after_for, ""));
            let cmd_upper = cmd.trim().to_ascii_uppercase();
            command = RlsCommand::from_keyword(&cmd_upper).ok_or_else(|| SessionError {
                message: format!("unsupported policy command {cmd}"),
            })?;
            tail = next;
            continue;
        }
        if let Some(after_to) = strip_prefix_keyword(tail, "TO ") {
            let (role_text, next) = split_until_keywords(after_to, &[" USING ", " WITH CHECK "]);
            let parsed_roles = parse_identifier_list(role_text);
            if parsed_roles.is_empty() {
                return Err(SessionError {
                    message: "CREATE POLICY TO requires at least one role".to_string(),
                });
            }
            roles = parsed_roles;
            tail = next;
            continue;
        }
        if let Some(after_using) = strip_prefix_keyword(tail, "USING ") {
            let (expr_text, consumed) = extract_parenthesized_expression(after_using)?;
            using_expr = Some(parse_policy_predicate(expr_text)?);
            tail = &after_using[consumed..];
            continue;
        }
        if let Some(after_check) = strip_prefix_keyword(tail, "WITH CHECK ") {
            let (expr_text, consumed) = extract_parenthesized_expression(after_check)?;
            check_expr = Some(parse_policy_predicate(expr_text)?);
            tail = &after_check[consumed..];
            continue;
        }
        return Err(SessionError {
            message: "unsupported CREATE POLICY clause".to_string(),
        });
    }

    Ok(SecurityCommand::CreatePolicy {
        policy_name: security::normalize_identifier(policy_name),
        table_name: security::parse_qualified_name(table_name_text),
        command,
        roles,
        using_expr,
        check_expr,
    })
}

fn parse_drop_policy_command(query: &str) -> Result<SecurityCommand, SessionError> {
    let rest = strip_prefix_keyword(query, "DROP POLICY ").ok_or_else(|| SessionError {
        message: "DROP POLICY syntax error".to_string(),
    })?;
    let mut if_exists = false;
    let rest = if let Some(after_if_exists) = strip_prefix_keyword(rest, "IF EXISTS ") {
        if_exists = true;
        after_if_exists
    } else {
        rest
    };
    let upper = rest.to_ascii_uppercase();
    let on_pos = upper.find(" ON ").ok_or_else(|| SessionError {
        message: "DROP POLICY requires ON <table>".to_string(),
    })?;
    let policy_name = rest[..on_pos].trim();
    let table_name = rest[on_pos + " ON ".len()..].trim();
    if policy_name.is_empty() || table_name.is_empty() {
        return Err(SessionError {
            message: "DROP POLICY requires policy and table names".to_string(),
        });
    }
    Ok(SecurityCommand::DropPolicy {
        policy_name: security::normalize_identifier(policy_name),
        table_name: security::parse_qualified_name(table_name),
        if_exists,
    })
}

fn parse_policy_predicate(raw: &str) -> Result<Expr, SessionError> {
    let sql = format!("SELECT 1 WHERE {raw}");
    let statement = parse_statement(&sql).map_err(|err| SessionError {
        message: format!("parse error: {err}"),
    })?;
    let Statement::Query(query) = statement else {
        return Err(SessionError {
            message: "policy expression must be a valid SQL predicate".to_string(),
        });
    };
    let QueryExpr::Select(select) = query.body else {
        return Err(SessionError {
            message: "policy expression must be a SELECT predicate".to_string(),
        });
    };
    select.where_clause.ok_or_else(|| SessionError {
        message: "policy expression is empty".to_string(),
    })
}

fn parse_identifier_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(security::normalize_identifier)
        .filter(|name| !name.is_empty())
        .collect()
}

fn starts_with_keyword(haystack: &str, keyword: &str) -> bool {
    haystack.len() >= keyword.len() && haystack[..keyword.len()].eq_ignore_ascii_case(keyword)
}

fn strip_prefix_keyword<'a>(haystack: &'a str, keyword: &str) -> Option<&'a str> {
    starts_with_keyword(haystack, keyword).then_some(&haystack[keyword.len()..])
}

fn contains_keyword(haystack: &str, keyword: &str) -> bool {
    haystack
        .to_ascii_uppercase()
        .contains(&keyword.to_ascii_uppercase())
}

fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim_start();
    let split = trimmed.find(char::is_whitespace)?;
    let left = trimmed[..split].trim();
    let right = trimmed[split..].trim_start();
    Some((left, right))
}

fn split_until_keywords<'a>(input: &'a str, keywords: &[&str]) -> (&'a str, &'a str) {
    let trimmed = input.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    let mut next_idx = trimmed.len();
    for keyword in keywords {
        let keyword_upper = keyword.to_ascii_uppercase();
        if let Some(idx) = upper.find(&keyword_upper)
            && idx < next_idx
        {
            next_idx = idx;
        }
    }
    if next_idx == trimmed.len() {
        (trimmed.trim(), "")
    } else {
        (trimmed[..next_idx].trim(), trimmed[next_idx..].trim_start())
    }
}

fn extract_parenthesized_expression(input: &str) -> Result<(&str, usize), SessionError> {
    let trimmed = input.trim_start();
    let leading_ws = input.len() - trimmed.len();
    if !trimmed.starts_with('(') {
        return Err(SessionError {
            message: "expected parenthesized expression".to_string(),
        });
    }

    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let chars = trimmed.char_indices().collect::<Vec<_>>();
    let mut end_idx = None;
    let mut i = 0usize;
    while i < chars.len() {
        let (byte_idx, ch) = chars[i];
        if in_single {
            if ch == '\'' {
                if i + 1 < chars.len() && chars[i + 1].1 == '\'' {
                    i += 1;
                } else {
                    in_single = false;
                }
            }
            i += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1].1 == '"' {
                    i += 1;
                } else {
                    in_double = false;
                }
            }
            i += 1;
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Err(SessionError {
                        message: "invalid parenthesized expression".to_string(),
                    });
                }
                depth -= 1;
                if depth == 0 {
                    end_idx = Some(byte_idx);
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let end_idx = end_idx.ok_or_else(|| SessionError {
        message: "unterminated parenthesized expression".to_string(),
    })?;
    let expr = &trimmed[1..end_idx];
    let consumed = leading_ws + end_idx + 1;
    Ok((expr.trim(), consumed))
}

pub(super) fn first_keyword_uppercase(query: &str) -> Option<String> {
    query
        .split_whitespace()
        .next()
        .map(|kw| kw.trim_matches(';').to_ascii_uppercase())
}

/// Split a SimpleQuery SQL string into individual statements using the lexer.
///
/// This delegates to `lex_sql` which already handles comments (`--`, `/* */`),
/// string literals, dollar-quoted strings, and all other SQL syntax. We simply
/// look for `Semicolon` tokens and extract the source text between them.
pub(super) fn split_simple_query_statements(query: &str) -> Vec<String> {
    use crate::parser::lexer::{TokenKind, lex_sql};

    let tokens = if let Ok(tokens) = lex_sql(query) {
        tokens
    } else {
        // If lexing fails, return the whole query as a single statement
        // and let the parser produce a proper error message.
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        return vec![trimmed.to_string()];
    };

    let mut statements = Vec::new();
    // Track the start of the first token in each statement
    let mut first_token_start: Option<usize> = None;
    let mut last_token_end: usize = 0;

    for token in &tokens {
        match &token.kind {
            TokenKind::Semicolon => {
                if let Some(start) = first_token_start {
                    let fragment = query[start..last_token_end].trim();
                    if !fragment.is_empty() {
                        statements.push(fragment.to_string());
                    }
                }
                first_token_start = None;
            }
            TokenKind::Eof => break,
            _ => {
                if first_token_start.is_none() {
                    first_token_start = Some(token.start);
                }
                last_token_end = token.end;
            }
        }
    }

    // Trailing statement without semicolon
    if let Some(start) = first_token_start {
        let fragment = query[start..last_token_end].trim();
        if !fragment.is_empty() {
            statements.push(fragment.to_string());
        }
    }

    statements
}
