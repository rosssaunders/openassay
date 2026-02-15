//! PL/pgSQL tokenizer for Phase 1 support.
//!
//! This module is modeled after PostgreSQL's scanner wrapper in
//! `postgres/src/pl/plpgsql/src/pl_scanner.c`, especially keyword-aware
//! tokenization and statement-boundary behavior around `:=` and `;`
//! (`pl_scanner.c:23-94`, `145-289`, `427-563`).

use std::fmt;

/// Scanner error for PL/pgSQL tokenization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlScanError {
    pub message: String,
    pub position: usize,
    pub line: usize,
    pub column: usize,
}

impl fmt::Display for PlPgSqlScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at byte {} (line {}, col {})",
            self.message, self.position, self.line, self.column
        )
    }
}

impl std::error::Error for PlPgSqlScanError {}

/// Subset of PL/pgSQL keywords needed for Phase 1 parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlKeyword {
    Declare,
    Begin,
    End,
    If,
    Then,
    Else,
    Elsif,
    Case,
    Loop,
    While,
    For,
    Foreach,
    Exit,
    Continue,
    Return,
    ReturnNext,
    ReturnQuery,
    Raise,
    Notice,
    Assert,
    Perform,
    Call,
    Commit,
    Rollback,
    Get,
    Diagnostics,
    Open,
    Fetch,
    Close,
    Execute,
    Do,
    Language,
    Null,
}

/// Byte span of a token in the original function text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlPgSqlSpan {
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub column: usize,
}

/// Token kinds produced by the PL/pgSQL scanner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlPgSqlTokenKind {
    Keyword(PlPgSqlKeyword),
    Identifier(String),
    StringLiteral(String),
    NumericLiteral(String),
    Assign,
    Semicolon,
    Comma,
    LParen,
    RParen,
    Dot,
    Colon,
    Equals,
    Operator(String),
    Eof,
}

/// Scanner token with source span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlToken {
    pub kind: PlPgSqlTokenKind,
    pub span: PlPgSqlSpan,
}

/// Tokenizes PL/pgSQL source text.
pub fn tokenize(source: &str) -> Result<Vec<PlPgSqlToken>, PlPgSqlScanError> {
    let mut tokens = Vec::new();
    let mut idx = 0usize;
    let mut line = 1usize;
    let mut column = 1usize;

    while idx < source.len() {
        let ch = next_char(source, idx).expect("idx validated by loop condition");

        if ch.is_whitespace() {
            let start = idx;
            idx += ch.len_utf8();
            while idx < source.len() {
                let c = next_char(source, idx).expect("idx validated by loop condition");
                if !c.is_whitespace() {
                    break;
                }
                idx += c.len_utf8();
            }
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        if source[idx..].starts_with("--") {
            let start = idx;
            idx += 2;
            while idx < source.len() {
                let c = next_char(source, idx).expect("idx validated by loop condition");
                idx += c.len_utf8();
                if c == '\n' {
                    break;
                }
            }
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        if source[idx..].starts_with("/*") {
            let start = idx;
            idx += 2;
            let mut depth = 1usize;
            while idx < source.len() && depth > 0 {
                if source[idx..].starts_with("/*") {
                    depth += 1;
                    idx += 2;
                    continue;
                }
                if source[idx..].starts_with("*/") {
                    depth -= 1;
                    idx += 2;
                    continue;
                }
                let c = next_char(source, idx).expect("idx validated by loop condition");
                idx += c.len_utf8();
            }
            if depth != 0 {
                return Err(PlPgSqlScanError {
                    message: "unterminated block comment".to_string(),
                    position: start,
                    line,
                    column,
                });
            }
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        let token_line = line;
        let token_col = column;
        let start = idx;

        if is_ident_start(ch) {
            idx += ch.len_utf8();
            while idx < source.len() {
                let c = next_char(source, idx).expect("idx validated by loop condition");
                if !is_ident_part(c) {
                    break;
                }
                idx += c.len_utf8();
            }
            let text = &source[start..idx];
            let lower = text.to_ascii_lowercase();
            let kind = match keyword_from_ident(&lower) {
                Some(keyword) => PlPgSqlTokenKind::Keyword(keyword),
                None => PlPgSqlTokenKind::Identifier(text.to_string()),
            };
            tokens.push(PlPgSqlToken {
                kind,
                span: PlPgSqlSpan {
                    start,
                    end: idx,
                    line: token_line,
                    column: token_col,
                },
            });
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        if ch.is_ascii_digit() {
            idx += ch.len_utf8();
            while idx < source.len() {
                let c = next_char(source, idx).expect("idx validated by loop condition");
                if !c.is_ascii_digit() {
                    break;
                }
                idx += c.len_utf8();
            }
            if idx < source.len() && source[idx..].starts_with('.') {
                let dot_idx = idx;
                idx += 1;
                let mut has_frac_digit = false;
                while idx < source.len() {
                    let c = next_char(source, idx).expect("idx validated by loop condition");
                    if !c.is_ascii_digit() {
                        break;
                    }
                    has_frac_digit = true;
                    idx += c.len_utf8();
                }
                if !has_frac_digit {
                    idx = dot_idx;
                }
            }
            tokens.push(PlPgSqlToken {
                kind: PlPgSqlTokenKind::NumericLiteral(source[start..idx].to_string()),
                span: PlPgSqlSpan {
                    start,
                    end: idx,
                    line: token_line,
                    column: token_col,
                },
            });
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        if ch == '\'' {
            idx += 1;
            let mut terminated = false;
            while idx < source.len() {
                let c = next_char(source, idx).expect("idx validated by loop condition");
                idx += c.len_utf8();
                if c == '\'' {
                    if idx < source.len() && source[idx..].starts_with('\'') {
                        idx += 1;
                    } else {
                        terminated = true;
                        break;
                    }
                }
            }
            if !terminated {
                return Err(PlPgSqlScanError {
                    message: "unterminated string literal".to_string(),
                    position: start,
                    line: token_line,
                    column: token_col,
                });
            }
            tokens.push(PlPgSqlToken {
                kind: PlPgSqlTokenKind::StringLiteral(source[start..idx].to_string()),
                span: PlPgSqlSpan {
                    start,
                    end: idx,
                    line: token_line,
                    column: token_col,
                },
            });
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        if ch == '$'
            && let Some(end_idx) = scan_dollar_quoted(source, start)
        {
            idx = end_idx;
            tokens.push(PlPgSqlToken {
                kind: PlPgSqlTokenKind::StringLiteral(source[start..idx].to_string()),
                span: PlPgSqlSpan {
                    start,
                    end: idx,
                    line: token_line,
                    column: token_col,
                },
            });
            advance_position(&source[start..idx], &mut line, &mut column);
            continue;
        }

        idx += ch.len_utf8();
        let kind = if ch == ':' && source[idx..].starts_with('=') {
            idx += 1;
            PlPgSqlTokenKind::Assign
        } else {
            match ch {
                ';' => PlPgSqlTokenKind::Semicolon,
                ',' => PlPgSqlTokenKind::Comma,
                '(' => PlPgSqlTokenKind::LParen,
                ')' => PlPgSqlTokenKind::RParen,
                '.' => PlPgSqlTokenKind::Dot,
                ':' => PlPgSqlTokenKind::Colon,
                '=' => PlPgSqlTokenKind::Equals,
                '+' | '-' | '*' | '/' | '%' | '<' | '>' | '!' | '|' => {
                    let mut end = idx;
                    while end < source.len() {
                        let c = next_char(source, end).expect("idx validated by loop condition");
                        if !(c == '=' || c == '>' || c == '<' || c == '|' || c == '!' || c == '#') {
                            break;
                        }
                        end += c.len_utf8();
                    }
                    idx = end;
                    PlPgSqlTokenKind::Operator(source[start..idx].to_string())
                }
                _ => PlPgSqlTokenKind::Operator(source[start..idx].to_string()),
            }
        };

        tokens.push(PlPgSqlToken {
            kind,
            span: PlPgSqlSpan {
                start,
                end: idx,
                line: token_line,
                column: token_col,
            },
        });
        advance_position(&source[start..idx], &mut line, &mut column);
    }

    tokens.push(PlPgSqlToken {
        kind: PlPgSqlTokenKind::Eof,
        span: PlPgSqlSpan {
            start: source.len(),
            end: source.len(),
            line,
            column,
        },
    });

    Ok(tokens)
}

/// Extracts SQL expression text from token `start_idx` up to `;`.
///
/// This is the scanner-level helper used for parsing assignment forms like
/// `name := <expr>;`, mirroring scanner-aware statement boundary handling in
/// PostgreSQL (`pl_scanner.c:427-521`).
pub fn extract_sql_expression(
    tokens: &[PlPgSqlToken],
    source: &str,
    start_idx: usize,
) -> Result<(String, usize), PlPgSqlScanError> {
    if start_idx >= tokens.len() {
        return Err(PlPgSqlScanError {
            message: "expression start is out of range".to_string(),
            position: source.len(),
            line: 0,
            column: 0,
        });
    }

    let mut idx = start_idx;
    let mut depth = 0usize;

    while idx < tokens.len() {
        match tokens[idx].kind {
            PlPgSqlTokenKind::LParen => depth += 1,
            PlPgSqlTokenKind::RParen => depth = depth.saturating_sub(1),
            PlPgSqlTokenKind::Semicolon if depth == 0 => {
                let start = tokens[start_idx].span.start;
                let end = tokens[idx].span.start;
                let expr = source[start..end].trim().to_string();
                return Ok((expr, idx));
            }
            PlPgSqlTokenKind::Eof => break,
            _ => {}
        }
        idx += 1;
    }

    let span = tokens[start_idx].span;
    Err(PlPgSqlScanError {
        message: "unterminated SQL expression (missing ';')".to_string(),
        position: span.start,
        line: span.line,
        column: span.column,
    })
}

fn next_char(source: &str, idx: usize) -> Option<char> {
    source.get(idx..)?.chars().next()
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_part(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_ascii_alphanumeric()
}

fn advance_position(text: &str, line: &mut usize, column: &mut usize) {
    for ch in text.chars() {
        if ch == '\n' {
            *line += 1;
            *column = 1;
        } else {
            *column += 1;
        }
    }
}

fn keyword_from_ident(ident: &str) -> Option<PlPgSqlKeyword> {
    match ident {
        "declare" => Some(PlPgSqlKeyword::Declare),
        "begin" => Some(PlPgSqlKeyword::Begin),
        "end" => Some(PlPgSqlKeyword::End),
        "if" => Some(PlPgSqlKeyword::If),
        "then" => Some(PlPgSqlKeyword::Then),
        "else" => Some(PlPgSqlKeyword::Else),
        "elsif" => Some(PlPgSqlKeyword::Elsif),
        "case" => Some(PlPgSqlKeyword::Case),
        "loop" => Some(PlPgSqlKeyword::Loop),
        "while" => Some(PlPgSqlKeyword::While),
        "for" => Some(PlPgSqlKeyword::For),
        "foreach" => Some(PlPgSqlKeyword::Foreach),
        "exit" => Some(PlPgSqlKeyword::Exit),
        "continue" => Some(PlPgSqlKeyword::Continue),
        "return" => Some(PlPgSqlKeyword::Return),
        "next" => Some(PlPgSqlKeyword::ReturnNext),
        "query" => Some(PlPgSqlKeyword::ReturnQuery),
        "raise" => Some(PlPgSqlKeyword::Raise),
        "notice" => Some(PlPgSqlKeyword::Notice),
        "assert" => Some(PlPgSqlKeyword::Assert),
        "perform" => Some(PlPgSqlKeyword::Perform),
        "call" => Some(PlPgSqlKeyword::Call),
        "commit" => Some(PlPgSqlKeyword::Commit),
        "rollback" => Some(PlPgSqlKeyword::Rollback),
        "get" => Some(PlPgSqlKeyword::Get),
        "diagnostics" => Some(PlPgSqlKeyword::Diagnostics),
        "open" => Some(PlPgSqlKeyword::Open),
        "fetch" => Some(PlPgSqlKeyword::Fetch),
        "close" => Some(PlPgSqlKeyword::Close),
        "execute" => Some(PlPgSqlKeyword::Execute),
        "do" => Some(PlPgSqlKeyword::Do),
        "language" => Some(PlPgSqlKeyword::Language),
        "null" => Some(PlPgSqlKeyword::Null),
        _ => None,
    }
}

fn scan_dollar_quoted(source: &str, start: usize) -> Option<usize> {
    let mut idx = start + 1;
    while idx < source.len() {
        let ch = next_char(source, idx)?;
        if ch == '$' {
            let delim_end = idx + 1;
            let delim = &source[start..delim_end];
            let haystack = &source[delim_end..];
            let rel = haystack.find(delim)?;
            return Some(delim_end + rel + delim.len());
        }
        if !is_ident_part(ch) {
            return None;
        }
        idx += ch.len_utf8();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{PlPgSqlKeyword, PlPgSqlTokenKind, extract_sql_expression, tokenize};

    #[test]
    fn tokenizes_declare_begin_end_blocks() {
        let src = "DECLARE x integer := 1; BEGIN x := x + 1; END;";
        let tokens = tokenize(src).expect("scan should succeed");

        assert!(
            tokens
                .iter()
                .any(|t| { matches!(t.kind, PlPgSqlTokenKind::Keyword(PlPgSqlKeyword::Declare)) })
        );
        assert!(
            tokens
                .iter()
                .any(|t| { matches!(t.kind, PlPgSqlTokenKind::Keyword(PlPgSqlKeyword::Begin)) })
        );
        assert!(
            tokens
                .iter()
                .any(|t| { matches!(t.kind, PlPgSqlTokenKind::Keyword(PlPgSqlKeyword::End)) })
        );
    }

    #[test]
    fn extracts_assignment_expression_until_semicolon() {
        let src = "v := format('x;y', (1 + 2));";
        let tokens = tokenize(src).expect("scan should succeed");
        let assign_pos = tokens
            .iter()
            .position(|t| matches!(t.kind, PlPgSqlTokenKind::Assign))
            .expect("assignment token should exist");
        let (expr, end_idx) =
            extract_sql_expression(&tokens, src, assign_pos + 1).expect("expression should parse");

        assert_eq!(expr, "format('x;y', (1 + 2))");
        assert!(matches!(tokens[end_idx].kind, PlPgSqlTokenKind::Semicolon));
    }
}
