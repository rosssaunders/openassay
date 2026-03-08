use std::cmp::Ordering;

use crate::executor::column_batch::{ColumnBatch, TypedColumn};
use crate::executor::exec_expr::like_match;
use crate::parser::ast::{BinaryOp, Expr, UnaryOp};
use crate::storage::tuple::ScalarValue;
use crate::utils::adt::misc::compare_values_for_predicate;

enum LikeLiteralMatcher {
    All,
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Generic {
        pattern: String,
        escape: Option<char>,
    },
}

impl LikeLiteralMatcher {
    fn new(pattern: String, escape: Option<char>) -> Self {
        classify_like_pattern(&pattern, escape).unwrap_or(Self::Generic { pattern, escape })
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            Self::All => true,
            Self::Exact(literal) => value == literal,
            Self::Prefix(literal) => value.starts_with(literal),
            Self::Suffix(literal) => value.ends_with(literal),
            Self::Contains(literal) => value.contains(literal),
            Self::Generic { pattern, escape } => like_match(value, pattern, *escape),
        }
    }
}

pub(crate) fn eval_columnar_predicate(expr: &Expr, batch: &ColumnBatch) -> Option<Vec<bool>> {
    eval_predicate(expr, batch).map(|truths| {
        truths
            .into_iter()
            .map(|truth| matches!(truth, Some(true)))
            .collect()
    })
}

fn eval_predicate(expr: &Expr, batch: &ColumnBatch) -> Option<Vec<Option<bool>>> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let left_truths = eval_predicate(left, batch)?;
            let right_truths = eval_predicate(right, batch)?;
            Some(
                left_truths
                    .into_iter()
                    .zip(right_truths)
                    .map(|(left, right)| match (left, right) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    })
                    .collect(),
            )
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let left_truths = eval_predicate(left, batch)?;
            let right_truths = eval_predicate(right, batch)?;
            Some(
                left_truths
                    .into_iter()
                    .zip(right_truths)
                    .map(|(left, right)| match (left, right) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    })
                    .collect(),
            )
        }
        Expr::Binary { left, op, right } => {
            let comparison = match op {
                BinaryOp::Eq => Some(Ordering::Equal),
                BinaryOp::NotEq => Some(Ordering::Equal),
                BinaryOp::Lt => Some(Ordering::Less),
                BinaryOp::Lte => Some(Ordering::Less),
                BinaryOp::Gt => Some(Ordering::Greater),
                BinaryOp::Gte => Some(Ordering::Greater),
                _ => None,
            }?;
            eval_comparison(left, op.clone(), right, comparison, batch)
        }
        Expr::IsNull { expr, negated } => {
            let column_idx = column_ref_index(expr, batch)?;
            Some(
                (0..batch.row_count)
                    .map(|row_idx| {
                        let is_null = matches!(
                            batch.columns[column_idx].value_at(row_idx),
                            ScalarValue::Null
                        );
                        Some(if *negated { !is_null } else { is_null })
                    })
                    .collect(),
            )
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let column_idx = column_ref_index(expr, batch)?;
            let pattern = literal_scalar(pattern)?;
            let escape_char = escape
                .as_deref()
                .and_then(literal_scalar)
                .and_then(extract_escape_char);
            Some(compare_column_to_like_literal(
                batch,
                column_idx,
                &pattern,
                *case_insensitive,
                *negated,
                escape_char,
            ))
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            let truths = eval_predicate(expr, batch)?;
            Some(
                truths
                    .into_iter()
                    .map(|truth| truth.map(|value| !value))
                    .collect(),
            )
        }
        _ => None,
    }
}

fn eval_comparison(
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    target_ordering: Ordering,
    batch: &ColumnBatch,
) -> Option<Vec<Option<bool>>> {
    if let Some(column_idx) = column_ref_index(left, batch) {
        let literal = literal_scalar(right)?;
        return Some(compare_column_to_literal(
            batch,
            column_idx,
            &literal,
            op,
            target_ordering,
        ));
    }
    if let Some(column_idx) = column_ref_index(right, batch) {
        let literal = literal_scalar(left)?;
        return Some(compare_column_to_literal(
            batch,
            column_idx,
            &literal,
            reverse_binary_op(op),
            target_ordering,
        ));
    }
    None
}

fn compare_column_to_literal(
    batch: &ColumnBatch,
    column_idx: usize,
    literal: &ScalarValue,
    op: BinaryOp,
    target_ordering: Ordering,
) -> Vec<Option<bool>> {
    match (&batch.columns[column_idx], literal) {
        (TypedColumn::Text(values, nulls), ScalarValue::Text(text)) => {
            return values
                .iter()
                .zip(nulls.iter().copied())
                .map(|(value, is_null)| {
                    if is_null {
                        return None;
                    }
                    let ordering = value.as_str().cmp(text.as_str());
                    Some(compare_ordering(ordering, op.clone(), target_ordering))
                })
                .collect();
        }
        (TypedColumn::Int64(values, nulls), ScalarValue::Int(literal)) => {
            return values
                .iter()
                .zip(nulls.iter().copied())
                .map(|(value, is_null)| {
                    if is_null {
                        return None;
                    }
                    Some(compare_ordering(
                        value.cmp(literal),
                        op.clone(),
                        target_ordering,
                    ))
                })
                .collect();
        }
        (TypedColumn::Float64(values, nulls), ScalarValue::Float(literal)) => {
            return values
                .iter()
                .zip(nulls.iter().copied())
                .map(|(value, is_null)| {
                    if is_null {
                        return None;
                    }
                    let ordering = value.partial_cmp(literal).unwrap_or(Ordering::Equal);
                    Some(compare_ordering(ordering, op.clone(), target_ordering))
                })
                .collect();
        }
        _ => {}
    }

    (0..batch.row_count)
        .map(|row_idx| {
            let value = batch.columns[column_idx].value_at(row_idx);
            if matches!(value, ScalarValue::Null) || matches!(literal, ScalarValue::Null) {
                return None;
            }
            let ordering = compare_values_for_predicate(&value, literal).ok()?;
            Some(compare_ordering(ordering, op.clone(), target_ordering))
        })
        .collect()
}

fn compare_ordering(ordering: Ordering, op: BinaryOp, target_ordering: Ordering) -> bool {
    match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt | BinaryOp::Gt => ordering == target_ordering,
        BinaryOp::Lte | BinaryOp::Gte => ordering == target_ordering || ordering == Ordering::Equal,
        _ => false,
    }
}

fn compare_column_to_like_literal(
    batch: &ColumnBatch,
    column_idx: usize,
    pattern: &ScalarValue,
    case_insensitive: bool,
    negated: bool,
    escape: Option<char>,
) -> Vec<Option<bool>> {
    let pattern_text = match pattern {
        ScalarValue::Null => return vec![None; batch.row_count],
        ScalarValue::Text(text) => {
            if case_insensitive {
                text.to_ascii_lowercase()
            } else {
                text.clone()
            }
        }
        other => {
            let rendered = other.render();
            if case_insensitive {
                rendered.to_ascii_lowercase()
            } else {
                rendered
            }
        }
    };
    let matcher = LikeLiteralMatcher::new(pattern_text, escape);

    if let TypedColumn::Text(values, nulls) = &batch.columns[column_idx] {
        return values
            .iter()
            .zip(nulls.iter().copied())
            .map(|(value, is_null)| {
                if is_null {
                    return None;
                }
                let matched = if case_insensitive {
                    matcher.matches(&value.to_ascii_lowercase())
                } else {
                    matcher.matches(value)
                };
                Some(if negated { !matched } else { matched })
            })
            .collect();
    }

    (0..batch.row_count)
        .map(|row_idx| {
            let value = batch.columns[column_idx].value_at(row_idx);
            if matches!(value, ScalarValue::Null) {
                return None;
            }
            let text = value.render();
            let haystack = if case_insensitive {
                text.to_ascii_lowercase()
            } else {
                text
            };
            let matched = matcher.matches(&haystack);
            Some(if negated { !matched } else { matched })
        })
        .collect()
}

fn classify_like_pattern(pattern: &str, escape: Option<char>) -> Option<LikeLiteralMatcher> {
    let escape_char = escape.unwrap_or('\\');
    let mut chars = pattern.chars();
    let mut literal = String::with_capacity(pattern.len());
    let mut has_prefix_wildcard = false;
    let mut has_suffix_wildcard = false;
    let mut wildcard_count = 0usize;

    while let Some(ch) = chars.next() {
        if ch == escape_char {
            let escaped = chars.next()?;
            literal.push(escaped);
            continue;
        }
        match ch {
            '%' => {
                wildcard_count += 1;
                let at_start = literal.is_empty() && wildcard_count == 1;
                let only_suffix_remaining = chars.clone().all(|next| next == '%');
                if at_start {
                    has_prefix_wildcard = true;
                } else if only_suffix_remaining {
                    has_suffix_wildcard = true;
                    break;
                } else {
                    return None;
                }
            }
            '_' => return None,
            _ => literal.push(ch),
        }
    }

    if wildcard_count == 0 {
        return Some(LikeLiteralMatcher::Exact(literal));
    }
    if literal.is_empty() {
        return Some(LikeLiteralMatcher::All);
    }
    match (has_prefix_wildcard, has_suffix_wildcard) {
        (true, true) => Some(LikeLiteralMatcher::Contains(literal)),
        (true, false) => Some(LikeLiteralMatcher::Suffix(literal)),
        (false, true) => Some(LikeLiteralMatcher::Prefix(literal)),
        (false, false) => None,
    }
}

fn extract_escape_char(value: ScalarValue) -> Option<char> {
    match value {
        ScalarValue::Null => None,
        other => {
            let text = other.render();
            if text.chars().count() == 1 {
                text.chars().next()
            } else {
                None
            }
        }
    }
}

fn column_ref_index(expr: &Expr, batch: &ColumnBatch) -> Option<usize> {
    let Expr::Identifier(parts) = expr else {
        return None;
    };
    let name = parts.join(".");
    batch.column_index(&name)
}

fn literal_scalar(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Null => Some(ScalarValue::Null),
        Expr::Boolean(value) => Some(ScalarValue::Bool(*value)),
        Expr::Integer(value) => Some(ScalarValue::Int(*value)),
        Expr::String(value) => Some(ScalarValue::Text(value.clone())),
        Expr::Float(value) => value
            .parse()
            .map(ScalarValue::Numeric)
            .or_else(|_| value.parse().map(ScalarValue::Float))
            .ok(),
        Expr::TypedLiteral { value, .. } => Some(ScalarValue::Text(value.clone())),
        Expr::Cast { expr, .. } => literal_scalar(expr),
        Expr::Unary {
            op: UnaryOp::Plus,
            expr,
        } => literal_scalar(expr),
        Expr::Unary {
            op: UnaryOp::Minus,
            expr,
        } => match literal_scalar(expr)? {
            ScalarValue::Int(value) => Some(ScalarValue::Int(-value)),
            ScalarValue::Float(value) => Some(ScalarValue::Float(-value)),
            ScalarValue::Numeric(value) => Some(ScalarValue::Numeric(-value)),
            _ => None,
        },
        _ => None,
    }
}

fn reverse_binary_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NotEq => BinaryOp::NotEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        _ => op,
    }
}

#[cfg(test)]
mod tests {
    use super::eval_columnar_predicate;
    use crate::executor::column_batch::ColumnBatch;
    use crate::parser::ast::{BinaryOp, Expr, UnaryOp};
    use crate::storage::tuple::ScalarValue;

    fn sample_batch() -> ColumnBatch {
        ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Null, ScalarValue::Text("b".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
            ],
            &["id".to_string(), "name".to_string()],
        )
    }

    #[test]
    fn filters_simple_comparison() {
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier(vec!["id".to_string()])),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Integer(1)),
        };

        assert_eq!(
            eval_columnar_predicate(&predicate, &sample_batch()),
            Some(vec![false, false, true])
        );
    }

    #[test]
    fn filters_is_null() {
        let predicate = Expr::IsNull {
            expr: Box::new(Expr::Identifier(vec!["id".to_string()])),
            negated: false,
        };

        assert_eq!(
            eval_columnar_predicate(&predicate, &sample_batch()),
            Some(vec![false, true, false])
        );
    }

    #[test]
    fn filters_text_inequality_without_scalar_fallback() {
        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier(vec!["name".to_string()])),
            op: BinaryOp::NotEq,
            right: Box::new(Expr::String("".to_string())),
        };

        assert_eq!(
            eval_columnar_predicate(&predicate, &sample_batch()),
            Some(vec![true, true, true])
        );
    }

    #[test]
    fn preserves_sql_null_semantics_for_not() {
        let predicate = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Binary {
                left: Box::new(Expr::Identifier(vec!["id".to_string()])),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(1)),
            }),
        };

        assert_eq!(
            eval_columnar_predicate(&predicate, &sample_batch()),
            Some(vec![true, false, false])
        );
    }

    #[test]
    fn filters_like_predicate_columnarly() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("https://google.com".to_string())],
                vec![ScalarValue::Text("https://example.com".to_string())],
                vec![ScalarValue::Null],
            ],
            &["url".to_string()],
        );
        let predicate = Expr::Like {
            expr: Box::new(Expr::Identifier(vec!["url".to_string()])),
            pattern: Box::new(Expr::String("%google%".to_string())),
            case_insensitive: false,
            negated: false,
            escape: None,
        };

        assert_eq!(
            eval_columnar_predicate(&predicate, &batch),
            Some(vec![true, false, false])
        );
    }
}
