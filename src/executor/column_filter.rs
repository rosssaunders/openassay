use std::cmp::Ordering;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::kernels::cmp;
use arrow::compute::{and_kleene, ilike, like, nilike, nlike, not, or_kleene};

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
            let combined = and_kleene(
                &truths_to_boolean_array(left_truths),
                &truths_to_boolean_array(right_truths),
            )
            .ok()?;
            Some(boolean_array_to_truths(combined))
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            let left_truths = eval_predicate(left, batch)?;
            let right_truths = eval_predicate(right, batch)?;
            let combined = or_kleene(
                &truths_to_boolean_array(left_truths),
                &truths_to_boolean_array(right_truths),
            )
            .ok()?;
            Some(boolean_array_to_truths(combined))
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
            let negated = not(&truths_to_boolean_array(truths)).ok()?;
            Some(boolean_array_to_truths(negated))
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
    if let Some(mask) = compare_arrow_column_to_literal(&batch.columns[column_idx], literal, &op) {
        return boolean_array_to_truths(mask);
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

fn compare_arrow_column_to_literal(
    column: &TypedColumn,
    literal: &ScalarValue,
    op: &BinaryOp,
) -> Option<BooleanArray> {
    match (column, literal) {
        (TypedColumn::Text(values, nulls), ScalarValue::Text(text)) => {
            let array = build_text_array(values, nulls);
            let scalar = StringArray::new_scalar(text.as_str());
            eval_arrow_comparison(&array, &scalar, op)
        }
        (TypedColumn::Text(values, nulls), other) => {
            let array = build_text_array(values, nulls);
            let rendered = other.render();
            let scalar = StringArray::new_scalar(rendered.as_str());
            eval_arrow_comparison(&array, &scalar, op)
        }
        (TypedColumn::Int64(values, nulls), ScalarValue::Int(value)) => {
            let array = build_int_array(values, nulls);
            let scalar = Int64Array::new_scalar(*value);
            eval_arrow_comparison(&array, &scalar, op)
        }
        (TypedColumn::Float64(values, nulls), ScalarValue::Float(value)) => {
            let array = build_float_array(values, nulls);
            let scalar = Float64Array::new_scalar(*value);
            eval_arrow_comparison(&array, &scalar, op)
        }
        _ => None,
    }
}

fn eval_arrow_comparison(
    left: &dyn arrow::array::Datum,
    right: &dyn arrow::array::Datum,
    op: &BinaryOp,
) -> Option<BooleanArray> {
    match op {
        BinaryOp::Eq => cmp::eq(left, right).ok(),
        BinaryOp::NotEq => cmp::neq(left, right).ok(),
        BinaryOp::Lt => cmp::lt(left, right).ok(),
        BinaryOp::Lte => cmp::lt_eq(left, right).ok(),
        BinaryOp::Gt => cmp::gt(left, right).ok(),
        BinaryOp::Gte => cmp::gt_eq(left, right).ok(),
        _ => None,
    }
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
        ScalarValue::Text(text) => text.clone(),
        other => other.render(),
    };

    if let TypedColumn::Text(values, nulls) = &batch.columns[column_idx]
        && escape.is_none()
    {
        let array = build_text_array(values, nulls);
        let scalar = StringArray::new_scalar(pattern_text.as_str());
        let mask = match (case_insensitive, negated) {
            (false, false) => like(&array, &scalar).ok(),
            (true, false) => ilike(&array, &scalar).ok(),
            (false, true) => nlike(&array, &scalar).ok(),
            (true, true) => nilike(&array, &scalar).ok(),
        };
        if let Some(mask) = mask {
            return boolean_array_to_truths(mask);
        }
    }

    let pattern_for_matcher = if case_insensitive {
        pattern_text.to_ascii_lowercase()
    } else {
        pattern_text
    };
    let matcher = LikeLiteralMatcher::new(pattern_for_matcher, escape);

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

fn truths_to_boolean_array(truths: Vec<Option<bool>>) -> BooleanArray {
    BooleanArray::from(truths)
}

fn boolean_array_to_truths(mask: BooleanArray) -> Vec<Option<bool>> {
    mask.iter().collect()
}

fn build_text_array(values: &[String], nulls: &[bool]) -> StringArray {
    values
        .iter()
        .zip(nulls.iter().copied())
        .map(|(value, is_null)| (!is_null).then_some(value.as_str()))
        .collect::<StringArray>()
}

fn build_int_array(values: &[i64], nulls: &[bool]) -> Int64Array {
    values
        .iter()
        .zip(nulls.iter().copied())
        .map(|(value, is_null)| (!is_null).then_some(*value))
        .collect::<Int64Array>()
}

fn build_float_array(values: &[f64], nulls: &[bool]) -> Float64Array {
    values
        .iter()
        .zip(nulls.iter().copied())
        .map(|(value, is_null)| (!is_null).then_some(*value))
        .collect::<Float64Array>()
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
