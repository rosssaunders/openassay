use std::cmp::Ordering;

use crate::executor::column_batch::ColumnBatch;
use crate::parser::ast::{BinaryOp, Expr, UnaryOp};
use crate::storage::tuple::ScalarValue;
use crate::utils::adt::misc::compare_values_for_predicate;

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
    (0..batch.row_count)
        .map(|row_idx| {
            let value = batch.columns[column_idx].value_at(row_idx);
            if matches!(value, ScalarValue::Null) || matches!(literal, ScalarValue::Null) {
                return None;
            }
            let ordering = compare_values_for_predicate(&value, literal).ok()?;
            let result = match op {
                BinaryOp::Eq => ordering == Ordering::Equal,
                BinaryOp::NotEq => ordering != Ordering::Equal,
                BinaryOp::Lt | BinaryOp::Gt => ordering == target_ordering,
                BinaryOp::Lte | BinaryOp::Gte => {
                    ordering == target_ordering || ordering == Ordering::Equal
                }
                _ => return None,
            };
            Some(result)
        })
        .collect()
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
}
