use std::cmp::Ordering;
use std::collections::HashMap;

use crate::executor::column_batch::ColumnBatch;
use crate::executor::exec_main::row_key;
use crate::executor::exec_main::{compare_order_keys, parse_non_negative_int};
use crate::parser::ast::{
    Expr, OrderByExpr, WindowDefinition, WindowFrame, WindowFrameBound, WindowSpec,
};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

#[derive(Debug, Clone)]
pub(crate) struct WindowPartitions {
    #[cfg_attr(not(test), allow(dead_code))]
    partition_ids: Vec<usize>,
    #[cfg_attr(not(test), allow(dead_code))]
    positions: Vec<usize>,
    partitions: Vec<WindowPartition>,
}

#[derive(Debug, Clone)]
pub(crate) struct WindowPartition {
    pub(crate) row_indices: Vec<usize>,
    pub(crate) order_keys: Vec<Vec<ScalarValue>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WindowColumnPlan {
    pub(crate) function_name: String,
    pub(crate) argument_kinds: Vec<WindowArgumentKind>,
    pub(crate) partition_by_indices: Vec<usize>,
    pub(crate) order_by: Vec<OrderByExpr>,
    pub(crate) order_by_indices: Vec<usize>,
    pub(crate) frame: Option<WindowFrame>,
}

#[derive(Debug, Clone)]
pub(crate) enum WindowArgumentKind {
    Wildcard,
    Column(usize),
    Constant(ScalarValue),
}

impl WindowPartitions {
    pub(crate) fn build(
        batch: &ColumnBatch,
        partition_by_indices: &[usize],
        order_by_indices: &[usize],
        order_by: &[OrderByExpr],
    ) -> Result<Self, EngineError> {
        let mut partitions_by_key: HashMap<String, usize> = HashMap::new();
        let mut partitions = Vec::<WindowPartition>::new();
        let mut partition_ids = vec![0; batch.row_count];
        let mut positions = vec![0; batch.row_count];

        for (row_idx, partition_id_slot) in
            partition_ids.iter_mut().enumerate().take(batch.row_count)
        {
            let key_values = partition_by_indices
                .iter()
                .map(|column_idx| batch.columns[*column_idx].value_at(row_idx))
                .collect::<Vec<_>>();
            let key = row_key(&key_values);
            let partition_id = if let Some(existing) = partitions_by_key.get(&key) {
                *existing
            } else {
                let idx = partitions.len();
                partitions.push(WindowPartition {
                    row_indices: Vec::new(),
                    order_keys: Vec::new(),
                });
                partitions_by_key.insert(key, idx);
                idx
            };
            *partition_id_slot = partition_id;
            partitions[partition_id].row_indices.push(row_idx);
        }

        for partition in &mut partitions {
            if order_by_indices.is_empty() {
                partition.order_keys = vec![Vec::new(); partition.row_indices.len()];
            } else {
                let mut decorated = partition
                    .row_indices
                    .iter()
                    .copied()
                    .map(|row_idx| {
                        let keys = order_by_indices
                            .iter()
                            .map(|column_idx| batch.columns[*column_idx].value_at(row_idx))
                            .collect::<Vec<_>>();
                        (row_idx, keys)
                    })
                    .collect::<Vec<_>>();
                decorated.sort_by(|left, right| compare_order_keys(&left.1, &right.1, order_by));
                partition.row_indices = decorated.iter().map(|(row_idx, _)| *row_idx).collect();
                partition.order_keys = decorated.into_iter().map(|(_, keys)| keys).collect();
            }
        }

        for (partition_id, partition) in partitions.iter().enumerate() {
            for (position, row_idx) in partition.row_indices.iter().copied().enumerate() {
                partition_ids[row_idx] = partition_id;
                positions[row_idx] = position;
            }
        }

        Ok(Self {
            partition_ids,
            positions,
            partitions,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn partition_for_row(&self, row_idx: usize) -> &WindowPartition {
        &self.partitions[self.partition_ids[row_idx]]
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn position_for_row(&self, row_idx: usize) -> usize {
        self.positions[row_idx]
    }
}

pub(crate) fn resolve_window_spec(
    spec: &WindowSpec,
    definitions: &[WindowDefinition],
) -> Result<WindowSpec, EngineError> {
    let Some(ref_name) = &spec.name else {
        return Ok(spec.clone());
    };

    let definition = definitions
        .iter()
        .find(|definition| definition.name == *ref_name)
        .ok_or_else(|| EngineError {
            message: format!("window \"{ref_name}\" does not exist"),
        })?;

    if !spec.partition_by.is_empty() {
        return Err(EngineError {
            message: format!("cannot override PARTITION BY clause of window \"{ref_name}\""),
        });
    }

    let mut resolved = definition.spec.clone();
    if !spec.order_by.is_empty() {
        resolved.order_by = spec.order_by.clone();
    }
    if spec.frame.is_some() {
        resolved.frame = spec.frame.clone();
    }
    resolved.name = None;
    Ok(resolved)
}

pub(crate) fn eval_window_function_columnar(
    plan: &WindowColumnPlan,
    partitions: &WindowPartitions,
    batch: &ColumnBatch,
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut results = vec![ScalarValue::Null; batch.row_count];
    for partition in &partitions.partitions {
        let partition_values = evaluate_partition(plan, partition, batch)?;
        for (position, row_idx) in partition.row_indices.iter().copied().enumerate() {
            results[row_idx] = partition_values[position].clone();
        }
    }
    Ok(results)
}

fn evaluate_partition(
    plan: &WindowColumnPlan,
    partition: &WindowPartition,
    batch: &ColumnBatch,
) -> Result<Vec<ScalarValue>, EngineError> {
    if partition.row_indices.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::with_capacity(partition.row_indices.len());
    match plan.function_name.as_str() {
        "row_number" => {
            for position in 0..partition.row_indices.len() {
                output.push(ScalarValue::Int((position + 1) as i64));
            }
        }
        "rank" => {
            let mut rank = 1usize;
            for position in 0..partition.row_indices.len() {
                if position > 0
                    && compare_order_keys(
                        &partition.order_keys[position - 1],
                        &partition.order_keys[position],
                        &plan.order_by,
                    ) != std::cmp::Ordering::Equal
                {
                    rank = position + 1;
                }
                output.push(ScalarValue::Int(rank as i64));
            }
        }
        "dense_rank" => {
            let mut rank = 1i64;
            for position in 0..partition.row_indices.len() {
                if position > 0
                    && compare_order_keys(
                        &partition.order_keys[position - 1],
                        &partition.order_keys[position],
                        &plan.order_by,
                    ) != std::cmp::Ordering::Equal
                {
                    rank += 1;
                }
                output.push(ScalarValue::Int(rank));
            }
        }
        "lag" | "lead" => {
            let column_index = argument_column_index(&plan.argument_kinds, 0, &plan.function_name)?;
            let offset = argument_constant_non_negative_int(&plan.argument_kinds, 1)?.unwrap_or(1);
            let default_value =
                argument_constant_value(&plan.argument_kinds, 2).unwrap_or(ScalarValue::Null);
            for position in 0..partition.row_indices.len() {
                let target = if plan.function_name == "lag" {
                    position.checked_sub(offset)
                } else {
                    position.checked_add(offset)
                };
                if let Some(target_position) = target
                    && let Some(row_idx) = partition.row_indices.get(target_position)
                {
                    output.push(batch.columns[column_index].value_at(*row_idx));
                } else {
                    output.push(default_value.clone());
                }
            }
        }
        "ntile" => {
            let buckets =
                argument_constant_non_negative_int(&plan.argument_kinds, 0)?.ok_or_else(|| {
                    EngineError {
                        message: "ntile() expects exactly one argument".to_string(),
                    }
                })?;
            if buckets == 0 {
                return Err(EngineError {
                    message: "ntile() argument must be positive".to_string(),
                });
            }
            let total = partition.row_indices.len();
            for position in 0..total {
                output.push(ScalarValue::Int(((position * buckets / total) + 1) as i64));
            }
        }
        "first_value" | "last_value" => {
            let column_index = argument_column_index(&plan.argument_kinds, 0, &plan.function_name)?;
            for position in 0..partition.row_indices.len() {
                let frame_positions = frame_positions(plan, partition, position)?;
                if let Some(target_position) = if plan.function_name == "first_value" {
                    frame_positions.first().copied()
                } else {
                    frame_positions.last().copied()
                } {
                    output.push(
                        batch.columns[column_index]
                            .value_at(partition.row_indices[target_position]),
                    );
                } else {
                    output.push(ScalarValue::Null);
                }
            }
        }
        "sum" | "count" | "avg" | "min" | "max" => {
            for position in 0..partition.row_indices.len() {
                let frame_positions = frame_positions(plan, partition, position)?;
                output.push(evaluate_aggregate_window(
                    plan,
                    partition,
                    batch,
                    &frame_positions,
                )?);
            }
        }
        other => {
            return Err(EngineError {
                message: format!("unsupported columnar window function {other}"),
            });
        }
    }
    Ok(output)
}

fn argument_column_index(
    argument_kinds: &[WindowArgumentKind],
    index: usize,
    function_name: &str,
) -> Result<usize, EngineError> {
    let Some(WindowArgumentKind::Column(column_index)) = argument_kinds.get(index) else {
        return Err(EngineError {
            message: format!("{function_name}() expects a column argument"),
        });
    };
    Ok(*column_index)
}

fn argument_constant_non_negative_int(
    argument_kinds: &[WindowArgumentKind],
    index: usize,
) -> Result<Option<usize>, EngineError> {
    let Some(argument) = argument_kinds.get(index) else {
        return Ok(None);
    };
    let WindowArgumentKind::Constant(value) = argument else {
        return Err(EngineError {
            message: "columnar window evaluation expects constant numeric arguments".to_string(),
        });
    };
    if matches!(value, ScalarValue::Null) {
        return Ok(None);
    }
    Ok(Some(parse_non_negative_int(value, "window argument")?))
}

fn argument_constant_value(
    argument_kinds: &[WindowArgumentKind],
    index: usize,
) -> Option<ScalarValue> {
    let Some(WindowArgumentKind::Constant(value)) = argument_kinds.get(index) else {
        return None;
    };
    Some(value.clone())
}

fn frame_positions(
    plan: &WindowColumnPlan,
    partition: &WindowPartition,
    current_position: usize,
) -> Result<Vec<usize>, EngineError> {
    let Some(frame) = &plan.frame else {
        if plan.order_by.is_empty() {
            return Ok((0..partition.row_indices.len()).collect());
        }
        let current_keys = &partition.order_keys[current_position];
        return Ok(partition
            .order_keys
            .iter()
            .enumerate()
            .filter(|(_, keys)| {
                compare_order_keys(keys, current_keys, &plan.order_by)
                    != std::cmp::Ordering::Greater
            })
            .map(|(position, _)| position)
            .collect());
    };

    let start = frame_bound_position(
        &frame.start,
        current_position,
        partition.row_indices.len(),
        true,
    )?;
    let end = frame_bound_position(
        &frame.end,
        current_position,
        partition.row_indices.len(),
        false,
    )?;
    if start > end {
        return Ok(Vec::new());
    }
    Ok((start..=end).collect())
}

fn frame_bound_position(
    bound: &WindowFrameBound,
    current_position: usize,
    partition_len: usize,
    is_start: bool,
) -> Result<usize, EngineError> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(0),
        WindowFrameBound::UnboundedFollowing => Ok(partition_len.saturating_sub(1)),
        WindowFrameBound::CurrentRow => Ok(current_position),
        WindowFrameBound::OffsetPreceding(expr) => {
            let ScalarValue::Int(offset) = eval_constant_expr(expr)? else {
                return Err(EngineError {
                    message: "window frame offset must be integer".to_string(),
                });
            };
            let offset = usize::try_from(offset).map_err(|_| EngineError {
                message: "window frame offset must be non-negative".to_string(),
            })?;
            Ok(current_position.saturating_sub(offset))
        }
        WindowFrameBound::OffsetFollowing(expr) => {
            let ScalarValue::Int(offset) = eval_constant_expr(expr)? else {
                return Err(EngineError {
                    message: "window frame offset must be integer".to_string(),
                });
            };
            let offset = usize::try_from(offset).map_err(|_| EngineError {
                message: "window frame offset must be non-negative".to_string(),
            })?;
            let position = current_position.saturating_add(offset);
            if is_start {
                Ok(position.min(partition_len))
            } else {
                Ok(position.min(partition_len.saturating_sub(1)))
            }
        }
    }
}

fn eval_constant_expr(expr: &Expr) -> Result<ScalarValue, EngineError> {
    match expr {
        Expr::Integer(value) => Ok(ScalarValue::Int(*value)),
        Expr::Cast { expr, .. } => eval_constant_expr(expr),
        _ => Err(EngineError {
            message: "columnar window evaluation only supports integer frame offsets".to_string(),
        }),
    }
}

fn evaluate_aggregate_window(
    plan: &WindowColumnPlan,
    partition: &WindowPartition,
    batch: &ColumnBatch,
    frame_positions: &[usize],
) -> Result<ScalarValue, EngineError> {
    match plan.function_name.as_str() {
        "count" => {
            if matches!(
                plan.argument_kinds.first(),
                Some(WindowArgumentKind::Wildcard)
            ) {
                return Ok(ScalarValue::Int(frame_positions.len() as i64));
            }
            let column_index = argument_column_index(&plan.argument_kinds, 0, "count")?;
            Ok(ScalarValue::Int(
                frame_positions
                    .iter()
                    .filter(|position| {
                        !matches!(
                            batch.columns[column_index].value_at(partition.row_indices[**position]),
                            ScalarValue::Null
                        )
                    })
                    .count() as i64,
            ))
        }
        "sum" | "avg" | "min" | "max" => {
            let column_index = argument_column_index(&plan.argument_kinds, 0, &plan.function_name)?;
            let mut values = frame_positions
                .iter()
                .map(|position| {
                    batch.columns[column_index].value_at(partition.row_indices[*position])
                })
                .filter(|value| !matches!(value, ScalarValue::Null))
                .collect::<Vec<_>>();
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            match plan.function_name.as_str() {
                "sum" => {
                    let mut total = values.remove(0);
                    for value in values {
                        total = match (total, value) {
                            (ScalarValue::Int(left), ScalarValue::Int(right)) => {
                                ScalarValue::Int(left + right)
                            }
                            (ScalarValue::Float(left), ScalarValue::Float(right)) => {
                                ScalarValue::Float(left + right)
                            }
                            (ScalarValue::Int(left), ScalarValue::Float(right)) => {
                                ScalarValue::Float(left as f64 + right)
                            }
                            (ScalarValue::Float(left), ScalarValue::Int(right)) => {
                                ScalarValue::Float(left + right as f64)
                            }
                            (ScalarValue::Numeric(left), ScalarValue::Numeric(right)) => {
                                ScalarValue::Numeric(left + right)
                            }
                            _ => {
                                return Err(EngineError {
                                    message:
                                        "unsupported SUM() input in columnar window evaluation"
                                            .to_string(),
                                });
                            }
                        };
                    }
                    Ok(total)
                }
                "avg" => {
                    let count = values.len() as f64;
                    let sum = evaluate_aggregate_window(
                        &WindowColumnPlan {
                            function_name: "sum".to_string(),
                            ..plan.clone()
                        },
                        partition,
                        batch,
                        frame_positions,
                    )?;
                    match sum {
                        ScalarValue::Int(total) => Ok(ScalarValue::Float(total as f64 / count)),
                        ScalarValue::Float(total) => Ok(ScalarValue::Float(total / count)),
                        ScalarValue::Numeric(total) => {
                            let divisor = rust_decimal::Decimal::from_f64_retain(count)
                                .ok_or_else(|| EngineError {
                                    message: "failed to compute numeric average".to_string(),
                                })?;
                            Ok(ScalarValue::Numeric(total / divisor))
                        }
                        _ => Err(EngineError {
                            message: "unsupported AVG() input in columnar window evaluation"
                                .to_string(),
                        }),
                    }
                }
                "min" => {
                    values.sort_by(scalar_cmp);
                    Ok(values.first().cloned().unwrap_or(ScalarValue::Null))
                }
                "max" => {
                    values.sort_by(scalar_cmp);
                    Ok(values.last().cloned().unwrap_or(ScalarValue::Null))
                }
                _ => Err(EngineError {
                    message: format!(
                        "unsupported aggregate window function for columnar evaluation: {}",
                        plan.function_name
                    ),
                }),
            }
        }
        _ => Err(EngineError {
            message: format!(
                "unsupported aggregate window function {}",
                plan.function_name
            ),
        }),
    }
}

pub(crate) fn expr_references_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) => true,
        Expr::FunctionCall {
            args, filter, over, ..
        } => {
            over.is_some()
                || filter.as_deref().is_some_and(expr_references_columns)
                || args.iter().any(expr_references_columns)
        }
        Expr::Cast { expr, .. }
        | Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::BooleanTest { expr, .. } => expr_references_columns(expr),
        Expr::Binary { left, right, .. }
        | Expr::AnyAll { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. } => {
            expr_references_columns(left) || expr_references_columns(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_columns(expr)
                || expr_references_columns(low)
                || expr_references_columns(high)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_columns(expr)
                || expr_references_columns(pattern)
                || escape.as_deref().is_some_and(expr_references_columns)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_columns(expr) || list.iter().any(expr_references_columns)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            expr_references_columns(operand)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_references_columns(when_expr) || expr_references_columns(then_expr)
                })
                || else_expr.as_deref().is_some_and(expr_references_columns)
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                expr_references_columns(when_expr) || expr_references_columns(then_expr)
            }) || else_expr.as_deref().is_some_and(expr_references_columns)
        }
        Expr::ArrayConstructor(items) => items.iter().any(expr_references_columns),
        Expr::Exists(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::InSubquery { .. }
        | Expr::Parameter(_)
        | Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Default
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::MultiColumnSubqueryRef { .. }
        | Expr::TypedLiteral { .. }
        | Expr::RowConstructor(_) => false,
        Expr::ArraySubscript { expr, index, .. } => {
            expr_references_columns(expr) || expr_references_columns(index)
        }
        Expr::ArraySlice {
            expr, start, end, ..
        } => {
            expr_references_columns(expr)
                || start.as_deref().is_some_and(expr_references_columns)
                || end.as_deref().is_some_and(expr_references_columns)
        }
    }
}

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    use ScalarValue::{Bool, Float, Int, Null, Numeric, Text};
    match (left, right) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(left), Bool(right)) => left.cmp(right),
        (Int(left), Int(right)) => left.cmp(right),
        (Float(left), Float(right)) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
        (Numeric(left), Numeric(right)) => left.cmp(right),
        (Text(left), Text(right)) => left.cmp(right),
        (Int(left), Float(right)) => (*left as f64).partial_cmp(right).unwrap_or(Ordering::Equal),
        (Float(left), Int(right)) => left
            .partial_cmp(&(*right as f64))
            .unwrap_or(Ordering::Equal),
        _ => left.render().cmp(&right.render()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        WindowArgumentKind, WindowColumnPlan, WindowPartitions, eval_window_function_columnar,
    };
    use crate::executor::column_batch::ColumnBatch;
    use crate::parser::ast::{OrderByExpr, WindowFrame, WindowFrameBound, WindowFrameUnits};
    use crate::storage::tuple::ScalarValue;

    fn order_by(expr: &str) -> OrderByExpr {
        OrderByExpr {
            expr: crate::parser::ast::Expr::Identifier(vec![expr.to_string()]),
            ascending: Some(true),
            using_operator: None,
        }
    }

    #[test]
    fn builds_partitions_once() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(2)],
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(1)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(3)],
            ],
            &["dept".to_string(), "id".to_string()],
        );
        let partitions =
            WindowPartitions::build(&batch, &[0], &[1], &[order_by("id")]).expect("build");

        assert_eq!(partitions.partitions.len(), 2);
        assert_eq!(partitions.partition_for_row(0).row_indices, vec![1, 0]);
        assert_eq!(partitions.position_for_row(1), 0);
    }

    #[test]
    fn evaluates_row_number_rank_and_lag() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Int(1),
                    ScalarValue::Int(10),
                ],
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Int(2),
                    ScalarValue::Int(10),
                ],
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Int(3),
                    ScalarValue::Int(20),
                ],
            ],
            &["dept".to_string(), "id".to_string(), "score".to_string()],
        );
        let partitions =
            WindowPartitions::build(&batch, &[0], &[2], &[order_by("score")]).expect("build");

        let row_number = eval_window_function_columnar(
            &WindowColumnPlan {
                function_name: "row_number".to_string(),
                argument_kinds: Vec::new(),
                partition_by_indices: vec![0],
                order_by: vec![order_by("score")],
                order_by_indices: vec![2],
                frame: None,
            },
            &partitions,
            &batch,
        )
        .expect("row_number should evaluate");
        let rank = eval_window_function_columnar(
            &WindowColumnPlan {
                function_name: "rank".to_string(),
                argument_kinds: Vec::new(),
                partition_by_indices: vec![0],
                order_by: vec![order_by("score")],
                order_by_indices: vec![2],
                frame: None,
            },
            &partitions,
            &batch,
        )
        .expect("rank should evaluate");
        let lag = eval_window_function_columnar(
            &WindowColumnPlan {
                function_name: "lag".to_string(),
                argument_kinds: vec![
                    WindowArgumentKind::Column(2),
                    WindowArgumentKind::Constant(ScalarValue::Int(1)),
                    WindowArgumentKind::Constant(ScalarValue::Int(0)),
                ],
                partition_by_indices: vec![0],
                order_by: vec![order_by("score")],
                order_by_indices: vec![2],
                frame: None,
            },
            &partitions,
            &batch,
        )
        .expect("lag should evaluate");

        assert_eq!(
            row_number,
            vec![
                ScalarValue::Int(1),
                ScalarValue::Int(2),
                ScalarValue::Int(3)
            ]
        );
        assert_eq!(
            rank,
            vec![
                ScalarValue::Int(1),
                ScalarValue::Int(1),
                ScalarValue::Int(3)
            ]
        );
        assert_eq!(
            lag,
            vec![
                ScalarValue::Int(0),
                ScalarValue::Int(10),
                ScalarValue::Int(10)
            ]
        );
    }

    #[test]
    fn evaluates_rows_frame_sum() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Int(10)],
                vec![ScalarValue::Int(2), ScalarValue::Int(20)],
                vec![ScalarValue::Int(3), ScalarValue::Int(30)],
            ],
            &["id".to_string(), "value".to_string()],
        );
        let partitions =
            WindowPartitions::build(&batch, &[], &[0], &[order_by("id")]).expect("build");
        let values = eval_window_function_columnar(
            &WindowColumnPlan {
                function_name: "sum".to_string(),
                argument_kinds: vec![WindowArgumentKind::Column(1)],
                partition_by_indices: Vec::new(),
                order_by: vec![order_by("id")],
                order_by_indices: vec![0],
                frame: Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowFrameBound::OffsetPreceding(crate::parser::ast::Expr::Integer(1)),
                    end: WindowFrameBound::CurrentRow,
                    exclusion: None,
                }),
            },
            &partitions,
            &batch,
        )
        .expect("sum should evaluate");

        assert_eq!(
            values,
            vec![
                ScalarValue::Int(10),
                ScalarValue::Int(30),
                ScalarValue::Int(50)
            ]
        );
    }
}
