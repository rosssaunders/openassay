use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::commands::sequence::{
    normalize_sequence_name_from_text, sequence_next_value, set_sequence_value,
    with_sequences_read, with_sequences_write,
};
use crate::parser::ast::{
    BinaryOp, ComparisonQuantifier, Expr, OrderByExpr, UnaryOp, WindowFrameBound,
    WindowFrameUnits, WindowSpec,
};
use crate::security;
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::{
    drain_native_ws_messages, execute_planned_query, native_ws_handles, plan_statement,
    with_ext_read, with_ext_write, ws_native, EngineError, QueryResult, WsConnection,
};
use crate::executor::exec_main::{
    compare_order_keys, eval_aggregate_function, execute_query_with_outer,
    is_aggregate_function, json_value_to_scalar, parse_non_negative_int, row_key,
};

pub(crate) type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct EvalScope {
    unqualified: HashMap<String, ScalarValue>,
    qualified: HashMap<String, ScalarValue>,
    ambiguous: HashSet<String>,
}

impl EvalScope {
    pub(crate) fn from_output_row(columns: &[String], row: &[ScalarValue]) -> Self {
        let mut scope = Self::default();
        for (col, value) in columns.iter().zip(row.iter()) {
            scope.insert_unqualified(col, value.clone());
        }
        scope
    }

    pub(crate) fn insert_unqualified(&mut self, key: &str, value: ScalarValue) {
        let key = key.to_ascii_lowercase();
        if self.ambiguous.contains(&key) {
            return;
        }
        #[allow(clippy::map_entry)]
        if self.unqualified.contains_key(&key) {
            self.unqualified.remove(&key);
            self.ambiguous.insert(key);
        } else {
            self.unqualified.insert(key, value);
        }
    }

    pub(crate) fn insert_qualified(&mut self, key: &str, value: ScalarValue) {
        self.qualified.insert(key.to_ascii_lowercase(), value);
    }

    pub(crate) fn lookup_identifier(&self, parts: &[String]) -> Result<ScalarValue, EngineError> {
        if parts.is_empty() {
            return Err(EngineError {
                message: "empty identifier".to_string(),
            });
        }

        if parts.len() == 1 {
            let key = parts[0].to_ascii_lowercase();
            if self.ambiguous.contains(&key) {
                return Err(EngineError {
                    message: format!("column reference \"{}\" is ambiguous", parts[0]),
                });
            }
            return self
                .unqualified
                .get(&key)
                .cloned()
                .ok_or_else(|| EngineError {
                    message: format!("unknown column \"{}\"", parts[0]),
                });
        }

        let key = parts
            .iter()
            .map(|p| p.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified
            .get(&key)
            .cloned()
            .ok_or_else(|| EngineError {
                message: format!("unknown column \"{}\"", parts.join(".")),
            })
    }

    pub(crate) fn lookup_join_column(&self, column: &str) -> Option<ScalarValue> {
        let key = column.to_ascii_lowercase();
        if !self.ambiguous.contains(&key)
            && let Some(value) = self.unqualified.get(&key)
        {
            return Some(value.clone());
        }

        let suffix = format!(".{}", key);
        let mut matches = self
            .qualified
            .iter()
            .filter_map(|(name, value)| name.ends_with(&suffix).then_some(value.clone()));
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(first)
    }

    pub(crate) fn force_unqualified(&mut self, key: &str, value: ScalarValue) {
        let key = key.to_ascii_lowercase();
        self.ambiguous.remove(&key);
        self.unqualified.insert(key, value);
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        for key in &other.ambiguous {
            self.unqualified.remove(key);
            self.ambiguous.insert(key.clone());
        }

        for (key, value) in &other.unqualified {
            self.insert_unqualified(key, value.clone());
        }
        for (key, value) in &other.qualified {
            self.qualified.insert(key.clone(), value.clone());
        }
    }

    pub(crate) fn inherit_outer(&mut self, outer: &Self) {
        for (key, value) in &outer.unqualified {
            if !self.unqualified.contains_key(key) && !self.ambiguous.contains(key) {
                self.unqualified.insert(key.clone(), value.clone());
            }
        }
        for (key, value) in &outer.qualified {
            self.qualified
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

pub(crate) fn eval_expr<'a>(
    expr: &'a Expr,
    scope: &'a EvalScope,
    params: &'a [Option<String>],
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
        Expr::Null => Ok(ScalarValue::Null),
        Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
        Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
        Expr::Float(v) => {
            let parsed = v.parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid float literal \"{v}\""),
            })?;
            Ok(ScalarValue::Float(parsed))
        }
        Expr::String(v) => Ok(ScalarValue::Text(v.clone())),
        Expr::Parameter(idx) => parse_param(*idx, params),
        Expr::Identifier(parts) => scope.lookup_identifier(parts),
        Expr::Unary { op, expr } => {
            let value = eval_expr(expr, scope, params).await?;
            eval_unary(op.clone(), value)
        }
        Expr::Binary { left, op, right } => {
            let lhs = eval_expr(left, scope, params).await?;
            let rhs = eval_expr(right, scope, params).await?;
            eval_binary(op.clone(), lhs, rhs)
        }
        Expr::AnyAll {
            left,
            op,
            right,
            quantifier,
        } => {
            let lhs = eval_expr(left, scope, params).await?;
            let rhs = eval_expr(right, scope, params).await?;
            eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
        }
        Expr::Exists(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            Ok(ScalarValue::Bool(!result.rows.is_empty()))
        }
        Expr::ScalarSubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if result.rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if result.rows.len() > 1 {
                return Err(EngineError {
                    message: "more than one row returned by a subquery used as an expression"
                        .to_string(),
                });
            }
            if result.rows[0].len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            Ok(result.rows[0][0].clone())
        }
        Expr::ArrayConstructor(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_expr(item, scope, params).await?);
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::ArraySubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let mut values = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                values.push(row.first().cloned().unwrap_or(ScalarValue::Null));
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lhs = eval_expr(expr, scope, params).await?;
            let mut rhs_values = Vec::with_capacity(list.len());
            for item in list {
                rhs_values.push(eval_expr(item, scope, params).await?);
            }
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let lhs = eval_expr(expr, scope, params).await?;
            let result = execute_query_with_outer(subquery, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let rhs_values = result
                .rows
                .iter()
                .map(|row| row.first().cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr(expr, scope, params).await?;
            let low_value = eval_expr(low, scope, params).await?;
            let high_value = eval_expr(high, scope, params).await?;
            eval_between_predicate(value, low_value, high_value, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let value = eval_expr(expr, scope, params).await?;
            let pattern_value = eval_expr(pattern, scope, params).await?;
            eval_like_predicate(value, pattern_value, *case_insensitive, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, scope, params).await?;
            let is_null = matches!(value, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_value = eval_expr(left, scope, params).await?;
            let right_value = eval_expr(right, scope, params).await?;
            eval_is_distinct_from(left_value, right_value, *negated)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let operand_value = eval_expr(operand, scope, params).await?;
            for (when_expr, then_expr) in when_then {
                let when_value = eval_expr(when_expr, scope, params).await?;
                if matches!(operand_value, ScalarValue::Null)
                    || matches!(when_value, ScalarValue::Null)
                {
                    continue;
                }
                if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal {
                    return eval_expr(then_expr, scope, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, scope, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when_expr, then_expr) in when_then {
                let condition = eval_expr(when_expr, scope, params).await?;
                if truthy(&condition) {
                    return eval_expr(then_expr, scope, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr(else_expr, scope, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::Cast { expr, type_name } => {
            let value = eval_expr(expr, scope, params).await?;
            eval_cast_scalar(value, type_name)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => eval_function(
            name,
            args,
            *distinct,
            order_by,
            within_group,
            filter.as_deref(),
            over.as_deref(),
            scope,
            params,
        )
        .await,
        Expr::Wildcard => Err(EngineError {
            message: "wildcard expression requires FROM support".to_string(),
        }),
    }
    })
}

pub(crate) fn eval_expr_with_window<'a>(
    expr: &'a Expr,
    scope: &'a EvalScope,
    row_idx: usize,
    all_rows: &'a [EvalScope],
    params: &'a [Option<String>],
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
        Expr::Null => Ok(ScalarValue::Null),
        Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
        Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
        Expr::Float(v) => {
            let parsed = v.parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid float literal \"{v}\""),
            })?;
            Ok(ScalarValue::Float(parsed))
        }
        Expr::String(v) => Ok(ScalarValue::Text(v.clone())),
        Expr::Parameter(idx) => parse_param(*idx, params),
        Expr::Identifier(parts) => scope.lookup_identifier(parts),
        Expr::Unary { op, expr } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            eval_unary(op.clone(), value)
        }
        Expr::Binary { left, op, right } => {
            let lhs = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let rhs = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_binary(op.clone(), lhs, rhs)
        }
        Expr::AnyAll {
            left,
            op,
            right,
            quantifier,
        } => {
            let lhs = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let rhs = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
        }
        Expr::Exists(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            Ok(ScalarValue::Bool(!result.rows.is_empty()))
        }
        Expr::ScalarSubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if result.rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if result.rows.len() > 1 {
                return Err(EngineError {
                    message: "more than one row returned by a subquery used as an expression"
                        .to_string(),
                });
            }
            if result.rows[0].len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            Ok(result.rows[0][0].clone())
        }
        Expr::ArrayConstructor(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_expr_with_window(item, scope, row_idx, all_rows, params).await?);
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::ArraySubquery(query) => {
            let result = execute_query_with_outer(query, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let mut values = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                values.push(row.first().cloned().unwrap_or(ScalarValue::Null));
            }
            Ok(ScalarValue::Array(values))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lhs = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let mut rhs = Vec::with_capacity(list.len());
            for item in list {
                rhs.push(eval_expr_with_window(item, scope, row_idx, all_rows, params).await?);
            }
            eval_in_membership(lhs, rhs, *negated)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let lhs = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let result = execute_query_with_outer(subquery, params, Some(scope)).await?;
            if !result.columns.is_empty() && result.columns.len() != 1 {
                return Err(EngineError {
                    message: "subquery must return only one column".to_string(),
                });
            }
            let rhs_values = result
                .rows
                .iter()
                .map(|row| row.first().cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();
            eval_in_membership(lhs, rhs_values, *negated)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let low_value = eval_expr_with_window(low, scope, row_idx, all_rows, params).await?;
            let high_value = eval_expr_with_window(high, scope, row_idx, all_rows, params).await?;
            eval_between_predicate(value, low_value, high_value, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let pattern_value =
                eval_expr_with_window(pattern, scope, row_idx, all_rows, params).await?;
            eval_like_predicate(value, pattern_value, *case_insensitive, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            let is_null = matches!(value, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::IsDistinctFrom {
            left,
            right,
            negated,
        } => {
            let left_value = eval_expr_with_window(left, scope, row_idx, all_rows, params).await?;
            let right_value = eval_expr_with_window(right, scope, row_idx, all_rows, params).await?;
            eval_is_distinct_from(left_value, right_value, *negated)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let operand_value =
                eval_expr_with_window(operand, scope, row_idx, all_rows, params).await?;
            for (when_expr, then_expr) in when_then {
                let when_value =
                    eval_expr_with_window(when_expr, scope, row_idx, all_rows, params).await?;
                if matches!(operand_value, ScalarValue::Null)
                    || matches!(when_value, ScalarValue::Null)
                {
                    continue;
                }
                if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal {
                    return eval_expr_with_window(then_expr, scope, row_idx, all_rows, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr_with_window(else_expr, scope, row_idx, all_rows, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when_expr, then_expr) in when_then {
                let condition =
                    eval_expr_with_window(when_expr, scope, row_idx, all_rows, params).await?;
                if truthy(&condition) {
                    return eval_expr_with_window(then_expr, scope, row_idx, all_rows, params).await;
                }
            }
            if let Some(else_expr) = else_expr {
                eval_expr_with_window(else_expr, scope, row_idx, all_rows, params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        Expr::Cast { expr, type_name } => {
            let value = eval_expr_with_window(expr, scope, row_idx, all_rows, params).await?;
            eval_cast_scalar(value, type_name)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => {
            if let Some(window) = over.as_deref() {
                if !within_group.is_empty() {
                    return Err(EngineError {
                        message: "WITHIN GROUP is not allowed for window functions".to_string(),
                    });
                }
                eval_window_function(
                    name,
                    args,
                    *distinct,
                    order_by,
                filter.as_deref(),
                window,
                row_idx,
                all_rows,
                params,
            )
            .await
            } else {
                let fn_name = name
                    .last()
                    .map(|n| n.to_ascii_lowercase())
                    .unwrap_or_default();
                if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
                    return Err(EngineError {
                        message: format!(
                            "{}() aggregate modifiers require grouped aggregate evaluation",
                            fn_name
                        ),
                    });
                }
                if is_aggregate_function(&fn_name) {
                    return Err(EngineError {
                        message: format!(
                            "aggregate function {}() must be used with grouped evaluation",
                            fn_name
                        ),
                    });
                }

                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(
                        eval_expr_with_window(arg, scope, row_idx, all_rows, params).await?,
                    );
                }
                eval_scalar_function(&fn_name, &values).await
            }
        }
        Expr::Wildcard => Err(EngineError {
            message: "wildcard expression requires FROM support".to_string(),
        }),
    }
    })
}

#[allow(clippy::too_many_arguments)]
async fn eval_window_function(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    filter: Option<&Expr>,
    window: &WindowSpec,
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let fn_name = name
        .last()
        .map(|n| n.to_ascii_lowercase())
        .unwrap_or_default();
    let mut partition = window_partition_rows(window, row_idx, all_rows, params).await?;
    let order_keys = window_order_keys(window, &mut partition, all_rows, params).await?;
    let current_pos = partition
        .iter()
        .position(|entry| *entry == row_idx)
        .ok_or_else(|| EngineError {
            message: "window row not found in partition".to_string(),
        })?;

    match fn_name.as_str() {
        "row_number" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "row_number() does not accept arguments".to_string(),
                });
            }
            Ok(ScalarValue::Int((current_pos + 1) as i64))
        }
        "rank" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "rank() does not accept arguments".to_string(),
                });
            }
            if order_keys.is_empty() {
                return Ok(ScalarValue::Int(1));
            }
            let mut rank = 1usize;
            for idx in 1..=current_pos {
                if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by)
                    != Ordering::Equal
                {
                    rank = idx + 1;
                }
            }
            Ok(ScalarValue::Int(rank as i64))
        }
        "dense_rank" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "dense_rank() does not accept arguments".to_string(),
                });
            }
            if order_keys.is_empty() {
                return Ok(ScalarValue::Int(1));
            }
            let mut rank = 1i64;
            for idx in 1..=current_pos {
                if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by)
                    != Ordering::Equal
                {
                    rank += 1;
                }
            }
            Ok(ScalarValue::Int(rank))
        }
        "lag" | "lead" => {
            if args.is_empty() || args.len() > 3 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects 1 to 3 arguments"),
                });
            }
            let offset = if let Some(offset_expr) = args.get(1) {
                let offset_value = eval_expr(offset_expr, &all_rows[row_idx], params).await?;
                if matches!(offset_value, ScalarValue::Null) {
                    return Ok(ScalarValue::Null);
                }
                parse_non_negative_int(&offset_value, "window offset")?
            } else {
                1usize
            };
            let target_pos = if fn_name == "lag" {
                current_pos.checked_sub(offset)
            } else {
                current_pos.checked_add(offset)
            };
            let Some(target_pos) = target_pos else {
                return Ok(if let Some(default_expr) = args.get(2) {
                    eval_expr(default_expr, &all_rows[row_idx], params).await?
                } else {
                    ScalarValue::Null
                });
            };
            if target_pos >= partition.len() {
                return Ok(if let Some(default_expr) = args.get(2) {
                    eval_expr(default_expr, &all_rows[row_idx], params).await?
                } else {
                    ScalarValue::Null
                });
            }
            let target_row = partition[target_pos];
            eval_expr(&args[0], &all_rows[target_row], params).await
        }
        "ntile" => {
            if args.len() != 1 {
                return Err(EngineError { message: "ntile() expects exactly one argument".to_string() });
            }
            let n = {
                let v = eval_expr(&args[0], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "ntile() expects integer")? as usize
            };
            if n == 0 { return Err(EngineError { message: "ntile() argument must be positive".to_string() }); }
            let total = partition.len();
            let bucket = (current_pos * n / total) + 1;
            Ok(ScalarValue::Int(bucket as i64))
        }
        "percent_rank" => {
            if !args.is_empty() { return Err(EngineError { message: "percent_rank() takes no arguments".to_string() }); }
            if partition.len() <= 1 { return Ok(ScalarValue::Float(0.0)); }
            // rank - 1 / count - 1
            let mut rank = 1usize;
            if !order_keys.is_empty() {
                for idx in 1..=current_pos {
                    if compare_order_keys(&order_keys[idx - 1], &order_keys[idx], &window.order_by) != Ordering::Equal {
                        rank = idx + 1;
                    }
                }
            }
            Ok(ScalarValue::Float((rank - 1) as f64 / (partition.len() - 1) as f64))
        }
        "cume_dist" => {
            if !args.is_empty() { return Err(EngineError { message: "cume_dist() takes no arguments".to_string() }); }
            if order_keys.is_empty() { return Ok(ScalarValue::Float(1.0)); }
            // Number of rows <= current row / total rows
            let current_key = &order_keys[current_pos];
            let count_le = order_keys.iter().filter(|k| {
                compare_order_keys(k, current_key, &window.order_by) != Ordering::Greater
            }).count();
            Ok(ScalarValue::Float(count_le as f64 / partition.len() as f64))
        }
        "first_value" => {
            if args.len() != 1 { return Err(EngineError { message: "first_value() expects one argument".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&first) = frame_rows.first() {
                eval_expr(&args[0], &all_rows[first], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "last_value" => {
            if args.len() != 1 { return Err(EngineError { message: "last_value() expects one argument".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&last) = frame_rows.last() {
                eval_expr(&args[0], &all_rows[last], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "nth_value" => {
            if args.len() != 2 { return Err(EngineError { message: "nth_value() expects two arguments".to_string() }); }
            let n = {
                let v = eval_expr(&args[1], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "nth_value() expects integer")? as usize
            };
            if n == 0 { return Err(EngineError { message: "nth_value() argument must be positive".to_string() }); }
            let frame_rows =
                window_frame_rows(window, &partition, &order_keys, current_pos, all_rows, params)
                    .await?;
            if let Some(&target) = frame_rows.get(n - 1) {
                eval_expr(&args[0], &all_rows[target], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "sum" | "count" | "avg" | "min" | "max" | "string_agg" | "array_agg" => {
            let frame_rows = window_frame_rows(
                window,
                &partition,
                &order_keys,
                current_pos,
                all_rows,
                params,
            )
            .await?;
            let scoped_rows = frame_rows
                .iter()
                .map(|idx| all_rows[*idx].clone())
                .collect::<Vec<_>>();
            eval_aggregate_function(
                &fn_name,
                args,
                distinct,
                order_by,
                &[],
                filter,
                &scoped_rows,
                params,
            )
            .await
        }
        _ => Err(EngineError {
            message: format!("unsupported window function {}", fn_name),
        }),
    }
}

async fn window_partition_rows(
    window: &WindowSpec,
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<usize>, EngineError> {
    if window.partition_by.is_empty() {
        return Ok((0..all_rows.len()).collect());
    }
    let current_scope = &all_rows[row_idx];
    let mut current_key = Vec::with_capacity(window.partition_by.len());
    for expr in &window.partition_by {
        current_key.push(eval_expr(expr, current_scope, params).await?);
    }
    let current_key = row_key(&current_key);
    let mut out = Vec::new();
    for (idx, scope) in all_rows.iter().enumerate() {
        let mut key_values = Vec::with_capacity(window.partition_by.len());
        for expr in &window.partition_by {
            key_values.push(eval_expr(expr, scope, params).await?);
        }
        if row_key(&key_values) == current_key {
            out.push(idx);
        }
    }
    Ok(out)
}

async fn window_order_keys(
    window: &WindowSpec,
    partition: &mut [usize],
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    if window.order_by.is_empty() {
        return Ok(Vec::new());
    }
    let mut decorated = Vec::with_capacity(partition.len());
    for idx in partition.iter().copied() {
        let mut keys = Vec::with_capacity(window.order_by.len());
        for order in &window.order_by {
            keys.push(eval_expr(&order.expr, &all_rows[idx], params).await?);
        }
        decorated.push((idx, keys));
    }
    decorated.sort_by(|left, right| compare_order_keys(&left.1, &right.1, &window.order_by));
    for (slot, (idx, _)) in partition.iter_mut().zip(decorated.iter()) {
        *slot = *idx;
    }
    Ok(decorated.into_iter().map(|(_, keys)| keys).collect())
}

async fn window_frame_rows(
    window: &WindowSpec,
    partition: &[usize],
    order_keys: &[Vec<ScalarValue>],
    current_pos: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<usize>, EngineError> {
    let Some(frame) = window.frame.as_ref() else {
        return Ok(partition.to_vec());
    };

    match frame.units {
        WindowFrameUnits::Rows => {
            let start = frame_row_position(
                &frame.start,
                current_pos,
                partition.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            let end = frame_row_position(
                &frame.end,
                current_pos,
                partition.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            if start > end {
                return Ok(Vec::new());
            }
            Ok(partition[start..=end].to_vec())
        }
        WindowFrameUnits::Range => {
            if window.order_by.is_empty() {
                return Ok(partition.to_vec());
            }
            let ascending = window.order_by[0].ascending != Some(false);
            let current_value = window_first_order_numeric_key(
                order_keys,
                current_pos,
                partition[current_pos],
                all_rows,
                window,
                params,
            )
            .await?;
            let effective_current = if ascending { current_value } else { -current_value };
            let start = range_bound_threshold(
                &frame.start,
                effective_current,
                true,
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            let end = range_bound_threshold(
                &frame.end,
                effective_current,
                false,
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;
            if let (Some(start), Some(end)) = (start, end)
                && start > end
            {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            for (pos, row_idx) in partition.iter().enumerate() {
                let value = window_first_order_numeric_key(
                    order_keys,
                    pos,
                    *row_idx,
                    all_rows,
                    window,
                    params,
                )
                .await?;
                let effective = if ascending { value } else { -value };
                if let Some(start) = start
                    && effective < start
                {
                    continue;
                }
                if let Some(end) = end
                    && effective > end
                {
                    continue;
                }
                out.push(*row_idx);
            }
            Ok(out)
        }
    }
}

async fn frame_row_position(
    bound: &WindowFrameBound,
    current_pos: usize,
    partition_len: usize,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<usize, EngineError> {
    let max_pos = partition_len.saturating_sub(1);
    Ok(match bound {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::UnboundedFollowing => max_pos,
        WindowFrameBound::CurrentRow => current_pos,
        WindowFrameBound::OffsetPreceding(expr) => {
            let offset = frame_bound_offset_usize(expr, current_scope, params).await?;
            current_pos.saturating_sub(offset)
        }
        WindowFrameBound::OffsetFollowing(expr) => {
            let offset = frame_bound_offset_usize(expr, current_scope, params).await?;
            current_pos.saturating_add(offset).min(max_pos)
        }
    })
}

async fn frame_bound_offset_usize(
    expr: &Expr,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<usize, EngineError> {
    let value = eval_expr(expr, current_scope, params).await?;
    parse_non_negative_int(&value, "window frame offset")
}

async fn frame_bound_offset_f64(
    expr: &Expr,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<f64, EngineError> {
    let value = eval_expr(expr, current_scope, params).await?;
    let parsed = parse_f64_scalar(&value, "window frame offset must be numeric")?;
    if parsed < 0.0 {
        return Err(EngineError {
            message: "window frame offset must be a non-negative number".to_string(),
        });
    }
    Ok(parsed)
}

async fn range_bound_threshold(
    bound: &WindowFrameBound,
    current: f64,
    is_start: bool,
    current_scope: &EvalScope,
    params: &[Option<String>],
) -> Result<Option<f64>, EngineError> {
    match bound {
        WindowFrameBound::UnboundedPreceding if is_start => Ok(None),
        WindowFrameBound::UnboundedFollowing if !is_start => Ok(None),
        WindowFrameBound::UnboundedFollowing if is_start => Ok(Some(f64::INFINITY)),
        WindowFrameBound::UnboundedPreceding if !is_start => Ok(Some(f64::NEG_INFINITY)),
        WindowFrameBound::CurrentRow => Ok(Some(current)),
        WindowFrameBound::OffsetPreceding(expr) => Ok(Some(
            current - frame_bound_offset_f64(expr, current_scope, params).await?,
        )),
        WindowFrameBound::OffsetFollowing(expr) => Ok(Some(
            current + frame_bound_offset_f64(expr, current_scope, params).await?,
        )),
        WindowFrameBound::UnboundedPreceding | WindowFrameBound::UnboundedFollowing => {
            Err(EngineError {
                message: "invalid RANGE frame bound configuration".to_string(),
            })
        }
    }
}

async fn window_first_order_numeric_key(
    order_keys: &[Vec<ScalarValue>],
    pos: usize,
    row_idx: usize,
    all_rows: &[EvalScope],
    window: &WindowSpec,
    params: &[Option<String>],
) -> Result<f64, EngineError> {
    if let Some(value) = order_keys.get(pos).and_then(|keys| keys.first()) {
        return parse_f64_scalar(value, "RANGE frame ORDER BY key must be numeric");
    }
    let expr = &window.order_by[0].expr;
    let value = eval_expr(expr, &all_rows[row_idx], params).await?;
    parse_f64_scalar(&value, "RANGE frame ORDER BY key must be numeric")
}

pub(crate) fn eval_in_membership(
    lhs: ScalarValue,
    rhs_values: Vec<ScalarValue>,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(lhs, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let mut saw_null = false;
    for rhs in rhs_values {
        if matches!(rhs, ScalarValue::Null) {
            saw_null = true;
            continue;
        }
        if compare_values_for_predicate(&lhs, &rhs)? == Ordering::Equal {
            return Ok(ScalarValue::Bool(!negated));
        }
    }

    if saw_null {
        Ok(ScalarValue::Null)
    } else {
        Ok(ScalarValue::Bool(negated))
    }
}

pub(crate) fn eval_between_predicate(
    value: ScalarValue,
    low: ScalarValue,
    high: ScalarValue,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null)
        || matches!(low, ScalarValue::Null)
        || matches!(high, ScalarValue::Null)
    {
        return Ok(ScalarValue::Null);
    }
    let in_range = compare_values_for_predicate(&value, &low)? != Ordering::Less
        && compare_values_for_predicate(&value, &high)? != Ordering::Greater;
    Ok(ScalarValue::Bool(if negated {
        !in_range
    } else {
        in_range
    }))
}

pub(crate) fn eval_like_predicate(
    value: ScalarValue,
    pattern: ScalarValue,
    case_insensitive: bool,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) || matches!(pattern, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut text = value.render();
    let mut pattern_text = pattern.render();
    if case_insensitive {
        text = text.to_ascii_lowercase();
        pattern_text = pattern_text.to_ascii_lowercase();
    }
    let matched = like_match(&text, &pattern_text);
    Ok(ScalarValue::Bool(if negated { !matched } else { matched }))
}

pub(crate) fn eval_is_distinct_from(
    left: ScalarValue,
    right: ScalarValue,
    negated: bool,
) -> Result<ScalarValue, EngineError> {
    let distinct = match (&left, &right) {
        (ScalarValue::Null, ScalarValue::Null) => false,
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => true,
        _ => compare_values_for_predicate(&left, &right)? != Ordering::Equal,
    };
    Ok(ScalarValue::Bool(if negated {
        !distinct
    } else {
        distinct
    }))
}

pub(crate) fn eval_cast_scalar(
    value: ScalarValue,
    type_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match type_name {
        "boolean" => Ok(ScalarValue::Bool(parse_bool_scalar(
            &value,
            "cannot cast value to boolean",
        )?)),
        "int8" => Ok(ScalarValue::Int(parse_i64_scalar(
            &value,
            "cannot cast value to bigint",
        )?)),
        "float8" => Ok(ScalarValue::Float(parse_f64_scalar(
            &value,
            "cannot cast value to double precision",
        )?)),
        "text" => Ok(ScalarValue::Text(value.render())),
        "date" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        "timestamp" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        other => Err(EngineError {
            message: format!("unsupported cast type {}", other),
        }),
    }
}

fn like_match(value: &str, pattern: &str) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let mut memo = HashMap::new();
    like_match_recursive(&value_chars, &pattern_chars, 0, 0, &mut memo)
}

fn like_match_recursive(
    value: &[char],
    pattern: &[char],
    vi: usize,
    pi: usize,
    memo: &mut HashMap<(usize, usize), bool>,
) -> bool {
    if let Some(cached) = memo.get(&(vi, pi)) {
        return *cached;
    }

    let result = if pi >= pattern.len() {
        vi >= value.len()
    } else {
        match pattern[pi] {
            '%' => {
                let mut i = vi;
                let mut matched = false;
                while i <= value.len() {
                    if like_match_recursive(value, pattern, i, pi + 1, memo) {
                        matched = true;
                        break;
                    }
                    i += 1;
                }
                matched
            }
            '_' => vi < value.len() && like_match_recursive(value, pattern, vi + 1, pi + 1, memo),
            '\\' => {
                if pi + 1 >= pattern.len() {
                    vi < value.len()
                        && value[vi] == '\\'
                        && like_match_recursive(value, pattern, vi + 1, pi + 1, memo)
                } else {
                    vi < value.len()
                        && value[vi] == pattern[pi + 1]
                        && like_match_recursive(value, pattern, vi + 1, pi + 2, memo)
                }
            }
            ch => {
                vi < value.len()
                    && value[vi] == ch
                    && like_match_recursive(value, pattern, vi + 1, pi + 1, memo)
            }
        }
    };

    memo.insert((vi, pi), result);
    result
}

fn parse_param(index: i32, params: &[Option<String>]) -> Result<ScalarValue, EngineError> {
    if index <= 0 {
        return Err(EngineError {
            message: format!("invalid parameter reference ${index}"),
        });
    }
    let idx = (index - 1) as usize;
    let value = params.get(idx).ok_or_else(|| EngineError {
        message: format!("missing value for parameter ${index}"),
    })?;

    let Some(raw) = value else {
        return Ok(ScalarValue::Null);
    };

    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(ScalarValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(ScalarValue::Bool(false));
    }
    if let Ok(v) = trimmed.parse::<i64>() {
        return Ok(ScalarValue::Int(v));
    }
    if let Ok(v) = trimmed.parse::<f64>() {
        return Ok(ScalarValue::Float(v));
    }
    Ok(ScalarValue::Text(raw.clone()))
}

pub(crate) fn eval_unary(op: UnaryOp, value: ScalarValue) -> Result<ScalarValue, EngineError> {
    match (op, value) {
        (UnaryOp::Not, ScalarValue::Bool(v)) => Ok(ScalarValue::Bool(!v)),
        (UnaryOp::Not, ScalarValue::Null) => Ok(ScalarValue::Null),
        (UnaryOp::Plus, ScalarValue::Int(v)) => Ok(ScalarValue::Int(v)),
        (UnaryOp::Plus, ScalarValue::Float(v)) => Ok(ScalarValue::Float(v)),
        (UnaryOp::Minus, ScalarValue::Int(v)) => Ok(ScalarValue::Int(-v)),
        (UnaryOp::Minus, ScalarValue::Float(v)) => Ok(ScalarValue::Float(-v)),
        _ => Err(EngineError {
            message: "invalid unary operation".to_string(),
        }),
    }
}

pub(crate) fn eval_binary(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    use BinaryOp::*;
    match op {
        Or => eval_logical_or(left, right),
        And => eval_logical_and(left, right),
        Eq => eval_comparison(left, right, |ord| ord == Ordering::Equal),
        NotEq => eval_comparison(left, right, |ord| ord != Ordering::Equal),
        Lt => eval_comparison(left, right, |ord| ord == Ordering::Less),
        Lte => eval_comparison(left, right, |ord| {
            matches!(ord, Ordering::Less | Ordering::Equal)
        }),
        Gt => eval_comparison(left, right, |ord| ord == Ordering::Greater),
        Gte => eval_comparison(left, right, |ord| {
            matches!(ord, Ordering::Greater | Ordering::Equal)
        }),
        Add => eval_add(left, right),
        Sub => eval_sub(left, right),
        Mul => numeric_bin(left, right, |a, b| a * b, |a, b| a * b),
        Div => numeric_div(left, right),
        Mod => numeric_mod(left, right),
        JsonGet => eval_json_get_operator(left, right, false),
        JsonGetText => eval_json_get_operator(left, right, true),
        JsonPath => eval_json_path_operator(left, right, false),
        JsonPathText => eval_json_path_operator(left, right, true),
        JsonPathExists => eval_json_path_predicate_operator(left, right, false),
        JsonPathMatch => eval_json_path_predicate_operator(left, right, true),
        JsonConcat => eval_json_concat_operator(left, right),
        JsonContains => eval_json_contains_operator(left, right),
        JsonContainedBy => eval_json_contained_by_operator(left, right),
        JsonHasKey => eval_json_has_key_operator(left, right),
        JsonHasAny => eval_json_has_any_all_operator(left, right, true),
        JsonHasAll => eval_json_has_any_all_operator(left, right, false),
        JsonDeletePath => eval_json_delete_path_operator(left, right),
    }
}

fn eval_add(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    if parse_numeric_operand(&left).is_ok() && parse_numeric_operand(&right).is_ok() {
        return numeric_bin(left, right, |a, b| a + b, |a, b| a + b);
    }

    if let Some(lhs) = parse_temporal_operand(&left) {
        let days = parse_i64_scalar(&right, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(lhs, days));
    }
    if let Some(rhs) = parse_temporal_operand(&right) {
        let days = parse_i64_scalar(&left, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(rhs, days));
    }

    Err(EngineError {
        message: "numeric operation expects numeric values".to_string(),
    })
}

fn eval_sub(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    if parse_numeric_operand(&left).is_ok() && parse_numeric_operand(&right).is_ok() {
        return numeric_bin(left, right, |a, b| a - b, |a, b| a - b);
    }

    if let Some(lhs) = parse_temporal_operand(&left) {
        if let Some(rhs) = parse_temporal_operand(&right) {
            if lhs.date_only && rhs.date_only {
                let left_days = days_from_civil(
                    lhs.datetime.date.year,
                    lhs.datetime.date.month,
                    lhs.datetime.date.day,
                );
                let right_days = days_from_civil(
                    rhs.datetime.date.year,
                    rhs.datetime.date.month,
                    rhs.datetime.date.day,
                );
                return Ok(ScalarValue::Int(left_days - right_days));
            }
            let left_epoch = datetime_to_epoch_seconds(lhs.datetime);
            let right_epoch = datetime_to_epoch_seconds(rhs.datetime);
            return Ok(ScalarValue::Int(left_epoch - right_epoch));
        }
        let days = parse_i64_scalar(&right, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(lhs, -days));
    }

    if matches!(left, ScalarValue::Text(_))
        && parse_json_document_arg(&left, "json operator -", 1).is_ok()
    {
        return eval_json_delete_operator(left, right);
    }

    Err(EngineError {
        message: "numeric operation expects numeric values".to_string(),
    })
}

fn eval_logical_or(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let lhs = parse_nullable_bool(&left, "argument of OR must be type boolean or null")?;
    let rhs = parse_nullable_bool(&right, "argument of OR must be type boolean or null")?;
    Ok(match (lhs, rhs) {
        (Some(true), _) | (_, Some(true)) => ScalarValue::Bool(true),
        (Some(false), Some(false)) => ScalarValue::Bool(false),
        _ => ScalarValue::Null,
    })
}

fn eval_logical_and(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let lhs = parse_nullable_bool(&left, "argument of AND must be type boolean or null")?;
    let rhs = parse_nullable_bool(&right, "argument of AND must be type boolean or null")?;
    Ok(match (lhs, rhs) {
        (Some(false), _) | (_, Some(false)) => ScalarValue::Bool(false),
        (Some(true), Some(true)) => ScalarValue::Bool(true),
        _ => ScalarValue::Null,
    })
}

fn eval_comparison(
    left: ScalarValue,
    right: ScalarValue,
    predicate: impl Fn(Ordering) -> bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    Ok(ScalarValue::Bool(predicate(compare_values_for_predicate(
        &left, &right,
    )?)))
}

fn compare_any_all_predicate(
    op: BinaryOp,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Option<bool>, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(None);
    }
    let ord = compare_values_for_predicate(left, right)?;
    let result = match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::NotEq => ord != Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::Lte => matches!(ord, Ordering::Less | Ordering::Equal),
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::Gte => matches!(ord, Ordering::Greater | Ordering::Equal),
        _ => {
            return Err(EngineError {
                message: "ANY/ALL expects comparison operator".to_string(),
            })
        }
    };
    Ok(Some(result))
}

pub(crate) fn eval_any_all(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
    quantifier: ComparisonQuantifier,
) -> Result<ScalarValue, EngineError> {
    if matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let items = match right {
        ScalarValue::Array(values) => values,
        _ => {
            return Err(EngineError {
                message: "ANY/ALL expects array argument".to_string(),
            })
        }
    };
    if items.is_empty() {
        return Ok(ScalarValue::Bool(matches!(
            quantifier,
            ComparisonQuantifier::All
        )));
    }
    let mut saw_null = false;
    for item in items {
        match compare_any_all_predicate(op.clone(), &left, &item)? {
            Some(true) => {
                if matches!(quantifier, ComparisonQuantifier::Any) {
                    return Ok(ScalarValue::Bool(true));
                }
            }
            Some(false) => {
                if matches!(quantifier, ComparisonQuantifier::All) {
                    return Ok(ScalarValue::Bool(false));
                }
            }
            None => {
                saw_null = true;
            }
        }
    }
    match quantifier {
        ComparisonQuantifier::Any => {
            if saw_null {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(false))
            }
        }
        ComparisonQuantifier::All => {
            if saw_null {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(true))
            }
        }
    }
}

fn numeric_bin(
    left: ScalarValue,
    right: ScalarValue,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;
    match (left_num, right_num) {
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(int_op(a, b))),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a as f64, b)))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(float_op(a, b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a, b)))
        }
    }
}

fn numeric_div(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;
    match (left_num, right_num) {
        (NumericOperand::Int(_), NumericOperand::Int(0))
        | (NumericOperand::Int(_), NumericOperand::Float(0.0))
        | (NumericOperand::Float(_), NumericOperand::Int(0))
        | (NumericOperand::Float(_), NumericOperand::Float(0.0)) => Err(EngineError {
            message: "division by zero".to_string(),
        }),
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(a / b)),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float((a as f64) / b))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(a / (b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => Ok(ScalarValue::Float(a / b)),
    }
}

pub(crate) fn numeric_mod(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_int = parse_i64_scalar(&left, "operator % expects integer values")?;
    let right_int = parse_i64_scalar(&right, "operator % expects integer values")?;
    match (left_int, right_int) {
        (_, 0) => Err(EngineError {
            message: "division by zero".to_string(),
        }),
        (a, b) => Ok(ScalarValue::Int(a % b)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn eval_function(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    within_group: &[OrderByExpr],
    filter: Option<&Expr>,
    over: Option<&crate::parser::ast::WindowSpec>,
    scope: &EvalScope,
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let fn_name = name
        .last()
        .map(|n| n.to_ascii_lowercase())
        .unwrap_or_default();
    if distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some() {
        return Err(EngineError {
            message: format!(
                "{}() aggregate modifiers require grouped aggregate evaluation",
                fn_name
            ),
        });
    }
    if over.is_some() {
        return Err(EngineError {
            message: "window functions require SELECT window evaluation context".to_string(),
        });
    }
    if is_aggregate_function(&fn_name) {
        return Err(EngineError {
            message: format!(
                "aggregate function {}() must be used with grouped evaluation",
                fn_name
            ),
        });
    }

    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_expr(arg, scope, params).await?);
    }

    // Handle schema-qualified extension functions (ws.connect, ws.send, ws.close)
    if name.len() == 2 {
        let schema = name[0].to_ascii_lowercase();
        if schema == "ws" {
            match fn_name.as_str() {
                "connect" => return execute_ws_connect(&values).await,
                "send" => return execute_ws_send(&values).await,
                "close" => return execute_ws_close(&values).await,
                "recv" => return execute_ws_recv(&values).await,
                _ => {
                    return Err(EngineError {
                        message: format!("function ws.{}() does not exist", fn_name),
                    });
                }
            }
        }
    }

    eval_scalar_function(&fn_name, &values).await
}

pub(crate) async fn eval_scalar_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "http_get" if args.len() == 1 => eval_http_get_builtin(&args[0]).await,
        "row" => Ok(ScalarValue::Text(
            JsonValue::Array(
                args.iter()
                    .map(scalar_to_json_value)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .to_string(),
        )),
        "to_json" | "to_jsonb" if args.len() == 1 => Ok(ScalarValue::Text(
            scalar_to_json_value(&args[0])?.to_string(),
        )),
        "row_to_json" if args.len() == 1 || args.len() == 2 => eval_row_to_json(args, fn_name),
        "array_to_json" if args.len() == 1 || args.len() == 2 => eval_array_to_json(args, fn_name),
        "json_object" if args.len() == 1 || args.len() == 2 => eval_json_object(args, fn_name),
        "json_build_object" | "jsonb_build_object" if args.len().is_multiple_of(2) => Ok(ScalarValue::Text(
            json_build_object_value(args)?.to_string(),
        )),
        "json_build_array" | "jsonb_build_array" => {
            Ok(ScalarValue::Text(json_build_array_value(args)?.to_string()))
        }
        "json_extract_path" | "jsonb_extract_path" if args.len() >= 2 => {
            eval_json_extract_path(args, false, fn_name)
        }
        "json_extract_path_text" | "jsonb_extract_path_text" if args.len() >= 2 => {
            eval_json_extract_path(args, true, fn_name)
        }
        "json_array_length" | "jsonb_array_length" if args.len() == 1 => {
            eval_json_array_length(&args[0], fn_name)
        }
        "json_typeof" | "jsonb_typeof" if args.len() == 1 => eval_json_typeof(&args[0], fn_name),
        "json_strip_nulls" | "jsonb_strip_nulls" if args.len() == 1 => {
            eval_json_strip_nulls(&args[0], fn_name)
        }
        "json_pretty" | "jsonb_pretty" if args.len() == 1 => eval_json_pretty(&args[0], fn_name),
        "jsonb_exists" if args.len() == 2 => eval_jsonb_exists(&args[0], &args[1]),
        "jsonb_exists_any" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], true, fn_name)
        }
        "jsonb_exists_all" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], false, fn_name)
        }
        "jsonb_path_exists" if args.len() >= 2 => eval_jsonb_path_exists(args, fn_name),
        "jsonb_path_match" if args.len() >= 2 => eval_jsonb_path_match(args, fn_name),
        "jsonb_path_query" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_path_query_array" if args.len() >= 2 => eval_jsonb_path_query_array(args, fn_name),
        "jsonb_path_query_first" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_set" if args.len() == 3 || args.len() == 4 => eval_jsonb_set(args),
        "jsonb_insert" if args.len() == 3 || args.len() == 4 => eval_jsonb_insert(args),
        "jsonb_set_lax" if args.len() >= 3 && args.len() <= 5 => eval_jsonb_set_lax(args),
        "nextval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "nextval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                let value = sequence_next_value(state, &sequence_name)?;
                Ok(ScalarValue::Int(value))
            })
        }
        "currval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "currval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_read(|sequences| {
                let Some(state) = sequences.get(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                if !state.called {
                    return Err(EngineError {
                        message: format!(
                            "currval of sequence \"{}\" is not yet defined",
                            sequence_name
                        ),
                    });
                }
                Ok(ScalarValue::Int(state.current))
            })
        }
        "setval" if args.len() == 2 || args.len() == 3 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "setval() expects text sequence name".to_string(),
                    });
                }
            };
            let value = parse_i64_scalar(&args[1], "setval() expects integer value")?;
            let is_called = if args.len() == 3 {
                parse_bool_scalar(&args[2], "setval() expects boolean third argument")?
            } else {
                true
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                set_sequence_value(state, &sequence_name, value, is_called)?;
                Ok(ScalarValue::Int(value))
            })
        }
        "lower" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_lowercase()))
        }
        "upper" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_uppercase()))
        }
        "length" | "char_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().chars().count() as i64))
        }
        "abs" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.abs())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.abs())),
            _ => Err(EngineError {
                message: "abs() expects numeric argument".to_string(),
            }),
        },
        "nullif" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            if matches!(args[1], ScalarValue::Null) {
                return Ok(args[0].clone());
            }
            if compare_values_for_predicate(&args[0], &args[1])? == Ordering::Equal {
                Ok(ScalarValue::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "greatest" if !args.is_empty() => eval_extremum(args, true),
        "least" if !args.is_empty() => eval_extremum(args, false),
        "concat" => {
            let mut out = String::new();
            for arg in args {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                out.push_str(&arg.render());
            }
            Ok(ScalarValue::Text(out))
        }
        "concat_ws" if !args.is_empty() => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let separator = args[0].render();
            let mut parts = Vec::new();
            for arg in &args[1..] {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                parts.push(arg.render());
            }
            Ok(ScalarValue::Text(parts.join(&separator)))
        }
        "substring" | "substr" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let start = parse_i64_scalar(&args[1], "substring() expects integer start index")?;
            let length = if args.len() == 3 {
                Some(parse_i64_scalar(
                    &args[2],
                    "substring() expects integer length",
                )?)
            } else {
                None
            };
            Ok(ScalarValue::Text(substring_chars(&input, start, length)?))
        }
        "position" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let needle = args[0].render();
            let haystack = args[1].render();
            Ok(ScalarValue::Int(find_substring_position(&haystack, &needle)))
        }
        "overlay" if args.len() == 3 || args.len() == 4 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let replacement = args[1].render();
            let start = parse_i64_scalar(&args[2], "overlay() expects integer start")?;
            let count = if args.len() == 4 {
                Some(parse_i64_scalar(&args[3], "overlay() expects integer count")?)
            } else {
                None
            };
            Ok(ScalarValue::Text(overlay_text(
                &input,
                &replacement,
                start,
                count,
            )?))
        }
        "left" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "left() expects integer length")?;
            Ok(ScalarValue::Text(left_chars(&input, count)))
        }
        "right" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "right() expects integer length")?;
            Ok(ScalarValue::Text(right_chars(&input, count)))
        }
        "btrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Both,
            )))
        }
        "ltrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Left,
            )))
        }
        "rtrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Right,
            )))
        }
        "replace" if args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let from = args[1].render();
            let to = args[2].render();
            Ok(ScalarValue::Text(input.replace(&from, &to)))
        }
        "ascii" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(ascii_code(&args[0].render())))
        }
        "chr" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let code = parse_i64_scalar(&args[0], "chr() expects integer")?;
            Ok(ScalarValue::Text(chr_from_code(code)?))
        }
        "encode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let data = args[0].render();
            let format = args[1].render();
            Ok(ScalarValue::Text(encode_bytes(data.as_bytes(), &format)?))
        }
        "decode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let format = args[1].render();
            let decoded = decode_bytes(&input, &format)?;
            Ok(ScalarValue::Text(String::from_utf8_lossy(&decoded).to_string()))
        }
        "date" if args.len() == 1 => eval_date_function(&args[0]),
        "timestamp" if args.len() == 1 => eval_timestamp_function(&args[0]),
        "now" | "current_timestamp" if args.is_empty() => {
            Ok(ScalarValue::Text(current_timestamp_string()?))
        }
        "clock_timestamp" if args.is_empty() => Ok(ScalarValue::Text(current_timestamp_string()?)),
        "current_date" if args.is_empty() => Ok(ScalarValue::Text(current_date_string()?)),
        "age" if args.len() == 1 || args.len() == 2 => eval_age(args),
        "extract" | "date_part" if args.len() == 2 => eval_extract_or_date_part(&args[0], &args[1]),
        "date_trunc" if args.len() == 2 => eval_date_trunc(&args[0], &args[1]),
        "date_add" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], true),
        "date_sub" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], false),
        "to_timestamp" if args.len() == 1 => eval_to_timestamp(&args[0]),
        "to_timestamp" if args.len() == 2 => eval_to_timestamp_with_format(&args[0], &args[1]),
        "to_date" if args.len() == 2 => eval_to_date_with_format(&args[0], &args[1]),
        "make_interval" if args.len() == 7 => eval_make_interval(args),
        "justify_hours" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Hours),
        "justify_days" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Days),
        "justify_interval" if args.len() == 1 => {
            eval_justify_interval(&args[0], JustifyMode::Full)
        }
        "isfinite" if args.len() == 1 => eval_isfinite(&args[0]),
        "coalesce" if !args.is_empty() => {
            for value in args {
                if !matches!(value, ScalarValue::Null) {
                    return Ok(value.clone());
                }
            }
            Ok(ScalarValue::Null)
        }
        // --- Math functions ---
        "ceil" | "ceiling" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.ceil())),
            _ => Err(EngineError { message: "ceil() expects numeric argument".to_string() }),
        },
        "floor" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.floor())),
            _ => Err(EngineError { message: "floor() expects numeric argument".to_string() }),
        },
        "round" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "round() expects integer scale")?
            } else { 0 };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).round() / factor))
                }
                _ => Err(EngineError { message: "round() expects numeric argument".to_string() }),
            }
        },
        "trunc" | "truncate" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "trunc() expects integer scale")?
            } else { 0 };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).trunc() / factor))
                }
                _ => Err(EngineError { message: "trunc() expects numeric argument".to_string() }),
            }
        },
        "power" | "pow" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let base = coerce_to_f64(&args[0], "power()")?;
            let exp = coerce_to_f64(&args[1], "power()")?;
            Ok(ScalarValue::Float(base.powf(exp)))
        },
        "sqrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "sqrt()")?;
            Ok(ScalarValue::Float(v.sqrt()))
        },
        "cbrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "cbrt()")?;
            Ok(ScalarValue::Float(v.cbrt()))
        },
        "exp" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "exp()")?;
            Ok(ScalarValue::Float(v.exp()))
        },
        "ln" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "ln()")?;
            Ok(ScalarValue::Float(v.ln()))
        },
        "log" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "log()")?;
            Ok(ScalarValue::Float(v.log10()))
        },
        "log" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let base = coerce_to_f64(&args[0], "log()")?;
            let v = coerce_to_f64(&args[1], "log()")?;
            Ok(ScalarValue::Float(v.log(base)))
        },
        "sin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "sin()")?.sin()))
        },
        "cos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "cos()")?.cos()))
        },
        "tan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "tan()")?.tan()))
        },
        "asin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "asin()")?.asin()))
        },
        "acos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "acos()")?.acos()))
        },
        "atan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "atan()")?.atan()))
        },
        "atan2" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = coerce_to_f64(&args[0], "atan2()")?;
            let x = coerce_to_f64(&args[1], "atan2()")?;
            Ok(ScalarValue::Float(y.atan2(x)))
        },
        "degrees" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "degrees()")?.to_degrees()))
        },
        "radians" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "radians()")?.to_radians()))
        },
        "sign" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.signum())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(if *f > 0.0 { 1.0 } else if *f < 0.0 { -1.0 } else { 0.0 })),
            _ => Err(EngineError { message: "sign() expects numeric argument".to_string() }),
        },
        "width_bucket" if args.len() == 4 => eval_width_bucket(args),
        "scale" if args.len() == 1 => eval_scale(&args[0]),
        "factorial" if args.len() == 1 => eval_factorial(&args[0]),
        "pi" if args.is_empty() => Ok(ScalarValue::Float(std::f64::consts::PI)),
        "random" if args.is_empty() => Ok(ScalarValue::Float(rand_f64())),
        "mod" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_binary(BinaryOp::Mod, args[0].clone(), args[1].clone())
        },
        "div" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = coerce_to_f64(&args[0], "div()")?;
            let b = coerce_to_f64(&args[1], "div()")?;
            if b == 0.0 { return Err(EngineError { message: "division by zero".to_string() }); }
            Ok(ScalarValue::Int((a / b).trunc() as i64))
        },
        "gcd" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = parse_i64_scalar(&args[0], "gcd() expects integer")?;
            let b = parse_i64_scalar(&args[1], "gcd() expects integer")?;
            Ok(ScalarValue::Int(gcd_i64(a, b)))
        },
        "lcm" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let a = parse_i64_scalar(&args[0], "lcm() expects integer")?;
            let b = parse_i64_scalar(&args[1], "lcm() expects integer")?;
            let g = gcd_i64(a, b);
            Ok(ScalarValue::Int(if g == 0 { 0 } else { (a / g * b).abs() }))
        },
        // --- Additional string functions ---
        "initcap" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(initcap_string(&args[0].render())))
        },
        "repeat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let s = args[0].render();
            let n = parse_i64_scalar(&args[1], "repeat() expects integer count")?;
            if n < 0 { return Ok(ScalarValue::Text(String::new())); }
            Ok(ScalarValue::Text(s.repeat(n as usize)))
        },
        "reverse" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(args[0].render().chars().rev().collect()))
        },
        "translate" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let from: Vec<char> = args[1].render().chars().collect();
            let to: Vec<char> = args[2].render().chars().collect();
            let result: String = input.chars().filter_map(|c| {
                if let Some(pos) = from.iter().position(|f| *f == c) {
                    to.get(pos).copied()
                } else {
                    Some(c)
                }
            }).collect();
            Ok(ScalarValue::Text(result))
        },
        "split_part" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let delimiter = args[1].render();
            let field = parse_i64_scalar(&args[2], "split_part() expects integer field")?;
            if field <= 0 { return Err(EngineError { message: "field position must be greater than zero".to_string() }); }
            let parts: Vec<&str> = input.split(&delimiter).collect();
            Ok(ScalarValue::Text(parts.get((field - 1) as usize).unwrap_or(&"").to_string()))
        },
        "strpos" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let haystack = args[0].render();
            let needle = args[1].render();
            Ok(ScalarValue::Int(haystack.find(&needle).map(|i| i as i64 + 1).unwrap_or(0)))
        },
        "lpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "lpad() expects integer length")? as usize;
            let fill = if args.len() == 3 { args[2].render() } else { " ".to_string() };
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, true)))
        },
        "rpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "rpad() expects integer length")? as usize;
            let fill = if args.len() == 3 { args[2].render() } else { " ".to_string() };
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, false)))
        },
        "quote_literal" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(quote_literal(&args[0].render())))
        },
        "quote_ident" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(quote_ident(&args[0].render())))
        },
        "quote_nullable" if args.len() == 1 => Ok(ScalarValue::Text(quote_nullable(&args[0]))),
        "md5" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Text(md5_hex(&args[0].render())))
        },
        "regexp_match" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_match(&args[0].render(), &args[1].render(), "")
        },
        "regexp_match" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_match(&args[0].render(), &args[1].render(), &args[2].render())
        },
        "regexp_replace" if args.len() == 3 || args.len() == 4 => {
            if args.iter().take(3).any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let source = args[0].render();
            let pattern = args[1].render();
            let replacement = args[2].render();
            let flags = if args.len() == 4 { args[3].render() } else { String::new() };
            eval_regexp_replace(&source, &pattern, &replacement, &flags)
        },
        "regexp_split_to_array" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            eval_regexp_split_to_array(&args[0].render(), &args[1].render())
        },
        "num_nulls" => Ok(ScalarValue::Int(count_nulls(args) as i64)),
        "num_nonnulls" => Ok(ScalarValue::Int(count_nonnulls(args) as i64)),
        // --- System info functions ---
        "version" if args.is_empty() => {
            Ok(ScalarValue::Text("Postrust 0.1.0 on Rust".to_string()))
        },
        "current_database" if args.is_empty() => {
            Ok(ScalarValue::Text("postrust".to_string()))
        },
        "current_schema" if args.is_empty() => {
            Ok(ScalarValue::Text("public".to_string()))
        },
        "current_user" | "session_user" | "user" if args.is_empty() => {
            let role = security::current_role();
            Ok(ScalarValue::Text(role))
        },
        "pg_backend_pid" if args.is_empty() => {
            Ok(ScalarValue::Int(std::process::id() as i64))
        },
        "pg_get_userbyid" if args.len() == 1 => {
            Ok(ScalarValue::Text("postrust".to_string()))
        },
        "has_table_privilege" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Bool(true))
        },
        "has_column_privilege" if args.len() == 3 || args.len() == 4 => {
            Ok(ScalarValue::Bool(true))
        },
        "has_schema_privilege" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Bool(true))
        },
        "pg_get_expr" if args.len() == 2 || args.len() == 3 => {
            Ok(ScalarValue::Null)
        },
        "pg_table_is_visible" if args.len() == 1 => {
            Ok(ScalarValue::Bool(true))
        },
        "pg_type_is_visible" if args.len() == 1 => {
            Ok(ScalarValue::Bool(true))
        },
        "obj_description" | "col_description" | "shobj_description" if !args.is_empty() => {
            Ok(ScalarValue::Null)
        },
        "format_type" if args.len() == 2 => {
            Ok(ScalarValue::Text("unknown".to_string()))
        },
        "pg_catalog.format_type" if args.len() == 2 => {
            Ok(ScalarValue::Text("unknown".to_string()))
        },
        // --- Make date/time ---
        "make_date" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = parse_i64_scalar(&args[0], "make_date() year")? as i32;
            let m = parse_i64_scalar(&args[1], "make_date() month")? as u32;
            let d = parse_i64_scalar(&args[2], "make_date() day")? as u32;
            Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02}", y, m, d)))
        },
        "make_timestamp" if args.len() == 6 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let y = parse_i64_scalar(&args[0], "year")? as i32;
            let mo = parse_i64_scalar(&args[1], "month")? as u32;
            let d = parse_i64_scalar(&args[2], "day")? as u32;
            let h = parse_i64_scalar(&args[3], "hour")? as u32;
            let mi = parse_i64_scalar(&args[4], "min")? as u32;
            let s = coerce_to_f64(&args[5], "sec")?;
            let sec = s.trunc() as u32;
            let frac = ((s - s.trunc()) * 1_000_000.0).round() as u32;
            if frac == 0 {
                Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, mo, d, h, mi, sec)))
            } else {
                Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", y, mo, d, h, mi, sec, frac)))
            }
        },
        "to_char" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            // Simplified: just return the first arg rendered
            Ok(ScalarValue::Text(args[0].render()))
        },
        "to_number" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let s = args[0].render();
            let cleaned: String = s.chars().filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-').collect();
            match cleaned.parse::<f64>() {
                Ok(v) => Ok(ScalarValue::Float(v)),
                Err(_) => Err(EngineError { message: format!("invalid input for to_number: {}", s) }),
            }
        },
        "array_append" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let mut values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_append() expects array as first argument".to_string(),
                    })
                }
            };
            values.push(args[1].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_prepend" if args.len() == 2 => {
            if matches!(args[1], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let mut values = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_prepend() expects array as second argument".to_string(),
                    })
                }
            };
            values.insert(0, args[0].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_cat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let mut left = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    })
                }
            };
            let right = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    })
                }
            };
            left.extend(right);
            Ok(ScalarValue::Array(left))
        }
        "array_remove" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_remove() expects array as first argument".to_string(),
                    })
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if !array_value_matches(&args[1], value)? {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_replace" if args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_replace() expects array as first argument".to_string(),
                    })
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if array_value_matches(&args[1], value)? {
                    out.push(args[2].clone());
                } else {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_position" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_position() expects array as first argument".to_string(),
                    })
                }
            };
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    return Ok(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Null)
        }
        "array_positions" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_positions() expects array as first argument".to_string(),
                    })
                }
            };
            let mut positions = Vec::new();
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    positions.push(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Array(positions))
        }
        "array_length" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_length() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_length() expects integer dimension")?;
            if dim != 1 {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_dims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_dims() expects array argument".to_string(),
                    })
                }
            };
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(format!("[1:{}]", values.len())))
        }
        "array_ndims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            match &args[0] {
                ScalarValue::Array(_) => Ok(ScalarValue::Int(1)),
                _ => Err(EngineError {
                    message: "array_ndims() expects array argument".to_string(),
                }),
            }
        }
        "array_fill" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let lengths = match &args[1] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_fill() expects array of lengths".to_string(),
                    })
                }
            };
            if lengths.len() != 1 {
                return Err(EngineError {
                    message: "array_fill() currently supports one-dimensional arrays".to_string(),
                });
            }
            let length = parse_i64_scalar(&lengths[0], "array_fill() expects integer length")?;
            if length < 0 {
                return Err(EngineError {
                    message: "array_fill() length must be non-negative".to_string(),
                });
            }
            let mut out = Vec::with_capacity(length as usize);
            for _ in 0..length {
                out.push(args[0].clone());
            }
            Ok(ScalarValue::Array(out))
        }
        "array_upper" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_upper() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_upper() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_lower" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_lower() expects array as first argument".to_string(),
                    })
                }
            };
            let dim = parse_i64_scalar(&args[1], "array_lower() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(1))
        }
        "cardinality" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            match &args[0] {
                ScalarValue::Array(values) => Ok(ScalarValue::Int(values.len() as i64)),
                _ => Err(EngineError {
                    message: "cardinality() expects array argument".to_string(),
                }),
            }
        }
        "string_to_array" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let input = args[0].render();
            let delimiter = if matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Array(vec![ScalarValue::Text(input)]));
            } else { args[1].render() };
            let null_str = args.get(2).and_then(|a| if matches!(a, ScalarValue::Null) { None } else { Some(a.render()) });
            let parts = if delimiter.is_empty() {
                input.chars().map(|c| c.to_string()).collect::<Vec<_>>()
            } else {
                input.split(&delimiter).map(|p| p.to_string()).collect::<Vec<_>>()
            };
            let values = parts.into_iter().map(|part| {
                if null_str.as_deref() == Some(part.as_str()) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Text(part)
                }
            }).collect();
            Ok(ScalarValue::Array(values))
        },
        "array_to_string" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let delimiter = args[1].render();
            let null_replacement = args.get(2).map(|a| a.render());
            let values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                ScalarValue::Text(text) => {
                    let inner = text.trim_start_matches('{').trim_end_matches('}');
                    if inner.is_empty() {
                        return Ok(ScalarValue::Text(String::new()));
                    }
                    inner.split(',').map(|part| {
                        let trimmed = part.trim();
                        if trimmed == "NULL" {
                            ScalarValue::Null
                        } else {
                            ScalarValue::Text(trimmed.to_string())
                        }
                    }).collect::<Vec<_>>()
                }
                _ => {
                    return Err(EngineError {
                        message: "array_to_string() expects array argument".to_string(),
                    })
                }
            };
            let result: Vec<String> = values.iter().filter_map(|value| {
                match value {
                    ScalarValue::Null => null_replacement.clone(),
                    _ => Some(value.render()),
                }
            }).collect();
            Ok(ScalarValue::Text(result.join(&delimiter)))
        },
        _ => Err(EngineError {
            message: format!("unsupported function call {}", fn_name),
        }),
    }
}

fn array_value_matches(target: &ScalarValue, candidate: &ScalarValue) -> Result<bool, EngineError> {
    match (target, candidate) {
        (ScalarValue::Null, ScalarValue::Null) => Ok(true),
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => Ok(false),
        _ => Ok(compare_values_for_predicate(target, candidate)? == Ordering::Equal),
    }
}

pub(crate) fn coerce_to_f64(v: &ScalarValue, context: &str) -> Result<f64, EngineError> {
    match v {
        ScalarValue::Int(i) => Ok(*i as f64),
        ScalarValue::Float(f) => Ok(*f),
        _ => Err(EngineError { message: format!("{} expects numeric argument", context) }),
    }
}

pub(crate) fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

pub(crate) fn initcap_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if capitalize_next {
                result.extend(c.to_uppercase());
                capitalize_next = false;
            } else {
                result.extend(c.to_lowercase());
            }
        } else {
            result.push(c);
            capitalize_next = true;
        }
    }
    result
}

fn pad_string(input: &str, len: usize, fill: &str, left: bool) -> String {
    let input_len = input.chars().count();
    if input_len >= len {
        return input.chars().take(len).collect();
    }
    let pad_len = len - input_len;
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return input.to_string();
    }
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    if left {
        format!("{}{}", padding, input)
    } else {
        format!("{}{}", input, padding)
    }
}

fn md5_hex(input: &str) -> String {
    let digest = md5_digest(input.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn md5_digest(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9,
        14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
        4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    let mut msg = input.to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            let start = i * 4;
            *word = u32::from_le_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }

        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            let rotate = a
                .wrapping_add(f)
                .wrapping_add(K[i])
                .wrapping_add(m[g]);
            b = b.wrapping_add(rotate.rotate_left(S[i]));
            a = temp;
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

pub(crate) fn encode_bytes(input: &[u8], format: &str) -> Result<String, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => Ok(input.iter().map(|b| format!("{:02x}", b)).collect()),
        "base64" => {
            use base64::Engine;
            Ok(base64::engine::general_purpose::STANDARD.encode(input))
        }
        "escape" => Ok(escape_bytes(input)),
        _ => Err(EngineError {
            message: format!("encode() unsupported format {}", format),
        }),
    }
}

pub(crate) fn decode_bytes(input: &str, format: &str) -> Result<Vec<u8>, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => decode_hex_bytes(input),
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(input.trim())
                .map_err(|_| EngineError {
                    message: "decode() invalid base64 input".to_string(),
                })
        }
        "escape" => decode_escape_bytes(input),
        _ => Err(EngineError {
            message: format!("decode() unsupported format {}", format),
        }),
    }
}

fn escape_bytes(input: &[u8]) -> String {
    let mut out = String::new();
    for &byte in input {
        if byte == b'\\' {
            out.push_str("\\\\");
        } else if (0x20..=0x7e).contains(&byte) {
            out.push(byte as char);
        } else {
            out.push('\\');
            out.push_str(&format!("{:03o}", byte));
        }
    }
    out
}

fn decode_hex_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let trimmed = input.trim();
    let hex = trimmed.strip_prefix("\\x").or_else(|| trimmed.strip_prefix("\\X")).unwrap_or(trimmed);
    if !hex.len().is_multiple_of(2) {
        return Err(EngineError {
            message: "decode() invalid hex input length".to_string(),
        });
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for idx in (0..bytes.len()).step_by(2) {
        let part = std::str::from_utf8(&bytes[idx..idx + 2]).unwrap_or("");
        let value = u8::from_str_radix(part, 16).map_err(|_| EngineError {
            message: "decode() invalid hex input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

fn decode_escape_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch as u8);
            continue;
        }
        let Some(next) = chars.peek().copied() else {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        };
        if next == '\\' {
            chars.next();
            out.push(b'\\');
            continue;
        }
        let mut octal = String::new();
        for _ in 0..3 {
            if let Some(digit) = chars.peek().copied()
                && digit.is_ascii_digit() && digit <= '7'
            {
                octal.push(digit);
                chars.next();
            }
        }
        if octal.is_empty() {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        }
        let value = u8::from_str_radix(&octal, 8).map_err(|_| EngineError {
            message: "decode() invalid escape input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

fn build_regex(pattern: &str, flags: &str, fn_name: &str) -> Result<regex::Regex, EngineError> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'g' => {}
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() unsupported regex flag {}", flag),
                });
            }
        }
    }
    builder.build().map_err(|err| EngineError {
        message: format!("{fn_name}() invalid regex: {err}"),
    })
}

fn text_array_from_options(items: &[Option<String>]) -> String {
    let rendered = items
        .iter()
        .map(|item| item.clone().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>();
    format!("{{{}}}", rendered.join(","))
}

fn eval_regexp_match(text: &str, pattern: &str, flags: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, flags, "regexp_match")?;
    let Some(caps) = regex.captures(text) else {
        return Ok(ScalarValue::Null);
    };
    Ok(ScalarValue::Text(regex_captures_to_array(&caps)))
}

fn regex_captures_to_array(caps: &regex::Captures<'_>) -> String {
    let mut items = Vec::new();
    if caps.len() > 1 {
        for idx in 1..caps.len() {
            items.push(caps.get(idx).map(|m| m.as_str().to_string()));
        }
    } else {
        items.push(caps.get(0).map(|m| m.as_str().to_string()));
    }
    text_array_from_options(&items)
}

pub(crate) fn eval_regexp_matches_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 && args.len() != 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().take(2).any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() == 3 { args[2].render() } else { String::new() };
    let global = flags.contains('g');
    let regex = build_regex(&pattern, &flags, fn_name)?;
    let mut rows = Vec::new();
    if global {
        for caps in regex.captures_iter(&text) {
            rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
        }
    } else if let Some(caps) = regex.captures(&text) {
        rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
    }
    Ok((vec![fn_name.to_string()], rows))
}

fn eval_regexp_split_to_array(text: &str, pattern: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, "", "regexp_split_to_array")?;
    let parts = regex
        .split(text)
        .map(|part| Some(part.to_string()))
        .collect::<Vec<_>>();
    Ok(ScalarValue::Text(text_array_from_options(&parts)))
}

pub(crate) fn eval_regexp_split_to_table_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 arguments"),
        });
    }
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let regex = build_regex(&pattern, "", fn_name)?;
    let rows = regex
        .split(&text)
        .map(|part| vec![ScalarValue::Text(part.to_string())])
        .collect();
    Ok((vec![fn_name.to_string()], rows))
}

fn rand_f64() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (seed as f64) / (u32::MAX as f64)
}

fn eval_regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
    let global = flags.contains('g');
    let regex = build_regex(pattern, flags, "regexp_replace")?;
    let out = if global {
        regex.replace_all(source, replacement).to_string()
    } else {
        regex.replace(source, replacement).to_string()
    };
    Ok(ScalarValue::Text(out))
}

fn eval_extremum(args: &[ScalarValue], greatest: bool) -> Result<ScalarValue, EngineError> {
    let mut best: Option<ScalarValue> = None;
    for arg in args {
        if matches!(arg, ScalarValue::Null) {
            continue;
        }
        match &best {
            None => best = Some(arg.clone()),
            Some(current) => {
                let cmp = compare_values_for_predicate(arg, current)?;
                let should_take = if greatest {
                    cmp == Ordering::Greater
                } else {
                    cmp == Ordering::Less
                };
                if should_take {
                    best = Some(arg.clone());
                }
            }
        }
    }
    Ok(best.unwrap_or(ScalarValue::Null))
}

pub(crate) fn scalar_to_json_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Null => Ok(JsonValue::Null),
        ScalarValue::Bool(v) => Ok(JsonValue::Bool(*v)),
        ScalarValue::Int(v) => Ok(JsonValue::Number(JsonNumber::from(*v))),
        ScalarValue::Float(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .ok_or_else(|| EngineError {
                message: "cannot convert non-finite float to JSON value".to_string(),
            }),
        ScalarValue::Text(v) => Ok(JsonValue::String(v.clone())),
        ScalarValue::Array(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(scalar_to_json_value(value)?);
            }
            Ok(JsonValue::Array(items))
        }
    }
}

pub(crate) fn json_build_object_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut object = JsonMap::new();
    for idx in (0..args.len()).step_by(2) {
        let key = match &args[idx] {
            ScalarValue::Null => {
                return Err(EngineError {
                    message: "json_build_object() key cannot be null".to_string(),
                });
            }
            other => other.render(),
        };
        let value = scalar_to_json_value(&args[idx + 1])?;
        object.insert(key, value);
    }
    Ok(JsonValue::Object(object))
}

pub(crate) fn json_build_array_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut items = Vec::with_capacity(args.len());
    for arg in args {
        items.push(scalar_to_json_value(arg)?);
    }
    Ok(JsonValue::Array(items))
}

fn parse_json_or_scalar_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => serde_json::from_str::<JsonValue>(text)
            .or_else(|_| scalar_to_json_value(value))
            .map_err(|err| EngineError {
                message: format!("cannot convert value to JSON: {err}"),
            }),
        _ => scalar_to_json_value(value),
    }
}

fn parse_json_constructor_pretty_arg(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<bool>, EngineError> {
    if args.len() < 2 {
        return Ok(Some(false));
    }
    if matches!(args[1], ScalarValue::Null) {
        return Ok(None);
    }
    parse_bool_scalar(
        &args[1],
        &format!("{fn_name}() expects boolean pretty argument"),
    )
    .map(Some)
}

fn maybe_pretty_json(value: &JsonValue, pretty: bool) -> Result<String, EngineError> {
    if pretty {
        serde_json::to_string_pretty(value).map_err(|err| EngineError {
            message: format!("failed to pretty-print JSON: {err}"),
        })
    } else {
        Ok(value.to_string())
    }
}

pub(crate) fn eval_row_to_json(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };

    let input = parse_json_or_scalar_value(&args[0])?;
    let object = match input {
        JsonValue::Object(map) => JsonValue::Object(map),
        JsonValue::Array(items) => {
            let mut map = JsonMap::new();
            for (idx, value) in items.into_iter().enumerate() {
                map.insert(format!("f{}", idx + 1), value);
            }
            JsonValue::Object(map)
        }
        scalar => {
            let mut map = JsonMap::new();
            map.insert("f1".to_string(), scalar);
            JsonValue::Object(map)
        }
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&object, pretty)?))
}

pub(crate) fn eval_array_to_json(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let parsed = parse_json_or_scalar_value(&args[0])?;
    let JsonValue::Array(_) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&parsed, pretty)?))
}

fn json_value_text_token(
    value: &JsonValue,
    fn_name: &str,
    key_mode: bool,
) -> Result<String, EngineError> {
    match value {
        JsonValue::Null if key_mode => Err(EngineError {
            message: format!("{fn_name}() key cannot be null"),
        }),
        JsonValue::Null => Ok("null".to_string()),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(v) => Ok(v.to_string()),
        JsonValue::Number(v) => Ok(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_object_pairs_from_array(
    value: &JsonValue,
    fn_name: &str,
) -> Result<Vec<(String, String)>, EngineError> {
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument must be a JSON array"),
        });
    };

    if items
        .iter()
        .all(|item| matches!(item, JsonValue::Array(inner) if inner.len() == 2))
    {
        let mut pairs = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Array(inner) = item else {
                continue;
            };
            let key = json_value_text_token(&inner[0], fn_name, true)?;
            let value = json_value_text_token(&inner[1], fn_name, false)?;
            pairs.push((key, value));
        }
        return Ok(pairs);
    }

    if items.len() % 2 != 0 {
        return Err(EngineError {
            message: format!("{fn_name}() requires an even-length text array"),
        });
    }

    let mut pairs = Vec::with_capacity(items.len() / 2);
    for idx in (0..items.len()).step_by(2) {
        let key = json_value_text_token(&items[idx], fn_name, true)?;
        let value = json_value_text_token(&items[idx + 1], fn_name, false)?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

pub(crate) fn eval_json_object(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let pairs = match args.len() {
        1 => {
            let source = parse_json_document_arg(&args[0], fn_name, 1)?;
            parse_json_object_pairs_from_array(&source, fn_name)?
        }
        2 => {
            let keys = parse_json_document_arg(&args[0], fn_name, 1)?;
            let values = parse_json_document_arg(&args[1], fn_name, 2)?;
            let JsonValue::Array(key_items) = keys else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 1 must be a JSON array"),
                });
            };
            let JsonValue::Array(value_items) = values else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 2 must be a JSON array"),
                });
            };
            if key_items.len() != value_items.len() {
                return Err(EngineError {
                    message: format!("{fn_name}() key/value array lengths must match"),
                });
            }
            key_items
                .iter()
                .zip(value_items.iter())
                .map(|(key, value)| {
                    Ok((
                        json_value_text_token(key, fn_name, true)?,
                        json_value_text_token(value, fn_name, false)?,
                    ))
                })
                .collect::<Result<Vec<_>, EngineError>>()?
        }
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}() expects one or two arguments"),
            });
        }
    };

    let mut object = JsonMap::new();
    for (key, value) in pairs {
        object.insert(key, JsonValue::String(value));
    }
    Ok(ScalarValue::Text(JsonValue::Object(object).to_string()))
}

pub(crate) fn parse_json_document_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSON text"),
        });
    };
    serde_json::from_str::<JsonValue>(text).map_err(|err| EngineError {
        message: format!("{fn_name}() argument {arg_index} is not valid JSON: {err}"),
    })
}

fn parse_json_path_segments(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<Vec<String>>, EngineError> {
    let mut out = Vec::with_capacity(args.len().saturating_sub(1));
    for path_arg in &args[1..] {
        match path_arg {
            ScalarValue::Null => return Ok(None),
            ScalarValue::Text(text) => out.push(text.clone()),
            ScalarValue::Int(v) => out.push(v.to_string()),
            ScalarValue::Float(v) => out.push(v.to_string()),
            ScalarValue::Bool(v) => out.push(v.to_string()),
            ScalarValue::Array(_) => {
                return Err(EngineError {
                    message: format!("{fn_name}() path arguments must be scalar values"),
                });
            }
        }
    }
    if out.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires at least one path argument"),
        });
    }
    Ok(Some(out))
}

fn extract_json_path_value<'a>(root: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = root;
    for segment in path {
        current = match current {
            JsonValue::Object(map) => map.get(segment)?,
            JsonValue::Array(array) => {
                let idx = segment.parse::<usize>().ok()?;
                array.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current)
}

pub(crate) fn json_value_text_output(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::String(text) => ScalarValue::Text(text.clone()),
        JsonValue::Bool(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Number(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

pub(crate) fn eval_json_extract_path(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&args[0], fn_name, 1)?;
    let Some(path) = parse_json_path_segments(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

pub(crate) fn eval_json_array_length(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let JsonValue::Array(items) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Int(items.len() as i64))
}

pub(crate) fn eval_json_typeof(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let ty = match parsed {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    };
    Ok(ScalarValue::Text(ty.to_string()))
}

fn strip_null_object_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if let Some(inner) = map.get_mut(&key) {
                    strip_null_object_fields(inner);
                }
                if map.get(&key).is_some_and(|candidate| candidate.is_null()) {
                    map.remove(&key);
                }
            }
        }
        JsonValue::Array(array) => {
            for item in array {
                strip_null_object_fields(item);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

pub(crate) fn eval_json_strip_nulls(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut parsed = parse_json_document_arg(value, fn_name, 1)?;
    strip_null_object_fields(&mut parsed);
    Ok(ScalarValue::Text(parsed.to_string()))
}

pub(crate) fn eval_json_pretty(value: &ScalarValue, fn_name: &str) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let pretty = serde_json::to_string_pretty(&parsed).map_err(|err| EngineError {
        message: format!("{fn_name}() failed to pretty-print JSON: {err}"),
    })?;
    Ok(ScalarValue::Text(pretty))
}

pub(crate) fn eval_jsonb_exists(target: &ScalarValue, key: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(key, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, "jsonb_exists", 1)?;
    let key = scalar_to_json_path_segment(key, "jsonb_exists")?;
    Ok(ScalarValue::Bool(json_has_key(&parsed, &key)))
}

pub(crate) fn eval_jsonb_exists_any_all(
    target: &ScalarValue,
    keys: &ScalarValue,
    any_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(keys, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, fn_name, 1)?;
    let keys = parse_json_path_operand(keys, fn_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() key array cannot be empty"),
        });
    }
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&parsed, key))
    } else {
        keys.iter().all(|key| json_has_key(&parsed, key))
    };
    Ok(ScalarValue::Bool(matched))
}

fn parse_json_path_text_array(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);
    if inner.trim().is_empty() {
        return Vec::new();
    }
    inner
        .split(',')
        .map(str::trim)
        .map(|segment| {
            segment
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .unwrap_or(segment)
                .to_string()
        })
        .collect()
}

fn parse_json_new_value_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => match serde_json::from_str::<JsonValue>(text) {
            Ok(parsed) => Ok(parsed),
            Err(_) => Ok(JsonValue::String(text.clone())),
        },
        _ => scalar_to_json_value(value).map_err(|err| EngineError {
            message: format!(
                "{fn_name}() argument {arg_index} is invalid: {}",
                err.message
            ),
        }),
    }
}

fn json_container_for_next_segment(next: Option<&str>) -> JsonValue {
    if next.is_some_and(|segment| segment.parse::<usize>().is_ok()) {
        JsonValue::Array(Vec::new())
    } else {
        JsonValue::Object(JsonMap::new())
    }
}

fn json_set_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    create_missing: bool,
) -> bool {
    if path.is_empty() {
        *root = new_value;
        return true;
    }

    let mut current = root;
    for idx in 0..path.len() {
        let segment = &path[idx];
        let is_last = idx + 1 == path.len();
        let next_segment = path.get(idx + 1).map(String::as_str);

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(segment) || create_missing {
                        map.insert(segment.clone(), new_value);
                        return true;
                    }
                    return false;
                }
                if !map.contains_key(segment) {
                    if !create_missing {
                        return false;
                    }
                    map.insert(
                        segment.clone(),
                        json_container_for_next_segment(next_segment),
                    );
                }
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                current = next;
            }
            JsonValue::Array(array) => {
                let Ok(index) = segment.parse::<usize>() else {
                    return false;
                };
                if is_last {
                    if index < array.len() {
                        array[index] = new_value;
                        return true;
                    }
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                    array[index] = new_value;
                    return true;
                }
                if index >= array.len() {
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                }
                if array[index].is_null() {
                    array[index] = json_container_for_next_segment(next_segment);
                }
                current = &mut array[index];
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        }
    }
    false
}

fn json_insert_array_index(len: usize, segment: &str, insert_after: bool) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    let mut pos = if index >= 0 {
        index as isize
    } else {
        len as isize + index as isize
    };
    if insert_after {
        pos += 1;
    }
    if pos < 0 {
        pos = 0;
    }
    if pos > len as isize {
        pos = len as isize;
    }
    Some(pos as usize)
}

fn json_insert_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    insert_after: bool,
) -> bool {
    if path.is_empty() {
        return false;
    }

    let mut current = root;
    for segment in &path[..path.len() - 1] {
        current = match current {
            JsonValue::Object(map) => {
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                next
            }
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), segment) else {
                    return false;
                };
                &mut array[idx]
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        };
    }

    let last = &path[path.len() - 1];
    match current {
        JsonValue::Object(map) => {
            if map.contains_key(last) {
                false
            } else {
                map.insert(last.clone(), new_value);
                true
            }
        }
        JsonValue::Array(array) => {
            let Some(pos) = json_insert_array_index(array.len(), last, insert_after) else {
                return false;
            };
            array.insert(pos, new_value);
            true
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_jsonb_set(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set", 3)?;
    let create_missing = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_set() expects boolean create_missing argument",
        )?
    } else {
        true
    };
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_jsonb_insert(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_insert", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_insert() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_insert", 3)?;
    let insert_after = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_insert() expects boolean insert_after argument",
        )?
    } else {
        false
    };
    let _ = json_insert_path(&mut target, &path, new_value, insert_after);
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_jsonb_set_lax(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set_lax", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set_lax() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let create_missing = if args.len() >= 4 {
        if matches!(args[3], ScalarValue::Null) {
            return Ok(ScalarValue::Null);
        }
        parse_bool_scalar(
            &args[3],
            "jsonb_set_lax() expects boolean create_if_missing argument",
        )?
    } else {
        true
    };

    if matches!(args[2], ScalarValue::Null) {
        let treatment = if args.len() >= 5 {
            if matches!(args[4], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            args[4].render().trim().to_ascii_lowercase()
        } else {
            "use_json_null".to_string()
        };
        match treatment.as_str() {
            "raise_exception" => {
                return Err(EngineError {
                    message: "jsonb_set_lax() null_value_treatment requested exception".to_string(),
                });
            }
            "use_json_null" => {
                let _ = json_set_path(&mut target, &path, JsonValue::Null, create_missing);
            }
            "delete_key" => {
                if !path.is_empty() {
                    let _ = json_remove_path(&mut target, &path);
                }
            }
            "return_target" => {}
            _ => {
                return Err(EngineError {
                    message: format!("jsonb_set_lax() unknown null_value_treatment {}", treatment),
                });
            }
        }
        return Ok(ScalarValue::Text(target.to_string()));
    }

    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set_lax", 3)?;
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

fn extract_json_get_value<'a>(target: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    match target {
        JsonValue::Object(map) => map.get(path),
        JsonValue::Array(array) => {
            let idx = path.parse::<i64>().ok()?;
            if idx >= 0 {
                array.get(idx as usize)
            } else {
                let back = idx.unsigned_abs() as usize;
                if back > array.len() {
                    None
                } else {
                    array.get(array.len().saturating_sub(back))
                }
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => None,
    }
}

fn scalar_to_json_path_segment(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<String, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator does not accept NULL path/key operand"),
        }),
        ScalarValue::Text(text) => Ok(text.clone()),
        ScalarValue::Int(v) => Ok(v.to_string()),
        ScalarValue::Float(v) => Ok(v.to_string()),
        ScalarValue::Bool(v) => Ok(v.to_string()),
        ScalarValue::Array(_) => Err(EngineError {
            message: format!("{operator_name} operator path operand must be scalar"),
        }),
    }
}

fn parse_json_path_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator path operand cannot be NULL"),
        }),
        ScalarValue::Text(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('[') {
                let parsed =
                    serde_json::from_str::<JsonValue>(trimmed).map_err(|err| EngineError {
                        message: format!(
                            "{operator_name} operator path operand must be text[]/json array: {err}"
                        ),
                    })?;
                let JsonValue::Array(items) = parsed else {
                    return Err(EngineError {
                        message: format!("{operator_name} operator path operand must be array"),
                    });
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::String(s) => out.push(s),
                        JsonValue::Number(n) => out.push(n.to_string()),
                        JsonValue::Bool(v) => out.push(v.to_string()),
                        JsonValue::Null => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array cannot contain null"
                                ),
                            });
                        }
                        JsonValue::Array(_) | JsonValue::Object(_) => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array entries must be scalar"
                                ),
                            });
                        }
                    }
                }
                return Ok(out);
            }
            Ok(parse_json_path_text_array(trimmed))
        }
        ScalarValue::Int(v) => Ok(vec![v.to_string()]),
        ScalarValue::Float(v) => Ok(vec![v.to_string()]),
        ScalarValue::Bool(v) => Ok(vec![v.to_string()]),
        ScalarValue::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if matches!(value, ScalarValue::Null) {
                    return Err(EngineError {
                        message: format!(
                            "{operator_name} operator path array cannot contain null"
                        ),
                    });
                }
                out.push(value.render());
            }
            Ok(out)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathStep {
    Key(String),
    Index(i64),
    Wildcard,
    Filter(JsonPathFilterExpr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonPathFilterOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterOperand {
    Current,
    CurrentPath(Vec<JsonPathStep>),
    RootPath(Vec<JsonPathStep>),
    Variable(String),
    Literal(JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterExpr {
    Exists(JsonPathFilterOperand),
    Compare {
        left: JsonPathFilterOperand,
        op: JsonPathFilterOp,
        right: JsonPathFilterOperand,
    },
    Truthy(JsonPathFilterOperand),
}

fn parse_jsonpath_text_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<String, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSONPath text"),
        });
    };
    Ok(text.clone())
}

fn parse_jsonpath_vars_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonMap<String, JsonValue>, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(JsonMap::new());
    }
    let parsed = parse_json_document_arg(value, fn_name, arg_index)?;
    let JsonValue::Object(vars) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be a JSON object"),
        });
    };
    Ok(vars)
}

fn parse_jsonpath_silent_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<bool, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(false);
    }
    parse_bool_scalar(
        value,
        &format!("{fn_name}() argument {arg_index} must be boolean"),
    )
}

fn strip_outer_parentheses(text: &str) -> &str {
    let mut trimmed = text.trim();
    loop {
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let mut depth = 0isize;
        let mut in_quote: Option<char> = None;
        let mut encloses = true;
        for (idx, ch) in trimmed.char_indices() {
            if let Some(quote) = in_quote {
                if ch == quote {
                    in_quote = None;
                } else if ch == '\\' {
                    continue;
                }
                continue;
            }
            if ch == '\'' || ch == '"' {
                in_quote = Some(ch);
                continue;
            }
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    encloses = false;
                    break;
                }
            }
        }
        if encloses {
            trimmed = trimmed[1..trimmed.len() - 1].trim();
        } else {
            return trimmed;
        }
    }
}

fn find_jsonpath_compare_operator(expr: &str) -> Option<(usize, &'static str, JsonPathFilterOp)> {
    let bytes = expr.as_bytes();
    let mut idx = 0usize;
    let mut in_quote: Option<u8> = None;
    let mut paren_depth = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if let Some(quote) = in_quote {
            if b == quote {
                in_quote = None;
            } else if b == b'\\' {
                idx += 1;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' | b'"' => {
                in_quote = Some(b);
                idx += 1;
                continue;
            }
            b'(' => {
                paren_depth += 1;
                idx += 1;
                continue;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }
        if paren_depth == 0 {
            for (token, op) in [
                ("==", JsonPathFilterOp::Eq),
                ("!=", JsonPathFilterOp::NotEq),
                (">=", JsonPathFilterOp::Gte),
                ("<=", JsonPathFilterOp::Lte),
                (">", JsonPathFilterOp::Gt),
                ("<", JsonPathFilterOp::Lt),
            ] {
                if expr[idx..].starts_with(token) {
                    return Some((idx, token, op));
                }
            }
        }
        idx += 1;
    }
    None
}

fn parse_jsonpath_filter_operand(
    token: &str,
    context: &str,
) -> Result<JsonPathFilterOperand, EngineError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(EngineError {
            message: format!("{context} JSONPath filter operand is empty"),
        });
    }

    if token == "@" {
        return Ok(JsonPathFilterOperand::Current);
    }
    if token.starts_with("@.") || token.starts_with("@[") {
        let rooted = format!("${}", &token[1..]);
        let steps = parse_jsonpath_steps(&rooted, context)?;
        return Ok(JsonPathFilterOperand::CurrentPath(steps));
    }
    if token == "$" {
        return Ok(JsonPathFilterOperand::RootPath(Vec::new()));
    }
    if token.starts_with("$.") || token.starts_with("$[") {
        let steps = parse_jsonpath_steps(token, context)?;
        return Ok(JsonPathFilterOperand::RootPath(steps));
    }
    if let Some(name) = token.strip_prefix('$')
        && !name.is_empty()
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Ok(JsonPathFilterOperand::Variable(name.to_string()));
    }

    if (token.starts_with('"') && token.ends_with('"'))
        || (token.starts_with('\'') && token.ends_with('\''))
    {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::String(
            token[1..token.len() - 1].to_string(),
        )));
    }
    if token.eq_ignore_ascii_case("true") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(true)));
    }
    if token.eq_ignore_ascii_case("false") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(false)));
    }
    if token.eq_ignore_ascii_case("null") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Null));
    }
    if let Ok(number) = serde_json::from_str::<JsonValue>(token)
        && matches!(number, JsonValue::Number(_))
    {
        return Ok(JsonPathFilterOperand::Literal(number));
    }

    Err(EngineError {
        message: format!("{context} unsupported JSONPath filter operand {token}"),
    })
}

fn parse_jsonpath_filter_expr(
    text: &str,
    context: &str,
) -> Result<JsonPathFilterExpr, EngineError> {
    let trimmed = strip_outer_parentheses(text).trim();
    if trimmed.is_empty() {
        return Err(EngineError {
            message: format!("{context} empty JSONPath filter expression"),
        });
    }

    if trimmed.len() > 7
        && trimmed[..6].eq_ignore_ascii_case("exists")
        && trimmed[6..].trim_start().starts_with('(')
    {
        let rest = trimmed[6..].trim_start();
        if let Some(inner) = rest
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        {
            let operand = parse_jsonpath_filter_operand(inner, context)?;
            return Ok(JsonPathFilterExpr::Exists(operand));
        }
    }

    if let Some((idx, token, op)) = find_jsonpath_compare_operator(trimmed) {
        let left = parse_jsonpath_filter_operand(&trimmed[..idx], context)?;
        let right = parse_jsonpath_filter_operand(&trimmed[idx + token.len()..], context)?;
        return Ok(JsonPathFilterExpr::Compare { left, op, right });
    }

    Ok(JsonPathFilterExpr::Truthy(parse_jsonpath_filter_operand(
        trimmed, context,
    )?))
}

fn parse_jsonpath_steps(path: &str, context: &str) -> Result<Vec<JsonPathStep>, EngineError> {
    let bytes = path.trim().as_bytes();
    if bytes.is_empty() || bytes[0] != b'$' {
        return Err(EngineError {
            message: format!("{context} JSONPath must start with '$'"),
        });
    }

    let mut steps = Vec::new();
    let mut idx = 1usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b' ' | b'\t' | b'\r' | b'\n' => idx += 1,
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath"),
                    });
                }
                if bytes[idx] == b'*' {
                    steps.push(JsonPathStep::Wildcard);
                    idx += 1;
                    continue;
                }
                if bytes[idx] == b'"' {
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'"' {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath key"),
                    });
                }
                let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                steps.push(JsonPathStep::Key(key.to_string()));
            }
            b'[' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                if bytes[idx] == b'*' {
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath wildcard index"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Wildcard);
                    continue;
                }
                if bytes[idx] == b'\'' || bytes[idx] == b'"' {
                    let quote = bytes[idx];
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != quote {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath bracket expression"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b']' {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                let token = std::str::from_utf8(&bytes[start..idx])
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                idx += 1;
                if token.is_empty() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath index"),
                    });
                }
                if let Ok(index) = token.parse::<i64>() {
                    steps.push(JsonPathStep::Index(index));
                } else {
                    steps.push(JsonPathStep::Key(token));
                }
            }
            b'?' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() || bytes[idx] != b'(' {
                    return Err(EngineError {
                        message: format!("{context} expected '(' after JSONPath filter marker"),
                    });
                }
                idx += 1;
                let start = idx;
                let mut depth = 1usize;
                let mut in_quote: Option<u8> = None;
                while idx < bytes.len() {
                    let b = bytes[idx];
                    if let Some(quote) = in_quote {
                        if b == quote {
                            in_quote = None;
                        } else if b == b'\\' {
                            idx += 1;
                        }
                        idx += 1;
                        continue;
                    }
                    if b == b'\'' || b == b'"' {
                        in_quote = Some(b);
                        idx += 1;
                        continue;
                    }
                    if b == b'(' {
                        depth += 1;
                        idx += 1;
                        continue;
                    }
                    if b == b')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        idx += 1;
                        continue;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() || depth != 0 {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath filter expression"),
                    });
                }
                let expr_text = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                idx += 1;
                steps.push(JsonPathStep::Filter(parse_jsonpath_filter_expr(
                    expr_text, context,
                )?));
            }
            _ => {
                return Err(EngineError {
                    message: format!("{context} invalid JSONPath near byte {}", idx),
                });
            }
        }
    }
    Ok(steps)
}

fn jsonpath_query_values(
    root: &JsonValue,
    steps: &[JsonPathStep],
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    let mut current = vec![root.clone()];
    for step in steps {
        let mut next = Vec::new();
        for value in current {
            match step {
                JsonPathStep::Key(key) => {
                    if let JsonValue::Object(map) = value
                        && let Some(found) = map.get(key)
                    {
                        next.push(found.clone());
                    }
                }
                JsonPathStep::Index(index) => {
                    if let JsonValue::Array(items) = value
                        && let Some(idx) = json_array_index_from_i64(items.len(), *index)
                    {
                        next.push(items[idx].clone());
                    }
                }
                JsonPathStep::Wildcard => match value {
                    JsonValue::Array(items) => next.extend(items),
                    JsonValue::Object(map) => next.extend(map.values().cloned()),
                    JsonValue::Null
                    | JsonValue::Bool(_)
                    | JsonValue::Number(_)
                    | JsonValue::String(_) => {}
                },
                JsonPathStep::Filter(expr) => {
                    if eval_jsonpath_filter_expr(
                        expr,
                        &value,
                        absolute_root,
                        vars,
                        context,
                        silent,
                    )? {
                        next.push(value);
                    }
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }
    Ok(current)
}

fn jsonpath_value_truthy(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => false,
        JsonValue::Bool(v) => *v,
        JsonValue::Number(v) => v.as_f64().is_some_and(|n| n != 0.0),
        JsonValue::String(v) => !v.is_empty(),
        JsonValue::Array(v) => !v.is_empty(),
        JsonValue::Object(v) => !v.is_empty(),
    }
}

fn json_values_compare(left: &JsonValue, right: &JsonValue) -> Result<Ordering, EngineError> {
    match (left, right) {
        (JsonValue::Array(_), JsonValue::Array(_))
        | (JsonValue::Object(_), JsonValue::Object(_)) => {
            if left == right {
                Ok(Ordering::Equal)
            } else {
                Ok(left.to_string().cmp(&right.to_string()))
            }
        }
        _ => {
            compare_values_for_predicate(&json_value_to_scalar(left), &json_value_to_scalar(right))
        }
    }
}

fn eval_jsonpath_filter_operand(
    operand: &JsonPathFilterOperand,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    match operand {
        JsonPathFilterOperand::Current => Ok(vec![current.clone()]),
        JsonPathFilterOperand::CurrentPath(steps) => {
            jsonpath_query_values(current, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::RootPath(steps) => {
            jsonpath_query_values(absolute_root, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::Variable(name) => {
            if let Some(value) = vars.get(name) {
                Ok(vec![value.clone()])
            } else if silent {
                Ok(Vec::new())
            } else {
                Err(EngineError {
                    message: format!("{context} JSONPath variable ${name} is not provided"),
                })
            }
        }
        JsonPathFilterOperand::Literal(value) => Ok(vec![value.clone()]),
    }
}

fn eval_jsonpath_filter_expr(
    expr: &JsonPathFilterExpr,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<bool, EngineError> {
    match expr {
        JsonPathFilterExpr::Exists(operand) => Ok(!eval_jsonpath_filter_operand(
            operand,
            current,
            absolute_root,
            vars,
            context,
            silent,
        )?
        .is_empty()),
        JsonPathFilterExpr::Truthy(operand) => {
            let values = eval_jsonpath_filter_operand(
                operand,
                current,
                absolute_root,
                vars,
                context,
                silent,
            )?;
            Ok(values.iter().any(jsonpath_value_truthy))
        }
        JsonPathFilterExpr::Compare { left, op, right } => {
            let left_values =
                eval_jsonpath_filter_operand(left, current, absolute_root, vars, context, silent)?;
            let right_values =
                eval_jsonpath_filter_operand(right, current, absolute_root, vars, context, silent)?;
            for left in &left_values {
                for right in &right_values {
                    let ordering = json_values_compare(left, right)?;
                    let matched = match op {
                        JsonPathFilterOp::Eq => ordering == Ordering::Equal,
                        JsonPathFilterOp::NotEq => ordering != Ordering::Equal,
                        JsonPathFilterOp::Gt => ordering == Ordering::Greater,
                        JsonPathFilterOp::Gte => {
                            matches!(ordering, Ordering::Greater | Ordering::Equal)
                        }
                        JsonPathFilterOp::Lt => ordering == Ordering::Less,
                        JsonPathFilterOp::Lte => {
                            matches!(ordering, Ordering::Less | Ordering::Equal)
                        }
                    };
                    if matched {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }
}

fn eval_json_path_predicate_operator(
    left: ScalarValue,
    right: ScalarValue,
    match_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(
        &left,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        1,
    )?;
    let path_text = parse_jsonpath_text_arg(
        &right,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        2,
    )?;
    let steps = parse_jsonpath_steps(
        &path_text,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
    )?;
    let vars = JsonMap::new();
    let values = jsonpath_query_values(
        &target,
        &steps,
        &target,
        &vars,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        false,
    )?;
    if match_mode {
        if let Some(first) = values.first() {
            Ok(ScalarValue::Bool(jsonpath_value_truthy(first)))
        } else {
            Ok(ScalarValue::Bool(false))
        }
    } else {
        Ok(ScalarValue::Bool(!values.is_empty()))
    }
}

pub(crate) fn jsonb_path_query_values(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Vec<JsonValue>, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(Vec::new());
    }
    let silent = if args.len() >= 4 {
        parse_jsonpath_silent_arg(&args[3], fn_name, 4)?
    } else {
        false
    };
    let evaluate = || -> Result<Vec<JsonValue>, EngineError> {
        let target = parse_json_document_arg(&args[0], fn_name, 1)?;
        let path_text = parse_jsonpath_text_arg(&args[1], fn_name, 2)?;
        let vars = if args.len() >= 3 {
            parse_jsonpath_vars_arg(&args[2], fn_name, 3)?
        } else {
            JsonMap::new()
        };
        let steps = parse_jsonpath_steps(&path_text, fn_name)?;
        jsonpath_query_values(&target, &steps, &target, &vars, fn_name, silent)
    };
    if silent {
        evaluate().or(Ok(Vec::new()))
    } else {
        evaluate()
    }
}

pub(crate) fn eval_jsonb_path_exists(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Bool(!values.is_empty()))
}

pub(crate) fn eval_jsonb_path_match(args: &[ScalarValue], fn_name: &str) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        match first {
            JsonValue::Null => Ok(ScalarValue::Null),
            JsonValue::Bool(v) => Ok(ScalarValue::Bool(*v)),
            _ => Ok(ScalarValue::Bool(jsonpath_value_truthy(first))),
        }
    } else {
        Ok(ScalarValue::Bool(false))
    }
}

pub(crate) fn eval_jsonb_path_query_array(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Text(JsonValue::Array(values).to_string()))
}

pub(crate) fn eval_jsonb_path_query_first(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        Ok(ScalarValue::Text(first.to_string()))
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(crate) fn eval_json_get_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ->/->>", 1)?;
    let path_segment = scalar_to_json_path_segment(&right, "->")?;
    let Some(found) = extract_json_get_value(&target, &path_segment) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

pub(crate) fn eval_json_path_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator #>/#>>", 1)?;
    let path = parse_json_path_operand(&right, "#>")?;
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

fn json_concat(lhs: JsonValue, rhs: JsonValue) -> JsonValue {
    match (lhs, rhs) {
        (JsonValue::Object(mut left), JsonValue::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            JsonValue::Object(left)
        }
        (JsonValue::Array(mut left), JsonValue::Array(right)) => {
            left.extend(right);
            JsonValue::Array(left)
        }
        (JsonValue::Array(mut left), right) => {
            left.push(right);
            JsonValue::Array(left)
        }
        (left, JsonValue::Array(right)) => {
            let mut out = Vec::with_capacity(right.len() + 1);
            out.push(left);
            out.extend(right);
            JsonValue::Array(out)
        }
        (left, right) => JsonValue::Array(vec![left, right]),
    }
}

pub(crate) fn eval_json_concat_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match (left, right) {
        (ScalarValue::Array(mut left_items), ScalarValue::Array(right_items)) => {
            left_items.extend(right_items);
            Ok(ScalarValue::Array(left_items))
        }
        (ScalarValue::Array(mut left_items), other) => {
            left_items.push(other);
            Ok(ScalarValue::Array(left_items))
        }
        (other, ScalarValue::Array(mut right_items)) => {
            right_items.insert(0, other);
            Ok(ScalarValue::Array(right_items))
        }
        (left, right) => {
            let lhs = parse_json_document_arg(&left, "json operator ||", 1)?;
            let rhs = parse_json_document_arg(&right, "json operator ||", 2)?;
            Ok(ScalarValue::Text(json_concat(lhs, rhs).to_string()))
        }
    }
}

fn json_contains(lhs: &JsonValue, rhs: &JsonValue) -> bool {
    match (lhs, rhs) {
        (JsonValue::Object(lmap), JsonValue::Object(rmap)) => rmap.iter().all(|(key, rvalue)| {
            lmap.get(key)
                .is_some_and(|lvalue| json_contains(lvalue, rvalue))
        }),
        (JsonValue::Array(larr), JsonValue::Array(rarr)) => rarr
            .iter()
            .all(|rvalue| larr.iter().any(|lvalue| json_contains(lvalue, rvalue))),
        _ => lhs == rhs,
    }
}

pub(crate) fn eval_json_contains_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator @>", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator @>", 2)?;
    Ok(ScalarValue::Bool(json_contains(&lhs, &rhs)))
}

pub(crate) fn eval_json_contained_by_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator <@", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator <@", 2)?;
    Ok(ScalarValue::Bool(json_contains(&rhs, &lhs)))
}

fn json_array_index_from_i64(len: usize, index: i64) -> Option<usize> {
    if index >= 0 {
        let idx = index as usize;
        if idx < len { Some(idx) } else { None }
    } else {
        let back = index.unsigned_abs() as usize;
        if back == 0 || back > len {
            None
        } else {
            Some(len - back)
        }
    }
}

fn json_array_index_from_segment(len: usize, segment: &str) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    json_array_index_from_i64(len, index)
}

fn json_remove_path(target: &mut JsonValue, path: &[String]) -> bool {
    if path.is_empty() {
        return false;
    }
    if path.len() == 1 {
        return match target {
            JsonValue::Object(map) => map.remove(&path[0]).is_some(),
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                    return false;
                };
                array.remove(idx);
                true
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                false
            }
        };
    }

    match target {
        JsonValue::Object(map) => map
            .get_mut(&path[0])
            .is_some_and(|next| json_remove_path(next, &path[1..])),
        JsonValue::Array(array) => {
            let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                return false;
            };
            json_remove_path(&mut array[idx], &path[1..])
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_json_delete_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator -", 1)?;
    match &mut target {
        JsonValue::Object(map) => {
            let key = scalar_to_json_path_segment(&right, "-")?;
            map.remove(&key);
        }
        JsonValue::Array(array) => match right {
            ScalarValue::Int(index) => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index) {
                    array.remove(idx);
                }
            }
            ScalarValue::Float(index) if index.fract() == 0.0 => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index as i64) {
                    array.remove(idx);
                }
            }
            ScalarValue::Text(text) => {
                array.retain(|item| !matches!(item, JsonValue::String(value) if value == &text));
            }
            _ => {
                return Err(EngineError {
                    message: "json operator - expects text key or integer array index".to_string(),
                });
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            return Err(EngineError {
                message: "json operator - expects object or array left operand".to_string(),
            });
        }
    }
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_json_delete_path_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator #-", 1)?;
    let path = parse_json_path_operand(&right, "#-")?;
    if !path.is_empty() {
        let _ = json_remove_path(&mut target, &path);
    }
    Ok(ScalarValue::Text(target.to_string()))
}

fn json_has_key(target: &JsonValue, key: &str) -> bool {
    match target {
        JsonValue::Object(map) => map.contains_key(key),
        JsonValue::Array(array) => array
            .iter()
            .any(|item| matches!(item, JsonValue::String(text) if text == key)),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_json_has_key_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?", 1)?;
    let key = scalar_to_json_path_segment(&right, "?")?;
    Ok(ScalarValue::Bool(json_has_key(&target, &key)))
}

fn parse_json_key_list_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    let keys = parse_json_path_operand(value, operator_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{operator_name} operator key array cannot be empty"),
        });
    }
    Ok(keys)
}

pub(crate) fn eval_json_has_any_all_operator(
    left: ScalarValue,
    right: ScalarValue,
    any_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?|/?&", 1)?;
    let keys = parse_json_key_list_operand(&right, if any_mode { "?|" } else { "?&" })?;
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&target, key))
    } else {
        keys.iter().all(|key| json_has_key(&target, key))
    };
    Ok(ScalarValue::Bool(matched))
}

async fn eval_http_get_builtin(url_value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_get() expects a text URL argument".to_string(),
        });
    };

    #[cfg(not(target_arch = "wasm32"))]
    {
        let response = reqwest::get(url).await.map_err(|err| EngineError {
            message: format!("http_get request failed: {err}"),
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(EngineError {
                message: format!("http_get() request failed with status {status}"),
            });
        }
        let body = response.text().await.map_err(|err| EngineError {
            message: format!("http_get body read failed: {err}"),
        })?;
        Ok(ScalarValue::Text(body))
    }

    #[cfg(target_arch = "wasm32")]
    {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;

        let window = web_sys::window().ok_or_else(|| EngineError {
            message: "http_get(): window is not available".to_string(),
        })?;
        let resp_value = JsFuture::from(window.fetch_with_str(url))
            .await
            .map_err(|e| EngineError {
                message: format!(
                    "http_get() request failed: {}",
                    e.as_string().unwrap_or_else(|| "unknown error".to_string())
                ),
            })?;
        let resp: web_sys::Response = resp_value.dyn_into().map_err(|_| EngineError {
            message: "http_get(): response was not a Response".to_string(),
        })?;
        if !resp.ok() {
            return Err(EngineError {
                message: format!(
                    "http_get() request failed with status {} {}",
                    resp.status(),
                    resp.status_text()
                ),
            });
        }
        let text_promise = resp.text().map_err(|_| EngineError {
            message: "http_get(): failed to read response body".to_string(),
        })?;
        let body = JsFuture::from(text_promise)
            .await
            .map_err(|_| EngineError {
                message: "http_get(): failed to read response body".to_string(),
            })?
            .as_string()
            .unwrap_or_default();
        Ok(ScalarValue::Text(body))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericOperand {
    Int(i64),
    Float(f64),
}

pub(crate) fn parse_numeric_operand(value: &ScalarValue) -> Result<NumericOperand, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(NumericOperand::Int(*v)),
        ScalarValue::Float(v) => Ok(NumericOperand::Float(*v)),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(NumericOperand::Int(parsed));
            }
            if let Ok(parsed) = v.parse::<f64>() {
                return Ok(NumericOperand::Float(parsed));
            }
            Err(EngineError {
                message: "numeric operation expects numeric values".to_string(),
            })
        }
        ScalarValue::Array(_) => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
        _ => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
    }
}

pub(crate) fn compare_values_for_predicate(
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Ordering, EngineError> {
    if let (Ok(left_num), Ok(right_num)) =
        (parse_numeric_operand(left), parse_numeric_operand(right))
    {
        let ord = match (left_num, right_num) {
            (NumericOperand::Int(a), NumericOperand::Int(b)) => a.cmp(&b),
            (NumericOperand::Int(a), NumericOperand::Float(b)) => {
                (a as f64).partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Int(b)) => {
                a.partial_cmp(&(b as f64)).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Float(b)) => {
                a.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
        };
        return Ok(ord);
    }

    if let (Some(left_bool), Some(right_bool)) = (try_parse_bool(left), try_parse_bool(right)) {
        return Ok(left_bool.cmp(&right_bool));
    }

    if let (Some(left_time), Some(right_time)) =
        (parse_temporal_operand(left), parse_temporal_operand(right))
    {
        if left_time.date_only && right_time.date_only {
            let left_days = days_from_civil(
                left_time.datetime.date.year,
                left_time.datetime.date.month,
                left_time.datetime.date.day,
            );
            let right_days = days_from_civil(
                right_time.datetime.date.year,
                right_time.datetime.date.month,
                right_time.datetime.date.day,
            );
            return Ok(left_days.cmp(&right_days));
        }
        let left_epoch = datetime_to_epoch_seconds(left_time.datetime);
        let right_epoch = datetime_to_epoch_seconds(right_time.datetime);
        return Ok(left_epoch.cmp(&right_epoch));
    }

    match (left, right) {
        (ScalarValue::Text(a), ScalarValue::Text(b)) => Ok(a.cmp(b)),
        _ => Ok(left.render().cmp(&right.render())),
    }
}

fn try_parse_bool(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Bool(v) => Some(*v),
        ScalarValue::Int(v) => Some(*v != 0),
        ScalarValue::Text(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "t" | "1" => Some(true),
                "false" | "f" | "0" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_nullable_bool(value: &ScalarValue, message: &str) -> Result<Option<bool>, EngineError> {
    match value {
        ScalarValue::Bool(v) => Ok(Some(*v)),
        ScalarValue::Null => Ok(None),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrimMode {
    Left,
    Right,
    Both,
}

pub(crate) fn substring_chars(input: &str, start: i64, length: Option<i64>) -> Result<String, EngineError> {
    if let Some(length) = length
        && length < 0
    {
        return Err(EngineError {
            message: "negative substring length not allowed".to_string(),
        });
    }
    let chars = input.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return Ok(String::new());
    }

    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx >= chars.len() {
        return Ok(String::new());
    }
    let end_idx = match length {
        Some(len) => start_idx.saturating_add(len as usize).min(chars.len()),
        None => chars.len(),
    };
    Ok(chars[start_idx..end_idx].iter().collect())
}

pub(crate) fn left_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        return chars[..(count as usize).min(chars.len())].iter().collect();
    }
    let keep = chars.len().saturating_sub(count.unsigned_abs() as usize);
    chars[..keep].iter().collect()
}

pub(crate) fn right_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        let keep = (count as usize).min(chars.len());
        return chars[chars.len() - keep..].iter().collect();
    }
    let drop = count.unsigned_abs() as usize;
    let start = drop.min(chars.len());
    chars[start..].iter().collect()
}

pub(crate) fn find_substring_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    let Some(byte_idx) = haystack.find(needle) else {
        return 0;
    };
    haystack[..byte_idx].chars().count() as i64 + 1
}

pub(crate) fn overlay_text(
    input: &str,
    replacement: &str,
    start: i64,
    count: Option<i64>,
) -> Result<String, EngineError> {
    let mut chars = input.chars().collect::<Vec<_>>();
    let replace_chars = replacement.chars().collect::<Vec<_>>();
    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx > chars.len() {
        return Ok(input.to_string());
    }
    let count = count.unwrap_or(replace_chars.len() as i64);
    if count < 0 {
        return Err(EngineError {
            message: "overlay() expects non-negative count".to_string(),
        });
    }
    let end_idx = start_idx.saturating_add(count as usize).min(chars.len());
    chars.splice(start_idx..end_idx, replace_chars);
    Ok(chars.iter().collect())
}

pub(crate) fn ascii_code(input: &str) -> i64 {
    input.chars().next().map(|c| c as i64).unwrap_or(0)
}

pub(crate) fn chr_from_code(code: i64) -> Result<String, EngineError> {
    if !(0..=255).contains(&code) {
        return Err(EngineError {
            message: "chr() expects value between 0 and 255".to_string(),
        });
    }
    let ch = char::from_u32(code as u32).ok_or_else(|| EngineError {
        message: "chr() expects valid Unicode code point".to_string(),
    })?;
    Ok(ch.to_string())
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn quote_ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

fn quote_nullable(value: &ScalarValue) -> String {
    if matches!(value, ScalarValue::Null) {
        "NULL".to_string()
    } else {
        quote_literal(&value.render())
    }
}

fn count_nulls(args: &[ScalarValue]) -> usize {
    args.iter().filter(|arg| matches!(arg, ScalarValue::Null)).count()
}

fn count_nonnulls(args: &[ScalarValue]) -> usize {
    args.len() - count_nulls(args)
}

pub(crate) fn trim_text(input: &str, trim_chars: Option<&str>, mode: TrimMode) -> String {
    match trim_chars {
        None => match mode {
            TrimMode::Left => input.trim_start().to_string(),
            TrimMode::Right => input.trim_end().to_string(),
            TrimMode::Both => input.trim().to_string(),
        },
        Some(chars) => trim_chars_from_text(input, chars, mode),
    }
}

fn trim_chars_from_text(input: &str, trim_chars: &str, mode: TrimMode) -> String {
    if trim_chars.is_empty() {
        return input.to_string();
    }
    let set: HashSet<char> = trim_chars.chars().collect();
    let chars = input.chars().collect::<Vec<_>>();

    let mut start = 0usize;
    let mut end = chars.len();
    if matches!(mode, TrimMode::Left | TrimMode::Both) {
        while start < end && set.contains(&chars[start]) {
            start += 1;
        }
    }
    if matches!(mode, TrimMode::Right | TrimMode::Both) {
        while end > start && set.contains(&chars[end - 1]) {
            end -= 1;
        }
    }
    chars[start..end].iter().collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TemporalOperand {
    datetime: DateTimeValue,
    date_only: bool,
}

fn parse_temporal_operand(value: &ScalarValue) -> Option<TemporalOperand> {
    let ScalarValue::Text(text) = value else {
        return None;
    };
    let trimmed = text.trim();
    let has_time = trimmed.contains('T') || trimmed.contains(' ');
    let datetime = parse_datetime_text(trimmed).ok()?;
    Some(TemporalOperand {
        datetime,
        date_only: !has_time,
    })
}

fn temporal_add_days(temporal: TemporalOperand, days: i64) -> ScalarValue {
    let mut datetime = temporal.datetime;
    datetime.date = add_days(datetime.date, days);
    if temporal.date_only {
        ScalarValue::Text(format_date(datetime.date))
    } else {
        ScalarValue::Text(format_timestamp(datetime))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateValue {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateTimeValue {
    pub(crate) date: DateValue,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
}

pub(crate) fn eval_date_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_date(datetime.date)))
}

pub(crate) fn eval_timestamp_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

pub(crate) fn eval_extract_or_date_part(
    field: &ScalarValue,
    source: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let datetime = parse_datetime_scalar(source)?;
    let value = match field_name.as_str() {
        "year" => ScalarValue::Int(datetime.date.year as i64),
        "month" => ScalarValue::Int(datetime.date.month as i64),
        "day" => ScalarValue::Int(datetime.date.day as i64),
        "hour" => ScalarValue::Int(datetime.hour as i64),
        "minute" => ScalarValue::Int(datetime.minute as i64),
        "second" => ScalarValue::Int(datetime.second as i64),
        "dow" => ScalarValue::Int(day_of_week(datetime.date) as i64),
        "doy" => ScalarValue::Int(day_of_year(datetime.date) as i64),
        "epoch" => ScalarValue::Float(datetime_to_epoch_seconds(datetime) as f64),
        _ => {
            return Err(EngineError {
                message: format!("unsupported date/time field {}", field_name),
            });
        }
    };
    Ok(value)
}

pub(crate) fn eval_date_trunc(field: &ScalarValue, source: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let mut datetime = parse_datetime_scalar(source)?;
    match field_name.as_str() {
        "year" => {
            datetime.date.month = 1;
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "month" => {
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "day" => {
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "hour" => {
            datetime.minute = 0;
            datetime.second = 0;
        }
        "minute" => {
            datetime.second = 0;
        }
        "second" => {}
        _ => {
            return Err(EngineError {
                message: format!("unsupported date_trunc field {}", field_name),
            });
        }
    }
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

pub(crate) fn eval_date_add_sub(
    date_value: &ScalarValue,
    day_delta: &ScalarValue,
    add: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(date_value, ScalarValue::Null) || matches!(day_delta, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(date_value)?;
    let mut days = parse_i64_scalar(day_delta, "date_add/date_sub expects integer day count")?;
    if !add {
        days = -days;
    }
    let shifted = add_days(datetime.date, days);
    Ok(ScalarValue::Text(format_date(shifted)))
}

pub(crate) fn current_timestamp_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_timestamp(dt))
}

pub(crate) fn current_date_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_date(dt.date))
}

#[derive(Debug, Clone, Copy)]
struct IntervalValue {
    months: i64,
    days: i64,
    seconds: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum JustifyMode {
    Hours,
    Days,
    Full,
}

pub(crate) fn eval_age(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let (left, right) = if args.len() == 1 {
        let current = current_utc_datetime()?;
        let current_midnight = DateTimeValue {
            date: current.date,
            hour: 0,
            minute: 0,
            second: 0,
        };
        (current_midnight, parse_datetime_scalar(&args[0])?)
    } else {
        (
            parse_datetime_scalar(&args[0])?,
            parse_datetime_scalar(&args[1])?,
        )
    };
    let delta = datetime_to_epoch_seconds(left) - datetime_to_epoch_seconds(right);
    let interval = interval_from_seconds(delta);
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_to_timestamp(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let seconds = parse_f64_scalar(value, "to_timestamp() expects numeric input")?;
    let dt = datetime_from_epoch_seconds(seconds.trunc() as i64);
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

pub(crate) fn eval_to_timestamp_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let dt = parse_datetime_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

pub(crate) fn eval_to_date_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let date = parse_date_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_date(date)))
}

pub(crate) fn eval_make_interval(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let years = parse_i64_scalar(&args[0], "make_interval() expects years")?;
    let months = parse_i64_scalar(&args[1], "make_interval() expects months")?;
    let weeks = parse_i64_scalar(&args[2], "make_interval() expects weeks")?;
    let days = parse_i64_scalar(&args[3], "make_interval() expects days")?;
    let hours = parse_i64_scalar(&args[4], "make_interval() expects hours")?;
    let mins = parse_i64_scalar(&args[5], "make_interval() expects mins")?;
    let secs = parse_f64_scalar(&args[6], "make_interval() expects secs")?;

    let interval = IntervalValue {
        months: years * 12 + months,
        days: weeks * 7 + days,
        seconds: hours * 3_600 + mins * 60 + secs.trunc() as i64,
    };
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_justify_interval(
    value: &ScalarValue,
    mode: JustifyMode,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut interval = parse_interval_text(&value.render())?;
    if matches!(mode, JustifyMode::Hours | JustifyMode::Full) {
        let extra_days = interval.seconds.div_euclid(86_400);
        interval.days += extra_days;
        interval.seconds = interval.seconds.rem_euclid(86_400);
    }
    if matches!(mode, JustifyMode::Days | JustifyMode::Full) {
        let extra_months = interval.days.div_euclid(30);
        interval.months += extra_months;
        interval.days = interval.days.rem_euclid(30);
    }
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_isfinite(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let finite = match value {
        ScalarValue::Text(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            !matches!(normalized.as_str(), "infinity" | "-infinity" | "nan")
        }
        ScalarValue::Float(f) => f.is_finite(),
        _ => true,
    };
    Ok(ScalarValue::Bool(finite))
}

fn eval_width_bucket(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let value = parse_f64_scalar(&args[0], "width_bucket() expects numeric value")?;
    let min = parse_f64_scalar(&args[1], "width_bucket() expects numeric min")?;
    let max = parse_f64_scalar(&args[2], "width_bucket() expects numeric max")?;
    let count = parse_i64_scalar(&args[3], "width_bucket() expects integer count")?;
    if count <= 0 {
        return Err(EngineError {
            message: "width_bucket() expects positive count".to_string(),
        });
    }
    if min == max {
        return Err(EngineError {
            message: "width_bucket() requires min and max to differ".to_string(),
        });
    }
    let buckets = count as f64;
    let bucket = if min < max {
        if value < min {
            0
        } else if value >= max {
            count + 1
        } else {
            (((value - min) * buckets) / (max - min)).floor() as i64 + 1
        }
    } else {
        if value > min {
            0
        } else if value <= max {
            count + 1
        } else {
            (((min - value) * buckets) / (min - max)).floor() as i64 + 1
        }
    };
    Ok(ScalarValue::Int(bucket))
}

fn eval_scale(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let rendered = value.render();
    let trimmed = rendered.trim();
    let main = if let Some(idx) = trimmed.find('e').or_else(|| trimmed.find('E')) {
        &trimmed[..idx]
    } else {
        trimmed
    };
    let scale = main
        .split_once('.')
        .map(|(_, frac)| frac.len() as i64)
        .unwrap_or(0);
    Ok(ScalarValue::Int(scale))
}

fn eval_factorial(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let n = parse_i64_scalar(value, "factorial() expects integer")?;
    if n < 0 {
        return Err(EngineError {
            message: "factorial() expects non-negative integer".to_string(),
        });
    }
    let mut acc: i64 = 1;
    for i in 1..=n {
        acc = acc.checked_mul(i).ok_or_else(|| EngineError {
            message: "factorial() overflowed".to_string(),
        })?;
    }
    Ok(ScalarValue::Int(acc))
}

fn current_utc_datetime() -> Result<DateTimeValue, EngineError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let epoch_seconds = match now.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(err) => -(err.duration().as_secs() as i64),
    };
    Ok(datetime_from_epoch_seconds(epoch_seconds))
}

fn parse_datetime_scalar(value: &ScalarValue) -> Result<DateTimeValue, EngineError> {
    match value {
        ScalarValue::Text(v) => parse_datetime_text(v),
        ScalarValue::Int(v) => Ok(datetime_from_epoch_seconds(*v)),
        ScalarValue::Float(v) => Ok(datetime_from_epoch_seconds(*v as i64)),
        _ => Err(EngineError {
            message: "expected date/timestamp-compatible value".to_string(),
        }),
    }
}

pub(crate) fn parse_datetime_text(text: &str) -> Result<DateTimeValue, EngineError> {
    let raw = text.trim();
    if raw.is_empty() {
        return Err(EngineError {
            message: "invalid date/timestamp value".to_string(),
        });
    }

    let (date_part, time_part) = if let Some(pos) = raw.find('T') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else if let Some(pos) = raw.find(' ') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else {
        (raw, None)
    };

    let date = parse_date_text(date_part)?;
    let (hour, minute, second) = match time_part {
        None => (0, 0, 0),
        Some(time_raw) => parse_time_text(time_raw)?,
    };
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_text(text: &str) -> Result<DateValue, EngineError> {
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(EngineError {
            message: "invalid date value".to_string(),
        });
    }
    let year = parts[0].parse::<i32>().map_err(|_| EngineError {
        message: "invalid date year".to_string(),
    })?;
    let month = parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date month".to_string(),
    })?;
    let day = parts[2].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date day".to_string(),
    })?;
    if month == 0 || month > 12 {
        return Err(EngineError {
            message: "invalid date month".to_string(),
        });
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(EngineError {
            message: "invalid date day".to_string(),
        });
    }
    Ok(DateValue { year, month, day })
}

fn parse_time_text(text: &str) -> Result<(u32, u32, u32), EngineError> {
    let mut cleaned = text.trim();
    if cleaned.ends_with('Z') {
        cleaned = &cleaned[..cleaned.len() - 1];
    }
    if let Some(sign_pos) = cleaned
        .char_indices()
        .find_map(|(idx, ch)| ((ch == '+' || ch == '-') && idx > 1).then_some(idx))
    {
        cleaned = &cleaned[..sign_pos];
    }
    let time_parts = cleaned.split(':').collect::<Vec<_>>();
    if time_parts.len() < 2 || time_parts.len() > 3 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    let hour = time_parts[0].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp hour".to_string(),
    })?;
    let minute = time_parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp minute".to_string(),
    })?;
    let second = if time_parts.len() == 3 {
        time_parts[2]
            .split('.')
            .next()
            .unwrap_or("")
            .parse::<u32>()
            .map_err(|_| EngineError {
                message: "invalid timestamp second".to_string(),
            })?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    Ok((hour, minute, second))
}

fn parse_datetime_with_format(text: &str, format: &str) -> Result<DateTimeValue, EngineError> {
    let (date, hour, minute, second) = parse_datetime_parts_with_format(text, format)?;
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_with_format(text: &str, format: &str) -> Result<DateValue, EngineError> {
    let (date, _hour, _minute, _second) = parse_datetime_parts_with_format(text, format)?;
    Ok(date)
}

fn parse_datetime_parts_with_format(
    text: &str,
    format: &str,
) -> Result<(DateValue, u32, u32, u32), EngineError> {
    let input = text.trim();
    let fmt = format.trim().to_ascii_uppercase();
    let mut in_idx = 0usize;
    let mut fmt_idx = 0usize;
    let bytes = input.as_bytes();

    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    let mut day: Option<u32> = None;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;

    while fmt_idx < fmt.len() {
        let remaining = &fmt[fmt_idx..];
        if remaining.starts_with("YYYY") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 4, "year")? as i32;
            year = Some(value);
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MM") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "month")? as u32;
            month = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("DD") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "day")? as u32;
            day = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("HH24") {
            hour = parse_fixed_digits(bytes, &mut in_idx, 2, "hour")? as u32;
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MI") {
            minute = parse_fixed_digits(bytes, &mut in_idx, 2, "minute")? as u32;
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("SS") {
            second = parse_fixed_digits(bytes, &mut in_idx, 2, "second")? as u32;
            fmt_idx += 2;
            continue;
        }

        let ch = fmt[fmt_idx..].chars().next().unwrap();
        let ch_len = ch.len_utf8();
        if in_idx >= bytes.len() || bytes[in_idx] != ch as u8 {
            return Err(EngineError {
                message: "to_timestamp/to_date format mismatch".to_string(),
            });
        }
        in_idx += 1;
        fmt_idx += ch_len;
    }

    if in_idx != bytes.len() {
        return Err(EngineError {
            message: "to_timestamp/to_date format mismatch".to_string(),
        });
    }

    let date = date_from_parts(
        year.ok_or_else(|| EngineError { message: "missing year".to_string() })?,
        month.ok_or_else(|| EngineError { message: "missing month".to_string() })?,
        day.ok_or_else(|| EngineError { message: "missing day".to_string() })?,
    )?;

    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid time component".to_string(),
        });
    }

    Ok((date, hour, minute, second))
}

fn parse_fixed_digits(
    bytes: &[u8],
    idx: &mut usize,
    count: usize,
    label: &str,
) -> Result<i64, EngineError> {
    if *idx + count > bytes.len() {
        return Err(EngineError {
            message: format!("invalid {label} in format input"),
        });
    }
    let slice = &bytes[*idx..*idx + count];
    let text = std::str::from_utf8(slice).unwrap_or("");
    let value = text.parse::<i64>().map_err(|_| EngineError {
        message: format!("invalid {label} in format input"),
    })?;
    *idx += count;
    Ok(value)
}

fn date_from_parts(year: i32, month: u32, day: u32) -> Result<DateValue, EngineError> {
    if month == 0 || month > 12 {
        return Err(EngineError {
            message: "invalid date month".to_string(),
        });
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(EngineError {
            message: "invalid date day".to_string(),
        });
    }
    Ok(DateValue { year, month, day })
}

pub(crate) fn format_date(date: DateValue) -> String {
    format!("{:04}-{:02}-{:02}", date.year, date.month, date.day)
}

pub(crate) fn format_timestamp(datetime: DateTimeValue) -> String {
    format!(
        "{} {:02}:{:02}:{:02}",
        format_date(datetime.date),
        datetime.hour,
        datetime.minute,
        datetime.second
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn day_of_year(date: DateValue) -> u32 {
    let mut total = 0u32;
    for month in 1..date.month {
        total += days_in_month(date.year, month);
    }
    total + date.day
}

fn day_of_week(date: DateValue) -> u32 {
    let days = days_from_civil(date.year, date.month, date.day);
    (days + 4).rem_euclid(7) as u32
}

fn add_days(date: DateValue, days: i64) -> DateValue {
    let day_number = days_from_civil(date.year, date.month, date.day);
    civil_from_days(day_number + days)
}

fn datetime_to_epoch_seconds(datetime: DateTimeValue) -> i64 {
    let days = days_from_civil(datetime.date.year, datetime.date.month, datetime.date.day);
    days * 86_400
        + datetime.hour as i64 * 3_600
        + datetime.minute as i64 * 60
        + datetime.second as i64
}

pub(crate) fn datetime_from_epoch_seconds(seconds: i64) -> DateTimeValue {
    let day = seconds.div_euclid(86_400);
    let sec_of_day = seconds.rem_euclid(86_400);
    let date = civil_from_days(day);
    DateTimeValue {
        date,
        hour: (sec_of_day / 3_600) as u32,
        minute: ((sec_of_day % 3_600) / 60) as u32,
        second: (sec_of_day % 60) as u32,
    }
}

fn interval_from_seconds(seconds: i64) -> IntervalValue {
    let days = seconds.div_euclid(86_400);
    let rem = seconds.rem_euclid(86_400);
    IntervalValue {
        months: 0,
        days,
        seconds: rem,
    }
}

fn parse_interval_text(text: &str) -> Result<IntervalValue, EngineError> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 5 {
        return Err(EngineError {
            message: "invalid interval value".to_string(),
        });
    }
    let months = parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval months".to_string(),
    })?;
    let days = parts[2].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval days".to_string(),
    })?;
    let time = parts[4];
    let time_parts = time.split(':').collect::<Vec<_>>();
    if time_parts.len() != 3 {
        return Err(EngineError {
            message: "invalid interval time".to_string(),
        });
    }
    let hour = time_parts[0]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval hour".to_string(),
        })?;
    let minute = time_parts[1]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval minute".to_string(),
        })?;
    let second = time_parts[2]
        .parse::<i64>()
        .map_err(|_| EngineError {
            message: "invalid interval second".to_string(),
        })?;
    let total_seconds = hour * 3_600 + minute * 60 + second;
    Ok(IntervalValue {
        months,
        days,
        seconds: total_seconds,
    })
}

fn format_interval(interval: IntervalValue) -> String {
    let sign = if interval.seconds < 0 { "-" } else { "" };
    let seconds = interval.seconds.abs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!(
        "{} mons {} days {}{:02}:{:02}:{:02}",
        interval.months, interval.days, sign, hours, minutes, secs
    )
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> DateValue {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    DateValue {
        year: year as i32,
        month: month as u32,
        day: day as u32,
    }
}

fn parse_i64_scalar(value: &ScalarValue, message: &str) -> Result<i64, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(*v),
        ScalarValue::Float(v) if v.fract() == 0.0 => Ok(*v as i64),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(parsed);
            }
            if let Ok(parsed) = v.parse::<f64>()
                && parsed.fract() == 0.0
            {
                return Ok(parsed as i64);
            }
            Err(EngineError {
                message: message.to_string(),
            })
        }
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_f64_scalar(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Text(v) => v.parse::<f64>().map_err(|_| EngineError {
            message: message.to_string(),
        }),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_f64_numeric_scalar(
    value: &ScalarValue,
    message: &str,
) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_bool_scalar(
    value: &ScalarValue,
    message: &str,
) -> Result<bool, EngineError> {
    try_parse_bool(value).ok_or_else(|| EngineError {
        message: message.to_string(),
    })
}

pub(crate) fn truthy(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Bool(v) => *v,
        ScalarValue::Null => false,
        ScalarValue::Int(v) => *v != 0,
        ScalarValue::Float(v) => *v != 0.0,
        ScalarValue::Text(v) => !v.is_empty(),
        ScalarValue::Array(values) => !values.is_empty(),
    }
}

// ── Extension & Function execution ──────────────────────────────────────────

pub(crate) fn is_ws_extension_loaded() -> bool {
    with_ext_read(|ext| ext.extensions.iter().any(|e| e.name == "ws"))
}

async fn execute_ws_connect(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let url = match args.first() {
        Some(ScalarValue::Text(u)) => u.clone(),
        _ => {
            return Err(EngineError {
                message: "ws.connect requires a URL argument".to_string(),
            });
        }
    };
    let on_open = match args.get(1) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let on_message = match args.get(2) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let on_close = match args.get(3) {
        Some(ScalarValue::Text(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    // Try real connection on native (non-wasm, non-test)
    #[cfg(not(target_arch = "wasm32"))]
    let real_result = ws_native::open_connection(&url);
    #[cfg(target_arch = "wasm32")]
    let real_result = ws_wasm::open_connection(&url);

    #[cfg(not(target_arch = "wasm32"))]
    let (real_io, initial_state) = match &real_result {
        Ok(_) => (true, "open".to_string()),
        Err(_) => (false, "connecting".to_string()),
    };
    #[cfg(target_arch = "wasm32")]
    let (real_io, initial_state) = match &real_result {
        Ok(_) => (true, "connecting".to_string()),
        Err(_) => (false, "connecting".to_string()),
    };

    let id = with_ext_write(|ext| {
        let id = ext.ws_next_id;
        ext.ws_next_id += 1;
        ext.ws_connections.insert(id, WsConnection {
            id,
            url,
            state: initial_state,
            opened_at: "2024-01-01 00:00:00".to_string(),
            messages_in: 0,
            messages_out: 0,
            on_open,
            on_message,
            on_close,
            inbound_queue: Vec::new(),
            real_io,
        });
        id
    });

    // Store the handle if real connection succeeded
    #[cfg(not(target_arch = "wasm32"))]
    if let Ok(handle) = real_result {
        native_ws_handles().lock().unwrap().insert(id, handle);
    }
    #[cfg(target_arch = "wasm32")]
    if let Ok(handle) = real_result {
        ws_wasm::store_handle(id, handle);
    }

    Ok(ScalarValue::Int(id))
}

async fn execute_ws_send(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.send requires a connection id".to_string(),
            });
        }
    };
    let _message = match args.get(1) {
        Some(ScalarValue::Text(m)) => m.clone(),
        _ => {
            return Err(EngineError {
                message: "ws.send requires a message argument".to_string(),
            });
        }
    };
    let (is_real, is_closed) = with_ext_read(|ext| {
        if let Some(conn) = ext.ws_connections.get(&conn_id) {
            Ok((conn.real_io, conn.state == "closed"))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })?;
    if is_closed {
        return Err(EngineError {
            message: format!("connection {} is closed", conn_id),
        });
    }

    // Send over real connection if available
    #[cfg(not(target_arch = "wasm32"))]
    if is_real {
        let handles = native_ws_handles().lock().unwrap();
        if let Some(handle) = handles.get(&conn_id) {
            ws_native::send_message(handle, &_message).map_err(|e| EngineError { message: e })?;
        }
    }
    #[cfg(target_arch = "wasm32")]
    if is_real {
        let send_result = ws_wasm::with_handle(conn_id, |handle| {
            ws_wasm::send_message(handle, &_message)
        });
        if let Some(Err(e)) = send_result {
            return Err(EngineError { message: e });
        }
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.messages_out += 1;
            Ok(ScalarValue::Bool(true))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

async fn execute_ws_close(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.close requires a connection id".to_string(),
            });
        }
    };
    // Close real connection if present
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut handles = native_ws_handles().lock().unwrap();
        if let Some(handle) = handles.get(&conn_id) {
            let _ = ws_native::close_connection(handle);
        }
        handles.remove(&conn_id);
    }
    #[cfg(target_arch = "wasm32")]
    {
        ws_wasm::with_handle(conn_id, |handle| {
            let _ = ws_wasm::close_connection(handle);
        });
        ws_wasm::remove_handle(conn_id);
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.state = "closed".to_string();
            Ok(ScalarValue::Bool(true))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

async fn execute_ws_recv(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.recv requires a connection id".to_string(),
            });
        }
    };

    // Drain any real incoming messages first
    #[cfg(not(target_arch = "wasm32"))]
    drain_native_ws_messages(conn_id);
    #[cfg(target_arch = "wasm32")]
    { sync_wasm_ws_state(conn_id); drain_wasm_ws_messages(conn_id); }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            if conn.inbound_queue.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Text(conn.inbound_queue.remove(0)))
            }
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

pub(crate) async fn execute_ws_messages(
    args: &[ScalarValue],
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if !is_ws_extension_loaded() {
        return Err(EngineError {
            message: "extension \"ws\" is not loaded".to_string(),
        });
    }
    let conn_id = match args.first() {
        Some(ScalarValue::Int(id)) => *id,
        _ => {
            return Err(EngineError {
                message: "ws.messages requires a connection id".to_string(),
            });
        }
    };

    // Drain any real incoming messages first
    #[cfg(not(target_arch = "wasm32"))]
    drain_native_ws_messages(conn_id);
    #[cfg(target_arch = "wasm32")]
    { sync_wasm_ws_state(conn_id); drain_wasm_ws_messages(conn_id); }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            let rows: Vec<Vec<ScalarValue>> = conn.inbound_queue
                .drain(..)
                .map(|m| vec![ScalarValue::Text(m)])
                .collect();
            Ok((vec!["message".to_string()], rows))
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })
}

/// Simulate receiving a message on a WebSocket connection (for testing).
/// Dispatches the on_message callback if set.
pub async fn ws_simulate_message(conn_id: i64, message: &str) -> Result<Vec<QueryResult>, EngineError> {
    let callback = with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.messages_in += 1;
            conn.inbound_queue.push(message.to_string());
            Ok(conn.on_message.clone())
        } else {
            Err(EngineError {
                message: format!("connection {} does not exist", conn_id),
            })
        }
    })?;

    let mut results = Vec::new();
    if let Some(func_name) = callback {
        // Look up the user function and execute it with the message as parameter
        let uf = with_ext_read(|ext| {
            ext.user_functions
                .iter()
                .find(|f| {
                    let fname = f.name.last().map(|s| s.as_str()).unwrap_or("");
                    fname == func_name.to_ascii_lowercase()
                })
                .cloned()
        });
        if let Some(uf) = uf {
            // Execute the function body with message substituted for the first parameter
            let body = uf.body.clone();
            // Simple parameter substitution: replace references to the first param with the message value
            let param_name = uf.params.first().and_then(|p| p.name.clone());
            let substituted = if let Some(pname) = param_name {
                body.replace(&pname, &format!("'{}'", message.replace('\'', "''")))
            } else {
                body.replace("$1", &format!("'{}'", message.replace('\'', "''")))
            };
            let stmt = crate::parser::sql_parser::parse_statement(&substituted)
                .map_err(|e| EngineError { message: format!("callback parse error: {}", e) })?;
            let planned = plan_statement(stmt)?;
            let result = execute_planned_query(&planned, &[]).await?;
            results.push(result);
        }
    }
    Ok(results)
}
