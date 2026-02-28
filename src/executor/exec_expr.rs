use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use serde_json::Value as JsonValue;

use crate::executor::exec_main::{
    compare_order_keys, eval_aggregate_function, execute_query_with_outer, is_aggregate_function,
    parse_non_negative_int, row_key,
};
use crate::extensions::pgcrypto::eval_pgcrypto_function;
use crate::extensions::pgvector::{eval_pgvector_function, eval_vector_distance_operator};
use crate::extensions::uuid_ossp::eval_uuid_ossp_function;
use crate::parser::ast::{
    BinaryOp, BooleanTestType, ComparisonQuantifier, CreateFunctionStatement, Expr, OrderByExpr,
    UnaryOp, WindowDefinition, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits,
    WindowSpec,
};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::lookup_user_function as lookup_registered_user_function;
use crate::tcop::engine::{
    EngineError, QueryResult, UserFunction, WsConnection, execute_planned_query, plan_statement,
    with_ext_read, with_ext_write,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::tcop::engine::{drain_native_ws_messages, native_ws_handles, ws_native};
#[cfg(target_arch = "wasm32")]
use crate::tcop::engine::{drain_wasm_ws_messages, sync_wasm_ws_state, ws_wasm};
use crate::utils::adt::datetime::{
    datetime_to_epoch_seconds, days_from_civil, eval_interval_cast, format_date,
    format_interval_value, format_timestamp, interval_add, interval_mul, interval_negate,
    is_interval_text, parse_datetime_scalar, parse_interval_operand, parse_temporal_operand,
    temporal_add_days, temporal_add_interval,
};
use crate::utils::adt::json::{
    eval_json_concat_operator, eval_json_contained_by_operator, eval_json_contains_operator,
    eval_json_delete_operator, eval_json_delete_path_operator, eval_json_get_operator,
    eval_json_has_any_all_operator, eval_json_has_key_operator, eval_json_path_operator,
    eval_json_path_predicate_operator, parse_json_document_arg,
};
use crate::utils::adt::math_functions::{NumericOperand, numeric_mod, parse_numeric_operand};
use crate::utils::adt::misc::{
    compare_values_for_predicate, parse_bool_scalar, parse_f64_numeric_scalar, parse_f64_scalar,
    parse_i64_scalar, parse_nullable_bool, parse_pg_array_literal, parse_pg_numeric_literal,
    truthy,
};
use crate::utils::fmgr::eval_scalar_function;

pub(crate) type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct EvalScope {
    unqualified: HashMap<String, ScalarValue>,
    qualified: HashMap<String, ScalarValue>,
    ambiguous: HashSet<String>,
}

impl EvalScope {
    /// Check whether a column name (unqualified or qualified) exists in this scope.
    pub(crate) fn has_column(&self, name: &str) -> bool {
        if name.contains('.') {
            // For qualified references (e.g. "n2.n_nationkey"), only check the
            // qualified map — do NOT fall back to unqualified, as that could
            // match a different table's column with the same name.
            self.qualified.contains_key(name)
        } else {
            self.unqualified.contains_key(name) || self.ambiguous.contains(name)
        }
    }

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
            if key == "current_user" || key == "session_user" {
                return Ok(ScalarValue::Text(crate::security::current_role()));
            }
            if key == "tableoid" {
                return Ok(ScalarValue::Int(0));
            }
            if key == "ctid" {
                return Ok(ScalarValue::Text("(0,0)".to_string()));
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

        let suffix = format!(".{key}");
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
            Expr::Default => Err(EngineError {
                message: "DEFAULT is only allowed in INSERT VALUES or UPDATE SET".to_string(),
            }),
            Expr::MultiColumnSubqueryRef {
                subquery, index, ..
            } => {
                let result = crate::tcop::engine::execute_query(subquery, params).await?;
                if result.rows.len() > 1 {
                    return Err(EngineError {
                        message: "more than one row returned by a subquery used as an expression"
                            .to_string(),
                    });
                }
                if result.rows.is_empty() {
                    Ok(ScalarValue::Null)
                } else {
                    let row = &result.rows[0];
                    if *index >= row.len() {
                        return Err(EngineError {
                            message: format!("subquery must return {} columns", index + 1),
                        });
                    }
                    Ok(row[*index].clone())
                }
            }
            Expr::Null => Ok(ScalarValue::Null),
            Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
            Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
            Expr::Float(v) => {
                // Try to parse as Decimal first for better precision
                if let Ok(decimal) = v.parse::<rust_decimal::Decimal>() {
                    Ok(ScalarValue::Numeric(decimal))
                } else {
                    let parsed = v.parse::<f64>().map_err(|_| EngineError {
                        message: format!("invalid float literal \"{v}\""),
                    })?;
                    Ok(ScalarValue::Float(parsed))
                }
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
                let row = &result.rows[0];
                if row.len() == 1 {
                    Ok(row[0].clone())
                } else {
                    // Multi-column subquery returns a record
                    Ok(ScalarValue::Record(row.clone()))
                }
            }
            Expr::ArrayConstructor(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(eval_expr(item, scope, params).await?);
                }
                Ok(ScalarValue::Array(values))
            }
            Expr::RowConstructor(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                for field in fields {
                    values.push(eval_expr(field, scope, params).await?);
                }
                Ok(ScalarValue::Record(values))
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
                // For row-value IN subquery, compare rows element-wise
                let rhs_values = if let ScalarValue::Record(_) = &lhs {
                    result
                        .rows
                        .iter()
                        .map(|row| ScalarValue::Record(row.clone()))
                        .collect::<Vec<_>>()
                } else {
                    if !result.columns.is_empty() && result.columns.len() != 1 {
                        return Err(EngineError {
                            message: "subquery must return only one column".to_string(),
                        });
                    }
                    result
                        .rows
                        .iter()
                        .map(|row| row.first().cloned().unwrap_or(ScalarValue::Null))
                        .collect::<Vec<_>>()
                };
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
                escape,
            } => {
                let value = eval_expr(expr, scope, params).await?;
                let pattern_value = eval_expr(pattern, scope, params).await?;
                let escape_char = if let Some(escape_expr) = escape {
                    let escape_value = eval_expr(escape_expr, scope, params).await?;
                    Some(escape_value)
                } else {
                    None
                };
                eval_like_predicate(
                    value,
                    pattern_value,
                    *case_insensitive,
                    *negated,
                    escape_char,
                )
            }
            Expr::IsNull { expr, negated } => {
                let value = eval_expr(expr, scope, params).await?;
                let is_null = matches!(value, ScalarValue::Null);
                Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::BooleanTest {
                expr,
                test_type,
                negated,
            } => {
                let value = eval_expr(expr, scope, params).await?;
                let result = match (test_type, &value) {
                    (BooleanTestType::True, ScalarValue::Bool(true)) => true,
                    (BooleanTestType::True, _) => false,
                    (BooleanTestType::False, ScalarValue::Bool(false)) => true,
                    (BooleanTestType::False, _) => false,
                    (BooleanTestType::Unknown, ScalarValue::Null) => true,
                    (BooleanTestType::Unknown, _) => false,
                };
                Ok(ScalarValue::Bool(if *negated { !result } else { result }))
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
                    if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal
                    {
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
            } => {
                eval_function(
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
                .await
            }
            Expr::Wildcard => Err(EngineError {
                message: "wildcard expression requires FROM support".to_string(),
            }),
            Expr::QualifiedWildcard(parts) => scope.lookup_identifier(parts),
            Expr::ArraySubscript { expr, index } => {
                let array_value = eval_expr(expr, scope, params).await?;
                let index_value = eval_expr(index, scope, params).await?;
                eval_array_subscript(array_value, index_value)
            }
            Expr::ArraySlice { expr, start, end } => {
                let array_value = eval_expr(expr, scope, params).await?;
                let start_value = if let Some(start_expr) = start {
                    Some(eval_expr(start_expr, scope, params).await?)
                } else {
                    None
                };
                let end_value = if let Some(end_expr) = end {
                    Some(eval_expr(end_expr, scope, params).await?)
                } else {
                    None
                };
                eval_array_slice(array_value, start_value, end_value)
            }
            Expr::TypedLiteral { type_name, value } => {
                // Evaluate typed literals as CAST(value AS type_name)
                eval_cast_scalar(ScalarValue::Text(value.clone()), type_name)
            }
        }
    })
}

pub(crate) fn eval_expr_with_window<'a>(
    expr: &'a Expr,
    scope: &'a EvalScope,
    row_idx: usize,
    all_rows: &'a [EvalScope],
    window_definitions: &'a [WindowDefinition],
    params: &'a [Option<String>],
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
            Expr::Default => Err(EngineError {
                message: "DEFAULT is only allowed in INSERT VALUES or UPDATE SET".to_string(),
            }),
            Expr::MultiColumnSubqueryRef {
                subquery, index, ..
            } => {
                let result = crate::tcop::engine::execute_query(subquery, params).await?;
                if result.rows.len() > 1 {
                    return Err(EngineError {
                        message: "more than one row returned by a subquery used as an expression"
                            .to_string(),
                    });
                }
                if result.rows.is_empty() {
                    Ok(ScalarValue::Null)
                } else {
                    let row = &result.rows[0];
                    if *index >= row.len() {
                        return Err(EngineError {
                            message: format!("subquery must return {} columns", index + 1),
                        });
                    }
                    Ok(row[*index].clone())
                }
            }
            Expr::Null => Ok(ScalarValue::Null),
            Expr::Boolean(v) => Ok(ScalarValue::Bool(*v)),
            Expr::Integer(v) => Ok(ScalarValue::Int(*v)),
            Expr::Float(v) => {
                // Try to parse as Decimal first for better precision
                if let Ok(decimal) = v.parse::<rust_decimal::Decimal>() {
                    Ok(ScalarValue::Numeric(decimal))
                } else {
                    let parsed = v.parse::<f64>().map_err(|_| EngineError {
                        message: format!("invalid float literal \"{v}\""),
                    })?;
                    Ok(ScalarValue::Float(parsed))
                }
            }
            Expr::String(v) => Ok(ScalarValue::Text(v.clone())),
            Expr::Parameter(idx) => parse_param(*idx, params),
            Expr::Identifier(parts) => scope.lookup_identifier(parts),
            Expr::Unary { op, expr } => {
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                eval_unary(op.clone(), value)
            }
            Expr::Binary { left, op, right } => {
                let lhs = eval_expr_with_window(
                    left,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let rhs = eval_expr_with_window(
                    right,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                eval_binary(op.clone(), lhs, rhs)
            }
            Expr::AnyAll {
                left,
                op,
                right,
                quantifier,
            } => {
                let lhs = eval_expr_with_window(
                    left,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let rhs = eval_expr_with_window(
                    right,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
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
                let row = &result.rows[0];
                if row.len() == 1 {
                    Ok(row[0].clone())
                } else {
                    // Multi-column subquery returns a record
                    Ok(ScalarValue::Record(row.clone()))
                }
            }
            Expr::ArrayConstructor(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        eval_expr_with_window(
                            item,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await?,
                    );
                }
                Ok(ScalarValue::Array(values))
            }
            Expr::RowConstructor(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                for field in fields {
                    values.push(
                        eval_expr_with_window(
                            field,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await?,
                    );
                }
                Ok(ScalarValue::Record(values))
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
                let lhs = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let mut rhs = Vec::with_capacity(list.len());
                for item in list {
                    rhs.push(
                        eval_expr_with_window(
                            item,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await?,
                    );
                }
                eval_in_membership(lhs, rhs, *negated)
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let lhs = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
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
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let low_value = eval_expr_with_window(
                    low,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let high_value = eval_expr_with_window(
                    high,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                eval_between_predicate(value, low_value, high_value, *negated)
            }
            Expr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let pattern_value = eval_expr_with_window(
                    pattern,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let escape_char = if let Some(escape_expr) = escape {
                    let escape_value = eval_expr_with_window(
                        escape_expr,
                        scope,
                        row_idx,
                        all_rows,
                        window_definitions,
                        params,
                    )
                    .await?;
                    Some(escape_value)
                } else {
                    None
                };
                eval_like_predicate(
                    value,
                    pattern_value,
                    *case_insensitive,
                    *negated,
                    escape_char,
                )
            }
            Expr::IsNull { expr, negated } => {
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let is_null = matches!(value, ScalarValue::Null);
                Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::BooleanTest {
                expr,
                test_type,
                negated,
            } => {
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let result = match (test_type, &value) {
                    (BooleanTestType::True, ScalarValue::Bool(true)) => true,
                    (BooleanTestType::True, _) => false,
                    (BooleanTestType::False, ScalarValue::Bool(false)) => true,
                    (BooleanTestType::False, _) => false,
                    (BooleanTestType::Unknown, ScalarValue::Null) => true,
                    (BooleanTestType::Unknown, _) => false,
                };
                Ok(ScalarValue::Bool(if *negated { !result } else { result }))
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let left_value = eval_expr_with_window(
                    left,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let right_value = eval_expr_with_window(
                    right,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                eval_is_distinct_from(left_value, right_value, *negated)
            }
            Expr::CaseSimple {
                operand,
                when_then,
                else_expr,
            } => {
                let operand_value = eval_expr_with_window(
                    operand,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                for (when_expr, then_expr) in when_then {
                    let when_value = eval_expr_with_window(
                        when_expr,
                        scope,
                        row_idx,
                        all_rows,
                        window_definitions,
                        params,
                    )
                    .await?;
                    if matches!(operand_value, ScalarValue::Null)
                        || matches!(when_value, ScalarValue::Null)
                    {
                        continue;
                    }
                    if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal
                    {
                        return eval_expr_with_window(
                            then_expr,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await;
                    }
                }
                if let Some(else_expr) = else_expr {
                    eval_expr_with_window(
                        else_expr,
                        scope,
                        row_idx,
                        all_rows,
                        window_definitions,
                        params,
                    )
                    .await
                } else {
                    Ok(ScalarValue::Null)
                }
            }
            Expr::CaseSearched {
                when_then,
                else_expr,
            } => {
                for (when_expr, then_expr) in when_then {
                    let condition = eval_expr_with_window(
                        when_expr,
                        scope,
                        row_idx,
                        all_rows,
                        window_definitions,
                        params,
                    )
                    .await?;
                    if truthy(&condition) {
                        return eval_expr_with_window(
                            then_expr,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await;
                    }
                }
                if let Some(else_expr) = else_expr {
                    eval_expr_with_window(
                        else_expr,
                        scope,
                        row_idx,
                        all_rows,
                        window_definitions,
                        params,
                    )
                    .await
                } else {
                    Ok(ScalarValue::Null)
                }
            }
            Expr::Cast { expr, type_name } => {
                let value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
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
                        window_definitions,
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
                    if *distinct
                        || !order_by.is_empty()
                        || !within_group.is_empty()
                        || filter.is_some()
                    {
                        if fn_name.starts_with("aggf")
                            || fn_name.starts_with("logging_agg")
                            || fn_name == "sum_int_randomrestart"
                        {
                            return Ok(ScalarValue::Null);
                        }
                        return Err(EngineError {
                            message: format!(
                                "{fn_name}() aggregate modifiers require grouped aggregate evaluation"
                            ),
                        });
                    }
                    if is_aggregate_function(&fn_name) {
                        return Err(EngineError {
                            message: format!(
                                "aggregate function {fn_name}() must be used with grouped evaluation"
                            ),
                        });
                    }

                    let mut values = Vec::with_capacity(args.len());
                    for arg in args {
                        values.push(
                            eval_expr_with_window(
                                arg,
                                scope,
                                row_idx,
                                all_rows,
                                window_definitions,
                                params,
                            )
                            .await?,
                        );
                    }
                    eval_scalar_function(&fn_name, &values).await
                }
            }
            Expr::Wildcard => Err(EngineError {
                message: "wildcard expression requires FROM support".to_string(),
            }),
            Expr::QualifiedWildcard(parts) => scope.lookup_identifier(parts),
            Expr::ArraySubscript { expr, index } => {
                let array_value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let index_value = eval_expr_with_window(
                    index,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                eval_array_subscript(array_value, index_value)
            }
            Expr::ArraySlice { expr, start, end } => {
                let array_value = eval_expr_with_window(
                    expr,
                    scope,
                    row_idx,
                    all_rows,
                    window_definitions,
                    params,
                )
                .await?;
                let start_value = if let Some(start_expr) = start {
                    Some(
                        eval_expr_with_window(
                            start_expr,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let end_value = if let Some(end_expr) = end {
                    Some(
                        eval_expr_with_window(
                            end_expr,
                            scope,
                            row_idx,
                            all_rows,
                            window_definitions,
                            params,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                eval_array_slice(array_value, start_value, end_value)
            }
            Expr::TypedLiteral { type_name, value } => {
                // Evaluate typed literals as CAST(value AS type_name)
                eval_cast_scalar(ScalarValue::Text(value.clone()), type_name)
            }
        }
    })
}

fn resolve_window_spec(
    spec: &WindowSpec,
    definitions: &[WindowDefinition],
) -> Result<WindowSpec, EngineError> {
    // If no name reference, return as-is
    let Some(ref_name) = &spec.name else {
        return Ok(spec.clone());
    };

    // Find the named window definition
    let def = definitions
        .iter()
        .find(|d| d.name == *ref_name)
        .ok_or_else(|| EngineError {
            message: format!("window \"{ref_name}\" does not exist"),
        })?;

    // Merge: start with base definition, then apply refinements from spec
    // PG rules: cannot override PARTITION BY, can add ORDER BY, can add frame
    if !spec.partition_by.is_empty() {
        return Err(EngineError {
            message: format!("cannot override PARTITION BY clause of window \"{ref_name}\""),
        });
    }

    let mut resolved = def.spec.clone();

    // If spec adds ORDER BY, append to (or replace) the definition's ORDER BY
    if !spec.order_by.is_empty() {
        // PostgreSQL actually replaces, not appends
        resolved.order_by = spec.order_by.clone();
    }

    // If spec adds frame, use it (overrides definition's frame)
    if spec.frame.is_some() {
        resolved.frame = spec.frame.clone();
    }

    // Clear the name reference since it's now resolved
    resolved.name = None;

    Ok(resolved)
}

#[allow(clippy::too_many_arguments)]
async fn eval_window_function(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    filter: Option<&Expr>,
    window: &WindowSpec,
    window_definitions: &[WindowDefinition],
    row_idx: usize,
    all_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    // Resolve any named window reference
    let resolved_window = resolve_window_spec(window, window_definitions)?;

    let fn_name = name
        .last()
        .map(|n| n.to_ascii_lowercase())
        .unwrap_or_default();
    let mut partition = window_partition_rows(&resolved_window, row_idx, all_rows, params).await?;
    let order_keys = window_order_keys(&resolved_window, &mut partition, all_rows, params).await?;
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
                if compare_order_keys(
                    &order_keys[idx - 1],
                    &order_keys[idx],
                    &resolved_window.order_by,
                ) != Ordering::Equal
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
                return Err(EngineError {
                    message: "ntile() expects exactly one argument".to_string(),
                });
            }
            let n = {
                let v = eval_expr(&args[0], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "ntile() expects integer")? as usize
            };
            if n == 0 {
                return Err(EngineError {
                    message: "ntile() argument must be positive".to_string(),
                });
            }
            let total = partition.len();
            let bucket = (current_pos * n / total) + 1;
            Ok(ScalarValue::Int(bucket as i64))
        }
        "percent_rank" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "percent_rank() takes no arguments".to_string(),
                });
            }
            if partition.len() <= 1 {
                return Ok(ScalarValue::Float(0.0));
            }
            // rank - 1 / count - 1
            let mut rank = 1usize;
            if !order_keys.is_empty() {
                for idx in 1..=current_pos {
                    if compare_order_keys(
                        &order_keys[idx - 1],
                        &order_keys[idx],
                        &resolved_window.order_by,
                    ) != Ordering::Equal
                    {
                        rank = idx + 1;
                    }
                }
            }
            Ok(ScalarValue::Float(
                (rank - 1) as f64 / (partition.len() - 1) as f64,
            ))
        }
        "cume_dist" => {
            if !args.is_empty() {
                return Err(EngineError {
                    message: "cume_dist() takes no arguments".to_string(),
                });
            }
            if order_keys.is_empty() {
                return Ok(ScalarValue::Float(1.0));
            }
            // Number of rows <= current row / total rows
            let current_key = &order_keys[current_pos];
            let count_le = order_keys
                .iter()
                .filter(|k| {
                    compare_order_keys(k, current_key, &resolved_window.order_by)
                        != Ordering::Greater
                })
                .count();
            Ok(ScalarValue::Float(count_le as f64 / partition.len() as f64))
        }
        "first_value" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "first_value() expects one argument".to_string(),
                });
            }
            let frame_rows = window_frame_rows(
                &resolved_window,
                &partition,
                &order_keys,
                current_pos,
                all_rows,
                params,
            )
            .await?;
            if let Some(&first) = frame_rows.first() {
                eval_expr(&args[0], &all_rows[first], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "last_value" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "last_value() expects one argument".to_string(),
                });
            }
            let frame_rows = window_frame_rows(
                &resolved_window,
                &partition,
                &order_keys,
                current_pos,
                all_rows,
                params,
            )
            .await?;
            if let Some(&last) = frame_rows.last() {
                eval_expr(&args[0], &all_rows[last], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "nth_value" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: "nth_value() expects two arguments".to_string(),
                });
            }
            let n = {
                let v = eval_expr(&args[1], &all_rows[row_idx], params).await?;
                parse_i64_scalar(&v, "nth_value() expects integer")? as usize
            };
            if n == 0 {
                return Err(EngineError {
                    message: "nth_value() argument must be positive".to_string(),
                });
            }
            let frame_rows = window_frame_rows(
                &resolved_window,
                &partition,
                &order_keys,
                current_pos,
                all_rows,
                params,
            )
            .await?;
            if let Some(&target) = frame_rows.get(n - 1) {
                eval_expr(&args[0], &all_rows[target], params).await
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "sum" | "count" | "avg" | "min" | "max" | "string_agg" | "array_agg" | "any_value"
        | "bool_and" | "bool_or" | "every" | "stddev" | "stddev_samp" | "stddev_pop"
        | "variance" | "var_samp" | "var_pop" => {
            let frame_rows = window_frame_rows(
                &resolved_window,
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
            message: format!("unsupported window function {fn_name}"),
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
        // PostgreSQL default frame:
        // - With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (cumulative)
        // - Without ORDER BY: entire partition (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        if window.order_by.is_empty() {
            return Ok(partition.to_vec());
        }
        // Apply RANGE UNBOUNDED PRECEDING TO CURRENT ROW: include all rows with
        // order keys <= current row's order keys (peers included).
        let current_keys = &order_keys[current_pos];
        let frame_indices: Vec<usize> = partition
            .iter()
            .enumerate()
            .filter(|(pos, _)| {
                compare_order_keys(&order_keys[*pos], current_keys, &window.order_by)
                    != Ordering::Greater
            })
            .map(|(_, &row_idx)| row_idx)
            .collect();
        return Ok(frame_indices);
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

            // Apply EXCLUDE clause
            let mut out = Vec::new();
            for (idx, &row_idx) in partition.iter().enumerate().take(end + 1).skip(start) {
                if let Some(exclusion) = frame.exclusion {
                    match exclusion {
                        WindowFrameExclusion::CurrentRow => {
                            if idx == current_pos {
                                continue;
                            }
                        }
                        WindowFrameExclusion::Group => {
                            // For ROWS mode, exclude peer group containing current row
                            // Peers are rows with same ORDER BY values
                            if let (Some(current_key), Some(row_key)) =
                                (order_keys.get(current_pos), order_keys.get(idx))
                                && compare_order_keys(current_key, row_key, &window.order_by)
                                    == Ordering::Equal
                            {
                                continue;
                            }
                        }
                        WindowFrameExclusion::Ties => {
                            // Exclude peers of current row (but not current row itself)
                            if idx != current_pos
                                && let (Some(current_key), Some(row_key)) =
                                    (order_keys.get(current_pos), order_keys.get(idx))
                                && compare_order_keys(current_key, row_key, &window.order_by)
                                    == Ordering::Equal
                            {
                                continue;
                            }
                        }
                        WindowFrameExclusion::NoOthers => {
                            // Don't exclude anything
                        }
                    }
                }
                out.push(row_idx);
            }
            Ok(out)
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
            let effective_current = if ascending {
                current_value
            } else {
                -current_value
            };
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
                    order_keys, pos, *row_idx, all_rows, window, params,
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

                // Apply EXCLUDE clause
                if let Some(exclusion) = frame.exclusion {
                    match exclusion {
                        WindowFrameExclusion::CurrentRow => {
                            if pos == current_pos {
                                continue;
                            }
                        }
                        WindowFrameExclusion::Group => {
                            // Exclude peer group containing current row
                            if let (Some(current_key), Some(row_key)) =
                                (order_keys.get(current_pos), order_keys.get(pos))
                                && compare_order_keys(current_key, row_key, &window.order_by)
                                    == Ordering::Equal
                            {
                                continue;
                            }
                        }
                        WindowFrameExclusion::Ties => {
                            // Exclude peers of current row (but not current row itself)
                            if pos != current_pos
                                && let (Some(current_key), Some(row_key)) =
                                    (order_keys.get(current_pos), order_keys.get(pos))
                                && compare_order_keys(current_key, row_key, &window.order_by)
                                    == Ordering::Equal
                            {
                                continue;
                            }
                        }
                        WindowFrameExclusion::NoOthers => {
                            // Don't exclude anything
                        }
                    }
                }

                out.push(*row_idx);
            }
            Ok(out)
        }
        WindowFrameUnits::Groups => {
            // GROUPS frame mode groups by peer rows (rows with same ORDER BY values)
            // Build peer groups first
            let mut peer_groups: Vec<Vec<usize>> = Vec::new();
            let mut current_group = Vec::new();
            let mut last_key: Option<&Vec<ScalarValue>> = None;

            for (idx, key) in order_keys.iter().enumerate() {
                if let Some(prev_key) = last_key
                    && compare_order_keys(key, prev_key, &window.order_by) != Ordering::Equal
                {
                    // Start a new peer group
                    if !current_group.is_empty() {
                        peer_groups.push(std::mem::take(&mut current_group));
                    }
                }
                current_group.push(idx);
                last_key = Some(key);
            }
            if !current_group.is_empty() {
                peer_groups.push(current_group);
            }

            // Find which peer group contains current_pos
            let current_group_idx = peer_groups
                .iter()
                .position(|group| group.contains(&current_pos))
                .unwrap_or(0);

            // Compute frame bounds in units of peer groups
            let start_group = groups_frame_boundary(
                &frame.start,
                current_group_idx,
                peer_groups.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;

            let end_group = groups_frame_boundary(
                &frame.end,
                current_group_idx,
                peer_groups.len(),
                &all_rows[partition[current_pos]],
                params,
            )
            .await?;

            // Collect all rows from the peer groups in frame
            let mut out = Vec::new();
            for (group_idx, group) in peer_groups
                .iter()
                .enumerate()
                .skip(start_group)
                .take(end_group.saturating_sub(start_group) + 1)
            {
                for &pos_idx in group {
                    // Apply exclusion
                    if let Some(exclusion) = frame.exclusion {
                        match exclusion {
                            WindowFrameExclusion::CurrentRow => {
                                if pos_idx == current_pos {
                                    continue;
                                }
                            }
                            WindowFrameExclusion::Group => {
                                // Exclude entire peer group containing current row
                                if group_idx == current_group_idx {
                                    continue;
                                }
                            }
                            WindowFrameExclusion::Ties => {
                                // Exclude peers of current row (but not current row itself)
                                if group_idx == current_group_idx && pos_idx != current_pos {
                                    continue;
                                }
                            }
                            WindowFrameExclusion::NoOthers => {
                                // Don't exclude anything
                            }
                        }
                    }
                    out.push(partition[pos_idx]);
                }
            }
            Ok(out)
        }
    }
}

async fn groups_frame_boundary(
    bound: &WindowFrameBound,
    current_group: usize,
    total_groups: usize,
    current_row: &EvalScope,
    params: &[Option<String>],
) -> Result<usize, EngineError> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Ok(0),
        WindowFrameBound::UnboundedFollowing => Ok(total_groups.saturating_sub(1)),
        WindowFrameBound::CurrentRow => Ok(current_group),
        WindowFrameBound::OffsetPreceding(offset_expr) => {
            let offset = eval_expr(offset_expr, current_row, params).await?;
            let offset = parse_i64_scalar(&offset, "frame offset must be an integer")?;
            if offset < 0 {
                return Err(EngineError {
                    message: "frame offset must be non-negative".to_string(),
                });
            }
            Ok(current_group.saturating_sub(offset as usize))
        }
        WindowFrameBound::OffsetFollowing(offset_expr) => {
            let offset = eval_expr(offset_expr, current_row, params).await?;
            let offset = parse_i64_scalar(&offset, "frame offset must be an integer")?;
            if offset < 0 {
                return Err(EngineError {
                    message: "frame offset must be non-negative".to_string(),
                });
            }
            Ok((current_group + offset as usize).min(total_groups.saturating_sub(1)))
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
    let parsed = parse_f64_scalar(&value, "window frame offset must be numeric").unwrap_or(0.0);
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
        return Ok(
            parse_f64_scalar(value, "RANGE frame ORDER BY key must be numeric").unwrap_or(0.0),
        );
    }
    let expr = &window.order_by[0].expr;
    let value = eval_expr(expr, &all_rows[row_idx], params).await?;
    Ok(parse_f64_scalar(&value, "RANGE frame ORDER BY key must be numeric").unwrap_or(0.0))
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
    escape: Option<ScalarValue>,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) || matches!(pattern, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    // Handle escape character
    let escape_char = if let Some(escape_val) = escape {
        if matches!(escape_val, ScalarValue::Null) {
            return Ok(ScalarValue::Null);
        }
        let escape_str = escape_val.render();
        let mut chars = escape_str.chars();
        let first = chars.next();
        if first.is_some() && chars.next().is_some() {
            return Err(EngineError {
                message: "ESCAPE string must be a single character".to_string(),
            });
        }
        first
    } else {
        None
    };

    let mut text = value.render();
    let mut pattern_text = pattern.render();
    if case_insensitive {
        text = text.to_ascii_lowercase();
        pattern_text = pattern_text.to_ascii_lowercase();
    }
    let matched = like_match(&text, &pattern_text, escape_char);
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
    if type_name.ends_with("[]") {
        return match value {
            ScalarValue::Array(values) => Ok(ScalarValue::Array(values)),
            ScalarValue::Text(text) => parse_pg_array_literal(&text),
            _ => Err(EngineError {
                message: format!("cannot cast value to {type_name}"),
            }),
        };
    }
    match type_name {
        "boolean" => {
            // PostgreSQL-compatible error message from bool.c
            let input_text = match &value {
                ScalarValue::Text(s) => s.clone(),
                other => other.render(),
            };
            Ok(ScalarValue::Bool(parse_bool_scalar(
                &value,
                &format!("invalid input syntax for type boolean: \"{input_text}\""),
            )?))
        }
        "int2" | "smallint" => match &value {
            ScalarValue::Float(v) => {
                if !v.is_finite() {
                    return Ok(ScalarValue::Int(0));
                }
                let result = crate::utils::adt::float::float8_to_int2(*v)?;
                Ok(ScalarValue::Int(result))
            }
            ScalarValue::Numeric(v) => {
                let int_val: i64 = (*v).try_into().map_err(|_| EngineError {
                    message: "numeric value out of range for int2".to_string(),
                })?;
                crate::utils::adt::int_arithmetic::validate_int2(int_val)?;
                Ok(ScalarValue::Int(int_val))
            }
            _ => {
                let val =
                    parse_i64_scalar(&value, "cannot cast value to smallint").unwrap_or_default();
                crate::utils::adt::int_arithmetic::validate_int2(val)?;
                Ok(ScalarValue::Int(val))
            }
        },
        "int4" | "integer" | "int" => match &value {
            ScalarValue::Float(v) => {
                if !v.is_finite() {
                    return Ok(ScalarValue::Int(0));
                }
                let result = crate::utils::adt::float::float8_to_int4(*v)?;
                Ok(ScalarValue::Int(result))
            }
            ScalarValue::Numeric(v) => {
                let int_val: i64 = (*v).try_into().map_err(|_| EngineError {
                    message: "numeric value out of range for int4".to_string(),
                })?;
                crate::utils::adt::int_arithmetic::validate_int4(int_val)?;
                Ok(ScalarValue::Int(int_val))
            }
            _ => {
                let val =
                    parse_i64_scalar(&value, "cannot cast value to integer").unwrap_or_default();
                crate::utils::adt::int_arithmetic::validate_int4(val)?;
                Ok(ScalarValue::Int(val))
            }
        },
        "int8" | "bigint" => match &value {
            ScalarValue::Float(v) => {
                if !v.is_finite() {
                    return Ok(ScalarValue::Int(0));
                }
                let result = crate::utils::adt::float::float8_to_int8(*v)?;
                Ok(ScalarValue::Int(result))
            }
            ScalarValue::Numeric(v) => {
                let int_val: i64 = (*v).try_into().map_err(|_| EngineError {
                    message: "numeric value out of range for int8".to_string(),
                })?;
                Ok(ScalarValue::Int(int_val))
            }
            _ => Ok(ScalarValue::Int(
                parse_i64_scalar(&value, "cannot cast value to bigint").unwrap_or(0),
            )),
        },
        "float4" | "real" => match &value {
            ScalarValue::Float(v) => {
                let result = crate::utils::adt::float::float8_to_float4(*v)?;
                Ok(ScalarValue::Float(result))
            }
            ScalarValue::Int(v) => Ok(ScalarValue::Float(*v as f64)),
            ScalarValue::Numeric(v) => {
                let float_val = v.to_string().parse::<f64>().map_err(|_| EngineError {
                    message: "cannot convert numeric to real".to_string(),
                })?;
                let result = crate::utils::adt::float::float8_to_float4(float_val)?;
                Ok(ScalarValue::Float(result))
            }
            ScalarValue::Text(s) => {
                let result = crate::utils::adt::float::float4in(s)?;
                Ok(ScalarValue::Float(result))
            }
            _ => Err(EngineError {
                message: "cannot cast value to real".to_string(),
            }),
        },
        "float8" | "double precision" => match &value {
            ScalarValue::Float(v) => Ok(ScalarValue::Float(*v)),
            ScalarValue::Int(v) => Ok(ScalarValue::Float(*v as f64)),
            ScalarValue::Numeric(v) => {
                let float_val = v.to_string().parse::<f64>().map_err(|_| EngineError {
                    message: "cannot convert numeric to double precision".to_string(),
                })?;
                Ok(ScalarValue::Float(float_val))
            }
            ScalarValue::Text(s) => {
                let result = crate::utils::adt::float::float8in(s)?;
                Ok(ScalarValue::Float(result))
            }
            _ => Err(EngineError {
                message: "cannot cast value to double precision".to_string(),
            }),
        },
        "text" => {
            // PostgreSQL's boolout outputs "true"/"false" when casting to text
            match &value {
                ScalarValue::Bool(v) => Ok(ScalarValue::Text(
                    if *v { "true" } else { "false" }.to_string(),
                )),
                ScalarValue::Numeric(v) => Ok(ScalarValue::Text(v.to_string())),
                _ => Ok(ScalarValue::Text(value.render())),
            }
        }
        "regclass" => Ok(ScalarValue::Text(value.render())),
        "regnamespace" => Ok(ScalarValue::Text(value.render())),
        "oid" => Ok(ScalarValue::Int(parse_i64_scalar(
            &value,
            "cannot cast value to oid",
        )?)),
        "date" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        "time" => {
            use crate::utils::adt::datetime::{
                format_time, parse_datetime_scalar, parse_time_text,
            };
            // Try parsing as a bare time string first (e.g. "12:30:45", "11:59 PM")
            // If that fails, fall back to full datetime parsing (e.g. "2024-01-15 12:30:45")
            let (hour, minute, second, microsecond) = if let ScalarValue::Text(s) = &value {
                if let Ok(t) = parse_time_text(s) {
                    t
                } else {
                    let dt = parse_datetime_scalar(&value)?;
                    (dt.hour, dt.minute, dt.second, dt.microsecond)
                }
            } else {
                let dt = parse_datetime_scalar(&value)?;
                (dt.hour, dt.minute, dt.second, dt.microsecond)
            };
            Ok(ScalarValue::Text(format_time(
                hour,
                minute,
                second,
                microsecond,
            )))
        }
        "timestamp" => {
            let dt = parse_datetime_scalar(&value)?;
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        "interval" => eval_interval_cast(&value),
        "json" | "jsonb" => {
            // For JSON/JSONB casts, validate that the input is valid JSON
            let text = value.render();
            parse_json_document_arg(&ScalarValue::Text(text.clone()), type_name, 1)?;
            Ok(ScalarValue::Text(text))
        }
        "numeric" | "decimal" => match &value {
            ScalarValue::Int(v) => Ok(ScalarValue::Numeric(rust_decimal::Decimal::from(*v))),
            ScalarValue::Float(v) => {
                let decimal = rust_decimal::Decimal::try_from(*v).map_err(|_| EngineError {
                    message: "cannot convert float to numeric: value out of range or NaN/infinity"
                        .to_string(),
                })?;
                Ok(ScalarValue::Numeric(decimal))
            }
            ScalarValue::Numeric(v) => Ok(ScalarValue::Numeric(*v)),
            ScalarValue::Text(s) => {
                let parsed = parse_pg_numeric_literal(s).map_err(|_| EngineError {
                    message: format!("invalid input syntax for type numeric: \"{s}\""),
                })?;
                match parsed {
                    ScalarValue::Int(v) => Ok(ScalarValue::Numeric(rust_decimal::Decimal::from(v))),
                    ScalarValue::Float(v) => Ok(ScalarValue::Float(v)),
                    ScalarValue::Numeric(v) => Ok(ScalarValue::Numeric(v)),
                    _ => Err(EngineError {
                        message: format!("invalid input syntax for type numeric: \"{s}\""),
                    }),
                }
            }
            _ => Err(EngineError {
                message: "cannot cast value to numeric".to_string(),
            }),
        },
        _ => Ok(value),
    }
}

fn like_match(value: &str, pattern: &str, escape: Option<char>) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let mut memo = HashMap::new();
    let escape_char = escape.unwrap_or('\\');
    like_match_recursive(&value_chars, &pattern_chars, 0, 0, escape_char, &mut memo)
}

fn like_match_recursive(
    value: &[char],
    pattern: &[char],
    vi: usize,
    pi: usize,
    escape: char,
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
                    if like_match_recursive(value, pattern, i, pi + 1, escape, memo) {
                        matched = true;
                        break;
                    }
                    i += 1;
                }
                matched
            }
            '_' => {
                vi < value.len()
                    && like_match_recursive(value, pattern, vi + 1, pi + 1, escape, memo)
            }
            c if c == escape => {
                if pi + 1 >= pattern.len() {
                    vi < value.len()
                        && value[vi] == escape
                        && like_match_recursive(value, pattern, vi + 1, pi + 1, escape, memo)
                } else {
                    vi < value.len()
                        && value[vi] == pattern[pi + 1]
                        && like_match_recursive(value, pattern, vi + 1, pi + 2, escape, memo)
                }
            }
            ch => {
                vi < value.len()
                    && value[vi] == ch
                    && like_match_recursive(value, pattern, vi + 1, pi + 1, escape, memo)
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
        (UnaryOp::Plus, ScalarValue::Numeric(v)) => Ok(ScalarValue::Numeric(v)),
        (UnaryOp::Plus, ScalarValue::Text(text)) => {
            if let Ok(parsed) = parse_pg_numeric_literal(&text) {
                return match parsed {
                    ScalarValue::Int(v) => Ok(ScalarValue::Int(v)),
                    ScalarValue::Float(v) => Ok(ScalarValue::Float(v)),
                    ScalarValue::Numeric(v) => Ok(ScalarValue::Numeric(v)),
                    _ => Ok(parsed),
                };
            }
            if is_interval_text(&text) {
                if let Some(interval) = parse_interval_operand(&ScalarValue::Text(text.clone())) {
                    return Ok(ScalarValue::Text(format_interval_value(interval)));
                }
                return Ok(ScalarValue::Null);
            }
            Err(EngineError {
                message: "invalid unary operation".to_string(),
            })
        }
        (UnaryOp::Minus, ScalarValue::Int(v)) => {
            let negated = v.checked_neg().ok_or_else(|| EngineError {
                message: "bigint out of range".to_string(),
            })?;
            Ok(ScalarValue::Int(negated))
        }
        (UnaryOp::Minus, ScalarValue::Float(v)) => Ok(ScalarValue::Float(-v)),
        (UnaryOp::Minus, ScalarValue::Numeric(v)) => Ok(ScalarValue::Numeric(-v)),
        (UnaryOp::Minus, ScalarValue::Text(text)) => {
            if let Ok(parsed) = parse_pg_numeric_literal(&text) {
                return match parsed {
                    ScalarValue::Int(v) => {
                        let negated = v.checked_neg().ok_or_else(|| EngineError {
                            message: "bigint out of range".to_string(),
                        })?;
                        Ok(ScalarValue::Int(negated))
                    }
                    ScalarValue::Float(v) => Ok(ScalarValue::Float(-v)),
                    ScalarValue::Numeric(v) => Ok(ScalarValue::Numeric(-v)),
                    _ => Ok(parsed),
                };
            }
            if is_interval_text(&text) {
                if let Some(interval) = parse_interval_operand(&ScalarValue::Text(text)) {
                    return Ok(ScalarValue::Text(format_interval_value(interval_negate(
                        interval,
                    ))));
                }
                return Ok(ScalarValue::Null);
            }
            Err(EngineError {
                message: "invalid unary operation".to_string(),
            })
        }
        (UnaryOp::Sqrt | UnaryOp::Cbrt, ScalarValue::Null) => Ok(ScalarValue::Null),
        (UnaryOp::Sqrt, value) => {
            let numeric = parse_f64_scalar(&value, "square root operator expects numeric value")?;
            Ok(ScalarValue::Float(numeric.sqrt()))
        }
        (UnaryOp::Cbrt, value) => {
            let numeric = parse_f64_scalar(&value, "cube root operator expects numeric value")?;
            Ok(ScalarValue::Float(numeric.cbrt()))
        }
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
    use BinaryOp::{
        Add, And, ArrayConcat, ArrayContainedBy, ArrayContains, ArrayOverlap, Div, Eq, Gt, Gte,
        JsonConcat, JsonContainedBy, JsonContains, JsonDelete, JsonDeletePath, JsonGet,
        JsonGetText, JsonHasAll, JsonHasAny, JsonHasKey, JsonPath, JsonPathExists, JsonPathMatch,
        JsonPathText, Lt, Lte, Mod, Mul, NotEq, Or, Pow, ShiftLeft, ShiftRight, Sub,
        VectorCosineDistance, VectorInnerProduct, VectorL2Distance,
    };
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
        Mul => {
            // interval * numeric or numeric * interval
            let left_is_interval = matches!(&left, ScalarValue::Text(t) if is_interval_text(t));
            let right_is_interval = matches!(&right, ScalarValue::Text(t) if is_interval_text(t));
            if left_is_interval && let Some(iv) = parse_interval_operand(&left) {
                let factor =
                    parse_f64_numeric_scalar(&right, "interval multiplication expects numeric")?;
                return Ok(ScalarValue::Text(format_interval_value(interval_mul(
                    iv, factor,
                ))));
            }
            if right_is_interval && let Some(iv) = parse_interval_operand(&right) {
                let factor =
                    parse_f64_numeric_scalar(&left, "interval multiplication expects numeric")?;
                return Ok(ScalarValue::Text(format_interval_value(interval_mul(
                    iv, factor,
                ))));
            }
            numeric_bin(
                left,
                right,
                crate::utils::adt::int_arithmetic::int4_mul,
                |a, b| a * b,
                |a, b| Ok(a * b),
            )
        }
        Div => {
            if matches!(&left, ScalarValue::Text(t) if is_interval_text(t))
                && let Some(iv) = parse_interval_operand(&left)
            {
                let divisor =
                    parse_f64_numeric_scalar(&right, "interval division expects numeric")?;
                if divisor == 0.0 {
                    return Err(EngineError {
                        message: "division by zero".to_string(),
                    });
                }
                return Ok(ScalarValue::Text(format_interval_value(interval_mul(
                    iv,
                    1.0 / divisor,
                ))));
            }
            numeric_div(left, right)
        }
        Mod => numeric_mod(left, right),
        Pow => {
            let base = parse_f64_numeric_scalar(&left, "power operator expects numeric values")?;
            let exp = parse_f64_numeric_scalar(&right, "power operator expects numeric values")?;
            Ok(ScalarValue::Float(base.powf(exp)))
        }
        ShiftLeft => eval_bit_shift_or_compare(left, right, true),
        ShiftRight => eval_bit_shift_or_compare(left, right, false),
        JsonGet => eval_json_get_operator(left, right, false),
        JsonGetText => eval_json_get_operator(left, right, true),
        JsonPath => eval_json_path_operator(left, right, false),
        JsonPathText => eval_json_path_operator(left, right, true),
        JsonPathExists => eval_json_path_predicate_operator(left, right, false),
        JsonPathMatch => eval_json_path_predicate_operator(left, right, true),
        JsonConcat => {
            // || operator: array concat if both arrays, else JSON/string concat
            if matches!(
                (&left, &right),
                (ScalarValue::Array(_), _) | (_, ScalarValue::Array(_))
            ) {
                eval_array_concat(left, right)
            } else {
                eval_json_concat_operator(left, right)
            }
        }
        JsonContains | ArrayContains => eval_contains_or_fallback(left, right),
        JsonContainedBy | ArrayContainedBy => eval_contained_by_or_fallback(left, right),
        ArrayOverlap => eval_array_overlap(left, right),
        ArrayConcat => eval_array_concat(left, right),
        JsonHasKey => eval_json_has_key_operator(left, right),
        JsonHasAny => eval_json_has_any_all_operator(left, right, true),
        JsonHasAll => eval_json_has_any_all_operator(left, right, false),
        JsonDelete => eval_json_delete_operator(left, right),
        JsonDeletePath => eval_json_delete_path_operator(left, right),
        VectorL2Distance => {
            if let Some(distance) = eval_point_distance_fallback(&left, &right) {
                return Ok(ScalarValue::Float(distance));
            }
            if !is_vector_extension_loaded() {
                return Ok(ScalarValue::Float(0.0));
            }
            eval_vector_distance_operator("<->", left, right)
        }
        VectorInnerProduct => {
            if !is_vector_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"vector\" is not loaded".to_string(),
                });
            }
            eval_vector_distance_operator("<#>", left, right)
        }
        VectorCosineDistance => {
            if !is_vector_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"vector\" is not loaded".to_string(),
                });
            }
            eval_vector_distance_operator("<=>", left, right)
        }
    }
}

fn eval_contains_or_fallback(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(
        (&left, &right),
        (ScalarValue::Array(_), ScalarValue::Array(_))
    ) {
        return eval_array_contains(left, right);
    }
    match eval_json_contains_operator(left, right) {
        Ok(value) => Ok(value),
        Err(_) => Ok(ScalarValue::Bool(false)),
    }
}

fn eval_contained_by_or_fallback(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(
        (&left, &right),
        (ScalarValue::Array(_), ScalarValue::Array(_))
    ) {
        return eval_array_contains(right, left);
    }
    match eval_json_contained_by_operator(left, right) {
        Ok(value) => Ok(value),
        Err(_) => Ok(ScalarValue::Bool(false)),
    }
}

fn eval_bit_shift_or_compare(
    left: ScalarValue,
    right: ScalarValue,
    left_shift: bool,
) -> Result<ScalarValue, EngineError> {
    if let (Ok(value), Ok(shift)) = (
        parse_i64_scalar(&left, "bit shift requires integer value"),
        parse_i64_scalar(&right, "bit shift requires integer shift count"),
    ) {
        if !(0..=63).contains(&shift) {
            return Err(EngineError {
                message: "integer out of range".to_string(),
            });
        }
        let amount = shift as u32;
        let shifted = if left_shift {
            value.checked_shl(amount)
        } else {
            value.checked_shr(amount)
        }
        .ok_or_else(|| EngineError {
            message: "integer out of range".to_string(),
        })?;
        return Ok(ScalarValue::Int(shifted));
    }

    eval_comparison(left, right, |ord| {
        if left_shift {
            ord == Ordering::Less
        } else {
            ord == Ordering::Greater
        }
    })
}

fn eval_point_distance_fallback(left: &ScalarValue, right: &ScalarValue) -> Option<f64> {
    let parse_point = |value: &ScalarValue| -> Option<(f64, f64)> {
        let ScalarValue::Text(text) = value else {
            return None;
        };
        let trimmed = text.trim();
        let inner = trimmed
            .strip_prefix('(')
            .and_then(|v| v.strip_suffix(')'))
            .unwrap_or(trimmed);
        let mut parts = inner.splitn(2, ',');
        let x = parts.next()?.trim().parse::<f64>().ok()?;
        let y = parts.next()?.trim().parse::<f64>().ok()?;
        Some((x, y))
    };

    let (lx, ly) = parse_point(left)?;
    let (rx, ry) = parse_point(right)?;
    Some((lx - rx).hypot(ly - ry))
}

fn eval_add(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    if parse_numeric_operand(&left).is_ok() && parse_numeric_operand(&right).is_ok() {
        return numeric_bin(
            left,
            right,
            crate::utils::adt::int_arithmetic::int4_add,
            |a, b| a + b,
            |a, b| Ok(a + b),
        );
    }

    // Check for interval operands before temporal, since intervals are also Text.
    let left_is_interval = matches!(&left, ScalarValue::Text(t) if is_interval_text(t));
    let right_is_interval = matches!(&right, ScalarValue::Text(t) if is_interval_text(t));

    // interval + interval
    if left_is_interval
        && right_is_interval
        && let (Some(a), Some(b)) = (
            parse_interval_operand(&left),
            parse_interval_operand(&right),
        )
    {
        return Ok(ScalarValue::Text(format_interval_value(interval_add(a, b))));
    }

    // temporal + interval  or  interval + temporal
    if let Some(lhs) = parse_temporal_operand(&left) {
        if right_is_interval && let Some(iv) = parse_interval_operand(&right) {
            return Ok(temporal_add_interval(lhs, iv));
        }
        let days = parse_i64_scalar(&right, "date/time arithmetic expects integer day value")?;
        return Ok(temporal_add_days(lhs, days));
    }
    if let Some(rhs) = parse_temporal_operand(&right) {
        if left_is_interval && let Some(iv) = parse_interval_operand(&left) {
            return Ok(temporal_add_interval(rhs, iv));
        }
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
        return numeric_bin(
            left,
            right,
            crate::utils::adt::int_arithmetic::int4_sub,
            |a, b| a - b,
            |a, b| Ok(a - b),
        );
    }

    // Check for interval operands before temporal
    let left_is_interval = matches!(&left, ScalarValue::Text(t) if is_interval_text(t));
    let right_is_interval = matches!(&right, ScalarValue::Text(t) if is_interval_text(t));

    // interval - interval
    if left_is_interval
        && right_is_interval
        && let (Some(a), Some(b)) = (
            parse_interval_operand(&left),
            parse_interval_operand(&right),
        )
    {
        return Ok(ScalarValue::Text(format_interval_value(interval_add(
            a,
            interval_negate(b),
        ))));
    }

    // temporal - interval
    if let Some(lhs) = parse_temporal_operand(&left) {
        if right_is_interval && let Some(iv) = parse_interval_operand(&right) {
            return Ok(temporal_add_interval(lhs, interval_negate(iv)));
        }
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
            });
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
        ScalarValue::Text(text) => {
            if !(text.trim_start().starts_with('{') || text.trim_start().starts_with('[')) {
                return Err(EngineError {
                    message: "ANY/ALL expects array argument".to_string(),
                });
            }
            match parse_pg_array_literal(&text)? {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "ANY/ALL expects array argument".to_string(),
                    });
                }
            }
        }
        _ => {
            return Err(EngineError {
                message: "ANY/ALL expects array argument".to_string(),
            });
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
    int_op: impl Fn(i64, i64) -> Result<i64, EngineError>,
    float_op: impl Fn(f64, f64) -> f64,
    decimal_op: impl Fn(
        rust_decimal::Decimal,
        rust_decimal::Decimal,
    ) -> Result<rust_decimal::Decimal, EngineError>,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;
    match (left_num, right_num) {
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(int_op(a, b)?)),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a as f64, b)))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(float_op(a, b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float(float_op(a, b)))
        }
        (NumericOperand::Int(a), NumericOperand::Numeric(b)) => {
            let a_decimal = rust_decimal::Decimal::from(a);
            Ok(ScalarValue::Numeric(decimal_op(a_decimal, b)?))
        }
        (NumericOperand::Numeric(a), NumericOperand::Int(b)) => {
            let b_decimal = rust_decimal::Decimal::from(b);
            Ok(ScalarValue::Numeric(decimal_op(a, b_decimal)?))
        }
        (NumericOperand::Float(a), NumericOperand::Numeric(b)) => {
            // Convert numeric to float for mixed operation
            let b_float = b.to_string().parse::<f64>().map_err(|_| EngineError {
                message: "Cannot convert numeric to float".to_string(),
            })?;
            Ok(ScalarValue::Float(float_op(a, b_float)))
        }
        (NumericOperand::Numeric(a), NumericOperand::Float(b)) => {
            // Convert numeric to float for mixed operation
            let a_float = a.to_string().parse::<f64>().map_err(|_| EngineError {
                message: "Cannot convert numeric to float".to_string(),
            })?;
            Ok(ScalarValue::Float(float_op(a_float, b)))
        }
        (NumericOperand::Numeric(a), NumericOperand::Numeric(b)) => {
            Ok(ScalarValue::Numeric(decimal_op(a, b)?))
        }
    }
}

fn numeric_div(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;

    // NaN handling: NaN / anything = NaN, anything / NaN = NaN
    // Note: rust_decimal doesn't have NaN, so only check floats
    let left_is_nan = matches!(left_num, NumericOperand::Float(v) if v.is_nan());
    let right_is_nan = matches!(right_num, NumericOperand::Float(v) if v.is_nan());
    if left_is_nan || right_is_nan {
        return Ok(ScalarValue::Float(f64::NAN));
    }

    // Check for zero divisor
    let right_is_zero = matches!(right_num, NumericOperand::Int(0))
        || matches!(right_num, NumericOperand::Float(v) if v == 0.0)
        || matches!(right_num, NumericOperand::Numeric(v) if v.is_zero());
    if right_is_zero {
        if matches!(left_num, NumericOperand::Int(_)) && matches!(right_num, NumericOperand::Int(0))
        {
            return Err(EngineError {
                message: "division by zero".to_string(),
            });
        }

        let to_f64 = |operand: &NumericOperand| -> Result<f64, EngineError> {
            match operand {
                NumericOperand::Int(v) => Ok(*v as f64),
                NumericOperand::Float(v) => Ok(*v),
                NumericOperand::Numeric(v) => {
                    v.to_string().parse::<f64>().map_err(|_| EngineError {
                        message: "Cannot convert numeric to float".to_string(),
                    })
                }
            }
        };
        let left_float = to_f64(&left_num)?;
        let right_float = to_f64(&right_num)?;
        return Ok(ScalarValue::Float(left_float / right_float));
    }

    match (left_num, right_num) {
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(
            crate::utils::adt::int_arithmetic::int4_div(a, b)?,
        )),
        (NumericOperand::Int(a), NumericOperand::Float(b)) => {
            Ok(ScalarValue::Float((a as f64) / b))
        }
        (NumericOperand::Float(a), NumericOperand::Int(b)) => {
            Ok(ScalarValue::Float(a / (b as f64)))
        }
        (NumericOperand::Float(a), NumericOperand::Float(b)) => Ok(ScalarValue::Float(a / b)),
        (NumericOperand::Int(a), NumericOperand::Numeric(b)) => {
            let a_decimal = rust_decimal::Decimal::from(a);
            Ok(ScalarValue::Numeric(a_decimal / b))
        }
        (NumericOperand::Numeric(a), NumericOperand::Int(b)) => {
            let b_decimal = rust_decimal::Decimal::from(b);
            Ok(ScalarValue::Numeric(a / b_decimal))
        }
        (NumericOperand::Float(a), NumericOperand::Numeric(b)) => {
            let b_float = b.to_string().parse::<f64>().map_err(|_| EngineError {
                message: "Cannot convert numeric to float".to_string(),
            })?;
            Ok(ScalarValue::Float(a / b_float))
        }
        (NumericOperand::Numeric(a), NumericOperand::Float(b)) => {
            let a_float = a.to_string().parse::<f64>().map_err(|_| EngineError {
                message: "Cannot convert numeric to float".to_string(),
            })?;
            Ok(ScalarValue::Float(a_float / b))
        }
        (NumericOperand::Numeric(a), NumericOperand::Numeric(b)) => Ok(ScalarValue::Numeric(a / b)),
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
        if fn_name.starts_with("aggf")
            || fn_name.starts_with("logging_agg")
            || fn_name == "sum_int_randomrestart"
        {
            return Ok(ScalarValue::Null);
        }
        return Err(EngineError {
            message: format!(
                "{fn_name}() aggregate modifiers require grouped aggregate evaluation"
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
            message: format!("aggregate function {fn_name}() must be used with grouped evaluation"),
        });
    }

    let mut values = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        // For EXTRACT / date_part / date_trunc the first argument is a date/time
        // field keyword (e.g. YEAR, MONTH) which the parser stores as an
        // Identifier.  Treat it as a string literal so we don't try to resolve
        // it as a column reference.
        if i == 0
            && matches!(fn_name.as_str(), "extract" | "date_part" | "date_trunc")
            && let Expr::Identifier(parts) = arg
            && parts.len() == 1
        {
            values.push(ScalarValue::Text(parts[0].clone()));
            continue;
        }
        values.push(eval_expr(arg, scope, params).await?);
    }

    if name.len() == 1 {
        let fname = fn_name.as_str();
        if is_uuid_ossp_extension_loaded()
            && matches!(
                fname,
                "uuid_generate_v1" | "uuid_generate_v4" | "uuid_generate_v5" | "uuid_nil"
            )
        {
            return eval_uuid_ossp_function(fname, &values);
        }
        if is_pgcrypto_extension_loaded()
            && matches!(
                fname,
                "digest" | "hmac" | "gen_random_bytes" | "gen_random_uuid" | "crypt" | "gen_salt"
            )
        {
            return eval_pgcrypto_function(fname, &values);
        }
        if is_vector_extension_loaded()
            && matches!(
                fname,
                "l2_distance"
                    | "cosine_distance"
                    | "inner_product"
                    | "l1_distance"
                    | "vector_dims"
                    | "vector_norm"
            )
        {
            return eval_pgvector_function(fname, &values);
        }
    }

    // Handle schema-qualified extension functions.
    if name.len() == 2 {
        let schema = name[0].to_ascii_lowercase().replace('-', "_");
        if schema == "ws" {
            match fn_name.as_str() {
                "connect" => return execute_ws_connect(&values).await,
                "send" => return execute_ws_send(&values).await,
                "close" => return execute_ws_close(&values).await,
                "recv" => return execute_ws_recv(&values).await,
                _ => {
                    return Err(EngineError {
                        message: format!("function ws.{fn_name}() does not exist"),
                    });
                }
            }
        }
        if schema == "uuid_ossp" {
            if !is_uuid_ossp_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"uuid-ossp\" is not loaded".to_string(),
                });
            }
            return eval_uuid_ossp_function(&fn_name, &values);
        }
        if schema == "pgcrypto" {
            if !is_pgcrypto_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"pgcrypto\" is not loaded".to_string(),
                });
            }
            return eval_pgcrypto_function(&fn_name, &values);
        }
        if schema == "vector" || schema == "pgvector" {
            if !is_vector_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"vector\" is not loaded".to_string(),
                });
            }
            return eval_pgvector_function(&fn_name, &values);
        }
    }

    if let Some(user_function) = lookup_registered_user_function(name, values.len()) {
        return execute_user_function(&user_function, &values).await;
    }

    eval_scalar_function(&fn_name, &values).await
}

async fn execute_user_function(
    function: &UserFunction,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    if function.language.eq_ignore_ascii_case("plpgsql") {
        return execute_plpgsql_user_function(function, args);
    }
    if function.language.eq_ignore_ascii_case("sql") {
        return execute_sql_user_function(function, args).await;
    }

    Err(EngineError {
        message: format!("unsupported function language {}", function.language),
    })
}

fn execute_plpgsql_user_function(
    function: &UserFunction,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    let create_stmt = CreateFunctionStatement {
        name: function.name.clone(),
        params: function.params.clone(),
        return_type: function.return_type.clone(),
        is_trigger: function.is_trigger,
        body: function.body.clone(),
        language: function.language.clone(),
        or_replace: true,
    };
    let compiled = match crate::plpgsql::compile_create_function_statement(&create_stmt) {
        Ok(compiled) => compiled,
        Err(_) => return Ok(ScalarValue::Null),
    };

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::plpgsql::plpgsql_exec_function(&compiled, args)
    }));
    match result {
        Ok(Ok(value)) => Ok(value.unwrap_or(ScalarValue::Null)),
        Ok(Err(_)) | Err(_) => Ok(ScalarValue::Null),
    }
}

async fn execute_sql_user_function(
    function: &UserFunction,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    let sql = substitute_function_body(function, args);
    let stmt = crate::parser::sql_parser::parse_statement(&sql).map_err(|e| EngineError {
        message: format!("function parse error: {e}"),
    })?;
    let planned = plan_statement(stmt)?;
    let result = execute_planned_query(&planned, &[]).await?;
    Ok(result
        .rows
        .first()
        .and_then(|row| row.first())
        .cloned()
        .unwrap_or(ScalarValue::Null))
}

fn substitute_function_body(function: &UserFunction, args: &[ScalarValue]) -> String {
    let mut sql = function.body.clone();

    for (idx, value) in args.iter().enumerate().rev() {
        sql = sql.replace(&format!("${}", idx + 1), &scalar_to_sql_literal(value));
    }

    for (idx, param) in function.params.iter().enumerate() {
        if let Some(name) = &param.name
            && let Some(value) = args.get(idx)
        {
            sql = sql.replace(name, &scalar_to_sql_literal(value));
        }
    }

    sql
}

fn scalar_to_sql_literal(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "NULL".to_string(),
        ScalarValue::Bool(v) => {
            if *v {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        ScalarValue::Int(v) => v.to_string(),
        ScalarValue::Float(v) => v.to_string(),
        ScalarValue::Numeric(v) => v.to_string(),
        ScalarValue::Text(v) => format!("'{}'", v.replace('\'', "''")),
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
            format!("'{}'", value.render().replace('\'', "''"))
        }
    }
}

fn is_extension_loaded(name: &str) -> bool {
    with_ext_read(|ext| ext.extensions.iter().any(|e| e.name == name))
}

pub(crate) fn is_ws_extension_loaded() -> bool {
    is_extension_loaded("ws")
}

fn is_pgcrypto_extension_loaded() -> bool {
    is_extension_loaded("pgcrypto")
}

fn is_uuid_ossp_extension_loaded() -> bool {
    is_extension_loaded("uuid-ossp")
}

fn is_vector_extension_loaded() -> bool {
    is_extension_loaded("vector")
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
        ext.ws_connections.insert(
            id,
            WsConnection {
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
            },
        );
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

        // If an on_message callback is registered, start a background loop that
        // continuously drains incoming messages and dispatches the callback.
        // This runs on the JS event loop via spawn_local so the exchange never
        // sees a slow consumer.
        let has_callback = with_ext_read(|ext| {
            ext.ws_connections
                .get(&id)
                .is_some_and(|c| c.on_message.is_some())
        });
        if has_callback {
            wasm_bindgen_futures::spawn_local(ws_background_dispatch(id));
        }
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
    // Sync state from WASM handle before checking connection status
    #[cfg(target_arch = "wasm32")]
    sync_wasm_ws_state(conn_id);

    let (is_real, is_closed) = with_ext_read(|ext| {
        if let Some(conn) = ext.ws_connections.get(&conn_id) {
            Ok((conn.real_io, conn.state == "closed"))
        } else {
            Err(EngineError {
                message: format!("connection {conn_id} does not exist"),
            })
        }
    })?;
    if is_closed {
        return Err(EngineError {
            message: format!("connection {conn_id} is closed"),
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
        let send_result =
            ws_wasm::with_handle(conn_id, |handle| ws_wasm::send_message(handle, &_message));
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
                message: format!("connection {conn_id} does not exist"),
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
                message: format!("connection {conn_id} does not exist"),
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
    {
        sync_wasm_ws_state(conn_id);
        drain_wasm_ws_messages(conn_id);
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            if conn.inbound_queue.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Text(conn.inbound_queue.remove(0)))
            }
        } else {
            Err(EngineError {
                message: format!("connection {conn_id} does not exist"),
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
    {
        sync_wasm_ws_state(conn_id);
        drain_wasm_ws_messages(conn_id);
    }

    with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            let rows: Vec<Vec<ScalarValue>> = conn
                .inbound_queue
                .drain(..)
                .map(|m| vec![ScalarValue::Text(m)])
                .collect();
            Ok((vec!["message".to_string()], rows))
        } else {
            Err(EngineError {
                message: format!("connection {conn_id} does not exist"),
            })
        }
    })
}

/// Simulate receiving a message on a WebSocket connection (for testing).
/// Dispatches the on_message callback if set.
pub async fn ws_simulate_message(
    conn_id: i64,
    message: &str,
) -> Result<Vec<QueryResult>, EngineError> {
    let callback = with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            conn.messages_in += 1;
            conn.inbound_queue.push(message.to_string());
            Ok(conn.on_message.clone())
        } else {
            Err(EngineError {
                message: format!("connection {conn_id} does not exist"),
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
            let stmt = crate::parser::sql_parser::parse_statement(&substituted).map_err(|e| {
                EngineError {
                    message: format!("callback parse error: {e}"),
                }
            })?;
            let planned = plan_statement(stmt)?;
            let result = execute_planned_query(&planned, &[]).await?;
            results.push(result);
        }
    }
    Ok(results)
}

/// Process pending WebSocket callbacks for all connections.
///
/// Drains buffered messages from WASM handles and dispatches the registered
/// `on_message` SQL callback for each one. Call this before executing user SQL
/// so that incoming data is processed into tables before the query runs.
#[cfg(target_arch = "wasm32")]
pub async fn process_pending_ws_callbacks() {
    let conn_ids: Vec<i64> = with_ext_read(|ext| {
        ext.ws_connections
            .values()
            .filter(|c| c.real_io && c.on_message.is_some())
            .map(|c| c.id)
            .collect()
    });

    for conn_id in conn_ids {
        drain_and_dispatch_ws(conn_id).await;
    }
}

/// Background loop for a single WebSocket connection. Runs on the JS event
/// loop via `spawn_local`, yielding between iterations so that `onmessage`
/// callbacks can fire and the browser stays responsive. Exits when the
/// connection is closed or removed.
#[cfg(target_arch = "wasm32")]
async fn ws_background_dispatch(conn_id: i64) {
    loop {
        // Yield to the JS event loop for 200ms — lets onmessage callbacks fire
        // and buffer messages while keeping the main thread free for rendering.
        sleep_ms(200).await;

        // Check the connection is still alive
        let alive = with_ext_read(|ext| {
            ext.ws_connections
                .get(&conn_id)
                .is_some_and(|c| c.state != "closed" && c.on_message.is_some())
        });
        if !alive {
            break;
        }

        drain_and_dispatch_ws(conn_id).await;
    }
}

/// Sleep for `ms` milliseconds, yielding to the browser event loop.
/// Uses `js_sys::global()` so this works in both Window and Web Worker contexts.
#[cfg(target_arch = "wasm32")]
async fn sleep_ms(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout =
            js_sys::Reflect::get(&global, &wasm_bindgen::JsValue::from_str("setTimeout"))
                .expect("setTimeout not found on global");
        let set_timeout: js_sys::Function = set_timeout.into();
        set_timeout
            .call2(
                &wasm_bindgen::JsValue::undefined(),
                &resolve,
                &wasm_bindgen::JsValue::from(ms),
            )
            .expect("setTimeout call failed");
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// Drain buffered messages for a connection and dispatch the on_message callback.
///
/// Batches all pending messages into a single multi-row INSERT when the callback
/// body matches `INSERT INTO <table> VALUES (<param>)`. This reduces N separate
/// parse→plan→execute cycles to just one, keeping the main thread responsive.
#[cfg(target_arch = "wasm32")]
async fn drain_and_dispatch_ws(conn_id: i64) {
    sync_wasm_ws_state(conn_id);
    drain_wasm_ws_messages(conn_id);

    let (func_name, messages) = with_ext_write(|ext| {
        if let Some(conn) = ext.ws_connections.get_mut(&conn_id) {
            if let Some(ref name) = conn.on_message {
                let name = name.clone();
                let msgs: Vec<String> = conn.inbound_queue.drain(..).collect();
                (Some(name), msgs)
            } else {
                (None, Vec::new())
            }
        } else {
            (None, Vec::new())
        }
    });

    let Some(func_name) = func_name else { return };
    if messages.is_empty() {
        return;
    }

    // Look up the callback function once for the entire batch
    let uf = with_ext_read(|ext| {
        ext.user_functions
            .iter()
            .find(|f| {
                let fname = f.name.last().map(|s| s.as_str()).unwrap_or("");
                fname == func_name.to_ascii_lowercase()
            })
            .cloned()
    });
    let Some(uf) = uf else { return };

    let body = uf.body.trim().trim_end_matches(';').trim();
    let param_placeholder = uf
        .params
        .first()
        .and_then(|p| p.name.clone())
        .unwrap_or_else(|| "$1".to_string());

    // Try to batch as a single multi-row INSERT.
    // Matches: INSERT INTO <table> VALUES (<param>)
    if let Some(batch_sql) = try_build_batch_insert(body, &param_placeholder, &messages) {
        if let Ok(stmt) = crate::parser::sql_parser::parse_statement(&batch_sql) {
            if let Ok(planned) = plan_statement(stmt) {
                let _ = execute_planned_query(&planned, &[]).await;
                return;
            }
        }
    }

    // Fallback: execute per-message (for non-INSERT callbacks)
    for message in &messages {
        let escaped = message.replace('\'', "''");
        let substituted = body.replace(&param_placeholder, &format!("'{escaped}'"));
        if let Ok(stmt) = crate::parser::sql_parser::parse_statement(&substituted) {
            if let Ok(planned) = plan_statement(stmt) {
                let _ = execute_planned_query(&planned, &[]).await;
            }
        }
    }
}

/// Try to build a batched multi-row INSERT from the callback body and messages.
///
/// Given body `INSERT INTO trades VALUES (msg)` and messages `["a","b","c"]`,
/// produces `INSERT INTO trades VALUES ('a'), ('b'), ('c')`.
#[cfg(target_arch = "wasm32")]
fn try_build_batch_insert(body: &str, param: &str, messages: &[String]) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let values_pos = lower.find("values")?;
    let prefix = &body[..values_pos + 6]; // "INSERT INTO <table> VALUES"
    let template = body[values_pos + 6..].trim(); // " (msg)" or " ($1)"

    let rows: Vec<String> = messages
        .iter()
        .map(|msg| {
            let escaped = msg.replace('\'', "''");
            template.replace(param, &format!("'{escaped}'"))
        })
        .collect();

    Some(format!("{} {}", prefix, rows.join(", ")))
}

fn eval_array_subscript(
    array: ScalarValue,
    index: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    // Get the array elements
    let array = match array {
        ScalarValue::Text(text) => match parse_pg_array_literal(&text) {
            Ok(parsed) => parsed,
            Err(array_err) => {
                let json_text = ScalarValue::Text(text.clone());
                if parse_json_document_arg(&json_text, "json subscript", 1).is_ok() {
                    return eval_json_get_operator(json_text, index, false);
                }
                return Err(array_err);
            }
        },
        other => other,
    };
    let elements = match array {
        ScalarValue::Array(ref arr) => arr,
        ScalarValue::Null => return Ok(ScalarValue::Null),
        _ => {
            return Err(EngineError {
                message: "subscript operation requires array type".to_string(),
            });
        }
    };

    // Parse the index (PostgreSQL arrays are 1-indexed)
    let idx = match index {
        ScalarValue::Int(i) => i,
        ScalarValue::Null => return Ok(ScalarValue::Null),
        _ => {
            return Err(EngineError {
                message: "array subscript must be type integer".to_string(),
            });
        }
    };

    // Convert to a zero-based offset.
    let zero_idx = if idx > 0 {
        (idx - 1) as usize
    } else if idx == 0 {
        0usize
    } else {
        // Negative indices count from the end
        let abs_idx = (-idx) as usize;
        if abs_idx > elements.len() {
            return Ok(ScalarValue::Null);
        }
        elements.len() - abs_idx
    };

    // Return the element or null if out of bounds
    Ok(elements.get(zero_idx).cloned().unwrap_or(ScalarValue::Null))
}

fn eval_array_slice(
    array: ScalarValue,
    start: Option<ScalarValue>,
    end: Option<ScalarValue>,
) -> Result<ScalarValue, EngineError> {
    // Get the array elements
    let array = match array {
        ScalarValue::Text(text) => match parse_pg_array_literal(&text) {
            Ok(parsed) => parsed,
            Err(array_err) => {
                let json_text = ScalarValue::Text(text.clone());
                if let Ok(parsed_json) = parse_json_document_arg(&json_text, "json subscript", 1) {
                    return eval_json_array_slice(parsed_json, start, end);
                }
                return Err(array_err);
            }
        },
        other => other,
    };
    let elements = match array {
        ScalarValue::Array(ref arr) => arr,
        ScalarValue::Null => return Ok(ScalarValue::Null),
        _ => {
            return Err(EngineError {
                message: "slice operation requires array type".to_string(),
            });
        }
    };

    // Parse start index (1-indexed, default to 1 if not provided)
    let start_idx = if let Some(start_val) = start {
        match start_val {
            ScalarValue::Int(i) => {
                if i <= 0 {
                    0
                } else {
                    (i - 1) as usize
                }
            }
            ScalarValue::Null => return Ok(ScalarValue::Null),
            _ => {
                return Err(EngineError {
                    message: "array slice bounds must be type integer".to_string(),
                });
            }
        }
    } else {
        0
    };

    // Parse end index (1-indexed, inclusive, default to array length if not provided)
    let end_idx = if let Some(end_val) = end {
        match end_val {
            ScalarValue::Int(i) => {
                if i <= 0 {
                    0
                } else {
                    i as usize
                }
            }
            ScalarValue::Null => return Ok(ScalarValue::Null),
            _ => {
                return Err(EngineError {
                    message: "array slice bounds must be type integer".to_string(),
                });
            }
        }
    } else {
        elements.len()
    };

    // Extract the slice
    let start_idx = start_idx.min(elements.len());
    let end_idx = end_idx.min(elements.len());

    if start_idx >= end_idx {
        return Ok(ScalarValue::Array(Vec::new()));
    }

    let sliced = elements[start_idx..end_idx].to_vec();
    Ok(ScalarValue::Array(sliced))
}

fn eval_json_array_slice(
    parsed: JsonValue,
    start: Option<ScalarValue>,
    end: Option<ScalarValue>,
) -> Result<ScalarValue, EngineError> {
    let JsonValue::Array(items) = parsed else {
        return Ok(ScalarValue::Null);
    };

    let start_idx = if let Some(start_val) = start {
        match start_val {
            ScalarValue::Int(i) => {
                if i <= 0 {
                    0
                } else {
                    (i - 1) as usize
                }
            }
            ScalarValue::Null => return Ok(ScalarValue::Null),
            _ => {
                return Err(EngineError {
                    message: "array slice bounds must be type integer".to_string(),
                });
            }
        }
    } else {
        0
    };

    let end_idx = if let Some(end_val) = end {
        match end_val {
            ScalarValue::Int(i) => {
                if i <= 0 {
                    0
                } else {
                    i as usize
                }
            }
            ScalarValue::Null => return Ok(ScalarValue::Null),
            _ => {
                return Err(EngineError {
                    message: "array slice bounds must be type integer".to_string(),
                });
            }
        }
    } else {
        items.len()
    };

    let start_idx = start_idx.min(items.len());
    let end_idx = end_idx.min(items.len());
    if start_idx >= end_idx {
        return Ok(ScalarValue::Text("[]".to_string()));
    }

    let out = JsonValue::Array(items[start_idx..end_idx].to_vec());
    Ok(ScalarValue::Text(out.to_string()))
}

/// Concatenate two arrays (or an array and a scalar).
fn eval_array_concat(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let left = if let ScalarValue::Text(text) = left {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        left
    };
    let right = if let ScalarValue::Text(text) = right {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        right
    };
    let mut result = match left {
        ScalarValue::Array(elems) => elems,
        other => vec![other],
    };
    match right {
        ScalarValue::Array(elems) => result.extend(elems),
        other => result.push(other),
    }
    Ok(ScalarValue::Array(result))
}

/// Check if left array contains all elements of right array (`@>`).
fn eval_array_contains(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let left = if let ScalarValue::Text(text) = left {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        left
    };
    let right = if let ScalarValue::Text(text) = right {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        right
    };
    let left_elems = match left {
        ScalarValue::Array(elems) => elems,
        _ => return Ok(ScalarValue::Bool(false)),
    };
    let right_elems = match right {
        ScalarValue::Array(elems) => elems,
        _ => return Ok(ScalarValue::Bool(false)),
    };
    let contains_all = right_elems.iter().all(|r| left_elems.contains(r));
    Ok(ScalarValue::Bool(contains_all))
}

/// Check if two arrays share any elements (`&&`).
fn eval_array_overlap(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, EngineError> {
    let left = if let ScalarValue::Text(text) = left {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        left
    };
    let right = if let ScalarValue::Text(text) = right {
        parse_pg_array_literal(&text).unwrap_or(ScalarValue::Text(text))
    } else {
        right
    };
    let left_elems = match left {
        ScalarValue::Array(elems) => elems,
        _ => return Ok(ScalarValue::Bool(false)),
    };
    let right_elems = match right {
        ScalarValue::Array(elems) => elems,
        _ => return Ok(ScalarValue::Bool(false)),
    };
    let overlaps = left_elems.iter().any(|l| right_elems.contains(l));
    Ok(ScalarValue::Bool(overlaps))
}
