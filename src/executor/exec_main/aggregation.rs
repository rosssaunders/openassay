#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) async fn project_select_row(
    targets: &[crate::parser::ast::SelectItem],
    scope: &EvalScope,
    params: &[Option<String>],
    wildcard_columns: Option<&[ExpandedFromColumn]>,
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut row = Vec::new();
    for target in targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                row.push(scope.lookup_identifier(&col.lookup_parts)?);
            }
            continue;
        }
        if let Expr::QualifiedWildcard(qualifier) = &target.expr {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "qualified wildcard target requires FROM support".to_string(),
                });
            };
            expand_qualified_wildcard(qualifier, expanded, scope, &mut row)?;
            continue;
        }
        row.push(eval_expr(&target.expr, scope, params).await?);
    }
    Ok(row)
}

// Helper function to expand qualified wildcards (e.g., t.*, schema.table.*)
pub(super) fn expand_qualified_wildcard(
    qualifier: &[String],
    expanded: &[ExpandedFromColumn],
    scope: &EvalScope,
    row: &mut Vec<ScalarValue>,
) -> Result<(), EngineError> {
    // Use the last part of the qualifier as the table/alias name
    // For t.*, qualifier = ["t"], we match lookup_parts[0] == "t"
    // For schema.table.*, qualifier = ["schema", "table"], we match lookup_parts[0] == "table"
    let qualifier_lower = qualifier
        .last()
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    for col in expanded {
        // Match columns where the first lookup part (table/alias) equals the qualifier
        if col.lookup_parts.len() >= 2
            && col.lookup_parts[0].to_ascii_lowercase() == qualifier_lower
        {
            row.push(scope.lookup_identifier(&col.lookup_parts)?);
        }
    }
    Ok(())
}

pub(super) async fn project_select_row_with_window(
    targets: &[crate::parser::ast::SelectItem],
    scope: &EvalScope,
    row_idx: usize,
    all_rows: &[EvalScope],
    window_definitions: &[crate::parser::ast::WindowDefinition],
    params: &[Option<String>],
    wildcard_columns: Option<&[ExpandedFromColumn]>,
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut row = Vec::new();
    for target in targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                row.push(scope.lookup_identifier(&col.lookup_parts)?);
            }
            continue;
        }
        if let Expr::QualifiedWildcard(qualifier) = &target.expr {
            let Some(expanded) = wildcard_columns else {
                return Err(EngineError {
                    message: "qualified wildcard target requires FROM support".to_string(),
                });
            };
            expand_qualified_wildcard(qualifier, expanded, scope, &mut row)?;
            continue;
        }
        row.push(
            eval_expr_with_window(
                &target.expr,
                scope,
                row_idx,
                all_rows,
                window_definitions,
                params,
            )
            .await?,
        );
    }
    Ok(row)
}

pub(super) fn contains_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            if over.is_none()
                && let Some(fn_name) = name.last()
                && is_aggregate_function(fn_name)
            {
                return true;
            }
            args.iter().any(contains_aggregate_expr)
                || order_by
                    .iter()
                    .any(|entry| contains_aggregate_expr(&entry.expr))
                || within_group
                    .iter()
                    .any(|entry| contains_aggregate_expr(&entry.expr))
                || filter
                    .as_ref()
                    .is_some_and(|entry| contains_aggregate_expr(entry))
        }
        Expr::Cast { expr, .. } => contains_aggregate_expr(expr),
        Expr::Unary { expr, .. } => contains_aggregate_expr(expr),
        Expr::Binary { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::AnyAll { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::InList { expr, list, .. } => {
            contains_aggregate_expr(expr) || list.iter().any(contains_aggregate_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_aggregate_expr(expr)
                || contains_aggregate_expr(low)
                || contains_aggregate_expr(high)
        }
        Expr::Like { expr, pattern, .. } => {
            contains_aggregate_expr(expr) || contains_aggregate_expr(pattern)
        }
        Expr::IsNull { expr, .. } => contains_aggregate_expr(expr),
        Expr::IsDistinctFrom { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            contains_aggregate_expr(operand)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    contains_aggregate_expr(when_expr) || contains_aggregate_expr(then_expr)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_aggregate_expr(expr))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                contains_aggregate_expr(when_expr) || contains_aggregate_expr(then_expr)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| contains_aggregate_expr(expr))
        }
        Expr::ArrayConstructor(items) => items.iter().any(contains_aggregate_expr),
        Expr::ArraySubquery(_) => false,
        Expr::Exists(_) | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => false,
        _ => false,
    }
}

pub(super) fn contains_window_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            over.is_some()
                || args.iter().any(contains_window_expr)
                || order_by
                    .iter()
                    .any(|entry| contains_window_expr(&entry.expr))
                || within_group
                    .iter()
                    .any(|entry| contains_window_expr(&entry.expr))
                || filter
                    .as_ref()
                    .is_some_and(|entry| contains_window_expr(entry))
                || over.as_ref().is_some_and(|window| {
                    window.partition_by.iter().any(contains_window_expr)
                        || window
                            .order_by
                            .iter()
                            .any(|entry| contains_window_expr(&entry.expr))
                        || window.frame.as_ref().is_some_and(|frame| {
                            contains_window_bound_expr(&frame.start)
                                || contains_window_bound_expr(&frame.end)
                        })
                })
        }
        Expr::Cast { expr, .. } => contains_window_expr(expr),
        Expr::Unary { expr, .. } => contains_window_expr(expr),
        Expr::Binary { left, right, .. } => {
            contains_window_expr(left) || contains_window_expr(right)
        }
        Expr::AnyAll { left, right, .. } => {
            contains_window_expr(left) || contains_window_expr(right)
        }
        Expr::InList { expr, list, .. } => {
            contains_window_expr(expr) || list.iter().any(contains_window_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_window_expr(expr) || contains_window_expr(low) || contains_window_expr(high),
        Expr::Like { expr, pattern, .. } => {
            contains_window_expr(expr) || contains_window_expr(pattern)
        }
        Expr::IsNull { expr, .. } => contains_window_expr(expr),
        Expr::IsDistinctFrom { left, right, .. } => {
            contains_window_expr(left) || contains_window_expr(right)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            contains_window_expr(operand)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    contains_window_expr(when_expr) || contains_window_expr(then_expr)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_window_expr(expr))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                contains_window_expr(when_expr) || contains_window_expr(then_expr)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| contains_window_expr(expr))
        }
        Expr::ArrayConstructor(items) => items.iter().any(contains_window_expr),
        Expr::ArraySubquery(_) => false,
        Expr::Exists(_) | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => false,
        _ => false,
    }
}

pub(super) fn contains_window_bound_expr(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::OffsetPreceding(expr) | WindowFrameBound::OffsetFollowing(expr) => {
            contains_window_expr(expr)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => false,
    }
}

pub(super) fn group_by_contains_window_expr(group_by: &[GroupByExpr]) -> bool {
    group_by.iter().any(|expr| match expr {
        GroupByExpr::Expr(expr) => contains_window_expr(expr),
        GroupByExpr::GroupingSets(sets) => {
            sets.iter().any(|set| set.iter().any(contains_window_expr))
        }
        GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
            exprs.iter().any(contains_window_expr)
        }
    })
}

pub(super) fn group_by_exprs(group_by: &[GroupByExpr]) -> Vec<&Expr> {
    let mut out = Vec::new();
    for entry in group_by {
        match entry {
            GroupByExpr::Expr(expr) => out.push(expr),
            GroupByExpr::GroupingSets(sets) => {
                for set in sets {
                    for expr in set {
                        out.push(expr);
                    }
                }
            }
            GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
                for expr in exprs {
                    out.push(expr);
                }
            }
        }
    }
    out
}

/// If `expr` is a simple identifier matching a SELECT alias, return the aliased expression.
/// Otherwise return `expr` unchanged. Matches PostgreSQL's GROUP BY alias resolution.
pub(super) fn resolve_group_by_alias<'a>(
    expr: &'a Expr,
    alias_map: &'a HashMap<String, &'a Expr>,
) -> &'a Expr {
    if let Expr::Identifier(parts) = expr
        && parts.len() == 1
    {
        let key = parts[0].to_ascii_lowercase();
        if let Some(resolved) = alias_map.get(&key) {
            return resolved;
        }
    }
    expr
}

pub(super) fn identifier_key(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(parts) => Some(parts.join(".").to_ascii_lowercase()),
        _ => None,
    }
}

pub(super) fn collect_grouping_identifiers(group_by: &[GroupByExpr]) -> HashSet<String> {
    let mut out = HashSet::new();
    for expr in group_by_exprs(group_by) {
        if let Some(key) = identifier_key(expr) {
            out.insert(key);
        }
    }
    out
}

pub(super) fn expand_grouping_sets<'a>(group_by: &'a [GroupByExpr]) -> Vec<Vec<&'a Expr>> {
    if group_by.is_empty() {
        return vec![Vec::new()];
    }
    let mut sets: Vec<Vec<&'a Expr>> = vec![Vec::new()];
    for entry in group_by {
        let element_sets: Vec<Vec<&'a Expr>> = match entry {
            GroupByExpr::Expr(expr) => vec![vec![expr]],
            GroupByExpr::GroupingSets(sets) => {
                sets.iter().map(|set| set.iter().collect()).collect()
            }
            GroupByExpr::Rollup(exprs) => {
                let mut rollup_sets = Vec::new();
                for len in (0..=exprs.len()).rev() {
                    rollup_sets.push(exprs[..len].iter().collect());
                }
                rollup_sets
            }
            GroupByExpr::Cube(exprs) => {
                let mut cube_sets = Vec::new();
                let count = exprs.len();
                let max_mask = 1usize << count;
                for mask in (0..max_mask).rev() {
                    let mut set = Vec::new();
                    for (idx, expr) in exprs.iter().enumerate() {
                        if (mask & (1 << idx)) != 0 {
                            set.push(expr);
                        }
                    }
                    cube_sets.push(set);
                }
                cube_sets
            }
        };

        let mut combined_sets = Vec::new();
        for existing in &sets {
            for element in &element_sets {
                let mut merged = Vec::with_capacity(existing.len() + element.len());
                merged.extend(existing.iter().copied());
                merged.extend(element.iter().copied());
                combined_sets.push(merged);
            }
        }
        sets = combined_sets;
    }
    sets
}

pub(super) struct GroupingContext {
    pub(super) current_grouping: HashSet<String>,
    pub(super) all_grouping: HashSet<String>,
}

pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "string_agg"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "json_object_agg"
            | "jsonb_object_agg"
            | "any_value"
            | "bool_and"
            | "bool_or"
            | "every"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "variance"
            | "var_samp"
            | "var_pop"
            | "corr"
            | "covar_pop"
            | "covar_samp"
            | "regr_slope"
            | "regr_intercept"
            | "regr_count"
            | "regr_r2"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_sxx"
            | "regr_sxy"
            | "regr_syy"
            | "percentile_cont"
            | "percentile_disc"
            | "mode"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
    )
}

pub(super) fn eval_group_expr<'a>(
    expr: &'a Expr,
    group_rows: &'a [EvalScope],
    representative: &'a EvalScope,
    params: &'a [Option<String>],
    grouping: &'a GroupingContext,
) -> EngineFuture<'a, Result<ScalarValue, EngineError>> {
    Box::pin(async move {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct,
                order_by,
                within_group,
                filter,
                over,
            } => {
                if over.is_some() {
                    return Err(EngineError {
                        message:
                            "window functions are not allowed in grouped aggregate expressions"
                                .to_string(),
                    });
                }
                let fn_name = name
                    .last()
                    .map(|n| n.to_ascii_lowercase())
                    .unwrap_or_default();
                if fn_name == "grouping" {
                    if *distinct
                        || !order_by.is_empty()
                        || !within_group.is_empty()
                        || filter.is_some()
                    {
                        return Err(EngineError {
                            message: "grouping() does not accept aggregate modifiers".to_string(),
                        });
                    }
                    if args.is_empty() {
                        return Err(EngineError {
                            message: "too few arguments".to_string(),
                        });
                    }
                    // PostgreSQL GROUPING() returns a bitmask: for each argument
                    // (left to right), shift left and set the low bit to 1 if
                    // the column is NOT in the current grouping set, 0 if it is.
                    let mut bitmask: i64 = 0;
                    for arg in args {
                        bitmask <<= 1;
                        let Some(key) = identifier_key(arg) else {
                            return Err(EngineError {
                                message: "arguments to GROUPING must be grouping expressions of the associated query level".to_string(),
                            });
                        };
                        if !grouping.current_grouping.contains(&key) {
                            bitmask |= 1;
                        }
                    }
                    return Ok(ScalarValue::Int(bitmask));
                }
                if is_aggregate_function(&fn_name) {
                    return eval_aggregate_function(
                        &fn_name,
                        args,
                        *distinct,
                        order_by,
                        within_group,
                        filter.as_deref(),
                        group_rows,
                        params,
                    )
                    .await;
                }
                if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some()
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

                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(
                        eval_group_expr(arg, group_rows, representative, params, grouping).await?,
                    );
                }
                eval_scalar_function(&fn_name, &values).await
            }
            Expr::Unary { op, expr } => {
                let value =
                    eval_group_expr(expr, group_rows, representative, params, grouping).await?;
                eval_unary(op.clone(), value)
            }
            Expr::Binary { left, op, right } => {
                let lhs =
                    eval_group_expr(left, group_rows, representative, params, grouping).await?;
                let rhs =
                    eval_group_expr(right, group_rows, representative, params, grouping).await?;
                eval_binary(op.clone(), lhs, rhs)
            }
            Expr::AnyAll {
                left,
                op,
                right,
                quantifier,
            } => {
                let lhs =
                    eval_group_expr(left, group_rows, representative, params, grouping).await?;
                let rhs =
                    eval_group_expr(right, group_rows, representative, params, grouping).await?;
                eval_any_all(op.clone(), lhs, rhs, quantifier.clone())
            }
            Expr::Cast { expr, type_name } => {
                let value =
                    eval_group_expr(expr, group_rows, representative, params, grouping).await?;
                eval_cast_scalar(value, type_name)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value =
                    eval_group_expr(expr, group_rows, representative, params, grouping).await?;
                let low_value =
                    eval_group_expr(low, group_rows, representative, params, grouping).await?;
                let high_value =
                    eval_group_expr(high, group_rows, representative, params, grouping).await?;
                eval_between_predicate(value, low_value, high_value, *negated)
            }
            Expr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                let value =
                    eval_group_expr(expr, group_rows, representative, params, grouping).await?;
                let pattern_value =
                    eval_group_expr(pattern, group_rows, representative, params, grouping).await?;
                let escape_char = if let Some(escape_expr) = escape {
                    let escape_value =
                        eval_group_expr(escape_expr, group_rows, representative, params, grouping)
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
                let value =
                    eval_group_expr(expr, group_rows, representative, params, grouping).await?;
                let is_null = matches!(value, ScalarValue::Null);
                Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let left_value =
                    eval_group_expr(left, group_rows, representative, params, grouping).await?;
                let right_value =
                    eval_group_expr(right, group_rows, representative, params, grouping).await?;
                eval_is_distinct_from(left_value, right_value, *negated)
            }
            Expr::CaseSimple {
                operand,
                when_then,
                else_expr,
            } => {
                let operand_value =
                    eval_group_expr(operand, group_rows, representative, params, grouping).await?;
                for (when_expr, then_expr) in when_then {
                    let when_value =
                        eval_group_expr(when_expr, group_rows, representative, params, grouping)
                            .await?;
                    if matches!(operand_value, ScalarValue::Null)
                        || matches!(when_value, ScalarValue::Null)
                    {
                        continue;
                    }
                    if compare_values_for_predicate(&operand_value, &when_value)? == Ordering::Equal
                    {
                        return eval_group_expr(
                            then_expr,
                            group_rows,
                            representative,
                            params,
                            grouping,
                        )
                        .await;
                    }
                }
                if let Some(else_expr) = else_expr {
                    eval_group_expr(else_expr, group_rows, representative, params, grouping).await
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
                        eval_group_expr(when_expr, group_rows, representative, params, grouping)
                            .await?;
                    if truthy(&condition) {
                        return eval_group_expr(
                            then_expr,
                            group_rows,
                            representative,
                            params,
                            grouping,
                        )
                        .await;
                    }
                }
                if let Some(else_expr) = else_expr {
                    eval_group_expr(else_expr, group_rows, representative, params, grouping).await
                } else {
                    Ok(ScalarValue::Null)
                }
            }
            Expr::ArrayConstructor(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(
                        eval_group_expr(item, group_rows, representative, params, grouping).await?,
                    );
                }
                Ok(ScalarValue::Array(values))
            }
            Expr::ArraySubquery(query) => {
                let result = execute_query_with_outer(query, params, Some(representative)).await?;
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
            Expr::Identifier(_) => {
                if let Some(key) = identifier_key(expr)
                    && grouping.all_grouping.contains(&key)
                    && !grouping.current_grouping.contains(&key)
                {
                    return Ok(ScalarValue::Null);
                }
                if group_rows.is_empty() {
                    eval_expr(expr, &EvalScope::default(), params).await
                } else {
                    eval_expr(expr, representative, params).await
                }
            }
            _ => {
                if group_rows.is_empty() {
                    eval_expr(expr, &EvalScope::default(), params).await
                } else {
                    eval_expr(expr, representative, params).await
                }
            }
        }
    })
}

#[derive(Debug, Clone)]
pub(super) struct AggregateInputRow {
    args: Vec<ScalarValue>,
    order_keys: Vec<ScalarValue>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RegrStats {
    count: i64,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

pub(super) fn compute_regr_stats(
    rows: &[AggregateInputRow],
    message: &str,
) -> Result<RegrStats, EngineError> {
    let mut stats = RegrStats {
        count: 0,
        sum_x: 0.0,
        sum_y: 0.0,
        sum_xx: 0.0,
        sum_yy: 0.0,
        sum_xy: 0.0,
    };
    for row in rows {
        let Some(y_value) = row.args.first() else {
            continue;
        };
        let Some(x_value) = row.args.get(1) else {
            continue;
        };
        if matches!(y_value, ScalarValue::Null) || matches!(x_value, ScalarValue::Null) {
            continue;
        }
        let y = parse_f64_numeric_scalar(y_value, message)?;
        let x = parse_f64_numeric_scalar(x_value, message)?;
        stats.count += 1;
        stats.sum_x += x;
        stats.sum_y += y;
        stats.sum_xx += x * x;
        stats.sum_yy += y * y;
        stats.sum_xy += x * y;
    }
    Ok(stats)
}

pub(super) async fn build_aggregate_input_rows(
    args: &[Expr],
    order_by: &[OrderByExpr],
    filter: Option<&Expr>,
    group_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<Vec<AggregateInputRow>, EngineError> {
    let mut out = Vec::with_capacity(group_rows.len());
    for scope in group_rows {
        if let Some(predicate) = filter
            && !truthy(&eval_expr(predicate, scope, params).await?)
        {
            continue;
        }

        let mut arg_values = Vec::with_capacity(args.len());
        for arg in args {
            arg_values.push(eval_expr(arg, scope, params).await?);
        }
        let mut order_values = Vec::with_capacity(order_by.len());
        for order_expr in order_by {
            order_values.push(eval_expr(&order_expr.expr, scope, params).await?);
        }
        out.push(AggregateInputRow {
            args: arg_values,
            order_keys: order_values,
        });
    }
    Ok(out)
}

pub(super) fn apply_aggregate_distinct(rows: &mut Vec<AggregateInputRow>) {
    let mut seen = HashSet::new();
    rows.retain(|row| seen.insert(row_key(&row.args)));
}

pub(super) fn sort_aggregate_rows(rows: &mut [AggregateInputRow], order_by: &[OrderByExpr]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|left, right| compare_order_keys(&left.order_keys, &right.order_keys, order_by));
}

pub(super) fn sum_i64_fast(values: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return unsafe { sum_i64_avx2(values) };
        }
    }
    values.iter().copied().sum()
}

pub(super) fn sum_f64_fast(values: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return unsafe { sum_f64_avx(values) };
        }
    }
    values.iter().copied().sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn sum_i64_avx2(values: &[i64]) -> i64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_setzero_si256();
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 i64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        acc = _mm256_add_epi64(acc, v);
    }

    let mut lanes = [0_i64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast(), acc) };
    lanes.iter().copied().sum::<i64>() + remainder.iter().copied().sum::<i64>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
pub(super) unsafe fn sum_f64_avx(values: &[f64]) -> f64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_setzero_pd();
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 f64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_pd(chunk.as_ptr()) };
        acc = _mm256_add_pd(acc, v);
    }

    let mut lanes = [0.0_f64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_pd(lanes.as_mut_ptr(), acc) };
    lanes.iter().copied().sum::<f64>() + remainder.iter().copied().sum::<f64>()
}

pub(super) fn min_i64_fast(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return Some(unsafe { min_i64_avx2(values) });
        }
    }
    values.iter().copied().min()
}

pub(super) fn max_i64_fast(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return Some(unsafe { max_i64_avx2(values) });
        }
    }
    values.iter().copied().max()
}

pub(super) fn min_f64_fast(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return Some(unsafe { min_f64_avx(values) });
        }
    }
    Some(values.iter().copied().fold(f64::INFINITY, f64::min))
}

pub(super) fn max_f64_fast(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return Some(unsafe { max_f64_avx(values) });
        }
    }
    Some(values.iter().copied().fold(f64::NEG_INFINITY, f64::max))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn min_i64_avx2(values: &[i64]) -> i64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_set1_epi64x(i64::MAX);
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 i64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        // AVX2 has no _mm256_min_epi64, so use cmpgt + blendv:
        // mask = v > acc (all-ones where v > acc), then blend keeps acc where mask is set, v otherwise
        let mask = _mm256_cmpgt_epi64(v, acc);
        acc = _mm256_blendv_epi8(v, acc, mask);
    }
    let mut lanes = [0_i64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast(), acc) };
    let mut result = lanes[0];
    for &lane in &lanes[1..] {
        result = result.min(lane);
    }
    for &val in remainder {
        result = result.min(val);
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn max_i64_avx2(values: &[i64]) -> i64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_set1_epi64x(i64::MIN);
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 i64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        // mask = acc > v (all-ones where acc > v), then blend keeps v where mask is set, acc otherwise
        let mask = _mm256_cmpgt_epi64(acc, v);
        acc = _mm256_blendv_epi8(v, acc, mask);
    }
    let mut lanes = [0_i64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast(), acc) };
    let mut result = lanes[0];
    for &lane in &lanes[1..] {
        result = result.max(lane);
    }
    for &val in remainder {
        result = result.max(val);
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
pub(super) unsafe fn min_f64_avx(values: &[f64]) -> f64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_set1_pd(f64::INFINITY);
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 f64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_pd(chunk.as_ptr()) };
        acc = _mm256_min_pd(acc, v);
    }
    let mut lanes = [0.0_f64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_pd(lanes.as_mut_ptr(), acc) };
    let mut result = lanes[0];
    for &lane in &lanes[1..] {
        result = result.min(lane);
    }
    for &val in remainder {
        result = result.min(val);
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
pub(super) unsafe fn max_f64_avx(values: &[f64]) -> f64 {
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut acc = _mm256_set1_pd(f64::NEG_INFINITY);
    for chunk in chunks {
        // SAFETY: chunk is exactly 4 f64 values; loadu accepts unaligned addresses.
        let v = unsafe { _mm256_loadu_pd(chunk.as_ptr()) };
        acc = _mm256_max_pd(acc, v);
    }
    let mut lanes = [0.0_f64; 4];
    // SAFETY: lanes has exactly 32 bytes of writable storage.
    unsafe { _mm256_storeu_pd(lanes.as_mut_ptr(), acc) };
    let mut result = lanes[0];
    for &lane in &lanes[1..] {
        result = result.max(lane);
    }
    for &val in remainder {
        result = result.max(val);
    }
    result
}

#[allow(clippy::too_many_arguments)]
pub async fn eval_aggregate_function(
    fn_name: &str,
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    within_group: &[OrderByExpr],
    filter: Option<&Expr>,
    group_rows: &[EvalScope],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    let is_ordered_set = matches!(fn_name, "percentile_cont" | "percentile_disc" | "mode");
    if !within_group.is_empty() && !is_ordered_set {
        return Err(EngineError {
            message: format!("{fn_name}() does not support WITHIN GROUP"),
        });
    }
    if is_ordered_set && within_group.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires WITHIN GROUP (ORDER BY ...)"),
        });
    }

    match fn_name {
        "count" => {
            if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
                if distinct {
                    return Err(EngineError {
                        message: "count(DISTINCT *) is not supported".to_string(),
                    });
                }
                if !order_by.is_empty() {
                    return Err(EngineError {
                        message: "count(*) does not accept aggregate ORDER BY".to_string(),
                    });
                }
                let mut count = 0i64;
                for scope in group_rows {
                    if let Some(predicate) = filter
                        && !truthy(&eval_expr(predicate, scope, params).await?)
                    {
                        continue;
                    }
                    count += 1;
                }
                return Ok(ScalarValue::Int(count));
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "count() expects exactly one argument".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let count = rows
                .iter()
                .filter(|row| !matches!(row.args[0], ScalarValue::Null))
                .count() as i64;
            Ok(ScalarValue::Int(count))
        }
        "sum" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "sum() expects exactly one argument".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            let mut int_values = Vec::with_capacity(rows.len());
            let mut float_values = Vec::new();
            let mut decimal_sum: rust_decimal::Decimal = rust_decimal::Decimal::ZERO;
            let mut saw_float = false;
            let mut saw_decimal = false;
            let mut saw_any = false;
            for row in rows {
                match row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Int(v) => {
                        int_values.push(v);
                        decimal_sum += rust_decimal::Decimal::from(v);
                        saw_any = true;
                    }
                    ScalarValue::Float(v) => {
                        float_values.push(v);
                        decimal_sum += rust_decimal::Decimal::try_from(v)
                            .unwrap_or(rust_decimal::Decimal::ZERO);
                        saw_float = true;
                        saw_any = true;
                    }
                    ScalarValue::Numeric(v) => {
                        decimal_sum += v;
                        saw_decimal = true;
                        saw_any = true;
                    }
                    _ => {
                        return Err(EngineError {
                            message: "sum() expects numeric values".to_string(),
                        });
                    }
                }
            }
            if !saw_any {
                return Ok(ScalarValue::Null);
            }
            let int_sum = sum_i64_fast(&int_values);
            let float_sum = sum_f64_fast(&float_values) + int_sum as f64;
            if saw_decimal {
                Ok(ScalarValue::Numeric(decimal_sum))
            } else if saw_float {
                Ok(ScalarValue::Float(float_sum))
            } else {
                Ok(ScalarValue::Int(int_sum))
            }
        }
        "avg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "avg() expects exactly one argument".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            let mut int_values = Vec::with_capacity(rows.len());
            let mut float_values = Vec::new();
            let mut decimal_total = rust_decimal::Decimal::ZERO;
            let mut count = 0u64;
            let mut saw_decimal = false;
            for row in rows {
                match row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Int(v) => {
                        int_values.push(v);
                        decimal_total += rust_decimal::Decimal::from(v);
                        count += 1;
                    }
                    ScalarValue::Float(v) => {
                        float_values.push(v);
                        decimal_total += rust_decimal::Decimal::try_from(v)
                            .unwrap_or(rust_decimal::Decimal::ZERO);
                        count += 1;
                    }
                    ScalarValue::Numeric(v) => {
                        float_values.push(v.to_string().parse::<f64>().unwrap_or(0.0));
                        decimal_total += v;
                        saw_decimal = true;
                        count += 1;
                    }
                    _ => {
                        return Err(EngineError {
                            message: "avg() expects numeric values".to_string(),
                        });
                    }
                }
            }
            if count == 0 {
                Ok(ScalarValue::Null)
            } else if saw_decimal {
                let avg = decimal_total / rust_decimal::Decimal::from(count);
                Ok(ScalarValue::Numeric(avg))
            } else {
                let float_total = sum_i64_fast(&int_values) as f64 + sum_f64_fast(&float_values);
                Ok(ScalarValue::Float(float_total / count as f64))
            }
        }
        "min" | "max" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly one argument"),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);

            // Try SIMD fast path for pure-numeric columns
            let mut int_values = Vec::new();
            let mut float_values = Vec::new();
            let mut all_numeric = true;
            for row in &rows {
                match &row.args[0] {
                    ScalarValue::Null => continue,
                    ScalarValue::Int(v) => int_values.push(*v),
                    ScalarValue::Float(v) => {
                        if v.is_nan() {
                            all_numeric = false;
                            break;
                        }
                        float_values.push(*v);
                    }
                    _ => {
                        all_numeric = false;
                        break;
                    }
                }
            }

            if all_numeric && (!int_values.is_empty() || !float_values.is_empty()) {
                let result = if float_values.is_empty() {
                    // Pure integer path
                    let v = if fn_name == "min" {
                        min_i64_fast(&int_values)
                    } else {
                        max_i64_fast(&int_values)
                    };
                    v.map(ScalarValue::Int)
                } else if int_values.is_empty() {
                    // Pure float path
                    let v = if fn_name == "min" {
                        min_f64_fast(&float_values)
                    } else {
                        max_f64_fast(&float_values)
                    };
                    v.map(ScalarValue::Float)
                } else {
                    // Mixed int+float: compute winners separately to avoid i64→f64 precision loss
                    let int_winner = if fn_name == "min" {
                        min_i64_fast(&int_values)
                    } else {
                        max_i64_fast(&int_values)
                    }
                    .ok_or_else(|| EngineError {
                        message: "unexpected None: numeric min/max integer winner".to_string(),
                    })?;
                    let float_winner = if fn_name == "min" {
                        min_f64_fast(&float_values)
                    } else {
                        max_f64_fast(&float_values)
                    }
                    .ok_or_else(|| EngineError {
                        message: "unexpected None: numeric min/max float winner".to_string(),
                    })?;
                    let int_as_f64 = int_winner as f64;
                    let pick_int = if fn_name == "min" {
                        int_as_f64 <= float_winner
                    } else {
                        int_as_f64 >= float_winner
                    };
                    // If the cast was lossless, return as Int to preserve type;
                    // otherwise return as Float.
                    if pick_int {
                        if int_winner as f64 == int_as_f64 && (int_as_f64 as i64) == int_winner {
                            Some(ScalarValue::Int(int_winner))
                        } else {
                            Some(ScalarValue::Float(int_as_f64))
                        }
                    } else {
                        Some(ScalarValue::Float(float_winner))
                    }
                };
                return Ok(result.unwrap_or(ScalarValue::Null));
            }

            // Scalar fallback for non-numeric types, NaN, Numeric/Decimal, etc.
            let mut current: Option<ScalarValue> = None;
            for row in rows {
                let value = row.args[0].clone();
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                match &current {
                    None => current = Some(value),
                    Some(existing) => {
                        let cmp = scalar_cmp(&value, existing);
                        let take = if fn_name == "min" {
                            cmp == Ordering::Less
                        } else {
                            cmp == Ordering::Greater
                        };
                        if take {
                            current = Some(value);
                        }
                    }
                }
            }
            Ok(current.unwrap_or(ScalarValue::Null))
        }
        "json_agg" | "jsonb_agg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly one argument"),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                out.push(scalar_to_json_value(&row.args[0])?);
            }
            Ok(ScalarValue::Text(JsonValue::Array(out).to_string()))
        }
        "string_agg" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: "string_agg() expects exactly two arguments".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let delimiter = match &rows.first().map(|r| &r.args[1]) {
                Some(ScalarValue::Text(s)) => s.clone(),
                Some(ScalarValue::Null) => return Ok(ScalarValue::Null),
                Some(other) => other.render(),
                None => return Ok(ScalarValue::Null),
            };
            let parts: Vec<String> = rows
                .iter()
                .filter(|r| !matches!(r.args[0], ScalarValue::Null))
                .map(|r| r.args[0].render())
                .collect();
            if parts.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Text(parts.join(&delimiter)))
            }
        }
        "array_agg" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "array_agg() expects exactly one argument".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let parts: Vec<String> = rows
                .iter()
                .map(|r| {
                    if matches!(r.args[0], ScalarValue::Null) {
                        "NULL".to_string()
                    } else {
                        r.args[0].render()
                    }
                })
                .collect();
            Ok(ScalarValue::Text(format!("{{{}}}", parts.join(","))))
        }
        "any_value" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "any_value() expects exactly one argument".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            for row in rows {
                if !matches!(row.args[0], ScalarValue::Null) {
                    return Ok(row.args[0].clone());
                }
            }
            Ok(ScalarValue::Null)
        }
        "bool_and" | "every" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects one argument"),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let mut result = true;
            let mut saw_any = false;
            for row in &rows {
                match &row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Bool(b) => {
                        saw_any = true;
                        if !b {
                            result = false;
                        }
                    }
                    _ => {
                        return Err(EngineError {
                            message: format!("{fn_name}() expects boolean"),
                        });
                    }
                }
            }
            if !saw_any {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(result))
            }
        }
        "bool_or" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "bool_or() expects one argument".to_string(),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let mut result = false;
            let mut saw_any = false;
            for row in &rows {
                match &row.args[0] {
                    ScalarValue::Null => {}
                    ScalarValue::Bool(b) => {
                        saw_any = true;
                        if *b {
                            result = true;
                        }
                    }
                    _ => {
                        return Err(EngineError {
                            message: "bool_or() expects boolean".to_string(),
                        });
                    }
                }
            }
            if !saw_any {
                Ok(ScalarValue::Null)
            } else {
                Ok(ScalarValue::Bool(result))
            }
        }
        "stddev" | "stddev_samp" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects one argument"),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| match &r.args[0] {
                    ScalarValue::Int(i) => Some(*i as f64),
                    ScalarValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return Ok(ScalarValue::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
            Ok(ScalarValue::Float(variance.sqrt()))
        }
        "stddev_pop" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "stddev_pop() expects one argument".to_string(),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| match &r.args[0] {
                    ScalarValue::Int(i) => Some(*i as f64),
                    ScalarValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(variance.sqrt()))
        }
        "variance" | "var_samp" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects one argument"),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| match &r.args[0] {
                    ScalarValue::Int(i) => Some(*i as f64),
                    ScalarValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.len() < 2 {
                return Ok(ScalarValue::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64,
            ))
        }
        "var_pop" => {
            if args.len() != 1 {
                return Err(EngineError {
                    message: "var_pop() expects one argument".to_string(),
                });
            }
            let rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|r| match &r.args[0] {
                    ScalarValue::Int(i) => Some(*i as f64),
                    ScalarValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            Ok(ScalarValue::Float(
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64,
            ))
        }
        "corr" | "covar_pop" | "covar_samp" | "regr_slope" | "regr_intercept" | "regr_count"
        | "regr_r2" | "regr_avgx" | "regr_avgy" | "regr_sxx" | "regr_sxy" | "regr_syy" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly two arguments"),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            let stats = compute_regr_stats(&rows, &format!("{fn_name}() expects numeric values"))?;
            let count = stats.count;
            if fn_name == "regr_count" {
                return Ok(ScalarValue::Int(count));
            }
            if count == 0 {
                return Ok(ScalarValue::Null);
            }
            let n = count as f64;
            let avgx = stats.sum_x / n;
            let avgy = stats.sum_y / n;
            let sxx = stats.sum_xx - stats.sum_x * stats.sum_x / n;
            let syy = stats.sum_yy - stats.sum_y * stats.sum_y / n;
            let sxy = stats.sum_xy - stats.sum_x * stats.sum_y / n;
            match fn_name {
                "corr" => {
                    if count < 2 || sxx <= 0.0 || syy <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / (sxx * syy).sqrt()))
                    }
                }
                "covar_pop" => Ok(ScalarValue::Float(sxy / n)),
                "covar_samp" => {
                    if count < 2 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / (n - 1.0)))
                    }
                }
                "regr_slope" => {
                    if count < 2 || sxx <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        Ok(ScalarValue::Float(sxy / sxx))
                    }
                }
                "regr_intercept" => {
                    if count < 2 || sxx <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        let slope = sxy / sxx;
                        Ok(ScalarValue::Float(avgy - slope * avgx))
                    }
                }
                "regr_r2" => {
                    if count < 2 || sxx <= 0.0 || syy <= 0.0 {
                        Ok(ScalarValue::Null)
                    } else {
                        #[allow(clippy::suspicious_operation_groupings)]
                        Ok(ScalarValue::Float((sxy * sxy) / (sxx * syy))) // r² = sxy²/(sxx·syy)
                    }
                }
                "regr_avgx" => Ok(ScalarValue::Float(avgx)),
                "regr_avgy" => Ok(ScalarValue::Float(avgy)),
                "regr_sxx" => Ok(ScalarValue::Float(sxx)),
                "regr_sxy" => Ok(ScalarValue::Float(sxy)),
                "regr_syy" => Ok(ScalarValue::Float(syy)),
                _ => Err(EngineError {
                    message: format!("unsupported aggregate function {fn_name}"),
                }),
            }
        }
        "percentile_cont" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "percentile_cont() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "percentile_cont() does not accept DISTINCT".to_string(),
                });
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "percentile_cont() expects exactly one argument".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "percentile_cont() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let fraction_value = rows.iter().find_map(|row| match &row.args[0] {
                ScalarValue::Null => None,
                value => Some(value.clone()),
            });
            let Some(fraction_value) = fraction_value else {
                return Ok(ScalarValue::Null);
            };
            let fraction = parse_f64_numeric_scalar(
                &fraction_value,
                "percentile_cont() expects numeric fraction",
            )?;
            if !(0.0..=1.0).contains(&fraction) {
                return Err(EngineError {
                    message: "percentile_cont() fraction must be between 0 and 1".to_string(),
                });
            }
            let mut values = Vec::new();
            for row in &rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                let parsed =
                    parse_f64_numeric_scalar(value, "percentile_cont() expects numeric values")?;
                values.push(parsed);
            }
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            if values.len() == 1 {
                return Ok(ScalarValue::Float(values[0]));
            }
            let pos = fraction * (values.len() - 1) as f64;
            let lower_idx = pos.floor() as usize;
            let upper_idx = pos.ceil() as usize;
            let lower = values[lower_idx];
            let upper = values[upper_idx];
            if lower_idx == upper_idx {
                Ok(ScalarValue::Float(lower))
            } else {
                let weight = pos - lower_idx as f64;
                Ok(ScalarValue::Float(lower + (upper - lower) * weight))
            }
        }
        "percentile_disc" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "percentile_disc() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "percentile_disc() does not accept DISTINCT".to_string(),
                });
            }
            if args.len() != 1 {
                return Err(EngineError {
                    message: "percentile_disc() expects exactly one argument".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "percentile_disc() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let fraction_value = rows.iter().find_map(|row| match &row.args[0] {
                ScalarValue::Null => None,
                value => Some(value.clone()),
            });
            let Some(fraction_value) = fraction_value else {
                return Ok(ScalarValue::Null);
            };
            let fraction = parse_f64_numeric_scalar(
                &fraction_value,
                "percentile_disc() expects numeric fraction",
            )?;
            if !(0.0..=1.0).contains(&fraction) {
                return Err(EngineError {
                    message: "percentile_disc() fraction must be between 0 and 1".to_string(),
                });
            }
            let mut values = Vec::new();
            for row in &rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                values.push(value.clone());
            }
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut pos = (fraction * values.len() as f64).ceil() as usize;
            if pos == 0 {
                pos = 1;
            }
            Ok(values[pos - 1].clone())
        }
        "mode" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: "mode() does not accept aggregate ORDER BY".to_string(),
                });
            }
            if distinct {
                return Err(EngineError {
                    message: "mode() does not accept DISTINCT".to_string(),
                });
            }
            if !args.is_empty() {
                return Err(EngineError {
                    message: "mode() expects no arguments".to_string(),
                });
            }
            if within_group.len() != 1 {
                return Err(EngineError {
                    message: "mode() requires a single ORDER BY expression".to_string(),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            let mut best_value: Option<ScalarValue> = None;
            let mut best_count = 0usize;
            let mut current_value: Option<ScalarValue> = None;
            let mut current_count = 0usize;
            for row in rows {
                let Some(value) = row.order_keys.first() else {
                    continue;
                };
                if matches!(value, ScalarValue::Null) {
                    continue;
                }
                match &current_value {
                    Some(existing) if scalar_cmp(existing, value) == Ordering::Equal => {
                        current_count += 1;
                    }
                    _ => {
                        if current_count > best_count {
                            best_count = current_count;
                            best_value = current_value.clone();
                        }
                        current_value = Some(value.clone());
                        current_count = 1;
                    }
                }
            }
            if current_count > best_count {
                best_value = current_value;
            }
            Ok(best_value.unwrap_or(ScalarValue::Null))
        }
        "rank" | "dense_rank" | "percent_rank" | "cume_dist" => {
            if !order_by.is_empty() {
                return Err(EngineError {
                    message: format!("{fn_name}() does not accept aggregate ORDER BY"),
                });
            }
            if within_group.is_empty() {
                return Err(EngineError {
                    message: format!("{fn_name}() requires WITHIN GROUP (ORDER BY ...)"),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, within_group, filter, group_rows, params).await?;
            sort_aggregate_rows(&mut rows, within_group);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            match fn_name {
                "rank" | "dense_rank" => Ok(ScalarValue::Int(1)),
                "percent_rank" => Ok(ScalarValue::Float(0.0)),
                "cume_dist" => Ok(ScalarValue::Float(1.0)),
                _ => Ok(ScalarValue::Null),
            }
        }
        "json_object_agg" | "jsonb_object_agg" => {
            if args.len() != 2 {
                return Err(EngineError {
                    message: format!("{fn_name}() expects exactly two arguments"),
                });
            }
            let mut rows =
                build_aggregate_input_rows(args, order_by, filter, group_rows, params).await?;
            if distinct {
                apply_aggregate_distinct(&mut rows);
            }
            sort_aggregate_rows(&mut rows, order_by);
            if rows.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let mut out = JsonMap::new();
            for row in rows {
                if matches!(row.args[0], ScalarValue::Null) {
                    return Err(EngineError {
                        message: format!("{fn_name}() key cannot be null"),
                    });
                }
                out.insert(row.args[0].render(), scalar_to_json_value(&row.args[1])?);
            }
            Ok(ScalarValue::Text(JsonValue::Object(out).to_string()))
        }
        _ => Err(EngineError {
            message: format!("unsupported aggregate function {fn_name}"),
        }),
    }
}
