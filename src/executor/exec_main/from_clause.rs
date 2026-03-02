#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Debug, Clone)]
pub struct TableEval {
    pub(crate) rows: Vec<EvalScope>,
    pub(crate) columns: Vec<String>,
    pub(crate) null_scope: EvalScope,
}

pub async fn evaluate_from_clause(
    from: &[TableExpression],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<Vec<EvalScope>, EngineError> {
    let mut current = vec![EvalScope::default()];
    for item in from {
        let mut next = Vec::new();
        match item {
            TableExpression::Function(_)
            | TableExpression::Subquery(SubqueryRef { lateral: true, .. }) => {
                // Table functions in FROM may reference prior FROM bindings; evaluate per lhs scope.
                for lhs_scope in &current {
                    let mut merged_outer = lhs_scope.clone();
                    if let Some(outer) = outer_scope {
                        merged_outer.inherit_outer(outer);
                    }
                    let rhs = evaluate_table_expression(item, params, Some(&merged_outer)).await?;
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
            _ => {
                let rhs = evaluate_table_expression(item, params, outer_scope).await?;
                for lhs_scope in &current {
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
        }
        current = next;
    }
    Ok(current)
}

/// Decompose an expression into AND-conjuncts.
pub(super) fn decompose_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    decompose_and_conjuncts_inner(expr, &mut conjuncts);
    conjuncts
}

pub(super) fn decompose_and_conjuncts_inner(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::Binary {
        op: crate::parser::ast::BinaryOp::And,
        left,
        right,
    } = expr
    {
        decompose_and_conjuncts_inner(left, out);
        decompose_and_conjuncts_inner(right, out);
    } else {
        out.push(expr.clone());
    }
}

/// Collect all column identifiers (unqualified and qualified) from an expression.
pub(super) fn collect_referenced_columns(expr: &Expr, columns: &mut HashSet<String>) {
    match expr {
        Expr::Identifier(parts) => {
            // Add the last part (unqualified column name)
            if let Some(last) = parts.last() {
                columns.insert(last.to_ascii_lowercase());
            }
            // Add the qualified form too
            if parts.len() >= 2 {
                columns.insert(
                    parts
                        .iter()
                        .map(|p| p.to_ascii_lowercase())
                        .collect::<Vec<_>>()
                        .join("."),
                );
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
        Expr::Unary { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::FunctionCall { args, filter, .. } => {
            for arg in args {
                collect_referenced_columns(arg, columns);
            }
            if let Some(filter) = filter.as_deref() {
                collect_referenced_columns(filter, columns);
            }
        }
        Expr::Cast { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::IsNull { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::BooleanTest { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_referenced_columns(expr, columns);
            collect_referenced_columns(low, columns);
            collect_referenced_columns(high, columns);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_referenced_columns(expr, columns);
            collect_referenced_columns(pattern, columns);
        }
        Expr::InList { expr, list, .. } => {
            collect_referenced_columns(expr, columns);
            for item in list {
                collect_referenced_columns(item, columns);
            }
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            collect_referenced_columns(operand, columns);
            for (when, then) in when_then {
                collect_referenced_columns(when, columns);
                collect_referenced_columns(then, columns);
            }
            if let Some(e) = else_expr.as_deref() {
                collect_referenced_columns(e, columns);
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                collect_referenced_columns(when, columns);
                collect_referenced_columns(then, columns);
            }
            if let Some(e) = else_expr.as_deref() {
                collect_referenced_columns(e, columns);
            }
        }
        Expr::AnyAll { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
        Expr::Exists(_) | Expr::ScalarSubquery(_) | Expr::InSubquery { .. } => {
            // Subqueries reference their own scope — mark as referencing all columns
            // so they won't be pushed down (they'll stay in remaining_predicate)
            columns.insert("__subquery__".to_string());
        }
        _ => {}
    }
}

/// Check whether all columns referenced by a predicate are available in the given scope.
pub(super) fn predicate_columns_available(predicate: &Expr, scope: &EvalScope) -> bool {
    let mut referenced = HashSet::new();
    collect_referenced_columns(predicate, &mut referenced);

    // If predicate contains subqueries, don't push it down
    if referenced.contains("__subquery__") {
        return false;
    }

    for col in &referenced {
        if !scope.has_column(col) {
            return false;
        }
    }
    true
}

pub(super) async fn relation_index_offsets_for_predicates(
    table: &crate::catalog::Table,
    qualifiers: &[String],
    relation_predicates: &[Expr],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<Option<Vec<usize>>, EngineError> {
    let mut index_descriptors =
        with_storage_read(|storage| storage.index_descriptors_for_table(table.oid()));
    if index_descriptors.is_empty() {
        return Ok(None);
    }
    index_descriptors.sort_by(|left, right| {
        right
            .column_names
            .len()
            .cmp(&left.column_names.len())
            .then(left.name.cmp(&right.name))
    });
    let table_columns = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<HashSet<_>>();
    let mut equality_values: HashMap<String, ScalarValue> = HashMap::new();

    for predicate in relation_predicates {
        let Some((column_name, value)) = extract_relation_equality_constraint(
            predicate,
            qualifiers,
            &table_columns,
            params,
            outer_scope,
        )
        .await?
        else {
            continue;
        };
        if let Some(existing) = equality_values.get(&column_name) {
            if scalar_cmp(existing, &value) != Ordering::Equal {
                return Ok(None);
            }
            continue;
        }
        equality_values.insert(column_name, value);
    }

    if equality_values.is_empty() {
        return Ok(None);
    }

    for descriptor in index_descriptors {
        if !descriptor
            .column_names
            .iter()
            .all(|column| equality_values.contains_key(column))
        {
            continue;
        }
        let key = descriptor
            .column_names
            .iter()
            .filter_map(|column| equality_values.get(column).cloned())
            .collect::<Vec<_>>();
        if key.len() != descriptor.column_names.len() {
            continue;
        }
        let offsets = with_storage_read(|storage| {
            storage.index_offsets_for_key(table.oid(), &descriptor.name, &key)
        });
        return Ok(Some(offsets));
    }
    Ok(None)
}

pub(super) async fn extract_relation_equality_constraint(
    predicate: &Expr,
    qualifiers: &[String],
    table_columns: &HashSet<String>,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<Option<(String, ScalarValue)>, EngineError> {
    let Expr::Binary {
        left,
        op: crate::parser::ast::BinaryOp::Eq,
        right,
    } = predicate
    else {
        return Ok(None);
    };

    if let Some(column_name) = relation_column_from_identifier(left, qualifiers, table_columns) {
        if !expr_is_index_lookup_constant(right) {
            return Ok(None);
        }
        let scope = outer_scope.cloned().unwrap_or_default();
        let value = eval_expr(right, &scope, params).await?;
        return Ok(Some((column_name, value)));
    }
    if let Some(column_name) = relation_column_from_identifier(right, qualifiers, table_columns) {
        if !expr_is_index_lookup_constant(left) {
            return Ok(None);
        }
        let scope = outer_scope.cloned().unwrap_or_default();
        let value = eval_expr(left, &scope, params).await?;
        return Ok(Some((column_name, value)));
    }
    Ok(None)
}

pub(super) fn relation_column_from_identifier(
    expr: &Expr,
    qualifiers: &[String],
    table_columns: &HashSet<String>,
) -> Option<String> {
    let Expr::Identifier(parts) = expr else {
        return None;
    };
    let normalized_parts = parts
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let column_name = normalized_parts.last()?.clone();
    if !table_columns.contains(&column_name) {
        return None;
    }
    if normalized_parts.len() == 1 {
        return Some(column_name);
    }
    let qualifier = normalized_parts[..normalized_parts.len() - 1].join(".");
    if qualifiers.iter().any(|candidate| candidate == &qualifier) {
        Some(column_name)
    } else {
        None
    }
}

pub(super) fn expr_is_index_lookup_constant(expr: &Expr) -> bool {
    let mut referenced = HashSet::new();
    collect_referenced_columns(expr, &mut referenced);
    referenced.is_empty()
}

/// Evaluate the FROM clause with predicate pushdown.
///
/// For each table in the FROM list, after computing the cross product with all
/// previous tables, immediately apply any WHERE conjuncts whose referenced columns
/// are all present in the current scope. This prevents the exponential blowup of
/// computing the full cartesian product before filtering.
///
/// Returns the filtered rows and any remaining predicate conjuncts that couldn't
/// be pushed down (e.g., those with subqueries).
pub(super) async fn evaluate_from_clause_with_pushdown(
    from: &[TableExpression],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    conjuncts: &[Expr],
) -> Result<(Vec<EvalScope>, Option<Expr>), EngineError> {
    let mut current = vec![EvalScope::default()];
    let mut applied: Vec<bool> = vec![false; conjuncts.len()];

    for item in from {
        let mut next = Vec::new();
        match item {
            TableExpression::Function(_)
            | TableExpression::Subquery(SubqueryRef { lateral: true, .. }) => {
                for lhs_scope in &current {
                    let mut merged_outer = lhs_scope.clone();
                    if let Some(outer) = outer_scope {
                        merged_outer.inherit_outer(outer);
                    }
                    let rhs = evaluate_table_expression(item, params, Some(&merged_outer)).await?;
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
            _ => {
                let rhs = evaluate_table_expression(item, params, outer_scope).await?;
                for lhs_scope in &current {
                    for rhs_scope in &rhs.rows {
                        next.push(combine_scopes(lhs_scope, rhs_scope, &HashSet::new()));
                    }
                }
            }
        }

        // Apply pushable predicates — filter early to avoid cartesian blowup
        for (i, conjunct) in conjuncts.iter().enumerate() {
            if next.is_empty() {
                break;
            }
            if applied[i] {
                continue;
            }
            // Check if this conjunct can be evaluated with the current scope
            if predicate_columns_available(conjunct, &next[0]) {
                let mut filtered = Vec::with_capacity(next.len());
                for scope in next {
                    if truthy(&eval_expr(conjunct, &scope, params).await?) {
                        filtered.push(scope);
                    }
                }
                next = filtered;
                applied[i] = true;
            }
        }

        current = next;
    }

    // Build remaining predicate from conjuncts that couldn't be pushed down
    let remaining: Vec<&Expr> = conjuncts
        .iter()
        .zip(applied.iter())
        .filter_map(|(c, &used)| if used { None } else { Some(c) })
        .collect();

    let remaining_predicate = if remaining.is_empty() {
        None
    } else {
        let mut expr = remaining[0].clone();
        for conjunct in &remaining[1..] {
            expr = Expr::Binary {
                op: crate::parser::ast::BinaryOp::And,
                left: Box::new(expr),
                right: Box::new((*conjunct).clone()),
            };
        }
        Some(expr)
    };

    Ok((current, remaining_predicate))
}

pub fn evaluate_table_expression<'a>(
    table: &'a TableExpression,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<TableEval, EngineError>> {
    Box::pin(async move {
        match table {
            TableExpression::Relation(rel) => evaluate_relation(rel, params, outer_scope).await,
            TableExpression::Function(function) => {
                evaluate_table_function(function, params, outer_scope).await
            }
            TableExpression::Subquery(sub) => {
                let result = execute_query_with_outer(&sub.query, params, outer_scope).await?;
                let mut columns = result.columns.clone();
                if !sub.column_aliases.is_empty() {
                    for (idx, alias) in sub.column_aliases.iter().take(columns.len()).enumerate() {
                        columns[idx] = alias.clone();
                    }
                }
                let qualifiers = sub
                    .alias
                    .as_ref()
                    .map(|alias| vec![alias.to_ascii_lowercase()])
                    .unwrap_or_default();
                let mut rows = Vec::with_capacity(result.rows.len());
                for row in &result.rows {
                    rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
                }
                let null_values = vec![ScalarValue::Null; columns.len()];
                let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

                Ok(TableEval {
                    rows,
                    columns,
                    null_scope,
                })
            }
            TableExpression::Join(join) => {
                let left = evaluate_table_expression(&join.left, params, outer_scope).await?;
                if is_lateral_table_expression(&join.right) {
                    evaluate_lateral_join(join, &left, params, outer_scope).await
                } else {
                    let right = evaluate_table_expression(&join.right, params, outer_scope).await?;
                    evaluate_join(
                        join.kind,
                        join.condition.as_ref(),
                        join.natural,
                        &left,
                        &right,
                        params,
                    )
                    .await
                }
            }
        }
    })
}

pub(super) fn is_lateral_table_expression(table: &TableExpression) -> bool {
    matches!(
        table,
        TableExpression::Subquery(SubqueryRef { lateral: true, .. })
    )
}

pub(super) async fn evaluate_lateral_join(
    join: &JoinExpr,
    left: &TableEval,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    if matches!(join.kind, JoinType::Right | JoinType::Full) {
        return Err(EngineError {
            message: "RIGHT/FULL JOIN with LATERAL is not supported".to_string(),
        });
    }

    let mut right_columns: Option<Vec<String>> = None;
    let mut right_null_scope: Option<EvalScope> = None;
    let mut using_columns: Option<Vec<String>> = None;
    let mut using_set: Option<HashSet<String>> = None;
    let empty_set: HashSet<String> = HashSet::new();
    let mut output_rows = Vec::new();

    if left.rows.is_empty() {
        let mut merged_outer = left.null_scope.clone();
        if let Some(outer) = outer_scope {
            merged_outer.inherit_outer(outer);
        }
        let right_eval =
            evaluate_table_expression(&join.right, params, Some(&merged_outer)).await?;
        right_columns = Some(right_eval.columns.clone());
        right_null_scope = Some(right_eval.null_scope);
    } else {
        for left_row in &left.rows {
            let mut merged_outer = left_row.clone();
            if let Some(outer) = outer_scope {
                merged_outer.inherit_outer(outer);
            }
            let right_eval =
                evaluate_table_expression(&join.right, params, Some(&merged_outer)).await?;

            if right_columns.is_none() {
                right_columns = Some(right_eval.columns.clone());
                right_null_scope = Some(right_eval.null_scope.clone());
                let cols = if join.natural {
                    left.columns
                        .iter()
                        .filter(|c| right_eval.columns.iter().any(|r| r.eq_ignore_ascii_case(c)))
                        .cloned()
                        .collect::<Vec<_>>()
                } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                    cols.clone()
                } else {
                    Vec::new()
                };
                let set = cols
                    .iter()
                    .map(|c| c.to_ascii_lowercase())
                    .collect::<HashSet<_>>();
                using_columns = Some(cols);
                using_set = Some(set);
            } else if right_columns.as_ref() != Some(&right_eval.columns) {
                return Err(EngineError {
                    message: "LATERAL subquery returned inconsistent columns".to_string(),
                });
            }

            let mut left_matched = false;
            let cols = using_columns.as_deref().unwrap_or(&[]);
            let set = using_set.as_ref().unwrap_or(&empty_set);
            for right_row in &right_eval.rows {
                let matches = match join.kind {
                    JoinType::Cross => true,
                    _ => {
                        join_condition_matches(
                            join.condition.as_ref(),
                            cols,
                            left_row,
                            right_row,
                            params,
                        )
                        .await?
                    }
                };
                if matches {
                    left_matched = true;
                    output_rows.push(combine_scopes(left_row, right_row, set));
                }
            }

            if !left_matched
                && matches!(join.kind, JoinType::Left)
                && let Some(null_scope) = right_null_scope.as_ref()
            {
                output_rows.push(combine_scopes(left_row, null_scope, set));
            }
        }
    }

    let right_columns = right_columns.unwrap_or_default();
    let using_set = using_set.unwrap_or_default();
    let mut output_columns = left.columns.clone();
    for col in &right_columns {
        if using_set.contains(&col.to_ascii_lowercase()) {
            continue;
        }
        output_columns.push(col.clone());
    }
    let null_scope = combine_scopes(
        &left.null_scope,
        &right_null_scope.unwrap_or_default(),
        &using_set,
    );

    Ok(TableEval {
        rows: output_rows,
        columns: output_columns,
        null_scope,
    })
}

pub(super) async fn evaluate_join(
    join_type: JoinType,
    condition: Option<&JoinCondition>,
    natural: bool,
    left: &TableEval,
    right: &TableEval,
    params: &[Option<String>],
) -> Result<TableEval, EngineError> {
    let using_columns = if natural {
        left.columns
            .iter()
            .filter(|c| right.columns.iter().any(|r| r.eq_ignore_ascii_case(c)))
            .cloned()
            .collect::<Vec<_>>()
    } else if let Some(JoinCondition::Using(cols)) = condition {
        cols.clone()
    } else {
        Vec::new()
    };
    let using_set: HashSet<String> = using_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();

    let mut output_rows = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];

    for left_row in &left.rows {
        let mut left_matched = false;
        for (right_idx, right_row) in right.rows.iter().enumerate() {
            let matches = match join_type {
                JoinType::Cross => true,
                _ => {
                    join_condition_matches(condition, &using_columns, left_row, right_row, params)
                        .await?
                }
            };

            if matches {
                left_matched = true;
                right_matched[right_idx] = true;
                output_rows.push(combine_scopes(left_row, right_row, &using_set));
            }
        }

        if !left_matched && matches!(join_type, JoinType::Left | JoinType::Full) {
            output_rows.push(combine_scopes(left_row, &right.null_scope, &using_set));
        }
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (right_idx, right_row) in right.rows.iter().enumerate() {
            if !right_matched[right_idx] {
                output_rows.push(combine_scopes(&left.null_scope, right_row, &using_set));
            }
        }
    }

    let mut output_columns = left.columns.clone();
    for col in &right.columns {
        if using_set.contains(&col.to_ascii_lowercase()) {
            continue;
        }
        output_columns.push(col.clone());
    }

    let null_scope = combine_scopes(&left.null_scope, &right.null_scope, &using_set);
    Ok(TableEval {
        rows: output_rows,
        columns: output_columns,
        null_scope,
    })
}
pub(super) async fn join_condition_matches(
    condition: Option<&JoinCondition>,
    using_columns: &[String],
    left_row: &EvalScope,
    right_row: &EvalScope,
    params: &[Option<String>],
) -> Result<bool, EngineError> {
    if let Some(JoinCondition::On(expr)) = condition {
        let scope = combine_scopes(left_row, right_row, &HashSet::new());
        return Ok(truthy(&eval_expr(expr, &scope, params).await?));
    }

    if !using_columns.is_empty() {
        for col in using_columns {
            let left_value = left_row
                .lookup_join_column(col)
                .ok_or_else(|| EngineError {
                    message: format!("column \"{col}\" does not exist in left side of JOIN"),
                })?;
            let right_value = right_row
                .lookup_join_column(col)
                .ok_or_else(|| EngineError {
                    message: format!("column \"{col}\" does not exist in right side of JOIN"),
                })?;

            if matches!(left_value, ScalarValue::Null) || matches!(right_value, ScalarValue::Null) {
                return Ok(false);
            }
            if scalar_cmp(&left_value, &right_value) != Ordering::Equal {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
