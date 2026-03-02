#[allow(clippy::wildcard_imports)]
use super::*;

pub async fn execute_query(
    query: &Query,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    execute_query_with_outer(query, params, None).await
}

pub fn execute_query_with_outer<'a>(
    query: &'a Query,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<QueryResult, EngineError>> {
    Box::pin(async move {
        let inherited_ctes = active_cte_context();
        let mut local_ctes = inherited_ctes.clone();

        if let Some(with) = &query.with {
            for cte in &with.ctes {
                let cte_name = cte.name.to_ascii_lowercase();
                let binding = if with.recursive
                    && query_references_relation(&cte.query, &cte_name)
                    && is_recursive_union_expr(&cte.query.body)
                {
                    evaluate_recursive_cte_binding(cte, params, outer_scope, &local_ctes).await?
                } else {
                    let cte_result = with_cte_context_async(local_ctes.clone(), || async {
                        execute_query_with_outer(&cte.query, params, outer_scope).await
                    })
                    .await?;
                    let mut columns = if !cte.column_names.is_empty() {
                        cte.column_names.clone()
                    } else {
                        cte_result.columns.clone()
                    };
                    let (search_idx, cycle_idx, path_idx) =
                        append_cte_aux_columns(&mut columns, cte);
                    let rows = cte_result
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(idx, row)| {
                            let mut normalized = normalize_row_width(row, columns.len());
                            populate_cte_aux_values(
                                &mut normalized,
                                (idx as i64) + 1,
                                search_idx,
                                cycle_idx,
                                path_idx,
                            );
                            normalized
                        })
                        .collect::<Vec<_>>();
                    CteBinding { columns, rows }
                };
                local_ctes.insert(cte_name, binding);
            }
        }

        with_cte_context_async(local_ctes, || async {
            // Check if ORDER BY references columns not in the SELECT output.
            // If so, temporarily augment the query body to include those columns
            // as hidden trailing targets, sort, then strip them.
            let extra_order_cols = collect_extra_order_by_columns(query);
            let (body, num_hidden) = if extra_order_cols.is_empty() {
                (query.body.clone(), 0)
            } else {
                augment_select_for_order_by(&query.body, &extra_order_cols)
            };

            let mut result = execute_query_expr_with_outer(&body, params, outer_scope).await?;
            apply_order_by(&mut result, query, params).await?;

            // Strip hidden ORDER BY columns
            if num_hidden > 0 {
                let visible = result.columns.len().saturating_sub(num_hidden);
                result.columns.truncate(visible);
                for row in &mut result.rows {
                    row.truncate(visible);
                }
            }

            apply_offset_limit(&mut result, query, params).await?;
            Ok(result)
        })
        .await
    })
}

/// Evaluate a recursive CTE to completion, returning all accumulated rows.
///
/// Translated from PostgreSQL's `ExecRecursiveUnion()` in
/// `src/backend/executor/nodeRecursiveunion.c`.
///
/// The algorithm executes the non-recursive term first, then loops executing
/// the recursive term with only the working table until no new rows are
/// produced. UNION mode deduplicates against accumulated results.
///
/// A safety iteration limit (`MAX_RECURSIVE_CTE_ITERATIONS`) prevents genuine
/// infinite recursion from hanging the engine.
pub(super) const MAX_RECURSIVE_CTE_ITERATIONS: usize = 2_048;

pub(super) async fn evaluate_recursive_cte_binding(
    cte: &crate::parser::ast::CommonTableExpr,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    inherited_ctes: &HashMap<String, CteBinding>,
) -> Result<CteBinding, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier,
        right,
    } = &cte.query.body
    else {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must be of the form non-recursive-term UNION [ALL] recursive-term",
                cte.name
            ),
        });
    };
    if *op != SetOperator::Union {
        return Err(EngineError {
            message: format!(
                "recursive query \"{}\" must use UNION or UNION ALL",
                cte.name
            ),
        });
    }
    validate_recursive_cte_terms(&cte.name, &cte_name, left, right)?;

    let seed = with_cte_context_async(inherited_ctes.clone(), || async {
        execute_query_expr_with_outer(left, params, outer_scope).await
    })
    .await?;
    let mut columns = if !cte.column_names.is_empty() {
        cte.column_names.clone()
    } else {
        seed.columns.clone()
    };
    let (search_idx, cycle_idx, path_idx) = append_cte_aux_columns(&mut columns, cte);
    let seed_rows = seed
        .rows
        .into_iter()
        .enumerate()
        .map(|(idx, row)| {
            let mut normalized = normalize_row_width(row, columns.len());
            populate_cte_aux_values(
                &mut normalized,
                (idx as i64) + 1,
                search_idx,
                cycle_idx,
                path_idx,
            );
            normalized
        })
        .collect::<Vec<_>>();
    let mut next_seq = (seed_rows.len() as i64) + 1;
    let mut all_rows = if matches!(quantifier, SetQuantifier::Distinct) {
        dedupe_rows(seed_rows.clone())
    } else {
        seed_rows.clone()
    };
    let mut working_rows = all_rows.clone();

    let mut iterations = 0usize;
    while !working_rows.is_empty() {
        if iterations >= MAX_RECURSIVE_CTE_ITERATIONS {
            break;
        }
        iterations += 1;

        let mut context = inherited_ctes.clone();
        context.insert(
            cte_name.clone(),
            CteBinding {
                columns: columns.clone(),
                rows: working_rows.clone(),
            },
        );
        let recursive_term = with_cte_context_async(context, || async {
            execute_query_expr_with_outer(right, params, outer_scope).await
        })
        .await?;
        let mut next_rows = recursive_term
            .rows
            .into_iter()
            .map(|row| {
                let mut normalized = normalize_row_width(row, columns.len());
                populate_cte_aux_values(&mut normalized, next_seq, search_idx, cycle_idx, path_idx);
                next_seq += 1;
                normalized
            })
            .collect::<Vec<_>>();
        if matches!(quantifier, SetQuantifier::Distinct) {
            let mut seen = all_rows
                .iter()
                .map(|row| row_key(row))
                .collect::<HashSet<_>>();
            let mut filtered = Vec::new();
            for row in next_rows {
                let key = row_key(&row);
                if seen.insert(key) {
                    filtered.push(row);
                }
            }
            next_rows = filtered;
        }

        if next_rows.is_empty() {
            break;
        }
        all_rows.extend(next_rows.iter().cloned());
        working_rows = next_rows;
    }

    Ok(CteBinding {
        columns,
        rows: all_rows,
    })
}

pub(super) fn execute_query_expr_with_outer<'a>(
    expr: &'a QueryExpr,
    params: &'a [Option<String>],
    outer_scope: Option<&'a EvalScope>,
) -> EngineFuture<'a, Result<QueryResult, EngineError>> {
    Box::pin(async move {
        match expr {
            QueryExpr::Select(select) => execute_select(select, params, outer_scope).await,
            QueryExpr::Nested(query) => execute_query_with_outer(query, params, outer_scope).await,
            QueryExpr::SetOperation {
                left,
                op,
                quantifier,
                right,
            } => execute_set_operation(left, *op, *quantifier, right, params, outer_scope).await,
            QueryExpr::Values(rows) => execute_values(rows, params, outer_scope).await,
            QueryExpr::Insert(insert) => Ok(QueryResult {
                columns: derive_dml_returning_columns(&insert.table_name, &insert.returning)
                    .unwrap_or_default(),
                rows: Vec::new(),
                command_tag: "INSERT 0".to_string(),
                rows_affected: 0,
            }),
            QueryExpr::Update(update) => Ok(QueryResult {
                columns: derive_dml_returning_columns(&update.table_name, &update.returning)
                    .unwrap_or_default(),
                rows: Vec::new(),
                command_tag: "UPDATE 0".to_string(),
                rows_affected: 0,
            }),
            QueryExpr::Delete(delete) => Ok(QueryResult {
                columns: derive_dml_returning_columns(&delete.table_name, &delete.returning)
                    .unwrap_or_default(),
                rows: Vec::new(),
                command_tag: "DELETE 0".to_string(),
                rows_affected: 0,
            }),
        }
    })
}

pub(super) async fn execute_values(
    rows: &[Vec<Expr>],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    // Execute VALUES query - return all rows with column names column1, column2, etc.
    let ncols = rows.first().map(|r| r.len()).unwrap_or(0);

    // Generate column names: column1, column2, ...
    let columns = (1..=ncols)
        .map(|i| format!("column{i}"))
        .collect::<Vec<_>>();

    let mut result_rows = Vec::new();
    let scope = outer_scope.cloned().unwrap_or_default();

    for row_exprs in rows {
        let mut row_values = Vec::new();
        for expr in row_exprs {
            let value = eval_expr(expr, &scope, params).await?;
            row_values.push(value);
        }
        result_rows.push(row_values);
    }

    let row_count = result_rows.len() as u64;
    Ok(QueryResult {
        columns,
        rows: result_rows,
        command_tag: String::new(),
        rows_affected: row_count,
    })
}

/// Check if any FROM table function has dynamic columns (unknown until execution).
pub(super) fn from_has_dynamic_columns(from: &[TableExpression]) -> bool {
    const DYNAMIC_FUNCTIONS: &[&str] = &["json_table", "iceberg_scan"];
    for item in from {
        if let TableExpression::Function(f) = item
            && f.column_aliases.is_empty()
            && let Some(name) = f.name.last()
            && DYNAMIC_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str())
        {
            return true;
        }
    }
    false
}

pub(super) async fn execute_select(
    select: &SelectStatement,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let cte_columns = active_cte_context()
        .into_iter()
        .map(|(name, binding)| (name, binding.columns))
        .collect::<HashMap<_, _>>();
    let has_wildcard = select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard | Expr::QualifiedWildcard(_)));
    let has_dynamic_from = has_wildcard && from_has_dynamic_columns(&select.from);
    let wildcard_columns = if has_wildcard && !has_dynamic_from {
        Some(expand_from_columns(&select.from, &cte_columns)?)
    } else {
        None
    };
    let columns = if has_dynamic_from {
        Vec::new()
    } else {
        derive_select_columns(select, &cte_columns)?
    };
    let mut rows = Vec::new();

    let has_aggregate = select
        .targets
        .iter()
        .any(|target| contains_aggregate_expr(&target.expr))
        || select.having.as_ref().is_some_and(contains_aggregate_expr);
    let has_window = select
        .targets
        .iter()
        .any(|target| contains_window_expr(&target.expr));

    if select
        .where_clause
        .as_ref()
        .is_some_and(contains_window_expr)
    {
        return Err(EngineError {
            message: "window functions are not allowed in WHERE".to_string(),
        });
    }
    if group_by_contains_window_expr(&select.group_by) {
        return Err(EngineError {
            message: "window functions are not allowed in GROUP BY".to_string(),
        });
    }
    if select.having.as_ref().is_some_and(contains_window_expr) {
        return Err(EngineError {
            message: "window functions are not allowed in HAVING".to_string(),
        });
    }

    // For dynamic-column table functions (e.g. json_table), evaluate FROM first
    // to discover columns, then derive wildcard_columns and output columns.
    // This avoids a double evaluation — the source_rows are captured here.
    let (wildcard_columns, columns, dynamic_source) = if has_dynamic_from {
        let mut discovered_columns = Vec::new();
        let mut all_rows = vec![EvalScope::default()];
        for item in &select.from {
            let table_eval = evaluate_table_expression(item, params, outer_scope).await?;
            let qualifier = match item {
                TableExpression::Function(f) => f
                    .alias
                    .as_ref()
                    .map(|a| a.to_ascii_lowercase())
                    .or_else(|| f.name.last().map(|n| n.to_ascii_lowercase())),
                _ => None,
            };
            for col in &table_eval.columns {
                let lookup_parts = if let Some(q) = &qualifier {
                    vec![q.clone(), col.clone()]
                } else {
                    vec![col.clone()]
                };
                discovered_columns.push(ExpandedFromColumn {
                    label: col.clone(),
                    lookup_parts,
                });
            }
            // Cross-join with accumulated rows
            let mut next = Vec::new();
            for lhs in &all_rows {
                for rhs in &table_eval.rows {
                    next.push(combine_scopes(lhs, rhs, &HashSet::new()));
                }
            }
            all_rows = next;
        }
        // Build output columns from wildcard expansion
        let mut out_columns = Vec::new();
        for target in &select.targets {
            if matches!(target.expr, Expr::Wildcard) {
                for col in &discovered_columns {
                    out_columns.push(col.label.clone());
                }
            } else if let Expr::QualifiedWildcard(qualifier) = &target.expr {
                let qualifier_lower = qualifier
                    .last()
                    .map(|s| s.to_ascii_lowercase())
                    .unwrap_or_default();
                for col in &discovered_columns {
                    if col.lookup_parts.len() >= 2
                        && col.lookup_parts[0].to_ascii_lowercase() == qualifier_lower
                    {
                        out_columns.push(col.label.clone());
                    }
                }
            } else if let Some(alias) = &target.alias {
                out_columns.push(alias.clone());
            } else {
                let name = match &target.expr {
                    Expr::Identifier(parts) => parts
                        .last()
                        .cloned()
                        .unwrap_or_else(|| "?column?".to_string()),
                    Expr::FunctionCall { name, .. } => name
                        .last()
                        .cloned()
                        .unwrap_or_else(|| "?column?".to_string()),
                    _ => "?column?".to_string(),
                };
                out_columns.push(name);
            }
        }
        (Some(discovered_columns), out_columns, Some(all_rows))
    } else {
        (wildcard_columns, columns, None)
    };

    let (mut source_rows, remaining_predicate) = if let Some(rows) = dynamic_source {
        (rows, select.where_clause.clone())
    } else if select.from.len() >= 2
        && let Some(where_clause) = select.where_clause.as_ref()
    {
        let conjuncts = decompose_and_conjuncts(where_clause);
        evaluate_from_clause_with_pushdown(&select.from, params, outer_scope, &conjuncts).await?
    } else {
        let source = if select.from.is_empty() {
            vec![outer_scope.cloned().unwrap_or_default()]
        } else if select.from.len() == 1 {
            match &select.from[0] {
                TableExpression::Relation(rel) => {
                    let relation_predicates = select
                        .where_clause
                        .as_ref()
                        .map_or_else(Vec::new, decompose_and_conjuncts);
                    evaluate_relation_with_predicates(
                        rel,
                        params,
                        outer_scope,
                        &relation_predicates,
                    )
                    .await?
                    .rows
                }
                _ => evaluate_from_clause(&select.from, params, outer_scope).await?,
            }
        } else {
            evaluate_from_clause(&select.from, params, outer_scope).await?
        };
        (source, select.where_clause.clone())
    };

    if let Some(outer) = outer_scope
        && !select.from.is_empty()
    {
        for scope in &mut source_rows {
            scope.inherit_outer(outer);
        }
    }

    let filtered_rows = if let Some(predicate) = &remaining_predicate {
        let mut rows = Vec::with_capacity(source_rows.len());
        for scope in source_rows {
            if !truthy(&eval_expr(predicate, &scope, params).await?) {
                continue;
            }
            rows.push(scope);
        }
        rows
    } else {
        source_rows
    };

    if !select.group_by.is_empty() || has_aggregate {
        if has_wildcard {
            return Err(EngineError {
                message: "wildcard target with grouped/aggregate projection is not implemented"
                    .to_string(),
            });
        }

        for expr in group_by_exprs(&select.group_by) {
            if contains_aggregate_expr(expr) {
                return Err(EngineError {
                    message: "aggregate functions are not allowed in GROUP BY".to_string(),
                });
            }
        }
        // Build a map of SELECT aliases for GROUP BY resolution.
        // PostgreSQL allows GROUP BY to reference output column names (aliases).
        let select_alias_map: HashMap<String, &Expr> = select
            .targets
            .iter()
            .filter_map(|target| {
                target
                    .alias
                    .as_ref()
                    .map(|alias| (alias.to_ascii_lowercase(), &target.expr))
            })
            .collect();

        let grouping_sets = expand_grouping_sets(&select.group_by);
        let all_grouping = collect_grouping_identifiers(&select.group_by);

        for grouping_set in grouping_sets {
            let current_grouping: HashSet<String> = grouping_set
                .iter()
                .filter_map(|expr| identifier_key(expr))
                .collect();
            let grouping_context = GroupingContext {
                current_grouping,
                all_grouping: all_grouping.clone(),
            };

            let mut groups: Vec<Vec<EvalScope>> = Vec::new();
            if grouping_set.is_empty() {
                groups.push(filtered_rows.clone());
            } else {
                let mut index_by_key: HashMap<String, usize> = HashMap::new();
                for scope in &filtered_rows {
                    let key_values = grouping_set
                        .iter()
                        .map(|expr| {
                            // Resolve GROUP BY aliases: if the expression is a simple
                            // identifier that matches a SELECT alias, use the aliased
                            // expression instead.
                            let resolved = resolve_group_by_alias(expr, &select_alias_map);
                            eval_expr(resolved, scope, params)
                        })
                        .collect::<Vec<_>>();
                    let key_values = {
                        let mut values = Vec::with_capacity(key_values.len());
                        for value in key_values {
                            values.push(value.await?);
                        }
                        values
                    };
                    let key = row_key(&key_values);
                    let idx = if let Some(existing) = index_by_key.get(&key) {
                        *existing
                    } else {
                        let idx = groups.len();
                        groups.push(Vec::new());
                        index_by_key.insert(key, idx);
                        idx
                    };
                    groups[idx].push(scope.clone());
                }
            }

            for group_rows in groups {
                let representative = group_rows.first().cloned().unwrap_or_default();
                if let Some(having) = &select.having {
                    let having_value = eval_group_expr(
                        having,
                        &group_rows,
                        &representative,
                        params,
                        &grouping_context,
                    )
                    .await?;
                    if !truthy(&having_value) {
                        continue;
                    }
                }

                let mut row = Vec::new();
                for target in &select.targets {
                    if matches!(target.expr, Expr::Wildcard) {
                        return Err(EngineError {
                            message: "wildcard target is not yet implemented in executor"
                                .to_string(),
                        });
                    }
                    row.push(
                        eval_group_expr(
                            &target.expr,
                            &group_rows,
                            &representative,
                            params,
                            &grouping_context,
                        )
                        .await?,
                    );
                }
                rows.push(row);
            }
        }
    } else if has_window {
        for (row_idx, scope) in filtered_rows.iter().enumerate() {
            let row = project_select_row_with_window(
                &select.targets,
                scope,
                row_idx,
                &filtered_rows,
                &select.window_definitions,
                params,
                wildcard_columns.as_deref(),
            )
            .await?;
            rows.push(row);
        }
    } else {
        for scope in filtered_rows {
            let row =
                project_select_row(&select.targets, &scope, params, wildcard_columns.as_deref())
                    .await?;
            rows.push(row);
        }
    }

    if matches!(select.quantifier, Some(SelectQuantifier::Distinct)) {
        if !select.distinct_on.is_empty() {
            // DISTINCT ON: keep first row for each distinct value of the ON expressions
            // Evaluate the exprs against a scope built from the projected columns
            let mut seen = HashSet::new();
            let mut deduped = Vec::new();
            for row in &rows {
                let scope = scope_from_row(&columns, row, &[], &columns);
                let mut key_parts = Vec::new();
                for expr in &select.distinct_on {
                    let val = eval_expr(expr, &scope, params).await?;
                    key_parts.push(val.render());
                }
                let key_str = key_parts.join("\0");
                if seen.insert(key_str) {
                    deduped.push(row.clone());
                }
            }
            rows = deduped;
        } else {
            rows = dedupe_rows(rows);
        }
    }

    Ok(QueryResult {
        columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}
