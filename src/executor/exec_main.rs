use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_mm256_setzero_si256, _mm256_loadu_si256, _mm256_add_epi64, _mm256_storeu_si256, _mm256_setzero_pd, _mm256_loadu_pd, _mm256_add_pd, _mm256_storeu_pd, _mm256_set1_epi64x, _mm256_cmpgt_epi64, _mm256_blendv_epi8, _mm256_set1_pd, _mm256_min_pd, _mm256_max_pd};

use crate::catalog::{SearchPath, TableKind, TypeSignature, with_catalog_read};
use crate::executor::exec_expr::{
    EngineFuture, EvalScope, eval_any_all, eval_between_predicate, eval_binary, eval_cast_scalar,
    eval_expr, eval_expr_with_window, eval_is_distinct_from, eval_like_predicate, eval_unary,
    execute_ws_messages, is_ws_extension_loaded,
};
use crate::parser::ast::SubqueryRef;
use crate::parser::ast::{
    Expr, GroupByExpr, JoinCondition, JoinExpr, JoinType, OrderByExpr, Query, QueryExpr,
    SelectItem, SelectQuantifier, SelectStatement, SetOperator, SetQuantifier, TableExpression,
    TableFunctionRef, TableRef, WindowFrameBound,
};
use crate::security::{self, RlsCommand, TablePrivilege};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::{
    CteBinding, EngineError, ExpandedFromColumn, QueryResult, active_cte_context,
    current_cte_binding, derive_select_columns, expand_from_columns, lookup_virtual_relation,
    query_references_relation, relation_row_visible_for_command, require_relation_privilege,
    type_signature_to_oid, validate_recursive_cte_terms, with_cte_context_async, with_ext_read,
    with_storage_read,
};
use crate::utils::adt::json::{
    json_value_text_output, jsonb_path_query_values, parse_json_document_arg, scalar_to_json_value,
};
#[cfg(target_arch = "wasm32")]
use crate::tcop::engine::{drain_wasm_ws_messages, sync_wasm_ws_state};
use crate::utils::adt::misc::{
    compare_values_for_predicate, eval_regexp_matches_set_function,
    eval_regexp_split_to_table_set_function, parse_f64_numeric_scalar, render_expr_to_sql, truthy,
};
use crate::utils::fmgr::eval_scalar_function;
use serde_json::{Map as JsonMap, Value as JsonValue};

pub(crate) async fn execute_query(
    query: &Query,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    execute_query_with_outer(query, params, None).await
}

pub(crate) fn execute_query_with_outer<'a>(
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
                let binding = if with.recursive && query_references_relation(&cte.query, &cte_name)
                {
                    evaluate_recursive_cte_binding(cte, params, outer_scope, &local_ctes).await?
                } else {
                    let cte_result = with_cte_context_async(local_ctes.clone(), || async {
                        execute_query_with_outer(&cte.query, params, outer_scope).await
                    })
                    .await?;
                    let columns = if !cte.column_names.is_empty() {
                        if cte.column_names.len() != cte_result.columns.len() {
                            return Err(EngineError {
                                message: format!(
                                    "WITH query \"{}\" has {} columns available but {} columns specified",
                                    cte.name,
                                    cte_result.columns.len(),
                                    cte.column_names.len()
                                ),
                            });
                        }
                        cte.column_names.clone()
                    } else {
                        cte_result.columns.clone()
                    };
                    CteBinding {
                        columns,
                        rows: cte_result.rows.clone(),
                    }
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

            let mut result =
                execute_query_expr_with_outer(&body, params, outer_scope).await?;
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
const MAX_RECURSIVE_CTE_ITERATIONS: usize = 10_000;

async fn evaluate_recursive_cte_binding(
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
    let columns = if !cte.column_names.is_empty() {
        if cte.column_names.len() != seed.columns.len() {
            return Err(EngineError {
                message: format!(
                    "WITH query \"{}\" has {} columns available but {} columns specified",
                    cte.name,
                    seed.columns.len(),
                    cte.column_names.len()
                ),
            });
        }
        cte.column_names.clone()
    } else {
        seed.columns.clone()
    };
    let mut all_rows = if matches!(quantifier, SetQuantifier::Distinct) {
        dedupe_rows(seed.rows.clone())
    } else {
        seed.rows.clone()
    };
    let mut working_rows = all_rows.clone();

    let mut iterations = 0usize;
    while !working_rows.is_empty() {
        if iterations >= MAX_RECURSIVE_CTE_ITERATIONS {
            return Err(EngineError {
                message: format!(
                    "recursive query \"{}\" exceeded {} iterations",
                    cte.name, MAX_RECURSIVE_CTE_ITERATIONS
                ),
            });
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

        if recursive_term.columns.len() != columns.len() {
            return Err(EngineError {
                message: "set-operation inputs must have matching column counts".to_string(),
            });
        }

        let mut next_rows = recursive_term.rows;
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

fn execute_query_expr_with_outer<'a>(
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
            QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
                Err(EngineError {
                    message: "data-modifying statements in WITH are not yet fully supported"
                        .to_string(),
                })
            }
        }
    })
}

async fn execute_values(
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

async fn execute_select(
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
    let wildcard_columns = if has_wildcard {
        Some(expand_from_columns(&select.from, &cte_columns)?)
    } else {
        None
    };
    let columns = derive_select_columns(select, &cte_columns)?;
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

    // Predicate pushdown: when there are multiple FROM tables and a WHERE clause,
    // decompose the WHERE into conjuncts and push applicable predicates into the
    // FROM clause evaluation, applying them incrementally as tables are joined.
    // This avoids computing the full cartesian product before filtering.
    let (mut source_rows, remaining_predicate) =
        if let Some(where_clause) = select.where_clause.as_ref().filter(|_| select.from.len() >= 2) {
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

#[derive(Debug, Clone)]
pub(crate) struct TableEval {
    pub(crate) rows: Vec<EvalScope>,
    pub(crate) columns: Vec<String>,
    pub(crate) null_scope: EvalScope,
}

pub(crate) async fn evaluate_from_clause(
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
fn decompose_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    decompose_and_conjuncts_inner(expr, &mut conjuncts);
    conjuncts
}

fn decompose_and_conjuncts_inner(expr: &Expr, out: &mut Vec<Expr>) {
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
fn collect_referenced_columns(expr: &Expr, columns: &mut HashSet<String>) {
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
fn predicate_columns_available(predicate: &Expr, scope: &EvalScope) -> bool {
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

async fn relation_index_offsets_for_predicates(
    table: &crate::catalog::Table,
    qualifiers: &[String],
    relation_predicates: &[Expr],
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<Option<Vec<usize>>, EngineError> {
    let mut index_descriptors = with_storage_read(|storage| {
        storage.index_descriptors_for_table(table.oid())
    });
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

async fn extract_relation_equality_constraint(
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

fn relation_column_from_identifier(
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

fn expr_is_index_lookup_constant(expr: &Expr) -> bool {
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
async fn evaluate_from_clause_with_pushdown(
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

pub(crate) fn evaluate_table_expression<'a>(
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
                let qualifiers = sub
                    .alias
                    .as_ref()
                    .map(|alias| vec![alias.to_ascii_lowercase()])
                    .unwrap_or_default();
                let mut rows = Vec::with_capacity(result.rows.len());
                for row in &result.rows {
                    rows.push(scope_from_row(
                        &result.columns,
                        row,
                        &qualifiers,
                        &result.columns,
                    ));
                }
                let null_values = vec![ScalarValue::Null; result.columns.len()];
                let null_scope =
                    scope_from_row(&result.columns, &null_values, &qualifiers, &result.columns);

                Ok(TableEval {
                    rows,
                    columns: result.columns,
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

fn is_lateral_table_expression(table: &TableExpression) -> bool {
    matches!(
        table,
        TableExpression::Subquery(SubqueryRef { lateral: true, .. })
    )
}

async fn evaluate_lateral_join(
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

async fn evaluate_table_function(
    function: &TableFunctionRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    let mut scope = EvalScope::default();
    if let Some(outer) = outer_scope {
        scope.inherit_outer(outer);
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        args.push(eval_expr(arg, &scope, params).await?);
    }

    let (mut columns, rows) = evaluate_set_returning_function(function, &args).await?;
    if !function.column_aliases.is_empty() {
        if function.column_aliases.len() != columns.len() {
            return Err(EngineError {
                message: format!(
                    "table function {} expects {} column aliases, got {}",
                    function
                        .name
                        .last()
                        .map(String::as_str)
                        .unwrap_or("function"),
                    columns.len(),
                    function.column_aliases.len()
                ),
            });
        }
        columns = function.column_aliases.clone();
    }

    let qualifiers = function
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .or_else(|| {
            function
                .name
                .last()
                .map(|name| vec![name.to_ascii_lowercase()])
        })
        .unwrap_or_default();
    let mut scoped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
    })
}

async fn evaluate_set_returning_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();

    match fn_name.as_str() {
        "json_array_elements" | "jsonb_array_elements" => {
            eval_json_array_elements_set_function(args, false, &fn_name)
        }
        "json_array_elements_text" | "jsonb_array_elements_text" => {
            eval_json_array_elements_set_function(args, true, &fn_name)
        }
        "json_each" | "jsonb_each" => eval_json_each_set_function(args, false, &fn_name),
        "json_each_text" | "jsonb_each_text" => eval_json_each_set_function(args, true, &fn_name),
        "json_object_keys" | "jsonb_object_keys" => {
            eval_json_object_keys_set_function(args, &fn_name)
        }
        "jsonb_path_query" => eval_jsonb_path_query_set_function(args, &fn_name),
        "json_to_record" | "jsonb_to_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, false)
        }
        "json_to_recordset" | "jsonb_to_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, false)
        }
        "json_populate_record" | "jsonb_populate_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, true)
        }
        "json_populate_recordset" | "jsonb_populate_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, true)
        }
        "regexp_matches" => eval_regexp_matches_set_function(args, &fn_name),
        "regexp_split_to_table" => eval_regexp_split_to_table_set_function(args, &fn_name),
        "generate_series" => eval_generate_series(args, &fn_name),
        "unnest" => eval_unnest_set_function(args, &fn_name),
        "pg_get_keywords" => eval_pg_get_keywords(),
        "messages" if function.name.len() == 2 && function.name[0].eq_ignore_ascii_case("ws") => {
            execute_ws_messages(args).await
        }
        _ => Err(EngineError {
            message: format!(
                "unsupported set-returning table function {}",
                function
                    .name
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
        }),
    }
}

fn eval_json_array_elements_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };

    let rows = items
        .iter()
        .map(|item| {
            let value = if text_mode {
                json_value_text_output(item)
            } else {
                ScalarValue::Text(item.to_string())
            };
            vec![value]
        })
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

fn eval_json_each_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string(), "value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .iter()
        .map(|(key, value)| {
            let value_col = if text_mode {
                json_value_text_output(value)
            } else {
                ScalarValue::Text(value.to_string())
            };
            vec![ScalarValue::Text(key.clone()), value_col]
        })
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string(), "value".to_string()], rows))
}

fn eval_json_object_keys_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .keys()
        .map(|key| vec![ScalarValue::Text(key.clone())])
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string()], rows))
}

pub(crate) fn json_value_to_scalar(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(v) => ScalarValue::Bool(*v),
        JsonValue::Number(n) => {
            if let Some(int) = n.as_i64() {
                ScalarValue::Int(int)
            } else if let Some(float) = n.as_f64() {
                ScalarValue::Float(float)
            } else {
                ScalarValue::Text(n.to_string())
            }
        }
        JsonValue::String(v) => ScalarValue::Text(v.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

fn eval_jsonb_path_query_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let values = jsonb_path_query_values(args, fn_name)?;
    let rows = values
        .into_iter()
        .map(|value| vec![ScalarValue::Text(value.to_string())])
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

fn eval_json_record_table_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
    fn_name: &str,
    recordset: bool,
    populate: bool,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if function.column_aliases.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires column aliases (for example AS t(col1, col2))"),
        });
    }
    let expected_args = if populate { 2 } else { 1 };
    if args.len() != expected_args {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly {expected_args} argument(s)"),
        });
    }

    let mut base = JsonMap::new();
    if populate && !matches!(args[0], ScalarValue::Null) {
        let base_value = parse_json_document_arg(&args[0], fn_name, 1)?;
        let JsonValue::Object(base_obj) = base_value else {
            return Err(EngineError {
                message: format!("{fn_name}() base argument must be a JSON object"),
            });
        };
        base = base_obj;
    }

    let json_arg_idx = if populate { 2 } else { 1 };
    if matches!(args[json_arg_idx - 1], ScalarValue::Null) {
        if recordset {
            return Ok((function.column_aliases.clone(), Vec::new()));
        }
        return Ok((
            function.column_aliases.clone(),
            vec![vec![ScalarValue::Null; function.column_aliases.len()]],
        ));
    }

    let source = parse_json_document_arg(&args[json_arg_idx - 1], fn_name, json_arg_idx)?;
    let objects = if recordset {
        let JsonValue::Array(items) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an array of objects"),
            });
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Object(map) = item else {
                return Err(EngineError {
                    message: format!("{fn_name}() JSON input must be an array of objects"),
                });
            };
            out.push(map);
        }
        out
    } else {
        let JsonValue::Object(map) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an object"),
            });
        };
        vec![map]
    };

    let mut rows = Vec::with_capacity(objects.len());
    for object in objects {
        let mut merged = base.clone();
        for (key, value) in object {
            merged.insert(key, value);
        }
        let mut row = Vec::with_capacity(function.column_aliases.len());
        for (idx, column) in function.column_aliases.iter().enumerate() {
            let mut value = merged
                .get(column)
                .map(json_value_to_scalar)
                .unwrap_or(ScalarValue::Null);
            if let Some(type_name) = function
                .column_alias_types
                .get(idx)
                .and_then(|entry| entry.as_deref())
            {
                value = eval_cast_scalar(value, type_name)?;
            }
            row.push(value);
        }
        rows.push(row);
    }

    Ok((function.column_aliases.clone(), rows))
}

fn eval_generate_series(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok((vec!["generate_series".to_string()], Vec::new()));
    }
    // Try integer series first
    let start = match &args[0] {
        ScalarValue::Int(i) => *i as f64,
        ScalarValue::Float(f) => *f,
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}() expects numeric arguments"),
            });
        }
    };
    let stop = match &args[1] {
        ScalarValue::Int(i) => *i as f64,
        ScalarValue::Float(f) => *f,
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}() expects numeric arguments"),
            });
        }
    };
    let step = if args.len() == 3 {
        match &args[2] {
            ScalarValue::Int(i) => *i as f64,
            ScalarValue::Float(f) => *f,
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() expects numeric step"),
                });
            }
        }
    } else if start <= stop {
        1.0
    } else {
        -1.0
    };
    if step == 0.0 {
        return Err(EngineError {
            message: "step size cannot be zero".to_string(),
        });
    }
    let use_int = matches!(
        (&args[0], &args[1]),
        (ScalarValue::Int(_), ScalarValue::Int(_))
    ) && (args.len() < 3 || matches!(&args[2], ScalarValue::Int(_)));
    let mut rows = Vec::new();
    let mut current = start;
    let max_rows = 1_000_000;
    loop {
        if rows.len() >= max_rows {
            break;
        }
        if step > 0.0 && current > stop {
            break;
        }
        if step < 0.0 && current < stop {
            break;
        }
        if use_int {
            rows.push(vec![ScalarValue::Int(current as i64)]);
        } else {
            rows.push(vec![ScalarValue::Float(current)]);
        }
        current += step;
    }
    Ok((vec!["generate_series".to_string()], rows))
}

fn eval_unnest_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects one argument"),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let text = args[0].render();
    let inner = text.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let rows: Vec<Vec<ScalarValue>> = inner
        .split(',')
        .map(|p| {
            let p = p.trim();
            if p == "NULL" {
                vec![ScalarValue::Null]
            } else {
                vec![ScalarValue::Text(p.to_string())]
            }
        })
        .collect();
    Ok((vec!["unnest".to_string()], rows))
}

fn eval_pg_get_keywords() -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let keywords = vec![
        ("select", "R", "reserved"),
        ("from", "R", "reserved"),
        ("where", "R", "reserved"),
        ("insert", "U", "unreserved"),
        ("update", "U", "unreserved"),
        ("delete", "U", "unreserved"),
        ("create", "U", "unreserved"),
        ("drop", "U", "unreserved"),
        ("alter", "U", "unreserved"),
        ("table", "U", "unreserved"),
    ];
    let columns = vec![
        "word".to_string(),
        "catcode".to_string(),
        "catdesc".to_string(),
    ];
    let rows = keywords
        .into_iter()
        .map(|(w, c, d)| {
            vec![
                ScalarValue::Text(w.to_string()),
                ScalarValue::Text(c.to_string()),
                ScalarValue::Text(d.to_string()),
            ]
        })
        .collect();
    Ok((columns, rows))
}

async fn evaluate_relation(
    rel: &TableRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    evaluate_relation_with_predicates(rel, params, outer_scope, &[]).await
}

async fn evaluate_relation_with_predicates(
    rel: &TableRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    relation_predicates: &[Expr],
) -> Result<TableEval, EngineError> {
    if rel.name.len() == 1
        && let Some(cte) = current_cte_binding(&rel.name[0])
    {
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![rel.name[0].to_ascii_lowercase()]
        };

        let mut scoped_rows = Vec::with_capacity(cte.rows.len());
        for row in &cte.rows {
            scoped_rows.push(scope_from_row(&cte.columns, row, &qualifiers, &cte.columns));
        }
        let null_values = vec![ScalarValue::Null; cte.columns.len()];
        let null_scope = scope_from_row(&cte.columns, &null_values, &qualifiers, &cte.columns);

        return Ok(TableEval {
            rows: scoped_rows,
            columns: cte.columns,
            null_scope,
        });
    }

    if let Some((schema_name, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
        let rows = virtual_relation_rows(&schema_name, &relation_name)?;
        let column_names = columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<Vec<_>>();
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![
                relation_name.to_ascii_lowercase(),
                format!("{}.{}", schema_name, relation_name),
            ]
        };
        let mut scoped_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            scoped_rows.push(scope_from_row(
                &column_names,
                row,
                &qualifiers,
                &column_names,
            ));
        }
        let null_values = vec![ScalarValue::Null; column_names.len()];
        let null_scope = scope_from_row(&column_names, &null_values, &qualifiers, &column_names);
        return Ok(TableEval {
            rows: scoped_rows,
            columns: column_names,
            null_scope,
        });
    }

    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let index_offsets = if relation_predicates.is_empty() {
        None
    } else {
        relation_index_offsets_for_predicates(
            &table,
            &qualifiers,
            relation_predicates,
            params,
            outer_scope,
        )
        .await?
    };

    let (columns, mut rows) = match table.kind() {
        TableKind::VirtualDual => (Vec::new(), vec![Vec::new()]),
        TableKind::Heap | TableKind::MaterializedView => {
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            let rows = with_storage_read(|storage| {
                let all_rows = storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default();
                if let Some(offsets) = &index_offsets {
                    offsets
                        .iter()
                        .filter_map(|offset| all_rows.get(*offset).cloned())
                        .collect::<Vec<_>>()
                } else {
                    all_rows
                }
            });
            (columns, rows)
        }
        TableKind::View => {
            let definition = table.view_definition().ok_or_else(|| EngineError {
                message: format!(
                    "view definition for relation \"{}\" is missing",
                    table.qualified_name()
                ),
            })?;
            let result = execute_query_with_outer(definition, params, outer_scope).await?;
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            if result.columns.len() != columns.len() {
                return Err(EngineError {
                    message: format!(
                        "view \"{}\" has invalid column definition",
                        table.qualified_name()
                    ),
                });
            }
            (columns, result.rows)
        }
    };
    if table.kind() != TableKind::VirtualDual {
        require_relation_privilege(&table, TablePrivilege::Select)?;
        let mut visible_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if relation_row_visible_for_command(&table, &row, RlsCommand::Select, params).await? {
                visible_rows.push(row);
            }
        }
        rows = visible_rows;
    }

    let mut scoped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
    })
}

fn virtual_relation_rows(
    schema: &str,
    relation: &str,
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    match (schema, relation) {
        ("pg_catalog", "pg_namespace") => {
            let mut entries = with_catalog_read(|catalog| {
                catalog
                    .schemas()
                    .map(|schema| (schema.oid(), schema.name().to_string()))
                    .collect::<Vec<_>>()
            });
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(|(oid, name)| vec![ScalarValue::Int(oid as i64), ScalarValue::Text(name)])
                .collect())
        }
        ("pg_catalog", "pg_class") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let has_index = !table.indexes().is_empty()
                            || !table.key_constraints().is_empty();
                        out.push((
                            table.oid(),
                            table.name().to_string(),
                            schema.oid(),
                            pg_relkind_for_table(table.kind()).to_string(),
                            has_index,
                        ));
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(oid, relname, relnamespace, relkind, relhasindex)| {
                    vec![
                        ScalarValue::Int(oid as i64),
                        ScalarValue::Text(relname),
                        ScalarValue::Int(relnamespace as i64),
                        ScalarValue::Text(relkind),
                        ScalarValue::Int(10), // relowner: superuser OID
                        ScalarValue::Bool(relhasindex),
                        ScalarValue::Bool(false), // relhasrules
                        ScalarValue::Bool(false), // relhastriggers
                        ScalarValue::Bool(false), // relisshared
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_attribute") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            out.push((
                                table.oid(),
                                column.ordinal(),
                                column.name().to_string(),
                                type_signature_to_oid(column.type_signature()),
                                !column.nullable(),
                                column.default().is_some(),
                            ));
                        }
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(attrelid, attnum, attname, atttypid, attnotnull, atthasdef)| {
                    vec![
                        ScalarValue::Int(attrelid as i64),
                        ScalarValue::Text(attname),
                        ScalarValue::Int(atttypid as i64),
                        ScalarValue::Int(attnum as i64 + 1),
                        ScalarValue::Bool(attnotnull),
                        ScalarValue::Bool(false), // attisdropped: live columns are never dropped
                        ScalarValue::Bool(atthasdef),
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_type") => {
            // typnamespace 11 = pg_catalog OID, typowner 10 = superuser
            // typlen: -1 = variable, otherwise fixed byte length
            let mut entries = vec![
                (16u32, "bool", 11u32, 10u32, 1i64, false, "b", 0u32, 0u32),
                (20u32, "int8", 11u32, 10u32, 8i64, true, "b", 0u32, 0u32),
                (25u32, "text", 11u32, 10u32, -1i64, false, "b", 0u32, 0u32),
                (701u32, "float8", 11u32, 10u32, 8i64, true, "b", 0u32, 0u32),
                (1082u32, "date", 11u32, 10u32, 4i64, true, "b", 0u32, 0u32),
                (1114u32, "timestamp", 11u32, 10u32, 8i64, true, "b", 0u32, 0u32),
                (1700u32, "numeric", 11u32, 10u32, -1i64, false, "b", 0u32, 0u32),
            ];
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(
                    |(oid, typname, typnamespace, typowner, typlen, typbyval, typtype, typelem, typarray)| {
                        vec![
                            ScalarValue::Int(oid as i64),
                            ScalarValue::Text(typname.to_string()),
                            ScalarValue::Int(typnamespace as i64),
                            ScalarValue::Int(typowner as i64),
                            ScalarValue::Int(typlen),
                            ScalarValue::Bool(typbyval),
                            ScalarValue::Text(typtype.to_string()),
                            ScalarValue::Int(typelem as i64),
                            ScalarValue::Int(typarray as i64),
                        ]
                    },
                )
                .collect())
        }
        ("information_schema", "tables") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        out.push((
                            schema.name().to_string(),
                            table.name().to_string(),
                            information_schema_table_type(table.kind()).to_string(),
                        ));
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(table_schema, table_name, table_type)| {
                    vec![
                        ScalarValue::Text(table_schema),
                        ScalarValue::Text(table_name),
                        ScalarValue::Text(table_type),
                    ]
                })
                .collect())
        }
        ("information_schema", "columns") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            let col_default = column
                                .default()
                                .map(|expr| render_expr_to_sql(expr));
                            out.push((
                                schema.name().to_string(),
                                table.name().to_string(),
                                column.ordinal(),
                                column.name().to_string(),
                                information_schema_data_type(column.type_signature()).to_string(),
                                if column.nullable() {
                                    "YES".to_string()
                                } else {
                                    "NO".to_string()
                                },
                                col_default,
                                information_schema_numeric_precision(column.type_signature()),
                            ));
                        }
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            Ok(entries
                .into_iter()
                .map(
                    |(table_schema, table_name, ordinal, column_name, data_type, is_nullable, col_default, numeric_precision)| {
                        vec![
                            ScalarValue::Text(table_schema),
                            ScalarValue::Text(table_name),
                            ScalarValue::Text(column_name),
                            ScalarValue::Int(ordinal as i64 + 1),
                            col_default.map(ScalarValue::Text).unwrap_or(ScalarValue::Null),
                            ScalarValue::Text(is_nullable),
                            ScalarValue::Text(data_type),
                            ScalarValue::Null, // character_maximum_length
                            numeric_precision.map(ScalarValue::Int).unwrap_or(ScalarValue::Null),
                            ScalarValue::Text("NO".to_string()), // is_identity
                        ]
                    },
                )
                .collect())
        }
        ("information_schema", "schemata") => {
            let schemas = with_catalog_read(|catalog| {
                catalog
                    .schemas()
                    .map(|s| s.name().to_string())
                    .collect::<Vec<_>>()
            });
            Ok(schemas
                .into_iter()
                .map(|name| {
                    vec![
                        ScalarValue::Text("openassay".to_string()),
                        ScalarValue::Text(name),
                        ScalarValue::Text("openassay".to_string()),
                    ]
                })
                .collect())
        }
        ("information_schema", "key_column_usage") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        // Primary key and unique constraints
                        for kc in table.key_constraints() {
                            let con_name = kc.name.clone().unwrap_or_else(|| {
                                format!(
                                    "{}_{}",
                                    table.name(),
                                    if kc.primary { "pkey" } else { "key" }
                                )
                            });
                            for (pos, col_name) in kc.columns.iter().enumerate() {
                                rows.push(vec![
                                    ScalarValue::Text(con_name.clone()),
                                    ScalarValue::Text(schema.name().to_string()),
                                    ScalarValue::Text(table.name().to_string()),
                                    ScalarValue::Text(col_name.clone()),
                                    ScalarValue::Int(pos as i64 + 1),
                                ]);
                            }
                        }
                        // Foreign key constraints
                        for fk in table.foreign_key_constraints() {
                            let con_name = fk.name.clone().unwrap_or_else(|| {
                                format!("{}_fkey", table.name())
                            });
                            for (pos, col_name) in fk.columns.iter().enumerate() {
                                rows.push(vec![
                                    ScalarValue::Text(con_name.clone()),
                                    ScalarValue::Text(schema.name().to_string()),
                                    ScalarValue::Text(table.name().to_string()),
                                    ScalarValue::Text(col_name.clone()),
                                    ScalarValue::Int(pos as i64 + 1),
                                ]);
                            }
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("information_schema", "table_constraints") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for kc in table.key_constraints() {
                            let con_name = kc.name.clone().unwrap_or_else(|| {
                                format!(
                                    "{}_{}",
                                    table.name(),
                                    if kc.primary { "pkey" } else { "key" }
                                )
                            });
                            let con_type = if kc.primary { "PRIMARY KEY" } else { "UNIQUE" };
                            rows.push(vec![
                                ScalarValue::Text(con_name),
                                ScalarValue::Text(schema.name().to_string()),
                                ScalarValue::Text(table.name().to_string()),
                                ScalarValue::Text(con_type.to_string()),
                                ScalarValue::Text("NO".to_string()),
                            ]);
                        }
                        for fk in table.foreign_key_constraints() {
                            let con_name = fk.name.clone().unwrap_or_else(|| {
                                format!("{}_fkey", table.name())
                            });
                            rows.push(vec![
                                ScalarValue::Text(con_name),
                                ScalarValue::Text(schema.name().to_string()),
                                ScalarValue::Text(table.name().to_string()),
                                ScalarValue::Text("FOREIGN KEY".to_string()),
                                ScalarValue::Text("NO".to_string()),
                            ]);
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_database") => {
            Ok(vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("openassay".to_string()),
                ScalarValue::Int(10),
                ScalarValue::Int(6), // UTF8
                ScalarValue::Text("en_US.UTF-8".to_string()),
            ]])
        }
        ("pg_catalog", "pg_roles") => {
            let role = security::current_role();
            Ok(vec![vec![
                ScalarValue::Int(10),
                ScalarValue::Text(role),
                ScalarValue::Bool(true),  // rolsuper
                ScalarValue::Bool(true),  // rolcanlogin
                ScalarValue::Bool(true),  // rolinherit
                ScalarValue::Bool(true),  // rolcreaterole
                ScalarValue::Bool(true),  // rolcreatedb
                ScalarValue::Int(-1),     // rolconnlimit: -1 means unlimited
            ]])
        }
        ("pg_catalog", "pg_settings") => Ok(crate::commands::variable::with_guc_read(|guc| {
            guc.iter()
                .map(|(name, value)| {
                    vec![
                        ScalarValue::Text(name.clone()),
                        ScalarValue::Text(value.clone()),
                        ScalarValue::Text("Ungrouped".to_string()),
                        ScalarValue::Text(String::new()),
                        ScalarValue::Text("string".to_string()), // vartype
                        ScalarValue::Text("user".to_string()),   // context
                    ]
                })
                .collect()
        })),
        ("pg_catalog", "pg_tables") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    if matches!(table.kind(), TableKind::Heap | TableKind::VirtualDual) {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text("openassay".to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_views") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    if matches!(table.kind(), TableKind::View | TableKind::MaterializedView) {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text("openassay".to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_indexes") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    for index in table.indexes() {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text(index.name.as_str().to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_proc") => {
            // Return user-defined functions
            Ok(with_ext_read(|ext| {
                ext.user_functions
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        vec![
                            ScalarValue::Int(90000 + i as i64),
                            ScalarValue::Text(f.name.last().cloned().unwrap_or_default()),
                            ScalarValue::Int(0), // pronamespace placeholder
                            ScalarValue::Int(10), // proowner: superuser
                            ScalarValue::Int(14), // prolang: 14 = sql
                            ScalarValue::Text(String::new()), // prosrc placeholder
                        ]
                    })
                    .collect()
            }))
        }
        ("pg_catalog", "pg_constraint") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                let mut con_oid: i64 = 70000;
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let col_ordinals: std::collections::HashMap<String, i64> = table
                            .columns()
                            .iter()
                            .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                            .collect();
                        for kc in table.key_constraints() {
                            let con_name = kc.name.clone().unwrap_or_else(|| {
                                format!(
                                    "{}_{}",
                                    table.name(),
                                    if kc.primary { "pkey" } else { "key" }
                                )
                            });
                            let contype = if kc.primary { "p" } else { "u" };
                            let conkey_parts = kc.columns
                                .iter()
                                .map(|c| col_ordinals
                                    .get(c)
                                    .copied()
                                    .ok_or_else(|| EngineError {
                                        message: format!("Column '{}' not found in table '{}'", c, table.name()),
                                    }))
                                .collect::<Result<Vec<i64>, _>>()?;
                            let conkey = format!("{{{}}}", conkey_parts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(","));
                            rows.push(vec![
                                ScalarValue::Int(con_oid),
                                ScalarValue::Text(con_name),
                                ScalarValue::Int(schema.oid() as i64),
                                ScalarValue::Text(contype.to_string()),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(0), // confrelid: 0 for non-FK
                                ScalarValue::Text(conkey),
                                ScalarValue::Null, // confkey: NULL for non-FK
                                ScalarValue::Null, // confdeltype: NULL for non-FK
                                ScalarValue::Null, // confupdtype: NULL for non-FK
                            ]);
                            con_oid += 1;
                        }
                        for fk in table.foreign_key_constraints() {
                            let con_name = fk.name.clone().unwrap_or_else(|| {
                                format!("{}_fkey", table.name())
                            });
                            let fk_conkey_parts = fk.columns
                                .iter()
                                .map(|c| col_ordinals
                                    .get(c)
                                    .copied()
                                    .ok_or_else(|| EngineError {
                                        message: format!("FK column '{}' not found in table '{}'", c, table.name()),
                                    }))
                                .collect::<Result<Vec<i64>, _>>()?;
                            let conkey = format!("{{{}}}", fk_conkey_parts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(","));
                            // Resolve referenced table OID
                            let ref_table_oid = catalog
                                .schema(fk.referenced_table.first().map(String::as_str).unwrap_or("public"))
                                .and_then(|s| {
                                    fk.referenced_table.last().and_then(|n| s.table(n))
                                })
                                .map(|t| t.oid() as i64)
                                .unwrap_or(0);
                            let ref_col_ordinals: std::collections::HashMap<String, i64> = catalog
                                .schema(fk.referenced_table.first().map(String::as_str).unwrap_or("public"))
                                .and_then(|s| fk.referenced_table.last().and_then(|n| s.table(n)))
                                .map(|t| {
                                    t.columns()
                                        .iter()
                                        .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                                        .collect()
                                })
                                .unwrap_or_default();
                            let confkey = format!(
                                "{{{}}}",
                                fk.referenced_columns
                                    .iter()
                                    .map(|c| ref_col_ordinals
                                        .get(c)
                                        .copied()
                                        .unwrap_or(0)
                                        .to_string())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                            let confdeltype = fk_action_char(fk.on_delete);
                            let confupdtype = fk_action_char(fk.on_update);
                            rows.push(vec![
                                ScalarValue::Int(con_oid),
                                ScalarValue::Text(con_name),
                                ScalarValue::Int(schema.oid() as i64),
                                ScalarValue::Text("f".to_string()),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(ref_table_oid),
                                ScalarValue::Text(conkey),
                                ScalarValue::Text(confkey),
                                ScalarValue::Text(confdeltype.to_string()),
                                ScalarValue::Text(confupdtype.to_string()),
                            ]);
                            con_oid += 1;
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_index") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                let mut index_oid: i64 = 80000;
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let col_ordinals: std::collections::HashMap<String, i64> = table
                            .columns()
                            .iter()
                            .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                            .collect();
                        // Key constraints (PK / UNIQUE) produce implicit indexes
                        for kc in table.key_constraints() {
                            let indkey_parts = kc
                                .columns
                                .iter()
                                .map(|c| col_ordinals
                                    .get(c)
                                    .copied()
                                    .ok_or_else(|| EngineError {
                                        message: format!("Column '{}' not found in table '{}'", c, table.name()),
                                    }))
                                .collect::<Result<Vec<i64>, _>>()?;
                            let indkey = indkey_parts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(" ");
                            rows.push(vec![
                                ScalarValue::Int(index_oid),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(kc.columns.len() as i64),
                                ScalarValue::Bool(true), // indisunique: all key constraints (PK and UNIQUE) create unique indexes
                                ScalarValue::Bool(kc.primary), // indisprimary
                                ScalarValue::Text(indkey),
                            ]);
                            index_oid += 1;
                        }
                        // Explicit indexes
                        for idx in table.indexes() {
                            let indkey_parts = idx
                                .columns
                                .iter()
                                .map(|c| col_ordinals
                                    .get(c)
                                    .copied()
                                    .ok_or_else(|| EngineError {
                                        message: format!("Column '{}' not found in table '{}'", c, table.name()),
                                    }))
                                .collect::<Result<Vec<i64>, _>>()?;
                            let indkey = indkey_parts.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(" ");
                            rows.push(vec![
                                ScalarValue::Int(index_oid),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(idx.columns.len() as i64),
                                ScalarValue::Bool(idx.unique),
                                ScalarValue::Bool(false), // indisprimary
                                ScalarValue::Text(indkey),
                            ]);
                            index_oid += 1;
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_attrdef") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            if let Some(default_expr) = column.default() {
                                rows.push(vec![
                                    ScalarValue::Int(table.oid() as i64),
                                    ScalarValue::Int(column.ordinal() as i64 + 1),
                                    ScalarValue::Text(render_expr_to_sql(default_expr)),
                                ]);
                            }
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_inherits") => {
            // OpenAssay does not support table inheritance; return empty result set
            Ok(Vec::new())
        }
        ("pg_catalog", "pg_extension") => Ok(with_ext_read(|ext| {
            ext.extensions
                .iter()
                .map(|e| {
                    vec![
                        ScalarValue::Text(e.name.clone()),
                        ScalarValue::Text(e.version.clone()),
                        ScalarValue::Text(e.description.clone()),
                    ]
                })
                .collect()
        })),
        ("ws", "connections") => {
            if !is_ws_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"ws\" is not loaded".to_string(),
                });
            }
            #[cfg(target_arch = "wasm32")]
            {
                let conn_ids: Vec<i64> =
                    with_ext_read(|ext| ext.ws_connections.keys().copied().collect());
                for id in conn_ids {
                    sync_wasm_ws_state(id);
                    drain_wasm_ws_messages(id);
                }
            }
            Ok(with_ext_read(|ext| {
                let mut conns: Vec<_> = ext.ws_connections.values().collect();
                conns.sort_by_key(|c| c.id);
                conns
                    .iter()
                    .map(|c| {
                        vec![
                            ScalarValue::Int(c.id),
                            ScalarValue::Text(c.url.clone()),
                            ScalarValue::Text(c.state.clone()),
                            ScalarValue::Text(c.opened_at.clone()),
                            ScalarValue::Int(c.messages_in),
                            ScalarValue::Int(c.messages_out),
                        ]
                    })
                    .collect()
            }))
        }
        _ => Err(EngineError {
            message: format!("relation \"{schema}.{relation}\" does not exist"),
        }),
    }
}

fn pg_relkind_for_table(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "r",
        TableKind::View => "v",
        TableKind::MaterializedView => "m",
    }
}

fn information_schema_table_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "BASE TABLE",
        TableKind::View => "VIEW",
        TableKind::MaterializedView => "MATERIALIZED VIEW",
    }
}

fn information_schema_data_type(signature: TypeSignature) -> &'static str {
    match signature {
        TypeSignature::Bool => "boolean",
        TypeSignature::Int8 => "bigint",
        TypeSignature::Float8 => "double precision",
        TypeSignature::Numeric => "numeric",
        TypeSignature::Text => "text",
        TypeSignature::Date => "date",
        TypeSignature::Timestamp => "timestamp without time zone",
        TypeSignature::Vector(_) => "vector",
    }
}

fn information_schema_numeric_precision(signature: TypeSignature) -> Option<i64> {
    match signature {
        TypeSignature::Int8 => Some(64),
        TypeSignature::Float8 => Some(53),
        TypeSignature::Numeric => None, // variable precision
        _ => None,
    }
}

fn fk_action_char(action: crate::parser::ast::ForeignKeyAction) -> char {
    use crate::parser::ast::ForeignKeyAction;
    match action {
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
    }
}

async fn evaluate_join(
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

async fn project_select_row(
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
fn expand_qualified_wildcard(
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

async fn project_select_row_with_window(
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

fn contains_aggregate_expr(expr: &Expr) -> bool {
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

fn contains_window_expr(expr: &Expr) -> bool {
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

fn contains_window_bound_expr(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::OffsetPreceding(expr) | WindowFrameBound::OffsetFollowing(expr) => {
            contains_window_expr(expr)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => false,
    }
}

fn group_by_contains_window_expr(group_by: &[GroupByExpr]) -> bool {
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

fn group_by_exprs(group_by: &[GroupByExpr]) -> Vec<&Expr> {
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
fn resolve_group_by_alias<'a>(
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

fn identifier_key(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(parts) => Some(parts.join(".").to_ascii_lowercase()),
        _ => None,
    }
}

fn collect_grouping_identifiers(group_by: &[GroupByExpr]) -> HashSet<String> {
    let mut out = HashSet::new();
    for expr in group_by_exprs(group_by) {
        if let Some(key) = identifier_key(expr) {
            out.insert(key);
        }
    }
    out
}

fn expand_grouping_sets<'a>(group_by: &'a [GroupByExpr]) -> Vec<Vec<&'a Expr>> {
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

struct GroupingContext {
    current_grouping: HashSet<String>,
    all_grouping: HashSet<String>,
}

pub(crate) fn is_aggregate_function(name: &str) -> bool {
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
    )
}

fn eval_group_expr<'a>(
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
struct AggregateInputRow {
    args: Vec<ScalarValue>,
    order_keys: Vec<ScalarValue>,
}

#[derive(Debug, Clone, Copy)]
struct RegrStats {
    count: i64,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

fn compute_regr_stats(rows: &[AggregateInputRow], message: &str) -> Result<RegrStats, EngineError> {
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

async fn build_aggregate_input_rows(
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

fn apply_aggregate_distinct(rows: &mut Vec<AggregateInputRow>) {
    let mut seen = HashSet::new();
    rows.retain(|row| seen.insert(row_key(&row.args)));
}

fn sort_aggregate_rows(rows: &mut [AggregateInputRow], order_by: &[OrderByExpr]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|left, right| compare_order_keys(&left.order_keys, &right.order_keys, order_by));
}

fn sum_i64_fast(values: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return unsafe { sum_i64_avx2(values) };
        }
    }
    values.iter().copied().sum()
}

fn sum_f64_fast(values: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return unsafe { sum_f64_avx(values) };
        }
    }
    values.iter().copied().sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i64_avx2(values: &[i64]) -> i64 {
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
unsafe fn sum_f64_avx(values: &[f64]) -> f64 {
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

fn min_i64_fast(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return Some(unsafe { min_i64_avx2(values) });
        }
    }
    Some(values.iter().copied().min().unwrap())
}

fn max_i64_fast(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx2") before calling.
            return Some(unsafe { max_i64_avx2(values) });
        }
    }
    Some(values.iter().copied().max().unwrap())
}

fn min_f64_fast(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return Some(unsafe { min_f64_avx(values) });
        }
    }
    Some(values.iter().copied().fold(f64::INFINITY, f64::min))
}

fn max_f64_fast(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx") {
            // SAFETY: We check CPU support with is_x86_feature_detected!("avx") before calling.
            return Some(unsafe { max_f64_avx(values) });
        }
    }
    Some(values.iter().copied().fold(f64::NEG_INFINITY, f64::max))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn min_i64_avx2(values: &[i64]) -> i64 {
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
unsafe fn max_i64_avx2(values: &[i64]) -> i64 {
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
unsafe fn min_f64_avx(values: &[f64]) -> f64 {
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
unsafe fn max_f64_avx(values: &[f64]) -> f64 {
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
pub(crate) async fn eval_aggregate_function(
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
                        min_i64_fast(&int_values).unwrap()
                    } else {
                        max_i64_fast(&int_values).unwrap()
                    };
                    let float_winner = if fn_name == "min" {
                        min_f64_fast(&float_values).unwrap()
                    } else {
                        max_f64_fast(&float_values).unwrap()
                    };
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

async fn join_condition_matches(
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

fn scope_from_row(
    columns: &[String],
    row: &[ScalarValue],
    qualifiers: &[String],
    visible_columns: &[String],
) -> EvalScope {
    let mut scope = EvalScope::default();
    for (col, value) in columns.iter().zip(row.iter()) {
        scope.insert_unqualified(col, value.clone());
        for qualifier in qualifiers {
            scope.insert_qualified(&format!("{qualifier}.{col}"), value.clone());
        }
    }

    // Ensure all visible columns exist even if row data is empty (e.g. relation with no rows).
    for col in visible_columns {
        if scope.lookup_join_column(col).is_none() {
            scope.insert_unqualified(col, ScalarValue::Null);
            for qualifier in qualifiers {
                scope.insert_qualified(&format!("{qualifier}.{col}"), ScalarValue::Null);
            }
        }
    }
    scope
}

pub(crate) fn scope_for_table_row(table: &crate::catalog::Table, row: &[ScalarValue]) -> EvalScope {
    let qualifiers = vec![table.name().to_string(), table.qualified_name()];
    scope_for_table_row_with_qualifiers(table, row, &qualifiers)
}

pub(crate) fn scope_for_table_row_with_qualifiers(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    qualifiers: &[String],
) -> EvalScope {
    let columns = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    scope_from_row(&columns, row, qualifiers, &columns)
}

pub(crate) fn combine_scopes(
    left: &EvalScope,
    right: &EvalScope,
    using_columns: &HashSet<String>,
) -> EvalScope {
    let mut out = left.clone();
    out.merge(right);

    for col in using_columns {
        if let Some(value) = left
            .lookup_join_column(col)
            .or_else(|| right.lookup_join_column(col))
        {
            out.force_unqualified(col, value);
        }
    }

    out
}

async fn execute_set_operation(
    left: &QueryExpr,
    op: SetOperator,
    quantifier: SetQuantifier,
    right: &QueryExpr,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let left_res = execute_query_expr_with_outer(left, params, outer_scope).await?;
    let right_res = execute_query_expr_with_outer(right, params, outer_scope).await?;
    if left_res.columns.len() != right_res.columns.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }

    let rows = match (op, quantifier) {
        (SetOperator::Union, SetQuantifier::All) => {
            let mut out = left_res.rows.clone();
            out.extend(right_res.rows.iter().cloned());
            out
        }
        (SetOperator::Union, SetQuantifier::Distinct) => dedupe_rows(
            left_res
                .rows
                .iter()
                .cloned()
                .chain(right_res.rows.iter().cloned())
                .collect(),
        ),
        (SetOperator::Intersect, SetQuantifier::Distinct) => {
            intersect_rows(&left_res.rows, &right_res.rows, false)
        }
        (SetOperator::Intersect, SetQuantifier::All) => {
            intersect_rows(&left_res.rows, &right_res.rows, true)
        }
        (SetOperator::Except, SetQuantifier::Distinct) => {
            except_rows(&left_res.rows, &right_res.rows, false)
        }
        (SetOperator::Except, SetQuantifier::All) => {
            except_rows(&left_res.rows, &right_res.rows, true)
        }
    };

    Ok(QueryResult {
        columns: left_res.columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}

fn dedupe_rows(rows: Vec<Vec<ScalarValue>>) -> Vec<Vec<ScalarValue>> {
    let mut seen = HashSet::with_capacity(rows.len());
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row_key(&row);
        if seen.insert(key) {
            out.push(row);
        }
    }
    out
}

fn intersect_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                out.push(row.clone());
            }
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

fn except_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                continue;
            }
            out.push(row.clone());
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if !right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

fn count_rows(rows: &[Vec<ScalarValue>]) -> std::collections::HashMap<String, usize> {
    let mut counts = std::collections::HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)).or_insert(0) += 1;
    }
    counts
}

pub(crate) fn row_key(row: &[ScalarValue]) -> String {
    let mut key = String::new();
    for (idx, value) in row.iter().enumerate() {
        if idx > 0 {
            key.push('|');
        }

        match value {
            ScalarValue::Null => key.push('N'),
            ScalarValue::Bool(flag) => {
                key.push_str("B:");
                key.push_str(if *flag { "true" } else { "false" });
            }
            ScalarValue::Int(number) => {
                key.push_str("I:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Float(number) => {
                key.push_str("F:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Numeric(number) => {
                key.push_str("N:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Text(text) => {
                key.push_str("T:");
                key.push_str(text);
            }
            ScalarValue::Array(_) => {
                key.push_str("A:");
                key.push_str(&value.render());
            }
            ScalarValue::Record(_) => {
                key.push_str("R:");
                key.push_str(&value.render());
            }
        }
    }
    key
}

/// Collect identifiers from ORDER BY that are not present in the SELECT output columns.
/// These need to be temporarily added to the SELECT for sorting.
fn collect_extra_order_by_columns(query: &Query) -> Vec<Expr> {
    let select_columns: HashSet<String> = match &query.body {
        QueryExpr::Select(select) => select
            .targets
            .iter()
            .filter_map(|target| {
                if let Some(alias) = &target.alias {
                    return Some(alias.to_ascii_lowercase());
                }
                if let Expr::Identifier(parts) = &target.expr {
                    parts.last().map(|p| p.to_ascii_lowercase())
                } else {
                    None
                }
            })
            .collect(),
        _ => return Vec::new(),
    };

    let mut extras = Vec::new();
    for spec in &query.order_by {
        if let Expr::Identifier(parts) = &spec.expr
            && parts.len() == 1
        {
            let name = parts[0].to_ascii_lowercase();
            if !select_columns.contains(&name) {
                extras.push(spec.expr.clone());
            }
        }
    }
    extras
}

/// Augment a SELECT query body to include extra ORDER BY columns as hidden trailing targets.
/// Returns the modified query body and the number of hidden columns added.
fn augment_select_for_order_by(body: &QueryExpr, extras: &[Expr]) -> (QueryExpr, usize) {
    if extras.is_empty() {
        return (body.clone(), 0);
    }
    if let QueryExpr::Select(select) = body {
        let mut new_select = select.clone();
        let mut count = 0;
        for expr in extras {
            new_select.targets.push(SelectItem {
                expr: expr.clone(),
                alias: None,
            });
            count += 1;
        }
        (QueryExpr::Select(new_select), count)
    } else {
        (body.clone(), 0)
    }
}

async fn apply_order_by(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    if query.order_by.is_empty() || result.rows.is_empty() {
        return Ok(());
    }

    let columns = result.columns.clone();
    let mut decorated = Vec::with_capacity(result.rows.len());
    for row in result.rows.drain(..) {
        let scope = EvalScope::from_output_row(&columns, &row);
        let mut keys = Vec::with_capacity(query.order_by.len());
        for spec in &query.order_by {
            keys.push(resolve_order_key(&spec.expr, &scope, &columns, &row, params).await?);
        }
        decorated.push((keys, row));
    }

    decorated.sort_by(|(ka, _), (kb, _)| compare_order_keys(ka, kb, &query.order_by));
    result.rows = decorated.into_iter().map(|(_, row)| row).collect();
    Ok(())
}

pub(crate) fn compare_order_keys(
    left: &[ScalarValue],
    right: &[ScalarValue],
    specs: &[crate::parser::ast::OrderByExpr],
) -> Ordering {
    for (idx, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        let ord = scalar_cmp(l, r);
        if ord != Ordering::Equal {
            if specs[idx].ascending == Some(false) {
                return ord.reverse();
            }
            return ord;
        }
    }
    Ordering::Equal
}

fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use ScalarValue::{Bool, Float, Int, Null, Text};
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Text(x), Text(y)) => x.cmp(y),
        (Int(x), Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Float(x), Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        _ => a.render().cmp(&b.render()),
    }
}

async fn resolve_order_key(
    expr: &Expr,
    scope: &EvalScope,
    columns: &[String],
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    if let Expr::Integer(pos) = expr
        && *pos > 0
    {
        let idx = (*pos as usize).saturating_sub(1);
        if idx < row.len() {
            return Ok(row[idx].clone());
        }
    }

    if let Expr::Identifier(parts) = expr
        && parts.len() == 1
    {
        let want = parts[0].to_ascii_lowercase();
        if let Some((idx, _)) = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.to_ascii_lowercase() == want)
        {
            return Ok(row[idx].clone());
        }
    }

    eval_expr(expr, scope, params).await
}

async fn apply_offset_limit(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    let offset = if let Some(expr) = &query.offset {
        parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "OFFSET",
        )?
    } else {
        0usize
    };

    let limit = if let Some(expr) = &query.limit {
        Some(parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "LIMIT",
        )?)
    } else {
        None
    };

    if offset > 0 {
        if offset >= result.rows.len() {
            result.rows.clear();
            return Ok(());
        }
        result.rows = result.rows[offset..].to_vec();
    }

    if let Some(limit) = limit
        && limit < result.rows.len()
    {
        result.rows.truncate(limit);
    }

    Ok(())
}

pub(crate) fn parse_non_negative_int(
    value: &ScalarValue,
    what: &str,
) -> Result<usize, EngineError> {
    match value {
        ScalarValue::Int(v) if *v >= 0 => Ok(*v as usize),
        ScalarValue::Text(v) => {
            let parsed = v.parse::<usize>().map_err(|_| EngineError {
                message: format!("{what} must be a non-negative integer"),
            })?;
            Ok(parsed)
        }
        _ => Err(EngineError {
            message: format!("{what} must be a non-negative integer"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        max_f64_fast, max_i64_fast, min_f64_fast, min_i64_fast, sum_f64_fast, sum_i64_fast,
    };

    #[test]
    fn fast_i64_sum_matches_scalar() {
        let values = (1..=2048).collect::<Vec<i64>>();
        assert_eq!(sum_i64_fast(&values), values.iter().copied().sum::<i64>());
    }

    #[test]
    fn fast_f64_sum_matches_scalar() {
        let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
        let fast = sum_f64_fast(&values);
        let scalar = values.iter().copied().sum::<f64>();
        assert!((fast - scalar).abs() < f64::EPSILON * 100.0);
    }

    #[test]
    fn fast_i64_min_matches_scalar() {
        let values = (1..=2048).collect::<Vec<i64>>();
        assert_eq!(min_i64_fast(&values), values.iter().copied().min());
    }

    #[test]
    fn fast_i64_max_matches_scalar() {
        let values = (1..=2048).collect::<Vec<i64>>();
        assert_eq!(max_i64_fast(&values), values.iter().copied().max());
    }

    #[test]
    fn fast_f64_min_matches_scalar() {
        let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
        let fast = min_f64_fast(&values).unwrap();
        let scalar = values.iter().copied().fold(f64::INFINITY, f64::min);
        assert!((fast - scalar).abs() < f64::EPSILON);
    }

    #[test]
    fn fast_f64_max_matches_scalar() {
        let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
        let fast = max_f64_fast(&values).unwrap();
        let scalar = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        assert!((fast - scalar).abs() < f64::EPSILON);
    }

    #[test]
    fn fast_i64_min_max_with_negatives() {
        let values: Vec<i64> = (-1024..=1024).collect();
        assert_eq!(min_i64_fast(&values), Some(-1024));
        assert_eq!(max_i64_fast(&values), Some(1024));
    }

    #[test]
    fn fast_f64_min_max_with_negatives() {
        let values: Vec<f64> = (-1024..=1024).map(|v| v as f64 * 0.1).collect();
        let min = min_f64_fast(&values).unwrap();
        let max = max_f64_fast(&values).unwrap();
        assert!((min - (-102.4)).abs() < f64::EPSILON);
        assert!((max - 102.4).abs() < f64::EPSILON);
    }
}
