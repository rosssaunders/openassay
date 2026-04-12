use super::table_functions::evaluate_table_function_with_predicate;
#[allow(clippy::wildcard_imports)]
use super::*;
use crate::executor::column_batch::{ColumnBatch, TypedColumn};
use crate::executor::columnar_agg::{AggKind, AggSpec, ColumnarAggregator, OutputExpr};
use crate::executor::pipeline::{
    BatchCollector, FilterStage, LimitStage, PipelineStage, ProjectStage,
};
use crate::executor::profiling;
use crate::executor::window_eval::{
    WindowArgumentKind, WindowColumnPlan, WindowPartitions, eval_window_function_columnar,
    expr_references_columns, resolve_window_spec,
};
use crate::parser::ast::WindowSpec;
use crate::storage::heap::{
    ColumnAggregateOp, ColumnAggregateRequest, ScanPredicate, ScanPredicateOp,
};
use crate::tcop::engine::with_storage_write;
use arrow::array::{Array, StringArray};
use std::collections::BTreeSet;

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

            let execution_query = Query {
                with: None,
                body: body.clone(),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
            let mut result = if scan_projection_hints_active() {
                if let QueryExpr::Select(select) = &body {
                    execute_select_with_query(select, query, params, outer_scope).await
                } else {
                    execute_query_expr_with_outer(&body, params, outer_scope).await
                }
            } else {
                with_scan_projection_hints(&execution_query, async {
                    if let QueryExpr::Select(select) = &body {
                        execute_select_with_query(select, query, params, outer_scope).await
                    } else {
                        execute_query_expr_with_outer(&body, params, outer_scope).await
                    }
                })
                .await
            }?;

            // Strip hidden ORDER BY columns
            if num_hidden > 0 {
                let visible = result.columns.len().saturating_sub(num_hidden);
                result.columns.truncate(visible);
                for row in &mut result.rows {
                    row.truncate(visible);
                }
            }

            if !matches!(body, QueryExpr::Select(_)) {
                apply_order_by(&mut result, query, params).await?;
                apply_offset_limit(&mut result, query, params).await?;
            }
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
    const DYNAMIC_FUNCTIONS: &[&str] = &["json_table", "iceberg_scan", "parquet_scan"];
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

fn can_use_simple_columnar_projection(
    select: &SelectStatement,
    has_aggregate: bool,
    has_window: bool,
    outer_scope: Option<&EvalScope>,
) -> bool {
    crate::executor::columnar_execution_enabled()
        && outer_scope.is_none()
        && !has_aggregate
        && !has_window
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.quantifier.is_none()
        && select.distinct_on.is_empty()
        && select.from.len() == 1
        && matches!(select.from[0], TableExpression::Relation(_))
        && select.targets.iter().all(|target| {
            matches!(
                target.expr,
                Expr::Identifier(_) | Expr::Wildcard | Expr::QualifiedWildcard(_)
            )
        })
}

#[derive(Debug, Clone)]
enum GroupKeySource {
    Column(usize),
    DerivedExpr(Expr),
}

#[derive(Debug, Clone)]
struct ColumnarAggPlan {
    group_key_sources: Vec<GroupKeySource>,
    agg_specs: Vec<AggSpec>,
    output_exprs: Vec<OutputExpr>,
    intermediate_columns: Vec<String>,
    final_target_exprs: Vec<Expr>,
}

fn can_use_columnar_aggregation(
    select: &SelectStatement,
    has_aggregate: bool,
    has_window: bool,
    outer_scope: Option<&EvalScope>,
) -> bool {
    crate::executor::columnar_execution_enabled()
        && outer_scope.is_none()
        && has_aggregate
        && !has_window
        && select.from.len() == 1
        && matches!(select.from[0], TableExpression::Relation(_))
        && select.having.is_none()
        && select.quantifier.is_none()
        && select.distinct_on.is_empty()
}

fn can_use_columnar_windows(
    select: &SelectStatement,
    has_aggregate: bool,
    has_window: bool,
    outer_scope: Option<&EvalScope>,
) -> bool {
    crate::executor::columnar_execution_enabled()
        && outer_scope.is_none()
        && !has_aggregate
        && has_window
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.quantifier.is_none()
        && select.distinct_on.is_empty()
        && select.from.len() == 1
        && matches!(select.from[0], TableExpression::Relation(_))
        && select.targets.iter().all(|target| {
            matches!(target.expr, Expr::Identifier(_))
                || matches!(target.expr, Expr::FunctionCall { over: Some(_), .. })
        })
}

#[derive(Debug, Clone)]
struct ColumnarRelationScanPlan {
    table: crate::catalog::Table,
    scan_predicates: Vec<ScanPredicate>,
    remaining_predicate: Option<Expr>,
    schema_batch: ColumnBatch,
}

enum SimpleLikeMatcher {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
}

impl SimpleLikeMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Exact(literal) => value == literal,
            Self::Prefix(literal) => value.starts_with(literal),
            Self::Suffix(literal) => value.ends_with(literal),
            Self::Contains(literal) => value.contains(literal),
        }
    }
}

fn for_each_matching_text_row(
    values: &StringArray,
    deleted_rows: Option<&[bool]>,
    matcher: &SimpleLikeMatcher,
    mut callback: impl FnMut(usize),
) {
    let has_nulls = values.null_count() > 0;
    match matcher {
        SimpleLikeMatcher::Exact(literal) => {
            for_each_matching_text_row_with(
                values,
                deleted_rows,
                has_nulls,
                |value| value == literal,
                &mut callback,
            );
        }
        SimpleLikeMatcher::Prefix(literal) => {
            for_each_matching_text_row_with(
                values,
                deleted_rows,
                has_nulls,
                |value| value.starts_with(literal),
                &mut callback,
            );
        }
        SimpleLikeMatcher::Suffix(literal) => {
            for_each_matching_text_row_with(
                values,
                deleted_rows,
                has_nulls,
                |value| value.ends_with(literal),
                &mut callback,
            );
        }
        SimpleLikeMatcher::Contains(literal) => {
            for_each_matching_text_row_with(
                values,
                deleted_rows,
                has_nulls,
                |value| value.contains(literal),
                &mut callback,
            );
        }
    }
}

fn for_each_matching_text_row_with(
    values: &StringArray,
    deleted_rows: Option<&[bool]>,
    has_nulls: bool,
    mut matches: impl FnMut(&str) -> bool,
    callback: &mut impl FnMut(usize),
) {
    match (deleted_rows, has_nulls) {
        (None, false) => {
            for row_idx in 0..values.len() {
                if matches(values.value(row_idx)) {
                    callback(row_idx);
                }
            }
        }
        (None, true) => {
            for row_idx in 0..values.len() {
                if !values.is_null(row_idx) && matches(values.value(row_idx)) {
                    callback(row_idx);
                }
            }
        }
        (Some(deleted_rows), false) => {
            for (row_idx, deleted) in deleted_rows.iter().copied().enumerate().take(values.len()) {
                if deleted {
                    continue;
                }
                if matches(values.value(row_idx)) {
                    callback(row_idx);
                }
            }
        }
        (Some(deleted_rows), true) => {
            for (row_idx, deleted) in deleted_rows.iter().copied().enumerate().take(values.len()) {
                if deleted || values.is_null(row_idx) {
                    continue;
                }
                if matches(values.value(row_idx)) {
                    callback(row_idx);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum ColumnarWindowTargetPlan {
    InputColumn(usize),
    Window(WindowColumnPlan),
}

#[derive(Debug, Clone)]
struct StageLimitSpec {
    offset: usize,
    limit: usize,
}

fn relation_uses_row_level_security(table: &crate::catalog::Table) -> bool {
    let role = security::current_role();
    let evaluation = security::rls_evaluation_for_role(&role, table.oid(), RlsCommand::Select);
    evaluation.enabled && !evaluation.bypass
}

fn empty_typed_column(signature: TypeSignature) -> TypedColumn {
    match signature {
        TypeSignature::Bool => TypedColumn::Bool(Vec::new(), Vec::new()),
        TypeSignature::Int8 => TypedColumn::Int64(Vec::new(), Vec::new()),
        TypeSignature::Float8 => TypedColumn::Float64(Vec::new(), Vec::new()),
        TypeSignature::Numeric => TypedColumn::Numeric(Vec::new(), Vec::new()),
        TypeSignature::Date => TypedColumn::Date(Vec::new(), Vec::new()),
        TypeSignature::Text | TypeSignature::Timestamp => TypedColumn::Text(Vec::new(), Vec::new()),
        _ => TypedColumn::Mixed(Vec::new()),
    }
}

fn schema_batch_for_table(table: &crate::catalog::Table) -> ColumnBatch {
    ColumnBatch {
        columns: table
            .columns()
            .iter()
            .map(|column| empty_typed_column(column.type_signature()))
            .collect(),
        column_names: table
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect(),
        row_count: 0,
        record_batch: None,
    }
}

async fn prepare_columnar_relation_scan(
    rel: &TableRef,
    relation_predicates: &[Expr],
    params: &[Option<String>],
) -> Result<Option<ColumnarRelationScanPlan>, EngineError> {
    let resolved_table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    });
    let Ok(table) = resolved_table else {
        return Ok(None);
    };
    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView)
        || relation_uses_row_level_security(&table)
    {
        return Ok(None);
    }
    require_relation_privilege(&table, TablePrivilege::Select)?;

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let column_indexes = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| (column.name().to_string(), idx))
        .collect::<HashMap<_, _>>();
    let table_columns = column_indexes.keys().cloned().collect::<HashSet<_>>();
    let mut scan_predicates = Vec::new();
    let mut applied = vec![false; relation_predicates.len()];
    for (idx, predicate) in relation_predicates.iter().enumerate() {
        if let Some(scan_predicate) = extract_relation_scan_predicate(
            predicate,
            &qualifiers,
            &table_columns,
            &column_indexes,
            params,
        )
        .await?
        {
            scan_predicates.push(scan_predicate);
            applied[idx] = true;
        }
    }

    Ok(Some(ColumnarRelationScanPlan {
        table: table.clone(),
        scan_predicates,
        remaining_predicate: remaining_predicate_from_applied(relation_predicates, &applied),
        schema_batch: schema_batch_for_table(&table),
    }))
}

async fn stage_limit_spec(
    query: Option<&Query>,
    params: &[Option<String>],
) -> Result<Option<StageLimitSpec>, EngineError> {
    let Some(query) = query else {
        return Ok(None);
    };
    if !query.order_by.is_empty() {
        return Ok(None);
    }
    let Some(limit_expr) = &query.limit else {
        return Ok(None);
    };
    let limit = parse_non_negative_int(
        &eval_expr(limit_expr, &EvalScope::default(), params).await?,
        "LIMIT",
    )?;
    let offset = if let Some(offset_expr) = &query.offset {
        parse_non_negative_int(
            &eval_expr(offset_expr, &EvalScope::default(), params).await?,
            "OFFSET",
        )?
    } else {
        0
    };
    Ok(Some(StageLimitSpec { offset, limit }))
}

fn aggregate_identifier_column_index(expr: &Expr, batch: &ColumnBatch) -> Option<usize> {
    let Expr::Identifier(parts) = expr else {
        return None;
    };
    batch.column_index(&parts.join("."))
}

fn aggregate_cast_identifier_column_index(
    expr: &Expr,
    expected_types: &[&str],
    batch: &ColumnBatch,
) -> Option<usize> {
    let Expr::Cast { expr, type_name } = expr else {
        return None;
    };
    if !expected_types
        .iter()
        .any(|expected| type_name.eq_ignore_ascii_case(expected))
    {
        return None;
    }
    aggregate_identifier_column_index(expr, batch)
}

fn plan_columnar_aggregate_kind(
    name: &[String],
    args: &[Expr],
    distinct: bool,
    order_by: &[OrderByExpr],
    within_group: &[OrderByExpr],
    filter: Option<&Expr>,
    over: Option<&WindowSpec>,
    batch: &ColumnBatch,
) -> Option<AggKind> {
    if !order_by.is_empty() || !within_group.is_empty() || filter.is_some() || over.is_some() {
        return None;
    }

    let fn_name = name.last()?.to_ascii_lowercase();
    match fn_name.as_str() {
        "count" if args.len() == 1 && matches!(args[0], Expr::Wildcard) => {
            if distinct {
                None
            } else {
                Some(AggKind::CountStar)
            }
        }
        "count" if args.len() == 1 && distinct => {
            let column_index = aggregate_identifier_column_index(&args[0], batch)?;
            match &batch.columns[column_index] {
                TypedColumn::Int64(_, _) => Some(AggKind::CountDistinctInt { column_index }),
                TypedColumn::Text(_, _) => Some(AggKind::CountDistinctText { column_index }),
                _ => None,
            }
        }
        "count" if args.len() == 1 => Some(AggKind::Count {
            column_index: aggregate_identifier_column_index(&args[0], batch)?,
        }),
        "sum" if args.len() == 1 && !distinct => {
            let column_index = aggregate_identifier_column_index(&args[0], batch)?;
            match &batch.columns[column_index] {
                TypedColumn::Int64(_, _) => Some(AggKind::SumInt { column_index }),
                TypedColumn::Float64(_, _) => Some(AggKind::SumFloat { column_index }),
                TypedColumn::Numeric(_, _) => Some(AggKind::SumNumeric { column_index }),
                _ => None,
            }
        }
        "avg" if args.len() == 1 && !distinct => {
            if let Some(column_index) = aggregate_identifier_column_index(&args[0], batch) {
                match &batch.columns[column_index] {
                    TypedColumn::Int64(_, _) => Some(AggKind::AvgInt { column_index }),
                    TypedColumn::Float64(_, _) => Some(AggKind::AvgFloat { column_index }),
                    TypedColumn::Numeric(_, _) => Some(AggKind::AvgNumeric { column_index }),
                    _ => None,
                }
            } else if let Some(column_index) =
                aggregate_cast_identifier_column_index(&args[0], &["numeric"], batch)
            {
                match &batch.columns[column_index] {
                    TypedColumn::Int64(_, _) => Some(AggKind::AvgNumericInt { column_index }),
                    TypedColumn::Numeric(_, _) => Some(AggKind::AvgNumeric { column_index }),
                    _ => None,
                }
            } else if let Some(column_index) = aggregate_cast_identifier_column_index(
                &args[0],
                &["float8", "double precision", "float4", "real"],
                batch,
            ) {
                match &batch.columns[column_index] {
                    TypedColumn::Int64(_, _) => Some(AggKind::AvgInt { column_index }),
                    TypedColumn::Float64(_, _) => Some(AggKind::AvgFloat { column_index }),
                    _ => None,
                }
            } else {
                None
            }
        }
        "min" if args.len() == 1 && !distinct => Some(AggKind::Min {
            column_index: aggregate_identifier_column_index(&args[0], batch)?,
        })
        .and_then(|kind| match kind {
            AggKind::Min { column_index } => match &batch.columns[column_index] {
                TypedColumn::Date(_, _) => Some(AggKind::MinDate { column_index }),
                _ => Some(AggKind::Min { column_index }),
            },
            _ => None,
        }),
        "max" if args.len() == 1 && !distinct => Some(AggKind::Max {
            column_index: aggregate_identifier_column_index(&args[0], batch)?,
        })
        .and_then(|kind| match kind {
            AggKind::Max { column_index } => match &batch.columns[column_index] {
                TypedColumn::Date(_, _) => Some(AggKind::MaxDate { column_index }),
                _ => Some(AggKind::Max { column_index }),
            },
            _ => None,
        }),
        _ => None,
    }
}

fn rewrite_columnar_agg_target_expr(
    expr: &Expr,
    group_output_names: &HashMap<String, String>,
    group_expr_outputs: &[(Expr, String)],
    agg_specs: &mut Vec<AggSpec>,
    output_exprs: &mut Vec<OutputExpr>,
    intermediate_columns: &mut Vec<String>,
    batch: &ColumnBatch,
) -> Option<Expr> {
    if let Some((_, name)) = group_expr_outputs
        .iter()
        .find(|(group_expr, _)| group_expr == expr)
    {
        return Some(Expr::Identifier(vec![name.clone()]));
    }
    match expr {
        Expr::Identifier(parts) => {
            let key = parts.join(".").to_ascii_lowercase();
            Some(Expr::Identifier(vec![
                group_output_names.get(&key)?.clone(),
            ]))
        }
        Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Parameter(_)
        | Expr::TypedLiteral { .. } => Some(expr.clone()),
        Expr::Cast { expr, type_name } => Some(Expr::Cast {
            expr: Box::new(rewrite_columnar_agg_target_expr(
                expr,
                group_output_names,
                group_expr_outputs,
                agg_specs,
                output_exprs,
                intermediate_columns,
                batch,
            )?),
            type_name: type_name.clone(),
        }),
        Expr::Unary { op, expr } => Some(Expr::Unary {
            op: op.clone(),
            expr: Box::new(rewrite_columnar_agg_target_expr(
                expr,
                group_output_names,
                group_expr_outputs,
                agg_specs,
                output_exprs,
                intermediate_columns,
                batch,
            )?),
        }),
        Expr::Binary { left, op, right } => Some(Expr::Binary {
            left: Box::new(rewrite_columnar_agg_target_expr(
                left,
                group_output_names,
                group_expr_outputs,
                agg_specs,
                output_exprs,
                intermediate_columns,
                batch,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_columnar_agg_target_expr(
                right,
                group_output_names,
                group_expr_outputs,
                agg_specs,
                output_exprs,
                intermediate_columns,
                batch,
            )?),
        }),
        Expr::FunctionCall {
            name,
            args,
            distinct,
            order_by,
            within_group,
            filter,
            over,
        } => {
            let kind = plan_columnar_aggregate_kind(
                name,
                args,
                *distinct,
                order_by,
                within_group,
                filter.as_deref(),
                over.as_deref(),
                batch,
            )?;
            let agg_index = agg_specs.len();
            agg_specs.push(AggSpec { kind });
            output_exprs.push(OutputExpr::Aggregate(agg_index));
            let temp_name = format!("__agg_{agg_index}");
            intermediate_columns.push(temp_name.clone());
            Some(Expr::Identifier(vec![temp_name]))
        }
        _ => None,
    }
}

fn plan_columnar_aggregation(
    select: &SelectStatement,
    batch: &ColumnBatch,
) -> Option<ColumnarAggPlan> {
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

    let grouping_exprs = select
        .group_by
        .iter()
        .map(|entry| match entry {
            GroupByExpr::Expr(expr) => Some(resolve_group_by_alias(expr, &select_alias_map)),
            GroupByExpr::GroupingSets(_) | GroupByExpr::Rollup(_) | GroupByExpr::Cube(_) => None,
        })
        .collect::<Option<Vec<_>>>()?;

    let mut group_key_sources = Vec::with_capacity(grouping_exprs.len());
    for expr in &grouping_exprs {
        if let Some(key) = identifier_key(expr) {
            group_key_sources.push(GroupKeySource::Column(batch.column_index(&key)?));
        } else {
            group_key_sources.push(GroupKeySource::DerivedExpr((*expr).clone()));
        }
    }

    let mut agg_specs = Vec::new();
    let mut output_exprs = Vec::new();
    let mut intermediate_columns = Vec::new();
    let mut group_output_names = HashMap::new();
    let mut group_expr_outputs = Vec::new();
    for (idx, expr) in grouping_exprs.iter().enumerate() {
        let temp_name = format!("__group_{idx}");
        if let Some(key) = identifier_key(expr) {
            group_output_names.insert(key, temp_name.clone());
        }
        group_expr_outputs.push(((*expr).clone(), temp_name.clone()));
        output_exprs.push(OutputExpr::GroupKey(idx));
        intermediate_columns.push(temp_name);
    }
    let mut final_target_exprs = Vec::with_capacity(select.targets.len());
    for target in &select.targets {
        let rewritten = rewrite_columnar_agg_target_expr(
            &target.expr,
            &group_output_names,
            &group_expr_outputs,
            &mut agg_specs,
            &mut output_exprs,
            &mut intermediate_columns,
            batch,
        )?;
        final_target_exprs.push(rewritten);
    }

    Some(ColumnarAggPlan {
        group_key_sources,
        agg_specs,
        output_exprs,
        intermediate_columns,
        final_target_exprs,
    })
}

fn agg_input_column_index(kind: &AggKind) -> Option<usize> {
    match kind {
        AggKind::CountStar => None,
        AggKind::Count { column_index }
        | AggKind::CountDistinctInt { column_index }
        | AggKind::CountDistinctText { column_index }
        | AggKind::SumInt { column_index }
        | AggKind::SumFloat { column_index }
        | AggKind::SumNumeric { column_index }
        | AggKind::AvgInt { column_index }
        | AggKind::AvgFloat { column_index }
        | AggKind::AvgNumericInt { column_index }
        | AggKind::AvgNumeric { column_index }
        | AggKind::MinDate { column_index }
        | AggKind::MaxDate { column_index }
        | AggKind::Min { column_index }
        | AggKind::Max { column_index } => Some(*column_index),
    }
}

fn remap_agg_kind(kind: &mut AggKind, index_map: &HashMap<usize, usize>) -> Option<()> {
    let remapped = match kind {
        AggKind::CountStar => return Some(()),
        AggKind::Count { column_index }
        | AggKind::CountDistinctInt { column_index }
        | AggKind::CountDistinctText { column_index }
        | AggKind::SumInt { column_index }
        | AggKind::SumFloat { column_index }
        | AggKind::SumNumeric { column_index }
        | AggKind::AvgInt { column_index }
        | AggKind::AvgFloat { column_index }
        | AggKind::AvgNumericInt { column_index }
        | AggKind::AvgNumeric { column_index }
        | AggKind::MinDate { column_index }
        | AggKind::MaxDate { column_index }
        | AggKind::Min { column_index }
        | AggKind::Max { column_index } => index_map.get(column_index).copied()?,
    };

    match kind {
        AggKind::CountStar => Some(()),
        AggKind::Count { column_index }
        | AggKind::CountDistinctInt { column_index }
        | AggKind::CountDistinctText { column_index }
        | AggKind::SumInt { column_index }
        | AggKind::SumFloat { column_index }
        | AggKind::SumNumeric { column_index }
        | AggKind::AvgInt { column_index }
        | AggKind::AvgFloat { column_index }
        | AggKind::AvgNumericInt { column_index }
        | AggKind::AvgNumeric { column_index }
        | AggKind::MinDate { column_index }
        | AggKind::MaxDate { column_index }
        | AggKind::Min { column_index }
        | AggKind::Max { column_index } => {
            *column_index = remapped;
            Some(())
        }
    }
}

fn projected_columns_for_columnar_aggregation(
    plan: &ColumnarAggPlan,
    batch: &ColumnBatch,
    remaining_predicate: Option<&Expr>,
) -> Option<Vec<usize>> {
    let mut projected = BTreeSet::new();
    for source in &plan.group_key_sources {
        match source {
            GroupKeySource::Column(column_index) => {
                projected.insert(*column_index);
            }
            GroupKeySource::DerivedExpr(expr) => {
                let mut referenced = HashSet::new();
                collect_referenced_columns(expr, &mut referenced);
                for column in referenced {
                    projected.insert(batch.column_index(&column)?);
                }
            }
        }
    }
    projected.extend(
        plan.agg_specs
            .iter()
            .filter_map(|spec| agg_input_column_index(&spec.kind)),
    );
    if let Some(predicate) = remaining_predicate {
        let mut referenced = HashSet::new();
        collect_referenced_columns(predicate, &mut referenced);
        for column in referenced {
            projected.insert(batch.column_index(&column)?);
        }
    }
    Some(projected.into_iter().collect())
}

fn remap_columnar_aggregation_plan(
    plan: &mut ColumnarAggPlan,
    projected_columns: &[usize],
) -> Option<()> {
    let index_map = projected_columns
        .iter()
        .enumerate()
        .map(|(projected_idx, original_idx)| (*original_idx, projected_idx))
        .collect::<HashMap<_, _>>();
    for source in &mut plan.group_key_sources {
        if let GroupKeySource::Column(column_index) = source {
            *column_index = index_map.get(column_index).copied()?;
        }
    }
    for spec in &mut plan.agg_specs {
        remap_agg_kind(&mut spec.kind, &index_map)?;
    }
    Some(())
}

fn identifier_projection_indices(exprs: &[Expr], columns: &[String]) -> Option<Vec<usize>> {
    exprs
        .iter()
        .map(|expr| {
            let Expr::Identifier(parts) = expr else {
                return None;
            };
            let qualified = parts.join(".");
            columns.iter().position(|column| {
                column.eq_ignore_ascii_case(&qualified)
                    || parts
                        .last()
                        .is_some_and(|short| column.eq_ignore_ascii_case(short))
            })
        })
        .collect()
}

fn columnar_group_key_indices(plan: &ColumnarAggPlan, base_column_count: usize) -> Vec<usize> {
    let mut next_derived = base_column_count;
    plan.group_key_sources
        .iter()
        .map(|source| match source {
            GroupKeySource::Column(column_index) => *column_index,
            GroupKeySource::DerivedExpr(_) => {
                let derived_index = next_derived;
                next_derived += 1;
                derived_index
            }
        })
        .collect()
}

async fn append_derived_group_key_columns(
    plan: &ColumnarAggPlan,
    batch: ColumnBatch,
    params: &[Option<String>],
) -> Result<ColumnBatch, EngineError> {
    let mut batch = batch;
    for (group_idx, source) in plan.group_key_sources.iter().enumerate() {
        let GroupKeySource::DerivedExpr(expr) = source else {
            continue;
        };
        if let Some(column) = eval_columnar_derived_group_expr(expr, &batch) {
            batch =
                batch.with_appended_typed_column(format!("__derived_group_{group_idx}"), column);
            continue;
        }
        let mut values = Vec::with_capacity(batch.row_count);
        for row_idx in 0..batch.row_count {
            let row = batch
                .columns
                .iter()
                .map(|column| column.value_at(row_idx))
                .collect::<Vec<_>>();
            let scope = EvalScope::from_output_row(&batch.column_names, &row);
            values.push(eval_expr(expr, &scope, params).await?);
        }
        batch = batch.with_appended_column(format!("__derived_group_{group_idx}"), values);
    }
    Ok(batch)
}

enum ColumnarTextSource {
    Column(usize),
    Literal(String),
    Null,
}

fn eval_columnar_derived_group_expr(expr: &Expr, batch: &ColumnBatch) -> Option<TypedColumn> {
    match expr {
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => eval_columnar_text_case(when_then, else_expr.as_deref(), batch),
        _ => None,
    }
}

fn eval_columnar_text_case(
    when_then: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    batch: &ColumnBatch,
) -> Option<TypedColumn> {
    let else_source = match else_expr {
        Some(expr) => text_source_for_columnar_eval(expr, batch)?,
        None => ColumnarTextSource::Null,
    };
    let (mut values, mut nulls) = text_source_column_values(&else_source, batch)?;

    for (when_expr, then_expr) in when_then.iter().rev() {
        let mask = eval_columnar_predicate(when_expr, batch)?;
        let then_source = text_source_for_columnar_eval(then_expr, batch)?;
        apply_text_source_mask(&mut values, &mut nulls, &mask, &then_source, batch)?;
    }

    Some(TypedColumn::Text(values, nulls))
}

fn text_source_for_columnar_eval(expr: &Expr, batch: &ColumnBatch) -> Option<ColumnarTextSource> {
    match expr {
        Expr::Identifier(parts) => {
            let column_idx = batch.column_index(&parts.join("."))?;
            match batch.columns.get(column_idx)? {
                TypedColumn::Text(_, _) => Some(ColumnarTextSource::Column(column_idx)),
                _ => None,
            }
        }
        Expr::String(value) | Expr::TypedLiteral { value, .. } => {
            Some(ColumnarTextSource::Literal(value.clone()))
        }
        Expr::Null => Some(ColumnarTextSource::Null),
        Expr::Cast { expr, .. } => text_source_for_columnar_eval(expr, batch),
        _ => None,
    }
}

fn text_source_column_values(
    source: &ColumnarTextSource,
    batch: &ColumnBatch,
) -> Option<(Vec<String>, Vec<bool>)> {
    match source {
        ColumnarTextSource::Column(column_idx) => match batch.columns.get(*column_idx)? {
            TypedColumn::Text(values, nulls) => Some((values.clone(), nulls.clone())),
            _ => None,
        },
        ColumnarTextSource::Literal(value) => Some((
            vec![value.clone(); batch.row_count],
            vec![false; batch.row_count],
        )),
        ColumnarTextSource::Null => Some((
            vec![String::new(); batch.row_count],
            vec![true; batch.row_count],
        )),
    }
}

fn apply_text_source_mask(
    values: &mut [String],
    nulls: &mut [bool],
    mask: &[bool],
    source: &ColumnarTextSource,
    batch: &ColumnBatch,
) -> Option<()> {
    match source {
        ColumnarTextSource::Column(column_idx) => match batch.columns.get(*column_idx)? {
            TypedColumn::Text(source_values, source_nulls) => {
                for row_idx in 0..values.len().min(mask.len()) {
                    if mask[row_idx] {
                        values[row_idx] = source_values[row_idx].clone();
                        nulls[row_idx] = source_nulls[row_idx];
                    }
                }
                Some(())
            }
            _ => None,
        },
        ColumnarTextSource::Literal(value) => {
            for row_idx in 0..values.len().min(mask.len()) {
                if mask[row_idx] {
                    values[row_idx] = value.clone();
                    nulls[row_idx] = false;
                }
            }
            Some(())
        }
        ColumnarTextSource::Null => {
            for row_idx in 0..values.len().min(mask.len()) {
                if mask[row_idx] {
                    values[row_idx].clear();
                    nulls[row_idx] = true;
                }
            }
            Some(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::eval_columnar_derived_group_expr;
    use crate::executor::column_batch::{ColumnBatch, TypedColumn};
    use crate::parser::ast::{BinaryOp, Expr};

    #[test]
    fn evaluates_searched_text_case_group_key_columnarly() {
        let batch = ColumnBatch {
            columns: vec![
                TypedColumn::Int64(vec![0, 1, 0], vec![false, false, false]),
                TypedColumn::Int64(vec![0, 0, 2], vec![false, false, false]),
                TypedColumn::Text(
                    vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
                    vec![false, false, false],
                ),
            ],
            column_names: vec![
                "SearchEngineID".to_string(),
                "AdvEngineID".to_string(),
                "Referer".to_string(),
            ],
            row_count: 3,
            record_batch: None,
        };

        let expr = Expr::CaseSearched {
            when_then: vec![(
                Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Identifier(vec!["SearchEngineID".to_string()])),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::Integer(0)),
                    }),
                    op: BinaryOp::And,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Identifier(vec!["AdvEngineID".to_string()])),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::Integer(0)),
                    }),
                },
                Expr::Identifier(vec!["Referer".to_string()]),
            )],
            else_expr: Some(Box::new(Expr::String(String::new()))),
        };

        let column = eval_columnar_derived_group_expr(&expr, &batch)
            .expect("case expression should vectorize");
        let TypedColumn::Text(values, nulls) = column else {
            panic!("expected text column");
        };

        assert_eq!(
            values,
            vec!["alpha".to_string(), String::new(), String::new()]
        );
        assert_eq!(nulls, vec![false, false, false]);
    }
}

fn projection_indices_for_simple_targets(
    targets: &[SelectItem],
    batch: &crate::executor::column_batch::ColumnBatch,
) -> Option<Vec<usize>> {
    let mut indices = Vec::new();
    for target in targets {
        match &target.expr {
            Expr::Identifier(parts) => {
                let name = parts.join(".");
                indices.push(batch.column_index(&name)?);
            }
            Expr::Wildcard | Expr::QualifiedWildcard(_) => {
                indices.extend(0..batch.columns.len());
            }
            _ => return None,
        }
    }
    Some(indices)
}

fn topn_projection_indices_for_query(
    query: &Query,
    batch: &ColumnBatch,
    predicate: Option<&Expr>,
    required_columns: &[usize],
) -> Option<Vec<usize>> {
    let mut projection = BTreeSet::new();
    projection.extend(required_columns.iter().copied());
    for spec in &query.order_by {
        let Expr::Identifier(parts) = &spec.expr else {
            return None;
        };
        projection.insert(batch.column_index(&parts.join("."))?);
    }
    if let Some(predicate) = predicate {
        let mut referenced = HashSet::new();
        collect_referenced_columns(predicate, &mut referenced);
        for column in referenced {
            projection.insert(batch.column_index(&column)?);
        }
    }
    Some(projection.into_iter().collect())
}

fn literal_text_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::String(value) => Some(value.clone()),
        Expr::TypedLiteral { value, .. } => Some(value.clone()),
        Expr::Cast { expr, .. } => literal_text_expr(expr),
        _ => None,
    }
}

fn classify_simple_like_predicate(
    expr: &Expr,
    batch: &ColumnBatch,
) -> Option<(usize, SimpleLikeMatcher)> {
    let Expr::Like {
        expr,
        pattern,
        case_insensitive,
        negated,
        escape,
    } = expr
    else {
        return None;
    };
    if *case_insensitive || *negated || escape.is_some() {
        return None;
    }
    let Expr::Identifier(parts) = expr.as_ref() else {
        return None;
    };
    let column_idx = batch.column_index(&parts.join("."))?;
    let pattern = literal_text_expr(pattern)?;

    if !pattern.contains('_') {
        if let Some(literal) = pattern
            .strip_prefix('%')
            .and_then(|value| value.strip_suffix('%'))
            && !literal.contains('%')
        {
            return Some((column_idx, SimpleLikeMatcher::Contains(literal.to_string())));
        }
        if let Some(literal) = pattern.strip_prefix('%')
            && !literal.contains('%')
        {
            return Some((column_idx, SimpleLikeMatcher::Suffix(literal.to_string())));
        }
        if let Some(literal) = pattern.strip_suffix('%')
            && !literal.contains('%')
        {
            return Some((column_idx, SimpleLikeMatcher::Prefix(literal.to_string())));
        }
        if !pattern.contains('%') {
            return Some((column_idx, SimpleLikeMatcher::Exact(pattern)));
        }
    }

    None
}

fn simple_like_matcher_from_pattern(pattern: String) -> Option<SimpleLikeMatcher> {
    if pattern.contains('_') {
        return None;
    }
    if let Some(literal) = pattern
        .strip_prefix('%')
        .and_then(|value| value.strip_suffix('%'))
        && !literal.contains('%')
    {
        return Some(SimpleLikeMatcher::Contains(literal.to_string()));
    }
    if let Some(literal) = pattern.strip_prefix('%')
        && !literal.contains('%')
    {
        return Some(SimpleLikeMatcher::Suffix(literal.to_string()));
    }
    if let Some(literal) = pattern.strip_suffix('%')
        && !literal.contains('%')
    {
        return Some(SimpleLikeMatcher::Prefix(literal.to_string()));
    }
    if !pattern.contains('%') {
        return Some(SimpleLikeMatcher::Exact(pattern));
    }
    None
}

fn classify_simple_like_scan_predicate(
    predicates: &[ScanPredicate],
) -> Option<(usize, SimpleLikeMatcher)> {
    let [predicate] = predicates else {
        return None;
    };
    if predicate.escape.is_some() || predicate.op != ScanPredicateOp::Like {
        return None;
    }
    let pattern = match &predicate.value {
        ScalarValue::Text(text) => text.clone(),
        other => other.render(),
    };
    simple_like_matcher_from_pattern(pattern).map(|matcher| (predicate.column_index, matcher))
}

async fn try_execute_simple_columnar_select(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    rel: &TableRef,
    relation_predicates: &[Expr],
    columns: &[String],
    _outer_scope: Option<&EvalScope>,
) -> Result<Option<QueryResult>, EngineError> {
    let _span = profiling::span("try_execute_simple_columnar_select");
    let Some(scan_plan) = prepare_columnar_relation_scan(rel, relation_predicates, params).await?
    else {
        return Ok(None);
    };
    let Some(projection_indices) =
        projection_indices_for_simple_targets(&select.targets, &scan_plan.schema_batch)
    else {
        return Ok(None);
    };
    let wildcard_only = select
        .targets
        .iter()
        .all(|target| matches!(target.expr, Expr::Wildcard | Expr::QualifiedWildcard(_)));

    let stage_limit = stage_limit_spec(query, params).await?;
    if wildcard_only
        && let Some(query) = query
        && stage_limit.is_none()
        && let Some(topn_projection) = topn_projection_indices_for_query(
            query,
            &scan_plan.schema_batch,
            scan_plan.remaining_predicate.as_ref(),
            &[],
        )
    {
        let topn_batch = scan_plan.schema_batch.project(&topn_projection);
        let topn_columns = topn_batch.column_names.clone();
        if let Some(mut offset_collector) =
            OffsetTopNCollector::new(query, &topn_columns, params).await?
        {
            let pushed_simple_like = scan_plan
                .remaining_predicate
                .is_none()
                .then(|| classify_simple_like_scan_predicate(&scan_plan.scan_predicates))
                .flatten();
            let simple_like = scan_plan
                .remaining_predicate
                .as_ref()
                .and_then(|predicate| classify_simple_like_predicate(predicate, &topn_batch));
            if let Some((source_column_idx, matcher)) = &pushed_simple_like {
                with_storage_write(|storage| {
                    storage.scan_record_batches_for_table(
                        scan_plan.table.oid(),
                        &mut |batch, deleted_rows, batch_start| {
                            let _span = profiling::span("columnar_offset_topn_pushed_like_stream");
                            let Some(values) = batch
                                .columns()
                                .get(*source_column_idx)
                                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                            else {
                                return Ok(true);
                            };
                            let deleted_rows = deleted_rows
                                .iter()
                                .any(|deleted| *deleted)
                                .then_some(deleted_rows);
                            for_each_matching_text_row(values, deleted_rows, matcher, |row_idx| {
                                offset_collector.push_record_batch_row(
                                    batch,
                                    &topn_projection,
                                    row_idx,
                                    batch_start + row_idx,
                                );
                            });
                            Ok(true)
                        },
                    )
                })
                .map_err(|message| EngineError { message })?;

                let offsets = offset_collector.finish();
                let rows = with_storage_write(|storage| {
                    storage.scan_rows_for_table(scan_plan.table.oid(), Some(&offsets), &[], None)
                })
                .map_err(|message| EngineError { message })?;
                return Ok(Some(QueryResult {
                    columns: columns.to_vec(),
                    rows_affected: rows.len() as u64,
                    rows,
                    command_tag: "SELECT".to_string(),
                }));
            }
            if let Some((column_idx, matcher)) = &simple_like {
                let source_column_idx = topn_projection[*column_idx];
                with_storage_write(|storage| {
                    if scan_plan.scan_predicates.is_empty() {
                        storage.scan_record_batches_for_table(
                            scan_plan.table.oid(),
                            &mut |batch, deleted_rows, batch_start| {
                                let _span =
                                    profiling::span("columnar_offset_topn_simple_like_stream");
                                let Some(values) =
                                    batch.columns().get(source_column_idx).and_then(|column| {
                                        column.as_any().downcast_ref::<StringArray>()
                                    })
                                else {
                                    return Ok(true);
                                };
                                let deleted_rows = deleted_rows
                                    .iter()
                                    .any(|deleted| *deleted)
                                    .then_some(deleted_rows);
                                for_each_matching_text_row(
                                    values,
                                    deleted_rows,
                                    matcher,
                                    |row_idx| {
                                        offset_collector.push_record_batch_row(
                                            batch,
                                            &topn_projection,
                                            row_idx,
                                            batch_start + row_idx,
                                        );
                                    },
                                );
                                Ok(true)
                            },
                        )
                    } else {
                        storage.scan_selected_offsets_for_table(
                            scan_plan.table.oid(),
                            &scan_plan.scan_predicates,
                            &mut |batch, selected_rows, offsets| {
                                let _span = profiling::span("columnar_offset_topn_simple_like_raw");
                                let Some(values) =
                                    batch.columns().get(source_column_idx).and_then(|column| {
                                        column.as_any().downcast_ref::<StringArray>()
                                    })
                                else {
                                    return Ok(true);
                                };
                                let mut matched_rows = Vec::new();
                                let mut matched_offsets = Vec::new();
                                for (&row_idx, &row_offset) in
                                    selected_rows.iter().zip(offsets.iter())
                                {
                                    if !values.is_null(row_idx)
                                        && matcher.matches(values.value(row_idx))
                                    {
                                        matched_rows.push(row_idx);
                                        matched_offsets.push(row_offset);
                                    }
                                }
                                offset_collector.push_record_batch_selected_rows(
                                    batch,
                                    &topn_projection,
                                    &matched_rows,
                                    &matched_offsets,
                                );
                                Ok(true)
                            },
                        )
                    }
                })
                .map_err(|message| EngineError { message })?;

                let offsets = offset_collector.finish();
                let rows = with_storage_write(|storage| {
                    storage.scan_rows_for_table(scan_plan.table.oid(), Some(&offsets), &[], None)
                })
                .map_err(|message| EngineError { message })?;
                return Ok(Some(QueryResult {
                    columns: columns.to_vec(),
                    rows_affected: rows.len() as u64,
                    rows,
                    command_tag: "SELECT".to_string(),
                }));
            }
            if scan_plan.remaining_predicate.is_none() {
                with_storage_write(|storage| {
                    if scan_plan.scan_predicates.is_empty() {
                        storage.scan_record_batches_for_table(
                            scan_plan.table.oid(),
                            &mut |batch, deleted_rows, batch_start| {
                                for row_idx in 0..batch.num_rows() {
                                    if deleted_rows.get(row_idx).copied().unwrap_or(false) {
                                        continue;
                                    }
                                    offset_collector.push_record_batch_row(
                                        batch,
                                        &topn_projection,
                                        row_idx,
                                        batch_start + row_idx,
                                    );
                                }
                                Ok(true)
                            },
                        )
                    } else {
                        storage.scan_selected_offsets_for_table(
                            scan_plan.table.oid(),
                            &scan_plan.scan_predicates,
                            &mut |batch, selected_rows, offsets| {
                                offset_collector.push_record_batch_selected_rows(
                                    batch,
                                    &topn_projection,
                                    &selected_rows,
                                    &offsets,
                                );
                                Ok(true)
                            },
                        )
                    }
                })
                .map_err(|message| EngineError { message })?;

                let offsets = offset_collector.finish();
                let rows = with_storage_write(|storage| {
                    storage.scan_rows_for_table(scan_plan.table.oid(), Some(&offsets), &[], None)
                })
                .map_err(|message| EngineError { message })?;
                return Ok(Some(QueryResult {
                    columns: columns.to_vec(),
                    rows_affected: rows.len() as u64,
                    rows,
                    command_tag: "SELECT".to_string(),
                }));
            }
            with_storage_write(|storage| {
                    storage.scan_selected_batches_for_table_with_offsets(
                        scan_plan.table.oid(),
                        &scan_plan.scan_predicates,
                        Some(topn_projection.as_slice()),
                        &mut |batch, selected_rows, offsets| {
                            if let Some((column_idx, matcher)) = &simple_like {
                                let _span = profiling::span(
                                    "columnar_offset_topn_simple_like_match",
                                );
                                let mut matched_rows = Vec::new();
                                let mut matched_offsets = Vec::new();
                                if let Some(TypedColumn::Text(values, nulls)) =
                                    batch.columns.get(*column_idx)
                                {
                                    for (&row_idx, &row_offset) in
                                        selected_rows.iter().zip(offsets.iter())
                                    {
                                        if !nulls[row_idx] && matcher.matches(&values[row_idx]) {
                                            matched_rows.push(row_idx);
                                            matched_offsets.push(row_offset);
                                        }
                                    }
                                }
                                if !matched_rows.is_empty() {
                                    offset_collector.push_selected_rows(
                                        &batch,
                                        &matched_rows,
                                        &matched_offsets,
                                    );
                                }
                                return Ok(true);
                            }
                            if let Some(predicate) = &scan_plan.remaining_predicate {
                                let _span = profiling::span(
                                    "columnar_offset_topn_predicate_filter",
                                );
                                let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
                                    return Err(
                                        "columnar predicate could not be evaluated in fused wildcard pipeline"
                                            .to_string(),
                                    );
                                };
                                let mut filtered_rows = Vec::new();
                                let mut filtered_offsets = Vec::new();
                                for (&row_idx, &row_offset) in
                                    selected_rows.iter().zip(offsets.iter())
                                {
                                    if mask.get(row_idx).copied().unwrap_or(false) {
                                        filtered_rows.push(row_idx);
                                        filtered_offsets.push(row_offset);
                                    }
                                }
                                offset_collector.push_selected_rows(
                                    &batch,
                                    &filtered_rows,
                                    &filtered_offsets,
                                );
                                return Ok(true);
                            }
                            if selected_rows.is_empty() {
                                return Ok(true);
                            }
                            let _span = profiling::span("columnar_offset_topn_push_selected_rows");
                            offset_collector.push_selected_rows(&batch, &selected_rows, &offsets);
                            Ok(true)
                        },
                    )
                })
                .map_err(|message| EngineError { message })?;

            let offsets = offset_collector.finish();
            let rows = with_storage_write(|storage| {
                storage.scan_rows_for_table(scan_plan.table.oid(), Some(&offsets), &[], None)
            })
            .map_err(|message| EngineError { message })?;
            return Ok(Some(QueryResult {
                columns: columns.to_vec(),
                rows_affected: rows.len() as u64,
                rows,
                command_tag: "SELECT".to_string(),
            }));
        }
    }
    if !wildcard_only
        && let Some(query) = query
        && stage_limit.is_none()
        && let Some(topn_projection) = topn_projection_indices_for_query(
            query,
            &scan_plan.schema_batch,
            scan_plan.remaining_predicate.as_ref(),
            &projection_indices,
        )
    {
        let topn_batch = scan_plan.schema_batch.project(&topn_projection);
        let topn_columns = topn_batch.column_names.clone();
        let projection_index_map = topn_projection
            .iter()
            .enumerate()
            .map(|(projected_idx, original_idx)| (*original_idx, projected_idx))
            .collect::<HashMap<_, _>>();
        let selected_positions = projection_indices
            .iter()
            .map(|column_idx| projection_index_map.get(column_idx).copied())
            .collect::<Option<Vec<_>>>();
        if let Some(mut topn_collector) =
            SimpleTopNCollector::new(query, &topn_columns, params).await?
            && let Some(selected_positions) = selected_positions
        {
            let simple_like = scan_plan
                .remaining_predicate
                .as_ref()
                .and_then(|predicate| classify_simple_like_predicate(predicate, &topn_batch));
            with_storage_write(|storage| {
                storage.scan_selected_batches_for_table(
                    scan_plan.table.oid(),
                    &scan_plan.scan_predicates,
                    Some(topn_projection.as_slice()),
                    &mut |batch, selected_rows| {
                        if let Some((column_idx, matcher)) = &simple_like {
                            let _span = profiling::span("columnar_topn_simple_like_match");
                            let matched_rows = if let Some(TypedColumn::Text(values, nulls)) =
                                batch.columns.get(*column_idx)
                            {
                                selected_rows
                                    .iter()
                                    .copied()
                                    .filter(|row_idx| {
                                        !nulls[*row_idx] && matcher.matches(&values[*row_idx])
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            };
                            if matched_rows.is_empty() {
                                return Ok(true);
                            }
                            let _span = profiling::span("columnar_topn_push_selected_rows");
                            topn_collector.push_selected_rows(&batch, &matched_rows);
                            return Ok(true);
                        }
                        if let Some(predicate) = &scan_plan.remaining_predicate {
                            let _span = profiling::span("columnar_topn_predicate_filter");
                            let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
                                return Err(
                                    "columnar predicate could not be evaluated in fused projected pipeline"
                                        .to_string(),
                                );
                            };
                            let filtered_rows = selected_rows
                                .iter()
                                .copied()
                                .filter(|row_idx| mask.get(*row_idx).copied().unwrap_or(false))
                                .collect::<Vec<_>>();
                            if filtered_rows.is_empty() {
                                return Ok(true);
                            }
                            let _span = profiling::span("columnar_topn_push_selected_rows");
                            topn_collector.push_selected_rows(&batch, &filtered_rows);
                            return Ok(true);
                        }
                        if selected_rows.is_empty() {
                            return Ok(true);
                        }
                        let _span = profiling::span("columnar_topn_push_selected_rows");
                        topn_collector.push_selected_rows(&batch, &selected_rows);
                        Ok(true)
                    },
                )
            })
            .map_err(|message| EngineError { message })?;

            let rows = topn_collector
                .finish()
                .into_iter()
                .map(|row| {
                    selected_positions
                        .iter()
                        .filter_map(|position| row.get(*position).cloned())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            return Ok(Some(QueryResult {
                columns: columns.to_vec(),
                rows_affected: rows.len() as u64,
                rows,
                command_tag: "SELECT".to_string(),
            }));
        }
    }
    if let Some(query) = query
        && stage_limit.is_none()
        && let Some(mut topn_collector) = SimpleTopNCollector::new(query, columns, params).await?
    {
        with_storage_write(|storage| {
            storage.scan_batches_for_table(
                scan_plan.table.oid(),
                &scan_plan.scan_predicates,
                None,
                &mut |batch| {
                    let filtered = if let Some(predicate) = &scan_plan.remaining_predicate {
                        let _span = profiling::span("columnar_select_filter_batch");
                        let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
                            return Err(
                                "columnar predicate could not be evaluated in fused pipeline"
                                    .to_string(),
                            );
                        };
                        batch.filter(&mask)
                    } else {
                        batch
                    };
                    if filtered.row_count == 0 {
                        return Ok(true);
                    }
                    let projected = filtered.project(&projection_indices);
                    if projected.row_count == 0 {
                        return Ok(true);
                    }
                    let _span = profiling::span("columnar_select_topn_push_batch");
                    topn_collector.push_batch(&projected);
                    Ok(true)
                },
            )
        })
        .map_err(|message| EngineError { message })?;

        let rows = topn_collector.finish();
        return Ok(Some(QueryResult {
            columns: columns.to_vec(),
            rows_affected: rows.len() as u64,
            rows,
            command_tag: "SELECT".to_string(),
        }));
    }

    let mut pipeline: Box<dyn PipelineStage> = Box::new(ProjectStage::new(
        projection_indices,
        Box::new(BatchCollector::new(columns.to_vec())),
    ));
    if let Some(limit_spec) = &stage_limit {
        pipeline = Box::new(LimitStage::new(
            limit_spec.limit,
            limit_spec.offset,
            pipeline,
        ));
    }
    if let Some(predicate) = scan_plan.remaining_predicate.clone() {
        if eval_columnar_predicate(&predicate, &scan_plan.schema_batch).is_none() {
            return Ok(None);
        }
        pipeline = Box::new(FilterStage::new(predicate, pipeline));
    }

    with_storage_write(|storage| {
        storage.scan_batches_for_table(
            scan_plan.table.oid(),
            &scan_plan.scan_predicates,
            None,
            &mut |batch| {
                pipeline
                    .push_batch(&batch)
                    .map(|_| true)
                    .map_err(|err| err.message)
            },
        )
    })
    .map_err(|message| EngineError { message })?;

    let result_batch = pipeline
        .finish()?
        .unwrap_or_else(|| ColumnBatch::empty(columns.to_vec()));
    let mut rows = result_batch.to_rows();

    if query.is_some()
        && stage_limit.is_none()
        && let Some(query) = query
    {
        let mut collector = QueryRowCollector::new(query, params).await?;
        for row in rows {
            if !collector.push_row(columns, row, params).await? {
                break;
            }
        }
        rows = collector.finish();
    }

    Ok(Some(QueryResult {
        columns: columns.to_vec(),
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    }))
}

async fn try_execute_columnar_aggregation(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    rel: &TableRef,
    relation_predicates: &[Expr],
    columns: &[String],
    _outer_scope: Option<&EvalScope>,
) -> Result<Option<QueryResult>, EngineError> {
    let _span = profiling::span("try_execute_columnar_aggregation");
    let Some(scan_plan) = prepare_columnar_relation_scan(rel, relation_predicates, params).await?
    else {
        return Ok(None);
    };
    let Some(mut plan) = plan_columnar_aggregation(select, &scan_plan.schema_batch) else {
        return Ok(None);
    };
    let Some(projected_columns) = projected_columns_for_columnar_aggregation(
        &plan,
        &scan_plan.schema_batch,
        scan_plan.remaining_predicate.as_ref(),
    ) else {
        return Ok(None);
    };
    if remap_columnar_aggregation_plan(&mut plan, &projected_columns).is_none() {
        return Ok(None);
    }
    let has_derived_group_keys = plan
        .group_key_sources
        .iter()
        .any(|source| matches!(source, GroupKeySource::DerivedExpr(_)));

    let result_batch = if has_derived_group_keys {
        let mut batch = with_storage_write(|storage| {
            storage.scan_columnar_for_table(
                scan_plan.table.oid(),
                &scan_plan.scan_predicates,
                Some(projected_columns.as_slice()),
            )
        })
        .map_err(|message| EngineError { message })?;
        if let Some(predicate) = &scan_plan.remaining_predicate {
            let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
                return Ok(None);
            };
            batch = batch.filter(&mask);
        }
        batch = append_derived_group_key_columns(&plan, batch, params).await?;
        let mut aggregator = ColumnarAggregator::new(
            columnar_group_key_indices(&plan, projected_columns.len()),
            plan.agg_specs,
            plan.output_exprs,
            plan.intermediate_columns.clone(),
        );
        aggregator.push_batch(&batch)?;
        aggregator.finish()?
    } else {
        let aggregator = ColumnarAggregator::new(
            columnar_group_key_indices(&plan, projected_columns.len()),
            plan.agg_specs,
            plan.output_exprs,
            plan.intermediate_columns.clone(),
        );
        let mut aggregator = aggregator;
        if let Some(predicate) = scan_plan.remaining_predicate.clone()
            && eval_columnar_predicate(&predicate, &scan_plan.schema_batch).is_none()
        {
            return Ok(None);
        }

        with_storage_write(|storage| {
            storage.scan_selected_batches_for_table(
                scan_plan.table.oid(),
                &scan_plan.scan_predicates,
                Some(projected_columns.as_slice()),
                &mut |batch, selected_rows| {
                    let selected_rows = if let Some(predicate) = &scan_plan.remaining_predicate {
                        let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
                            return Err(
                                "columnar predicate could not be evaluated in grouped aggregation"
                                    .to_string(),
                            );
                        };
                        selected_rows
                            .iter()
                            .copied()
                            .filter(|row_idx| mask.get(*row_idx).copied().unwrap_or(false))
                            .collect::<Vec<_>>()
                    } else {
                        selected_rows
                    };
                    if selected_rows.is_empty() {
                        return Ok(true);
                    }
                    aggregator
                        .push_selected_rows(&batch, &selected_rows)
                        .map(|()| true)
                        .map_err(|err| err.message)
                },
            )
        })
        .map_err(|message| EngineError { message })?;

        aggregator.finish()?
    };

    if let Some(projection_indices) =
        identifier_projection_indices(&plan.final_target_exprs, &plan.intermediate_columns)
    {
        let projected_batch = result_batch.project(&projection_indices);
        if let Some(query) = query
            && let Some(mut topn_collector) =
                SimpleTopNCollector::new(query, columns, params).await?
        {
            topn_collector.push_batch(&projected_batch);
            let rows = topn_collector.finish();
            return Ok(Some(QueryResult {
                columns: columns.to_vec(),
                rows_affected: rows.len() as u64,
                rows,
                command_tag: "SELECT".to_string(),
            }));
        }

        let mut rows = projected_batch.to_rows();
        if let Some(query) = query {
            let mut collector = QueryRowCollector::new(query, params).await?;
            for row in rows {
                if !collector.push_row(columns, row, params).await? {
                    break;
                }
            }
            rows = collector.finish();
        }

        return Ok(Some(QueryResult {
            columns: columns.to_vec(),
            rows_affected: rows.len() as u64,
            rows,
            command_tag: "SELECT".to_string(),
        }));
    }

    let mut rows = Vec::with_capacity(result_batch.row_count);
    let compiled_final_targets = compile_exprs(&plan.final_target_exprs)?;
    for row in result_batch.to_rows() {
        let scope = EvalScope::from_output_row(&plan.intermediate_columns, &row);
        let mut projected = Vec::with_capacity(plan.final_target_exprs.len());
        for (expr, compiled) in plan.final_target_exprs.iter().zip(&compiled_final_targets) {
            projected
                .push(eval_expr_maybe_compiled(expr, compiled.as_ref(), &scope, params).await?);
        }
        rows.push(projected);
    }

    if let Some(query) = query {
        let mut collector = QueryRowCollector::new(query, params).await?;
        for row in rows {
            if !collector.push_row(columns, row, params).await? {
                break;
            }
        }
        rows = collector.finish();
    }

    Ok(Some(QueryResult {
        columns: columns.to_vec(),
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    }))
}

async fn plan_columnar_window_targets(
    select: &SelectStatement,
    batch: &ColumnBatch,
    params: &[Option<String>],
) -> Result<Option<Vec<ColumnarWindowTargetPlan>>, EngineError> {
    let mut targets = Vec::with_capacity(select.targets.len());
    for target in &select.targets {
        match &target.expr {
            Expr::Identifier(parts) => {
                let Some(column_index) = batch.column_index(&parts.join(".")) else {
                    return Ok(None);
                };
                targets.push(ColumnarWindowTargetPlan::InputColumn(column_index));
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                order_by,
                within_group,
                filter,
                over: Some(window),
            } => {
                if *distinct || !order_by.is_empty() || !within_group.is_empty() || filter.is_some()
                {
                    return Ok(None);
                }
                let function_name = name
                    .last()
                    .map_or_else(String::new, |value| value.to_ascii_lowercase());
                if !matches!(
                    function_name.as_str(),
                    "row_number"
                        | "rank"
                        | "dense_rank"
                        | "lag"
                        | "lead"
                        | "ntile"
                        | "first_value"
                        | "last_value"
                        | "sum"
                        | "count"
                        | "avg"
                        | "min"
                        | "max"
                ) {
                    return Ok(None);
                }
                let resolved_window = resolve_window_spec(window, &select.window_definitions)?;
                if resolved_window
                    .frame
                    .as_ref()
                    .is_some_and(|frame| frame.exclusion.is_some())
                {
                    return Ok(None);
                }
                if resolved_window
                    .frame
                    .as_ref()
                    .is_some_and(|frame| frame.units != crate::parser::ast::WindowFrameUnits::Rows)
                {
                    return Ok(None);
                }
                let partition_by_indices = resolved_window
                    .partition_by
                    .iter()
                    .map(|expr| match expr {
                        Expr::Identifier(parts) => batch.column_index(&parts.join(".")),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| EngineError {
                        message: "columnar window partition expressions must be simple columns"
                            .to_string(),
                    })?;
                let order_by_indices = resolved_window
                    .order_by
                    .iter()
                    .map(|entry| match &entry.expr {
                        Expr::Identifier(parts) => batch.column_index(&parts.join(".")),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| EngineError {
                        message: "columnar window ORDER BY expressions must be simple columns"
                            .to_string(),
                    })?;
                let mut argument_kinds = Vec::with_capacity(args.len());
                for arg in args {
                    match arg {
                        Expr::Wildcard => argument_kinds.push(WindowArgumentKind::Wildcard),
                        Expr::Identifier(parts) => {
                            let Some(column_index) = batch.column_index(&parts.join(".")) else {
                                return Ok(None);
                            };
                            argument_kinds.push(WindowArgumentKind::Column(column_index));
                        }
                        _ if !expr_references_columns(arg) => {
                            argument_kinds.push(WindowArgumentKind::Constant(
                                eval_expr(arg, &EvalScope::default(), params).await?,
                            ));
                        }
                        _ => return Ok(None),
                    }
                }
                targets.push(ColumnarWindowTargetPlan::Window(WindowColumnPlan {
                    function_name,
                    argument_kinds,
                    partition_by_indices,
                    order_by: resolved_window.order_by,
                    order_by_indices,
                    frame: resolved_window.frame,
                }));
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(targets))
}

async fn try_execute_columnar_windows(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    rel: &TableRef,
    relation_predicates: &[Expr],
    columns: &[String],
    outer_scope: Option<&EvalScope>,
) -> Result<Option<QueryResult>, EngineError> {
    let _span = profiling::span("try_execute_columnar_windows");
    let (table_eval, pushed_predicates) =
        evaluate_relation_with_predicates_columnar(rel, params, outer_scope, relation_predicates)
            .await?;
    let Some(mut batch) = table_eval.batch else {
        return Ok(None);
    };
    let mut applied = vec![false; relation_predicates.len()];
    for idx in pushed_predicates {
        if let Some(flag) = applied.get_mut(idx) {
            *flag = true;
        }
    }
    let remaining_predicate = remaining_predicate_from_applied(relation_predicates, &applied);
    if let Some(predicate) = &remaining_predicate {
        let Some(mask) = eval_columnar_predicate(predicate, &batch) else {
            return Ok(None);
        };
        batch = batch.filter(&mask);
    }

    let Some(target_plans) = plan_columnar_window_targets(select, &batch, params).await? else {
        return Ok(None);
    };

    let mut partition_cache: HashMap<String, WindowPartitions> = HashMap::new();
    let mut computed_window_columns: Vec<Option<Vec<ScalarValue>>> = vec![None; target_plans.len()];
    for (idx, target_plan) in target_plans.iter().enumerate() {
        let ColumnarWindowTargetPlan::Window(plan) = target_plan else {
            continue;
        };
        let cache_key = format!(
            "{:?}|{:?}|{:?}",
            plan.partition_by_indices, plan.order_by_indices, plan.order_by
        );
        let partitions = if let Some(existing) = partition_cache.get(&cache_key) {
            existing.clone()
        } else {
            let built = WindowPartitions::build(
                &batch,
                &plan.partition_by_indices,
                &plan.order_by_indices,
                &plan.order_by,
            )?;
            partition_cache.insert(cache_key, built.clone());
            built
        };
        computed_window_columns[idx] =
            Some(eval_window_function_columnar(plan, &partitions, &batch)?);
    }

    let mut rows = Vec::with_capacity(batch.row_count);
    for row_idx in 0..batch.row_count {
        let mut row = Vec::with_capacity(target_plans.len());
        for (target_idx, target_plan) in target_plans.iter().enumerate() {
            match target_plan {
                ColumnarWindowTargetPlan::InputColumn(column_index) => {
                    row.push(batch.columns[*column_index].value_at(row_idx));
                }
                ColumnarWindowTargetPlan::Window(_) => {
                    row.push(
                        computed_window_columns[target_idx]
                            .as_ref()
                            .and_then(|values| values.get(row_idx))
                            .cloned()
                            .unwrap_or(ScalarValue::Null),
                    );
                }
            }
        }
        rows.push(row);
    }

    if let Some(query) = query {
        let mut collector = QueryRowCollector::new(query, params).await?;
        for row in rows {
            if !collector.push_row(columns, row, params).await? {
                break;
            }
        }
        rows = collector.finish();
    }

    Ok(Some(QueryResult {
        columns: columns.to_vec(),
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    }))
}

pub(super) async fn execute_select(
    select: &SelectStatement,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    execute_select_internal(select, None, params, outer_scope).await
}

async fn execute_select_with_query(
    select: &SelectStatement,
    query: &Query,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    execute_select_internal(select, Some(query), params, outer_scope).await
}

fn resolve_simple_aggregate_column(
    expr: &Expr,
    qualifiers: &[String],
    column_indexes: &HashMap<String, usize>,
) -> Option<usize> {
    match expr {
        Expr::Identifier(parts) if parts.len() == 1 => {
            column_indexes.get(&parts[0].to_ascii_lowercase()).copied()
        }
        Expr::Identifier(parts)
            if parts.len() == 2 && qualifiers.iter().any(|q| q.eq_ignore_ascii_case(&parts[0])) =>
        {
            column_indexes.get(&parts[1].to_ascii_lowercase()).copied()
        }
        _ => None,
    }
}

fn simple_aggregate_request_impl(
    expr: &Expr,
    qualifiers: &[String],
    column_indexes: &HashMap<String, usize>,
    allow_distinct_count: bool,
) -> Option<ColumnAggregateRequest> {
    let Expr::FunctionCall {
        name,
        args,
        distinct,
        order_by,
        within_group,
        filter,
        over,
    } = expr
    else {
        return None;
    };
    if name.len() != 1
        || !order_by.is_empty()
        || !within_group.is_empty()
        || filter.is_some()
        || over.is_some()
    {
        return None;
    }

    match name[0].to_ascii_lowercase().as_str() {
        "count" => {
            if args.len() != 1 || (*distinct && !allow_distinct_count) {
                return None;
            }
            if matches!(args[0], Expr::Wildcard) {
                return Some(ColumnAggregateRequest {
                    op: ColumnAggregateOp::CountAll,
                    column_index: None,
                    distinct: false,
                });
            }
            resolve_simple_aggregate_column(&args[0], qualifiers, column_indexes).map(
                |column_index| ColumnAggregateRequest {
                    op: ColumnAggregateOp::Count,
                    column_index: Some(column_index),
                    distinct: *distinct,
                },
            )
        }
        "sum" | "avg" => {
            if args.len() != 1 || *distinct {
                return None;
            }
            resolve_simple_aggregate_column(&args[0], qualifiers, column_indexes).map(
                |column_index| ColumnAggregateRequest {
                    op: if name[0].eq_ignore_ascii_case("sum") {
                        ColumnAggregateOp::Sum
                    } else {
                        ColumnAggregateOp::Avg
                    },
                    column_index: Some(column_index),
                    distinct: false,
                },
            )
        }
        _ => None,
    }
}

fn simple_aggregate_request(
    expr: &Expr,
    qualifiers: &[String],
    column_indexes: &HashMap<String, usize>,
) -> Option<ColumnAggregateRequest> {
    simple_aggregate_request_impl(expr, qualifiers, column_indexes, false)
}

fn simple_group_aggregate_request(
    expr: &Expr,
    qualifiers: &[String],
    column_indexes: &HashMap<String, usize>,
) -> Option<ColumnAggregateRequest> {
    simple_aggregate_request_impl(expr, qualifiers, column_indexes, true)
}

#[derive(Debug, Clone, Copy)]
enum SimpleGroupedTarget {
    GroupColumn(usize),
    Aggregate(usize),
}

async fn try_execute_simple_column_aggregates(
    select: &SelectStatement,
    query: Option<&Query>,
    outer_scope: Option<&EvalScope>,
    columns: &[String],
) -> Result<Option<QueryResult>, EngineError> {
    if outer_scope.is_some()
        || select.targets.is_empty()
        || select.from.len() != 1
        || select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.window_definitions.is_empty()
        || select.quantifier.is_some()
    {
        return Ok(None);
    }

    if let Some(query) = query
        && (!query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some())
    {
        return Ok(None);
    }

    let TableExpression::Relation(rel) = &select.from[0] else {
        return Ok(None);
    };
    if rel.name.len() == 1 && current_cte_binding(&rel.name[0]).is_some() {
        return Ok(None);
    }
    if lookup_virtual_relation(&rel.name).is_some() {
        return Ok(None);
    }

    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView) {
        return Ok(None);
    }

    require_relation_privilege(&table, TablePrivilege::Select)?;
    let role = security::current_role();
    let evaluation = security::rls_evaluation_for_role(&role, table.oid(), RlsCommand::Select);
    if evaluation.enabled && !evaluation.bypass {
        return Ok(None);
    }

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let column_indexes = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| (column.name().to_string(), idx))
        .collect::<HashMap<_, _>>();
    let Some(requests) = select
        .targets
        .iter()
        .map(|target| simple_aggregate_request(&target.expr, &qualifiers, &column_indexes))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    let Some(values) = crate::tcop::engine::with_storage_write(|storage| {
        storage.aggregate_columns_for_table(table.oid(), &requests)
    })
    .map_err(|message| EngineError { message })?
    else {
        return Ok(None);
    };

    Ok(Some(QueryResult {
        columns: columns.to_vec(),
        rows: vec![values],
        command_tag: "SELECT".to_string(),
        rows_affected: 1,
    }))
}

async fn try_execute_simple_grouped_column_aggregates(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    columns: &[String],
) -> Result<Option<QueryResult>, EngineError> {
    if outer_scope.is_some()
        || select.targets.is_empty()
        || select.from.len() != 1
        || select.where_clause.is_some()
        || select.group_by.is_empty()
        || select.having.is_some()
        || !select.window_definitions.is_empty()
        || select.quantifier.is_some()
    {
        return Ok(None);
    }

    let TableExpression::Relation(rel) = &select.from[0] else {
        return Ok(None);
    };
    if rel.name.len() == 1 && current_cte_binding(&rel.name[0]).is_some() {
        return Ok(None);
    }
    if lookup_virtual_relation(&rel.name).is_some() {
        return Ok(None);
    }

    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView) {
        return Ok(None);
    }

    require_relation_privilege(&table, TablePrivilege::Select)?;
    let role = security::current_role();
    let evaluation = security::rls_evaluation_for_role(&role, table.oid(), RlsCommand::Select);
    if evaluation.enabled && !evaluation.bypass {
        return Ok(None);
    }

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let column_indexes = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| (column.name().to_string(), idx))
        .collect::<HashMap<_, _>>();

    let Some(group_column_indexes) = select
        .group_by
        .iter()
        .map(|group_expr| match group_expr {
            GroupByExpr::Expr(expr) => {
                resolve_simple_aggregate_column(expr, &qualifiers, &column_indexes)
            }
            GroupByExpr::GroupingSets(_) | GroupByExpr::Rollup(_) | GroupByExpr::Cube(_) => None,
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    let mut aggregate_requests = Vec::new();
    let mut target_plan = Vec::with_capacity(select.targets.len());
    for target in &select.targets {
        if let Some(column_index) =
            resolve_simple_aggregate_column(&target.expr, &qualifiers, &column_indexes)
        {
            let Some(group_position) = group_column_indexes
                .iter()
                .position(|group_index| *group_index == column_index)
            else {
                return Ok(None);
            };
            target_plan.push(SimpleGroupedTarget::GroupColumn(group_position));
            continue;
        }

        let Some(request) =
            simple_group_aggregate_request(&target.expr, &qualifiers, &column_indexes)
        else {
            return Ok(None);
        };
        let request_index = aggregate_requests.len();
        aggregate_requests.push(request);
        target_plan.push(SimpleGroupedTarget::Aggregate(request_index));
    }

    let Some(group_rows) = crate::tcop::engine::with_storage_write(|storage| {
        storage.group_aggregate_columns_for_table(
            table.oid(),
            &group_column_indexes,
            &aggregate_requests,
        )
    })
    .map_err(|message| EngineError { message })?
    else {
        return Ok(None);
    };

    let mut rows = Vec::with_capacity(group_rows.len());
    for (group_values, aggregate_values) in group_rows {
        let mut row = Vec::with_capacity(target_plan.len());
        for target in &target_plan {
            match target {
                SimpleGroupedTarget::GroupColumn(group_idx) => {
                    row.push(group_values[*group_idx].clone());
                }
                SimpleGroupedTarget::Aggregate(aggregate_idx) => {
                    row.push(aggregate_values[*aggregate_idx].clone());
                }
            }
        }
        rows.push(row);
    }

    let mut result = QueryResult {
        columns: columns.to_vec(),
        rows,
        command_tag: "SELECT".to_string(),
        rows_affected: 0,
    };
    if let Some(query) = query {
        apply_order_by(&mut result, query, params).await?;
        apply_offset_limit(&mut result, query, params).await?;
    }
    result.rows_affected = result.rows.len() as u64;
    Ok(Some(result))
}

async fn try_execute_simple_count_star(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    columns: &[String],
) -> Result<Option<QueryResult>, EngineError> {
    let _span = profiling::span("try_execute_simple_count_star");
    if outer_scope.is_some()
        || select.targets.len() != 1
        || select.from.len() != 1
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.window_definitions.is_empty()
        || select.quantifier.is_some()
    {
        return Ok(None);
    }

    if let Some(query) = query
        && (!query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some())
    {
        return Ok(None);
    }

    let target = &select.targets[0];
    let Expr::FunctionCall {
        name,
        args,
        distinct,
        order_by,
        within_group,
        filter,
        over,
    } = &target.expr
    else {
        return Ok(None);
    };
    if name.len() != 1
        || !name[0].eq_ignore_ascii_case("count")
        || args.len() != 1
        || !matches!(args[0], Expr::Wildcard)
        || *distinct
        || !order_by.is_empty()
        || !within_group.is_empty()
        || filter.is_some()
        || over.is_some()
    {
        return Ok(None);
    }

    let TableExpression::Relation(rel) = &select.from[0] else {
        return Ok(None);
    };

    if rel.name.len() == 1 && current_cte_binding(&rel.name[0]).is_some() {
        return Ok(None);
    }
    if lookup_virtual_relation(&rel.name).is_some() {
        return Ok(None);
    }

    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView) {
        return Ok(None);
    }

    require_relation_privilege(&table, TablePrivilege::Select)?;
    let role = security::current_role();
    let evaluation = security::rls_evaluation_for_role(&role, table.oid(), RlsCommand::Select);
    if evaluation.enabled && !evaluation.bypass {
        return Ok(None);
    }

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let column_indexes = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| (column.name().to_string(), idx))
        .collect::<HashMap<_, _>>();
    let table_columns = column_indexes.keys().cloned().collect::<HashSet<_>>();
    let relation_predicates = select
        .where_clause
        .as_ref()
        .map_or_else(Vec::new, decompose_and_conjuncts);

    let mut scan_predicates = Vec::with_capacity(relation_predicates.len());
    for predicate in &relation_predicates {
        let Some(scan_predicate) = extract_relation_scan_predicate(
            predicate,
            &qualifiers,
            &table_columns,
            &column_indexes,
            params,
        )
        .await?
        else {
            return Ok(None);
        };
        scan_predicates.push(scan_predicate);
    }

    let index_offsets = if relation_predicates.is_empty() {
        None
    } else {
        relation_index_offsets_for_predicates(
            &table,
            &qualifiers,
            &relation_predicates,
            params,
            outer_scope,
        )
        .await?
    };

    let count = crate::tcop::engine::with_storage_write(|storage| {
        storage.count_rows_for_table(table.oid(), index_offsets.as_deref(), &scan_predicates)
    })
    .map_err(|message| EngineError { message })?;

    Ok(Some(QueryResult {
        columns: columns.to_vec(),
        rows: vec![vec![ScalarValue::Int(count as i64)]],
        command_tag: "SELECT".to_string(),
        rows_affected: 1,
    }))
}

async fn execute_select_internal(
    select: &SelectStatement,
    query: Option<&Query>,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let _span = profiling::span("execute_select_internal");
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
    if let Some(result) =
        try_execute_simple_column_aggregates(select, query, outer_scope, &columns).await?
    {
        return Ok(result);
    }
    if let Some(result) =
        try_execute_simple_grouped_column_aggregates(select, query, params, outer_scope, &columns)
            .await?
    {
        return Ok(result);
    }
    if let Some(result) =
        try_execute_simple_count_star(select, query, params, outer_scope, &columns).await?
    {
        return Ok(result);
    }
    let mut row_collector = if let Some(query) = query {
        Some(QueryRowCollector::new(query, params).await?)
    } else {
        None
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
        if select.from.is_empty() {
            (
                vec![outer_scope.cloned().unwrap_or_default()],
                select.where_clause.clone(),
            )
        } else if select.from.len() == 1 {
            match &select.from[0] {
                TableExpression::Relation(rel) => {
                    let relation_predicates = select
                        .where_clause
                        .as_ref()
                        .map_or_else(Vec::new, decompose_and_conjuncts);
                    let projected_columns =
                        next_scan_projection_hint().and_then(|hint| hint.projected_columns);
                    if can_use_simple_columnar_projection(
                        select,
                        has_aggregate,
                        has_window,
                        outer_scope,
                    ) && let Some(result) = try_execute_simple_columnar_select(
                        select,
                        query,
                        params,
                        rel,
                        &relation_predicates,
                        &columns,
                        outer_scope,
                    )
                    .await?
                    {
                        profiling::record_duration("select_path_simple_columnar", 1);
                        return Ok(result);
                    }
                    if can_use_columnar_aggregation(select, has_aggregate, has_window, outer_scope)
                        && let Some(result) = try_execute_columnar_aggregation(
                            select,
                            query,
                            params,
                            rel,
                            &relation_predicates,
                            &columns,
                            outer_scope,
                        )
                        .await?
                    {
                        profiling::record_duration("select_path_columnar_aggregation", 1);
                        return Ok(result);
                    }
                    if can_use_columnar_windows(select, has_aggregate, has_window, outer_scope)
                        && let Some(result) = try_execute_columnar_windows(
                            select,
                            query,
                            params,
                            rel,
                            &relation_predicates,
                            &columns,
                            outer_scope,
                        )
                        .await?
                    {
                        profiling::record_duration("select_path_columnar_windows", 1);
                        return Ok(result);
                    }
                    let (table_eval, pushed_predicates) = evaluate_relation_with_predicates(
                        rel,
                        params,
                        outer_scope,
                        &relation_predicates,
                        projected_columns,
                    )
                    .await?;
                    let mut applied = vec![false; relation_predicates.len()];
                    for idx in pushed_predicates {
                        if let Some(flag) = applied.get_mut(idx) {
                            *flag = true;
                        }
                    }
                    let remaining_predicate =
                        remaining_predicate_from_applied(&relation_predicates, &applied);
                    (table_eval.rows, remaining_predicate)
                }
                TableExpression::Function(function) => {
                    if let Some(table_eval) = evaluate_table_function_with_predicate(
                        function,
                        params,
                        outer_scope,
                        select.where_clause.as_ref(),
                    )
                    .await?
                    {
                        (table_eval.rows, select.where_clause.clone())
                    } else {
                        (
                            evaluate_from_clause(&select.from, params, outer_scope).await?,
                            select.where_clause.clone(),
                        )
                    }
                }
                _ => (
                    evaluate_from_clause(&select.from, params, outer_scope).await?,
                    select.where_clause.clone(),
                ),
            }
        } else {
            (
                evaluate_from_clause(&select.from, params, outer_scope).await?,
                select.where_clause.clone(),
            )
        }
    };

    if let Some(outer) = outer_scope
        && !select.from.is_empty()
    {
        for scope in &mut source_rows {
            scope.inherit_outer(outer);
        }
    }

    let compiled_remaining_predicate = remaining_predicate
        .as_ref()
        .map(try_compile_expr)
        .transpose()?
        .flatten();
    let compiled_select_targets = compile_select_targets(&select.targets)?;

    let supports_direct_streaming =
        row_collector.is_some() && !has_aggregate && !has_window && select.quantifier.is_none();
    let emit_directly_to_collector = row_collector.is_some() && select.quantifier.is_none();
    if supports_direct_streaming {
        for scope in source_rows {
            if let Some(predicate) = &remaining_predicate
                && !truthy(
                    &eval_expr_maybe_compiled(
                        predicate,
                        compiled_remaining_predicate.as_ref(),
                        &scope,
                        params,
                    )
                    .await?,
                )
            {
                continue;
            }
            let row = project_select_row_compiled(
                &select.targets,
                &compiled_select_targets,
                &scope,
                params,
                wildcard_columns.as_deref(),
            )
            .await?;
            if !row_collector
                .as_mut()
                .ok_or_else(|| EngineError {
                    message: "streaming collector must be initialized".to_string(),
                })?
                .push_row(&columns, row, params)
                .await?
            {
                break;
            }
        }

        let rows = row_collector
            .take()
            .ok_or_else(|| EngineError {
                message: "streaming collector must be present".to_string(),
            })?
            .finish();
        return Ok(QueryResult {
            columns,
            rows_affected: rows.len() as u64,
            rows,
            command_tag: "SELECT".to_string(),
        });
    }

    let filtered_rows = if let Some(predicate) = &remaining_predicate {
        let mut rows = Vec::with_capacity(source_rows.len());
        for scope in source_rows {
            if !truthy(
                &eval_expr_maybe_compiled(
                    predicate,
                    compiled_remaining_predicate.as_ref(),
                    &scope,
                    params,
                )
                .await?,
            ) {
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
                if emit_directly_to_collector {
                    if !row_collector
                        .as_mut()
                        .ok_or_else(|| EngineError {
                            message: "collector must be present when emitting directly".to_string(),
                        })?
                        .push_row(&columns, row, params)
                        .await?
                    {
                        break;
                    }
                } else {
                    rows.push(row);
                }
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
            if emit_directly_to_collector {
                if !row_collector
                    .as_mut()
                    .ok_or_else(|| EngineError {
                        message: "collector must be present when emitting directly".to_string(),
                    })?
                    .push_row(&columns, row, params)
                    .await?
                {
                    break;
                }
            } else {
                rows.push(row);
            }
        }
    } else {
        for scope in filtered_rows {
            let row = project_select_row_compiled(
                &select.targets,
                &compiled_select_targets,
                &scope,
                params,
                wildcard_columns.as_deref(),
            )
            .await?;
            if emit_directly_to_collector {
                if !row_collector
                    .as_mut()
                    .ok_or_else(|| EngineError {
                        message: "collector must be present when emitting directly".to_string(),
                    })?
                    .push_row(&columns, row, params)
                    .await?
                {
                    break;
                }
            } else {
                rows.push(row);
            }
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

    if let Some(mut collector) = row_collector {
        for row in rows {
            if !collector.push_row(&columns, row, params).await? {
                break;
            }
        }
        let rows = collector.finish();
        return Ok(QueryResult {
            columns,
            rows_affected: rows.len() as u64,
            rows,
            command_tag: "SELECT".to_string(),
        });
    }

    Ok(QueryResult {
        columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}
