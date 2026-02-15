use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::catalog::search_path::SearchPath;
use crate::catalog::system_catalogs::lookup_virtual_relation;
use crate::catalog::{TypeSignature, with_catalog_read};
use crate::executor::exec_expr::{EvalScope, eval_expr};
use crate::executor::exec_main::{scope_for_table_row, scope_for_table_row_with_qualifiers};
use crate::parser::ast::{
    BinaryOp, Expr, GroupByExpr, JoinCondition, OrderByExpr, Query, QueryExpr, SelectItem,
    SelectStatement, SetOperator, TableExpression, TableFunctionRef, UnaryOp,
};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

const PG_BOOL_OID: u32 = 16;
const PG_INT8_OID: u32 = 20;
const PG_TEXT_OID: u32 = 25;
const PG_FLOAT8_OID: u32 = 701;
const PG_NUMERIC_OID: u32 = 1700;
const PG_DATE_OID: u32 = 1082;
const PG_TIMESTAMP_OID: u32 = 1114;

pub(crate) fn type_signature_to_oid(ty: TypeSignature) -> u32 {
    match ty {
        TypeSignature::Bool => PG_BOOL_OID,
        TypeSignature::Int8 => PG_INT8_OID,
        TypeSignature::Float8 => PG_FLOAT8_OID,
        TypeSignature::Numeric => PG_NUMERIC_OID,
        TypeSignature::Text => PG_TEXT_OID,
        TypeSignature::Date => PG_DATE_OID,
        TypeSignature::Timestamp => PG_TIMESTAMP_OID,
    }
}

pub fn type_oid_size(type_oid: u32) -> i16 {
    match type_oid {
        PG_BOOL_OID => 1,
        PG_INT8_OID => 8,
        PG_FLOAT8_OID => 8,
        PG_DATE_OID => 4,
        PG_TIMESTAMP_OID => 8,
        _ => -1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedOutputColumn {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
}

#[derive(Debug, Clone, Default)]
struct TypeScope {
    unqualified: HashMap<String, u32>,
    qualified: HashMap<String, u32>,
    ambiguous: HashSet<String>,
}

impl TypeScope {
    fn insert_unqualified(&mut self, key: &str, type_oid: u32) {
        let key = key.to_ascii_lowercase();
        if self.ambiguous.contains(&key) {
            return;
        }
        #[allow(clippy::map_entry)]
        if self.unqualified.contains_key(&key) {
            self.unqualified.remove(&key);
            self.ambiguous.insert(key);
        } else {
            self.unqualified.insert(key, type_oid);
        }
    }

    fn insert_qualified(&mut self, parts: &[String], type_oid: u32) {
        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.insert(key, type_oid);
    }

    fn lookup_identifier(&self, parts: &[String]) -> Option<u32> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            let key = parts[0].to_ascii_lowercase();
            if self.ambiguous.contains(&key) {
                return None;
            }
            return self.unqualified.get(&key).copied();
        }

        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.get(&key).copied()
    }
}

#[derive(Debug, Clone)]
struct ExpandedFromTypeColumn {
    label: String,
    lookup_parts: Vec<String>,
    type_oid: u32,
}

fn cast_type_name_to_oid(type_name: &str) -> u32 {
    match type_name.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => PG_BOOL_OID,
        "int8" | "int4" | "int2" | "bigint" | "integer" | "smallint" => PG_INT8_OID,
        "float8" | "float4" | "numeric" | "decimal" | "real" => PG_FLOAT8_OID,
        "date" => PG_DATE_OID,
        "timestamp" | "timestamptz" => PG_TIMESTAMP_OID,
        _ => PG_TEXT_OID,
    }
}

fn infer_numeric_result_oid(left: u32, right: u32) -> u32 {
    if left == PG_FLOAT8_OID || right == PG_FLOAT8_OID {
        PG_FLOAT8_OID
    } else {
        PG_INT8_OID
    }
}

fn infer_common_type_oid(
    exprs: &[Expr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    let mut oid = PG_TEXT_OID;
    for expr in exprs {
        let next = infer_expr_type_oid(expr, scope, ctes);
        if next == PG_TEXT_OID {
            continue;
        }
        if oid == PG_TEXT_OID {
            oid = next;
            continue;
        }
        if oid == next {
            continue;
        }
        if (oid == PG_INT8_OID || oid == PG_FLOAT8_OID)
            && (next == PG_INT8_OID || next == PG_FLOAT8_OID)
        {
            oid = infer_numeric_result_oid(oid, next);
            continue;
        }
        oid = PG_TEXT_OID;
    }
    oid
}

fn infer_function_return_oid(
    name: &[String],
    args: &[Expr],
    within_group: &[OrderByExpr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    let fn_name = name
        .last()
        .map(|part| part.to_ascii_lowercase())
        .unwrap_or_default();
    match fn_name.as_str() {
        "count" | "char_length" | "length" | "nextval" | "currval" | "setval" | "strpos"
        | "position" | "ascii" | "pg_backend_pid" | "width_bucket" | "scale" | "factorial"
        | "num_nulls" | "num_nonnulls" => PG_INT8_OID,
        "extract" | "date_part" => PG_INT8_OID,
        "avg" | "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop"
        | "corr" | "covar_pop" | "covar_samp" | "regr_slope" | "regr_intercept" | "regr_r2"
        | "regr_avgx" | "regr_avgy" | "regr_sxx" | "regr_sxy" | "regr_syy" | "percentile_cont" => {
            PG_FLOAT8_OID
        }
        "regr_count" => PG_INT8_OID,
        "bool_and"
        | "bool_or"
        | "every"
        | "has_table_privilege"
        | "has_column_privilege"
        | "has_schema_privilege"
        | "pg_table_is_visible"
        | "pg_type_is_visible"
        | "isfinite" => PG_BOOL_OID,
        "abs" | "ceil" | "ceiling" | "floor" | "round" | "trunc" | "sign" | "mod" => args
            .first()
            .map(|expr| infer_expr_type_oid(expr, scope, ctes))
            .unwrap_or(PG_FLOAT8_OID),
        "power" | "pow" | "sqrt" | "cbrt" | "exp" | "ln" | "log" | "sin" | "cos" | "tan"
        | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "pi" | "random"
        | "to_number" => PG_FLOAT8_OID,
        "div" | "gcd" | "lcm" | "ntile" | "row_number" | "rank" | "dense_rank" => PG_INT8_OID,
        "percent_rank" | "cume_dist" => PG_FLOAT8_OID,
        "sum" => args
            .first()
            .map(|expr| {
                let oid = infer_expr_type_oid(expr, scope, ctes);
                if oid == PG_FLOAT8_OID {
                    PG_FLOAT8_OID
                } else {
                    PG_INT8_OID
                }
            })
            .unwrap_or(PG_INT8_OID),
        "percentile_disc" | "mode" => within_group
            .first()
            .map(|entry| infer_expr_type_oid(&entry.expr, scope, ctes))
            .unwrap_or(PG_TEXT_OID),
        "min" | "max" | "nullif" => args
            .first()
            .map(|expr| infer_expr_type_oid(expr, scope, ctes))
            .unwrap_or(PG_TEXT_OID),
        "coalesce" | "greatest" | "least" => infer_common_type_oid(args, scope, ctes),
        "date" | "current_date" | "to_date" => PG_DATE_OID,
        "timestamp" | "current_timestamp" | "now" | "date_trunc" | "to_timestamp"
        | "clock_timestamp" => PG_TIMESTAMP_OID,
        "date_add" | "date_sub" => PG_DATE_OID,
        "jsonb_path_exists" | "jsonb_path_match" | "jsonb_exists" | "jsonb_exists_any"
        | "jsonb_exists_all" => PG_BOOL_OID,
        "connect" if name.len() == 2 && name[0].eq_ignore_ascii_case("ws") => PG_INT8_OID,
        "send" | "close" if name.len() == 2 && name[0].eq_ignore_ascii_case("ws") => PG_BOOL_OID,
        _ => PG_TEXT_OID,
    }
}

fn infer_expr_type_oid(
    expr: &Expr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> u32 {
    match expr {
        Expr::Identifier(parts) => scope.lookup_identifier(parts).unwrap_or(PG_TEXT_OID),
        Expr::String(_) => PG_TEXT_OID,
        Expr::Integer(_) => PG_INT8_OID,
        Expr::Float(_) => PG_FLOAT8_OID,
        Expr::Boolean(_) => PG_BOOL_OID,
        Expr::Null => PG_TEXT_OID,
        Expr::Default => PG_TEXT_OID,
        Expr::Parameter(_) => PG_TEXT_OID,
        Expr::FunctionCall {
            name,
            args,
            within_group,
            ..
        } => infer_function_return_oid(name, args, within_group, scope, ctes),
        Expr::Cast { type_name, .. } => cast_type_name_to_oid(type_name),
        Expr::Wildcard | Expr::QualifiedWildcard(_) => PG_TEXT_OID,
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => PG_BOOL_OID,
            UnaryOp::Plus | UnaryOp::Minus => infer_expr_type_oid(expr, scope, ctes),
        },
        Expr::Binary { left, op, right } => {
            let left_oid = infer_expr_type_oid(left, scope, ctes);
            let right_oid = infer_expr_type_oid(right, scope, ctes);
            match op {
                BinaryOp::Or
                | BinaryOp::And
                | BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::Gt
                | BinaryOp::Gte
                | BinaryOp::JsonContains
                | BinaryOp::JsonContainedBy
                | BinaryOp::JsonPathExists
                | BinaryOp::JsonPathMatch
                | BinaryOp::JsonHasKey
                | BinaryOp::JsonHasAny
                | BinaryOp::JsonHasAll
                | BinaryOp::ArrayContains
                | BinaryOp::ArrayContainedBy
                | BinaryOp::ArrayOverlap => PG_BOOL_OID,
                BinaryOp::JsonGet
                | BinaryOp::JsonGetText
                | BinaryOp::JsonPath
                | BinaryOp::JsonPathText
                | BinaryOp::JsonConcat
                | BinaryOp::JsonDelete
                | BinaryOp::JsonDeletePath => PG_TEXT_OID,
                BinaryOp::ArrayConcat => left_oid,
                BinaryOp::Add => {
                    if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && right_oid == PG_INT8_OID
                    {
                        left_oid
                    } else if (right_oid == PG_DATE_OID || right_oid == PG_TIMESTAMP_OID)
                        && left_oid == PG_INT8_OID
                    {
                        right_oid
                    } else {
                        infer_numeric_result_oid(left_oid, right_oid)
                    }
                }
                BinaryOp::Sub => {
                    if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && (right_oid == PG_DATE_OID || right_oid == PG_TIMESTAMP_OID)
                    {
                        PG_INT8_OID
                    } else if (left_oid == PG_DATE_OID || left_oid == PG_TIMESTAMP_OID)
                        && right_oid == PG_INT8_OID
                    {
                        left_oid
                    } else if left_oid == PG_TEXT_OID && right_oid == PG_TEXT_OID {
                        PG_TEXT_OID
                    } else {
                        infer_numeric_result_oid(left_oid, right_oid)
                    }
                }
                BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    infer_numeric_result_oid(left_oid, right_oid)
                }
            }
        }
        Expr::AnyAll { .. } => PG_BOOL_OID,
        Expr::Exists(_)
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::BooleanTest { .. } => PG_BOOL_OID,
        Expr::CaseSimple {
            when_then,
            else_expr,
            ..
        }
        | Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            let mut result_exprs = when_then
                .iter()
                .map(|(_, then_expr)| then_expr.clone())
                .collect::<Vec<_>>();
            if let Some(else_expr) = else_expr {
                result_exprs.push((**else_expr).clone());
            }
            infer_common_type_oid(&result_exprs, scope, ctes)
        }
        Expr::ScalarSubquery(query) => {
            let mut nested = ctes.clone();
            derive_query_output_columns_with_ctes(query, &mut nested)
                .ok()
                .and_then(|cols| cols.first().map(|col| col.type_oid))
                .unwrap_or(PG_TEXT_OID)
        }
        Expr::ArrayConstructor(_) | Expr::ArraySubquery(_) => PG_TEXT_OID,
        Expr::ArraySubscript { expr, .. } => {
            // Return the element type - for simplicity, return text type for now
            // A more complete implementation would infer element type from array type
            infer_expr_type_oid(expr, scope, ctes);
            PG_TEXT_OID
        }
        Expr::ArraySlice { expr, .. } => {
            // Return the same type as the array
            infer_expr_type_oid(expr, scope, ctes)
        }
        Expr::TypedLiteral { type_name, .. } => cast_type_name_to_oid(type_name),
        Expr::RowConstructor(_) => 2249, // record type OID
        Expr::MultiColumnSubqueryRef { .. } => PG_TEXT_OID, // approximate
    }
}

fn infer_select_target_name(target: &SelectItem) -> Result<String, EngineError> {
    if let Some(alias) = &target.alias {
        return Ok(alias.clone());
    }
    let name = match &target.expr {
        Expr::Identifier(parts) => parts
            .last()
            .cloned()
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::FunctionCall { name, .. } => name
            .last()
            .cloned()
            .unwrap_or_else(|| "?column?".to_string()),
        Expr::Wildcard => {
            return Err(EngineError {
                message: "wildcard target requires FROM support".to_string(),
            });
        }
        _ => "?column?".to_string(),
    };
    Ok(name)
}

pub(crate) fn derive_query_output_columns(
    query: &Query,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let mut ctes = HashMap::new();
    derive_query_output_columns_with_ctes(query, &mut ctes)
}

pub(crate) fn derive_query_output_columns_with_ctes(
    query: &Query,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let mut local_ctes = ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            let cte_name = cte.name.to_ascii_lowercase();
            let cols = if with.recursive && query_references_relation(&cte.query, &cte_name) {
                derive_recursive_cte_output_columns(cte, &local_ctes)?
            } else {
                derive_query_output_columns_with_ctes(&cte.query, &mut local_ctes)?
            };
            local_ctes.insert(cte_name, cols);
        }
    }
    derive_query_expr_output_columns(&query.body, &local_ctes)
}

fn derive_recursive_cte_output_columns(
    cte: &crate::parser::ast::CommonTableExpr,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier: _,
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

    let left_cols = derive_query_expr_output_columns(left, ctes)?;
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_output_columns(right, &recursive_ctes)?;
    if left_cols.len() != right_cols.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }
    Ok(left_cols)
}

fn derive_query_expr_output_columns(
    expr: &QueryExpr,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    match expr {
        QueryExpr::Select(select) => derive_select_output_columns(select, ctes),
        QueryExpr::SetOperation { left, right, .. } => {
            let left_cols = derive_query_expr_output_columns(left, ctes)?;
            let right_cols = derive_query_expr_output_columns(right, ctes)?;
            if left_cols.len() != right_cols.len() {
                return Err(EngineError {
                    message: "set-operation inputs must have matching column counts".to_string(),
                });
            }
            Ok(left_cols)
        }
        QueryExpr::Nested(query) => {
            let mut nested = ctes.clone();
            derive_query_output_columns_with_ctes(query, &mut nested)
        }
        QueryExpr::Values(rows) => {
            // VALUES query - derive column names and types from first row
            let ncols = rows.first().map(|r| r.len()).unwrap_or(0);
            Ok((1..=ncols)
                .map(|i| PlannedOutputColumn {
                    name: format!("column{i}"),
                    type_oid: PG_TEXT_OID, // Default to text, actual type determined at execution
                })
                .collect())
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => Err(EngineError {
            message: "data-modifying statements in WITH are not yet fully supported".to_string(),
        }),
    }
}

fn derive_select_output_columns(
    select: &SelectStatement,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<PlannedOutputColumn>, EngineError> {
    let from_columns = if select.from.is_empty() {
        Vec::new()
    } else {
        expand_from_columns_typed(&select.from, ctes).unwrap_or_default()
    };
    let wildcard_columns = if select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard))
    {
        Some(expand_from_columns_typed(&select.from, ctes)?)
    } else {
        None
    };

    let mut type_scope = TypeScope::default();
    for col in from_columns {
        type_scope.insert_unqualified(&col.label, col.type_oid);
        type_scope.insert_qualified(&col.lookup_parts, col.type_oid);
    }

    let mut columns = Vec::new();
    for target in &select.targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            columns.extend(expanded.iter().map(|col| PlannedOutputColumn {
                name: col.label.clone(),
                type_oid: col.type_oid,
            }));
            continue;
        }

        columns.push(PlannedOutputColumn {
            name: infer_select_target_name(target)?,
            type_oid: infer_expr_type_oid(&target.expr, &type_scope, ctes),
        });
    }
    Ok(columns)
}

pub(crate) fn derive_query_columns(query: &Query) -> Result<Vec<String>, EngineError> {
    let mut ctes = HashMap::new();
    derive_query_columns_with_ctes(query, &mut ctes)
}

fn derive_query_columns_with_ctes(
    query: &Query,
    ctes: &mut HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let mut local_ctes = ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            let cte_name = cte.name.to_ascii_lowercase();
            let cols = if with.recursive && query_references_relation(&cte.query, &cte_name) {
                derive_recursive_cte_columns(cte, &local_ctes)?
            } else {
                derive_query_columns_with_ctes(&cte.query, &mut local_ctes)?
            };
            local_ctes.insert(cte_name, cols);
        }
    }
    derive_query_expr_columns(&query.body, &local_ctes)
}

fn derive_recursive_cte_columns(
    cte: &crate::parser::ast::CommonTableExpr,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let cte_name = cte.name.to_ascii_lowercase();
    let QueryExpr::SetOperation {
        left,
        op,
        quantifier: _,
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

    let left_cols = derive_query_expr_columns(left, ctes)?;
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_columns(right, &recursive_ctes)?;
    if left_cols.len() != right_cols.len() {
        return Err(EngineError {
            message: "set-operation inputs must have matching column counts".to_string(),
        });
    }
    Ok(left_cols)
}

pub(crate) fn query_references_relation(query: &Query, relation_name: &str) -> bool {
    if query_expr_references_relation(&query.body, relation_name) {
        return true;
    }
    if query
        .order_by
        .iter()
        .any(|order| expr_references_relation(&order.expr, relation_name))
    {
        return true;
    }
    if query
        .limit
        .as_ref()
        .is_some_and(|expr| expr_references_relation(expr, relation_name))
    {
        return true;
    }
    if query
        .offset
        .as_ref()
        .is_some_and(|expr| expr_references_relation(expr, relation_name))
    {
        return true;
    }
    query.with.as_ref().is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| query_references_relation(&cte.query, relation_name))
    })
}

fn query_expr_references_relation(expr: &QueryExpr, relation_name: &str) -> bool {
    match expr {
        QueryExpr::Select(select) => {
            select
                .from
                .iter()
                .any(|from| table_expression_references_relation(from, relation_name))
                || select
                    .targets
                    .iter()
                    .any(|target| expr_references_relation(&target.expr, relation_name))
                || select
                    .where_clause
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
                || select.group_by.iter().any(|group_expr| match group_expr {
                    GroupByExpr::Expr(expr) => expr_references_relation(expr, relation_name),
                    GroupByExpr::GroupingSets(sets) => sets.iter().any(|set| {
                        set.iter()
                            .any(|expr| expr_references_relation(expr, relation_name))
                    }),
                    GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => exprs
                        .iter()
                        .any(|expr| expr_references_relation(expr, relation_name)),
                })
                || select
                    .having
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        QueryExpr::SetOperation { left, right, .. } => {
            query_expr_references_relation(left, relation_name)
                || query_expr_references_relation(right, relation_name)
        }
        QueryExpr::Nested(query) => query_references_relation(query, relation_name),
        QueryExpr::Values(rows) => {
            // Check if any value expression references the relation
            rows.iter().any(|row| {
                row.iter()
                    .any(|expr| expr_references_relation(expr, relation_name))
            })
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
            // DML statements in CTEs not yet fully supported
            false
        }
    }
}

fn table_expression_references_relation(table: &TableExpression, relation_name: &str) -> bool {
    match table {
        TableExpression::Relation(rel) => {
            rel.name.len() == 1 && rel.name[0].eq_ignore_ascii_case(relation_name)
        }
        TableExpression::Function(function) => function
            .args
            .iter()
            .any(|arg| expr_references_relation(arg, relation_name)),
        TableExpression::Subquery(sub) => query_references_relation(&sub.query, relation_name),
        TableExpression::Join(join) => {
            table_expression_references_relation(&join.left, relation_name)
                || table_expression_references_relation(&join.right, relation_name)
                || join
                    .condition
                    .as_ref()
                    .is_some_and(|condition| match condition {
                        JoinCondition::On(expr) => expr_references_relation(expr, relation_name),
                        JoinCondition::Using(_) => false,
                    })
        }
    }
}

fn expr_references_relation(expr: &Expr, relation_name: &str) -> bool {
    match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            args.iter()
                .any(|arg| expr_references_relation(arg, relation_name))
                || order_by
                    .iter()
                    .any(|entry| expr_references_relation(&entry.expr, relation_name))
                || within_group
                    .iter()
                    .any(|entry| expr_references_relation(&entry.expr, relation_name))
                || filter
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::Cast { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::Unary { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::Binary { left, right, .. } => {
            expr_references_relation(left, relation_name)
                || expr_references_relation(right, relation_name)
        }
        Expr::Exists(query) | Expr::ScalarSubquery(query) => {
            query_references_relation(query, relation_name)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_relation(expr, relation_name)
                || list
                    .iter()
                    .any(|item| expr_references_relation(item, relation_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_references_relation(expr, relation_name)
                || query_references_relation(subquery, relation_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_relation(expr, relation_name)
                || expr_references_relation(low, relation_name)
                || expr_references_relation(high, relation_name)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_references_relation(expr, relation_name)
                || expr_references_relation(pattern, relation_name)
        }
        Expr::IsNull { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::BooleanTest { expr, .. } => expr_references_relation(expr, relation_name),
        Expr::IsDistinctFrom { left, right, .. } => {
            expr_references_relation(left, relation_name)
                || expr_references_relation(right, relation_name)
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            expr_references_relation(operand, relation_name)
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_references_relation(when_expr, relation_name)
                        || expr_references_relation(then_expr, relation_name)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            when_then.iter().any(|(when_expr, then_expr)| {
                expr_references_relation(when_expr, relation_name)
                    || expr_references_relation(then_expr, relation_name)
            }) || else_expr
                .as_ref()
                .is_some_and(|expr| expr_references_relation(expr, relation_name))
        }
        Expr::ArrayConstructor(items) => items
            .iter()
            .any(|item| expr_references_relation(item, relation_name)),
        Expr::ArraySubquery(query) => query_references_relation(query, relation_name),
        _ => false,
    }
}

pub(crate) fn validate_recursive_cte_terms(
    cte_display_name: &str,
    cte_name: &str,
    left: &QueryExpr,
    right: &QueryExpr,
) -> Result<(), EngineError> {
    if query_expr_references_relation(left, cte_name) {
        return Err(EngineError {
            message: format!(
                "recursive query \"{cte_display_name}\" has self-reference in non-recursive term"
            ),
        });
    }
    if !query_expr_references_relation(right, cte_name) {
        return Err(EngineError {
            message: format!(
                "recursive query \"{cte_display_name}\" must reference itself in recursive term"
            ),
        });
    }
    Ok(())
}

fn derive_query_expr_columns(
    expr: &QueryExpr,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    match expr {
        QueryExpr::Select(select) => derive_select_columns(select, ctes),
        QueryExpr::SetOperation { left, right, .. } => {
            let left_cols = derive_query_expr_columns(left, ctes)?;
            let right_cols = derive_query_expr_columns(right, ctes)?;
            if left_cols.len() != right_cols.len() {
                return Err(EngineError {
                    message: "set-operation inputs must have matching column counts".to_string(),
                });
            }
            Ok(left_cols)
        }
        QueryExpr::Nested(query) => {
            let mut nested = ctes.clone();
            derive_query_columns_with_ctes(query, &mut nested)
        }
        QueryExpr::Values(rows) => {
            // VALUES query - derive column names
            let ncols = rows.first().map(|r| r.len()).unwrap_or(0);
            Ok((1..=ncols).map(|i| format!("column{i}")).collect())
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => Err(EngineError {
            message: "data-modifying statements in WITH are not yet fully supported".to_string(),
        }),
    }
}

pub(crate) fn derive_select_columns(
    select: &SelectStatement,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let wildcard_columns = if select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard))
    {
        Some(expand_from_columns(&select.from, ctes)?)
    } else {
        None
    };
    let mut columns = Vec::with_capacity(select.targets.len());
    for target in &select.targets {
        if matches!(target.expr, Expr::Wildcard) {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                columns.push(col.label.clone());
            }
            continue;
        }

        if let Some(alias) = &target.alias {
            columns.push(alias.clone());
            continue;
        }
        let name = match &target.expr {
            Expr::Identifier(parts) => parts
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            Expr::FunctionCall { name, .. } => name
                .last()
                .cloned()
                .unwrap_or_else(|| "?column?".to_string()),
            Expr::Wildcard => {
                return Err(EngineError {
                    message: "wildcard target requires FROM support".to_string(),
                });
            }
            _ => "?column?".to_string(),
        };
        columns.push(name);
    }
    Ok(columns)
}

pub(crate) fn derive_dml_returning_columns(
    table_name: &[String],
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<String>, EngineError> {
    if returning.is_empty() {
        return Ok(Vec::new());
    }
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    derive_returning_columns_from_table(&table, returning)
}

pub(crate) fn derive_dml_returning_column_type_oids(
    table_name: &[String],
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<u32>, EngineError> {
    if returning.is_empty() {
        return Ok(Vec::new());
    }
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    derive_returning_column_type_oids_from_table(&table, returning)
}

fn derive_returning_column_type_oids_from_table(
    table: &crate::catalog::Table,
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<u32>, EngineError> {
    let mut scope = TypeScope::default();
    for column in table.columns() {
        let oid = type_signature_to_oid(column.type_signature());
        scope.insert_unqualified(column.name(), oid);
        scope.insert_qualified(&[table.name().to_string(), column.name().to_string()], oid);
        scope.insert_qualified(&[table.qualified_name(), column.name().to_string()], oid);
    }

    let ctes = HashMap::new();
    let mut out = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            out.extend(
                table
                    .columns()
                    .iter()
                    .map(|column| type_signature_to_oid(column.type_signature())),
            );
            continue;
        }
        out.push(infer_expr_type_oid(&target.expr, &scope, &ctes));
    }
    Ok(out)
}

pub(crate) fn derive_returning_columns_from_table(
    table: &crate::catalog::Table,
    returning: &[crate::parser::ast::SelectItem],
) -> Result<Vec<String>, EngineError> {
    let mut columns = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            columns.extend(
                table
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string()),
            );
            continue;
        }
        if let Some(alias) = &target.alias {
            columns.push(alias.clone());
            continue;
        }
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
        columns.push(name);
    }
    Ok(columns)
}

pub(crate) async fn project_returning_row(
    returning: &[crate::parser::ast::SelectItem],
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let scope = scope_for_table_row(table, row);
    project_returning_row_from_scope(returning, row, &scope, params).await
}

pub(crate) async fn project_returning_row_with_qualifiers(
    returning: &[crate::parser::ast::SelectItem],
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    qualifiers: &[String],
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let scope = scope_for_table_row_with_qualifiers(table, row, qualifiers);
    project_returning_row_from_scope(returning, row, &scope, params).await
}

async fn project_returning_row_from_scope(
    returning: &[crate::parser::ast::SelectItem],
    row: &[ScalarValue],
    scope: &EvalScope,
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let mut out = Vec::new();
    for target in returning {
        if matches!(target.expr, Expr::Wildcard) {
            out.extend(row.iter().cloned());
            continue;
        }
        out.push(eval_expr(&target.expr, scope, params).await?);
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub(crate) struct ExpandedFromColumn {
    pub(crate) label: String,
    pub(crate) lookup_parts: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CteBinding {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<ScalarValue>>,
}

thread_local! {
    static ACTIVE_CTE_STACK: RefCell<Vec<HashMap<String, CteBinding>>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn current_cte_binding(name: &str) -> Option<CteBinding> {
    ACTIVE_CTE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|ctes| ctes.get(&name.to_ascii_lowercase()).cloned())
    })
}

#[allow(dead_code)]
fn with_cte_context<T>(ctes: HashMap<String, CteBinding>, f: impl FnOnce() -> T) -> T {
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().push(ctes);
        let out = f();
        stack.borrow_mut().pop();
        out
    })
}

pub(crate) async fn with_cte_context_async<T, F, Fut>(ctes: HashMap<String, CteBinding>, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().push(ctes);
    });
    let out = f().await;
    ACTIVE_CTE_STACK.with(|stack| {
        stack.borrow_mut().pop();
    });
    out
}

pub(crate) fn active_cte_context() -> HashMap<String, CteBinding> {
    ACTIVE_CTE_STACK.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}

pub(crate) fn expand_from_columns(
    from: &[TableExpression],
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<ExpandedFromColumn>, EngineError> {
    if from.is_empty() {
        return Err(EngineError {
            message: "wildcard target requires FROM support".to_string(),
        });
    }

    let mut out = Vec::new();
    for table in from {
        out.extend(expand_table_expression_columns(table, ctes)?);
    }
    Ok(out)
}

fn expand_table_expression_columns(
    table: &TableExpression,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<ExpandedFromColumn>, EngineError> {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1 {
                let key = rel.name[0].to_ascii_lowercase();
                if let Some(columns) = ctes.get(&key) {
                    let qualifier = rel
                        .alias
                        .as_ref()
                        .map(|alias| alias.to_ascii_lowercase())
                        .unwrap_or(key);
                    return Ok(columns
                        .iter()
                        .map(|column| ExpandedFromColumn {
                            label: column.to_string(),
                            lookup_parts: vec![qualifier.clone(), column.to_string()],
                        })
                        .collect());
                }
            }
            if let Some((_, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
                let qualifier = rel
                    .alias
                    .as_ref()
                    .map(|alias| alias.to_ascii_lowercase())
                    .unwrap_or(relation_name);
                return Ok(columns
                    .iter()
                    .map(|column| ExpandedFromColumn {
                        label: column.name.clone(),
                        lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                    })
                    .collect());
            }
            let table = with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&rel.name, &SearchPath::default())
                    .cloned()
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
            let qualifier = rel
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .unwrap_or_else(|| table.name().to_string());
            Ok(table
                .columns()
                .iter()
                .map(|column| ExpandedFromColumn {
                    label: column.name().to_string(),
                    lookup_parts: vec![qualifier.clone(), column.name().to_string()],
                })
                .collect())
        }
        TableExpression::Function(function) => {
            let column_names = if function.column_aliases.is_empty() {
                table_function_output_columns(function)
            } else {
                function.column_aliases.clone()
            };
            let qualifier = function
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .or_else(|| function.name.last().map(|name| name.to_ascii_lowercase()));
            Ok(column_names
                .into_iter()
                .map(|column_name| {
                    let lookup_parts = if let Some(qualifier) = &qualifier {
                        vec![qualifier.clone(), column_name.clone()]
                    } else {
                        vec![column_name.clone()]
                    };
                    ExpandedFromColumn {
                        label: column_name,
                        lookup_parts,
                    }
                })
                .collect())
        }
        TableExpression::Subquery(sub) => {
            let mut nested = ctes.clone();
            let cols = derive_query_columns_with_ctes(&sub.query, &mut nested)?;
            if let Some(alias) = &sub.alias {
                let qualifier = alias.to_ascii_lowercase();
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromColumn {
                        label: col.clone(),
                        lookup_parts: vec![qualifier.clone(), col],
                    })
                    .collect())
            } else {
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromColumn {
                        label: col.clone(),
                        lookup_parts: vec![col],
                    })
                    .collect())
            }
        }
        TableExpression::Join(join) => {
            let left_cols = expand_table_expression_columns(&join.left, ctes)?;
            let right_cols = expand_table_expression_columns(&join.right, ctes)?;
            let using_columns = if join.natural {
                left_cols
                    .iter()
                    .filter(|c| {
                        right_cols
                            .iter()
                            .any(|r| r.label.eq_ignore_ascii_case(&c.label))
                    })
                    .map(|c| c.label.clone())
                    .collect::<Vec<_>>()
            } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                cols.clone()
            } else {
                Vec::new()
            };
            let using_set: HashSet<String> = using_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect();

            let mut out = left_cols;
            for col in right_cols {
                if using_set.contains(&col.label.to_ascii_lowercase()) {
                    continue;
                }
                out.push(col);
            }
            Ok(out)
        }
    }
}

fn expand_from_columns_typed(
    from: &[TableExpression],
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<ExpandedFromTypeColumn>, EngineError> {
    if from.is_empty() {
        return Err(EngineError {
            message: "wildcard target requires FROM support".to_string(),
        });
    }

    let mut out = Vec::new();
    for table in from {
        out.extend(expand_table_expression_columns_typed(table, ctes)?);
    }
    Ok(out)
}

fn expand_table_expression_columns_typed(
    table: &TableExpression,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<Vec<ExpandedFromTypeColumn>, EngineError> {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1 {
                let key = rel.name[0].to_ascii_lowercase();
                if let Some(columns) = ctes.get(&key) {
                    let qualifier = rel
                        .alias
                        .as_ref()
                        .map(|alias| alias.to_ascii_lowercase())
                        .unwrap_or(key);
                    return Ok(columns
                        .iter()
                        .map(|column| ExpandedFromTypeColumn {
                            label: column.name.to_string(),
                            lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                            type_oid: column.type_oid,
                        })
                        .collect());
                }
            }
            if let Some((_, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
                let qualifier = rel
                    .alias
                    .as_ref()
                    .map(|alias| alias.to_ascii_lowercase())
                    .unwrap_or(relation_name);
                return Ok(columns
                    .iter()
                    .map(|column| ExpandedFromTypeColumn {
                        label: column.name.clone(),
                        lookup_parts: vec![qualifier.clone(), column.name.to_string()],
                        type_oid: column.type_oid,
                    })
                    .collect());
            }
            let table = with_catalog_read(|catalog| {
                catalog
                    .resolve_table(&rel.name, &SearchPath::default())
                    .cloned()
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
            let qualifier = rel
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .unwrap_or_else(|| table.name().to_string());
            Ok(table
                .columns()
                .iter()
                .map(|column| ExpandedFromTypeColumn {
                    label: column.name().to_string(),
                    lookup_parts: vec![qualifier.clone(), column.name().to_string()],
                    type_oid: type_signature_to_oid(column.type_signature()),
                })
                .collect())
        }
        TableExpression::Function(function) => {
            let column_names = if function.column_aliases.is_empty() {
                table_function_output_columns(function)
            } else {
                function.column_aliases.clone()
            };
            let mut column_type_oids =
                table_function_output_type_oids(function, column_names.len());
            if column_type_oids.len() < column_names.len() {
                column_type_oids.resize(column_names.len(), PG_TEXT_OID);
            }
            let qualifier = function
                .alias
                .as_ref()
                .map(|alias| alias.to_ascii_lowercase())
                .or_else(|| function.name.last().map(|name| name.to_ascii_lowercase()));
            Ok(column_names
                .into_iter()
                .enumerate()
                .map(|(idx, column_name)| {
                    let lookup_parts = if let Some(qualifier) = &qualifier {
                        vec![qualifier.clone(), column_name.clone()]
                    } else {
                        vec![column_name.clone()]
                    };
                    ExpandedFromTypeColumn {
                        label: column_name,
                        lookup_parts,
                        type_oid: *column_type_oids.get(idx).unwrap_or(&PG_TEXT_OID),
                    }
                })
                .collect())
        }
        TableExpression::Subquery(sub) => {
            let mut nested = ctes.clone();
            let cols = derive_query_output_columns_with_ctes(&sub.query, &mut nested)?;
            if let Some(alias) = &sub.alias {
                let qualifier = alias.to_ascii_lowercase();
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![qualifier.clone(), col.name],
                        type_oid: col.type_oid,
                    })
                    .collect())
            } else {
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![col.name],
                        type_oid: col.type_oid,
                    })
                    .collect())
            }
        }
        TableExpression::Join(join) => {
            let left_cols = expand_table_expression_columns_typed(&join.left, ctes)?;
            let right_cols = expand_table_expression_columns_typed(&join.right, ctes)?;
            let using_columns = if join.natural {
                left_cols
                    .iter()
                    .filter(|c| {
                        right_cols
                            .iter()
                            .any(|r| r.label.eq_ignore_ascii_case(&c.label))
                    })
                    .map(|c| c.label.clone())
                    .collect::<Vec<_>>()
            } else if let Some(JoinCondition::Using(cols)) = &join.condition {
                cols.clone()
            } else {
                Vec::new()
            };
            let using_set: HashSet<String> = using_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect();

            let mut out = left_cols;
            for col in right_cols {
                if using_set.contains(&col.label.to_ascii_lowercase()) {
                    continue;
                }
                out.push(col);
            }
            Ok(out)
        }
    }
}

fn table_function_output_columns(function: &TableFunctionRef) -> Vec<String> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();
    match fn_name.as_str() {
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" => {
            vec!["key".to_string(), "value".to_string()]
        }
        "json_object_keys" | "jsonb_object_keys" => vec!["key".to_string()],
        "generate_series" => vec!["generate_series".to_string()],
        "unnest" => vec!["unnest".to_string()],
        "regexp_matches" => vec!["regexp_matches".to_string()],
        "regexp_split_to_table" => vec!["regexp_split_to_table".to_string()],
        "pg_get_keywords" => vec![
            "word".to_string(),
            "catcode".to_string(),
            "catdesc".to_string(),
        ],
        _ => vec!["value".to_string()],
    }
}

fn table_function_output_type_oids(function: &TableFunctionRef, count: usize) -> Vec<u32> {
    if !function.column_alias_types.is_empty() {
        return function
            .column_alias_types
            .iter()
            .take(count)
            .map(|entry| {
                entry
                    .as_deref()
                    .map(cast_type_name_to_oid)
                    .unwrap_or(PG_TEXT_OID)
            })
            .collect();
    }

    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();
    let mut oids = match fn_name.as_str() {
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" => {
            vec![PG_TEXT_OID, PG_TEXT_OID]
        }
        "json_object_keys" | "jsonb_object_keys" => vec![PG_TEXT_OID],
        _ => vec![PG_TEXT_OID],
    };
    if oids.len() < count {
        oids.resize(count, PG_TEXT_OID);
    } else if oids.len() > count {
        oids.truncate(count);
    }
    oids
}
