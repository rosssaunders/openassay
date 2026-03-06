use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::catalog::search_path::SearchPath;
use crate::catalog::system_catalogs::lookup_virtual_relation;
use crate::catalog::{TypeSignature, with_catalog_read};
use crate::executor::exec_expr::{EvalScope, eval_expr};
use crate::executor::exec_main::{scope_for_table_row, scope_for_table_row_with_qualifiers};
use crate::parser::ast::{
    BinaryOp, Expr, GroupByExpr, JoinCondition, OrderByExpr, Query, QueryExpr, SelectItem,
    SelectStatement, SetOperator, Statement, SubscriptContainerType, SubscriptValueType,
    TableExpression, TableFunctionRef, UnaryOp,
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
const PG_VECTOR_OID: u32 = 6000;
const PG_FLOAT4_ARRAY_OID: u32 = 1021;

pub(crate) fn type_signature_to_oid(ty: TypeSignature) -> u32 {
    match ty {
        TypeSignature::Bool => PG_BOOL_OID,
        TypeSignature::Int8 => PG_INT8_OID,
        TypeSignature::Float8 => PG_FLOAT8_OID,
        TypeSignature::Numeric => PG_NUMERIC_OID,
        TypeSignature::Text => PG_TEXT_OID,
        TypeSignature::Date => PG_DATE_OID,
        TypeSignature::Timestamp => PG_TIMESTAMP_OID,
        TypeSignature::Vector(_) => PG_VECTOR_OID,
    }
}

pub fn type_oid_size(type_oid: u32) -> i16 {
    match type_oid {
        PG_BOOL_OID => 1,
        PG_INT8_OID => 8,
        PG_FLOAT8_OID => 8,
        PG_DATE_OID => 4,
        PG_TIMESTAMP_OID => 8,
        PG_VECTOR_OID => -1,
        _ => -1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedOutputColumn {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
    pub(crate) subscript_value_type: SubscriptValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedExprType {
    type_oid: u32,
    subscript_value_type: SubscriptValueType,
}

impl ResolvedExprType {
    fn new(type_oid: u32, value_type: &SubscriptValueType) -> Self {
        Self {
            type_oid,
            subscript_value_type: value_type.clone(),
        }
    }

    fn scalar(type_oid: u32) -> Self {
        Self {
            type_oid,
            subscript_value_type: SubscriptValueType::Other,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TypeScope {
    unqualified: HashMap<String, ResolvedExprType>,
    qualified: HashMap<String, ResolvedExprType>,
    ambiguous: HashSet<String>,
}

impl TypeScope {
    fn insert_unqualified(&mut self, key: &str, resolved: ResolvedExprType) {
        let key = key.to_ascii_lowercase();
        if self.ambiguous.contains(&key) {
            return;
        }
        #[allow(clippy::map_entry)]
        if self.unqualified.contains_key(&key) {
            self.unqualified.remove(&key);
            self.ambiguous.insert(key);
        } else {
            self.unqualified.insert(key, resolved);
        }
    }

    fn insert_qualified(&mut self, parts: &[String], resolved: ResolvedExprType) {
        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.insert(key, resolved);
    }

    fn lookup_identifier(&self, parts: &[String]) -> Option<ResolvedExprType> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            let key = parts[0].to_ascii_lowercase();
            if self.ambiguous.contains(&key) {
                return None;
            }
            return self.unqualified.get(&key).cloned();
        }

        let key = parts
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        self.qualified.get(&key).cloned()
    }
}

#[derive(Debug, Clone)]
struct ExpandedFromTypeColumn {
    label: String,
    lookup_parts: Vec<String>,
    type_oid: u32,
    subscript_value_type: SubscriptValueType,
}

fn array_type(inner: SubscriptValueType) -> SubscriptValueType {
    SubscriptValueType::Array(Box::new(inner))
}

fn type_name_subscript_value_type(type_name: &str) -> SubscriptValueType {
    let normalized = type_name.trim().to_ascii_lowercase();
    if normalized == "jsonb" {
        return SubscriptValueType::Jsonb;
    }
    if let Some(inner) = normalized.strip_suffix("[]") {
        return array_type(type_name_subscript_value_type(inner));
    }
    SubscriptValueType::Other
}

fn common_array_element_subscript_type(
    exprs: &[Expr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> SubscriptValueType {
    let mut current = SubscriptValueType::Unknown;
    for expr in exprs {
        let next = infer_expr_type(expr, scope, ctes).subscript_value_type;
        if matches!(next, SubscriptValueType::Unknown) {
            continue;
        }
        if matches!(current, SubscriptValueType::Unknown) {
            current = next;
            continue;
        }
        if current != next {
            return SubscriptValueType::Other;
        }
    }
    if matches!(current, SubscriptValueType::Unknown) {
        SubscriptValueType::Other
    } else {
        current
    }
}

fn cast_type_name_to_oid(type_name: &str) -> u32 {
    match type_name.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => PG_BOOL_OID,
        "int8" | "int4" | "int2" | "bigint" | "integer" | "smallint" => PG_INT8_OID,
        "float8" | "float4" | "numeric" | "decimal" | "real" => PG_FLOAT8_OID,
        "date" => PG_DATE_OID,
        "timestamp" | "timestamptz" => PG_TIMESTAMP_OID,
        "vector" => PG_VECTOR_OID,
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
        let next = infer_expr_type(expr, scope, ctes).type_oid;
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

fn infer_function_return_type(
    name: &[String],
    args: &[Expr],
    within_group: &[OrderByExpr],
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> ResolvedExprType {
    let fn_name = name
        .last()
        .map(|part| part.to_ascii_lowercase())
        .unwrap_or_default();
    let type_oid = match fn_name.as_str() {
        "count" | "char_length" | "length" | "nextval" | "currval" | "setval" | "strpos"
        | "position" | "ascii" | "pg_backend_pid" | "width_bucket" | "scale" | "factorial"
        | "num_nulls" | "num_nonnulls" => PG_INT8_OID,
        "extract" | "date_part" => PG_INT8_OID,
        "avg" => args
            .first()
            .map(|expr| {
                if infer_expr_type(expr, scope, ctes).type_oid == PG_VECTOR_OID {
                    PG_VECTOR_OID
                } else {
                    PG_FLOAT8_OID
                }
            })
            .unwrap_or(PG_FLOAT8_OID),
        "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop" | "corr"
        | "covar_pop" | "covar_samp" | "regr_slope" | "regr_intercept" | "regr_r2"
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
            .map(|expr| infer_expr_type(expr, scope, ctes).type_oid)
            .unwrap_or(PG_FLOAT8_OID),
        "power" | "pow" | "sqrt" | "cbrt" | "exp" | "ln" | "log" | "log10" | "sin" | "cos"
        | "tan" | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "pi" | "random"
        | "to_number" => PG_FLOAT8_OID,
        "div" | "gcd" | "lcm" | "ntile" | "row_number" | "rank" | "dense_rank" => PG_INT8_OID,
        "percent_rank" | "cume_dist" => PG_FLOAT8_OID,
        "sum" => args
            .first()
            .map(|expr| {
                let oid = infer_expr_type(expr, scope, ctes).type_oid;
                if oid == PG_FLOAT8_OID {
                    PG_FLOAT8_OID
                } else {
                    PG_INT8_OID
                }
            })
            .unwrap_or(PG_INT8_OID),
        "percentile_disc" | "mode" => within_group
            .first()
            .map(|entry| infer_expr_type(&entry.expr, scope, ctes).type_oid)
            .unwrap_or(PG_TEXT_OID),
        "min" | "max" | "nullif" => args
            .first()
            .map(|expr| infer_expr_type(expr, scope, ctes).type_oid)
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
        "uuid_generate_v1" | "uuid_generate_v4" | "uuid_generate_v5" | "uuid_nil"
        | "gen_random_uuid" => PG_TEXT_OID,
        "digest" | "hmac" | "gen_random_bytes" | "crypt" | "gen_salt" => PG_TEXT_OID,
        "l2_distance" | "cosine_distance" | "inner_product" | "l1_distance" | "vector_norm" => {
            PG_FLOAT8_OID
        }
        "vector_dims" => PG_INT8_OID,
        "subvector" | "vector_concat" => PG_VECTOR_OID,
        "vector_to_float4" => PG_FLOAT4_ARRAY_OID,
        _ => PG_TEXT_OID,
    };

    let value_type = match fn_name.as_str() {
        "jsonb_build_object"
        | "jsonb_build_array"
        | "jsonb_set"
        | "jsonb_insert"
        | "jsonb_set_lax"
        | "jsonb_concat"
        | "jsonb_delete"
        | "jsonb_delete_path"
        | "jsonb_extract_path"
        | "jsonb_path_query_array"
        | "jsonb_path_query_first"
        | "jsonb_strip_nulls"
        | "jsonb_agg"
        | "jsonb_object_agg"
        | "to_jsonb" => SubscriptValueType::Jsonb,
        "vector_to_float4" | "array_agg" | "string_to_array" | "regexp_split_to_array" => {
            array_type(SubscriptValueType::Other)
        }
        _ => SubscriptValueType::Other,
    };
    ResolvedExprType::new(type_oid, &value_type)
}

fn infer_expr_type(
    expr: &Expr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> ResolvedExprType {
    match expr {
        Expr::Identifier(parts) => scope
            .lookup_identifier(parts)
            .unwrap_or_else(|| ResolvedExprType::scalar(PG_TEXT_OID)),
        Expr::String(_) => ResolvedExprType::scalar(PG_TEXT_OID),
        Expr::Integer(_) => ResolvedExprType::scalar(PG_INT8_OID),
        Expr::Float(_) => ResolvedExprType::scalar(PG_FLOAT8_OID),
        Expr::Boolean(_) => ResolvedExprType::scalar(PG_BOOL_OID),
        Expr::Null | Expr::Default | Expr::Parameter(_) => ResolvedExprType::scalar(PG_TEXT_OID),
        Expr::FunctionCall {
            name,
            args,
            within_group,
            ..
        } => infer_function_return_type(name, args, within_group, scope, ctes),
        Expr::Cast { type_name, .. } | Expr::TypedLiteral { type_name, .. } => {
            ResolvedExprType::new(
                cast_type_name_to_oid(type_name),
                &type_name_subscript_value_type(type_name),
            )
        }
        Expr::Wildcard | Expr::QualifiedWildcard(_) => ResolvedExprType::scalar(PG_TEXT_OID),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => ResolvedExprType::scalar(PG_BOOL_OID),
            UnaryOp::Plus | UnaryOp::Minus => infer_expr_type(expr, scope, ctes),
        },
        Expr::Binary { left, op, right } => {
            let left_ty = infer_expr_type(left, scope, ctes);
            let right_ty = infer_expr_type(right, scope, ctes);
            let type_oid = match op {
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
                BinaryOp::ArrayConcat => left_ty.type_oid,
                BinaryOp::VectorL2Distance
                | BinaryOp::VectorInnerProduct
                | BinaryOp::VectorCosineDistance => PG_FLOAT8_OID,
                BinaryOp::Add => {
                    if left_ty.type_oid == PG_VECTOR_OID && right_ty.type_oid == PG_VECTOR_OID {
                        PG_VECTOR_OID
                    } else if (left_ty.type_oid == PG_DATE_OID
                        || left_ty.type_oid == PG_TIMESTAMP_OID)
                        && right_ty.type_oid == PG_INT8_OID
                    {
                        left_ty.type_oid
                    } else if (right_ty.type_oid == PG_DATE_OID
                        || right_ty.type_oid == PG_TIMESTAMP_OID)
                        && left_ty.type_oid == PG_INT8_OID
                    {
                        right_ty.type_oid
                    } else {
                        infer_numeric_result_oid(left_ty.type_oid, right_ty.type_oid)
                    }
                }
                BinaryOp::Sub => {
                    if left_ty.type_oid == PG_VECTOR_OID && right_ty.type_oid == PG_VECTOR_OID {
                        PG_VECTOR_OID
                    } else if (left_ty.type_oid == PG_DATE_OID
                        || left_ty.type_oid == PG_TIMESTAMP_OID)
                        && (right_ty.type_oid == PG_DATE_OID
                            || right_ty.type_oid == PG_TIMESTAMP_OID)
                    {
                        PG_INT8_OID
                    } else if (left_ty.type_oid == PG_DATE_OID
                        || left_ty.type_oid == PG_TIMESTAMP_OID)
                        && right_ty.type_oid == PG_INT8_OID
                    {
                        left_ty.type_oid
                    } else if left_ty.type_oid == PG_TEXT_OID && right_ty.type_oid == PG_TEXT_OID {
                        PG_TEXT_OID
                    } else {
                        infer_numeric_result_oid(left_ty.type_oid, right_ty.type_oid)
                    }
                }
                BinaryOp::Mul => {
                    if left_ty.type_oid == PG_VECTOR_OID && right_ty.type_oid == PG_VECTOR_OID {
                        PG_VECTOR_OID
                    } else {
                        infer_numeric_result_oid(left_ty.type_oid, right_ty.type_oid)
                    }
                }
                BinaryOp::Div
                | BinaryOp::Mod
                | BinaryOp::Pow
                | BinaryOp::ShiftLeft
                | BinaryOp::ShiftRight => {
                    infer_numeric_result_oid(left_ty.type_oid, right_ty.type_oid)
                }
            };
            let value_type = match op {
                BinaryOp::ArrayConcat => array_type(SubscriptValueType::Other),
                _ => SubscriptValueType::Other,
            };
            ResolvedExprType::new(type_oid, &value_type)
        }
        Expr::AnyAll { .. }
        | Expr::Exists(_)
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::BooleanTest { .. } => ResolvedExprType::scalar(PG_BOOL_OID),
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
            ResolvedExprType::scalar(infer_common_type_oid(&result_exprs, scope, ctes))
        }
        Expr::ScalarSubquery(query) => {
            let mut nested = ctes.clone();
            let subquery_type = derive_query_output_columns_with_ctes(query, &mut nested)
                .ok()
                .and_then(|cols| cols.first().cloned());
            if let Some(column) = subquery_type {
                ResolvedExprType::new(column.type_oid, &column.subscript_value_type)
            } else {
                ResolvedExprType::scalar(PG_TEXT_OID)
            }
        }
        Expr::ArrayConstructor(exprs) => ResolvedExprType::new(
            PG_TEXT_OID,
            &array_type(common_array_element_subscript_type(exprs, scope, ctes)),
        ),
        Expr::ArraySubquery(_) => {
            ResolvedExprType::new(PG_TEXT_OID, &array_type(SubscriptValueType::Other))
        }
        Expr::ArraySubscript {
            expr,
            container_type,
            ..
        } => {
            let value_type = match container_type {
                SubscriptContainerType::Jsonb => SubscriptValueType::Jsonb,
                SubscriptContainerType::Array => {
                    match infer_expr_subscript_value_type(expr, scope, ctes) {
                        SubscriptValueType::Array(inner) => *inner,
                        _ => SubscriptValueType::Other,
                    }
                }
                SubscriptContainerType::Other | SubscriptContainerType::Unknown => {
                    SubscriptValueType::Other
                }
            };
            ResolvedExprType::new(PG_TEXT_OID, &value_type)
        }
        Expr::ArraySlice {
            expr,
            container_type,
            ..
        } => {
            let value_type = if matches!(container_type, SubscriptContainerType::Array) {
                infer_expr_subscript_value_type(expr, scope, ctes)
            } else {
                SubscriptValueType::Other
            };
            ResolvedExprType::new(PG_TEXT_OID, &value_type)
        }
        Expr::RowConstructor(_) => ResolvedExprType::scalar(2249),
        Expr::MultiColumnSubqueryRef { .. } => ResolvedExprType::scalar(PG_TEXT_OID),
    }
}

fn infer_expr_subscript_value_type(
    expr: &Expr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> SubscriptValueType {
    infer_expr_type(expr, scope, ctes).subscript_value_type
}

fn build_scope_from_columns(columns: Vec<ExpandedFromTypeColumn>) -> TypeScope {
    let mut scope = TypeScope::default();
    for column in columns {
        let resolved = ResolvedExprType::new(column.type_oid, &column.subscript_value_type);
        scope.insert_unqualified(&column.label, resolved.clone());
        scope.insert_qualified(&column.lookup_parts, resolved);
    }
    scope
}

fn extend_scope_with_table(
    scope: &mut TypeScope,
    table_name: &[String],
    alias: Option<&str>,
) -> Result<(), EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    let qualifier = alias
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_else(|| table.name().to_string());
    for column in table.columns() {
        let resolved = ResolvedExprType::new(
            type_signature_to_oid(column.type_signature()),
            column.subscript_value_type(),
        );
        scope.insert_unqualified(column.name(), resolved.clone());
        scope.insert_qualified(
            &[qualifier.clone(), column.name().to_string()],
            resolved.clone(),
        );
        scope.insert_qualified(
            &[
                table.qualified_name().to_string(),
                column.name().to_string(),
            ],
            resolved,
        );
    }
    Ok(())
}

fn annotate_window_spec(
    spec: &mut crate::parser::ast::WindowSpec,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    for expr in &mut spec.partition_by {
        let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
    }
    for order_by in &mut spec.order_by {
        let _ = annotate_expr_subscript_types(&mut order_by.expr, scope, ctes)?;
    }
    if let Some(frame) = &mut spec.frame {
        annotate_window_frame_bound(&mut frame.start, scope, ctes)?;
        annotate_window_frame_bound(&mut frame.end, scope, ctes)?;
    }
    Ok(())
}

fn annotate_window_frame_bound(
    bound: &mut crate::parser::ast::WindowFrameBound,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match bound {
        crate::parser::ast::WindowFrameBound::OffsetPreceding(expr)
        | crate::parser::ast::WindowFrameBound::OffsetFollowing(expr) => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
        }
        crate::parser::ast::WindowFrameBound::UnboundedPreceding
        | crate::parser::ast::WindowFrameBound::CurrentRow
        | crate::parser::ast::WindowFrameBound::UnboundedFollowing => {}
    }
    Ok(())
}

fn annotate_group_by_expr(
    expr: &mut GroupByExpr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match expr {
        GroupByExpr::Expr(expr) => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
        }
        GroupByExpr::GroupingSets(sets) => {
            for set in sets {
                for expr in set {
                    let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
                }
            }
        }
        GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
            for expr in exprs {
                let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            }
        }
    }
    Ok(())
}

fn annotate_table_expression_subscript_types(
    table: &mut TableExpression,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match table {
        TableExpression::Relation(_) => Ok(()),
        TableExpression::Function(function) => {
            let scope = TypeScope::default();
            for arg in &mut function.args {
                let _ = annotate_expr_subscript_types(arg, &scope, ctes)?;
            }
            Ok(())
        }
        TableExpression::Subquery(subquery) => {
            annotate_query_subscript_types_with_ctes(&mut subquery.query, ctes)
        }
        TableExpression::Join(join) => {
            annotate_table_expression_subscript_types(&mut join.left, ctes)?;
            annotate_table_expression_subscript_types(&mut join.right, ctes)?;
            Ok(())
        }
    }
}

fn annotate_join_conditions(
    table: &mut TableExpression,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match table {
        TableExpression::Join(join) => {
            annotate_join_conditions(&mut join.left, scope, ctes)?;
            annotate_join_conditions(&mut join.right, scope, ctes)?;
            if let Some(JoinCondition::On(expr)) = &mut join.condition {
                let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            }
            Ok(())
        }
        TableExpression::Relation(_)
        | TableExpression::Function(_)
        | TableExpression::Subquery(_) => Ok(()),
    }
}

fn annotate_select_subscript_types(
    select: &mut SelectStatement,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    for table in &mut select.from {
        annotate_table_expression_subscript_types(table, ctes)?;
    }
    let scope = if select.from.is_empty() {
        TypeScope::default()
    } else {
        build_scope_from_columns(expand_from_columns_typed(&select.from, ctes)?)
    };
    for table in &mut select.from {
        annotate_join_conditions(table, &scope, ctes)?;
    }
    for target in &mut select.targets {
        let _ = annotate_expr_subscript_types(&mut target.expr, &scope, ctes)?;
    }
    if let Some(where_clause) = &mut select.where_clause {
        let _ = annotate_expr_subscript_types(where_clause, &scope, ctes)?;
    }
    for expr in &mut select.group_by {
        annotate_group_by_expr(expr, &scope, ctes)?;
    }
    if let Some(having) = &mut select.having {
        let _ = annotate_expr_subscript_types(having, &scope, ctes)?;
    }
    for window in &mut select.window_definitions {
        annotate_window_spec(&mut window.spec, &scope, ctes)?;
    }
    Ok(())
}

fn annotate_query_expr_subscript_types(
    expr: &mut QueryExpr,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match expr {
        QueryExpr::Select(select) => annotate_select_subscript_types(select, ctes),
        QueryExpr::SetOperation { left, right, .. } => {
            annotate_query_expr_subscript_types(left, ctes)?;
            annotate_query_expr_subscript_types(right, ctes)
        }
        QueryExpr::Nested(query) => annotate_query_subscript_types_with_ctes(query, ctes),
        QueryExpr::Values(rows) => {
            let scope = TypeScope::default();
            for row in rows {
                for expr in row {
                    let _ = annotate_expr_subscript_types(expr, &scope, ctes)?;
                }
            }
            Ok(())
        }
        QueryExpr::Insert(insert) => annotate_insert_subscript_types(insert, ctes),
        QueryExpr::Update(update) => annotate_update_subscript_types(update, ctes),
        QueryExpr::Delete(delete) => annotate_delete_subscript_types(delete, ctes),
    }
}

fn annotate_query_subscript_types_with_ctes(
    query: &mut Query,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    let mut local_ctes = ctes.clone();
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
            annotate_query_subscript_types_with_ctes(&mut cte.query, &mut local_ctes)?;
            let mut cols = derive_query_output_columns_with_ctes(&cte.query, &mut local_ctes)?;
            if !cte.column_names.is_empty() {
                for (idx, name) in cte.column_names.iter().take(cols.len()).enumerate() {
                    cols[idx].name = name.clone();
                }
            }
            append_cte_aux_output_columns(&mut cols, cte);
            local_ctes.insert(cte.name.to_ascii_lowercase(), cols);
        }
    }
    annotate_query_expr_subscript_types(&mut query.body, &mut local_ctes)?;
    let scope = TypeScope::default();
    for order_by in &mut query.order_by {
        let _ = annotate_expr_subscript_types(&mut order_by.expr, &scope, &local_ctes)?;
    }
    if let Some(limit) = &mut query.limit {
        let _ = annotate_expr_subscript_types(limit, &scope, &local_ctes)?;
    }
    if let Some(offset) = &mut query.offset {
        let _ = annotate_expr_subscript_types(offset, &scope, &local_ctes)?;
    }
    Ok(())
}

fn annotate_insert_subscript_types(
    insert: &mut crate::parser::ast::InsertStatement,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    match &mut insert.source {
        crate::parser::ast::InsertSource::Values(rows) => {
            let scope = TypeScope::default();
            for row in rows {
                for expr in row {
                    let _ = annotate_expr_subscript_types(expr, &scope, ctes)?;
                }
            }
        }
        crate::parser::ast::InsertSource::Query(query) => {
            annotate_query_subscript_types_with_ctes(query, ctes)?;
        }
    }
    let mut scope = TypeScope::default();
    extend_scope_with_table(
        &mut scope,
        &insert.table_name,
        insert.table_alias.as_deref(),
    )?;
    for target in &mut insert.returning {
        let _ = annotate_expr_subscript_types(&mut target.expr, &scope, ctes)?;
    }
    if let Some(on_conflict) = &mut insert.on_conflict {
        match on_conflict {
            crate::parser::ast::OnConflictClause::DoNothing { .. } => {}
            crate::parser::ast::OnConflictClause::DoUpdate {
                assignments,
                where_clause,
                ..
            } => {
                for assignment in assignments {
                    let _ = annotate_expr_subscript_types(&mut assignment.value, &scope, ctes)?;
                }
                if let Some(where_clause) = where_clause {
                    let _ = annotate_expr_subscript_types(where_clause, &scope, ctes)?;
                }
            }
        }
    }
    Ok(())
}

fn annotate_update_subscript_types(
    update: &mut crate::parser::ast::UpdateStatement,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    for table in &mut update.from {
        annotate_table_expression_subscript_types(table, ctes)?;
    }
    let mut scope = if update.from.is_empty() {
        TypeScope::default()
    } else {
        build_scope_from_columns(expand_from_columns_typed(&update.from, ctes)?)
    };
    extend_scope_with_table(&mut scope, &update.table_name, update.alias.as_deref())?;
    for table in &mut update.from {
        annotate_join_conditions(table, &scope, ctes)?;
    }
    for assignment in &mut update.assignments {
        let _ = annotate_expr_subscript_types(&mut assignment.value, &scope, ctes)?;
    }
    if let Some(where_clause) = &mut update.where_clause {
        let _ = annotate_expr_subscript_types(where_clause, &scope, ctes)?;
    }
    for target in &mut update.returning {
        let _ = annotate_expr_subscript_types(&mut target.expr, &scope, ctes)?;
    }
    Ok(())
}

fn annotate_delete_subscript_types(
    delete: &mut crate::parser::ast::DeleteStatement,
    ctes: &mut HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<(), EngineError> {
    for table in &mut delete.using {
        annotate_table_expression_subscript_types(table, ctes)?;
    }
    let mut scope = if delete.using.is_empty() {
        TypeScope::default()
    } else {
        build_scope_from_columns(expand_from_columns_typed(&delete.using, ctes)?)
    };
    extend_scope_with_table(&mut scope, &delete.table_name, None)?;
    for table in &mut delete.using {
        annotate_join_conditions(table, &scope, ctes)?;
    }
    if let Some(where_clause) = &mut delete.where_clause {
        let _ = annotate_expr_subscript_types(where_clause, &scope, ctes)?;
    }
    for target in &mut delete.returning {
        let _ = annotate_expr_subscript_types(&mut target.expr, &scope, ctes)?;
    }
    Ok(())
}

fn annotate_expr_subscript_types(
    expr: &mut Expr,
    scope: &TypeScope,
    ctes: &HashMap<String, Vec<PlannedOutputColumn>>,
) -> Result<SubscriptValueType, EngineError> {
    let value_type = match expr {
        Expr::Identifier(parts) => scope
            .lookup_identifier(parts)
            .map_or(SubscriptValueType::Other, |ty| ty.subscript_value_type),
        Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Default
        | Expr::Parameter(_)
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::MultiColumnSubqueryRef { .. } => SubscriptValueType::Other,
        Expr::FunctionCall {
            name,
            args,
            within_group,
            filter,
            over,
            ..
        } => {
            for arg in args.iter_mut() {
                let _ = annotate_expr_subscript_types(arg, scope, ctes)?;
            }
            for order_by in within_group.iter_mut() {
                let _ = annotate_expr_subscript_types(&mut order_by.expr, scope, ctes)?;
            }
            if let Some(filter) = filter {
                let _ = annotate_expr_subscript_types(filter, scope, ctes)?;
            }
            if let Some(over) = over {
                annotate_window_spec(over, scope, ctes)?;
            }
            infer_function_return_type(name, args, within_group, scope, ctes).subscript_value_type
        }
        Expr::Cast { expr, type_name } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            type_name_subscript_value_type(type_name)
        }
        Expr::TypedLiteral { type_name, .. } => type_name_subscript_value_type(type_name),
        Expr::Unary { expr, .. } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            infer_expr_subscript_value_type(expr, scope, ctes)
        }
        Expr::Binary { left, right, op } => {
            let _ = annotate_expr_subscript_types(left, scope, ctes)?;
            let _ = annotate_expr_subscript_types(right, scope, ctes)?;
            match op {
                BinaryOp::ArrayConcat => array_type(SubscriptValueType::Other),
                _ => SubscriptValueType::Other,
            }
        }
        Expr::AnyAll { left, right, .. } | Expr::IsDistinctFrom { left, right, .. } => {
            let _ = annotate_expr_subscript_types(left, scope, ctes)?;
            let _ = annotate_expr_subscript_types(right, scope, ctes)?;
            SubscriptValueType::Other
        }
        Expr::Exists(query) => {
            let mut nested = ctes.clone();
            annotate_query_subscript_types_with_ctes(query, &mut nested)?;
            SubscriptValueType::Other
        }
        Expr::ScalarSubquery(query) => {
            let mut nested = ctes.clone();
            annotate_query_subscript_types_with_ctes(query, &mut nested)?;
            derive_query_output_columns_with_ctes(query, &mut nested)
                .ok()
                .and_then(|cols| cols.first().map(|col| col.subscript_value_type.clone()))
                .unwrap_or(SubscriptValueType::Other)
        }
        Expr::ArraySubquery(query) => {
            let mut nested = ctes.clone();
            annotate_query_subscript_types_with_ctes(query, &mut nested)?;
            let element_type = derive_query_output_columns_with_ctes(query, &mut nested)
                .ok()
                .and_then(|cols| cols.first().map(|col| col.subscript_value_type.clone()))
                .unwrap_or(SubscriptValueType::Other);
            array_type(element_type)
        }
        Expr::InList { expr, list, .. } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            for item in list {
                let _ = annotate_expr_subscript_types(item, scope, ctes)?;
            }
            SubscriptValueType::Other
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            let mut nested = ctes.clone();
            annotate_query_subscript_types_with_ctes(subquery, &mut nested)?;
            SubscriptValueType::Other
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            let _ = annotate_expr_subscript_types(low, scope, ctes)?;
            let _ = annotate_expr_subscript_types(high, scope, ctes)?;
            SubscriptValueType::Other
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            let _ = annotate_expr_subscript_types(pattern, scope, ctes)?;
            if let Some(escape) = escape {
                let _ = annotate_expr_subscript_types(escape, scope, ctes)?;
            }
            SubscriptValueType::Other
        }
        Expr::IsNull { expr, .. } | Expr::BooleanTest { expr, .. } => {
            let _ = annotate_expr_subscript_types(expr, scope, ctes)?;
            SubscriptValueType::Other
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let _ = annotate_expr_subscript_types(operand, scope, ctes)?;
            for (when, then) in when_then {
                let _ = annotate_expr_subscript_types(when, scope, ctes)?;
                let _ = annotate_expr_subscript_types(then, scope, ctes)?;
            }
            if let Some(else_expr) = else_expr {
                let _ = annotate_expr_subscript_types(else_expr, scope, ctes)?;
            }
            SubscriptValueType::Other
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                let _ = annotate_expr_subscript_types(when, scope, ctes)?;
                let _ = annotate_expr_subscript_types(then, scope, ctes)?;
            }
            if let Some(else_expr) = else_expr {
                let _ = annotate_expr_subscript_types(else_expr, scope, ctes)?;
            }
            SubscriptValueType::Other
        }
        Expr::ArrayConstructor(values) => {
            for value in values.iter_mut() {
                let _ = annotate_expr_subscript_types(value, scope, ctes)?;
            }
            array_type(common_array_element_subscript_type(values, scope, ctes))
        }
        Expr::RowConstructor(values) => {
            for value in values.iter_mut() {
                let _ = annotate_expr_subscript_types(value, scope, ctes)?;
            }
            SubscriptValueType::Other
        }
        Expr::ArraySubscript {
            expr,
            index,
            container_type,
        } => {
            let base_type = annotate_expr_subscript_types(expr, scope, ctes)?;
            let _ = annotate_expr_subscript_types(index, scope, ctes)?;
            *container_type = base_type.container_type();
            match base_type {
                SubscriptValueType::Jsonb => SubscriptValueType::Jsonb,
                SubscriptValueType::Array(inner) => *inner,
                SubscriptValueType::Other | SubscriptValueType::Unknown => {
                    SubscriptValueType::Other
                }
            }
        }
        Expr::ArraySlice {
            expr,
            start,
            end,
            container_type,
        } => {
            let base_type = annotate_expr_subscript_types(expr, scope, ctes)?;
            if let Some(start) = start {
                let _ = annotate_expr_subscript_types(start, scope, ctes)?;
            }
            if let Some(end) = end {
                let _ = annotate_expr_subscript_types(end, scope, ctes)?;
            }
            *container_type = base_type.container_type();
            match base_type {
                SubscriptValueType::Array(_) => base_type,
                SubscriptValueType::Jsonb
                | SubscriptValueType::Other
                | SubscriptValueType::Unknown => SubscriptValueType::Other,
            }
        }
    };
    Ok(value_type)
}

pub(crate) fn annotate_statement_subscript_types(
    statement: &mut Statement,
) -> Result<(), EngineError> {
    let mut ctes = HashMap::new();
    match statement {
        Statement::Query(query) => annotate_query_subscript_types_with_ctes(query, &mut ctes),
        Statement::Insert(insert) => annotate_insert_subscript_types(insert, &mut ctes),
        Statement::Update(update) => annotate_update_subscript_types(update, &mut ctes),
        Statement::Delete(delete) => annotate_delete_subscript_types(delete, &mut ctes),
        _ => Ok(()),
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
        Expr::QualifiedWildcard(_) => {
            return Err(EngineError {
                message: "qualified wildcard target requires FROM support".to_string(),
            });
        }
        _ => "?column?".to_string(),
    };
    Ok(name)
}

fn is_recursive_union_expr(expr: &QueryExpr) -> bool {
    matches!(
        expr,
        QueryExpr::SetOperation {
            op: SetOperator::Union,
            ..
        }
    )
}

fn harmonize_setop_columns(mut left: Vec<String>, right: &[String]) -> Vec<String> {
    if left.len() < right.len() {
        left.extend(right.iter().skip(left.len()).cloned());
    } else if right.len() < left.len() {
        left.truncate(right.len());
    }
    left
}

fn harmonize_setop_output_columns(
    mut left: Vec<PlannedOutputColumn>,
    right: &[PlannedOutputColumn],
) -> Vec<PlannedOutputColumn> {
    if left.len() < right.len() {
        left.extend(right.iter().skip(left.len()).cloned());
    } else if right.len() < left.len() {
        left.truncate(right.len());
    }
    left
}

fn append_cte_aux_columns(columns: &mut Vec<String>, cte: &crate::parser::ast::CommonTableExpr) {
    if let Some(search) = &cte.search_clause
        && !columns
            .iter()
            .any(|col| col.eq_ignore_ascii_case(&search.set_column))
    {
        columns.push(search.set_column.clone());
    }
    if let Some(cycle) = &cte.cycle_clause {
        if !columns
            .iter()
            .any(|col| col.eq_ignore_ascii_case(&cycle.set_column))
        {
            columns.push(cycle.set_column.clone());
        }
        if !columns
            .iter()
            .any(|col| col.eq_ignore_ascii_case(&cycle.using_column))
        {
            columns.push(cycle.using_column.clone());
        }
    }
}

fn append_cte_aux_output_columns(
    columns: &mut Vec<PlannedOutputColumn>,
    cte: &crate::parser::ast::CommonTableExpr,
) {
    if let Some(search) = &cte.search_clause
        && !columns
            .iter()
            .any(|col| col.name.eq_ignore_ascii_case(&search.set_column))
    {
        columns.push(PlannedOutputColumn {
            name: search.set_column.clone(),
            type_oid: PG_INT8_OID,
            subscript_value_type: SubscriptValueType::Other,
        });
    }
    if let Some(cycle) = &cte.cycle_clause {
        if !columns
            .iter()
            .any(|col| col.name.eq_ignore_ascii_case(&cycle.set_column))
        {
            columns.push(PlannedOutputColumn {
                name: cycle.set_column.clone(),
                type_oid: PG_BOOL_OID,
                subscript_value_type: SubscriptValueType::Other,
            });
        }
        if !columns
            .iter()
            .any(|col| col.name.eq_ignore_ascii_case(&cycle.using_column))
        {
            columns.push(PlannedOutputColumn {
                name: cycle.using_column.clone(),
                type_oid: PG_TEXT_OID,
                subscript_value_type: SubscriptValueType::Other,
            });
        }
    }
}

fn wildcard_matches_qualifier(lookup_parts: &[String], qualifier: &[String]) -> bool {
    let Some(target) = qualifier.last() else {
        return false;
    };
    lookup_parts
        .first()
        .is_some_and(|part| part.eq_ignore_ascii_case(target))
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
            let cols = if with.recursive
                && query_references_relation(&cte.query, &cte_name)
                && is_recursive_union_expr(&cte.query.body)
            {
                derive_recursive_cte_output_columns(cte, &local_ctes)?
            } else {
                let mut cols = derive_query_output_columns_with_ctes(&cte.query, &mut local_ctes)?;
                if !cte.column_names.is_empty() {
                    let renamed = cte
                        .column_names
                        .iter()
                        .enumerate()
                        .map(|(idx, name)| PlannedOutputColumn {
                            name: name.clone(),
                            type_oid: cols.get(idx).map_or(PG_TEXT_OID, |col| col.type_oid),
                            subscript_value_type: cols
                                .get(idx)
                                .map_or(SubscriptValueType::Other, |col| {
                                    col.subscript_value_type.clone()
                                }),
                        })
                        .collect::<Vec<_>>();
                    cols = renamed;
                }
                append_cte_aux_output_columns(&mut cols, cte);
                cols
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

    let mut left_cols = derive_query_expr_output_columns(left, ctes)?;
    if !cte.column_names.is_empty() {
        left_cols = cte
            .column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| PlannedOutputColumn {
                name: name.clone(),
                type_oid: left_cols.get(idx).map_or(PG_TEXT_OID, |col| col.type_oid),
                subscript_value_type: left_cols.get(idx).map_or(SubscriptValueType::Other, |col| {
                    col.subscript_value_type.clone()
                }),
            })
            .collect();
    }
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_output_columns(right, &recursive_ctes)?;
    let mut out = harmonize_setop_output_columns(left_cols, &right_cols);
    append_cte_aux_output_columns(&mut out, cte);
    Ok(out)
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
            Ok(harmonize_setop_output_columns(left_cols, &right_cols))
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
                    subscript_value_type: SubscriptValueType::Other,
                })
                .collect())
        }
        QueryExpr::Insert(insert) => {
            let names = derive_dml_returning_columns(&insert.table_name, &insert.returning)
                .unwrap_or_default();
            let types =
                derive_dml_returning_column_type_oids(&insert.table_name, &insert.returning)
                    .unwrap_or_else(|_| vec![PG_TEXT_OID; names.len()]);
            Ok(names
                .into_iter()
                .enumerate()
                .map(|(idx, name)| PlannedOutputColumn {
                    name,
                    type_oid: types.get(idx).copied().unwrap_or(PG_TEXT_OID),
                    subscript_value_type: SubscriptValueType::Other,
                })
                .collect())
        }
        QueryExpr::Update(update) => {
            let names = derive_dml_returning_columns(&update.table_name, &update.returning)
                .unwrap_or_default();
            let types =
                derive_dml_returning_column_type_oids(&update.table_name, &update.returning)
                    .unwrap_or_else(|_| vec![PG_TEXT_OID; names.len()]);
            Ok(names
                .into_iter()
                .enumerate()
                .map(|(idx, name)| PlannedOutputColumn {
                    name,
                    type_oid: types.get(idx).copied().unwrap_or(PG_TEXT_OID),
                    subscript_value_type: SubscriptValueType::Other,
                })
                .collect())
        }
        QueryExpr::Delete(delete) => {
            let names = derive_dml_returning_columns(&delete.table_name, &delete.returning)
                .unwrap_or_default();
            let types =
                derive_dml_returning_column_type_oids(&delete.table_name, &delete.returning)
                    .unwrap_or_else(|_| vec![PG_TEXT_OID; names.len()]);
            Ok(names
                .into_iter()
                .enumerate()
                .map(|(idx, name)| PlannedOutputColumn {
                    name,
                    type_oid: types.get(idx).copied().unwrap_or(PG_TEXT_OID),
                    subscript_value_type: SubscriptValueType::Other,
                })
                .collect())
        }
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
        .any(|target| matches!(target.expr, Expr::Wildcard | Expr::QualifiedWildcard(_)))
    {
        Some(expand_from_columns_typed(&select.from, ctes)?)
    } else {
        None
    };

    let mut type_scope = TypeScope::default();
    for col in from_columns {
        let resolved = ResolvedExprType::new(col.type_oid, &col.subscript_value_type);
        type_scope.insert_unqualified(&col.label, resolved.clone());
        type_scope.insert_qualified(&col.lookup_parts, resolved);
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
                subscript_value_type: col.subscript_value_type.clone(),
            }));
            continue;
        }
        if let Expr::QualifiedWildcard(qualifier) = &target.expr {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "qualified wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                if wildcard_matches_qualifier(&col.lookup_parts, qualifier) {
                    columns.push(PlannedOutputColumn {
                        name: col.label.clone(),
                        type_oid: col.type_oid,
                        subscript_value_type: col.subscript_value_type.clone(),
                    });
                }
            }
            continue;
        }

        let resolved = infer_expr_type(&target.expr, &type_scope, ctes);
        columns.push(PlannedOutputColumn {
            name: infer_select_target_name(target)?,
            type_oid: resolved.type_oid,
            subscript_value_type: resolved.subscript_value_type,
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
            let cols = if with.recursive
                && query_references_relation(&cte.query, &cte_name)
                && is_recursive_union_expr(&cte.query.body)
            {
                derive_recursive_cte_columns(cte, &local_ctes)?
            } else {
                let mut cols = derive_query_columns_with_ctes(&cte.query, &mut local_ctes)?;
                if !cte.column_names.is_empty() {
                    cols = cte.column_names.clone();
                }
                append_cte_aux_columns(&mut cols, cte);
                cols
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

    let mut left_cols = derive_query_expr_columns(left, ctes)?;
    if !cte.column_names.is_empty() {
        left_cols = cte.column_names.clone();
    }
    let mut recursive_ctes = ctes.clone();
    recursive_ctes.insert(cte_name, left_cols.clone());
    let right_cols = derive_query_expr_columns(right, &recursive_ctes)?;
    let mut out = harmonize_setop_columns(left_cols, &right_cols);
    append_cte_aux_columns(&mut out, cte);
    Ok(out)
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
            Ok(harmonize_setop_columns(left_cols, &right_cols))
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
        QueryExpr::Insert(insert) => Ok(derive_dml_returning_columns(
            &insert.table_name,
            &insert.returning,
        )
        .unwrap_or_default()),
        QueryExpr::Update(update) => Ok(derive_dml_returning_columns(
            &update.table_name,
            &update.returning,
        )
        .unwrap_or_default()),
        QueryExpr::Delete(delete) => Ok(derive_dml_returning_columns(
            &delete.table_name,
            &delete.returning,
        )
        .unwrap_or_default()),
    }
}

pub(crate) fn derive_select_columns(
    select: &SelectStatement,
    ctes: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, EngineError> {
    let wildcard_columns = if select
        .targets
        .iter()
        .any(|target| matches!(target.expr, Expr::Wildcard | Expr::QualifiedWildcard(_)))
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
        if let Expr::QualifiedWildcard(qualifier) = &target.expr {
            let Some(expanded) = &wildcard_columns else {
                return Err(EngineError {
                    message: "qualified wildcard target requires FROM support".to_string(),
                });
            };
            for col in expanded {
                if wildcard_matches_qualifier(&col.lookup_parts, qualifier) {
                    columns.push(col.label.clone());
                }
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
        let resolved = ResolvedExprType::new(oid, column.subscript_value_type());
        scope.insert_unqualified(column.name(), resolved.clone());
        scope.insert_qualified(
            &[table.name().to_string(), column.name().to_string()],
            resolved.clone(),
        );
        scope.insert_qualified(
            &[
                table.qualified_name().to_string(),
                column.name().to_string(),
            ],
            resolved,
        );
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
        out.push(infer_expr_type(&target.expr, &scope, &ctes).type_oid);
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
            let column_names = effective_table_function_columns(function);
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
            let mut cols = derive_query_columns_with_ctes(&sub.query, &mut nested)?;
            if !sub.column_aliases.is_empty() {
                for (idx, alias) in sub.column_aliases.iter().take(cols.len()).enumerate() {
                    cols[idx] = alias.clone();
                }
            }
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
                            subscript_value_type: column.subscript_value_type.clone(),
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
                        subscript_value_type: SubscriptValueType::Other,
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
                    subscript_value_type: column.subscript_value_type().clone(),
                })
                .collect())
        }
        TableExpression::Function(function) => {
            let column_names = effective_table_function_columns(function);
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
                        subscript_value_type: SubscriptValueType::Other,
                    }
                })
                .collect())
        }
        TableExpression::Subquery(sub) => {
            let mut nested = ctes.clone();
            let mut cols = derive_query_output_columns_with_ctes(&sub.query, &mut nested)?;
            if !sub.column_aliases.is_empty() {
                for (idx, alias) in sub.column_aliases.iter().take(cols.len()).enumerate() {
                    cols[idx].name = alias.clone();
                }
            }
            if let Some(alias) = &sub.alias {
                let qualifier = alias.to_ascii_lowercase();
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![qualifier.clone(), col.name],
                        type_oid: col.type_oid,
                        subscript_value_type: col.subscript_value_type,
                    })
                    .collect())
            } else {
                Ok(cols
                    .into_iter()
                    .map(|col| ExpandedFromTypeColumn {
                        label: col.name.clone(),
                        lookup_parts: vec![col.name],
                        type_oid: col.type_oid,
                        subscript_value_type: col.subscript_value_type,
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

fn effective_table_function_columns(function: &TableFunctionRef) -> Vec<String> {
    if !function.column_aliases.is_empty() {
        return function.column_aliases.clone();
    }

    let mut columns = table_function_output_columns(function);
    if columns.len() == 1
        && let Some(alias) = &function.alias
        && function
            .name
            .last()
            .is_some_and(|name| name.eq_ignore_ascii_case("generate_series"))
    {
        columns[0] = alias.clone();
    }
    columns
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
        "json_populate_record"
        | "jsonb_populate_record"
        | "json_populate_recordset"
        | "jsonb_populate_recordset"
        | "json_table" => Vec::new(),
        "generate_series" => vec!["generate_series".to_string()],
        "unnest" => vec!["unnest".to_string()],
        "regexp_matches" => vec!["regexp_matches".to_string()],
        "regexp_split_to_table" => vec!["regexp_split_to_table".to_string()],
        "string_to_table" => vec!["string_to_table".to_string()],
        "pg_get_keywords" => vec![
            "word".to_string(),
            "catcode".to_string(),
            "catdesc".to_string(),
        ],
        "iceberg_metadata" => vec![
            "table_uuid".to_string(),
            "format_version".to_string(),
            "last_updated".to_string(),
            "current_schema_id".to_string(),
            "partition_spec".to_string(),
            "snapshot_count".to_string(),
            "total_data_files".to_string(),
        ],
        "pg_input_error_info" => vec![
            "message".to_string(),
            "detail".to_string(),
            "hint".to_string(),
            "sql_error_code".to_string(),
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
        "json_populate_record"
        | "jsonb_populate_record"
        | "json_populate_recordset"
        | "jsonb_populate_recordset" => Vec::new(),
        "iceberg_metadata" => vec![
            PG_TEXT_OID, // table_uuid
            20,          // format_version (int8)
            20,          // last_updated (int8)
            20,          // current_schema_id (int8)
            PG_TEXT_OID, // partition_spec
            20,          // snapshot_count (int8)
            20,          // total_data_files (int8)
        ],
        "pg_input_error_info" => vec![PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID, PG_TEXT_OID],
        _ => vec![PG_TEXT_OID],
    };
    if oids.len() < count {
        oids.resize(count, PG_TEXT_OID);
    } else if oids.len() > count {
        oids.truncate(count);
    }
    oids
}
