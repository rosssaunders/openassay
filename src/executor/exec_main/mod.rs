use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, File};
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    _mm256_add_epi64, _mm256_add_pd, _mm256_blendv_epi8, _mm256_cmpgt_epi64, _mm256_loadu_pd,
    _mm256_loadu_si256, _mm256_max_pd, _mm256_min_pd, _mm256_set1_epi64x, _mm256_set1_pd,
    _mm256_setzero_pd, _mm256_setzero_si256, _mm256_storeu_pd, _mm256_storeu_si256,
};

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
    current_cte_binding, derive_select_columns, expand_from_columns, lookup_user_function,
    lookup_virtual_relation, query_references_relation, relation_row_visible_for_command,
    require_relation_privilege, type_signature_to_oid, validate_recursive_cte_terms,
    with_cte_context_async, with_ext_read, with_storage_read,
};
#[cfg(target_arch = "wasm32")]
use crate::tcop::engine::{drain_wasm_ws_messages, sync_wasm_ws_state};
use crate::tcop::pquery::derive_dml_returning_columns;
use crate::utils::adt::json::{
    eval_http_get, extract_json_path_value, json_value_text_output, jsonb_path_query_values,
    parse_json_document_arg, scalar_to_json_value,
};
use crate::utils::adt::misc::{
    compare_values_for_predicate, eval_regexp_matches_set_function,
    eval_regexp_split_to_table_set_function, eval_string_to_table_set_function,
    parse_f64_numeric_scalar, render_expr_to_sql, truthy,
};
use crate::utils::fmgr::eval_scalar_function;
#[cfg(not(target_arch = "wasm32"))]
use parquet::file::reader::{FileReader, SerializedFileReader};
#[cfg(not(target_arch = "wasm32"))]
use parquet::record::Field as ParquetField;
use serde_json::{Map as JsonMap, Value as JsonValue};

mod aggregation;
mod from_clause;
mod order_limit;
mod query_pipeline;
mod scope_utils;
mod set_operations;
mod table_functions;
#[cfg(test)]
mod tests;

pub(crate) use aggregation::{eval_aggregate_function, is_aggregate_function};
pub(crate) use from_clause::{TableEval, evaluate_from_clause, evaluate_table_expression};
pub(crate) use order_limit::{compare_order_keys, parse_non_negative_int};
pub(crate) use query_pipeline::{execute_query, execute_query_with_outer};
pub(crate) use scope_utils::{
    combine_scopes, scope_for_table_row, scope_for_table_row_with_qualifiers,
};
pub(crate) use set_operations::row_key;
pub(crate) use table_functions::json_value_to_scalar;

use aggregation::{
    GroupingContext, collect_grouping_identifiers, contains_aggregate_expr, contains_window_expr,
    eval_group_expr, expand_grouping_sets, group_by_contains_window_expr, group_by_exprs,
    identifier_key, project_select_row, project_select_row_with_window, resolve_group_by_alias,
};
use from_clause::{
    decompose_and_conjuncts, evaluate_from_clause_with_pushdown,
    relation_index_offsets_for_predicates,
};
use order_limit::{
    apply_offset_limit, apply_order_by, augment_select_for_order_by,
    collect_extra_order_by_columns, scalar_cmp,
};
use query_pipeline::execute_query_expr_with_outer;
use scope_utils::scope_from_row;
use set_operations::{
    append_cte_aux_columns, dedupe_rows, execute_set_operation, is_recursive_union_expr,
    normalize_row_width, populate_cte_aux_values,
};
use table_functions::{
    evaluate_relation, evaluate_relation_with_predicates, evaluate_table_function,
};
