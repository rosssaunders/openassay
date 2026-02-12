//! Semantic analysis pass that runs between parsing and execution.
//!
//! The analyzer validates a parsed AST [`Statement`] against the catalog,
//! performing name resolution, type checking, and function resolution.
//! It does **not** transform the AST — it is a validation-only pass that
//! catches errors early with PostgreSQL-compatible error messages.

pub mod binding;
pub mod functions;
pub mod types;

#[cfg(test)]
mod tests;

use crate::catalog::SearchPath;
use crate::parser::ast::{Query, QueryExpr, Statement};
use crate::tcop::engine::EngineError;

use binding::BindingContext;

/// Analyze a parsed statement, validating names, types, and functions.
///
/// This is called between parsing and planning/execution. Errors returned
/// here should match PostgreSQL error messages where possible.
///
/// Currently this is a **validation-only** pass that catches obvious errors
/// (wrong function arg counts, type mismatches in LIMIT/WHERE contexts).
/// Table/column existence is still checked at execution time to avoid
/// breaking the existing plan-then-execute contract.
pub fn analyze(statement: &Statement, search_path: &SearchPath) -> Result<(), EngineError> {
    match statement {
        Statement::Query(query) => analyze_query(query, search_path),
        Statement::Insert(insert) => {
            // Validate the source query if present
            if let crate::parser::ast::InsertSource::Query(query) = &insert.source {
                analyze_query(query, search_path)?;
            }
            Ok(())
        }
        Statement::Update(update) => {
            if let Some(ref where_clause) = update.where_clause {
                types::check_expr_for_boolean_context(where_clause)?;
            }
            Ok(())
        }
        Statement::Delete(delete) => {
            if let Some(ref where_clause) = delete.where_clause {
                types::check_expr_for_boolean_context(where_clause)?;
            }
            Ok(())
        }
        // For DDL and other statements, analysis is handled downstream
        _ => Ok(()),
    }
}

/// Analyze a top-level query (SELECT / set operations / CTEs).
fn analyze_query(query: &Query, search_path: &SearchPath) -> Result<(), EngineError> {
    // Collect CTE names for binding resolution
    let mut cte_names: Vec<String> = Vec::new();
    if let Some(ref with_clause) = query.with {
        for cte in &with_clause.ctes {
            cte_names.push(cte.name.to_lowercase());
        }
        // Analyze each CTE's sub-query
        for cte in &with_clause.ctes {
            analyze_query(&cte.query, search_path)?;
        }
    }

    analyze_query_expr(&query.body, search_path, &cte_names)?;

    // Validate LIMIT/OFFSET expressions if present
    if let Some(ref limit) = query.limit {
        types::check_expr_is_numeric(limit)?;
    }
    if let Some(ref offset) = query.offset {
        types::check_expr_is_numeric(offset)?;
    }

    Ok(())
}

/// Analyze a query expression (SELECT body or set operation).
fn analyze_query_expr(
    expr: &QueryExpr,
    search_path: &SearchPath,
    cte_names: &[String],
) -> Result<(), EngineError> {
    match expr {
        QueryExpr::Select(select) => {
            let mut ctx = BindingContext::new(search_path.clone(), cte_names.to_vec());
            binding::resolve_select(&mut ctx, select)?;
            // Check WHERE clause is boolean-compatible
            if let Some(ref where_clause) = select.where_clause {
                types::check_expr_for_boolean_context(where_clause)?;
            }
            if let Some(ref having) = select.having {
                types::check_expr_for_boolean_context(having)?;
            }
            functions::check_select_functions(select)?;
        }
        QueryExpr::SetOperation { left, right, .. } => {
            analyze_query_expr(left, search_path, cte_names)?;
            analyze_query_expr(right, search_path, cte_names)?;
        }
        QueryExpr::Nested(query) => {
            analyze_query(query, search_path)?;
        }
        QueryExpr::Values(_rows) => {
            // VALUES query - no special analysis needed for now
            // Types will be inferred at execution time
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
            return Err(EngineError {
                message: "data-modifying statements in WITH are not yet fully supported"
                    .to_string(),
            });
        }
    }
    Ok(())
}
