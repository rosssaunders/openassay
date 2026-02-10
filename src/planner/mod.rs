use std::fmt;

use crate::parser::ast::{
    DeleteStatement, Expr, InsertSource, InsertStatement, Query, QueryExpr, SelectItem,
    SelectStatement, Statement, UpdateStatement,
};

pub mod cost;
pub mod logical;
pub mod physical;
pub mod stats;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerError {
    pub message: String,
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PlannerError {}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub query: Query,
    pub logical: logical::LogicalPlan,
    pub physical: physical::PhysicalPlan,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub statement: InsertStatement,
    pub source_plan: Option<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub statement: UpdateStatement,
    pub source_plan: Option<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub statement: DeleteStatement,
    pub source_plan: Option<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct MergePlan {
    pub statement: crate::parser::ast::MergeStatement,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PlanNode {
    Query(QueryPlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Merge(MergePlan),
    PassThrough(Statement),
}

pub fn plan(statement: &Statement) -> PlanNode {
    match statement {
        Statement::Query(query) => plan_query_with_fallback(statement, query),
        Statement::Insert(insert) => plan_insert_with_fallback(statement, insert),
        Statement::Update(update) => plan_update_with_fallback(statement, update),
        Statement::Delete(delete) => plan_delete_with_fallback(statement, delete),
        Statement::Merge(merge) => PlanNode::Merge(MergePlan {
            statement: merge.clone(),
        }),
        _ => PlanNode::PassThrough(statement.clone()),
    }
}

pub fn plan_query(query: &Query) -> Result<QueryPlan, PlannerError> {
    let logical = logical::build_logical_plan(query);
    let physical = physical::plan_physical(&logical)?;
    Ok(QueryPlan {
        query: query.clone(),
        logical,
        physical,
    })
}

fn plan_insert(insert: &InsertStatement) -> Result<InsertPlan, PlannerError> {
    let source_plan = match &insert.source {
        InsertSource::Query(query) => Some(plan_query(query)?),
        InsertSource::Values(_) => None,
    };
    Ok(InsertPlan {
        statement: insert.clone(),
        source_plan,
    })
}

fn plan_update(update: &UpdateStatement) -> Result<UpdatePlan, PlannerError> {
    let source_plan = if update.from.is_empty() {
        None
    } else {
        Some(plan_query(&build_update_source_query(update))?)
    };
    Ok(UpdatePlan {
        statement: update.clone(),
        source_plan,
    })
}

fn plan_delete(delete: &DeleteStatement) -> Result<DeletePlan, PlannerError> {
    let source_plan = if delete.using.is_empty() {
        None
    } else {
        Some(plan_query(&build_delete_source_query(delete))?)
    };
    Ok(DeletePlan {
        statement: delete.clone(),
        source_plan,
    })
}

fn build_update_source_query(update: &UpdateStatement) -> Query {
    let from = update.from.clone();
    Query {
        with: None,
        body: QueryExpr::Select(SelectStatement {
            quantifier: None,
            distinct_on: Vec::new(),
            targets: vec![SelectItem {
                expr: Expr::Wildcard,
                alias: None,
            }],
            from,
            where_clause: update.where_clause.clone(),
            group_by: Vec::new(),
            having: None,
        }),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

fn build_delete_source_query(delete: &DeleteStatement) -> Query {
    let from = delete.using.clone();
    Query {
        with: None,
        body: QueryExpr::Select(SelectStatement {
            quantifier: None,
            distinct_on: Vec::new(),
            targets: vec![SelectItem {
                expr: Expr::Wildcard,
                alias: None,
            }],
            from,
            where_clause: delete.where_clause.clone(),
            group_by: Vec::new(),
            having: None,
        }),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

impl PlanNode {
    pub fn explain(&self, lines: &mut Vec<String>, indent: usize) {
        match self {
            PlanNode::Query(plan) => plan.physical.explain(lines, indent),
            PlanNode::Insert(_) => physical::explain_leaf(lines, indent, "Insert"),
            PlanNode::Update(_) => physical::explain_leaf(lines, indent, "Update"),
            PlanNode::Delete(_) => physical::explain_leaf(lines, indent, "Delete"),
            PlanNode::Merge(_) => physical::explain_leaf(lines, indent, "Merge"),
            PlanNode::PassThrough(_) => physical::explain_leaf(lines, indent, "PassThrough"),
        }
    }
}

fn plan_query_with_fallback(statement: &Statement, query: &Query) -> PlanNode {
    if !should_plan_query(query) {
        return PlanNode::PassThrough(statement.clone());
    }
    match plan_query(query) {
        Ok(plan) => PlanNode::Query(plan),
        Err(_) => PlanNode::PassThrough(statement.clone()),
    }
}

fn plan_insert_with_fallback(statement: &Statement, insert: &InsertStatement) -> PlanNode {
    if let InsertSource::Query(query) = &insert.source
        && !should_plan_query(query)
    {
        return PlanNode::PassThrough(statement.clone());
    }
    match plan_insert(insert) {
        Ok(plan) => PlanNode::Insert(plan),
        Err(_) => PlanNode::PassThrough(statement.clone()),
    }
}

fn plan_update_with_fallback(statement: &Statement, update: &UpdateStatement) -> PlanNode {
    if !update.from.is_empty() {
        let query = build_update_source_query(update);
        if !should_plan_query(&query) {
            return PlanNode::PassThrough(statement.clone());
        }
    }
    match plan_update(update) {
        Ok(plan) => PlanNode::Update(plan),
        Err(_) => PlanNode::PassThrough(statement.clone()),
    }
}

fn plan_delete_with_fallback(statement: &Statement, delete: &DeleteStatement) -> PlanNode {
    if !delete.using.is_empty() {
        let query = build_delete_source_query(delete);
        if !should_plan_query(&query) {
            return PlanNode::PassThrough(statement.clone());
        }
    }
    match plan_delete(delete) {
        Ok(plan) => PlanNode::Delete(plan),
        Err(_) => PlanNode::PassThrough(statement.clone()),
    }
}

fn should_plan_query(query: &Query) -> bool {
    match &query.body {
        QueryExpr::Select(select) => should_plan_select(select),
        _ => true,
    }
}

fn should_plan_select(_select: &SelectStatement) -> bool {
    true
}
