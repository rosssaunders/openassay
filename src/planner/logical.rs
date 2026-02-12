use std::collections::HashSet;

use crate::parser::ast::{
    Expr, GroupByExpr, JoinCondition, JoinExpr, JoinType, OrderByExpr, Query, QueryExpr,
    SelectItem, SelectQuantifier, SelectStatement, SetOperator, SetQuantifier, TableExpression,
    TableFunctionRef, TableRef,
};

#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub table: TableRef,
}

#[derive(Debug, Clone)]
pub struct LogicalFunctionScan {
    pub function: TableFunctionRef,
}

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub kind: JoinType,
    pub condition: Option<JoinCondition>,
    pub natural: bool,
}

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub predicate: Expr,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalProject {
    pub targets: Vec<SelectItem>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalAggregate {
    pub group_by: Vec<GroupByExpr>,
    pub having: Option<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSort {
    pub order_by: Vec<OrderByExpr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalCteBinding {
    pub name: String,
    pub plan: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalCte {
    pub recursive: bool,
    pub ctes: Vec<LogicalCteBinding>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalCteScan {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogicalWindow {
    pub expressions: Vec<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSetOp {
    pub op: SetOperator,
    pub quantifier: SetQuantifier,
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalDistinct {
    pub on: Vec<Expr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LogicalSubquery {
    pub plan: Box<LogicalPlan>,
    pub alias: Option<String>,
    pub lateral: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Result,
    Scan(LogicalScan),
    FunctionScan(LogicalFunctionScan),
    CteScan(LogicalCteScan),
    Subquery(LogicalSubquery),
    Join(LogicalJoin),
    Filter(LogicalFilter),
    Aggregate(LogicalAggregate),
    Window(LogicalWindow),
    Project(LogicalProject),
    Distinct(LogicalDistinct),
    Sort(LogicalSort),
    Limit(LogicalLimit),
    Cte(LogicalCte),
    SetOp(LogicalSetOp),
}

pub fn build_logical_plan(query: &Query) -> LogicalPlan {
    let mut ctx = BuildContext::default();
    build_logical_plan_with_ctx(query, &mut ctx)
}

fn build_logical_plan_with_ctx(query: &Query, ctx: &mut BuildContext) -> LogicalPlan {
    let with_clause = query.with.as_ref();
    if let Some(with) = with_clause {
        ctx.push_cte_scope(with.ctes.iter().map(|cte| cte.name.clone()));
        let ctes = with
            .ctes
            .iter()
            .map(|cte| LogicalCteBinding {
                name: cte.name.clone(),
                plan: Box::new(build_logical_plan_with_ctx(&cte.query, ctx)),
            })
            .collect();
        let mut plan = build_query_expr(&query.body, ctx);
        plan = apply_query_clauses(plan, query);
        ctx.pop_cte_scope();
        return LogicalPlan::Cte(LogicalCte {
            recursive: with.recursive,
            ctes,
            input: Box::new(plan),
        });
    }

    let plan = build_query_expr(&query.body, ctx);
    apply_query_clauses(plan, query)
}

fn apply_query_clauses(mut plan: LogicalPlan, query: &Query) -> LogicalPlan {
    let order_window_exprs = collect_window_exprs_from_order_by(&query.order_by);
    if !order_window_exprs.is_empty() && !plan_has_window(&plan) {
        plan = LogicalPlan::Window(LogicalWindow {
            expressions: order_window_exprs,
            input: Box::new(plan),
        });
    }

    if !query.order_by.is_empty() {
        plan = LogicalPlan::Sort(LogicalSort {
            order_by: query.order_by.clone(),
            input: Box::new(plan),
        });
    }

    if query.limit.is_some() || query.offset.is_some() {
        plan = LogicalPlan::Limit(LogicalLimit {
            limit: query.limit.clone(),
            offset: query.offset.clone(),
            input: Box::new(plan),
        });
    }

    plan
}

fn build_query_expr(expr: &QueryExpr, ctx: &mut BuildContext) -> LogicalPlan {
    match expr {
        QueryExpr::Select(select) => build_select_plan(select, ctx),
        QueryExpr::Nested(inner) => build_logical_plan_with_ctx(inner, ctx),
        QueryExpr::SetOperation {
            left,
            op,
            quantifier,
            right,
        } => LogicalPlan::SetOp(LogicalSetOp {
            op: *op,
            quantifier: *quantifier,
            left: Box::new(build_query_expr(left, ctx)),
            right: Box::new(build_query_expr(right, ctx)),
        }),
        QueryExpr::Values(_rows) => {
            // VALUES query - just return Result plan for now
            // The executor will handle the actual evaluation
            LogicalPlan::Result
        }
    }
}

fn build_select_plan(select: &SelectStatement, ctx: &mut BuildContext) -> LogicalPlan {
    let mut plan = if select.from.is_empty() {
        LogicalPlan::Result
    } else {
        build_from_clause(&select.from, ctx)
    };

    if let Some(predicate) = &select.where_clause {
        plan = LogicalPlan::Filter(LogicalFilter {
            predicate: predicate.clone(),
            input: Box::new(plan),
        });
    }

    if !select.group_by.is_empty() || select.having.is_some() {
        plan = LogicalPlan::Aggregate(LogicalAggregate {
            group_by: select.group_by.clone(),
            having: select.having.clone(),
            input: Box::new(plan),
        });
    }

    let window_exprs = collect_window_exprs_from_targets(&select.targets);
    if !window_exprs.is_empty() {
        plan = LogicalPlan::Window(LogicalWindow {
            expressions: window_exprs,
            input: Box::new(plan),
        });
    }

    plan = LogicalPlan::Project(LogicalProject {
        targets: select.targets.clone(),
        input: Box::new(plan),
    });

    if matches!(select.quantifier, Some(SelectQuantifier::Distinct))
        || !select.distinct_on.is_empty()
    {
        plan = LogicalPlan::Distinct(LogicalDistinct {
            on: select.distinct_on.clone(),
            input: Box::new(plan),
        });
    }

    plan
}

fn build_from_clause(from: &[TableExpression], ctx: &mut BuildContext) -> LogicalPlan {
    let mut iter = from.iter();
    let Some(first) = iter.next() else {
        return LogicalPlan::Result;
    };
    let mut plan = build_table_expression(first, ctx);
    for item in iter {
        let right = build_table_expression(item, ctx);
        plan = LogicalPlan::Join(LogicalJoin {
            left: Box::new(plan),
            right: Box::new(right),
            kind: JoinType::Cross,
            condition: None,
            natural: false,
        });
    }
    plan
}

fn build_table_expression(table: &TableExpression, ctx: &mut BuildContext) -> LogicalPlan {
    match table {
        TableExpression::Relation(rel) => ctx
            .cte_name_for_table(rel)
            .map(|name| LogicalPlan::CteScan(LogicalCteScan {
                name,
                alias: rel.alias.clone(),
            }))
            .unwrap_or_else(|| LogicalPlan::Scan(LogicalScan { table: rel.clone() })),
        TableExpression::Function(function) => LogicalPlan::FunctionScan(LogicalFunctionScan {
            function: function.clone(),
        }),
        TableExpression::Subquery(subquery) => LogicalPlan::Subquery(LogicalSubquery {
            plan: Box::new(build_logical_plan_with_ctx(&subquery.query, ctx)),
            alias: subquery.alias.clone(),
            lateral: subquery.lateral,
        }),
        TableExpression::Join(join) => build_join_expression(join, ctx),
    }
}

fn build_join_expression(join: &JoinExpr, ctx: &mut BuildContext) -> LogicalPlan {
    LogicalPlan::Join(LogicalJoin {
        left: Box::new(build_table_expression(&join.left, ctx)),
        right: Box::new(build_table_expression(&join.right, ctx)),
        kind: join.kind,
        condition: join.condition.clone(),
        natural: join.natural,
    })
}

#[derive(Default)]
struct BuildContext {
    cte_scopes: Vec<HashSet<String>>,
}

impl BuildContext {
    fn push_cte_scope<I>(&mut self, names: I)
    where
        I: IntoIterator<Item = String>,
    {
        let scope = names
            .into_iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        self.cte_scopes.push(scope);
    }

    fn pop_cte_scope(&mut self) {
        self.cte_scopes.pop();
    }

    fn is_cte_name(&self, name: &str) -> bool {
        self.cte_scopes
            .iter()
            .rev()
            .any(|scope| scope.contains(&name.to_ascii_lowercase()))
    }

    fn cte_name_for_table(&self, table: &TableRef) -> Option<String> {
        if table.name.len() == 1 {
            let name = &table.name[0];
            if self.is_cte_name(name) {
                return Some(name.to_ascii_lowercase());
            }
        }
        None
    }
}

fn collect_window_exprs_from_targets(targets: &[SelectItem]) -> Vec<Expr> {
    let mut out = Vec::new();
    for target in targets {
        collect_window_exprs(&target.expr, &mut out);
    }
    out
}

fn collect_window_exprs_from_order_by(order_by: &[OrderByExpr]) -> Vec<Expr> {
    let mut out = Vec::new();
    for expr in order_by {
        collect_window_exprs(&expr.expr, &mut out);
    }
    out
}

fn collect_window_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::FunctionCall { over: Some(_), .. } => out.push(expr.clone()),
        Expr::FunctionCall { args, filter, .. } => {
            for arg in args {
                collect_window_exprs(arg, out);
            }
            if let Some(filter) = filter.as_deref() {
                collect_window_exprs(filter, out);
            }
        }
        Expr::Cast { expr, .. } => collect_window_exprs(expr, out),
        Expr::Unary { expr, .. } => collect_window_exprs(expr, out),
        Expr::Binary { left, right, .. } => {
            collect_window_exprs(left, out);
            collect_window_exprs(right, out);
        }
        Expr::AnyAll { left, right, .. } => {
            collect_window_exprs(left, out);
            collect_window_exprs(right, out);
        }
        Expr::Exists(query) => collect_window_exprs_from_query(query, out),
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) => {
            collect_window_exprs_from_query(query, out);
        }
        Expr::ArrayConstructor(values) => {
            for value in values {
                collect_window_exprs(value, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_window_exprs(expr, out);
            for value in list {
                collect_window_exprs(value, out);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_window_exprs(expr, out);
            collect_window_exprs_from_query(subquery, out);
        }
        Expr::Between { expr, low, high, .. } => {
            collect_window_exprs(expr, out);
            collect_window_exprs(low, out);
            collect_window_exprs(high, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_window_exprs(expr, out);
            collect_window_exprs(pattern, out);
        }
        Expr::IsNull { expr, .. } => collect_window_exprs(expr, out),
        Expr::IsDistinctFrom { left, right, .. } => {
            collect_window_exprs(left, out);
            collect_window_exprs(right, out);
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            collect_window_exprs(operand, out);
            for (when, then) in when_then {
                collect_window_exprs(when, out);
                collect_window_exprs(then, out);
            }
            if let Some(expr) = else_expr.as_deref() {
                collect_window_exprs(expr, out);
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                collect_window_exprs(when, out);
                collect_window_exprs(then, out);
            }
            if let Some(expr) = else_expr.as_deref() {
                collect_window_exprs(expr, out);
            }
        }
        Expr::Identifier(_)
        | Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Parameter(_)
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::TypedLiteral { .. } => {}
        Expr::ArraySubscript { expr, index } => {
            collect_window_exprs(expr, out);
            collect_window_exprs(index, out);
        }
        Expr::ArraySlice { expr, start, end } => {
            collect_window_exprs(expr, out);
            if let Some(start) = start {
                collect_window_exprs(start, out);
            }
            if let Some(end) = end {
                collect_window_exprs(end, out);
            }
        }
    }
}

fn collect_window_exprs_from_query(query: &Query, out: &mut Vec<Expr>) {
    match &query.body {
        QueryExpr::Select(select) => {
            for target in &select.targets {
                collect_window_exprs(&target.expr, out);
            }
            if let Some(predicate) = &select.where_clause {
                collect_window_exprs(predicate, out);
            }
            if let Some(having) = &select.having {
                collect_window_exprs(having, out);
            }
        }
        QueryExpr::SetOperation { left, right, .. } => {
            collect_window_exprs_from_query_expr(left, out);
            collect_window_exprs_from_query_expr(right, out);
        }
        QueryExpr::Nested(inner) => collect_window_exprs_from_query(inner, out),
        QueryExpr::Values(rows) => {
            // Collect window expressions from all value expressions
            for row in rows {
                for expr in row {
                    collect_window_exprs(expr, out);
                }
            }
        }
    }
}

fn collect_window_exprs_from_query_expr(expr: &QueryExpr, out: &mut Vec<Expr>) {
    match expr {
        QueryExpr::Select(select) => {
            for target in &select.targets {
                collect_window_exprs(&target.expr, out);
            }
        }
        QueryExpr::SetOperation { left, right, .. } => {
            collect_window_exprs_from_query_expr(left, out);
            collect_window_exprs_from_query_expr(right, out);
        }
        QueryExpr::Nested(inner) => collect_window_exprs_from_query(inner, out),
        QueryExpr::Values(rows) => {
            // Collect window expressions from all value expressions
            for row in rows {
                for expr in row {
                    collect_window_exprs(expr, out);
                }
            }
        }
    }
}

fn plan_has_window(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Window(_) => true,
        LogicalPlan::Join(join) => plan_has_window(&join.left) || plan_has_window(&join.right),
        LogicalPlan::Filter(filter) => plan_has_window(&filter.input),
        LogicalPlan::Aggregate(aggregate) => plan_has_window(&aggregate.input),
        LogicalPlan::Project(project) => plan_has_window(&project.input),
        LogicalPlan::Distinct(distinct) => plan_has_window(&distinct.input),
        LogicalPlan::Sort(sort) => plan_has_window(&sort.input),
        LogicalPlan::Limit(limit) => plan_has_window(&limit.input),
        LogicalPlan::SetOp(set_op) => {
            plan_has_window(&set_op.left) || plan_has_window(&set_op.right)
        }
        LogicalPlan::Subquery(_) => false,
        LogicalPlan::Cte(cte) => plan_has_window(&cte.input),
        LogicalPlan::Result
        | LogicalPlan::Scan(_)
        | LogicalPlan::FunctionScan(_)
        | LogicalPlan::CteScan(_) => false,
    }
}
