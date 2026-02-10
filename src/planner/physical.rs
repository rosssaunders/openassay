use std::collections::HashMap;

use crate::catalog::IndexSpec;
use crate::parser::ast::{
    Expr, GroupByExpr, JoinCondition, JoinType, OrderByExpr, SelectItem, SetOperator,
    SetQuantifier, TableFunctionRef, TableRef,
};

use super::cost::{
    self, JoinStrategy, PlanCost,
};
use super::logical::{
    LogicalAggregate, LogicalCte, LogicalCteScan, LogicalDistinct, LogicalFilter, LogicalJoin,
    LogicalLimit, LogicalPlan, LogicalProject, LogicalScan, LogicalSetOp, LogicalSort, LogicalWindow,
};
use super::stats::{self, TableStats};
use super::PlannerError;

#[derive(Debug, Clone)]
pub enum ScanType {
    Seq,
    Index(IndexSpec),
}

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table: TableRef,
    pub filter: Option<Expr>,
    pub scan_type: ScanType,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct FunctionScanPlan {
    pub function: TableFunctionRef,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct SubqueryPlan {
    pub plan: Box<PhysicalPlan>,
    pub alias: Option<String>,
    pub lateral: bool,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub predicate: Expr,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct ProjectPlan {
    pub targets: Vec<SelectItem>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct AggregatePlan {
    pub group_by: Vec<GroupByExpr>,
    pub having: Option<Expr>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct DistinctPlan {
    pub on: Vec<Expr>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct SortPlan {
    pub order_by: Vec<OrderByExpr>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct LimitPlan {
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct CtePlanBinding {
    pub name: String,
    pub plan: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct CtePlan {
    pub recursive: bool,
    pub ctes: Vec<CtePlanBinding>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct CteScanPlan {
    pub name: String,
    pub alias: Option<String>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct WindowPlan {
    pub expressions: Vec<Expr>,
    pub input: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<JoinCondition>,
    pub natural: bool,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct SetOpPlan {
    pub op: SetOperator,
    pub quantifier: SetQuantifier,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub struct ResultPlan {
    pub cost: PlanCost,
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Result(ResultPlan),
    Scan(ScanPlan),
    FunctionScan(FunctionScanPlan),
    CteScan(CteScanPlan),
    Subquery(SubqueryPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Aggregate(AggregatePlan),
    Window(WindowPlan),
    Distinct(DistinctPlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    Cte(CtePlan),
    SetOp(SetOpPlan),
    HashJoin(JoinPlan),
    NestedLoopJoin(JoinPlan),
}

impl PhysicalPlan {
    pub fn cost(&self) -> PlanCost {
        match self {
            PhysicalPlan::Result(plan) => plan.cost,
            PhysicalPlan::Scan(plan) => plan.cost,
            PhysicalPlan::FunctionScan(plan) => plan.cost,
            PhysicalPlan::CteScan(plan) => plan.cost,
            PhysicalPlan::Subquery(plan) => plan.cost,
            PhysicalPlan::Filter(plan) => plan.cost,
            PhysicalPlan::Project(plan) => plan.cost,
            PhysicalPlan::Aggregate(plan) => plan.cost,
            PhysicalPlan::Window(plan) => plan.cost,
            PhysicalPlan::Distinct(plan) => plan.cost,
            PhysicalPlan::Sort(plan) => plan.cost,
            PhysicalPlan::Limit(plan) => plan.cost,
            PhysicalPlan::Cte(plan) => plan.cost,
            PhysicalPlan::SetOp(plan) => plan.cost,
            PhysicalPlan::HashJoin(plan) => plan.cost,
            PhysicalPlan::NestedLoopJoin(plan) => plan.cost,
        }
    }

    pub fn explain(&self, lines: &mut Vec<String>, indent: usize) {
        let prefix = " ".repeat(indent);
        match self {
            PhysicalPlan::Result(plan) => {
                lines.push(format!(
                    "{}Result  ({})",
                    prefix,
                    format_cost(plan.cost)
                ));
            }
            PhysicalPlan::Scan(plan) => {
                let label = match &plan.scan_type {
                    ScanType::Seq => "Seq Scan",
                    ScanType::Index(index) => {
                        let name = index.name.clone();
                        lines.push(format!("{}Index Cond: <predicate>", prefix));
                        return lines.push(format!(
                            "{}Index Scan using {} on {}  ({})",
                            prefix,
                            name,
                            plan.table.name.join("."),
                            format_cost(plan.cost)
                        ));
                    }
                };
                lines.push(format!(
                    "{}{} on {}  ({})",
                    prefix,
                    label,
                    plan.table.name.join("."),
                    format_cost(plan.cost)
                ));
                if plan.filter.is_some() {
                    lines.push(format!("{}  Filter: <predicate>", prefix));
                }
            }
            PhysicalPlan::FunctionScan(plan) => {
                lines.push(format!(
                    "{}Function Scan {}  ({})",
                    prefix,
                    plan.function.name.join("."),
                    format_cost(plan.cost)
                ));
            }
            PhysicalPlan::CteScan(plan) => {
                let label = plan.alias.as_deref().unwrap_or(&plan.name);
                lines.push(format!(
                    "{}CTE Scan on {}  ({})",
                    prefix,
                    label,
                    format_cost(plan.cost)
                ));
            }
            PhysicalPlan::Subquery(plan) => {
                lines.push(format!("{}Subquery Scan  ({})", prefix, format_cost(plan.cost)));
                plan.plan.explain(lines, indent + 2);
            }
            PhysicalPlan::Filter(plan) => {
                lines.push(format!("{}Filter  ({})", prefix, format_cost(plan.cost)));
                lines.push(format!("{}  Filter: <predicate>", prefix));
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Project(plan) => {
                lines.push(format!("{}Result  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Aggregate(plan) => {
                lines.push(format!("{}Aggregate  ({})", prefix, format_cost(plan.cost)));
                if !plan.group_by.is_empty() {
                    lines.push(format!("{}  Group Key: <keys>", prefix));
                }
                if plan.having.is_some() {
                    lines.push(format!("{}  Filter: <having>", prefix));
                }
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Window(plan) => {
                lines.push(format!("{}Window  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Distinct(plan) => {
                lines.push(format!("{}Unique  ({})", prefix, format_cost(plan.cost)));
                if !plan.on.is_empty() {
                    lines.push(format!("{}  Unique Key: <keys>", prefix));
                }
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Sort(plan) => {
                lines.push(format!("{}Sort  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Limit(plan) => {
                lines.push(format!("{}Limit  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::Cte(plan) => {
                lines.push(format!("{}CTE  ({})", prefix, format_cost(plan.cost)));
                for cte in &plan.ctes {
                    lines.push(format!(
                        "{}  CTE {}  ({})",
                        prefix,
                        cte.name,
                        format_cost(cte.plan.cost())
                    ));
                    cte.plan.explain(lines, indent + 4);
                }
                plan.input.explain(lines, indent + 2);
            }
            PhysicalPlan::SetOp(plan) => {
                lines.push(format!(
                    "{}{op:?}  ({cost})",
                    prefix,
                    op = plan.op,
                    cost = format_cost(plan.cost)
                ));
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
            PhysicalPlan::HashJoin(plan) => {
                lines.push(format!("{}Hash Join  ({})", prefix, format_cost(plan.cost)));
                if plan.condition.is_some() || plan.natural {
                    lines.push(format!("{}  Hash Cond: <condition>", prefix));
                }
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
            PhysicalPlan::NestedLoopJoin(plan) => {
                lines.push(format!("{}Nested Loop  ({})", prefix, format_cost(plan.cost)));
                if plan.condition.is_some() || plan.natural {
                    lines.push(format!("{}  Join Filter: <condition>", prefix));
                }
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
        }
    }
}

pub fn explain_leaf(lines: &mut Vec<String>, indent: usize, label: &str) {
    let prefix = " ".repeat(indent);
    lines.push(format!("{}{label}  (cost=0.00..0.01 rows=1 width=0)", prefix));
}

pub fn plan_physical(logical: &LogicalPlan) -> Result<PhysicalPlan, PlannerError> {
    let mut ctx = PlannerContext::default();
    plan_with_context(logical, &mut ctx)
}

#[derive(Default)]
struct PlannerContext {
    stats_cache: HashMap<String, TableStats>,
    cte_costs: Vec<HashMap<String, PlanCost>>,
}

impl PlannerContext {
    fn stats_for_table(&mut self, table: &TableRef) -> Result<TableStats, PlannerError> {
        let key = table.name.join(".");
        if let Some(stats) = self.stats_cache.get(&key) {
            return Ok(stats.clone());
        }
        let stats = stats::table_stats(table)?;
        self.stats_cache.insert(key, stats.clone());
        Ok(stats)
    }

    fn push_cte_scope<I>(&mut self, names: I)
    where
        I: IntoIterator<Item = String>,
    {
        let mut scope = HashMap::new();
        for name in names {
            scope.insert(name.to_ascii_lowercase(), PlanCost::new(1.0, 0.0, 1.0));
        }
        self.cte_costs.push(scope);
    }

    fn pop_cte_scope(&mut self) {
        self.cte_costs.pop();
    }

    fn update_cte_cost(&mut self, name: &str, cost: PlanCost) {
        if let Some(scope) = self.cte_costs.last_mut() {
            scope.insert(name.to_ascii_lowercase(), cost);
        }
    }

    fn cte_cost(&self, name: &str) -> Option<PlanCost> {
        let key = name.to_ascii_lowercase();
        self.cte_costs
            .iter()
            .rev()
            .find_map(|scope| scope.get(&key).copied())
    }
}

fn plan_with_context(logical: &LogicalPlan, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    match logical {
        LogicalPlan::Result => Ok(PhysicalPlan::Result(ResultPlan {
            cost: PlanCost::new(1.0, 0.0, 1.0),
        })),
        LogicalPlan::Scan(scan) => plan_scan(scan, None, ctx),
        LogicalPlan::FunctionScan(scan) => Ok(PhysicalPlan::FunctionScan(FunctionScanPlan {
            function: scan.function.clone(),
            cost: PlanCost::new(1.0, 0.0, 1.0),
        })),
        LogicalPlan::CteScan(scan) => plan_cte_scan(scan, ctx),
        LogicalPlan::Subquery(subquery) => {
            let planned = plan_with_context(&subquery.plan, ctx)?;
            let cost = planned.cost();
            Ok(PhysicalPlan::Subquery(SubqueryPlan {
                plan: Box::new(planned),
                alias: subquery.alias.clone(),
                lateral: subquery.lateral,
                cost,
            }))
        }
        LogicalPlan::Filter(filter) => plan_filter(filter, ctx),
        LogicalPlan::Project(project) => plan_project(project, ctx),
        LogicalPlan::Aggregate(aggregate) => plan_aggregate(aggregate, ctx),
        LogicalPlan::Window(window) => plan_window(window, ctx),
        LogicalPlan::Distinct(distinct) => plan_distinct(distinct, ctx),
        LogicalPlan::Sort(sort) => plan_sort(sort, ctx),
        LogicalPlan::Limit(limit) => plan_limit(limit, ctx),
        LogicalPlan::Cte(cte) => plan_cte(cte, ctx),
        LogicalPlan::SetOp(set_op) => plan_set_op(set_op, ctx),
        LogicalPlan::Join(join) => plan_join(join, ctx),
    }
}

fn plan_scan(scan: &LogicalScan, filter: Option<&Expr>, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let stats = ctx.stats_for_table(&scan.table)?;
    let row_count = stats.row_count as f64;
    let mut selectivity = filter
        .map(|pred| estimate_filter_selectivity(pred, Some(&stats)))
        .unwrap_or(1.0);
    if selectivity <= 0.0 {
        selectivity = 0.01;
    }
    let index = filter.and_then(|pred| index_for_filter(pred, &stats));
    let base_cost = match &index {
        Some(_) => cost::index_scan_cost(row_count),
        None => cost::seq_scan_cost(row_count),
    };
    let cost = apply_selectivity(base_cost, selectivity);
    let scan_type = match index {
        Some(index) => ScanType::Index(index),
        None => ScanType::Seq,
    };
    Ok(PhysicalPlan::Scan(ScanPlan {
        table: scan.table.clone(),
        filter: filter.cloned(),
        scan_type,
        cost,
    }))
}

fn plan_filter(filter: &LogicalFilter, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    if let LogicalPlan::Scan(scan) = &*filter.input {
        return plan_scan(scan, Some(&filter.predicate), ctx);
    }

    let input = plan_with_context(&filter.input, ctx)?;
    let selectivity = estimate_filter_selectivity(&filter.predicate, None);
    let cost = cost::filter_cost(input.cost(), selectivity);
    Ok(PhysicalPlan::Filter(FilterPlan {
        predicate: filter.predicate.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_project(project: &LogicalProject, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&project.input, ctx)?;
    let cost = cost::project_cost(input.cost());
    Ok(PhysicalPlan::Project(ProjectPlan {
        targets: project.targets.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_aggregate(aggregate: &LogicalAggregate, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&aggregate.input, ctx)?;
    let groups = if aggregate.group_by.is_empty() {
        1.0
    } else {
        input.cost().rows * 0.1
    };
    let cost = cost::aggregate_cost(input.cost(), groups);
    Ok(PhysicalPlan::Aggregate(AggregatePlan {
        group_by: aggregate.group_by.clone(),
        having: aggregate.having.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_distinct(distinct: &LogicalDistinct, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&distinct.input, ctx)?;
    let cost = cost::distinct_cost(input.cost());
    Ok(PhysicalPlan::Distinct(DistinctPlan {
        on: distinct.on.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_sort(sort: &LogicalSort, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&sort.input, ctx)?;
    let cost = cost::sort_cost(input.cost());
    Ok(PhysicalPlan::Sort(SortPlan {
        order_by: sort.order_by.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_limit(limit: &LogicalLimit, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&limit.input, ctx)?;
    let limit_value = limit.limit.as_ref().and_then(literal_f64);
    let cost = cost::limit_cost(input.cost(), limit_value);
    Ok(PhysicalPlan::Limit(LimitPlan {
        limit: limit.limit.clone(),
        offset: limit.offset.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_window(window: &LogicalWindow, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&window.input, ctx)?;
    let cost = cost::window_cost(input.cost());
    Ok(PhysicalPlan::Window(WindowPlan {
        expressions: window.expressions.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_cte_scan(scan: &LogicalCteScan, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let base_cost = ctx
        .cte_cost(&scan.name)
        .unwrap_or_else(|| PlanCost::new(1.0, 0.0, 1.0));
    let cost = cost::cte_scan_cost(base_cost);
    Ok(PhysicalPlan::CteScan(CteScanPlan {
        name: scan.name.clone(),
        alias: scan.alias.clone(),
        cost,
    }))
}

fn plan_cte(cte: &LogicalCte, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    ctx.push_cte_scope(cte.ctes.iter().map(|cte| cte.name.clone()));
    let result = (|| {
        let mut planned_ctes = Vec::with_capacity(cte.ctes.len());
        for binding in &cte.ctes {
            let plan = plan_with_context(&binding.plan, ctx)?;
            ctx.update_cte_cost(&binding.name, plan.cost());
            planned_ctes.push(CtePlanBinding {
                name: binding.name.clone(),
                plan: Box::new(plan),
            });
        }
        let input = plan_with_context(&cte.input, ctx)?;
        let cost = cost::cte_cost(input.cost(), planned_ctes.iter().map(|cte| cte.plan.cost()));
        Ok(PhysicalPlan::Cte(CtePlan {
            recursive: cte.recursive,
            ctes: planned_ctes,
            input: Box::new(input),
            cost,
        }))
    })();
    ctx.pop_cte_scope();
    result
}

fn plan_set_op(set_op: &LogicalSetOp, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let left = plan_with_context(&set_op.left, ctx)?;
    let right = plan_with_context(&set_op.right, ctx)?;
    let cost = cost::set_op_cost(left.cost(), right.cost());
    Ok(PhysicalPlan::SetOp(SetOpPlan {
        op: set_op.op,
        quantifier: set_op.quantifier,
        left: Box::new(left),
        right: Box::new(right),
        cost,
    }))
}

fn plan_join(join: &LogicalJoin, ctx: &mut PlannerContext) -> Result<PhysicalPlan, PlannerError> {
    let left = plan_with_context(&join.left, ctx)?;
    let right = plan_with_context(&join.right, ctx)?;
    let strategy = cost::choose_join_strategy(left.cost(), right.cost());
    let has_condition = join.condition.is_some() || join.natural;
    let cost = cost::join_cost(left.cost(), right.cost(), strategy, has_condition);
    let plan = JoinPlan {
        left: Box::new(left),
        right: Box::new(right),
        join_type: join.kind,
        condition: join.condition.clone(),
        natural: join.natural,
        cost,
    };
    Ok(match strategy {
        JoinStrategy::Hash => PhysicalPlan::HashJoin(plan),
        JoinStrategy::NestedLoop => PhysicalPlan::NestedLoopJoin(plan),
    })
}

fn apply_selectivity(cost: PlanCost, selectivity: f64) -> PlanCost {
    let rows = cost.rows * selectivity;
    PlanCost::new(rows, cost.startup_cost, cost.total_cost + rows * 0.1)
}

fn estimate_filter_selectivity(predicate: &Expr, stats: Option<&TableStats>) -> f64 {
    match predicate {
        Expr::Binary { op, left, right } => {
            if matches!(op, crate::parser::ast::BinaryOp::And) {
                return estimate_filter_selectivity(left, stats)
                    * estimate_filter_selectivity(right, stats);
            }
            if matches!(op, crate::parser::ast::BinaryOp::Or) {
                let left_sel = estimate_filter_selectivity(left, stats);
                let right_sel = estimate_filter_selectivity(right, stats);
                return 1.0 - (1.0 - left_sel) * (1.0 - right_sel);
            }
            if matches!(op, crate::parser::ast::BinaryOp::Eq) {
                return 0.1;
            }
            cost::default_filter_selectivity()
        }
        Expr::IsNull { expr, negated } => {
            if let Some(stats) = stats
                && let Some(column) = identifier_name(expr)
                && let Some(null_fraction) = stats.null_fraction(&column)
            {
                return if *negated { 1.0 - null_fraction } else { null_fraction };
            }
            if *negated { 0.9 } else { 0.1 }
        }
        _ => cost::default_filter_selectivity(),
    }
}

fn index_for_filter(predicate: &Expr, stats: &TableStats) -> Option<IndexSpec> {
    let column = indexable_column(predicate)?;
    stats.index_on_column(&column)
}

fn indexable_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Binary { op, left, right } => {
            if matches!(op, crate::parser::ast::BinaryOp::And) {
                return indexable_column(left).or_else(|| indexable_column(right));
            }
            if !matches!(op, crate::parser::ast::BinaryOp::Eq) {
                return None;
            }
            identifier_name(left).or_else(|| identifier_name(right))
        }
        _ => None,
    }
}

fn identifier_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(parts) => parts.last().cloned(),
        _ => None,
    }
}

fn literal_f64(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Integer(value) => Some(*value as f64),
        Expr::Float(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn format_cost(cost: PlanCost) -> String {
    format!(
        "cost={:.2}..{:.2} rows={} width=0",
        cost.startup_cost,
        cost.total_cost,
        cost.rows.round() as i64
    )
}
