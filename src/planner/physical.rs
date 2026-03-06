use std::collections::{HashMap, HashSet};

use crate::catalog::IndexSpec;
use crate::parser::ast::{
    Expr, GroupByExpr, JoinCondition, JoinType, OrderByExpr, Query, QueryExpr, SelectItem,
    SelectStatement, SetOperator, SetQuantifier, TableExpression, TableFunctionRef, TableRef,
    WindowFrameBound, WindowSpec,
};

use super::PlannerError;
use super::cost::{self, JoinStrategy, PlanCost};
use super::logical::{
    LogicalAggregate, LogicalCte, LogicalCteScan, LogicalDistinct, LogicalFilter, LogicalJoin,
    LogicalLimit, LogicalPlan, LogicalProject, LogicalScan, LogicalSetOp, LogicalSort,
    LogicalWindow,
};
use super::stats::{self, TableStats};

#[derive(Debug, Clone)]
pub enum ScanType {
    Seq,
    Index(IndexSpec),
}

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table: TableRef,
    pub filter: Option<Expr>,
    pub projected_columns: Option<Vec<usize>>,
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
            Self::Result(plan) => plan.cost,
            Self::Scan(plan) => plan.cost,
            Self::FunctionScan(plan) => plan.cost,
            Self::CteScan(plan) => plan.cost,
            Self::Subquery(plan) => plan.cost,
            Self::Filter(plan) => plan.cost,
            Self::Project(plan) => plan.cost,
            Self::Aggregate(plan) => plan.cost,
            Self::Window(plan) => plan.cost,
            Self::Distinct(plan) => plan.cost,
            Self::Sort(plan) => plan.cost,
            Self::Limit(plan) => plan.cost,
            Self::Cte(plan) => plan.cost,
            Self::SetOp(plan) => plan.cost,
            Self::HashJoin(plan) => plan.cost,
            Self::NestedLoopJoin(plan) => plan.cost,
        }
    }

    pub fn explain(&self, lines: &mut Vec<String>, indent: usize) {
        let prefix = " ".repeat(indent);
        match self {
            Self::Result(plan) => {
                lines.push(format!("{}Result  ({})", prefix, format_cost(plan.cost)));
            }
            Self::Scan(plan) => {
                let label = match &plan.scan_type {
                    ScanType::Seq => "Seq Scan",
                    ScanType::Index(index) => {
                        let name = index.name.clone();
                        lines.push(format!("{prefix}Index Cond: <predicate>"));
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
                    lines.push(format!("{prefix}  Filter: <predicate>"));
                }
            }
            Self::FunctionScan(plan) => {
                lines.push(format!(
                    "{}Function Scan {}  ({})",
                    prefix,
                    plan.function.name.join("."),
                    format_cost(plan.cost)
                ));
            }
            Self::CteScan(plan) => {
                let label = plan.alias.as_deref().unwrap_or(&plan.name);
                lines.push(format!(
                    "{}CTE Scan on {}  ({})",
                    prefix,
                    label,
                    format_cost(plan.cost)
                ));
            }
            Self::Subquery(plan) => {
                lines.push(format!(
                    "{}Subquery Scan  ({})",
                    prefix,
                    format_cost(plan.cost)
                ));
                plan.plan.explain(lines, indent + 2);
            }
            Self::Filter(plan) => {
                lines.push(format!("{}Filter  ({})", prefix, format_cost(plan.cost)));
                lines.push(format!("{prefix}  Filter: <predicate>"));
                plan.input.explain(lines, indent + 2);
            }
            Self::Project(plan) => {
                lines.push(format!("{}Result  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            Self::Aggregate(plan) => {
                lines.push(format!("{}Aggregate  ({})", prefix, format_cost(plan.cost)));
                if !plan.group_by.is_empty() {
                    lines.push(format!("{prefix}  Group Key: <keys>"));
                }
                if plan.having.is_some() {
                    lines.push(format!("{prefix}  Filter: <having>"));
                }
                plan.input.explain(lines, indent + 2);
            }
            Self::Window(plan) => {
                lines.push(format!("{}Window  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            Self::Distinct(plan) => {
                lines.push(format!("{}Unique  ({})", prefix, format_cost(plan.cost)));
                if !plan.on.is_empty() {
                    lines.push(format!("{prefix}  Unique Key: <keys>"));
                }
                plan.input.explain(lines, indent + 2);
            }
            Self::Sort(plan) => {
                lines.push(format!("{}Sort  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            Self::Limit(plan) => {
                lines.push(format!("{}Limit  ({})", prefix, format_cost(plan.cost)));
                plan.input.explain(lines, indent + 2);
            }
            Self::Cte(plan) => {
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
            Self::SetOp(plan) => {
                lines.push(format!(
                    "{}{op:?}  ({cost})",
                    prefix,
                    op = plan.op,
                    cost = format_cost(plan.cost)
                ));
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
            Self::HashJoin(plan) => {
                lines.push(format!("{}Hash Join  ({})", prefix, format_cost(plan.cost)));
                if plan.condition.is_some() || plan.natural {
                    lines.push(format!("{prefix}  Hash Cond: <condition>"));
                }
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
            Self::NestedLoopJoin(plan) => {
                lines.push(format!(
                    "{}Nested Loop  ({})",
                    prefix,
                    format_cost(plan.cost)
                ));
                if plan.condition.is_some() || plan.natural {
                    lines.push(format!("{prefix}  Join Filter: <condition>"));
                }
                plan.left.explain(lines, indent + 2);
                plan.right.explain(lines, indent + 2);
            }
        }
    }
}

pub fn explain_leaf(lines: &mut Vec<String>, indent: usize, label: &str) {
    let prefix = " ".repeat(indent);
    lines.push(format!("{prefix}{label}  (cost=0.00..0.01 rows=1 width=0)"));
}

pub fn plan_physical(logical: &LogicalPlan) -> Result<PhysicalPlan, PlannerError> {
    let mut ctx = PlannerContext::default();
    plan_with_context(logical, &mut ctx)
}

pub fn annotate_scan_projections(query: &Query, plan: &mut PhysicalPlan) {
    let projections = collect_query_scan_projections(query);
    assign_scan_projections(plan, &mut projections.into_iter());
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

fn plan_with_context(
    logical: &LogicalPlan,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_scan(
    scan: &LogicalScan,
    filter: Option<&Expr>,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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
        projected_columns: None,
        scan_type,
        cost,
    }))
}

fn plan_filter(
    filter: &LogicalFilter,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_project(
    project: &LogicalProject,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&project.input, ctx)?;
    let cost = cost::project_cost(input.cost());
    Ok(PhysicalPlan::Project(ProjectPlan {
        targets: project.targets.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_aggregate(
    aggregate: &LogicalAggregate,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_distinct(
    distinct: &LogicalDistinct,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_limit(
    limit: &LogicalLimit,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_window(
    window: &LogicalWindow,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
    let input = plan_with_context(&window.input, ctx)?;
    let cost = cost::window_cost(input.cost());
    Ok(PhysicalPlan::Window(WindowPlan {
        expressions: window.expressions.clone(),
        input: Box::new(input),
        cost,
    }))
}

fn plan_cte_scan(
    scan: &LogicalCteScan,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

fn plan_set_op(
    set_op: &LogicalSetOp,
    ctx: &mut PlannerContext,
) -> Result<PhysicalPlan, PlannerError> {
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

#[derive(Debug, Clone)]
struct ScanProjectionAssignment {
    columns: Vec<String>,
    qualifiers: Vec<String>,
    projected: HashSet<usize>,
    force_full_scan: bool,
    output_index: usize,
}

impl ScanProjectionAssignment {
    fn from_table(table: &TableRef) -> Option<Self> {
        let resolved = crate::catalog::with_catalog_read(|catalog| {
            catalog
                .resolve_table(&table.name, &crate::catalog::SearchPath::default())
                .ok()
                .cloned()
        })?;
        let columns = resolved
            .columns()
            .iter()
            .map(|column| column.name().to_ascii_lowercase())
            .collect::<Vec<_>>();
        let qualifiers = if let Some(alias) = &table.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![resolved.name().to_string(), resolved.qualified_name()]
        };
        Some(Self {
            columns,
            qualifiers,
            projected: HashSet::new(),
            force_full_scan: false,
            output_index: 0,
        })
    }

    fn mark_column(&mut self, column: &str) {
        if self.force_full_scan {
            return;
        }
        if let Some(index) = self
            .columns
            .iter()
            .position(|candidate| candidate == &column.to_ascii_lowercase())
        {
            self.projected.insert(index);
        }
    }

    fn mark_all(&mut self) {
        self.force_full_scan = true;
    }

    fn output_projection(&self) -> Option<Vec<usize>> {
        if self.force_full_scan || self.projected.len() == self.columns.len() {
            return None;
        }
        let mut projected = self.projected.iter().copied().collect::<Vec<_>>();
        projected.sort_unstable();
        Some(projected)
    }
}

#[derive(Debug, Clone, Default)]
struct TableShape {
    relation_indexes: Vec<usize>,
    output_columns: Option<HashSet<String>>,
    has_opaque_output: bool,
}

fn collect_query_scan_projections(query: &Query) -> Vec<Option<Vec<usize>>> {
    let mut output = Vec::new();
    let mut cte_names = Vec::new();
    if let Some(with) = &query.with {
        cte_names.extend(with.ctes.iter().map(|cte| cte.name.to_ascii_lowercase()));
        for cte in &with.ctes {
            collect_query_scan_projections_inner(&cte.query, &cte_names, &mut output);
        }
    }
    collect_query_scan_projections_inner(query, &cte_names, &mut output);
    output
}

fn collect_query_scan_projections_inner(
    query: &Query,
    cte_names: &[String],
    output: &mut Vec<Option<Vec<usize>>>,
) {
    match &query.body {
        QueryExpr::Select(select) => {
            collect_select_scan_projections(select, &query.order_by, cte_names, output);
        }
        QueryExpr::Nested(inner) => {
            let mut nested_cte_names = cte_names.to_vec();
            if let Some(with) = &inner.with {
                nested_cte_names.extend(with.ctes.iter().map(|cte| cte.name.to_ascii_lowercase()));
                for cte in &with.ctes {
                    collect_query_scan_projections_inner(&cte.query, &nested_cte_names, output);
                }
            }
            collect_query_scan_projections_inner(inner, &nested_cte_names, output);
        }
        QueryExpr::SetOperation { left, right, .. } => {
            collect_query_expr_scan_projections(left, cte_names, output);
            collect_query_expr_scan_projections(right, cte_names, output);
        }
        QueryExpr::Values(_)
        | QueryExpr::Insert(_)
        | QueryExpr::Update(_)
        | QueryExpr::Delete(_) => {}
    }
}

fn collect_query_expr_scan_projections(
    expr: &QueryExpr,
    cte_names: &[String],
    output: &mut Vec<Option<Vec<usize>>>,
) {
    match expr {
        QueryExpr::Select(select) => {
            collect_select_scan_projections(select, &[], cte_names, output)
        }
        QueryExpr::Nested(query) => collect_query_scan_projections_inner(query, cte_names, output),
        QueryExpr::SetOperation { left, right, .. } => {
            collect_query_expr_scan_projections(left, cte_names, output);
            collect_query_expr_scan_projections(right, cte_names, output);
        }
        QueryExpr::Values(_)
        | QueryExpr::Insert(_)
        | QueryExpr::Update(_)
        | QueryExpr::Delete(_) => {}
    }
}

fn collect_select_scan_projections(
    select: &SelectStatement,
    order_by: &[OrderByExpr],
    cte_names: &[String],
    output: &mut Vec<Option<Vec<usize>>>,
) {
    let mut relations = Vec::new();
    for table in &select.from {
        register_table_expression(table, cte_names, output, &mut relations);
    }

    mark_join_columns(&select.from, &mut relations);

    let select_aliases = select
        .targets
        .iter()
        .filter_map(|target| {
            target
                .alias
                .as_ref()
                .map(|alias| (alias.to_ascii_lowercase(), target.expr.clone()))
        })
        .collect::<HashMap<_, _>>();

    for target in &select.targets {
        mark_expr_columns(&target.expr, &mut relations, &select_aliases, true);
    }
    if let Some(predicate) = &select.where_clause {
        mark_expr_columns(predicate, &mut relations, &select_aliases, false);
    }
    for expr in &select.distinct_on {
        mark_expr_columns(expr, &mut relations, &select_aliases, true);
    }
    for expr in &select.group_by {
        mark_group_by_columns(expr, &mut relations, &select_aliases);
    }
    if let Some(having) = &select.having {
        mark_expr_columns(having, &mut relations, &select_aliases, false);
    }
    for target in &select.targets {
        mark_window_columns_from_expr(&target.expr, &mut relations, &select_aliases);
    }
    for expr in order_by {
        mark_expr_columns(&expr.expr, &mut relations, &select_aliases, true);
    }
    for definition in &select.window_definitions {
        mark_window_spec_columns(&definition.spec, &mut relations, &select_aliases);
    }

    for relation in relations {
        output[relation.output_index] = relation.output_projection();
    }
}

fn register_table_expression(
    table: &TableExpression,
    cte_names: &[String],
    output: &mut Vec<Option<Vec<usize>>>,
    relations: &mut Vec<ScanProjectionAssignment>,
) -> TableShape {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1
                && cte_names
                    .iter()
                    .any(|name| name == &rel.name[0].to_ascii_lowercase())
            {
                return TableShape {
                    relation_indexes: Vec::new(),
                    output_columns: None,
                    has_opaque_output: true,
                };
            }

            let Some(mut assignment) = ScanProjectionAssignment::from_table(rel) else {
                return TableShape {
                    relation_indexes: Vec::new(),
                    output_columns: None,
                    has_opaque_output: true,
                };
            };
            let output_index = output.len();
            output.push(None);
            assignment.output_index = output_index;
            let relation_index = relations.len();
            let output_columns = assignment.columns.iter().cloned().collect::<HashSet<_>>();
            relations.push(assignment);
            TableShape {
                relation_indexes: vec![relation_index],
                output_columns: Some(output_columns),
                has_opaque_output: false,
            }
        }
        TableExpression::Function(_) => TableShape {
            relation_indexes: Vec::new(),
            output_columns: None,
            has_opaque_output: true,
        },
        TableExpression::Subquery(subquery) => {
            collect_query_scan_projections_inner(&subquery.query, cte_names, output);
            TableShape {
                relation_indexes: Vec::new(),
                output_columns: None,
                has_opaque_output: true,
            }
        }
        TableExpression::Join(join) => {
            let left = register_table_expression(&join.left, cte_names, output, relations);
            let right = register_table_expression(&join.right, cte_names, output, relations);
            if join.natural {
                if left.has_opaque_output || right.has_opaque_output {
                    mark_relations_full(&left.relation_indexes, relations);
                    mark_relations_full(&right.relation_indexes, relations);
                } else if let (Some(left_columns), Some(right_columns)) =
                    (&left.output_columns, &right.output_columns)
                {
                    for column in left_columns.intersection(right_columns) {
                        mark_column_in_relations(&left.relation_indexes, column, relations);
                        mark_column_in_relations(&right.relation_indexes, column, relations);
                    }
                }
            } else if let Some(JoinCondition::Using(columns)) = &join.condition {
                for column in columns {
                    let column = column.to_ascii_lowercase();
                    mark_column_in_relations(&left.relation_indexes, &column, relations);
                    mark_column_in_relations(&right.relation_indexes, &column, relations);
                }
            }

            let output_columns = match (&left.output_columns, &right.output_columns) {
                (Some(left_columns), Some(right_columns)) => {
                    let mut merged = left_columns.clone();
                    merged.extend(right_columns.iter().cloned());
                    Some(merged)
                }
                _ => None,
            };

            let mut relation_indexes = left.relation_indexes;
            relation_indexes.extend(right.relation_indexes);
            TableShape {
                relation_indexes,
                output_columns,
                has_opaque_output: left.has_opaque_output || right.has_opaque_output,
            }
        }
    }
}

fn mark_join_columns(from: &[TableExpression], relations: &mut [ScanProjectionAssignment]) {
    for table in from {
        mark_join_columns_for_table(table, relations);
    }
}

fn mark_join_columns_for_table(
    table: &TableExpression,
    relations: &mut [ScanProjectionAssignment],
) {
    match table {
        TableExpression::Join(join) => {
            if let Some(JoinCondition::On(expr)) = &join.condition {
                mark_expr_columns(expr, relations, &HashMap::new(), false);
            }
            mark_join_columns_for_table(&join.left, relations);
            mark_join_columns_for_table(&join.right, relations);
        }
        TableExpression::Subquery(_)
        | TableExpression::Relation(_)
        | TableExpression::Function(_) => {}
    }
}

fn mark_window_columns_from_expr(
    expr: &Expr,
    relations: &mut [ScanProjectionAssignment],
    select_aliases: &HashMap<String, Expr>,
) {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            filter,
            over,
            ..
        } => {
            let is_plain_count_star = name
                .last()
                .is_some_and(|fn_name| fn_name.eq_ignore_ascii_case("count"))
                && args.len() == 1
                && matches!(args[0], Expr::Wildcard);
            if !is_plain_count_star {
                for arg in args {
                    mark_expr_columns(arg, relations, select_aliases, false);
                }
            }
            if let Some(filter) = filter.as_deref() {
                mark_expr_columns(filter, relations, select_aliases, false);
            }
            if let Some(over) = over {
                mark_window_spec_columns(over, relations, select_aliases);
            }
        }
        Expr::Binary { left, right, .. }
        | Expr::AnyAll { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. } => {
            mark_window_columns_from_expr(left, relations, select_aliases);
            mark_window_columns_from_expr(right, relations, select_aliases);
        }
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::BooleanTest { expr, .. } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
            mark_window_columns_from_expr(low, relations, select_aliases);
            mark_window_columns_from_expr(high, relations, select_aliases);
        }
        Expr::Like { expr, pattern, .. } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
            mark_window_columns_from_expr(pattern, relations, select_aliases);
        }
        Expr::InList { expr, list, .. } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
            for item in list {
                mark_window_columns_from_expr(item, relations, select_aliases);
            }
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            mark_window_columns_from_expr(operand, relations, select_aliases);
            for (when, then) in when_then {
                mark_window_columns_from_expr(when, relations, select_aliases);
                mark_window_columns_from_expr(then, relations, select_aliases);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_window_columns_from_expr(expr, relations, select_aliases);
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                mark_window_columns_from_expr(when, relations, select_aliases);
                mark_window_columns_from_expr(then, relations, select_aliases);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_window_columns_from_expr(expr, relations, select_aliases);
            }
        }
        Expr::ArrayConstructor(values) | Expr::RowConstructor(values) => {
            for value in values {
                mark_window_columns_from_expr(value, relations, select_aliases);
            }
        }
        Expr::ArraySubscript { expr, index, .. } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
            mark_window_columns_from_expr(index, relations, select_aliases);
        }
        Expr::ArraySlice {
            expr, start, end, ..
        } => {
            mark_window_columns_from_expr(expr, relations, select_aliases);
            if let Some(start) = start {
                mark_window_columns_from_expr(start, relations, select_aliases);
            }
            if let Some(end) = end {
                mark_window_columns_from_expr(end, relations, select_aliases);
            }
        }
        Expr::Identifier(_)
        | Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Default
        | Expr::Parameter(_)
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::TypedLiteral { .. }
        | Expr::Exists(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::InSubquery { .. }
        | Expr::MultiColumnSubqueryRef { .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
        }
    }
}

fn mark_window_spec_columns(
    spec: &WindowSpec,
    relations: &mut [ScanProjectionAssignment],
    select_aliases: &HashMap<String, Expr>,
) {
    for expr in &spec.partition_by {
        mark_expr_columns(expr, relations, select_aliases, true);
    }
    for expr in &spec.order_by {
        mark_expr_columns(&expr.expr, relations, select_aliases, true);
    }
    if let Some(frame) = &spec.frame {
        mark_window_frame_bound_columns(&frame.start, relations, select_aliases);
        mark_window_frame_bound_columns(&frame.end, relations, select_aliases);
    }
}

fn mark_window_frame_bound_columns(
    bound: &WindowFrameBound,
    relations: &mut [ScanProjectionAssignment],
    select_aliases: &HashMap<String, Expr>,
) {
    match bound {
        WindowFrameBound::OffsetPreceding(expr) | WindowFrameBound::OffsetFollowing(expr) => {
            mark_expr_columns(expr, relations, select_aliases, false);
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => {}
    }
}

fn mark_expr_columns(
    expr: &Expr,
    relations: &mut [ScanProjectionAssignment],
    select_aliases: &HashMap<String, Expr>,
    allow_select_alias: bool,
) {
    match expr {
        Expr::Identifier(parts) => {
            if parts.len() == 1 {
                let name = parts[0].to_ascii_lowercase();
                if allow_select_alias && let Some(alias_expr) = select_aliases.get(&name) {
                    mark_expr_columns(alias_expr, relations, select_aliases, false);
                    return;
                }
                if mark_record_reference(&name, relations) {
                    return;
                }
                for relation in relations {
                    relation.mark_column(&name);
                }
                return;
            }

            let qualifier = parts[..parts.len() - 1]
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            let column = parts
                .last()
                .map_or_else(String::new, |part| part.to_ascii_lowercase());
            for relation in relations {
                if relation
                    .qualifiers
                    .iter()
                    .any(|candidate| candidate == &qualifier)
                {
                    relation.mark_column(&column);
                }
            }
        }
        Expr::Wildcard => {
            for relation in relations {
                relation.mark_all();
            }
        }
        Expr::QualifiedWildcard(parts) => {
            let qualifier = parts
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            for relation in relations {
                if relation
                    .qualifiers
                    .iter()
                    .any(|candidate| candidate == &qualifier)
                {
                    relation.mark_all();
                }
            }
        }
        Expr::FunctionCall {
            name,
            args,
            filter,
            over,
            order_by,
            within_group,
            distinct: _,
            ..
        } => {
            let is_plain_count_star = name
                .last()
                .is_some_and(|fn_name| fn_name.eq_ignore_ascii_case("count"))
                && args.len() == 1
                && matches!(args[0], Expr::Wildcard);
            if !is_plain_count_star {
                for arg in args {
                    mark_expr_columns(arg, relations, select_aliases, false);
                }
            }
            if let Some(filter) = filter.as_deref() {
                mark_expr_columns(filter, relations, select_aliases, false);
            }
            if let Some(over) = over {
                mark_window_spec_columns(over, relations, select_aliases);
            }
            for expr in order_by {
                mark_expr_columns(&expr.expr, relations, select_aliases, false);
            }
            for expr in within_group {
                mark_expr_columns(&expr.expr, relations, select_aliases, false);
            }
        }
        Expr::Binary { left, right, .. }
        | Expr::AnyAll { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. } => {
            mark_expr_columns(left, relations, select_aliases, false);
            mark_expr_columns(right, relations, select_aliases, false);
        }
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::BooleanTest { expr, .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            mark_expr_columns(low, relations, select_aliases, false);
            mark_expr_columns(high, relations, select_aliases, false);
        }
        Expr::Like { expr, pattern, .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            mark_expr_columns(pattern, relations, select_aliases, false);
        }
        Expr::InList { expr, list, .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            for item in list {
                mark_expr_columns(item, relations, select_aliases, false);
            }
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            mark_expr_columns(operand, relations, select_aliases, false);
            for (when, then) in when_then {
                mark_expr_columns(when, relations, select_aliases, false);
                mark_expr_columns(then, relations, select_aliases, false);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_expr_columns(expr, relations, select_aliases, false);
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                mark_expr_columns(when, relations, select_aliases, false);
                mark_expr_columns(then, relations, select_aliases, false);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_expr_columns(expr, relations, select_aliases, false);
            }
        }
        Expr::ArrayConstructor(values) | Expr::RowConstructor(values) => {
            for value in values {
                mark_expr_columns(value, relations, select_aliases, false);
            }
        }
        Expr::ArraySubscript { expr, index, .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            mark_expr_columns(index, relations, select_aliases, false);
        }
        Expr::ArraySlice {
            expr, start, end, ..
        } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            if let Some(start) = start {
                mark_expr_columns(start, relations, select_aliases, false);
            }
            if let Some(end) = end {
                mark_expr_columns(end, relations, select_aliases, false);
            }
        }
        Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Default
        | Expr::Parameter(_)
        | Expr::TypedLiteral { .. } => {}
        Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) | Expr::Exists(query) => {
            mark_outer_relation_columns_in_query(query, relations);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            mark_expr_columns(expr, relations, select_aliases, false);
            mark_outer_relation_columns_in_query(subquery, relations);
        }
        Expr::MultiColumnSubqueryRef { subquery, .. } => {
            mark_outer_relation_columns_in_query(subquery, relations);
        }
    }
}

fn mark_record_reference(name: &str, relations: &mut [ScanProjectionAssignment]) -> bool {
    let matches_qualifier = relations.iter().any(|relation| {
        relation
            .qualifiers
            .iter()
            .any(|qualifier| qualifier == name)
            && !relation.columns.iter().any(|column| column == name)
    });
    if !matches_qualifier {
        return false;
    }
    for relation in relations {
        if relation
            .qualifiers
            .iter()
            .any(|qualifier| qualifier == name)
        {
            relation.mark_all();
        }
    }
    true
}

fn mark_group_by_columns(
    expr: &GroupByExpr,
    relations: &mut [ScanProjectionAssignment],
    select_aliases: &HashMap<String, Expr>,
) {
    match expr {
        GroupByExpr::Expr(expr) => mark_expr_columns(expr, relations, select_aliases, true),
        GroupByExpr::GroupingSets(sets) => {
            for set in sets {
                for expr in set {
                    mark_expr_columns(expr, relations, select_aliases, true);
                }
            }
        }
        GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
            for expr in exprs {
                mark_expr_columns(expr, relations, select_aliases, true);
            }
        }
    }
}

fn mark_outer_relation_columns_in_query(query: &Query, relations: &mut [ScanProjectionAssignment]) {
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            mark_outer_relation_columns_in_query(&cte.query, relations);
        }
    }
    mark_outer_relation_columns_in_query_expr(&query.body, relations);
    for expr in &query.order_by {
        mark_outer_relation_columns_in_expr(&expr.expr, relations);
    }
}

fn mark_outer_relation_columns_in_query_expr(
    expr: &QueryExpr,
    relations: &mut [ScanProjectionAssignment],
) {
    match expr {
        QueryExpr::Select(select) => {
            for table in &select.from {
                mark_outer_relation_columns_in_table_expression(table, relations);
            }
            for target in &select.targets {
                mark_outer_relation_columns_in_expr(&target.expr, relations);
            }
            if let Some(predicate) = &select.where_clause {
                mark_outer_relation_columns_in_expr(predicate, relations);
            }
            for expr in &select.group_by {
                match expr {
                    GroupByExpr::Expr(expr) => mark_outer_relation_columns_in_expr(expr, relations),
                    GroupByExpr::GroupingSets(sets) => {
                        for set in sets {
                            for expr in set {
                                mark_outer_relation_columns_in_expr(expr, relations);
                            }
                        }
                    }
                    GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
                        for expr in exprs {
                            mark_outer_relation_columns_in_expr(expr, relations);
                        }
                    }
                }
            }
            if let Some(having) = &select.having {
                mark_outer_relation_columns_in_expr(having, relations);
            }
            for expr in &select.window_definitions {
                for part in &expr.spec.partition_by {
                    mark_outer_relation_columns_in_expr(part, relations);
                }
                for part in &expr.spec.order_by {
                    mark_outer_relation_columns_in_expr(&part.expr, relations);
                }
            }
        }
        QueryExpr::Nested(query) => mark_outer_relation_columns_in_query(query, relations),
        QueryExpr::SetOperation { left, right, .. } => {
            mark_outer_relation_columns_in_query_expr(left, relations);
            mark_outer_relation_columns_in_query_expr(right, relations);
        }
        QueryExpr::Values(rows) => {
            for row in rows {
                for expr in row {
                    mark_outer_relation_columns_in_expr(expr, relations);
                }
            }
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {}
    }
}

fn mark_outer_relation_columns_in_table_expression(
    table: &TableExpression,
    relations: &mut [ScanProjectionAssignment],
) {
    match table {
        TableExpression::Subquery(subquery) => {
            mark_outer_relation_columns_in_query(&subquery.query, relations);
        }
        TableExpression::Join(join) => {
            mark_outer_relation_columns_in_table_expression(&join.left, relations);
            mark_outer_relation_columns_in_table_expression(&join.right, relations);
            if let Some(JoinCondition::On(expr)) = &join.condition {
                mark_outer_relation_columns_in_expr(expr, relations);
            }
        }
        TableExpression::Relation(_) | TableExpression::Function(_) => {}
    }
}

fn mark_outer_relation_columns_in_expr(expr: &Expr, relations: &mut [ScanProjectionAssignment]) {
    match expr {
        Expr::Identifier(parts) => {
            mark_identifier_columns(parts, relations);
        }
        Expr::Binary { left, right, .. }
        | Expr::AnyAll { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. } => {
            mark_outer_relation_columns_in_expr(left, relations);
            mark_outer_relation_columns_in_expr(right, relations);
        }
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::BooleanTest { expr, .. } => {
            mark_outer_relation_columns_in_expr(expr, relations);
        }
        Expr::FunctionCall {
            args,
            filter,
            over,
            order_by,
            within_group,
            ..
        } => {
            for arg in args {
                mark_outer_relation_columns_in_expr(arg, relations);
            }
            if let Some(filter) = filter.as_deref() {
                mark_outer_relation_columns_in_expr(filter, relations);
            }
            if let Some(over) = over {
                for expr in &over.partition_by {
                    mark_outer_relation_columns_in_expr(expr, relations);
                }
                for expr in &over.order_by {
                    mark_outer_relation_columns_in_expr(&expr.expr, relations);
                }
            }
            for expr in order_by {
                mark_outer_relation_columns_in_expr(&expr.expr, relations);
            }
            for expr in within_group {
                mark_outer_relation_columns_in_expr(&expr.expr, relations);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            mark_outer_relation_columns_in_expr(low, relations);
            mark_outer_relation_columns_in_expr(high, relations);
        }
        Expr::Like { expr, pattern, .. } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            mark_outer_relation_columns_in_expr(pattern, relations);
        }
        Expr::InList { expr, list, .. } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            for item in list {
                mark_outer_relation_columns_in_expr(item, relations);
            }
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            mark_outer_relation_columns_in_expr(operand, relations);
            for (when, then) in when_then {
                mark_outer_relation_columns_in_expr(when, relations);
                mark_outer_relation_columns_in_expr(then, relations);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_outer_relation_columns_in_expr(expr, relations);
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                mark_outer_relation_columns_in_expr(when, relations);
                mark_outer_relation_columns_in_expr(then, relations);
            }
            if let Some(expr) = else_expr.as_deref() {
                mark_outer_relation_columns_in_expr(expr, relations);
            }
        }
        Expr::ArrayConstructor(values) | Expr::RowConstructor(values) => {
            for value in values {
                mark_outer_relation_columns_in_expr(value, relations);
            }
        }
        Expr::ArraySubscript { expr, index, .. } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            mark_outer_relation_columns_in_expr(index, relations);
        }
        Expr::ArraySlice {
            expr, start, end, ..
        } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            if let Some(start) = start {
                mark_outer_relation_columns_in_expr(start, relations);
            }
            if let Some(end) = end {
                mark_outer_relation_columns_in_expr(end, relations);
            }
        }
        Expr::Exists(query) | Expr::ScalarSubquery(query) | Expr::ArraySubquery(query) => {
            mark_outer_relation_columns_in_query(query, relations);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            mark_outer_relation_columns_in_expr(expr, relations);
            mark_outer_relation_columns_in_query(subquery, relations);
        }
        Expr::MultiColumnSubqueryRef { subquery, .. } => {
            mark_outer_relation_columns_in_query(subquery, relations);
        }
        Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::String(_)
        | Expr::Integer(_)
        | Expr::Float(_)
        | Expr::Boolean(_)
        | Expr::Null
        | Expr::Default
        | Expr::Parameter(_)
        | Expr::TypedLiteral { .. } => {}
    }
}

fn mark_identifier_columns(parts: &[String], relations: &mut [ScanProjectionAssignment]) {
    if parts.len() == 1 {
        let name = parts[0].to_ascii_lowercase();
        if mark_record_reference(&name, relations) {
            return;
        }
        for relation in relations {
            relation.mark_column(&name);
        }
        return;
    }

    let qualifier = parts[..parts.len() - 1]
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".");
    let column = parts
        .last()
        .map_or_else(String::new, |part| part.to_ascii_lowercase());
    for relation in relations {
        if relation
            .qualifiers
            .iter()
            .any(|candidate| candidate == &qualifier)
        {
            relation.mark_column(&column);
        }
    }
}

fn mark_relations_full(indexes: &[usize], relations: &mut [ScanProjectionAssignment]) {
    for index in indexes {
        if let Some(relation) = relations.get_mut(*index) {
            relation.mark_all();
        }
    }
}

fn mark_column_in_relations(
    indexes: &[usize],
    column: &str,
    relations: &mut [ScanProjectionAssignment],
) {
    for index in indexes {
        if let Some(relation) = relations.get_mut(*index) {
            relation.mark_column(column);
        }
    }
}

fn assign_scan_projections(
    plan: &mut PhysicalPlan,
    projections: &mut impl Iterator<Item = Option<Vec<usize>>>,
) {
    match plan {
        PhysicalPlan::Scan(scan) => {
            scan.projected_columns = projections.next().flatten();
        }
        PhysicalPlan::Filter(filter) => assign_scan_projections(&mut filter.input, projections),
        PhysicalPlan::Project(project) => assign_scan_projections(&mut project.input, projections),
        PhysicalPlan::Aggregate(aggregate) => {
            assign_scan_projections(&mut aggregate.input, projections);
        }
        PhysicalPlan::Window(window) => assign_scan_projections(&mut window.input, projections),
        PhysicalPlan::Distinct(distinct) => {
            assign_scan_projections(&mut distinct.input, projections)
        }
        PhysicalPlan::Sort(sort) => assign_scan_projections(&mut sort.input, projections),
        PhysicalPlan::Limit(limit) => assign_scan_projections(&mut limit.input, projections),
        PhysicalPlan::Subquery(subquery) => {
            assign_scan_projections(&mut subquery.plan, projections)
        }
        PhysicalPlan::SetOp(set_op) => {
            assign_scan_projections(&mut set_op.left, projections);
            assign_scan_projections(&mut set_op.right, projections);
        }
        PhysicalPlan::HashJoin(join) | PhysicalPlan::NestedLoopJoin(join) => {
            assign_scan_projections(&mut join.left, projections);
            assign_scan_projections(&mut join.right, projections);
        }
        PhysicalPlan::Cte(cte) => {
            for binding in &mut cte.ctes {
                assign_scan_projections(&mut binding.plan, projections);
            }
            assign_scan_projections(&mut cte.input, projections);
        }
        PhysicalPlan::Result(_) | PhysicalPlan::FunctionScan(_) | PhysicalPlan::CteScan(_) => {}
    }
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
                return if *negated {
                    1.0 - null_fraction
                } else {
                    null_fraction
                };
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
