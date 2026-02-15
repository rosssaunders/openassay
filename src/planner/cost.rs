#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanCost {
    pub rows: f64,
    pub startup_cost: f64,
    pub total_cost: f64,
}

impl PlanCost {
    pub fn new(rows: f64, startup_cost: f64, total_cost: f64) -> Self {
        Self {
            rows: rows.max(0.0),
            startup_cost: startup_cost.max(0.0),
            total_cost: total_cost.max(startup_cost),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    Hash,
    NestedLoop,
}

const DEFAULT_FILTER_SELECTIVITY: f64 = 0.25;
const DEFAULT_JOIN_SELECTIVITY: f64 = 0.1;

pub fn seq_scan_cost(rows: f64) -> PlanCost {
    PlanCost::new(rows, 0.0, rows)
}

pub fn index_scan_cost(rows: f64) -> PlanCost {
    let expected = rows * 0.1;
    PlanCost::new(expected, 0.0, expected + rows.ln().max(1.0))
}

pub fn filter_cost(input: PlanCost, selectivity: f64) -> PlanCost {
    let rows = input.rows * selectivity;
    PlanCost::new(rows, input.startup_cost, input.total_cost + rows)
}

pub fn project_cost(input: PlanCost) -> PlanCost {
    PlanCost::new(
        input.rows,
        input.startup_cost,
        input.total_cost + input.rows * 0.01,
    )
}

pub fn aggregate_cost(input: PlanCost, groups: f64) -> PlanCost {
    let rows = groups.max(1.0).min(input.rows);
    PlanCost::new(
        rows,
        input.startup_cost,
        input.total_cost + input.rows * 0.5,
    )
}

pub fn sort_cost(input: PlanCost) -> PlanCost {
    let sort_cost = input.rows * input.rows.max(1.0).ln().max(1.0);
    PlanCost::new(input.rows, input.startup_cost, input.total_cost + sort_cost)
}

pub fn distinct_cost(input: PlanCost) -> PlanCost {
    PlanCost::new(
        input.rows * 0.5,
        input.startup_cost,
        input.total_cost + input.rows * 0.2,
    )
}

pub fn window_cost(input: PlanCost) -> PlanCost {
    PlanCost::new(
        input.rows,
        input.startup_cost,
        input.total_cost + input.rows * 0.2,
    )
}

pub fn limit_cost(input: PlanCost, limit: Option<f64>) -> PlanCost {
    let rows = limit.map(|l| l.min(input.rows)).unwrap_or(input.rows);
    PlanCost::new(rows, input.startup_cost, input.total_cost)
}

pub fn cte_scan_cost(input: PlanCost) -> PlanCost {
    PlanCost::new(
        input.rows,
        input.startup_cost,
        input.total_cost + input.rows * 0.05,
    )
}

pub fn cte_cost<I>(input: PlanCost, cte_costs: I) -> PlanCost
where
    I: IntoIterator<Item = PlanCost>,
{
    let mut startup = input.startup_cost;
    let mut total = input.total_cost;
    for cost in cte_costs {
        startup += cost.startup_cost;
        total += cost.total_cost;
    }
    PlanCost::new(input.rows, startup, total)
}

pub fn set_op_cost(left: PlanCost, right: PlanCost) -> PlanCost {
    PlanCost::new(
        left.rows + right.rows,
        left.startup_cost + right.startup_cost,
        left.total_cost + right.total_cost,
    )
}

pub fn choose_join_strategy(left: PlanCost, right: PlanCost) -> JoinStrategy {
    if left.rows.max(right.rows) > 1000.0 || left.rows * right.rows > 10_000.0 {
        JoinStrategy::Hash
    } else {
        JoinStrategy::NestedLoop
    }
}

pub fn join_cost(
    left: PlanCost,
    right: PlanCost,
    strategy: JoinStrategy,
    has_condition: bool,
) -> PlanCost {
    let selectivity = if has_condition {
        DEFAULT_JOIN_SELECTIVITY
    } else {
        1.0
    };
    let rows = left.rows * right.rows * selectivity;
    let total = match strategy {
        JoinStrategy::Hash => left.total_cost + right.total_cost + left.rows + right.rows,
        JoinStrategy::NestedLoop => left.total_cost + left.rows * right.total_cost,
    };
    PlanCost::new(rows, left.startup_cost + right.startup_cost, total)
}

pub fn default_filter_selectivity() -> f64 {
    DEFAULT_FILTER_SELECTIVITY
}
