use std::future::Future;

use super::{plan, PlanNode};
use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
use crate::parser::sql_parser::parse_statement;
use crate::planner::logical::LogicalPlan;
use crate::planner::physical::{PhysicalPlan, ScanType};
use crate::tcop::engine::{execute_planned_query, plan_statement, reset_global_storage_for_tests};

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should start")
        .block_on(future)
}

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        reset_global_storage_for_tests();
        f()
    })
}

fn run_statement(sql: &str) {
    let statement = parse_statement(sql).expect("statement should parse");
    let planned = plan_statement(statement).expect("statement should plan");
    block_on(execute_planned_query(&planned, &[])).expect("statement should execute");
}

fn plan_query(sql: &str) -> crate::planner::QueryPlan {
    let statement = parse_statement(sql).expect("statement should parse");
    match plan(&statement) {
        PlanNode::Query(plan) => plan,
        other => panic!("expected query plan, got {other:?}"),
    }
}

fn logical_contains<F>(plan: &LogicalPlan, predicate: &F) -> bool
where
    F: Fn(&LogicalPlan) -> bool,
{
    if predicate(plan) {
        return true;
    }
    match plan {
        LogicalPlan::Join(join) => {
            logical_contains(&join.left, predicate) || logical_contains(&join.right, predicate)
        }
        LogicalPlan::Filter(filter) => logical_contains(&filter.input, predicate),
        LogicalPlan::Aggregate(aggregate) => logical_contains(&aggregate.input, predicate),
        LogicalPlan::Window(window) => logical_contains(&window.input, predicate),
        LogicalPlan::Project(project) => logical_contains(&project.input, predicate),
        LogicalPlan::Distinct(distinct) => logical_contains(&distinct.input, predicate),
        LogicalPlan::Sort(sort) => logical_contains(&sort.input, predicate),
        LogicalPlan::Limit(limit) => logical_contains(&limit.input, predicate),
        LogicalPlan::SetOp(set_op) => {
            logical_contains(&set_op.left, predicate) || logical_contains(&set_op.right, predicate)
        }
        LogicalPlan::Subquery(subquery) => logical_contains(&subquery.plan, predicate),
        LogicalPlan::Cte(cte) => {
            cte.ctes.iter().any(|cte| logical_contains(&cte.plan, predicate))
                || logical_contains(&cte.input, predicate)
        }
        LogicalPlan::Result
        | LogicalPlan::Scan(_)
        | LogicalPlan::FunctionScan(_)
        | LogicalPlan::CteScan(_) => false,
    }
}

fn physical_contains<F>(plan: &PhysicalPlan, predicate: &F) -> bool
where
    F: Fn(&PhysicalPlan) -> bool,
{
    if predicate(plan) {
        return true;
    }
    match plan {
        PhysicalPlan::Filter(filter) => physical_contains(&filter.input, predicate),
        PhysicalPlan::Project(project) => physical_contains(&project.input, predicate),
        PhysicalPlan::Aggregate(aggregate) => physical_contains(&aggregate.input, predicate),
        PhysicalPlan::Window(window) => physical_contains(&window.input, predicate),
        PhysicalPlan::Distinct(distinct) => physical_contains(&distinct.input, predicate),
        PhysicalPlan::Sort(sort) => physical_contains(&sort.input, predicate),
        PhysicalPlan::Limit(limit) => physical_contains(&limit.input, predicate),
        PhysicalPlan::SetOp(set_op) => {
            physical_contains(&set_op.left, predicate) || physical_contains(&set_op.right, predicate)
        }
        PhysicalPlan::HashJoin(join) | PhysicalPlan::NestedLoopJoin(join) => {
            physical_contains(&join.left, predicate) || physical_contains(&join.right, predicate)
        }
        PhysicalPlan::Subquery(subquery) => physical_contains(&subquery.plan, predicate),
        PhysicalPlan::Cte(cte) => {
            cte.ctes.iter().any(|cte| physical_contains(&cte.plan, predicate))
                || physical_contains(&cte.input, predicate)
        }
        PhysicalPlan::Result(_)
        | PhysicalPlan::Scan(_)
        | PhysicalPlan::FunctionScan(_)
        | PhysicalPlan::CteScan(_) => false,
    }
}

fn extract_scan(plan: &PhysicalPlan) -> &crate::planner::physical::ScanPlan {
    match plan {
        PhysicalPlan::Scan(scan) => scan,
        PhysicalPlan::Project(project) => extract_scan(&project.input),
        PhysicalPlan::Filter(filter) => extract_scan(&filter.input),
        _ => panic!("expected scan plan, got {plan:?}"),
    }
}

fn extract_join(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::NestedLoopJoin(_) | PhysicalPlan::HashJoin(_) => plan,
        PhysicalPlan::Project(project) => extract_join(&project.input),
        PhysicalPlan::Filter(filter) => extract_join(&filter.input),
        _ => panic!("expected join plan, got {plan:?}"),
    }
}

#[test]
fn plans_index_scan_for_simple_filter() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE t (id int8)");
        run_statement("CREATE INDEX t_id_idx ON t (id)");
        run_statement("INSERT INTO t VALUES (1), (2)");
        let stmt = parse_statement("SELECT * FROM t WHERE id = 1").expect("statement should parse");
        let plan = plan(&stmt);
        match plan {
            PlanNode::Query(query_plan) => {
                let scan = extract_scan(&query_plan.physical);
                assert!(matches!(scan.scan_type, ScanType::Index(_)));
                assert!(scan.filter.is_some());
            }
            other => panic!("expected query plan, got {other:?}"),
        }
    });
}

#[test]
fn plans_nested_loop_join_for_small_inputs() {
    with_isolated_state(|| {
        run_statement("CREATE TABLE a (id int8)");
        run_statement("CREATE TABLE b (id int8)");
        run_statement("INSERT INTO a VALUES (1)");
        run_statement("INSERT INTO b VALUES (1)");
        let stmt =
            parse_statement("SELECT * FROM a JOIN b ON a.id = b.id").expect("statement should parse");
        let plan = plan(&stmt);
        match plan {
            PlanNode::Query(query_plan) => {
                let join = extract_join(&query_plan.physical);
                assert!(matches!(join, PhysicalPlan::NestedLoopJoin(_)));
            }
            other => panic!("expected query plan, got {other:?}"),
        }
    });
}

#[test]
fn plans_cte_queries() {
    let plan = plan_query("WITH cte AS (SELECT 1) SELECT * FROM cte");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Cte(_))));
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::CteScan(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Cte(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::CteScan(_))));
}

#[test]
fn plans_window_queries() {
    let plan = plan_query("SELECT row_number() OVER (ORDER BY 1)");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Window(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Window(_))));
}

#[test]
fn plans_aggregate_queries() {
    let plan = plan_query("SELECT count(*) FROM (SELECT 1 AS id) t GROUP BY id");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Aggregate(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Aggregate(_))));
}

#[test]
fn plans_order_by_queries() {
    let plan = plan_query("SELECT 1 ORDER BY 1");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Sort(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Sort(_))));
}

#[test]
fn plans_limit_queries() {
    let plan = plan_query("SELECT 1 LIMIT 1 OFFSET 1");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Limit(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Limit(_))));
}

#[test]
fn plans_distinct_queries() {
    let plan = plan_query("SELECT DISTINCT 1");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::Distinct(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::Distinct(_))));
}

#[test]
fn plans_set_op_queries() {
    let plan = plan_query("SELECT 1 UNION SELECT 2");
    assert!(logical_contains(&plan.logical, &|plan| matches!(plan, LogicalPlan::SetOp(_))));
    assert!(physical_contains(&plan.physical, &|plan| matches!(plan, PhysicalPlan::SetOp(_))));
}
