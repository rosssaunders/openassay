use crate::parser::ast::{ExplainStatement, QueryExpr, Statement, TableExpression};
use crate::planner::{self, PlanNode};
use crate::tcop::engine::{
    EngineError, QueryResult, ScalarValue, execute_planned_query, plan_statement,
};

pub async fn execute_explain(
    explain: &ExplainStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let mut plan_lines = Vec::new();
    let planned = planner::plan(&explain.statement);
    match planned {
        PlanNode::PassThrough(statement) => describe_statement_plan(&statement, &mut plan_lines, 0),
        other => other.explain(&mut plan_lines, 0),
    }

    if explain.analyze {
        let start = std::time::Instant::now();
        let inner_result =
            execute_planned_query(&plan_statement((*explain.statement).clone())?, params).await?;
        let elapsed = start.elapsed();
        plan_lines.push("Planning Time: 0.001 ms".to_string());
        plan_lines.push(format!(
            "Execution Time: {:.3} ms",
            elapsed.as_secs_f64() * 1000.0
        ));
        plan_lines.push(format!("  (actual rows={})", inner_result.rows.len()));
    }

    let rows = plan_lines
        .into_iter()
        .map(|line| vec![ScalarValue::Text(line)])
        .collect();
    Ok(QueryResult {
        columns: vec!["QUERY PLAN".to_string()],
        rows,
        command_tag: "EXPLAIN".to_string(),
        rows_affected: 0,
    })
}

fn describe_statement_plan(stmt: &Statement, lines: &mut Vec<String>, indent: usize) {
    let prefix = " ".repeat(indent);
    match stmt {
        Statement::Query(query) => match &query.body {
            QueryExpr::Select(select) => {
                if select.from.is_empty() {
                    lines.push(format!(
                        "{}Result  (cost=0.00..0.01 rows=1 width=0)",
                        prefix
                    ));
                } else {
                    lines.push(format!(
                        "{}Seq Scan  (cost=0.00..1.00 rows=1 width=0)",
                        prefix
                    ));
                    for table_expr in &select.from {
                        if let TableExpression::Relation(rel) = table_expr {
                            lines.push(format!("{}  on {}", prefix, rel.name.join(".")));
                        }
                    }
                }
                if select.where_clause.is_some() {
                    lines.push(format!("{}  Filter: <condition>", prefix));
                }
            }
            QueryExpr::SetOperation { op, .. } => {
                lines.push(format!(
                    "{}{:?}  (cost=0.00..1.00 rows=1 width=0)",
                    prefix, op
                ));
            }
            QueryExpr::Nested(inner) => {
                describe_statement_plan(&Statement::Query((**inner).clone()), lines, indent);
            }
            QueryExpr::Values(rows) => {
                lines.push(format!(
                    "{}Values Scan  (cost=0.00..0.01 rows={} width=0)",
                    prefix,
                    rows.len()
                ));
            }
            QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
                lines.push(format!(
                    "{}Data-modifying statement in WITH (not yet supported)",
                    prefix
                ));
            }
        },
        Statement::Insert(_) => {
            lines.push(format!(
                "{}Insert  (cost=0.00..1.00 rows=0 width=0)",
                prefix
            ));
        }
        Statement::Update(_) => {
            lines.push(format!(
                "{}Update  (cost=0.00..1.00 rows=0 width=0)",
                prefix
            ));
        }
        Statement::Delete(_) => {
            lines.push(format!(
                "{}Delete  (cost=0.00..1.00 rows=0 width=0)",
                prefix
            ));
        }
        _ => lines.push(format!("{}Utility Statement", prefix)),
    }
}
