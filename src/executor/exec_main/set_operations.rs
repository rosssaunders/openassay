#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) async fn execute_set_operation(
    left: &QueryExpr,
    op: SetOperator,
    quantifier: SetQuantifier,
    right: &QueryExpr,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<QueryResult, EngineError> {
    let left_res = execute_query_expr_with_outer(left, params, outer_scope).await?;
    let right_res = execute_query_expr_with_outer(right, params, outer_scope).await?;
    let width = left_res.columns.len().max(right_res.columns.len());
    let mut columns = left_res.columns.clone();
    if columns.len() < width {
        for idx in columns.len()..width {
            columns.push(
                right_res
                    .columns
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("column{}", idx + 1)),
            );
        }
    }
    let left_rows = left_res
        .rows
        .into_iter()
        .map(|row| normalize_row_width(row, width))
        .collect::<Vec<_>>();
    let right_rows = right_res
        .rows
        .into_iter()
        .map(|row| normalize_row_width(row, width))
        .collect::<Vec<_>>();

    let rows = match (op, quantifier) {
        (SetOperator::Union, SetQuantifier::All) => {
            let mut out = left_rows.clone();
            out.extend(right_rows.iter().cloned());
            out
        }
        (SetOperator::Union, SetQuantifier::Distinct) => dedupe_rows(
            left_rows
                .iter()
                .cloned()
                .chain(right_rows.iter().cloned())
                .collect(),
        ),
        (SetOperator::Intersect, SetQuantifier::Distinct) => {
            intersect_rows(&left_rows, &right_rows, false)
        }
        (SetOperator::Intersect, SetQuantifier::All) => {
            intersect_rows(&left_rows, &right_rows, true)
        }
        (SetOperator::Except, SetQuantifier::Distinct) => {
            except_rows(&left_rows, &right_rows, false)
        }
        (SetOperator::Except, SetQuantifier::All) => except_rows(&left_rows, &right_rows, true),
    };

    Ok(QueryResult {
        columns,
        rows_affected: rows.len() as u64,
        rows,
        command_tag: "SELECT".to_string(),
    })
}

pub(super) fn normalize_row_width(mut row: Vec<ScalarValue>, width: usize) -> Vec<ScalarValue> {
    if row.len() < width {
        row.resize(width, ScalarValue::Null);
    } else if row.len() > width {
        row.truncate(width);
    }
    row
}

pub(super) fn is_recursive_union_expr(expr: &QueryExpr) -> bool {
    matches!(
        expr,
        QueryExpr::SetOperation {
            op: SetOperator::Union,
            ..
        }
    )
}

pub(super) fn append_cte_aux_columns(
    columns: &mut Vec<String>,
    cte: &crate::parser::ast::CommonTableExpr,
) -> (Option<usize>, Option<usize>, Option<usize>) {
    let mut search_idx = None;
    if let Some(search) = &cte.search_clause {
        if let Some(idx) = columns
            .iter()
            .position(|col| col.eq_ignore_ascii_case(&search.set_column))
        {
            search_idx = Some(idx);
        } else {
            columns.push(search.set_column.clone());
            search_idx = Some(columns.len() - 1);
        }
    }

    let mut cycle_idx = None;
    let mut path_idx = None;
    if let Some(cycle) = &cte.cycle_clause {
        if let Some(idx) = columns
            .iter()
            .position(|col| col.eq_ignore_ascii_case(&cycle.set_column))
        {
            cycle_idx = Some(idx);
        } else {
            columns.push(cycle.set_column.clone());
            cycle_idx = Some(columns.len() - 1);
        }
        if let Some(idx) = columns
            .iter()
            .position(|col| col.eq_ignore_ascii_case(&cycle.using_column))
        {
            path_idx = Some(idx);
        } else {
            columns.push(cycle.using_column.clone());
            path_idx = Some(columns.len() - 1);
        }
    }
    (search_idx, cycle_idx, path_idx)
}

pub(super) fn populate_cte_aux_values(
    row: &mut [ScalarValue],
    seq: i64,
    search_idx: Option<usize>,
    cycle_idx: Option<usize>,
    path_idx: Option<usize>,
) {
    if let Some(idx) = search_idx
        && idx < row.len()
    {
        row[idx] = ScalarValue::Int(seq);
    }
    if let Some(idx) = cycle_idx
        && idx < row.len()
    {
        row[idx] = ScalarValue::Bool(false);
    }
    if let Some(idx) = path_idx
        && idx < row.len()
    {
        row[idx] = ScalarValue::Null;
    }
}

pub(super) fn dedupe_rows(rows: Vec<Vec<ScalarValue>>) -> Vec<Vec<ScalarValue>> {
    let mut seen = HashSet::with_capacity(rows.len());
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row_key(&row);
        if seen.insert(key) {
            out.push(row);
        }
    }
    out
}

pub(super) fn intersect_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                out.push(row.clone());
            }
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

pub(super) fn except_rows(
    left: &[Vec<ScalarValue>],
    right: &[Vec<ScalarValue>],
    all: bool,
) -> Vec<Vec<ScalarValue>> {
    if all {
        let mut out = Vec::new();
        let mut right_counts = count_rows(right);
        for row in left {
            let key = row_key(row);
            if let Some(count) = right_counts.get_mut(&key)
                && *count > 0
            {
                *count -= 1;
                continue;
            }
            out.push(row.clone());
        }
        return out;
    }

    let right_keys: HashSet<String> = right.iter().map(|r| row_key(r)).collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in left {
        let key = row_key(row);
        if !right_keys.contains(&key) && seen.insert(key) {
            out.push(row.clone());
        }
    }
    out
}

pub(super) fn count_rows(rows: &[Vec<ScalarValue>]) -> std::collections::HashMap<String, usize> {
    let mut counts = std::collections::HashMap::new();
    for row in rows {
        *counts.entry(row_key(row)).or_insert(0) += 1;
    }
    counts
}

pub fn row_key(row: &[ScalarValue]) -> String {
    let mut key = String::new();
    for (idx, value) in row.iter().enumerate() {
        if idx > 0 {
            key.push('|');
        }

        match value {
            ScalarValue::Null => key.push('N'),
            ScalarValue::Bool(flag) => {
                key.push_str("B:");
                key.push_str(if *flag { "true" } else { "false" });
            }
            ScalarValue::Int(number) => {
                key.push_str("I:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Float(number) => {
                key.push_str("F:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Numeric(number) => {
                key.push_str("N:");
                key.push_str(&number.to_string());
            }
            ScalarValue::Text(text) => {
                key.push_str("T:");
                key.push_str(text);
            }
            ScalarValue::Array(_) => {
                key.push_str("A:");
                key.push_str(&value.render());
            }
            ScalarValue::Record(_) => {
                key.push_str("R:");
                key.push_str(&value.render());
            }
            ScalarValue::Vector(_) => {
                key.push_str("V:");
                key.push_str(&value.render());
            }
        }
    }
    key
}
