#[allow(clippy::wildcard_imports)]
use super::*;
use crate::executor::column_batch::{ColumnBatch, TypedColumn};
use std::collections::BinaryHeap;

pub(super) struct QueryRowCollector {
    strategy: RowCollectionStrategy,
}

pub(super) struct SimpleTopNCollector {
    heap: BinaryHeap<TopNEntry>,
    order_by: Vec<OrderByExpr>,
    order_indices: Vec<usize>,
    capacity: usize,
    offset: usize,
    limit: usize,
    sequence: usize,
}

pub(super) struct OffsetTopNCollector {
    heap: BinaryHeap<OffsetTopNEntry>,
    order_by: Vec<OrderByExpr>,
    order_indices: Vec<usize>,
    capacity: usize,
    offset: usize,
    limit: usize,
    sequence: usize,
}

enum RowCollectionStrategy {
    All {
        rows: Vec<CollectedRow>,
        order_by: Vec<OrderByExpr>,
        offset: usize,
        limit: Option<usize>,
    },
    LimitOnly {
        rows: Vec<Vec<ScalarValue>>,
        offset: usize,
        limit: usize,
    },
    TopN {
        heap: BinaryHeap<TopNEntry>,
        order_by: Vec<OrderByExpr>,
        capacity: usize,
        offset: usize,
        limit: usize,
        sequence: usize,
    },
}

struct TopNEntry {
    keys: Vec<TopNKeyPart>,
    row: Vec<ScalarValue>,
    sequence: usize,
}

struct OffsetTopNEntry {
    keys: Vec<TopNKeyPart>,
    row_offset: usize,
    sequence: usize,
}

struct CollectedRow {
    keys: Vec<ScalarValue>,
    row: Vec<ScalarValue>,
}

struct TopNKeyPart {
    value: ScalarValue,
    descending: bool,
}

impl QueryRowCollector {
    pub(super) async fn new(query: &Query, params: &[Option<String>]) -> Result<Self, EngineError> {
        let offset = if let Some(expr) = &query.offset {
            parse_non_negative_int(
                &eval_expr(expr, &EvalScope::default(), params).await?,
                "OFFSET",
            )?
        } else {
            0usize
        };

        let limit = if let Some(expr) = &query.limit {
            Some(parse_non_negative_int(
                &eval_expr(expr, &EvalScope::default(), params).await?,
                "LIMIT",
            )?)
        } else {
            None
        };

        let strategy = match limit {
            Some(limit) if !query.order_by.is_empty() => RowCollectionStrategy::TopN {
                heap: BinaryHeap::new(),
                order_by: query.order_by.clone(),
                capacity: offset.saturating_add(limit),
                offset,
                limit,
                sequence: 0,
            },
            Some(limit) => RowCollectionStrategy::LimitOnly {
                rows: Vec::new(),
                offset,
                limit,
            },
            None => RowCollectionStrategy::All {
                rows: Vec::new(),
                order_by: query.order_by.clone(),
                offset,
                limit: None,
            },
        };

        Ok(Self { strategy })
    }

    pub(super) async fn push_row(
        &mut self,
        columns: &[String],
        row: Vec<ScalarValue>,
        params: &[Option<String>],
    ) -> Result<bool, EngineError> {
        match &mut self.strategy {
            RowCollectionStrategy::All { rows, order_by, .. } => {
                let keys = if order_by.is_empty() {
                    Vec::new()
                } else {
                    resolve_order_keys(order_by, columns, &row, params).await?
                };
                rows.push(CollectedRow { keys, row });
                Ok(true)
            }
            RowCollectionStrategy::LimitOnly {
                rows,
                offset,
                limit,
            } => {
                let target = offset.saturating_add(*limit);
                if rows.len() < target {
                    rows.push(row);
                }
                Ok(rows.len() < target)
            }
            RowCollectionStrategy::TopN {
                heap,
                order_by,
                capacity,
                sequence,
                ..
            } => {
                if *capacity == 0 {
                    return Ok(true);
                }
                let keys = resolve_order_keys(order_by, columns, &row, params).await?;
                let entry = TopNEntry::new(keys, order_by, row, *sequence);
                *sequence += 1;
                if heap.len() < *capacity {
                    heap.push(entry);
                } else if heap.peek().is_some_and(|worst| entry < *worst) {
                    let _ = heap.pop();
                    heap.push(entry);
                }
                Ok(true)
            }
        }
    }

    pub(super) fn finish(self) -> Vec<Vec<ScalarValue>> {
        match self.strategy {
            RowCollectionStrategy::All {
                mut rows,
                order_by,
                offset,
                limit,
            } => {
                if !order_by.is_empty() {
                    rows.sort_by(|left, right| {
                        compare_order_keys(&left.keys, &right.keys, &order_by)
                    });
                }
                let mut rows = rows.into_iter().map(|row| row.row).collect::<Vec<_>>();
                apply_offset_limit_to_rows(&mut rows, offset, limit);
                rows
            }
            RowCollectionStrategy::LimitOnly {
                mut rows,
                offset,
                limit,
            } => {
                apply_offset_limit_to_rows(&mut rows, offset, Some(limit));
                rows
            }
            RowCollectionStrategy::TopN {
                heap,
                offset,
                limit,
                ..
            } => {
                let mut rows = heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(|entry| entry.row)
                    .collect::<Vec<_>>();
                apply_offset_limit_to_rows(&mut rows, offset, Some(limit));
                rows
            }
        }
    }
}

impl SimpleTopNCollector {
    pub(super) async fn new(
        query: &Query,
        columns: &[String],
        params: &[Option<String>],
    ) -> Result<Option<Self>, EngineError> {
        if query.order_by.is_empty() {
            return Ok(None);
        }
        let Some(limit_expr) = &query.limit else {
            return Ok(None);
        };

        let offset = if let Some(expr) = &query.offset {
            parse_non_negative_int(
                &eval_expr(expr, &EvalScope::default(), params).await?,
                "OFFSET",
            )?
        } else {
            0usize
        };
        let limit = parse_non_negative_int(
            &eval_expr(limit_expr, &EvalScope::default(), params).await?,
            "LIMIT",
        )?;
        let order_indices = query
            .order_by
            .iter()
            .map(|spec| match &spec.expr {
                Expr::Identifier(parts) => columns.iter().position(|column| {
                    column.eq_ignore_ascii_case(&parts.join("."))
                        || parts
                            .last()
                            .is_some_and(|short| column.eq_ignore_ascii_case(short))
                }),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();

        let Some(order_indices) = order_indices else {
            return Ok(None);
        };

        Ok(Some(Self {
            heap: BinaryHeap::new(),
            order_by: query.order_by.clone(),
            order_indices,
            capacity: offset.saturating_add(limit),
            offset,
            limit,
            sequence: 0,
        }))
    }

    pub(super) fn push_batch(&mut self, batch: &ColumnBatch) {
        if self.capacity == 0 {
            return;
        }

        for row_idx in 0..batch.row_count {
            self.push_row(batch, row_idx);
        }
    }

    pub(super) fn push_selected_rows(&mut self, batch: &ColumnBatch, row_indices: &[usize]) {
        if self.capacity == 0 {
            return;
        }

        for &row_idx in row_indices {
            self.push_row(batch, row_idx);
        }
    }

    pub(super) fn finish(self) -> Vec<Vec<ScalarValue>> {
        let mut rows = self
            .heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.row)
            .collect::<Vec<_>>();
        apply_offset_limit_to_rows(&mut rows, self.offset, Some(self.limit));
        rows
    }

    fn push_row(&mut self, batch: &ColumnBatch, row_idx: usize) {
        if row_idx >= batch.row_count {
            return;
        }

        let keys = self
            .order_indices
            .iter()
            .map(|column_idx| batch.columns[*column_idx].value_at(row_idx))
            .collect::<Vec<_>>();
        let probe = TopNEntry::new(keys, &self.order_by, Vec::new(), self.sequence);
        self.sequence += 1;

        if self.heap.len() < self.capacity {
            let row = batch
                .columns
                .iter()
                .map(|column| column.value_at(row_idx))
                .collect::<Vec<_>>();
            self.heap.push(TopNEntry { row, ..probe });
        } else if self.heap.peek().is_some_and(|worst| probe < *worst) {
            let row = batch
                .columns
                .iter()
                .map(|column| column.value_at(row_idx))
                .collect::<Vec<_>>();
            let _ = self.heap.pop();
            self.heap.push(TopNEntry { row, ..probe });
        }
    }
}

impl OffsetTopNCollector {
    pub(super) async fn new(
        query: &Query,
        columns: &[String],
        params: &[Option<String>],
    ) -> Result<Option<Self>, EngineError> {
        let Some(base) = SimpleTopNCollector::new(query, columns, params).await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            heap: BinaryHeap::new(),
            order_by: base.order_by,
            order_indices: base.order_indices,
            capacity: base.capacity,
            offset: base.offset,
            limit: base.limit,
            sequence: 0,
        }))
    }

    pub(super) fn push_batch(&mut self, batch: &ColumnBatch, offsets: &[usize]) {
        if self.capacity == 0 {
            return;
        }

        for (row_idx, row_offset) in offsets.iter().copied().enumerate().take(batch.row_count) {
            self.push_row(batch, row_idx, row_offset);
        }
    }

    pub(super) fn push_selected_rows(
        &mut self,
        batch: &ColumnBatch,
        row_indices: &[usize],
        offsets: &[usize],
    ) {
        if self.capacity == 0 {
            return;
        }

        for (&row_idx, &row_offset) in row_indices.iter().zip(offsets.iter()) {
            self.push_row(batch, row_idx, row_offset);
        }
    }

    pub(super) fn push_matching_text_rows<F>(
        &mut self,
        batch: &ColumnBatch,
        column_idx: usize,
        offsets: &[usize],
        mut matches: F,
    ) where
        F: FnMut(&str) -> bool,
    {
        let Some(TypedColumn::Text(values, nulls)) = batch.columns.get(column_idx) else {
            return;
        };
        for (row_idx, ((value, is_null), row_offset)) in values
            .iter()
            .zip(nulls.iter().copied())
            .zip(offsets.iter().copied())
            .enumerate()
        {
            if !is_null && matches(value) {
                self.push_row(batch, row_idx, row_offset);
            }
        }
    }

    fn push_row(&mut self, batch: &ColumnBatch, row_idx: usize, row_offset: usize) {
        if self.capacity == 0 {
            return;
        }

        if row_idx >= batch.row_count {
            return;
        }

        let keys = self
            .order_indices
            .iter()
            .map(|column_idx| batch.columns[*column_idx].value_at(row_idx))
            .collect::<Vec<_>>();
        let probe = OffsetTopNEntry::new(keys, &self.order_by, row_offset, self.sequence);
        self.sequence += 1;

        if self.heap.len() < self.capacity {
            self.heap.push(probe);
        } else if self.heap.peek().is_some_and(|worst| probe < *worst) {
            let _ = self.heap.pop();
            self.heap.push(probe);
        }
    }

    pub(super) fn finish(self) -> Vec<usize> {
        let mut offsets = self
            .heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.row_offset)
            .collect::<Vec<_>>();
        apply_offset_limit_items(&mut offsets, self.offset, Some(self.limit));
        offsets
    }
}

impl TopNEntry {
    fn new(
        keys: Vec<ScalarValue>,
        order_by: &[OrderByExpr],
        row: Vec<ScalarValue>,
        sequence: usize,
    ) -> Self {
        let keys = keys
            .into_iter()
            .enumerate()
            .map(|(idx, value)| TopNKeyPart {
                value,
                descending: order_by[idx].ascending == Some(false),
            })
            .collect();
        Self {
            keys,
            row,
            sequence,
        }
    }
}

impl OffsetTopNEntry {
    fn new(
        keys: Vec<ScalarValue>,
        order_by: &[OrderByExpr],
        row_offset: usize,
        sequence: usize,
    ) -> Self {
        let keys = keys
            .into_iter()
            .enumerate()
            .map(|(idx, value)| TopNKeyPart {
                value,
                descending: order_by[idx].ascending == Some(false),
            })
            .collect();
        Self {
            keys,
            row_offset,
            sequence,
        }
    }
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TopNEntry {}

impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        for (left, right) in self.keys.iter().zip(&other.keys) {
            let ord = scalar_cmp(&left.value, &right.value);
            let ord = if left.descending { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.sequence.cmp(&other.sequence)
    }
}

impl PartialEq for OffsetTopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OffsetTopNEntry {}

impl PartialOrd for OffsetTopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetTopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        for (left, right) in self.keys.iter().zip(&other.keys) {
            let ord = scalar_cmp(&left.value, &right.value);
            let ord = if left.descending { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.sequence.cmp(&other.sequence)
    }
}

async fn resolve_order_keys(
    order_by: &[OrderByExpr],
    columns: &[String],
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<Vec<ScalarValue>, EngineError> {
    let scope = EvalScope::from_output_row(columns, row);
    let mut keys = Vec::with_capacity(order_by.len());
    for spec in order_by {
        keys.push(resolve_order_key(&spec.expr, &scope, columns, row, params).await?);
    }
    Ok(keys)
}

fn apply_offset_limit_items<T>(items: &mut Vec<T>, offset: usize, limit: Option<usize>) {
    if offset > 0 {
        if offset >= items.len() {
            items.clear();
            return;
        }
        items.drain(0..offset);
    }

    if let Some(limit) = limit
        && limit < items.len()
    {
        items.truncate(limit);
    }
}

fn apply_offset_limit_to_rows(
    rows: &mut Vec<Vec<ScalarValue>>,
    offset: usize,
    limit: Option<usize>,
) {
    apply_offset_limit_items(rows, offset, limit);
}

/// Collect identifiers from ORDER BY that are not present in the SELECT output columns.
/// These need to be temporarily added to the SELECT for sorting.
pub(super) fn collect_extra_order_by_columns(query: &Query) -> Vec<Expr> {
    let select_columns: HashSet<String> = match &query.body {
        QueryExpr::Select(select) => select
            .targets
            .iter()
            .filter_map(|target| {
                if let Some(alias) = &target.alias {
                    return Some(alias.to_ascii_lowercase());
                }
                if let Expr::Identifier(parts) = &target.expr {
                    parts.last().map(|p| p.to_ascii_lowercase())
                } else {
                    None
                }
            })
            .collect(),
        _ => return Vec::new(),
    };

    let mut extras = Vec::new();
    for spec in &query.order_by {
        if let Expr::Identifier(parts) = &spec.expr
            && parts.len() == 1
        {
            let name = parts[0].to_ascii_lowercase();
            if !select_columns.contains(&name) {
                extras.push(spec.expr.clone());
            }
        }
    }
    extras
}

/// Augment a SELECT query body to include extra ORDER BY columns as hidden trailing targets.
/// Returns the modified query body and the number of hidden columns added.
pub(super) fn augment_select_for_order_by(body: &QueryExpr, extras: &[Expr]) -> (QueryExpr, usize) {
    if extras.is_empty() {
        return (body.clone(), 0);
    }
    if let QueryExpr::Select(select) = body {
        let mut new_select = select.clone();
        let mut count = 0;
        for expr in extras {
            new_select.targets.push(SelectItem {
                expr: expr.clone(),
                alias: None,
            });
            count += 1;
        }
        (QueryExpr::Select(new_select), count)
    } else {
        (body.clone(), 0)
    }
}

pub(super) async fn apply_order_by(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    if query.order_by.is_empty() || result.rows.is_empty() {
        return Ok(());
    }

    let columns = result.columns.clone();
    let mut decorated = Vec::with_capacity(result.rows.len());
    for row in result.rows.drain(..) {
        let scope = EvalScope::from_output_row(&columns, &row);
        let mut keys = Vec::with_capacity(query.order_by.len());
        for spec in &query.order_by {
            keys.push(resolve_order_key(&spec.expr, &scope, &columns, &row, params).await?);
        }
        decorated.push((keys, row));
    }

    decorated.sort_by(|(ka, _), (kb, _)| compare_order_keys(ka, kb, &query.order_by));
    result.rows = decorated.into_iter().map(|(_, row)| row).collect();
    Ok(())
}

pub fn compare_order_keys(
    left: &[ScalarValue],
    right: &[ScalarValue],
    specs: &[crate::parser::ast::OrderByExpr],
) -> Ordering {
    for (idx, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        let ord = scalar_cmp(l, r);
        if ord != Ordering::Equal {
            if specs[idx].ascending == Some(false) {
                return ord.reverse();
            }
            return ord;
        }
    }
    Ordering::Equal
}

pub(super) fn scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use ScalarValue::{Bool, Float, Int, Null, Text};
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Text(x), Text(y)) => x.cmp(y),
        (Int(x), Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Float(x), Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        _ => a.render().cmp(&b.render()),
    }
}

pub(super) async fn resolve_order_key(
    expr: &Expr,
    scope: &EvalScope,
    columns: &[String],
    row: &[ScalarValue],
    params: &[Option<String>],
) -> Result<ScalarValue, EngineError> {
    if let Expr::Integer(pos) = expr
        && *pos > 0
    {
        let idx = (*pos as usize).saturating_sub(1);
        if idx < row.len() {
            return Ok(row[idx].clone());
        }
    }

    if let Expr::Identifier(parts) = expr
        && parts.len() == 1
    {
        let want = parts[0].to_ascii_lowercase();
        if let Some((idx, _)) = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.to_ascii_lowercase() == want)
        {
            return Ok(row[idx].clone());
        }
    }
    if let Expr::Identifier(parts) = expr
        && parts.len() > 1
    {
        let want = parts
            .last()
            .map(|part| part.to_ascii_lowercase())
            .unwrap_or_default();
        if let Some((idx, _)) = columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.to_ascii_lowercase() == want)
        {
            return Ok(row[idx].clone());
        }
    }

    eval_expr(expr, scope, params).await
}

pub(super) async fn apply_offset_limit(
    result: &mut QueryResult,
    query: &Query,
    params: &[Option<String>],
) -> Result<(), EngineError> {
    let offset = if let Some(expr) = &query.offset {
        parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "OFFSET",
        )?
    } else {
        0usize
    };

    let limit = if let Some(expr) = &query.limit {
        Some(parse_non_negative_int(
            &eval_expr(expr, &EvalScope::default(), params).await?,
            "LIMIT",
        )?)
    } else {
        None
    };

    if offset > 0 {
        if offset >= result.rows.len() {
            result.rows.clear();
            return Ok(());
        }
        result.rows = result.rows[offset..].to_vec();
    }

    if let Some(limit) = limit
        && limit < result.rows.len()
    {
        result.rows.truncate(limit);
    }

    Ok(())
}

pub fn parse_non_negative_int(value: &ScalarValue, what: &str) -> Result<usize, EngineError> {
    match value {
        ScalarValue::Int(v) if *v >= 0 => Ok(*v as usize),
        ScalarValue::Text(v) => {
            let parsed = v.parse::<usize>().map_err(|_| EngineError {
                message: format!("{what} must be a non-negative integer"),
            })?;
            Ok(parsed)
        }
        _ => Err(EngineError {
            message: format!("{what} must be a non-negative integer"),
        }),
    }
}
