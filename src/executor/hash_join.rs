use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use rust_decimal::Decimal;

use crate::executor::column_batch::{ColumnBatch, TypedColumn};
use crate::executor::exec_expr::{EvalScope, eval_expr};
use crate::parser::ast::{BinaryOp, Expr, JoinType};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::{compare_values_for_predicate, truthy};

#[derive(Debug, Clone)]
pub(crate) struct HashJoinExecutor {
    join_type: JoinType,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    residual: Option<Expr>,
}

#[derive(Debug, Clone)]
pub(crate) struct HashJoinResult {
    pub(crate) batch: ColumnBatch,
    pub(crate) row_pairs: Vec<(Option<usize>, Option<usize>)>,
}

impl HashJoinExecutor {
    pub(crate) fn new(
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        residual: Option<Expr>,
    ) -> Self {
        Self {
            join_type,
            left_key_indices,
            right_key_indices,
            residual,
        }
    }

    pub(crate) async fn execute(
        &self,
        left: &ColumnBatch,
        right: &ColumnBatch,
        left_rows: &[EvalScope],
        right_rows: &[EvalScope],
        params: &[Option<String>],
    ) -> Result<HashJoinResult, EngineError> {
        let mut build_map: HashMap<u64, Vec<usize>> = HashMap::new();
        for right_row_idx in 0..right.row_count {
            if row_has_null_key(right, &self.right_key_indices, right_row_idx) {
                continue;
            }
            let hash = hash_typed_key(right, &self.right_key_indices, right_row_idx);
            build_map.entry(hash).or_default().push(right_row_idx);
        }

        let mut builders = left
            .columns
            .iter()
            .chain(&right.columns)
            .map(|column| ColumnBuilder::for_column(column))
            .collect::<Vec<_>>();
        let mut row_pairs = Vec::new();
        let mut left_matched = vec![false; left.row_count];
        let mut right_matched = vec![false; right.row_count];

        for (left_row_idx, left_matched_flag) in left_matched.iter_mut().enumerate() {
            if row_has_null_key(left, &self.left_key_indices, left_row_idx) {
                continue;
            }

            let hash = hash_typed_key(left, &self.left_key_indices, left_row_idx);
            let Some(candidates) = build_map.get(&hash) else {
                continue;
            };

            for right_row_idx in self
                .matching_right_rows(
                    left,
                    left_row_idx,
                    right,
                    candidates,
                    left_rows,
                    right_rows,
                    params,
                )
                .await?
            {
                *left_matched_flag = true;
                right_matched[right_row_idx] = true;
                append_pair(&mut builders, left, left_row_idx, right, right_row_idx);
                row_pairs.push((Some(left_row_idx), Some(right_row_idx)));
            }
        }

        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            for (left_row_idx, matched) in left_matched.iter().copied().enumerate() {
                if matched {
                    continue;
                }
                append_left_with_right_nulls(&mut builders, left, left_row_idx, right);
                row_pairs.push((Some(left_row_idx), None));
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (right_row_idx, matched) in right_matched.iter().copied().enumerate() {
                if matched {
                    continue;
                }
                append_left_nulls_with_right(&mut builders, left, right, right_row_idx);
                row_pairs.push((None, Some(right_row_idx)));
            }
        }

        let batch = ColumnBatch {
            columns: builders.into_iter().map(ColumnBuilder::finish).collect(),
            column_names: left
                .column_names
                .iter()
                .chain(&right.column_names)
                .cloned()
                .collect(),
            row_count: row_pairs.len(),
        };

        Ok(HashJoinResult { batch, row_pairs })
    }

    async fn matching_right_rows(
        &self,
        left: &ColumnBatch,
        left_row_idx: usize,
        right: &ColumnBatch,
        candidates: &[usize],
        left_rows: &[EvalScope],
        right_rows: &[EvalScope],
        params: &[Option<String>],
    ) -> Result<Vec<usize>, EngineError> {
        let mut matches = Vec::new();
        for right_row_idx in candidates.iter().copied() {
            if !key_rows_equal(
                left,
                &self.left_key_indices,
                left_row_idx,
                right,
                &self.right_key_indices,
                right_row_idx,
            )? {
                continue;
            }

            if let Some(residual) = &self.residual {
                let scope =
                    combine_join_scopes(&left_rows[left_row_idx], &right_rows[right_row_idx]);
                if !truthy(&eval_expr(residual, &scope, params).await?) {
                    continue;
                }
            }

            matches.push(right_row_idx);
        }
        Ok(matches)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone)]
enum ColumnBuilder {
    Bool(Vec<bool>, Vec<bool>),
    Int64(Vec<i64>, Vec<bool>),
    Float64(Vec<f64>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Numeric(Vec<Decimal>, Vec<bool>),
    Mixed(Vec<ScalarValue>),
}

impl ColumnBuilder {
    fn for_column(column: &TypedColumn) -> Self {
        match column {
            TypedColumn::Bool(_, _) => Self::Bool(Vec::new(), Vec::new()),
            TypedColumn::Int64(_, _) => Self::Int64(Vec::new(), Vec::new()),
            TypedColumn::Float64(_, _) => Self::Float64(Vec::new(), Vec::new()),
            TypedColumn::Date(_, _) => Self::Mixed(Vec::new()),
            TypedColumn::Text(_, _) => Self::Text(Vec::new(), Vec::new()),
            TypedColumn::Numeric(_, _) => Self::Numeric(Vec::new(), Vec::new()),
            TypedColumn::Mixed(_) => Self::Mixed(Vec::new()),
        }
    }

    fn append_from(&mut self, column: &TypedColumn, row_idx: usize) {
        match (self, column) {
            (Self::Bool(values, nulls), TypedColumn::Bool(source, source_nulls)) => {
                values.push(source[row_idx]);
                nulls.push(source_nulls[row_idx]);
            }
            (Self::Int64(values, nulls), TypedColumn::Int64(source, source_nulls)) => {
                values.push(source[row_idx]);
                nulls.push(source_nulls[row_idx]);
            }
            (Self::Float64(values, nulls), TypedColumn::Float64(source, source_nulls)) => {
                values.push(source[row_idx]);
                nulls.push(source_nulls[row_idx]);
            }
            (Self::Text(values, nulls), TypedColumn::Text(source, source_nulls)) => {
                values.push(source[row_idx].clone());
                nulls.push(source_nulls[row_idx]);
            }
            (Self::Numeric(values, nulls), TypedColumn::Numeric(source, source_nulls)) => {
                values.push(source[row_idx]);
                nulls.push(source_nulls[row_idx]);
            }
            (Self::Mixed(values), _) => values.push(scalar_value_at(column, row_idx)),
            (builder, source) => {
                let mut values = match std::mem::replace(builder, Self::Mixed(Vec::new())) {
                    Self::Bool(values, nulls) => typed_values_to_scalars_bool(values, nulls),
                    Self::Int64(values, nulls) => typed_values_to_scalars_int(values, nulls),
                    Self::Float64(values, nulls) => typed_values_to_scalars_float(values, nulls),
                    Self::Text(values, nulls) => typed_values_to_scalars_text(values, nulls),
                    Self::Numeric(values, nulls) => typed_values_to_scalars_numeric(values, nulls),
                    Self::Mixed(values) => values,
                };
                values.push(scalar_value_at(source, row_idx));
                *builder = Self::Mixed(values);
            }
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(values, nulls) => {
                values.push(false);
                nulls.push(true);
            }
            Self::Int64(values, nulls) => {
                values.push(0);
                nulls.push(true);
            }
            Self::Float64(values, nulls) => {
                values.push(0.0);
                nulls.push(true);
            }
            Self::Text(values, nulls) => {
                values.push(String::new());
                nulls.push(true);
            }
            Self::Numeric(values, nulls) => {
                values.push(Decimal::ZERO);
                nulls.push(true);
            }
            Self::Mixed(values) => values.push(ScalarValue::Null),
        }
    }

    fn finish(self) -> TypedColumn {
        match self {
            Self::Bool(values, nulls) => TypedColumn::Bool(values, nulls),
            Self::Int64(values, nulls) => TypedColumn::Int64(values, nulls),
            Self::Float64(values, nulls) => TypedColumn::Float64(values, nulls),
            Self::Text(values, nulls) => TypedColumn::Text(values, nulls),
            Self::Numeric(values, nulls) => TypedColumn::Numeric(values, nulls),
            Self::Mixed(values) => TypedColumn::Mixed(values),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn extract_equi_join_keys(
    on_expr: &Expr,
    left_columns: &[String],
    right_columns: &[String],
) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
    extract_equi_join_keys_inner(on_expr, |parts| {
        resolve_identifier_side_from_columns(parts, left_columns, right_columns)
    })
}

pub(crate) fn extract_equi_join_keys_scoped(
    on_expr: &Expr,
    left_columns: &[String],
    right_columns: &[String],
    left_scope: &EvalScope,
    right_scope: &EvalScope,
) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
    extract_equi_join_keys_inner(on_expr, |parts| {
        resolve_identifier_side(parts, left_columns, right_columns, left_scope, right_scope)
    })
}

fn extract_equi_join_keys_inner(
    on_expr: &Expr,
    resolve_identifier: impl Fn(&[String]) -> Option<(JoinSide, usize)>,
) -> Option<(Vec<usize>, Vec<usize>, Option<Expr>)> {
    let conjuncts = decompose_and_conjuncts(on_expr);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residuals = Vec::new();

    for conjunct in conjuncts {
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = &conjunct
        else {
            residuals.push(conjunct);
            continue;
        };

        let Some((left_side, left_idx)) = extract_identifier_side(left, &resolve_identifier) else {
            residuals.push(conjunct);
            continue;
        };
        let Some((right_side, right_idx)) = extract_identifier_side(right, &resolve_identifier)
        else {
            residuals.push(conjunct);
            continue;
        };

        match (left_side, right_side) {
            (JoinSide::Left, JoinSide::Right) => {
                left_keys.push(left_idx);
                right_keys.push(right_idx);
            }
            (JoinSide::Right, JoinSide::Left) => {
                left_keys.push(right_idx);
                right_keys.push(left_idx);
            }
            _ => residuals.push(conjunct),
        }
    }

    if left_keys.is_empty() {
        return None;
    }

    Some((left_keys, right_keys, compose_and_conjuncts(residuals)))
}

fn extract_identifier_side(
    expr: &Expr,
    resolve_identifier: &impl Fn(&[String]) -> Option<(JoinSide, usize)>,
) -> Option<(JoinSide, usize)> {
    match expr {
        Expr::Identifier(parts) => resolve_identifier(parts),
        Expr::Cast { expr, .. } => extract_identifier_side(expr, resolve_identifier),
        _ => None,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_identifier_side_from_columns(
    parts: &[String],
    left_columns: &[String],
    right_columns: &[String],
) -> Option<(JoinSide, usize)> {
    let left_idx = column_index_for_identifier(left_columns, parts);
    let right_idx = column_index_for_identifier(right_columns, parts);
    match (left_idx, right_idx) {
        (Some(idx), None) => Some((JoinSide::Left, idx)),
        (None, Some(idx)) => Some((JoinSide::Right, idx)),
        _ => None,
    }
}

fn resolve_identifier_side(
    parts: &[String],
    left_columns: &[String],
    right_columns: &[String],
    left_scope: &EvalScope,
    right_scope: &EvalScope,
) -> Option<(JoinSide, usize)> {
    let qualified_name = parts
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".");
    let short_name = parts.last()?.to_ascii_lowercase();
    let lookup_name = if parts.len() > 1 {
        qualified_name.as_str()
    } else {
        short_name.as_str()
    };

    let left_matches = left_scope.has_column(lookup_name)
        && column_index_for_identifier(left_columns, parts).is_some();
    let right_matches = right_scope.has_column(lookup_name)
        && column_index_for_identifier(right_columns, parts).is_some();

    match (left_matches, right_matches) {
        (true, false) => {
            column_index_for_identifier(left_columns, parts).map(|idx| (JoinSide::Left, idx))
        }
        (false, true) => {
            column_index_for_identifier(right_columns, parts).map(|idx| (JoinSide::Right, idx))
        }
        _ => None,
    }
}

fn column_index_for_identifier(columns: &[String], parts: &[String]) -> Option<usize> {
    let qualified = parts
        .iter()
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".");
    let short = parts.last()?.to_ascii_lowercase();

    let exact_matches = columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| column.eq_ignore_ascii_case(&qualified).then_some(idx))
        .collect::<Vec<_>>();
    if exact_matches.len() == 1 {
        return exact_matches.first().copied();
    }
    if exact_matches.len() > 1 {
        return None;
    }

    let short_matches = columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            let candidate = column
                .rsplit('.')
                .next()
                .unwrap_or(column)
                .to_ascii_lowercase();
            (candidate == short).then_some(idx)
        })
        .collect::<Vec<_>>();
    if short_matches.len() == 1 {
        short_matches.first().copied()
    } else {
        None
    }
}

fn decompose_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    decompose_and_conjuncts_inner(expr, &mut conjuncts);
    conjuncts
}

fn decompose_and_conjuncts_inner(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = expr
    {
        decompose_and_conjuncts_inner(left, out);
        decompose_and_conjuncts_inner(right, out);
    } else {
        out.push(expr.clone());
    }
}

fn compose_and_conjuncts(mut conjuncts: Vec<Expr>) -> Option<Expr> {
    if conjuncts.is_empty() {
        return None;
    }

    let mut expr = conjuncts.remove(0);
    for conjunct in conjuncts {
        expr = Expr::Binary {
            left: Box::new(expr),
            op: BinaryOp::And,
            right: Box::new(conjunct),
        };
    }
    Some(expr)
}

fn row_has_null_key(batch: &ColumnBatch, key_indices: &[usize], row_idx: usize) -> bool {
    key_indices
        .iter()
        .any(|column_idx| is_null_at(&batch.columns[*column_idx], row_idx))
}

fn hash_typed_key(batch: &ColumnBatch, key_indices: &[usize], row_idx: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    for column_idx in key_indices {
        hash_typed_value(&batch.columns[*column_idx], row_idx, &mut hasher);
    }
    hasher.finish()
}

fn hash_typed_value(column: &TypedColumn, row_idx: usize, hasher: &mut DefaultHasher) {
    match column {
        TypedColumn::Bool(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                1u8.hash(hasher);
                values[row_idx].hash(hasher);
            }
        }
        TypedColumn::Int64(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                2u8.hash(hasher);
                values[row_idx].hash(hasher);
            }
        }
        TypedColumn::Float64(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                3u8.hash(hasher);
                values[row_idx].to_bits().hash(hasher);
            }
        }
        TypedColumn::Date(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                6u8.hash(hasher);
                values[row_idx].hash(hasher);
            }
        }
        TypedColumn::Text(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                4u8.hash(hasher);
                values[row_idx].hash(hasher);
            }
        }
        TypedColumn::Numeric(values, nulls) => {
            if nulls[row_idx] {
                0u8.hash(hasher);
            } else {
                5u8.hash(hasher);
                values[row_idx].normalize().to_string().hash(hasher);
            }
        }
        TypedColumn::Mixed(values) => hash_scalar_value(&values[row_idx], hasher),
    }
}

fn hash_scalar_value(value: &ScalarValue, hasher: &mut DefaultHasher) {
    match value {
        ScalarValue::Null => 0u8.hash(hasher),
        ScalarValue::Bool(flag) => {
            1u8.hash(hasher);
            flag.hash(hasher);
        }
        ScalarValue::Int(number) => {
            2u8.hash(hasher);
            number.hash(hasher);
        }
        ScalarValue::Float(number) => {
            3u8.hash(hasher);
            number.to_bits().hash(hasher);
        }
        ScalarValue::Text(text) => {
            4u8.hash(hasher);
            text.hash(hasher);
        }
        ScalarValue::Numeric(decimal) => {
            5u8.hash(hasher);
            decimal.normalize().to_string().hash(hasher);
        }
        other => {
            6u8.hash(hasher);
            format!("{other:?}").hash(hasher);
        }
    }
}

fn key_rows_equal(
    left: &ColumnBatch,
    left_key_indices: &[usize],
    left_row_idx: usize,
    right: &ColumnBatch,
    right_key_indices: &[usize],
    right_row_idx: usize,
) -> Result<bool, EngineError> {
    for (left_key_idx, right_key_idx) in left_key_indices.iter().zip(right_key_indices) {
        if !typed_values_equal(
            &left.columns[*left_key_idx],
            left_row_idx,
            &right.columns[*right_key_idx],
            right_row_idx,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn typed_values_equal(
    left: &TypedColumn,
    left_row_idx: usize,
    right: &TypedColumn,
    right_row_idx: usize,
) -> Result<bool, EngineError> {
    let ordering = match (left, right) {
        (
            TypedColumn::Bool(left_values, left_nulls),
            TypedColumn::Bool(right_values, right_nulls),
        ) if !left_nulls[left_row_idx] && !right_nulls[right_row_idx] => {
            Some(left_values[left_row_idx].cmp(&right_values[right_row_idx]))
        }
        (
            TypedColumn::Int64(left_values, left_nulls),
            TypedColumn::Int64(right_values, right_nulls),
        ) if !left_nulls[left_row_idx] && !right_nulls[right_row_idx] => {
            Some(left_values[left_row_idx].cmp(&right_values[right_row_idx]))
        }
        (
            TypedColumn::Float64(left_values, left_nulls),
            TypedColumn::Float64(right_values, right_nulls),
        ) if !left_nulls[left_row_idx] && !right_nulls[right_row_idx] => {
            left_values[left_row_idx].partial_cmp(&right_values[right_row_idx])
        }
        (
            TypedColumn::Text(left_values, left_nulls),
            TypedColumn::Text(right_values, right_nulls),
        ) if !left_nulls[left_row_idx] && !right_nulls[right_row_idx] => {
            Some(left_values[left_row_idx].cmp(&right_values[right_row_idx]))
        }
        (
            TypedColumn::Numeric(left_values, left_nulls),
            TypedColumn::Numeric(right_values, right_nulls),
        ) if !left_nulls[left_row_idx] && !right_nulls[right_row_idx] => {
            Some(left_values[left_row_idx].cmp(&right_values[right_row_idx]))
        }
        _ => Some(compare_values_for_predicate(
            &scalar_value_at(left, left_row_idx),
            &scalar_value_at(right, right_row_idx),
        )?),
    };
    Ok(matches!(ordering, Some(Ordering::Equal)))
}

fn scalar_value_at(column: &TypedColumn, row_idx: usize) -> ScalarValue {
    match column {
        TypedColumn::Bool(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Bool(values[row_idx])
            }
        }
        TypedColumn::Int64(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Int(values[row_idx])
            }
        }
        TypedColumn::Float64(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Float(values[row_idx])
            }
        }
        TypedColumn::Date(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Text(crate::utils::adt::datetime::format_date(
                    crate::utils::adt::datetime::datetime_from_epoch_seconds(
                        i64::from(values[row_idx]).saturating_mul(86_400),
                    )
                    .date,
                ))
            }
        }
        TypedColumn::Text(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Text(values[row_idx].clone())
            }
        }
        TypedColumn::Numeric(values, nulls) => {
            if nulls[row_idx] {
                ScalarValue::Null
            } else {
                ScalarValue::Numeric(values[row_idx])
            }
        }
        TypedColumn::Mixed(values) => values[row_idx].clone(),
    }
}

fn is_null_at(column: &TypedColumn, row_idx: usize) -> bool {
    match column {
        TypedColumn::Bool(_, nulls)
        | TypedColumn::Int64(_, nulls)
        | TypedColumn::Float64(_, nulls)
        | TypedColumn::Date(_, nulls)
        | TypedColumn::Text(_, nulls)
        | TypedColumn::Numeric(_, nulls) => nulls[row_idx],
        TypedColumn::Mixed(values) => matches!(values[row_idx], ScalarValue::Null),
    }
}

fn append_pair(
    builders: &mut [ColumnBuilder],
    left: &ColumnBatch,
    left_row_idx: usize,
    right: &ColumnBatch,
    right_row_idx: usize,
) {
    let left_width = left.columns.len();
    for (builder, column) in builders.iter_mut().zip(&left.columns) {
        builder.append_from(column, left_row_idx);
    }
    for (builder, column) in builders[left_width..].iter_mut().zip(&right.columns) {
        builder.append_from(column, right_row_idx);
    }
}

fn append_left_with_right_nulls(
    builders: &mut [ColumnBuilder],
    left: &ColumnBatch,
    left_row_idx: usize,
    right: &ColumnBatch,
) {
    let left_width = left.columns.len();
    for (builder, column) in builders.iter_mut().zip(&left.columns) {
        builder.append_from(column, left_row_idx);
    }
    for builder in &mut builders[left_width..left_width + right.columns.len()] {
        builder.append_null();
    }
}

fn append_left_nulls_with_right(
    builders: &mut [ColumnBuilder],
    left: &ColumnBatch,
    right: &ColumnBatch,
    right_row_idx: usize,
) {
    let left_width = left.columns.len();
    for builder in &mut builders[..left_width] {
        builder.append_null();
    }
    for (builder, column) in builders[left_width..].iter_mut().zip(&right.columns) {
        builder.append_from(column, right_row_idx);
    }
}

fn typed_values_to_scalars_bool(values: Vec<bool>, nulls: Vec<bool>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .zip(nulls)
        .map(|(value, is_null)| {
            if is_null {
                ScalarValue::Null
            } else {
                ScalarValue::Bool(value)
            }
        })
        .collect()
}

fn typed_values_to_scalars_int(values: Vec<i64>, nulls: Vec<bool>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .zip(nulls)
        .map(|(value, is_null)| {
            if is_null {
                ScalarValue::Null
            } else {
                ScalarValue::Int(value)
            }
        })
        .collect()
}

fn typed_values_to_scalars_float(values: Vec<f64>, nulls: Vec<bool>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .zip(nulls)
        .map(|(value, is_null)| {
            if is_null {
                ScalarValue::Null
            } else {
                ScalarValue::Float(value)
            }
        })
        .collect()
}

fn typed_values_to_scalars_text(values: Vec<String>, nulls: Vec<bool>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .zip(nulls)
        .map(|(value, is_null)| {
            if is_null {
                ScalarValue::Null
            } else {
                ScalarValue::Text(value)
            }
        })
        .collect()
}

fn typed_values_to_scalars_numeric(values: Vec<Decimal>, nulls: Vec<bool>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .zip(nulls)
        .map(|(value, is_null)| {
            if is_null {
                ScalarValue::Null
            } else {
                ScalarValue::Numeric(value)
            }
        })
        .collect()
}

fn combine_join_scopes(left: &EvalScope, right: &EvalScope) -> EvalScope {
    let mut out = left.clone();
    out.merge(right);
    out
}

pub(crate) fn using_join_key_indices(
    using_columns: &[String],
    left_columns: &[String],
    right_columns: &[String],
) -> Option<(Vec<usize>, Vec<usize>)> {
    let mut left_keys = Vec::with_capacity(using_columns.len());
    let mut right_keys = Vec::with_capacity(using_columns.len());
    for column in using_columns {
        let parts = vec![column.clone()];
        let left_idx = column_index_for_identifier(left_columns, &parts)?;
        let right_idx = column_index_for_identifier(right_columns, &parts)?;
        left_keys.push(left_idx);
        right_keys.push(right_idx);
    }
    Some((left_keys, right_keys))
}

#[cfg(test)]
mod tests {
    use super::{
        HashJoinExecutor, JoinSide, extract_equi_join_keys, key_rows_equal,
        resolve_identifier_side_from_columns, using_join_key_indices,
    };
    use crate::executor::column_batch::ColumnBatch;
    use crate::executor::exec_expr::EvalScope;
    use crate::parser::ast::{JoinCondition, QueryExpr, Statement, TableExpression};
    use crate::parser::sql_parser::parse_statement;
    use crate::storage::tuple::ScalarValue;

    fn batch(rows: &[Vec<ScalarValue>], columns: &[&str]) -> ColumnBatch {
        ColumnBatch::from_rows(
            rows,
            &columns
                .iter()
                .map(|column| (*column).to_string())
                .collect::<Vec<_>>(),
        )
    }

    fn join_rows(rows: &[Vec<ScalarValue>], columns: &[&str], qualifier: &str) -> Vec<EvalScope> {
        rows.iter()
            .map(|row| {
                let mut scope = EvalScope::default();
                for (column, value) in columns.iter().zip(row) {
                    scope.insert_unqualified(column, value.clone());
                    scope.insert_qualified(&format!("{qualifier}.{column}"), value.clone());
                }
                scope
            })
            .collect()
    }

    fn parse_join_on_expr(sql: &str) -> crate::parser::ast::Expr {
        let Statement::Query(query) = parse_statement(sql).expect("statement should parse") else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query");
        };
        let TableExpression::Join(join) = &select.from[0] else {
            panic!("expected join table expression");
        };
        let Some(JoinCondition::On(expr)) = &join.condition else {
            panic!("expected join ON condition");
        };
        expr.clone()
    }

    #[test]
    fn executes_inner_hash_join() {
        let left_rows = vec![
            vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
            vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
        ];
        let right_rows = vec![
            vec![ScalarValue::Int(2), ScalarValue::Text("x".to_string())],
            vec![ScalarValue::Int(1), ScalarValue::Text("y".to_string())],
        ];
        let left = batch(&left_rows, &["id", "label"]);
        let right = batch(&right_rows, &["id", "payload"]);
        let executor =
            HashJoinExecutor::new(crate::parser::ast::JoinType::Inner, vec![0], vec![0], None);

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(executor.execute(
                &left,
                &right,
                &join_rows(&left_rows, &["id", "label"], "l"),
                &join_rows(&right_rows, &["id", "payload"], "r"),
                &[],
            ))
            .expect("hash join should succeed");

        assert_eq!(
            result.batch.to_rows(),
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Int(1),
                    ScalarValue::Text("y".to_string()),
                ],
                vec![
                    ScalarValue::Int(2),
                    ScalarValue::Text("b".to_string()),
                    ScalarValue::Int(2),
                    ScalarValue::Text("x".to_string()),
                ],
            ]
        );
        assert_eq!(
            result.row_pairs,
            vec![(Some(0), Some(1)), (Some(1), Some(0))]
        );
    }

    #[test]
    fn executes_left_right_and_full_outer_hash_joins() {
        let left_rows = vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]];
        let right_rows = vec![vec![ScalarValue::Int(2)], vec![ScalarValue::Int(3)]];
        let left = batch(&left_rows, &["id"]);
        let right = batch(&right_rows, &["id"]);
        let left_scopes = join_rows(&left_rows, &["id"], "l");
        let right_scopes = join_rows(&right_rows, &["id"], "r");

        let run = |join_type| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime should build")
                .block_on(
                    HashJoinExecutor::new(join_type, vec![0], vec![0], None).execute(
                        &left,
                        &right,
                        &left_scopes,
                        &right_scopes,
                        &[],
                    ),
                )
                .expect("hash join should succeed")
                .batch
                .to_rows()
        };

        assert_eq!(
            run(crate::parser::ast::JoinType::Left),
            vec![
                vec![ScalarValue::Int(2), ScalarValue::Int(2)],
                vec![ScalarValue::Int(1), ScalarValue::Null],
            ]
        );
        assert_eq!(
            run(crate::parser::ast::JoinType::Right),
            vec![
                vec![ScalarValue::Int(2), ScalarValue::Int(2)],
                vec![ScalarValue::Null, ScalarValue::Int(3)],
            ]
        );
        assert_eq!(
            run(crate::parser::ast::JoinType::Full),
            vec![
                vec![ScalarValue::Int(2), ScalarValue::Int(2)],
                vec![ScalarValue::Int(1), ScalarValue::Null],
                vec![ScalarValue::Null, ScalarValue::Int(3)],
            ]
        );
    }

    #[test]
    fn executes_multi_key_hash_join() {
        let left = batch(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Text("x".to_string())],
                vec![ScalarValue::Int(1), ScalarValue::Text("y".to_string())],
            ],
            &["id", "tag"],
        );
        let right = batch(
            &[
                vec![ScalarValue::Int(1), ScalarValue::Text("y".to_string())],
                vec![ScalarValue::Int(1), ScalarValue::Text("x".to_string())],
            ],
            &["id", "tag"],
        );

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(
                HashJoinExecutor::new(
                    crate::parser::ast::JoinType::Inner,
                    vec![0, 1],
                    vec![0, 1],
                    None,
                )
                .execute(
                    &left,
                    &right,
                    &vec![EvalScope::default(), EvalScope::default()],
                    &vec![EvalScope::default(), EvalScope::default()],
                    &[],
                ),
            )
            .expect("hash join should succeed");

        assert_eq!(
            result.batch.to_rows(),
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("x".to_string()),
                    ScalarValue::Int(1),
                    ScalarValue::Text("x".to_string()),
                ],
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("y".to_string()),
                    ScalarValue::Int(1),
                    ScalarValue::Text("y".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn filters_bucket_candidates_by_actual_key_values() {
        let left = batch(&[vec![ScalarValue::Int(2)]], &["id"]);
        let right = batch(
            &[vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]],
            &["id"],
        );

        let matches = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(
                HashJoinExecutor::new(crate::parser::ast::JoinType::Inner, vec![0], vec![0], None)
                    .matching_right_rows(
                        &left,
                        0,
                        &right,
                        &[0, 1],
                        &[EvalScope::default()],
                        &[EvalScope::default(), EvalScope::default()],
                        &[],
                    ),
            )
            .expect("candidate filtering should succeed");

        assert_eq!(matches, vec![1]);
        assert!(
            !key_rows_equal(&left, &[0], 0, &right, &[0], 0).expect("comparison should succeed")
        );
    }

    #[test]
    fn handles_empty_inputs() {
        let left = batch(&[], &["id"]);
        let right = batch(&[vec![ScalarValue::Int(1)]], &["id"]);

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(
                HashJoinExecutor::new(crate::parser::ast::JoinType::Full, vec![0], vec![0], None)
                    .execute(&left, &right, &[], &[EvalScope::default()], &[]),
            )
            .expect("hash join should succeed");

        assert_eq!(
            result.batch.to_rows(),
            vec![vec![ScalarValue::Null, ScalarValue::Int(1)]]
        );
    }

    #[test]
    fn extracts_simple_equi_join_keys() {
        let expr = parse_join_on_expr("SELECT * FROM a JOIN b ON a.x = b.y");
        let extracted = extract_equi_join_keys(
            &expr,
            &["a.x".to_string(), "a.z".to_string()],
            &["b.y".to_string(), "b.w".to_string()],
        )
        .expect("equi keys should be extracted");

        assert_eq!(extracted.0, vec![0]);
        assert_eq!(extracted.1, vec![0]);
        assert!(extracted.2.is_none());
    }

    #[test]
    fn extracts_compound_equi_join_keys_and_residual() {
        let expr =
            parse_join_on_expr("SELECT * FROM a JOIN b ON a.x = b.y AND a.z = b.w AND a.n > 10");
        let extracted = extract_equi_join_keys(
            &expr,
            &["a.x".to_string(), "a.z".to_string(), "a.n".to_string()],
            &["b.y".to_string(), "b.w".to_string()],
        )
        .expect("equi keys should be extracted");

        assert_eq!(extracted.0, vec![0, 1]);
        assert_eq!(extracted.1, vec![0, 1]);
        assert!(extracted.2.is_some());
    }

    #[test]
    fn returns_none_when_no_equi_keys_exist() {
        let expr = parse_join_on_expr("SELECT * FROM a JOIN b ON a.x > b.y");
        assert!(
            extract_equi_join_keys(&expr, &["a.x".to_string()], &["b.y".to_string()]).is_none()
        );
    }

    #[test]
    fn resolves_using_columns_to_join_keys() {
        let (left_keys, right_keys) = using_join_key_indices(
            &["id".to_string(), "tag".to_string()],
            &["id".to_string(), "tag".to_string()],
            &["id".to_string(), "tag".to_string()],
        )
        .expect("USING columns should resolve");

        assert_eq!(left_keys, vec![0, 1]);
        assert_eq!(right_keys, vec![0, 1]);
    }

    #[test]
    fn resolves_identifier_side_from_columns_only_when_unambiguous() {
        assert_eq!(
            resolve_identifier_side_from_columns(
                &["id".to_string()],
                &["id".to_string()],
                &["other".to_string()]
            ),
            Some((JoinSide::Left, 0))
        );
        assert!(
            resolve_identifier_side_from_columns(
                &["id".to_string()],
                &["id".to_string()],
                &["id".to_string()]
            )
            .is_none()
        );
    }
}
