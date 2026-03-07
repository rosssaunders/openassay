use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use rust_decimal::Decimal;

use crate::executor::column_batch::{ColumnBatch, TypedColumn};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::compare_values_for_predicate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutputExpr {
    GroupKey(usize),
    Aggregate(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct AggSpec {
    pub(crate) kind: AggKind,
}

#[derive(Debug, Clone)]
pub(crate) enum AggKind {
    CountStar,
    Count { column_index: usize },
    SumInt { column_index: usize },
    SumFloat { column_index: usize },
    SumNumeric { column_index: usize },
    AvgInt { column_index: usize },
    AvgFloat { column_index: usize },
    AvgNumeric { column_index: usize },
    Min { column_index: usize },
    Max { column_index: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnarAggregator {
    group_key_indices: Vec<usize>,
    output_exprs: Vec<OutputExpr>,
    output_column_names: Vec<String>,
    accumulators: Vec<AggAccumulator>,
    group_map: HashMap<u64, Vec<(usize, Vec<ScalarValue>)>>,
    group_keys: Vec<Vec<ScalarValue>>,
    group_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum AggAccumulator {
    Count {
        counts: Vec<i64>,
        column_index: Option<usize>,
    },
    SumInt {
        sums: Vec<i64>,
        saw_non_null: Vec<bool>,
        column_index: usize,
    },
    SumFloat {
        sums: Vec<f64>,
        saw_non_null: Vec<bool>,
        column_index: usize,
    },
    SumNumeric {
        sums: Vec<Decimal>,
        saw_non_null: Vec<bool>,
        column_index: usize,
    },
    AvgInt {
        sums: Vec<i64>,
        counts: Vec<i64>,
        column_index: usize,
    },
    AvgFloat {
        sums: Vec<f64>,
        counts: Vec<i64>,
        column_index: usize,
    },
    AvgNumeric {
        sums: Vec<Decimal>,
        counts: Vec<i64>,
        column_index: usize,
    },
    MinMaxScalar {
        values: Vec<Option<ScalarValue>>,
        column_index: usize,
        is_min: bool,
    },
}

impl ColumnarAggregator {
    pub(crate) fn new(
        group_key_indices: Vec<usize>,
        agg_specs: Vec<AggSpec>,
        output_exprs: Vec<OutputExpr>,
        output_column_names: Vec<String>,
    ) -> Self {
        let accumulators = agg_specs
            .into_iter()
            .map(AggAccumulator::from_spec)
            .collect();
        Self {
            group_key_indices,
            output_exprs,
            output_column_names,
            accumulators,
            group_map: HashMap::new(),
            group_keys: Vec::new(),
            group_count: 0,
        }
    }

    pub(crate) fn push_batch(&mut self, batch: &ColumnBatch) -> Result<(), EngineError> {
        if batch.row_count == 0 {
            return Ok(());
        }

        let mut group_indices = Vec::with_capacity(batch.row_count);
        for row_idx in 0..batch.row_count {
            let key_values = self
                .group_key_indices
                .iter()
                .map(|column_idx| scalar_value_at(&batch.columns[*column_idx], row_idx))
                .collect::<Vec<_>>();
            let group_idx = self.lookup_or_insert_group(key_values)?;
            group_indices.push(group_idx);
        }

        for accumulator in &mut self.accumulators {
            accumulator.update_batch(batch, &group_indices)?;
        }

        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<ColumnBatch, EngineError> {
        if self.group_count == 0 && self.group_key_indices.is_empty() {
            self.ensure_group(Vec::new());
        }

        let mut rows = Vec::with_capacity(self.group_count);
        for group_idx in 0..self.group_count {
            let mut row = Vec::with_capacity(self.output_exprs.len());
            for output in &self.output_exprs {
                match output {
                    OutputExpr::GroupKey(key_idx) => {
                        row.push(
                            self.group_keys
                                .get(group_idx)
                                .and_then(|keys| keys.get(*key_idx))
                                .cloned()
                                .unwrap_or(ScalarValue::Null),
                        );
                    }
                    OutputExpr::Aggregate(acc_idx) => {
                        row.push(self.accumulators[*acc_idx].finalize(group_idx));
                    }
                }
            }
            rows.push(row);
        }
        Ok(ColumnBatch::from_rows(&rows, &self.output_column_names))
    }

    fn lookup_or_insert_group(
        &mut self,
        key_values: Vec<ScalarValue>,
    ) -> Result<usize, EngineError> {
        let hash = hash_group_key(&key_values);
        self.lookup_or_insert_group_with_hash(hash, key_values)
    }

    fn lookup_or_insert_group_with_hash(
        &mut self,
        hash: u64,
        key_values: Vec<ScalarValue>,
    ) -> Result<usize, EngineError> {
        if let Some(entries) = self.group_map.get(&hash) {
            for (group_idx, existing_keys) in entries {
                if existing_keys == &key_values {
                    return Ok(*group_idx);
                }
            }
        }

        let group_idx = self.ensure_group(key_values.clone());
        self.group_map
            .entry(hash)
            .or_default()
            .push((group_idx, key_values));
        Ok(group_idx)
    }

    fn ensure_group(&mut self, key_values: Vec<ScalarValue>) -> usize {
        let group_idx = self.group_count;
        self.group_keys.push(key_values);
        self.group_count += 1;
        for accumulator in &mut self.accumulators {
            accumulator.push_group();
        }
        group_idx
    }
}

impl AggAccumulator {
    fn from_spec(spec: AggSpec) -> Self {
        match spec.kind {
            AggKind::CountStar => Self::Count {
                counts: Vec::new(),
                column_index: None,
            },
            AggKind::Count { column_index } => Self::Count {
                counts: Vec::new(),
                column_index: Some(column_index),
            },
            AggKind::SumInt { column_index } => Self::SumInt {
                sums: Vec::new(),
                saw_non_null: Vec::new(),
                column_index,
            },
            AggKind::SumFloat { column_index } => Self::SumFloat {
                sums: Vec::new(),
                saw_non_null: Vec::new(),
                column_index,
            },
            AggKind::SumNumeric { column_index } => Self::SumNumeric {
                sums: Vec::new(),
                saw_non_null: Vec::new(),
                column_index,
            },
            AggKind::AvgInt { column_index } => Self::AvgInt {
                sums: Vec::new(),
                counts: Vec::new(),
                column_index,
            },
            AggKind::AvgFloat { column_index } => Self::AvgFloat {
                sums: Vec::new(),
                counts: Vec::new(),
                column_index,
            },
            AggKind::AvgNumeric { column_index } => Self::AvgNumeric {
                sums: Vec::new(),
                counts: Vec::new(),
                column_index,
            },
            AggKind::Min { column_index } => Self::MinMaxScalar {
                values: Vec::new(),
                column_index,
                is_min: true,
            },
            AggKind::Max { column_index } => Self::MinMaxScalar {
                values: Vec::new(),
                column_index,
                is_min: false,
            },
        }
    }

    fn push_group(&mut self) {
        match self {
            Self::Count { counts, .. } => counts.push(0),
            Self::SumInt {
                sums, saw_non_null, ..
            } => {
                sums.push(0);
                saw_non_null.push(false);
            }
            Self::SumFloat {
                sums, saw_non_null, ..
            } => {
                sums.push(0.0);
                saw_non_null.push(false);
            }
            Self::SumNumeric {
                sums, saw_non_null, ..
            } => {
                sums.push(Decimal::ZERO);
                saw_non_null.push(false);
            }
            Self::AvgInt { sums, counts, .. } => {
                sums.push(0);
                counts.push(0);
            }
            Self::AvgFloat { sums, counts, .. } => {
                sums.push(0.0);
                counts.push(0);
            }
            Self::AvgNumeric { sums, counts, .. } => {
                sums.push(Decimal::ZERO);
                counts.push(0);
            }
            Self::MinMaxScalar { values, .. } => values.push(None),
        }
    }

    fn update_batch(
        &mut self,
        batch: &ColumnBatch,
        group_indices: &[usize],
    ) -> Result<(), EngineError> {
        match self {
            Self::Count {
                counts,
                column_index: None,
            } => {
                for &group_idx in group_indices {
                    counts[group_idx] += 1;
                }
                Ok(())
            }
            Self::Count {
                counts,
                column_index: Some(column_index),
            } => {
                let column = &batch.columns[*column_index];
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    if !is_null_at(column, row_idx) {
                        counts[group_idx] += 1;
                    }
                }
                Ok(())
            }
            Self::SumInt {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_int(batch, group_indices, *column_index, sums, saw_non_null),
            Self::SumFloat {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_float(batch, group_indices, *column_index, sums, saw_non_null),
            Self::SumNumeric {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_numeric(batch, group_indices, *column_index, sums, saw_non_null),
            Self::AvgInt {
                sums,
                counts,
                column_index,
            } => update_avg_int(batch, group_indices, *column_index, sums, counts),
            Self::AvgFloat {
                sums,
                counts,
                column_index,
            } => update_avg_float(batch, group_indices, *column_index, sums, counts),
            Self::AvgNumeric {
                sums,
                counts,
                column_index,
            } => update_avg_numeric(batch, group_indices, *column_index, sums, counts),
            Self::MinMaxScalar {
                values,
                column_index,
                is_min,
            } => update_min_max(batch, group_indices, *column_index, values, *is_min),
        }
    }

    fn finalize(&self, group_idx: usize) -> ScalarValue {
        match self {
            Self::Count { counts, .. } => ScalarValue::Int(counts[group_idx]),
            Self::SumInt {
                sums, saw_non_null, ..
            } => {
                if saw_non_null[group_idx] {
                    ScalarValue::Int(sums[group_idx])
                } else {
                    ScalarValue::Null
                }
            }
            Self::SumFloat {
                sums, saw_non_null, ..
            } => {
                if saw_non_null[group_idx] {
                    ScalarValue::Float(sums[group_idx])
                } else {
                    ScalarValue::Null
                }
            }
            Self::SumNumeric {
                sums, saw_non_null, ..
            } => {
                if saw_non_null[group_idx] {
                    ScalarValue::Numeric(sums[group_idx])
                } else {
                    ScalarValue::Null
                }
            }
            Self::AvgInt { sums, counts, .. } => {
                if counts[group_idx] == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(sums[group_idx] as f64 / counts[group_idx] as f64)
                }
            }
            Self::AvgFloat { sums, counts, .. } => {
                if counts[group_idx] == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(sums[group_idx] / counts[group_idx] as f64)
                }
            }
            Self::AvgNumeric { sums, counts, .. } => {
                if counts[group_idx] == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Numeric(sums[group_idx] / Decimal::from(counts[group_idx]))
                }
            }
            Self::MinMaxScalar { values, .. } => {
                values[group_idx].clone().unwrap_or(ScalarValue::Null)
            }
        }
    }
}

fn update_sum_int(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [i64],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                saw_non_null[group_idx] = true;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(int) expects an int8-compatible column".to_string(),
        }),
    }
}

fn update_sum_float(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [f64],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Float64(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                saw_non_null[group_idx] = true;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(float) expects a float8-compatible column".to_string(),
        }),
    }
}

fn update_sum_numeric(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [Decimal],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Numeric(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                saw_non_null[group_idx] = true;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(numeric) expects a numeric column".to_string(),
        }),
    }
}

fn update_avg_int(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [i64],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                counts[group_idx] += 1;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(int) expects an int8-compatible column".to_string(),
        }),
    }
}

fn update_avg_float(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [f64],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Float64(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                counts[group_idx] += 1;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(float) expects a float8-compatible column".to_string(),
        }),
    }
}

fn update_avg_numeric(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [Decimal],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Numeric(values, nulls) => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                if nulls[row_idx] {
                    continue;
                }
                sums[group_idx] += values[row_idx];
                counts[group_idx] += 1;
            }
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(numeric) expects a numeric column".to_string(),
        }),
    }
}

fn update_min_max(
    batch: &ColumnBatch,
    group_indices: &[usize],
    column_index: usize,
    values: &mut [Option<ScalarValue>],
    is_min: bool,
) -> Result<(), EngineError> {
    for (row_idx, &group_idx) in group_indices.iter().enumerate() {
        let value = scalar_value_at(&batch.columns[column_index], row_idx);
        if matches!(value, ScalarValue::Null) {
            continue;
        }
        match &values[group_idx] {
            None => values[group_idx] = Some(value),
            Some(existing) => {
                let cmp = compare_values_for_predicate(&value, existing)?;
                let take = if is_min {
                    cmp == Ordering::Less
                } else {
                    cmp == Ordering::Greater
                };
                if take {
                    values[group_idx] = Some(value);
                }
            }
        }
    }
    Ok(())
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
        | TypedColumn::Text(_, nulls)
        | TypedColumn::Numeric(_, nulls) => nulls[row_idx],
        TypedColumn::Mixed(values) => matches!(values[row_idx], ScalarValue::Null),
    }
}

pub(crate) fn hash_group_key(values: &[ScalarValue]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for value in values {
        match value {
            ScalarValue::Null => 0u8.hash(&mut hasher),
            ScalarValue::Bool(flag) => {
                1u8.hash(&mut hasher);
                flag.hash(&mut hasher);
            }
            ScalarValue::Int(number) => {
                2u8.hash(&mut hasher);
                number.hash(&mut hasher);
            }
            ScalarValue::Float(number) => {
                3u8.hash(&mut hasher);
                number.to_bits().hash(&mut hasher);
            }
            ScalarValue::Text(text) => {
                4u8.hash(&mut hasher);
                text.hash(&mut hasher);
            }
            ScalarValue::Numeric(decimal) => {
                5u8.hash(&mut hasher);
                decimal.normalize().to_string().hash(&mut hasher);
            }
            other => {
                6u8.hash(&mut hasher);
                format!("{other:?}").hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::{AggKind, AggSpec, ColumnarAggregator, OutputExpr};
    use crate::executor::column_batch::ColumnBatch;
    use crate::storage::tuple::ScalarValue;
    use rust_decimal::Decimal;

    #[test]
    fn aggregates_single_group_sum_count_avg() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(10), ScalarValue::Int(20)],
                vec![ScalarValue::Int(30), ScalarValue::Int(40)],
            ],
            &["salary".to_string(), "age".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            Vec::new(),
            vec![
                AggSpec {
                    kind: AggKind::SumInt { column_index: 0 },
                },
                AggSpec {
                    kind: AggKind::CountStar,
                },
                AggSpec {
                    kind: AggKind::AvgInt { column_index: 1 },
                },
            ],
            vec![
                OutputExpr::Aggregate(0),
                OutputExpr::Aggregate(1),
                OutputExpr::Aggregate(2),
            ],
            vec!["sum".to_string(), "count".to_string(), "avg".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![vec![
                ScalarValue::Int(40),
                ScalarValue::Int(2),
                ScalarValue::Float(30.0),
            ]]
        );
    }

    #[test]
    fn aggregates_multiple_groups_and_handles_collisions() {
        let mut collision_probe = ColumnarAggregator::new(
            vec![0],
            vec![AggSpec {
                kind: AggKind::SumInt { column_index: 1 },
            }],
            vec![OutputExpr::GroupKey(0), OutputExpr::Aggregate(0)],
            vec!["dept".to_string(), "sum".to_string()],
        );
        let group_a = collision_probe
            .lookup_or_insert_group_with_hash(7, vec![ScalarValue::Text("a".to_string())])
            .expect("group insert should work");
        let group_b = collision_probe
            .lookup_or_insert_group_with_hash(7, vec![ScalarValue::Text("b".to_string())])
            .expect("collision insert should work");
        assert_ne!(group_a, group_b);

        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(1)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(2)],
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(3)],
            ],
            &["dept".to_string(), "salary".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            vec![0],
            vec![AggSpec {
                kind: AggKind::SumInt { column_index: 1 },
            }],
            vec![OutputExpr::GroupKey(0), OutputExpr::Aggregate(0)],
            vec!["dept".to_string(), "sum".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(4)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(2)],
            ]
        );
    }

    #[test]
    fn aggregates_empty_input_into_global_row() {
        let batch = ColumnBatch::from_rows(&[], &["salary".to_string()]);
        let mut aggregator = ColumnarAggregator::new(
            Vec::new(),
            vec![
                AggSpec {
                    kind: AggKind::CountStar,
                },
                AggSpec {
                    kind: AggKind::SumInt { column_index: 0 },
                },
            ],
            vec![OutputExpr::Aggregate(0), OutputExpr::Aggregate(1)],
            vec!["count".to_string(), "sum".to_string()],
        );
        aggregator
            .push_batch(&batch)
            .expect("empty batch should succeed");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![vec![ScalarValue::Int(0), ScalarValue::Null]]
        );
    }

    #[test]
    fn ignores_nulls_in_numeric_aggregates() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Null],
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Numeric(Decimal::new(250, 2)),
                ],
            ],
            &["dept".to_string(), "salary".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            vec![0],
            vec![
                AggSpec {
                    kind: AggKind::Count { column_index: 1 },
                },
                AggSpec {
                    kind: AggKind::AvgNumeric { column_index: 1 },
                },
            ],
            vec![
                OutputExpr::GroupKey(0),
                OutputExpr::Aggregate(0),
                OutputExpr::Aggregate(1),
            ],
            vec!["dept".to_string(), "count".to_string(), "avg".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![vec![
                ScalarValue::Text("a".to_string()),
                ScalarValue::Int(1),
                ScalarValue::Numeric(Decimal::new(250, 2)),
            ]]
        );
    }
}
