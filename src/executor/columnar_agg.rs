use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use rust_decimal::Decimal;

use crate::executor::column_batch::{ColumnBatch, TypedColumn, typed_column_from_scalars};
use crate::executor::profiling;
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::datetime::{datetime_from_epoch_seconds, format_date};
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
    CountDistinctInt { column_index: usize },
    CountDistinctText { column_index: usize },
    SumInt { column_index: usize },
    SumFloat { column_index: usize },
    SumNumeric { column_index: usize },
    AvgInt { column_index: usize },
    AvgFloat { column_index: usize },
    AvgNumericInt { column_index: usize },
    AvgNumeric { column_index: usize },
    MinDate { column_index: usize },
    MaxDate { column_index: usize },
    Min { column_index: usize },
    Max { column_index: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnarAggregator {
    group_key_indices: Vec<usize>,
    output_exprs: Vec<OutputExpr>,
    output_column_names: Vec<String>,
    accumulators: Vec<AggAccumulator>,
    group_map: HashMap<u64, Vec<usize>>,
    group_keys: Vec<Vec<ScalarValue>>,
    group_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum AggAccumulator {
    Count {
        counts: Vec<i64>,
        column_index: Option<usize>,
    },
    CountDistinctInt {
        counts: Vec<i64>,
        seen: HashSet<(usize, i64)>,
        column_index: usize,
    },
    CountDistinctText {
        counts: Vec<i64>,
        seen: HashMap<usize, HashSet<String>>,
        column_index: usize,
    },
    SumInt {
        sums: Vec<i128>,
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
        sums: Vec<i128>,
        counts: Vec<i64>,
        column_index: usize,
    },
    AvgFloat {
        sums: Vec<f64>,
        counts: Vec<i64>,
        column_index: usize,
    },
    AvgNumericInt {
        sums: Vec<i128>,
        counts: Vec<i64>,
        column_index: usize,
    },
    AvgNumeric {
        sums: Vec<Decimal>,
        counts: Vec<i64>,
        column_index: usize,
    },
    MinMaxDate {
        values: Vec<Option<i32>>,
        column_index: usize,
        is_min: bool,
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
        let _span = profiling::span("columnar_aggregator_push_batch");
        if batch.row_count == 0 {
            return Ok(());
        }

        let row_indices = (0..batch.row_count).collect::<Vec<_>>();
        self.push_rows(batch, &row_indices)
    }

    pub(crate) fn push_selected_rows(
        &mut self,
        batch: &ColumnBatch,
        row_indices: &[usize],
    ) -> Result<(), EngineError> {
        let _span = profiling::span("columnar_aggregator_push_selected_rows");
        if row_indices.is_empty() {
            return Ok(());
        }

        self.push_rows(batch, row_indices)
    }

    fn push_rows(&mut self, batch: &ColumnBatch, row_indices: &[usize]) -> Result<(), EngineError> {
        if row_indices.is_empty() {
            return Ok(());
        }

        let mut group_indices = Vec::with_capacity(row_indices.len());
        for &row_idx in row_indices {
            let hash = hash_group_key_for_row(batch, &self.group_key_indices, row_idx);
            let group_idx = self
                .find_group_for_row(batch, row_idx, hash)
                .unwrap_or_else(|| self.insert_group_from_row(batch, row_idx, hash));
            group_indices.push(group_idx);
        }

        for accumulator in &mut self.accumulators {
            accumulator.update_rows(batch, Some(row_indices), &group_indices)?;
        }

        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<ColumnBatch, EngineError> {
        let _span = profiling::span("columnar_aggregator_finish");
        if self.group_count == 0 && self.group_key_indices.is_empty() {
            self.ensure_group(Vec::new());
        }

        let columns = self
            .output_exprs
            .iter()
            .map(|output| match output {
                OutputExpr::GroupKey(key_idx) => {
                    let values = (0..self.group_count)
                        .map(|group_idx| {
                            self.group_keys
                                .get(group_idx)
                                .and_then(|keys| keys.get(*key_idx))
                                .cloned()
                                .unwrap_or(ScalarValue::Null)
                        })
                        .collect::<Vec<_>>();
                    typed_column_from_scalars(values)
                }
                OutputExpr::Aggregate(acc_idx) => {
                    let values = (0..self.group_count)
                        .map(|group_idx| self.accumulators[*acc_idx].finalize(group_idx))
                        .collect::<Vec<_>>();
                    typed_column_from_scalars(values)
                }
            })
            .collect();

        Ok(ColumnBatch::new(self.output_column_names, columns))
    }

    #[cfg(test)]
    fn lookup_or_insert_group(&mut self, key_values: Vec<ScalarValue>) -> usize {
        let hash = hash_group_key(&key_values);
        if let Some(entries) = self.group_map.get(&hash) {
            for &group_idx in entries {
                if self
                    .group_keys
                    .get(group_idx)
                    .is_some_and(|existing_keys| existing_keys == &key_values)
                {
                    return group_idx;
                }
            }
        }

        let group_idx = self.ensure_group(key_values);
        self.group_map.entry(hash).or_default().push(group_idx);
        group_idx
    }

    fn find_group_for_row(&self, batch: &ColumnBatch, row_idx: usize, hash: u64) -> Option<usize> {
        self.group_map.get(&hash).and_then(|entries| {
            entries
                .iter()
                .copied()
                .find(|group_idx| self.group_key_matches_row(batch, row_idx, *group_idx))
        })
    }

    fn insert_group_from_row(&mut self, batch: &ColumnBatch, row_idx: usize, hash: u64) -> usize {
        let key_values = materialize_group_key(batch, &self.group_key_indices, row_idx);
        let group_idx = self.ensure_group(key_values);
        self.group_map.entry(hash).or_default().push(group_idx);
        group_idx
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

    fn group_key_matches_row(&self, batch: &ColumnBatch, row_idx: usize, group_idx: usize) -> bool {
        self.group_keys.get(group_idx).is_some_and(|existing_keys| {
            self.group_key_indices
                .iter()
                .zip(existing_keys)
                .all(|(column_idx, existing_value)| {
                    scalar_value_matches_row_value(
                        existing_value,
                        &batch.columns[*column_idx],
                        row_idx,
                    )
                })
        })
    }
}

#[derive(Default)]
struct FastHasher(u64);

impl FastHasher {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn with_seed() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.0 == 0 {
            self.0 = Self::OFFSET_BASIS;
        }
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
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
            AggKind::CountDistinctInt { column_index } => Self::CountDistinctInt {
                counts: Vec::new(),
                seen: HashSet::new(),
                column_index,
            },
            AggKind::CountDistinctText { column_index } => Self::CountDistinctText {
                counts: Vec::new(),
                seen: HashMap::new(),
                column_index,
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
            AggKind::AvgNumericInt { column_index } => Self::AvgNumericInt {
                sums: Vec::new(),
                counts: Vec::new(),
                column_index,
            },
            AggKind::AvgNumeric { column_index } => Self::AvgNumeric {
                sums: Vec::new(),
                counts: Vec::new(),
                column_index,
            },
            AggKind::MinDate { column_index } => Self::MinMaxDate {
                values: Vec::new(),
                column_index,
                is_min: true,
            },
            AggKind::MaxDate { column_index } => Self::MinMaxDate {
                values: Vec::new(),
                column_index,
                is_min: false,
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
            Self::CountDistinctInt { counts, .. } => counts.push(0),
            Self::CountDistinctText { counts, .. } => counts.push(0),
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
            Self::AvgNumericInt { sums, counts, .. } => {
                sums.push(0);
                counts.push(0);
            }
            Self::AvgNumeric { sums, counts, .. } => {
                sums.push(Decimal::ZERO);
                counts.push(0);
            }
            Self::MinMaxDate { values, .. } => values.push(None),
            Self::MinMaxScalar { values, .. } => values.push(None),
        }
    }

    fn update_rows(
        &mut self,
        batch: &ColumnBatch,
        row_indices: Option<&[usize]>,
        group_indices: &[usize],
    ) -> Result<(), EngineError> {
        match self {
            Self::Count {
                counts,
                column_index: None,
            } => {
                for_each_row_group(row_indices, group_indices, |_, group_idx| {
                    counts[group_idx] += 1;
                });
                Ok(())
            }
            Self::Count {
                counts,
                column_index: Some(column_index),
            } => {
                let column = &batch.columns[*column_index];
                for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                    if !is_null_at(column, row_idx) {
                        counts[group_idx] += 1;
                    }
                });
                Ok(())
            }
            Self::CountDistinctInt {
                counts,
                seen,
                column_index,
            } => update_count_distinct_int(
                batch,
                row_indices,
                group_indices,
                *column_index,
                counts,
                seen,
            ),
            Self::CountDistinctText {
                counts,
                seen,
                column_index,
            } => update_count_distinct_text(
                batch,
                row_indices,
                group_indices,
                *column_index,
                counts,
                seen,
            ),
            Self::SumInt {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_int(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                saw_non_null,
            ),
            Self::SumFloat {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_float(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                saw_non_null,
            ),
            Self::SumNumeric {
                sums,
                saw_non_null,
                column_index,
            } => update_sum_numeric(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                saw_non_null,
            ),
            Self::AvgInt {
                sums,
                counts,
                column_index,
            } => update_avg_int(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                counts,
            ),
            Self::AvgFloat {
                sums,
                counts,
                column_index,
            } => update_avg_float(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                counts,
            ),
            Self::AvgNumericInt {
                sums,
                counts,
                column_index,
            } => update_avg_numeric_int(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                counts,
            ),
            Self::AvgNumeric {
                sums,
                counts,
                column_index,
            } => update_avg_numeric(
                batch,
                row_indices,
                group_indices,
                *column_index,
                sums,
                counts,
            ),
            Self::MinMaxDate {
                values,
                column_index,
                is_min,
            } => update_min_max_date(
                batch,
                row_indices,
                group_indices,
                *column_index,
                values,
                *is_min,
            ),
            Self::MinMaxScalar {
                values,
                column_index,
                is_min,
            } => update_min_max(
                batch,
                row_indices,
                group_indices,
                *column_index,
                values,
                *is_min,
            ),
        }
    }

    fn finalize(&self, group_idx: usize) -> ScalarValue {
        match self {
            Self::Count { counts, .. } => ScalarValue::Int(counts[group_idx]),
            Self::CountDistinctInt { counts, .. } => ScalarValue::Int(counts[group_idx]),
            Self::CountDistinctText { counts, .. } => ScalarValue::Int(counts[group_idx]),
            Self::SumInt {
                sums, saw_non_null, ..
            } => {
                if saw_non_null[group_idx] {
                    if let Ok(value) = i64::try_from(sums[group_idx]) {
                        ScalarValue::Int(value)
                    } else {
                        ScalarValue::Numeric(Decimal::from_i128_with_scale(sums[group_idx], 0))
                    }
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
            Self::AvgNumericInt { sums, counts, .. } => {
                if counts[group_idx] == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Numeric(
                        Decimal::from_i128_with_scale(sums[group_idx], 0)
                            / Decimal::from(counts[group_idx]),
                    )
                }
            }
            Self::AvgNumeric { sums, counts, .. } => {
                if counts[group_idx] == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Numeric(sums[group_idx] / Decimal::from(counts[group_idx]))
                }
            }
            Self::MinMaxDate { values, .. } => values[group_idx]
                .map(date_scalar_from_days)
                .unwrap_or(ScalarValue::Null),
            Self::MinMaxScalar { values, .. } => {
                values[group_idx].clone().unwrap_or(ScalarValue::Null)
            }
        }
    }
}

fn for_each_row_group(
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    mut callback: impl FnMut(usize, usize),
) {
    match row_indices {
        Some(row_indices) => {
            debug_assert_eq!(row_indices.len(), group_indices.len());
            for (&row_idx, &group_idx) in row_indices.iter().zip(group_indices.iter()) {
                callback(row_idx, group_idx);
            }
        }
        None => {
            for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                callback(row_idx, group_idx);
            }
        }
    }
}

fn update_count_distinct_int(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    counts: &mut [i64],
    seen: &mut HashSet<(usize, i64)>,
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                if seen.insert((group_idx, values[row_idx])) {
                    counts[group_idx] += 1;
                }
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar count(distinct int) expects an int8-compatible column".to_string(),
        }),
    }
}

fn update_count_distinct_text(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    counts: &mut [i64],
    seen: &mut HashMap<usize, HashSet<String>>,
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Text(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                let group_seen = seen.entry(group_idx).or_default();
                if group_seen.insert(values[row_idx].clone()) {
                    counts[group_idx] += 1;
                }
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar count(distinct text) expects a text-compatible column".to_string(),
        }),
    }
}

fn update_sum_int(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [i128],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += i128::from(values[row_idx]);
                saw_non_null[group_idx] = true;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(int) expects an int8-compatible column".to_string(),
        }),
    }
}

fn update_sum_float(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [f64],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Float64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += values[row_idx];
                saw_non_null[group_idx] = true;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(float) expects a float8-compatible column".to_string(),
        }),
    }
}

fn update_sum_numeric(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [Decimal],
    saw_non_null: &mut [bool],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Numeric(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += values[row_idx];
                saw_non_null[group_idx] = true;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar sum(numeric) expects a numeric column".to_string(),
        }),
    }
}

fn update_avg_int(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [i128],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += i128::from(values[row_idx]);
                counts[group_idx] += 1;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(int) expects an int8-compatible column".to_string(),
        }),
    }
}

fn update_avg_float(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [f64],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Float64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += values[row_idx];
                counts[group_idx] += 1;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(float) expects a float8-compatible column".to_string(),
        }),
    }
}

fn update_avg_numeric(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [Decimal],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Numeric(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += values[row_idx];
                counts[group_idx] += 1;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(numeric) expects a numeric column".to_string(),
        }),
    }
}

fn update_avg_numeric_int(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    sums: &mut [i128],
    counts: &mut [i64],
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Int64(values, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                sums[group_idx] += i128::from(values[row_idx]);
                counts[group_idx] += 1;
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar avg(numeric cast from int) expects an int8-compatible column"
                .to_string(),
        }),
    }
}

fn update_min_max(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    values: &mut [Option<ScalarValue>],
    is_min: bool,
) -> Result<(), EngineError> {
    let mut error = None;
    for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
        if error.is_some() {
            return;
        }
        let value = scalar_value_at(&batch.columns[column_index], row_idx);
        if matches!(value, ScalarValue::Null) {
            return;
        }
        match &values[group_idx] {
            None => values[group_idx] = Some(value),
            Some(existing) => {
                let cmp = match compare_values_for_predicate(&value, existing) {
                    Ok(cmp) => cmp,
                    Err(err) => {
                        error = Some(err);
                        return;
                    }
                };
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
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(())
}

fn update_min_max_date(
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
    group_indices: &[usize],
    column_index: usize,
    values: &mut [Option<i32>],
    is_min: bool,
) -> Result<(), EngineError> {
    match &batch.columns[column_index] {
        TypedColumn::Date(days, nulls) => {
            for_each_row_group(row_indices, group_indices, |row_idx, group_idx| {
                if nulls[row_idx] {
                    return;
                }
                let candidate = days[row_idx];
                match values[group_idx] {
                    None => values[group_idx] = Some(candidate),
                    Some(existing) => {
                        let take = if is_min {
                            candidate < existing
                        } else {
                            candidate > existing
                        };
                        if take {
                            values[group_idx] = Some(candidate);
                        }
                    }
                }
            });
            Ok(())
        }
        _ => Err(EngineError {
            message: "columnar min/max(date) expects a date column".to_string(),
        }),
    }
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
                date_scalar_from_days(values[row_idx])
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

fn materialize_group_key(
    batch: &ColumnBatch,
    group_key_indices: &[usize],
    row_idx: usize,
) -> Vec<ScalarValue> {
    group_key_indices
        .iter()
        .map(|column_idx| scalar_value_at(&batch.columns[*column_idx], row_idx))
        .collect()
}

fn scalar_value_matches_row_value(
    existing_value: &ScalarValue,
    column: &TypedColumn,
    row_idx: usize,
) -> bool {
    match column {
        TypedColumn::Bool(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Bool(value) if *value == values[row_idx])
            }
        }
        TypedColumn::Int64(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Int(value) if *value == values[row_idx])
            }
        }
        TypedColumn::Float64(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Float(value) if *value == values[row_idx])
            }
        }
        TypedColumn::Date(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Text(value) if value == &date_text_from_days(values[row_idx]))
            }
        }
        TypedColumn::Text(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Text(value) if value == &values[row_idx])
            }
        }
        TypedColumn::Numeric(values, nulls) => {
            if nulls[row_idx] {
                matches!(existing_value, ScalarValue::Null)
            } else {
                matches!(existing_value, ScalarValue::Numeric(value) if *value == values[row_idx])
            }
        }
        TypedColumn::Mixed(values) => values[row_idx] == *existing_value,
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn hash_group_key(values: &[ScalarValue]) -> u64 {
    let mut hasher = FastHasher::with_seed();
    for value in values {
        hash_scalar_value(value, &mut hasher);
    }
    hasher.finish()
}

fn hash_group_key_for_row(batch: &ColumnBatch, group_key_indices: &[usize], row_idx: usize) -> u64 {
    let mut hasher = FastHasher::with_seed();
    for column_idx in group_key_indices {
        hash_row_value(&batch.columns[*column_idx], row_idx, &mut hasher);
    }
    hasher.finish()
}

fn hash_row_value<H: Hasher>(column: &TypedColumn, row_idx: usize, hasher: &mut H) {
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

fn date_scalar_from_days(days: i32) -> ScalarValue {
    ScalarValue::Text(date_text_from_days(days))
}

fn date_text_from_days(days: i32) -> String {
    let epoch_seconds = i64::from(days).saturating_mul(86_400);
    let date = datetime_from_epoch_seconds(epoch_seconds).date;
    format_date(date)
}

fn hash_scalar_value<H: Hasher>(value: &ScalarValue, hasher: &mut H) {
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
        let group_a =
            collision_probe.lookup_or_insert_group(vec![ScalarValue::Text("a".to_string())]);
        let group_b =
            collision_probe.lookup_or_insert_group(vec![ScalarValue::Text("b".to_string())]);
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

    #[test]
    fn aggregates_selected_rows_without_filter_materialization() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(1)],
                vec![ScalarValue::Text("skip".to_string()), ScalarValue::Int(20)],
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(3)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(4)],
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
            .push_selected_rows(&batch, &[0, 2, 3])
            .expect("selected rows should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(4)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(4)],
            ]
        );
    }

    #[test]
    fn counts_distinct_int_per_group_without_counting_nulls() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(10)],
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(10)],
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(11)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(10)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Null],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(10)],
            ],
            &["dept".to_string(), "user_id".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            vec![0],
            vec![AggSpec {
                kind: AggKind::CountDistinctInt { column_index: 1 },
            }],
            vec![OutputExpr::GroupKey(0), OutputExpr::Aggregate(0)],
            vec!["dept".to_string(), "users".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(2)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(1)],
            ]
        );
    }

    #[test]
    fn counts_distinct_text_per_group_without_counting_nulls() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Text("x".to_string()),
                ],
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Text("x".to_string()),
                ],
                vec![
                    ScalarValue::Text("a".to_string()),
                    ScalarValue::Text("y".to_string()),
                ],
                vec![
                    ScalarValue::Text("b".to_string()),
                    ScalarValue::Text("x".to_string()),
                ],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Null],
                vec![
                    ScalarValue::Text("b".to_string()),
                    ScalarValue::Text("x".to_string()),
                ],
            ],
            &["dept".to_string(), "phrase".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            vec![0],
            vec![AggSpec {
                kind: AggKind::CountDistinctText { column_index: 1 },
            }],
            vec![OutputExpr::GroupKey(0), OutputExpr::Aggregate(0)],
            vec!["dept".to_string(), "phrases".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![
                vec![ScalarValue::Text("a".to_string()), ScalarValue::Int(2)],
                vec![ScalarValue::Text("b".to_string()), ScalarValue::Int(1)],
            ]
        );
    }

    #[test]
    fn counts_distinct_text_without_counting_nulls() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Text("beta".to_string())],
                vec![ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Null],
            ],
            &["phrase".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            Vec::new(),
            vec![AggSpec {
                kind: AggKind::CountDistinctText { column_index: 0 },
            }],
            vec![OutputExpr::Aggregate(0)],
            vec!["phrases".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(output.to_rows(), vec![vec![ScalarValue::Int(2)]]);
    }

    #[test]
    fn avg_int_handles_large_running_totals_without_overflow() {
        let batch = ColumnBatch::from_rows(
            &[
                vec![ScalarValue::Int(i64::MAX)],
                vec![ScalarValue::Int(i64::MAX - 2)],
            ],
            &["value".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            Vec::new(),
            vec![AggSpec {
                kind: AggKind::AvgInt { column_index: 0 },
            }],
            vec![OutputExpr::Aggregate(0)],
            vec!["avg".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![vec![ScalarValue::Float(i64::MAX as f64 - 1.0)]]
        );
    }

    #[test]
    fn avg_numeric_int_preserves_numeric_result() {
        let batch = ColumnBatch::from_rows(
            &[vec![ScalarValue::Int(1)], vec![ScalarValue::Int(2)]],
            &["value".to_string()],
        );
        let mut aggregator = ColumnarAggregator::new(
            Vec::new(),
            vec![AggSpec {
                kind: AggKind::AvgNumericInt { column_index: 0 },
            }],
            vec![OutputExpr::Aggregate(0)],
            vec!["avg".to_string()],
        );

        aggregator
            .push_batch(&batch)
            .expect("batch should aggregate");
        let output = aggregator.finish().expect("finish should succeed");

        assert_eq!(
            output.to_rows(),
            vec![vec![ScalarValue::Numeric(Decimal::new(15, 1))]]
        );
    }
}
