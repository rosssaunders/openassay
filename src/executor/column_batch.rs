use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch};
use rust_decimal::Decimal;

use crate::storage::tuple::{ScalarValue, arrow_value_to_scalar_value};

#[derive(Debug, Clone)]
pub struct ColumnBatch {
    pub columns: Vec<TypedColumn>,
    pub column_names: Vec<String>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub enum TypedColumn {
    Bool(Vec<bool>, Vec<bool>),
    Int64(Vec<i64>, Vec<bool>),
    Float64(Vec<f64>, Vec<bool>),
    Text(Vec<String>, Vec<bool>),
    Numeric(Vec<Decimal>, Vec<bool>),
    Mixed(Vec<ScalarValue>),
}

impl ColumnBatch {
    pub fn empty(column_names: Vec<String>) -> Self {
        let columns = column_names
            .iter()
            .map(|_| TypedColumn::Mixed(Vec::new()))
            .collect();
        Self {
            columns,
            column_names,
            row_count: 0,
        }
    }

    pub fn from_record_batch(rb: &RecordBatch, column_names: &[String]) -> Self {
        let columns = rb
            .columns()
            .iter()
            .map(|column| typed_column_from_array(column.as_ref(), rb.num_rows()))
            .collect();
        Self {
            columns,
            column_names: column_names.to_vec(),
            row_count: rb.num_rows(),
        }
    }

    pub fn from_rows(rows: &[Vec<ScalarValue>], column_names: &[String]) -> Self {
        let row_count = rows.len();
        let columns = column_names
            .iter()
            .enumerate()
            .map(|(column_idx, _)| {
                let values = rows
                    .iter()
                    .map(|row| row.get(column_idx).cloned().unwrap_or(ScalarValue::Null))
                    .collect::<Vec<_>>();
                typed_column_from_scalars(values)
            })
            .collect();
        Self {
            columns,
            column_names: column_names.to_vec(),
            row_count,
        }
    }

    pub fn to_rows(&self) -> Vec<Vec<ScalarValue>> {
        let mut rows = Vec::with_capacity(self.row_count);
        for row_idx in 0..self.row_count {
            let mut row = Vec::with_capacity(self.columns.len());
            for column in &self.columns {
                row.push(column.value_at(row_idx));
            }
            rows.push(row);
        }
        rows
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        let normalized = name.to_ascii_lowercase();
        self.column_names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(&normalized))
            .or_else(|| {
                normalized.rsplit('.').next().and_then(|short_name| {
                    self.column_names
                        .iter()
                        .position(|candidate| candidate.eq_ignore_ascii_case(short_name))
                })
            })
    }

    pub fn filter(&self, mask: &[bool]) -> Self {
        let row_count = self.row_count.min(mask.len());
        let columns = self
            .columns
            .iter()
            .map(|column| column.filter(&mask[..row_count]))
            .collect();
        Self {
            columns,
            column_names: self.column_names.clone(),
            row_count: mask
                .iter()
                .take(row_count)
                .filter(|selected| **selected)
                .count(),
        }
    }

    pub fn project(&self, indices: &[usize]) -> Self {
        let columns = indices
            .iter()
            .filter_map(|idx| self.columns.get(*idx).cloned())
            .collect::<Vec<_>>();
        let column_names = indices
            .iter()
            .filter_map(|idx| self.column_names.get(*idx).cloned())
            .collect::<Vec<_>>();
        Self {
            columns,
            column_names,
            row_count: self.row_count,
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Self {
        let start = offset.min(self.row_count);
        let end = start.saturating_add(len).min(self.row_count);
        let columns = self
            .columns
            .iter()
            .map(|column| column.slice(start, end - start))
            .collect();
        Self {
            columns,
            column_names: self.column_names.clone(),
            row_count: end - start,
        }
    }

    pub(crate) fn append_batch(&mut self, other: &Self) -> Result<(), String> {
        if self.row_count == 0 {
            *self = other.clone();
            return Ok(());
        }
        if self.column_names != other.column_names {
            return Err("column batch schemas do not match".to_string());
        }
        if self.columns.len() != other.columns.len() {
            return Err("column batch widths do not match".to_string());
        }
        for (left, right) in self.columns.iter_mut().zip(&other.columns) {
            left.append(right);
        }
        self.row_count += other.row_count;
        Ok(())
    }
}

impl TypedColumn {
    pub(crate) fn value_at(&self, row_idx: usize) -> ScalarValue {
        match self {
            Self::Bool(values, nulls) => {
                if nulls.get(row_idx).copied().unwrap_or(true) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Bool(values[row_idx])
                }
            }
            Self::Int64(values, nulls) => {
                if nulls.get(row_idx).copied().unwrap_or(true) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int(values[row_idx])
                }
            }
            Self::Float64(values, nulls) => {
                if nulls.get(row_idx).copied().unwrap_or(true) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(values[row_idx])
                }
            }
            Self::Text(values, nulls) => {
                if nulls.get(row_idx).copied().unwrap_or(true) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Text(values[row_idx].clone())
                }
            }
            Self::Numeric(values, nulls) => {
                if nulls.get(row_idx).copied().unwrap_or(true) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Numeric(values[row_idx])
                }
            }
            Self::Mixed(values) => values.get(row_idx).cloned().unwrap_or(ScalarValue::Null),
        }
    }

    fn filter(&self, mask: &[bool]) -> Self {
        match self {
            Self::Bool(values, nulls) => {
                let (values, nulls) = filter_typed(values, nulls, mask);
                Self::Bool(values, nulls)
            }
            Self::Int64(values, nulls) => {
                let (values, nulls) = filter_typed(values, nulls, mask);
                Self::Int64(values, nulls)
            }
            Self::Float64(values, nulls) => {
                let (values, nulls) = filter_typed(values, nulls, mask);
                Self::Float64(values, nulls)
            }
            Self::Text(values, nulls) => {
                let (values, nulls) = filter_typed(values, nulls, mask);
                Self::Text(values, nulls)
            }
            Self::Numeric(values, nulls) => {
                let (values, nulls) = filter_typed(values, nulls, mask);
                Self::Numeric(values, nulls)
            }
            Self::Mixed(values) => Self::Mixed(
                values
                    .iter()
                    .zip(mask.iter().copied())
                    .filter_map(|(value, selected)| selected.then_some(value.clone()))
                    .collect(),
            ),
        }
    }

    fn slice(&self, offset: usize, len: usize) -> Self {
        let end = offset.saturating_add(len);
        match self {
            Self::Bool(values, nulls) => {
                Self::Bool(values[offset..end].to_vec(), nulls[offset..end].to_vec())
            }
            Self::Int64(values, nulls) => {
                Self::Int64(values[offset..end].to_vec(), nulls[offset..end].to_vec())
            }
            Self::Float64(values, nulls) => {
                Self::Float64(values[offset..end].to_vec(), nulls[offset..end].to_vec())
            }
            Self::Text(values, nulls) => {
                Self::Text(values[offset..end].to_vec(), nulls[offset..end].to_vec())
            }
            Self::Numeric(values, nulls) => {
                Self::Numeric(values[offset..end].to_vec(), nulls[offset..end].to_vec())
            }
            Self::Mixed(values) => Self::Mixed(values[offset..end].to_vec()),
        }
    }

    fn append(&mut self, other: &Self) {
        match (self, other) {
            (Self::Bool(left_values, left_nulls), Self::Bool(right_values, right_nulls)) => {
                left_values.extend(right_values.iter().copied());
                left_nulls.extend(right_nulls.iter().copied());
            }
            (Self::Int64(left_values, left_nulls), Self::Int64(right_values, right_nulls)) => {
                left_values.extend(right_values.iter().copied());
                left_nulls.extend(right_nulls.iter().copied());
            }
            (Self::Float64(left_values, left_nulls), Self::Float64(right_values, right_nulls)) => {
                left_values.extend(right_values.iter().copied());
                left_nulls.extend(right_nulls.iter().copied());
            }
            (Self::Text(left_values, left_nulls), Self::Text(right_values, right_nulls)) => {
                left_values.extend(right_values.iter().cloned());
                left_nulls.extend(right_nulls.iter().copied());
            }
            (Self::Numeric(left_values, left_nulls), Self::Numeric(right_values, right_nulls)) => {
                left_values.extend(right_values.iter().copied());
                left_nulls.extend(right_nulls.iter().copied());
            }
            (Self::Mixed(left_values), Self::Mixed(right_values)) => {
                left_values.extend(right_values.iter().cloned());
            }
            (left, right) => {
                let mut merged = left.to_scalars();
                merged.extend(right.to_scalars());
                *left = Self::Mixed(merged);
            }
        }
    }

    fn to_scalars(&self) -> Vec<ScalarValue> {
        match self {
            Self::Bool(values, nulls) => values
                .iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if *is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Bool(*value)
                    }
                })
                .collect(),
            Self::Int64(values, nulls) => values
                .iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if *is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Int(*value)
                    }
                })
                .collect(),
            Self::Float64(values, nulls) => values
                .iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if *is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Float(*value)
                    }
                })
                .collect(),
            Self::Text(values, nulls) => values
                .iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if *is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Text(value.clone())
                    }
                })
                .collect(),
            Self::Numeric(values, nulls) => values
                .iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if *is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Numeric(*value)
                    }
                })
                .collect(),
            Self::Mixed(values) => values.clone(),
        }
    }
}

fn typed_column_from_array(array: &dyn Array, row_count: usize) -> TypedColumn {
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        let mut out = Vec::with_capacity(row_count);
        let mut nulls = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            let is_null = values.is_null(idx);
            nulls.push(is_null);
            out.push(if is_null { false } else { values.value(idx) });
        }
        return TypedColumn::Bool(out, nulls);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        let mut out = Vec::with_capacity(row_count);
        let mut nulls = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            let is_null = values.is_null(idx);
            nulls.push(is_null);
            out.push(if is_null { 0 } else { values.value(idx) });
        }
        return TypedColumn::Int64(out, nulls);
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        let mut out = Vec::with_capacity(row_count);
        let mut nulls = Vec::with_capacity(row_count);
        for idx in 0..row_count {
            let is_null = values.is_null(idx);
            nulls.push(is_null);
            out.push(if is_null { 0.0 } else { values.value(idx) });
        }
        return TypedColumn::Float64(out, nulls);
    }

    let values = (0..row_count)
        .map(|row_idx| arrow_value_to_scalar_value(array, row_idx))
        .collect::<Vec<_>>();
    typed_column_from_scalars(values)
}

fn typed_column_from_scalars(values: Vec<ScalarValue>) -> TypedColumn {
    if let Some((typed, nulls)) = all_bools(&values) {
        return TypedColumn::Bool(typed, nulls);
    }
    if let Some((typed, nulls)) = all_ints(&values) {
        return TypedColumn::Int64(typed, nulls);
    }
    if let Some((typed, nulls)) = all_floats(&values) {
        return TypedColumn::Float64(typed, nulls);
    }
    if let Some((typed, nulls)) = all_text(&values) {
        return TypedColumn::Text(typed, nulls);
    }
    if let Some((typed, nulls)) = all_numeric(&values) {
        return TypedColumn::Numeric(typed, nulls);
    }
    TypedColumn::Mixed(values)
}

fn all_bools(values: &[ScalarValue]) -> Option<(Vec<bool>, Vec<bool>)> {
    let mut typed = Vec::with_capacity(values.len());
    let mut nulls = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Bool(v) => {
                typed.push(*v);
                nulls.push(false);
            }
            ScalarValue::Null => {
                typed.push(false);
                nulls.push(true);
            }
            _ => return None,
        }
    }
    Some((typed, nulls))
}

fn all_ints(values: &[ScalarValue]) -> Option<(Vec<i64>, Vec<bool>)> {
    let mut typed = Vec::with_capacity(values.len());
    let mut nulls = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Int(v) => {
                typed.push(*v);
                nulls.push(false);
            }
            ScalarValue::Null => {
                typed.push(0);
                nulls.push(true);
            }
            _ => return None,
        }
    }
    Some((typed, nulls))
}

fn all_floats(values: &[ScalarValue]) -> Option<(Vec<f64>, Vec<bool>)> {
    let mut typed = Vec::with_capacity(values.len());
    let mut nulls = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Float(v) => {
                typed.push(*v);
                nulls.push(false);
            }
            ScalarValue::Null => {
                typed.push(0.0);
                nulls.push(true);
            }
            _ => return None,
        }
    }
    Some((typed, nulls))
}

fn all_text(values: &[ScalarValue]) -> Option<(Vec<String>, Vec<bool>)> {
    let mut typed = Vec::with_capacity(values.len());
    let mut nulls = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Text(v) => {
                typed.push(v.clone());
                nulls.push(false);
            }
            ScalarValue::Null => {
                typed.push(String::new());
                nulls.push(true);
            }
            _ => return None,
        }
    }
    Some((typed, nulls))
}

fn all_numeric(values: &[ScalarValue]) -> Option<(Vec<Decimal>, Vec<bool>)> {
    let mut typed = Vec::with_capacity(values.len());
    let mut nulls = Vec::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Numeric(v) => {
                typed.push(*v);
                nulls.push(false);
            }
            ScalarValue::Null => {
                typed.push(Decimal::ZERO);
                nulls.push(true);
            }
            _ => return None,
        }
    }
    Some((typed, nulls))
}

fn filter_typed<T: Clone>(values: &[T], nulls: &[bool], mask: &[bool]) -> (Vec<T>, Vec<bool>) {
    let mut filtered_values = Vec::new();
    let mut filtered_nulls = Vec::new();
    for ((value, is_null), selected) in values.iter().zip(nulls).zip(mask) {
        if *selected {
            filtered_values.push(value.clone());
            filtered_nulls.push(*is_null);
        }
    }
    (filtered_values, filtered_nulls)
}

#[cfg(test)]
mod tests {
    use super::ColumnBatch;
    use crate::storage::tuple::ScalarValue;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use rust_decimal::Decimal;
    use std::sync::Arc;

    #[test]
    fn from_rows_round_trips_to_rows() {
        let rows = vec![
            vec![
                ScalarValue::Int(1),
                ScalarValue::Text("a".to_string()),
                ScalarValue::Numeric(Decimal::new(125, 2)),
            ],
            vec![
                ScalarValue::Int(2),
                ScalarValue::Null,
                ScalarValue::Numeric(Decimal::new(250, 2)),
            ],
        ];
        let batch = ColumnBatch::from_rows(
            &rows,
            &["id".to_string(), "name".to_string(), "price".to_string()],
        );
        assert_eq!(batch.to_rows(), rows);
    }

    #[test]
    fn filters_and_projects_rows() {
        let rows = vec![
            vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
            vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
            vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
        ];
        let batch = ColumnBatch::from_rows(&rows, &["id".to_string(), "name".to_string()]);
        let filtered = batch.filter(&[false, true, true]).project(&[1]);

        assert_eq!(filtered.column_names, vec!["name".to_string()]);
        assert_eq!(
            filtered.to_rows(),
            vec![
                vec![ScalarValue::Text("b".to_string())],
                vec![ScalarValue::Text("c".to_string())],
            ]
        );
    }

    #[test]
    fn builds_from_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .expect("record batch should build");

        let column_batch =
            ColumnBatch::from_record_batch(&batch, &["id".to_string(), "name".to_string()]);

        assert_eq!(
            column_batch.to_rows(),
            vec![
                vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                vec![ScalarValue::Null, ScalarValue::Text("b".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Null],
            ]
        );
    }
}
