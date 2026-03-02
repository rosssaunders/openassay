use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::QueryResult;

/// Convert a single column of a [`QueryResult`] to an Arrow [`ArrayRef`].
///
/// `col_idx` is the zero-based column index.
/// `data_type` must match the column's inferred [`DataType`].
pub fn build_column_array(
    result: &QueryResult,
    col_idx: usize,
    data_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(result.rows.len());
            for row in &result.rows {
                match row.get(col_idx) {
                    Some(ScalarValue::Bool(v)) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(result.rows.len());
            for row in &result.rows {
                match row.get(col_idx) {
                    Some(ScalarValue::Int(v)) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(result.rows.len());
            for row in &result.rows {
                match row.get(col_idx) {
                    Some(ScalarValue::Float(v)) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(result.rows.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for row in &result.rows {
                match row.get(col_idx) {
                    Some(ScalarValue::Numeric(d)) => {
                        let scaled = decimal_to_i128(d, *precision, *scale)?;
                        builder.append_value(scaled);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Default: render every value as its text representation
        _ => {
            let mut builder = StringBuilder::new();
            for row in &result.rows {
                match row.get(col_idx) {
                    Some(ScalarValue::Null) | None => builder.append_null(),
                    Some(v) => builder.append_value(v.render()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Convert all columns of a [`QueryResult`] to Arrow arrays, in schema order.
pub fn build_column_arrays(
    result: &QueryResult,
    schema: &Schema,
) -> Result<Vec<ArrayRef>, ArrowError> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| build_column_array(result, col_idx, field.data_type()))
        .collect()
}

/// Convert a [`rust_decimal::Decimal`] to the `i128` representation required
/// by Arrow's `Decimal128(precision, scale)` type.
///
/// Returns an [`ArrowError`] if the value overflows `i128` or cannot be scaled.
fn decimal_to_i128(
    d: &rust_decimal::Decimal,
    precision: u8,
    scale: i8,
) -> Result<i128, ArrowError> {
    // Bring the decimal to the target scale by multiplying by 10^(target_scale - current_scale)
    // or dividing if the current scale is larger.
    let current_scale = d.scale() as i8;
    let mantissa = d.mantissa(); // i128 significand

    let result = if scale >= current_scale {
        let diff = (scale - current_scale) as u32;
        let factor = 10_i128.pow(diff);
        mantissa.checked_mul(factor).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "decimal value {d} overflows Decimal128({precision}, {scale})"
            ))
        })?
    } else {
        let diff = (current_scale - scale) as u32;
        let divisor = 10_i128.pow(diff);
        mantissa / divisor
    };
    Ok(result)
}
