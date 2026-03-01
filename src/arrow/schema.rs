use arrow::datatypes::{DataType, Field, Schema};

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::QueryResult;

/// Infer an Arrow [`DataType`] from the first non-null value in a column.
/// Falls back to `Utf8` when the column is entirely null or empty.
fn infer_data_type(result: &QueryResult, col_idx: usize) -> DataType {
    for row in &result.rows {
        match row.get(col_idx) {
            Some(ScalarValue::Bool(_)) => return DataType::Boolean,
            Some(ScalarValue::Int(_)) => return DataType::Int64,
            Some(ScalarValue::Float(_)) => return DataType::Float64,
            // Decimal128(38, 6): 38 significant digits, 6 decimal places
            Some(ScalarValue::Numeric(_)) => return DataType::Decimal128(38, 6),
            Some(ScalarValue::Text(_)) => return DataType::Utf8,
            // Arrays, Records and Vectors are serialised as their text representation
            Some(ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_)) => {
                return DataType::Utf8;
            }
            Some(ScalarValue::Null) | None => continue,
        }
    }
    DataType::Utf8
}

/// Build an Arrow [`Schema`] for the given [`QueryResult`] by inspecting
/// the first non-null value in every column to determine its data type.
/// All fields are marked as nullable.
pub fn build_schema(result: &QueryResult) -> Schema {
    let fields: Vec<Field> = result
        .columns
        .iter()
        .enumerate()
        .map(|(col_idx, name)| Field::new(name, infer_data_type(result, col_idx), true))
        .collect();
    Schema::new(fields)
}
