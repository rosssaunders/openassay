use openassay::storage::tuple::ScalarValue;
use openassay::tcop::engine::QueryResult;
use serde_json::{Number, Value, json};

pub(crate) fn split_table_reference(input: &str) -> Result<(String, String), String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("table name cannot be empty".to_string());
    }

    let parts = trimmed.split('.').map(str::trim).collect::<Vec<_>>();
    match parts.as_slice() {
        [table] if !table.is_empty() => Ok(("public".to_string(), (*table).to_string())),
        [schema, table] if !schema.is_empty() && !table.is_empty() => {
            Ok(((*schema).to_string(), (*table).to_string()))
        }
        _ => Err(format!(
            "invalid table reference '{trimmed}' (use table or schema.table)"
        )),
    }
}

pub(crate) fn quote_identifier(identifier: &str) -> Result<String, String> {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return Err("identifier cannot be empty".to_string());
    }
    Ok(format!("\"{}\"", trimmed.replace('"', "\"\"")))
}

pub(crate) fn quote_qualified_identifier(identifier: &str) -> Result<String, String> {
    let trimmed = identifier.trim();
    let parts = trimmed.split('.').map(str::trim).collect::<Vec<_>>();
    match parts.as_slice() {
        [single] if !single.is_empty() => quote_identifier(single),
        [schema, name] if !schema.is_empty() && !name.is_empty() => Ok(format!(
            "{}.{}",
            quote_identifier(schema)?,
            quote_identifier(name)?
        )),
        _ => Err(format!(
            "invalid identifier '{trimmed}' (use name or schema.name)"
        )),
    }
}

pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn split_csv_paths(path: Option<&str>) -> Vec<String> {
    let Some(path) = path else {
        return Vec::new();
    };
    path.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub(crate) fn vector_literal(values: &[f64]) -> Result<String, String> {
    if values.iter().any(|value| !value.is_finite()) {
        return Err("vector values must be finite numbers".to_string());
    }
    serde_json::to_string(values).map_err(|error| error.to_string())
}

pub(crate) fn query_result_to_json(result: QueryResult, execution_time_ms: u64) -> Value {
    let row_count = if result.columns.is_empty() {
        result.rows_affected
    } else {
        u64::try_from(result.rows.len()).unwrap_or(u64::MAX)
    };

    let rows = result
        .rows
        .iter()
        .map(|row| Value::Array(row.iter().map(scalar_to_json).collect()))
        .collect::<Vec<_>>();

    json!({
        "columns": result.columns,
        "rows": rows,
        "rowCount": row_count,
        "executionTimeMs": execution_time_ms,
    })
}

pub(crate) fn scalar_to_json(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Bool(boolean) => Value::Bool(*boolean),
        ScalarValue::Int(int) => Value::Number(Number::from(*int)),
        ScalarValue::Float(float) => Number::from_f64(*float)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(float.to_string())),
        ScalarValue::Numeric(decimal) => Value::String(decimal.to_string()),
        ScalarValue::Text(text) => Value::String(text.clone()),
        ScalarValue::Array(values) | ScalarValue::Record(values) => {
            Value::Array(values.iter().map(scalar_to_json).collect())
        }
        ScalarValue::Vector(values) => Value::Array(
            values
                .iter()
                .map(|value| {
                    Number::from_f64(f64::from(*value))
                        .map(Value::Number)
                        .unwrap_or_else(|| Value::String(value.to_string()))
                })
                .collect(),
        ),
    }
}

pub(crate) fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Bool(boolean) => Some(boolean.to_string()),
        ScalarValue::Int(int) => Some(int.to_string()),
        ScalarValue::Float(float) => Some(float.to_string()),
        ScalarValue::Numeric(decimal) => Some(decimal.to_string()),
        ScalarValue::Text(text) => Some(text.clone()),
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
            Some(value.render())
        }
    }
}

pub(crate) fn scalar_to_u64(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::Int(int) if *int >= 0 => u64::try_from(*int).ok(),
        ScalarValue::Float(float) if *float >= 0.0 && float.is_finite() => {
            Some((*float).round() as u64)
        }
        ScalarValue::Numeric(decimal) => decimal.to_string().parse::<u64>().ok(),
        ScalarValue::Text(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}
