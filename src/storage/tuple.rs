use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, FixedSizeListArray, FixedSizeListBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, NullArray, NullBuilder, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::{Value as JsonValue, json};

use crate::utils::adt::datetime::{
    DateValue, datetime_to_epoch_seconds, format_date, format_timestamp, parse_datetime_text,
};

const ENCODED_SCALAR_PREFIX: &str = "__openassay__:";

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Numeric(rust_decimal::Decimal),
    Text(String),
    Array(Vec<Self>),
    /// Row/Record type for ROW(a, b, c) or (a, b, c) expressions
    Record(Vec<Self>),
    /// Fixed-dimension float vector used by the pgvector extension
    Vector(Vec<f32>),
}

impl ScalarValue {
    pub fn render(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(v) => if *v { "t" } else { "f" }.to_string(),
            Self::Int(v) => v.to_string(),
            Self::Float(v) => render_float8(*v),
            Self::Numeric(v) => v.to_string(),
            Self::Text(v) => v.clone(),
            Self::Array(values) => render_array_literal(values),
            Self::Record(values) => {
                let parts: Vec<String> = values
                    .iter()
                    .map(|v| {
                        if matches!(v, Self::Null) {
                            String::new()
                        } else {
                            v.render()
                        }
                    })
                    .collect();
                format!("({})", parts.join(","))
            }
            Self::Vector(values) => render_vector_literal(values),
        }
    }
}

pub fn scalar_value_to_arrow_value(val: &ScalarValue, builder: &mut dyn ArrayBuilder) {
    append_scalar_value_to_builder(val, builder)
        .expect("Arrow builder type should match inferred storage schema");
}

pub fn arrow_value_to_scalar_value(array: &dyn Array, index: usize) -> ScalarValue {
    if array.is_null(index) {
        return ScalarValue::Null;
    }

    if let Some(array) = array.as_any().downcast_ref::<NullArray>() {
        let _ = array;
        return ScalarValue::Null;
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return ScalarValue::Bool(array.value(index));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return ScalarValue::Int(array.value(index));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return ScalarValue::Float(array.value(index));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return decode_string_scalar(array.value(index));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return ScalarValue::Text(String::from_utf8_lossy(array.value(index)).into_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
        let days = array.value(index) as i64;
        let epoch_seconds = days.saturating_mul(86_400);
        let date = crate::utils::adt::datetime::datetime_from_epoch_seconds(epoch_seconds).date;
        return ScalarValue::Text(format_date(date));
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let value = array.value(index);
        if value == i64::MAX {
            return ScalarValue::Text("infinity".to_string());
        }
        if value == i64::MIN {
            return ScalarValue::Text("-infinity".to_string());
        }
        let seconds = value.div_euclid(1_000_000);
        let micros = value.rem_euclid(1_000_000) as u32;
        let mut datetime = crate::utils::adt::datetime::datetime_from_epoch_seconds(seconds);
        datetime.microsecond = micros;
        return ScalarValue::Text(format_timestamp(datetime));
    }
    if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = array.value(index);
        let float_values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("vector storage should use Float64 child arrays");
        let mut out = Vec::with_capacity(float_values.len());
        for idx in 0..float_values.len() {
            if float_values.is_null(idx) {
                out.push(0.0);
            } else {
                out.push(float_values.value(idx) as f32);
            }
        }
        return ScalarValue::Vector(out);
    }

    ScalarValue::Text(array_value_to_string(array, index))
}

pub fn scalar_values_schema(columns: &[(String, ScalarValue)]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, value)| Field::new(name, infer_data_type(value), true))
        .collect();
    Schema::new(fields)
}

/// Render a float8 (f64) value matching PostgreSQL's output format.
///
/// PostgreSQL uses shortest-representation output (extra_float_digits = 1 by default in modern PG).
/// Special values: NaN, Infinity, -Infinity.
/// Integer-valued floats are rendered without decimal point (e.g. 0, 1, -5).
pub fn render_float8(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    // Use Rust's default Display which gives shortest representation

    v.to_string()
}

/// Render a float4 (f32) value matching PostgreSQL's output format.
/// Float4 has less precision than float8, so we cast to f32 first.
pub fn render_float4(v: f64) -> String {
    let v32 = v as f32;
    if v32.is_nan() {
        return "NaN".to_string();
    }
    if v32.is_infinite() {
        return if v32.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    let text = format!("{v32}");
    text
}

fn render_array_literal(values: &[ScalarValue]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|value| match value {
            ScalarValue::Null => "NULL".to_string(),
            _ => value.render(),
        })
        .collect();
    format!("{{{}}}", parts.join(","))
}

fn render_vector_literal(values: &[f32]) -> String {
    let parts: Vec<String> = values.iter().map(|v| render_float4(*v as f64)).collect();
    format!("[{}]", parts.join(","))
}

pub(crate) fn append_scalar_value_to_builder(
    val: &ScalarValue,
    builder: &mut dyn ArrayBuilder,
) -> Result<(), String> {
    if matches!(val, ScalarValue::Null) {
        append_null_to_builder(builder)?;
        return Ok(());
    }

    if let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() {
        let _ = builder;
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        match val {
            ScalarValue::Bool(v) => builder.append_value(*v),
            _ => builder.append_null(),
        }
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        match val {
            ScalarValue::Int(v) => builder.append_value(*v),
            _ => builder.append_null(),
        }
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        match val {
            ScalarValue::Float(v) => builder.append_value(*v),
            _ => builder.append_null(),
        }
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        match val {
            ScalarValue::Text(v) => builder.append_value(encode_text_scalar(v)),
            _ => builder.append_value(encode_tagged_scalar(val)),
        }
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
        builder.append_value(val.render().into_bytes());
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        match val {
            ScalarValue::Text(text) => {
                let days = date_text_to_date32(text)?;
                builder.append_value(days);
            }
            _ => builder.append_null(),
        }
        return Ok(());
    }
    if let Some(builder) = builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
    {
        match val {
            ScalarValue::Text(text) => {
                let micros = timestamp_text_to_microseconds(text)?;
                builder.append_value(micros);
            }
            _ => builder.append_null(),
        }
        return Ok(());
    }
    if let Some(builder) = builder
        .as_any_mut()
        .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
    {
        if let ScalarValue::Vector(values) = val {
            let expected_len = builder.value_length() as usize;
            if values.len() != expected_len {
                return Err(format!(
                    "vector length {} does not match schema length {expected_len}",
                    values.len()
                ));
            }
            for value in values {
                append_scalar_value_to_builder(
                    &ScalarValue::Float(f64::from(*value)),
                    builder.values().as_mut(),
                )?;
            }
            builder.append(true);
        } else {
            for _ in 0..builder.value_length() {
                append_null_to_builder(builder.values().as_mut())?;
            }
            builder.append(false);
        }
        return Ok(());
    }

    Err("unsupported Arrow builder type".to_string())
}

fn append_null_to_builder(builder: &mut dyn ArrayBuilder) -> Result<(), String> {
    if let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
    {
        builder.append_null();
        return Ok(());
    }
    if let Some(builder) = builder
        .as_any_mut()
        .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
    {
        for _ in 0..builder.value_length() {
            append_null_to_builder(builder.values().as_mut())?;
        }
        builder.append(false);
        return Ok(());
    }

    Err("unsupported Arrow builder type".to_string())
}

fn infer_data_type(value: &ScalarValue) -> DataType {
    match value {
        ScalarValue::Null => DataType::Utf8,
        ScalarValue::Bool(_) => DataType::Boolean,
        ScalarValue::Int(_) => DataType::Int64,
        ScalarValue::Float(_) => DataType::Float64,
        ScalarValue::Numeric(_) => DataType::Utf8,
        ScalarValue::Text(_) => DataType::Utf8,
        ScalarValue::Array(_) => DataType::Utf8,
        ScalarValue::Record(_) => DataType::Utf8,
        ScalarValue::Vector(values) => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            values.len() as i32,
        ),
    }
}

fn encode_text_scalar(value: &str) -> String {
    if value.starts_with(ENCODED_SCALAR_PREFIX) {
        format!("{ENCODED_SCALAR_PREFIX}text:{value}")
    } else {
        value.to_string()
    }
}

fn encode_tagged_scalar(value: &ScalarValue) -> String {
    format!("{ENCODED_SCALAR_PREFIX}{}", scalar_to_json(value))
}

fn decode_string_scalar(value: &str) -> ScalarValue {
    if let Some(encoded) = value.strip_prefix(ENCODED_SCALAR_PREFIX) {
        if let Some(text) = encoded.strip_prefix("text:") {
            return ScalarValue::Text(text.to_string());
        }
        return json_to_scalar(encoded).unwrap_or_else(|| ScalarValue::Text(value.to_string()));
    }
    ScalarValue::Text(value.to_string())
}

fn scalar_to_json(value: &ScalarValue) -> String {
    json_scalar_value(value).to_string()
}

fn json_to_scalar(value: &str) -> Option<ScalarValue> {
    let json: JsonValue = serde_json::from_str(value).ok()?;
    scalar_from_json_value(&json).ok()
}

fn json_scalar_value(value: &ScalarValue) -> JsonValue {
    match value {
        ScalarValue::Null => json!({ "type": "null" }),
        ScalarValue::Bool(v) => json!({ "type": "bool", "value": v }),
        ScalarValue::Int(v) => json!({ "type": "int", "value": v }),
        ScalarValue::Float(v) => json!({ "type": "float", "value": v }),
        ScalarValue::Numeric(v) => json!({ "type": "numeric", "value": v.to_string() }),
        ScalarValue::Text(v) => json!({ "type": "text", "value": v }),
        ScalarValue::Array(values) => json!({
            "type": "array",
            "value": values.iter().map(json_scalar_value).collect::<Vec<_>>()
        }),
        ScalarValue::Record(values) => json!({
            "type": "record",
            "value": values.iter().map(json_scalar_value).collect::<Vec<_>>()
        }),
        ScalarValue::Vector(values) => json!({ "type": "vector", "value": values }),
    }
}

fn scalar_from_json_value(value: &JsonValue) -> Result<ScalarValue, String> {
    let tag = value
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "encoded scalar type is missing".to_string())?;
    match tag {
        "null" => Ok(ScalarValue::Null),
        "bool" => value
            .get("value")
            .and_then(JsonValue::as_bool)
            .map(ScalarValue::Bool)
            .ok_or_else(|| "encoded bool value is missing".to_string()),
        "int" => value
            .get("value")
            .and_then(JsonValue::as_i64)
            .map(ScalarValue::Int)
            .ok_or_else(|| "encoded int value is missing".to_string()),
        "float" => value
            .get("value")
            .and_then(JsonValue::as_f64)
            .map(ScalarValue::Float)
            .ok_or_else(|| "encoded float value is missing".to_string()),
        "numeric" => value
            .get("value")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "encoded numeric value is missing".to_string())?
            .parse()
            .map(ScalarValue::Numeric)
            .map_err(|_| "encoded numeric value is invalid".to_string()),
        "text" => value
            .get("value")
            .and_then(JsonValue::as_str)
            .map(|v| ScalarValue::Text(v.to_string()))
            .ok_or_else(|| "encoded text value is missing".to_string()),
        "array" => decode_json_sequence(value, ScalarValue::Array),
        "record" => decode_json_sequence(value, ScalarValue::Record),
        "vector" => {
            let values = value
                .get("value")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| "encoded vector is missing".to_string())?
                .iter()
                .map(|item| {
                    item.as_f64()
                        .map(|v| v as f32)
                        .ok_or_else(|| "encoded vector element is invalid".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ScalarValue::Vector(values))
        }
        _ => Err("unsupported encoded scalar type".to_string()),
    }
}

fn decode_json_sequence(
    value: &JsonValue,
    wrap: fn(Vec<ScalarValue>) -> ScalarValue,
) -> Result<ScalarValue, String> {
    let items = value
        .get("value")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "encoded sequence is missing".to_string())?
        .iter()
        .map(scalar_from_json_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(wrap(items))
}

fn date_text_to_date32(text: &str) -> Result<i32, String> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("infinity") {
        return Ok(days_since_epoch(DateValue {
            year: 294_276,
            month: 12,
            day: 31,
        }));
    }
    if text.eq_ignore_ascii_case("-infinity") {
        return Ok(days_since_epoch(DateValue {
            year: -4_713,
            month: 1,
            day: 1,
        }));
    }
    let datetime = parse_datetime_text(text).map_err(|err| err.message)?;
    Ok(days_since_epoch(datetime.date))
}

fn timestamp_text_to_microseconds(text: &str) -> Result<i64, String> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("infinity") {
        return Ok(i64::MAX);
    }
    if text.eq_ignore_ascii_case("-infinity") {
        return Ok(i64::MIN);
    }
    let datetime = parse_datetime_text(text).map_err(|err| err.message)?;
    let seconds = datetime_to_epoch_seconds(datetime);
    Ok(seconds
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(datetime.microsecond)))
}

fn days_since_epoch(date: DateValue) -> i32 {
    let epoch = parse_datetime_text("1970-01-01")
        .expect("1970-01-01 should parse")
        .date;
    let target = parse_datetime_text(&format_date(date))
        .expect("formatted date should parse")
        .date;
    let epoch_seconds = datetime_to_epoch_seconds(crate::utils::adt::datetime::DateTimeValue {
        date: epoch,
        hour: 0,
        minute: 0,
        second: 0,
        microsecond: 0,
    });
    let target_seconds = datetime_to_epoch_seconds(crate::utils::adt::datetime::DateTimeValue {
        date: target,
        hour: 0,
        minute: 0,
        second: 0,
        microsecond: 0,
    });
    ((target_seconds - epoch_seconds) / 86_400) as i32
}

fn array_value_to_string(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return array.value(index).to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return if array.value(index) { "t" } else { "f" }.to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return array.value(index).to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return render_float8(array.value(index));
    }
    "NULL".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBinaryColumn {
    pub name: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyBinarySnapshot {
    pub qualified_name: String,
    pub columns: Vec<CopyBinaryColumn>,
    pub rows: Vec<Vec<ScalarValue>>,
}

#[cfg(test)]
mod tests {
    use arrow::array::builder::make_builder;

    use super::{
        ScalarValue, arrow_value_to_scalar_value, scalar_value_to_arrow_value, scalar_values_schema,
    };

    #[test]
    fn scalar_value_arrow_roundtrip_preserves_supported_variants() {
        let columns = vec![
            ("flag".to_string(), ScalarValue::Bool(true)),
            ("count".to_string(), ScalarValue::Int(7)),
            ("ratio".to_string(), ScalarValue::Float(3.5)),
            (
                "amount".to_string(),
                ScalarValue::Numeric("42.125".parse().expect("numeric")),
            ),
            ("name".to_string(), ScalarValue::Text("alpha".to_string())),
            (
                "items".to_string(),
                ScalarValue::Array(vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("x".to_string()),
                ]),
            ),
            (
                "record".to_string(),
                ScalarValue::Record(vec![ScalarValue::Bool(false), ScalarValue::Int(9)]),
            ),
            (
                "embedding".to_string(),
                ScalarValue::Vector(vec![1.0, 2.5, 3.0]),
            ),
        ];
        let schema = scalar_values_schema(&columns);

        for (idx, (_, value)) in columns.iter().enumerate() {
            let mut builder = make_builder(schema.field(idx).data_type(), 1);
            scalar_value_to_arrow_value(value, builder.as_mut());
            let array = builder.finish();
            let actual = arrow_value_to_scalar_value(array.as_ref(), 0);
            assert_eq!(&actual, value);
        }
    }

    #[test]
    fn utf8_escape_roundtrip_preserves_user_text_prefixes() {
        let columns = vec![(
            "text".to_string(),
            ScalarValue::Text("__openassay__:literal".to_string()),
        )];
        let schema = scalar_values_schema(&columns);
        let mut builder = make_builder(schema.field(0).data_type(), 1);
        scalar_value_to_arrow_value(&columns[0].1, builder.as_mut());
        let array = builder.finish();
        let actual = arrow_value_to_scalar_value(array.as_ref(), 0);
        assert_eq!(actual, columns[0].1);
    }
}
