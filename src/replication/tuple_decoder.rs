use byteorder::{BigEndian, ByteOrder};

use crate::catalog::{ColumnSpec, TypeSignature};
use crate::replication::pgoutput::{RelationColumn, TupleColumn, TupleData};
use crate::replication::ReplicationError;
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::coerce_value_for_column_spec;
use crate::utils::adt::datetime::{datetime_from_epoch_seconds, format_date, format_timestamp};

const PG_EPOCH_UNIX_OFFSET_SECS: i64 = 946684800;

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedColumn {
    Unchanged,
    Value(ScalarValue),
}

pub fn type_signature_for_oid(oid: u32) -> TypeSignature {
    match oid {
        16 => TypeSignature::Bool,
        20 | 21 | 23 => TypeSignature::Int8,
        700 | 701 => TypeSignature::Float8,
        1700 => TypeSignature::Numeric,
        1082 => TypeSignature::Date,
        1114 | 1184 => TypeSignature::Timestamp,
        _ => TypeSignature::Text,
    }
}

pub fn decode_tuple(
    columns: &[RelationColumn],
    tuple: &TupleData,
) -> Result<Vec<DecodedColumn>, ReplicationError> {
    if columns.len() != tuple.columns.len() {
        return Err(ReplicationError {
            message: "tuple column count mismatch".to_string(),
        });
    }
    let mut out = Vec::with_capacity(columns.len());
    for (col, value) in columns.iter().zip(tuple.columns.iter()) {
        match value {
            TupleColumn::Null => out.push(DecodedColumn::Value(ScalarValue::Null)),
            TupleColumn::Unchanged => out.push(DecodedColumn::Unchanged),
            TupleColumn::Text(text) => {
                let signature = type_signature_for_oid(col.type_oid);
                out.push(DecodedColumn::Value(decode_text_value(signature, text)?));
            }
            TupleColumn::Binary(bytes) => {
                let signature = type_signature_for_oid(col.type_oid);
                out.push(DecodedColumn::Value(decode_binary_value(signature, bytes)?));
            }
        }
    }
    Ok(out)
}

pub fn decode_text_value(
    signature: TypeSignature,
    text: &str,
) -> Result<ScalarValue, ReplicationError> {
    let spec = ColumnSpec::new("replication_col", signature);
    coerce_value_for_column_spec(ScalarValue::Text(text.to_string()), &spec).map_err(|err| {
        ReplicationError {
            message: err.message,
        }
    })
}

pub fn decode_binary_value(
    signature: TypeSignature,
    bytes: &[u8],
) -> Result<ScalarValue, ReplicationError> {
    match signature {
        TypeSignature::Bool => {
            if bytes.len() != 1 {
                return Err(ReplicationError {
                    message: "invalid boolean binary length".to_string(),
                });
            }
            Ok(ScalarValue::Bool(bytes[0] != 0))
        }
        TypeSignature::Int8 => match bytes.len() {
            8 => Ok(ScalarValue::Int(BigEndian::read_i64(bytes))),
            4 => Ok(ScalarValue::Int(BigEndian::read_i32(bytes) as i64)),
            2 => Ok(ScalarValue::Int(BigEndian::read_i16(bytes) as i64)),
            _ => Err(ReplicationError {
                message: "invalid int binary length".to_string(),
            }),
        },
        TypeSignature::Float8 => match bytes.len() {
            8 => Ok(ScalarValue::Float(f64::from_bits(BigEndian::read_u64(bytes)))),
            4 => Ok(ScalarValue::Float(f32::from_bits(BigEndian::read_u32(bytes)) as f64)),
            _ => Err(ReplicationError {
                message: "invalid float binary length".to_string(),
            }),
        },
        TypeSignature::Date => {
            if bytes.len() != 4 {
                return Err(ReplicationError {
                    message: "invalid date binary length".to_string(),
                });
            }
            let days = BigEndian::read_i32(bytes) as i64;
            let seconds = PG_EPOCH_UNIX_OFFSET_SECS + days * 86_400;
            let dt = datetime_from_epoch_seconds(seconds);
            Ok(ScalarValue::Text(format_date(dt.date)))
        }
        TypeSignature::Timestamp => {
            if bytes.len() != 8 {
                return Err(ReplicationError {
                    message: "invalid timestamp binary length".to_string(),
                });
            }
            let micros = BigEndian::read_i64(bytes);
            let seconds = PG_EPOCH_UNIX_OFFSET_SECS + micros / 1_000_000;
            let dt = datetime_from_epoch_seconds(seconds);
            Ok(ScalarValue::Text(format_timestamp(dt)))
        }
        TypeSignature::Numeric => {
            // PostgreSQL numeric binary format is complex. For now, decode as text then parse.
            let text_value = String::from_utf8_lossy(bytes);
            let decimal = text_value.parse::<rust_decimal::Decimal>().map_err(|_| {
                ReplicationError {
                    message: format!("invalid numeric binary data: {text_value}"),
                }
            })?;
            Ok(ScalarValue::Numeric(decimal))
        }
        TypeSignature::Text => Ok(ScalarValue::Text(
            String::from_utf8_lossy(bytes).to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, oid: u32) -> RelationColumn {
        RelationColumn {
            flags: 0,
            name: name.to_string(),
            type_oid: oid,
            type_modifier: -1,
        }
    }

    #[test]
    fn decodes_text_values() {
        let columns = vec![col("id", 20), col("flag", 16), col("note", 25)];
        let tuple = TupleData {
            columns: vec![
                TupleColumn::Text("42".to_string()),
                TupleColumn::Text("true".to_string()),
                TupleColumn::Text("hello".to_string()),
            ],
        };
        let decoded = decode_tuple(&columns, &tuple).unwrap();
        assert_eq!(
            decoded,
            vec![
                DecodedColumn::Value(ScalarValue::Int(42)),
                DecodedColumn::Value(ScalarValue::Bool(true)),
                DecodedColumn::Value(ScalarValue::Text("hello".to_string()))
            ]
        );
    }

    #[test]
    fn decodes_binary_values() {
        let columns = vec![col("id", 20), col("ratio", 701)];
        let tuple = TupleData {
            columns: vec![
                TupleColumn::Binary(42i64.to_be_bytes().to_vec()),
                TupleColumn::Binary((3.5f64).to_bits().to_be_bytes().to_vec()),
            ],
        };
        let decoded = decode_tuple(&columns, &tuple).unwrap();
        assert_eq!(
            decoded,
            vec![
                DecodedColumn::Value(ScalarValue::Int(42)),
                DecodedColumn::Value(ScalarValue::Float(3.5))
            ]
        );
    }

    #[test]
    fn handles_null_and_unchanged() {
        let columns = vec![col("id", 20), col("note", 25)];
        let tuple = TupleData {
            columns: vec![TupleColumn::Null, TupleColumn::Unchanged],
        };
        let decoded = decode_tuple(&columns, &tuple).unwrap();
        assert_eq!(
            decoded,
            vec![
                DecodedColumn::Value(ScalarValue::Null),
                DecodedColumn::Unchanged
            ]
        );
    }

    #[test]
    fn decodes_date_and_timestamp_text() {
        let columns = vec![col("day", 1082), col("ts", 1114)];
        let tuple = TupleData {
            columns: vec![
                TupleColumn::Text("2024-02-01".to_string()),
                TupleColumn::Text("2024-02-01 10:11:12".to_string()),
            ],
        };
        let decoded = decode_tuple(&columns, &tuple).unwrap();
        assert_eq!(
            decoded,
            vec![
                DecodedColumn::Value(ScalarValue::Text("2024-02-01".to_string())),
                DecodedColumn::Value(ScalarValue::Text("2024-02-01 10:11:12".to_string())),
            ]
        );
    }
}
