#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn encode_result_data_row_message(
    row: &[ScalarValue],
    fields: &[RowDescriptionField],
) -> Result<BackendMessage, SessionError> {
    if row.len() != fields.len() {
        return Err(SessionError {
            message: "row width does not match row description field count".to_string(),
        });
    }
    // Binary-format *encoding* is opt-in per column via Bind's
    // result_format_codes. NULL must surface with length -1 regardless of
    // the chosen format. `BackendMessage::DataRow` uses `Vec<String>` with
    // no NULL slot, so whenever a row contains a NULL or any column is
    // binary we use `DataRowBinary` (which accepts `Option<Vec<u8>>`) even
    // for text fields — the bytes are still UTF-8 in that case. The on-wire
    // format is identical either way; the message-variant choice is purely
    // an internal representation detail of OpenAssay.
    let needs_null_slot = row.iter().any(|value| matches!(value, ScalarValue::Null));
    let needs_binary = fields.iter().any(|field| field.format_code == 1);
    if !needs_binary && !needs_null_slot {
        return Ok(BackendMessage::DataRow {
            values: row.iter().map(ScalarValue::render).collect(),
        });
    }

    let mut values = Vec::with_capacity(row.len());
    for (value, field) in row.iter().zip(fields.iter()) {
        values.push(encode_result_field(value, field)?);
    }
    Ok(BackendMessage::DataRowBinary { values })
}

fn encode_result_field(
    value: &ScalarValue,
    field: &RowDescriptionField,
) -> Result<Option<Vec<u8>>, SessionError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(None);
    }
    let encoded = match field.format_code {
        0 => value.render().into_bytes(),
        1 => encode_binary_scalar(value, field.type_oid, "result")?,
        other => {
            return Err(SessionError {
                message: format!("unsupported result format code {other}"),
            });
        }
    };
    Ok(Some(encoded))
}

pub(super) fn encode_binary_scalar(
    value: &ScalarValue,
    type_oid: PgType,
    context: &str,
) -> Result<Vec<u8>, SessionError> {
    // Per pg_type.oid. Kept as match literals (rather than named constants)
    // for grep-ability and to match what clients put on the wire.
    match (type_oid, value) {
        // ── booleans ────────────────────────────────────────────────
        (16, ScalarValue::Bool(v)) => Ok(vec![u8::from(*v)]),
        (16, ScalarValue::Text(v)) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(vec![1]),
            "false" | "f" | "0" => Ok(vec![0]),
            _ => Err(SessionError {
                message: format!("{context} boolean field is invalid"),
            }),
        },
        // ── integer family: narrow from the i64 storage representation
        // Widths are validated per-type so drivers that sent smaller values
        // still decode correctly.
        (21, ScalarValue::Int(v)) => {
            let narrow = i16::try_from(*v).map_err(|_| SessionError {
                message: format!("{context} int2 value {v} out of range"),
            })?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        (23 | 26 | 24, ScalarValue::Int(v)) => {
            // int4 / oid / regproc all ship as 4-byte big-endian.
            let narrow = i32::try_from(*v).map_err(|_| SessionError {
                message: format!("{context} int4 value {v} out of range"),
            })?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        (20, ScalarValue::Int(v)) => Ok(v.to_be_bytes().to_vec()),
        (20 | 21 | 23 | 26 | 24, ScalarValue::Text(v)) => {
            // Text-stored integers (e.g. from literal rendering) — reparse
            // and re-encode at the declared wire width.
            let parsed: i64 = v.trim().parse().map_err(|_| SessionError {
                message: format!("{context} integer field is invalid"),
            })?;
            match type_oid {
                20 => Ok(parsed.to_be_bytes().to_vec()),
                21 => Ok(i16::try_from(parsed)
                    .map_err(|_| SessionError {
                        message: format!("{context} int2 value {parsed} out of range"),
                    })?
                    .to_be_bytes()
                    .to_vec()),
                _ => Ok(i32::try_from(parsed)
                    .map_err(|_| SessionError {
                        message: format!("{context} int4 value {parsed} out of range"),
                    })?
                    .to_be_bytes()
                    .to_vec()),
            }
        }
        // ── float family ────────────────────────────────────────────
        (700, ScalarValue::Float(v)) => {
            // float4: narrow f64 -> f32. Note: f32::from(f64) doesn't exist;
            // use the `as` conversion, which matches PG's internal narrowing.
            #[allow(clippy::cast_possible_truncation)]
            let narrow = *v as f32;
            Ok(narrow.to_bits().to_be_bytes().to_vec())
        }
        (701, ScalarValue::Float(v)) => Ok(v.to_bits().to_be_bytes().to_vec()),
        (700, ScalarValue::Text(v)) => {
            let parsed: f64 = v.trim().parse().map_err(|_| SessionError {
                message: format!("{context} float4 field is invalid"),
            })?;
            #[allow(clippy::cast_possible_truncation)]
            let narrow = parsed as f32;
            Ok(narrow.to_bits().to_be_bytes().to_vec())
        }
        (701, ScalarValue::Text(v)) => v
            .trim()
            .parse::<f64>()
            .map(|parsed| parsed.to_bits().to_be_bytes().to_vec())
            .map_err(|_| SessionError {
                message: format!("{context} float8 field is invalid"),
            }),
        // ── text-like: the wire format is just raw UTF-8 ────────────
        (25 | 1042 | 1043 | 19 | 114, ScalarValue::Text(v)) => Ok(v.as_bytes().to_vec()),
        (25 | 1042 | 1043 | 19 | 114, other) => Ok(other.render().into_bytes()),
        // ── bytea: literal bytes. ScalarValue::Text stores hex-escaped
        // form ("\x010203") after CAST; we un-hex before shipping.
        (17, ScalarValue::Text(v)) => decode_pg_bytea_text(v, context),
        // ── uuid: 16 bytes, parsed from text form with or without dashes
        (2950, ScalarValue::Text(v)) => decode_uuid_text(v, context),
        // ── date: i32 days since 2000-01-01
        (1082, ScalarValue::Text(v)) => {
            let days = parse_pg_date_days(v)?;
            Ok(days.to_be_bytes().to_vec())
        }
        // ── timestamp / timestamptz: i64 microseconds since 2000-01-01 00:00:00 UTC
        (1114 | 1184, ScalarValue::Text(v)) => {
            let micros = parse_pg_timestamp_micros(v)?;
            Ok(micros.to_be_bytes().to_vec())
        }
        // ── time: i64 microseconds since 00:00:00
        (1083, ScalarValue::Text(v)) => {
            let micros = parse_pg_time_micros(v)?;
            Ok(micros.to_be_bytes().to_vec())
        }
        // ── interval: i64 microseconds, i32 days, i32 months (16 bytes).
        // The engine's `IntervalValue::seconds` is second-precision only, so
        // sub-second fractions of the text form are already truncated before
        // we reach this encoder — we multiply seconds × 1_000_000 on the wire.
        (1186, ScalarValue::Text(v)) => {
            let (months, days, micros) = parse_pg_interval_parts(v)?;
            let mut out = Vec::with_capacity(16);
            out.extend_from_slice(&micros.to_be_bytes());
            out.extend_from_slice(&days.to_be_bytes());
            out.extend_from_slice(&months.to_be_bytes());
            Ok(out)
        }
        // ── jsonb: 1-byte version prefix (always 1 per PG 9.4+) then JSON text
        (3802, ScalarValue::Text(v)) => {
            let mut out = Vec::with_capacity(v.len() + 1);
            out.push(1);
            out.extend_from_slice(v.as_bytes());
            Ok(out)
        }
        // ── 1-D array: header then length-prefixed element payloads.
        // Multi-dim arrays are intentionally rejected on decode; encoding is
        // driven by `ScalarValue::Array(Vec<…>)` which is flat by construction.
        (array_oid, ScalarValue::Array(values))
            if crate::types::element_oid_from_array_oid(array_oid).is_some() =>
        {
            encode_pg_array_binary(array_oid, values, context)
        }
        // ── NULL: callers use length=-1 framing; payload is empty.
        (_, ScalarValue::Null) => Ok(Vec::new()),
        _ => Err(SessionError {
            message: format!("{context} binary type oid {type_oid} is not supported"),
        }),
    }
}

/// Build the PG binary array wire payload for a flat (1-D) `ScalarValue::Array`.
///
/// Layout (see Postgres `array_send` in src/backend/utils/adt/arrayfuncs.c):
/// ```text
///   i32 ndim            number of dimensions (1 here)
///   i32 dataoffset      0 — NULLs are inline as length=-1, no null bitmap
///   i32 element_oid     OID of the element type
///   per-dim:
///     i32 size            len(values)
///     i32 lower_bound     1 (PG default)
///   per-element:
///     i32 length          -1 for NULL, otherwise byte count
///     bytes[]             encoded element (recursive encode_binary_scalar)
/// ```
fn encode_pg_array_binary(
    array_oid: PgType,
    values: &[ScalarValue],
    context: &str,
) -> Result<Vec<u8>, SessionError> {
    let element_oid =
        crate::types::element_oid_from_array_oid(array_oid).ok_or_else(|| SessionError {
            message: format!("{context} binary array oid {array_oid} is not supported"),
        })?;
    let mut out = Vec::with_capacity(20 + values.len() * 8);
    out.extend_from_slice(&1i32.to_be_bytes()); // ndim
    out.extend_from_slice(&0i32.to_be_bytes()); // dataoffset
    out.extend_from_slice(&element_oid.to_be_bytes());
    // For empty arrays PG sometimes emits ndim=0 with no dim headers. We
    // always emit ndim=1 + a zero-length dim; tokio-postgres accepts both.
    out.extend_from_slice(&(values.len() as i32).to_be_bytes()); // dim size
    out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
    for value in values {
        if matches!(value, ScalarValue::Null) {
            out.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let bytes = encode_binary_scalar(value, element_oid, context)?;
        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

/// Decode the Postgres bytea text format into raw bytes.
///
/// Supports both the modern `\x`-hex form (`'\x01ab02'`) and the legacy
/// octal-escape form (`'\\001\\002'`). Bytes with no escape pass through.
fn decode_pg_bytea_text(text: &str, context: &str) -> Result<Vec<u8>, SessionError> {
    if let Some(hex) = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
    {
        let cleaned: String = hex.chars().filter(|ch| !ch.is_ascii_whitespace()).collect();
        if !cleaned.len().is_multiple_of(2) {
            return Err(SessionError {
                message: format!("{context} bytea hex literal has odd length"),
            });
        }
        let mut out = Vec::with_capacity(cleaned.len() / 2);
        for chunk in cleaned.as_bytes().chunks(2) {
            let byte = u8::from_str_radix(
                std::str::from_utf8(chunk).map_err(|_| SessionError {
                    message: format!("{context} bytea hex literal is invalid"),
                })?,
                16,
            )
            .map_err(|_| SessionError {
                message: format!("{context} bytea hex literal is invalid"),
            })?;
            out.push(byte);
        }
        return Ok(out);
    }
    // Fallback: treat as already-raw bytes. Legacy-escape handling isn't
    // implemented here; callers that need it should use the \x form.
    Ok(text.as_bytes().to_vec())
}

/// Decode a UUID from its 36-char hyphenated text form (or 32-char no-dash
/// form) into 16 raw bytes.
fn decode_uuid_text(text: &str, context: &str) -> Result<Vec<u8>, SessionError> {
    let trimmed = text.trim();
    let hex: String = trimmed.chars().filter(|ch| *ch != '-').collect();
    if hex.len() != 32 {
        return Err(SessionError {
            message: format!("{context} uuid text length is invalid"),
        });
    }
    let mut out = Vec::with_capacity(16);
    for chunk in hex.as_bytes().chunks(2) {
        let byte = u8::from_str_radix(
            std::str::from_utf8(chunk).map_err(|_| SessionError {
                message: format!("{context} uuid is not valid utf8"),
            })?,
            16,
        )
        .map_err(|_| SessionError {
            message: format!("{context} uuid hex is invalid"),
        })?;
        out.push(byte);
    }
    Ok(out)
}

pub(super) fn decode_binary_scalar(
    raw: &[u8],
    type_oid: PgType,
    context: &str,
) -> Result<ScalarValue, SessionError> {
    match type_oid {
        16 => {
            if raw.len() != 1 {
                return Err(SessionError {
                    message: format!("{context} boolean field length must be 1"),
                });
            }
            Ok(ScalarValue::Bool(raw[0] != 0))
        }
        21 => {
            if raw.len() != 2 {
                return Err(SessionError {
                    message: format!("{context} int2 field length must be 2"),
                });
            }
            Ok(ScalarValue::Int(i64::from(i16::from_be_bytes([
                raw[0], raw[1],
            ]))))
        }
        23 | 26 | 24 => {
            if raw.len() != 4 {
                return Err(SessionError {
                    message: format!("{context} int4/oid field length must be 4"),
                });
            }
            Ok(ScalarValue::Int(i64::from(i32::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3],
            ]))))
        }
        20 => {
            if raw.len() != 8 {
                return Err(SessionError {
                    message: format!("{context} int8 field length must be 8"),
                });
            }
            Ok(ScalarValue::Int(i64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ])))
        }
        700 => {
            if raw.len() != 4 {
                return Err(SessionError {
                    message: format!("{context} float4 field length must be 4"),
                });
            }
            let bits = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
            Ok(ScalarValue::Float(f64::from(f32::from_bits(bits))))
        }
        701 => {
            if raw.len() != 8 {
                return Err(SessionError {
                    message: format!("{context} float8 field length must be 8"),
                });
            }
            let bits = u64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Ok(ScalarValue::Float(f64::from_bits(bits)))
        }
        // text / varchar / bpchar / name / json — all raw UTF-8 on the wire
        25 | 1042 | 1043 | 19 | 114 => Ok(ScalarValue::Text(
            String::from_utf8(raw.to_vec()).map_err(|_| SessionError {
                message: format!("{context} text-like field is not valid utf8"),
            })?,
        )),
        // bytea: raw bytes → render as `\x…` hex text (matches ScalarValue
        // convention elsewhere in the engine).
        17 => {
            let mut out = String::with_capacity(2 + raw.len() * 2);
            out.push_str("\\x");
            for b in raw {
                out.push_str(&format!("{b:02x}"));
            }
            Ok(ScalarValue::Text(out))
        }
        // uuid: 16 bytes → canonical hyphenated text
        2950 => {
            if raw.len() != 16 {
                return Err(SessionError {
                    message: format!("{context} uuid field length must be 16"),
                });
            }
            let mut out = String::with_capacity(36);
            for (i, b) in raw.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    out.push('-');
                }
                out.push_str(&format!("{b:02x}"));
            }
            Ok(ScalarValue::Text(out))
        }
        1082 => {
            if raw.len() != 4 {
                return Err(SessionError {
                    message: format!("{context} date field length must be 4"),
                });
            }
            let days = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
            Ok(ScalarValue::Text(format_pg_date_from_days(days)))
        }
        1114 | 1184 => {
            if raw.len() != 8 {
                return Err(SessionError {
                    message: format!("{context} timestamp field length must be 8"),
                });
            }
            let micros = i64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Ok(ScalarValue::Text(format_pg_timestamp_from_micros(micros)))
        }
        1083 => {
            if raw.len() != 8 {
                return Err(SessionError {
                    message: format!("{context} time field length must be 8"),
                });
            }
            let micros = i64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Ok(ScalarValue::Text(format_pg_time_from_micros(micros)))
        }
        1186 => {
            if raw.len() != 16 {
                return Err(SessionError {
                    message: format!("{context} interval field length must be 16"),
                });
            }
            let micros = i64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            let days = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
            let months = i32::from_be_bytes([raw[12], raw[13], raw[14], raw[15]]);
            Ok(ScalarValue::Text(format_pg_interval_from_parts(
                months, days, micros,
            )))
        }
        // jsonb binary format: 1-byte version prefix (1), then UTF-8 JSON text
        3802 => {
            if raw.is_empty() {
                return Err(SessionError {
                    message: format!("{context} jsonb field is empty"),
                });
            }
            if raw[0] != 1 {
                return Err(SessionError {
                    message: format!("{context} unsupported jsonb binary version {}", raw[0]),
                });
            }
            Ok(ScalarValue::Text(
                String::from_utf8(raw[1..].to_vec()).map_err(|_| SessionError {
                    message: format!("{context} jsonb payload is not valid utf8"),
                })?,
            ))
        }
        other if crate::types::element_oid_from_array_oid(other).is_some() => {
            decode_pg_array_binary(raw, other, context)
        }
        other => Err(SessionError {
            message: format!("{context} binary type oid {other} is not supported"),
        }),
    }
}

/// Decode the PG binary array wire format into `ScalarValue::Array`. Only
/// 1-D arrays are supported — `ndim > 1` is rejected rather than flattened
/// so callers don't silently lose structure.
fn decode_pg_array_binary(
    raw: &[u8],
    array_oid: PgType,
    context: &str,
) -> Result<ScalarValue, SessionError> {
    if raw.len() < 12 {
        return Err(SessionError {
            message: format!("{context} array header truncated"),
        });
    }
    let ndim = i32::from_be_bytes(raw[0..4].try_into().unwrap());
    // PG sometimes emits ndim=0 for empty arrays; accept that as an empty 1-D.
    if !(0..=1).contains(&ndim) {
        return Err(SessionError {
            message: format!("{context} multi-dim arrays (ndim={ndim}) are not supported yet"),
        });
    }
    // dataoffset at [4..8] — we don't implement null bitmaps; PG normally
    // writes 0 here. Non-zero means a null bitmap is present, which we skip
    // because NULLs are always encoded inline as length=-1 on the send side.
    // Reading a null bitmap here would be incorrect for flat 1-D decoding;
    // surface an error so we don't silently mis-read.
    let dataoffset = i32::from_be_bytes(raw[4..8].try_into().unwrap());
    if dataoffset != 0 {
        return Err(SessionError {
            message: format!("{context} array null bitmap is not supported"),
        });
    }
    let element_oid = u32::from_be_bytes(raw[8..12].try_into().unwrap());
    let expected_element =
        crate::types::element_oid_from_array_oid(array_oid).ok_or_else(|| SessionError {
            message: format!("{context} array oid {array_oid} is not supported"),
        })?;
    if element_oid != expected_element {
        return Err(SessionError {
            message: format!(
                "{context} array element oid mismatch: header={element_oid}, expected={expected_element}"
            ),
        });
    }
    if ndim == 0 {
        return Ok(ScalarValue::Array(Vec::new()));
    }
    if raw.len() < 20 {
        return Err(SessionError {
            message: format!("{context} array dim header truncated"),
        });
    }
    let dim_size = i32::from_be_bytes(raw[12..16].try_into().unwrap());
    // lower_bound at [16..20] — we don't propagate it because ScalarValue::Array
    // is always 1-based in the engine. Ignore.
    let mut cursor = 20;
    let mut values = Vec::with_capacity(dim_size.max(0) as usize);
    for _ in 0..dim_size {
        if cursor + 4 > raw.len() {
            return Err(SessionError {
                message: format!("{context} array element length truncated"),
            });
        }
        let len = i32::from_be_bytes(raw[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        if len == -1 {
            values.push(ScalarValue::Null);
            continue;
        }
        if len < 0 {
            return Err(SessionError {
                message: format!("{context} array element has negative length {len}"),
            });
        }
        let end = cursor + len as usize;
        if end > raw.len() {
            return Err(SessionError {
                message: format!("{context} array element payload truncated"),
            });
        }
        let elem = decode_binary_scalar(&raw[cursor..end], element_oid, context)?;
        values.push(elem);
        cursor = end;
    }
    Ok(ScalarValue::Array(values))
}

pub(super) fn parse_pg_date_days(text: &str) -> Result<i32, SessionError> {
    let (year, month, day) = parse_date_ymd(text.trim())?;
    let day_number = days_from_civil(year, month, day);
    let pg_epoch = days_from_civil(2000, 1, 1);
    let delta = day_number - pg_epoch;
    i32::try_from(delta).map_err(|_| SessionError {
        message: "date value is out of range".to_string(),
    })
}

pub(super) fn parse_pg_timestamp_micros(text: &str) -> Result<i64, SessionError> {
    let trimmed = text.trim();
    let (date_part, time_part) = if let Some((date, time)) = trimmed.split_once(' ') {
        (date, time)
    } else if let Some((date, time)) = trimmed.split_once('T') {
        (date, time)
    } else {
        (trimmed, "00:00:00")
    };
    let (year, month, day) = parse_date_ymd(date_part.trim())?;
    let (hour, minute, second, micros) = parse_time_hms_micros(time_part.trim())?;
    let day_number = days_from_civil(year, month, day);
    let pg_epoch = days_from_civil(2000, 1, 1);
    let delta_days = day_number - pg_epoch;
    let seconds_of_day = (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;
    Ok(delta_days * 86_400_000_000 + seconds_of_day * 1_000_000 + micros as i64)
}

pub(super) fn format_pg_date_from_days(days: i32) -> String {
    let pg_epoch = days_from_civil(2000, 1, 1);
    let absolute_days = pg_epoch + days as i64;
    let (year, month, day) = civil_from_days(absolute_days);
    format!("{year:04}-{month:02}-{day:02}")
}

pub(super) fn parse_pg_time_micros(text: &str) -> Result<i64, SessionError> {
    let (hour, minute, second, micros) = parse_time_hms_micros(text.trim())?;
    Ok((hour as i64) * 3_600_000_000
        + (minute as i64) * 60_000_000
        + (second as i64) * 1_000_000
        + (micros as i64))
}

pub(super) fn format_pg_time_from_micros(micros: i64) -> String {
    let day_micros = 86_400_000_000i64;
    let clamped = micros.rem_euclid(day_micros);
    let hour = clamped / 3_600_000_000;
    let minute = (clamped % 3_600_000_000) / 60_000_000;
    let second = (clamped % 60_000_000) / 1_000_000;
    let fractional = clamped % 1_000_000;
    if fractional == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{hour:02}:{minute:02}:{second:02}.{fractional:06}")
    }
}

/// Parse the engine's interval text format `"M mons D days [-]HH:MM:SS"` into
/// `(months, days, microseconds)` for the wire encoder. The engine stores
/// seconds with no sub-second precision, so we multiply by 1_000_000 here.
pub(super) fn parse_pg_interval_parts(text: &str) -> Result<(i32, i32, i64), SessionError> {
    let trimmed = text.trim();
    let invalid = || SessionError {
        message: format!("interval text `{trimmed}` is invalid"),
    };
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() != 5 || parts[1] != "mons" || parts[3] != "days" {
        return Err(invalid());
    }
    let months: i32 = parts[0].parse().map_err(|_| invalid())?;
    let days: i32 = parts[2].parse().map_err(|_| invalid())?;
    let time_part = parts[4];
    let (sign, hms) = match time_part.strip_prefix('-') {
        Some(rest) => (-1i64, rest),
        None => (1i64, time_part),
    };
    // Interval HMS is unbounded — `INTERVAL '36 hours'` renders as
    // `"36:00:00"`, which `parse_time_hms_micros` would reject. Split
    // inline without the 24-hour cap.
    let hms_parts: Vec<&str> = hms.split(':').collect();
    if hms_parts.len() != 3 {
        return Err(invalid());
    }
    let hour: u64 = hms_parts[0].parse().map_err(|_| invalid())?;
    let minute: u64 = hms_parts[1].parse().map_err(|_| invalid())?;
    if minute >= 60 {
        return Err(invalid());
    }
    let (second, micros_frac) = if let Some((sec, frac)) = hms_parts[2].split_once('.') {
        let second: u64 = sec.parse().map_err(|_| invalid())?;
        let digits: String = frac.chars().take(6).collect();
        let mut padded = digits;
        while padded.len() < 6 {
            padded.push('0');
        }
        let micros: u64 = padded.parse().map_err(|_| invalid())?;
        (second, micros)
    } else {
        let second: u64 = hms_parts[2].parse().map_err(|_| invalid())?;
        (second, 0)
    };
    if second >= 60 {
        return Err(invalid());
    }
    let magnitude = (hour as i64) * 3_600_000_000
        + (minute as i64) * 60_000_000
        + (second as i64) * 1_000_000
        + (micros_frac as i64);
    Ok((months, days, sign * magnitude))
}

/// Format `(months, days, microseconds)` back into the engine's interval text
/// format so callers see a consistent `ScalarValue::Text` representation.
pub(super) fn format_pg_interval_from_parts(months: i32, days: i32, micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs = micros.unsigned_abs();
    let hours = abs / 3_600_000_000;
    let minutes = (abs % 3_600_000_000) / 60_000_000;
    let seconds = (abs % 60_000_000) / 1_000_000;
    // The engine's text format does not carry sub-second digits; the decoder
    // preserves that shape so round-tripping stays stable.
    format!("{months} mons {days} days {sign}{hours:02}:{minutes:02}:{seconds:02}")
}

pub(super) fn format_pg_timestamp_from_micros(micros: i64) -> String {
    let pg_epoch = days_from_civil(2000, 1, 1);
    let day_micros = 86_400_000_000i64;
    let days = micros.div_euclid(day_micros);
    let micros_of_day = micros.rem_euclid(day_micros);
    let absolute_days = pg_epoch + days;
    let (year, month, day) = civil_from_days(absolute_days);
    let hour = micros_of_day / 3_600_000_000;
    let minute = (micros_of_day % 3_600_000_000) / 60_000_000;
    let second = (micros_of_day % 60_000_000) / 1_000_000;
    let fractional = micros_of_day % 1_000_000;
    if fractional == 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{fractional:06}")
    }
}

fn parse_date_ymd(text: &str) -> Result<(i32, u32, u32), SessionError> {
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(SessionError {
            message: "date value is invalid".to_string(),
        });
    }
    let year = parts[0].parse::<i32>().map_err(|_| SessionError {
        message: "date year is invalid".to_string(),
    })?;
    let month = parts[1].parse::<u32>().map_err(|_| SessionError {
        message: "date month is invalid".to_string(),
    })?;
    let day = parts[2].parse::<u32>().map_err(|_| SessionError {
        message: "date day is invalid".to_string(),
    })?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(SessionError {
            message: "date value is out of range".to_string(),
        });
    }
    Ok((year, month, day))
}

fn parse_time_hms_micros(text: &str) -> Result<(u32, u32, u32, u32), SessionError> {
    let parts = text.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(SessionError {
            message: "timestamp time component is invalid".to_string(),
        });
    }
    let hour = parts[0].parse::<u32>().map_err(|_| SessionError {
        message: "timestamp hour is invalid".to_string(),
    })?;
    let minute = parts[1].parse::<u32>().map_err(|_| SessionError {
        message: "timestamp minute is invalid".to_string(),
    })?;
    let (second, micros) = if let Some((sec, frac)) = parts[2].split_once('.') {
        let second = sec.parse::<u32>().map_err(|_| SessionError {
            message: "timestamp second is invalid".to_string(),
        })?;
        let digits = frac
            .chars()
            .take(6)
            .filter(|ch| ch.is_ascii_digit())
            .collect::<String>();
        let mut micros_text = digits;
        while micros_text.len() < 6 {
            micros_text.push('0');
        }
        let micros = if micros_text.is_empty() {
            0
        } else {
            micros_text.parse::<u32>().map_err(|_| SessionError {
                message: "timestamp fractional second is invalid".to_string(),
            })?
        };
        (second, micros)
    } else {
        let second = parts[2].parse::<u32>().map_err(|_| SessionError {
            message: "timestamp second is invalid".to_string(),
        })?;
        (second, 0)
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(SessionError {
            message: "timestamp time is out of range".to_string(),
        });
    }
    Ok((hour, minute, second, micros))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let mut y = year;
    let m = month as i32;
    y -= (m <= 2) as i32;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}
