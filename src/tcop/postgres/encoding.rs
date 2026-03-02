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
    let requires_binary = fields.iter().any(|field| field.format_code == 1)
        || row.iter().any(|value| matches!(value, ScalarValue::Null));
    if !requires_binary {
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
    match (type_oid, value) {
        (16, ScalarValue::Bool(v)) => Ok(vec![u8::from(*v)]),
        (20, ScalarValue::Int(v)) => Ok(v.to_be_bytes().to_vec()),
        (701, ScalarValue::Float(v)) => Ok(v.to_bits().to_be_bytes().to_vec()),
        (25, ScalarValue::Text(v)) => Ok(v.as_bytes().to_vec()),
        (25, other) => Ok(other.render().into_bytes()),
        (20, ScalarValue::Text(v)) => v
            .trim()
            .parse::<i64>()
            .map(|parsed| parsed.to_be_bytes().to_vec())
            .map_err(|_| SessionError {
                message: format!("{context} integer field is invalid"),
            }),
        (701, ScalarValue::Text(v)) => v
            .trim()
            .parse::<f64>()
            .map(|parsed| parsed.to_bits().to_be_bytes().to_vec())
            .map_err(|_| SessionError {
                message: format!("{context} float field is invalid"),
            }),
        (16, ScalarValue::Text(v)) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(vec![1]),
            "false" | "f" | "0" => Ok(vec![0]),
            _ => Err(SessionError {
                message: format!("{context} boolean field is invalid"),
            }),
        },
        (1082, ScalarValue::Text(v)) => {
            let days = parse_pg_date_days(v)?;
            Ok(days.to_be_bytes().to_vec())
        }
        (1114, ScalarValue::Text(v)) => {
            let micros = parse_pg_timestamp_micros(v)?;
            Ok(micros.to_be_bytes().to_vec())
        }
        (_, ScalarValue::Null) => Ok(Vec::new()),
        _ => Err(SessionError {
            message: format!("{context} binary type oid {type_oid} is not supported"),
        }),
    }
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
        25 => Ok(ScalarValue::Text(String::from_utf8(raw.to_vec()).map_err(
            |_| SessionError {
                message: format!("{context} text field is not valid utf8"),
            },
        )?)),
        1082 => {
            if raw.len() != 4 {
                return Err(SessionError {
                    message: format!("{context} date field length must be 4"),
                });
            }
            let days = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
            Ok(ScalarValue::Text(format_pg_date_from_days(days)))
        }
        1114 => {
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
        other => Err(SessionError {
            message: format!("{context} binary type oid {other} is not supported"),
        }),
    }
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
