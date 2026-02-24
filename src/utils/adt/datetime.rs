use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::{parse_f64_scalar, parse_i64_scalar};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalOperand {
    pub(crate) datetime: DateTimeValue,
    pub(crate) date_only: bool,
}

/// Returns true if the text represents a special infinity datetime value.
#[allow(dead_code)]
pub(crate) fn is_infinity_datetime(text: &str) -> bool {
    let lower = text.trim().to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "infinity" | "+infinity" | "inf" | "+inf" | "-infinity" | "-inf"
    )
}

pub(crate) fn parse_temporal_operand(value: &ScalarValue) -> Option<TemporalOperand> {
    let ScalarValue::Text(text) = value else {
        return None;
    };
    let trimmed = text.trim();
    let has_time = trimmed.contains('T') || trimmed.contains(' ');
    let datetime = parse_datetime_text(trimmed).ok()?;
    Some(TemporalOperand {
        datetime,
        date_only: !has_time,
    })
}

pub(crate) fn temporal_add_days(temporal: TemporalOperand, days: i64) -> ScalarValue {
    // Infinity dates stay as infinity under arithmetic
    if temporal.datetime.date.year == 294276 && temporal.datetime.date.month == 12 {
        return ScalarValue::Text("infinity".to_string());
    }
    if temporal.datetime.date.year == -4713
        && temporal.datetime.date.month == 1
        && temporal.datetime.date.day == 1
    {
        return ScalarValue::Text("-infinity".to_string());
    }
    let mut datetime = temporal.datetime;
    datetime.date = add_days(datetime.date, days);
    if temporal.date_only {
        ScalarValue::Text(format_date(datetime.date))
    } else {
        ScalarValue::Text(format_timestamp(datetime))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateValue {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateTimeValue {
    pub(crate) date: DateValue,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) microsecond: u32,
}

pub(crate) fn eval_date_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_date(datetime.date)))
}

pub(crate) fn eval_timestamp_function(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(value)?;
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

pub(crate) fn eval_extract_or_date_part(
    field: &ScalarValue,
    source: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let datetime = parse_datetime_scalar(source)?;
    let value = match field_name.as_str() {
        "year" => ScalarValue::Int(datetime.date.year as i64),
        "month" => ScalarValue::Int(datetime.date.month as i64),
        "day" => ScalarValue::Int(datetime.date.day as i64),
        "hour" => ScalarValue::Int(datetime.hour as i64),
        "minute" => ScalarValue::Int(datetime.minute as i64),
        "second" => {
            // Return seconds with fractional part
            let total_seconds =
                datetime.second as f64 + (datetime.microsecond as f64 / 1_000_000.0);
            ScalarValue::Float(total_seconds)
        }
        "millisecond" => {
            // Return milliseconds including seconds part
            let total_ms =
                (datetime.second as f64 * 1000.0) + (datetime.microsecond as f64 / 1000.0);
            ScalarValue::Float(total_ms)
        }
        "microsecond" => {
            // Return microseconds including seconds part
            let total_us = (datetime.second as i64 * 1_000_000) + datetime.microsecond as i64;
            ScalarValue::Int(total_us)
        }
        "dow" => ScalarValue::Int(day_of_week(datetime.date) as i64),
        "doy" => ScalarValue::Int(day_of_year(datetime.date) as i64),
        "epoch" => {
            // Return epoch with fractional seconds
            let base_epoch = datetime_to_epoch_seconds(datetime);
            let frac = datetime.microsecond as f64 / 1_000_000.0;
            ScalarValue::Float(base_epoch as f64 + frac)
        }
        _ => {
            return Err(EngineError {
                message: format!("unsupported date/time field {field_name}"),
            });
        }
    };
    Ok(value)
}

pub(crate) fn eval_date_trunc(
    field: &ScalarValue,
    source: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(field, ScalarValue::Null) || matches!(source, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let field_name = field.render().trim().to_ascii_lowercase();
    let mut datetime = parse_datetime_scalar(source)?;
    match field_name.as_str() {
        "year" => {
            datetime.date.month = 1;
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
            datetime.microsecond = 0;
        }
        "month" => {
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
            datetime.microsecond = 0;
        }
        "day" => {
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
            datetime.microsecond = 0;
        }
        "hour" => {
            datetime.minute = 0;
            datetime.second = 0;
            datetime.microsecond = 0;
        }
        "minute" => {
            datetime.second = 0;
            datetime.microsecond = 0;
        }
        "second" => {
            datetime.microsecond = 0;
        }
        _ => {
            return Err(EngineError {
                message: format!("unsupported date_trunc field {field_name}"),
            });
        }
    }
    Ok(ScalarValue::Text(format_timestamp(datetime)))
}

pub(crate) fn eval_date_add_sub(
    date_value: &ScalarValue,
    day_delta: &ScalarValue,
    add: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(date_value, ScalarValue::Null) || matches!(day_delta, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let datetime = parse_datetime_scalar(date_value)?;
    let mut days = parse_i64_scalar(day_delta, "date_add/date_sub expects integer day count")?;
    if !add {
        days = -days;
    }
    let shifted = add_days(datetime.date, days);
    Ok(ScalarValue::Text(format_date(shifted)))
}

pub(crate) fn current_timestamp_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_timestamp(dt))
}

pub(crate) fn current_date_string() -> Result<String, EngineError> {
    let dt = current_utc_datetime()?;
    Ok(format_date(dt.date))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntervalValue {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) seconds: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum JustifyMode {
    Hours,
    Days,
    Full,
}

pub(crate) fn eval_age(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let (left, right) = if args.len() == 1 {
        let current = current_utc_datetime()?;
        let current_midnight = DateTimeValue {
            date: current.date,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
        };
        (current_midnight, parse_datetime_scalar(&args[0])?)
    } else {
        (
            parse_datetime_scalar(&args[0])?,
            parse_datetime_scalar(&args[1])?,
        )
    };
    let delta = datetime_to_epoch_seconds(left) - datetime_to_epoch_seconds(right);
    let interval = interval_from_seconds(delta);
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_to_timestamp(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let seconds = parse_f64_scalar(value, "to_timestamp() expects numeric input")?;
    let dt = datetime_from_epoch_seconds(seconds.trunc() as i64);
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

pub(crate) fn eval_to_timestamp_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let dt = parse_datetime_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_timestamp(dt)))
}

pub(crate) fn eval_to_date_with_format(
    text: &ScalarValue,
    format: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(text, ScalarValue::Null) || matches!(format, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let date = parse_date_with_format(&text.render(), &format.render())?;
    Ok(ScalarValue::Text(format_date(date)))
}

pub(crate) fn eval_make_interval(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let years = parse_i64_scalar(&args[0], "make_interval() expects years")?;
    let months = parse_i64_scalar(&args[1], "make_interval() expects months")?;
    let weeks = parse_i64_scalar(&args[2], "make_interval() expects weeks")?;
    let days = parse_i64_scalar(&args[3], "make_interval() expects days")?;
    let hours = parse_i64_scalar(&args[4], "make_interval() expects hours")?;
    let mins = parse_i64_scalar(&args[5], "make_interval() expects mins")?;
    let secs = parse_f64_scalar(&args[6], "make_interval() expects secs")?;

    let interval = IntervalValue {
        months: years * 12 + months,
        days: weeks * 7 + days,
        seconds: hours * 3_600 + mins * 60 + secs.trunc() as i64,
    };
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_justify_interval(
    value: &ScalarValue,
    mode: JustifyMode,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut interval = parse_interval_text(&value.render())?;
    if matches!(mode, JustifyMode::Hours | JustifyMode::Full) {
        let extra_days = interval.seconds.div_euclid(86_400);
        interval.days += extra_days;
        interval.seconds = interval.seconds.rem_euclid(86_400);
    }
    if matches!(mode, JustifyMode::Days | JustifyMode::Full) {
        let extra_months = interval.days.div_euclid(30);
        interval.months += extra_months;
        interval.days = interval.days.rem_euclid(30);
    }
    Ok(ScalarValue::Text(format_interval(interval)))
}

pub(crate) fn eval_isfinite(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let finite = match value {
        ScalarValue::Text(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            !matches!(
                normalized.as_str(),
                "infinity" | "+infinity" | "inf" | "+inf" | "-infinity" | "-inf" | "nan"
            )
        }
        ScalarValue::Float(f) => f.is_finite(),
        _ => true,
    };
    Ok(ScalarValue::Bool(finite))
}

fn current_utc_datetime() -> Result<DateTimeValue, EngineError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let epoch_seconds = match now.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(err) => -(err.duration().as_secs() as i64),
    };
    Ok(datetime_from_epoch_seconds(epoch_seconds))
}

pub(crate) fn parse_datetime_scalar(value: &ScalarValue) -> Result<DateTimeValue, EngineError> {
    match value {
        ScalarValue::Text(v) => parse_datetime_text(v),
        ScalarValue::Int(v) => Ok(datetime_from_epoch_seconds(*v)),
        ScalarValue::Float(v) => Ok(datetime_from_epoch_seconds(*v as i64)),
        _ => Err(EngineError {
            message: "expected date/timestamp-compatible value".to_string(),
        }),
    }
}

pub(crate) fn parse_datetime_text(text: &str) -> Result<DateTimeValue, EngineError> {
    let raw = text.trim();
    if raw.is_empty() {
        return Err(EngineError {
            message: "invalid date/timestamp value".to_string(),
        });
    }

    // Handle special values
    let normalized = raw.to_ascii_lowercase();
    let first_token = normalized.split_whitespace().next().unwrap_or_default();
    match first_token {
        "now" | "current" => return current_utc_datetime(),
        "today" => {
            let dt = current_utc_datetime()?;
            return Ok(DateTimeValue {
                date: dt.date,
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
            });
        }
        "yesterday" => {
            let dt = current_utc_datetime()?;
            let date = add_days(dt.date, -1);
            return Ok(DateTimeValue {
                date,
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
            });
        }
        "tomorrow" => {
            let dt = current_utc_datetime()?;
            let date = add_days(dt.date, 1);
            return Ok(DateTimeValue {
                date,
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
            });
        }
        _ => {}
    }
    match normalized.as_str() {
        "infinity" | "+infinity" | "inf" | "+inf" => {
            // Return a sentinel max date to represent +infinity
            return Ok(DateTimeValue {
                date: DateValue {
                    year: 294276,
                    month: 12,
                    day: 31,
                },
                hour: 23,
                minute: 59,
                second: 59,
                microsecond: 999999,
            });
        }
        "-infinity" | "-inf" => {
            // Return a sentinel min date to represent -infinity
            return Ok(DateTimeValue {
                date: DateValue {
                    year: -4713,
                    month: 1,
                    day: 1,
                },
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
            });
        }
        "epoch" => {
            return Ok(DateTimeValue {
                date: DateValue {
                    year: 1970,
                    month: 1,
                    day: 1,
                },
                hour: 0,
                minute: 0,
                second: 0,
                microsecond: 0,
            });
        }
        _ => {}
    }

    // Split date and time parts
    // Be smart about this: spaces in dates like "January 8, 1999" should not be treated as date/time separators
    // Only split if we see a time-like pattern (HH:MM:SS with colons)
    let (date_part, time_part) = if let Some(pos) = raw.find('T') {
        // ISO 8601 format with 'T' separator
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else {
        // Check for space followed by time pattern (must contain colon for time)
        let mut date_end = raw.len();
        let mut has_time = false;

        // Look for a space followed by something that looks like a time (contains colon)
        for (i, c) in raw.char_indices() {
            if c == ' ' && i + 1 < raw.len() {
                let remaining = &raw[i + 1..];
                // A time must contain a colon (HH:MM or HH:MM:SS format)
                if remaining.contains(':') {
                    date_end = i;
                    has_time = true;
                    break;
                }
            }
        }

        if has_time {
            (&raw[..date_end], Some(&raw[date_end + 1..]))
        } else {
            (raw, None)
        }
    };

    // If no time part was found, check if the whole string is a time-only value
    // (e.g. "12:30:45", "13:30:25.575401", "11:59 PM"). Detect by: no T separator
    // found, no time_part split, and the string looks like HH:MM...
    if time_part.is_none() && date_part.contains(':') {
        let colon_pos = date_part.find(':').unwrap();
        // An hour is at most 2 digits (0-24); a 4-digit year before ':' is a date
        if colon_pos <= 2
            && let Ok((hour, minute, second, microsecond)) = parse_time_text(date_part)
        {
            // Use epoch date as placeholder; callers interested in time only
            // (EXTRACT, ::time cast, etc.) won't use the date field.
            return Ok(DateTimeValue {
                date: DateValue {
                    year: 2000,
                    month: 1,
                    day: 1,
                },
                hour,
                minute,
                second,
                microsecond,
            });
        }
    }

    let date = parse_date_text(date_part)?;
    let (hour, minute, second, microsecond) = match time_part {
        None => (0, 0, 0, 0),
        Some(time_raw) => parse_time_text(time_raw)?,
    };
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
        microsecond,
    })
}

fn parse_date_text(text: &str) -> Result<DateValue, EngineError> {
    let trimmed = text.trim();

    // Check for BC suffix
    let (date_str, is_bc) = if trimmed.to_ascii_uppercase().ends_with(" BC") {
        let s = &trimmed[..trimmed.len() - 3];
        (s.trim(), true)
    } else {
        (trimmed, false)
    };

    // Try ISO format first: YYYY-MM-DD
    if let Some(date) = try_parse_iso_date(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    // Try compact formats: YYYYMMDD, YYMMDD
    if let Some(date) = try_parse_compact_date(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    // Try Julian day: J<number>
    if let Some(date) = try_parse_julian_date(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    // Try year.day format: YYYY.DDD
    if let Some(date) = try_parse_year_day_format(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    // Try formats with month names
    if let Some(date) = try_parse_with_month_name(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    // Try numeric formats with separators (-, /, space)
    if let Some(date) = try_parse_numeric_date(date_str) {
        return apply_bc_if_needed(date, is_bc);
    }

    Err(EngineError {
        message: format!("invalid date value: \"{text}\""),
    })
}

fn apply_bc_if_needed(mut date: DateValue, is_bc: bool) -> Result<DateValue, EngineError> {
    if is_bc {
        date.year = -(date.year - 1);
    }
    Ok(date)
}

fn try_parse_iso_date(text: &str) -> Option<DateValue> {
    let parts: Vec<&str> = text.split('-').collect();
    if parts.len() != 3 {
        return None;
    }

    let year = parts[0].parse::<i32>().ok()?;
    let month = parts[1].parse::<u32>().ok()?;
    let day = parts[2].parse::<u32>().ok()?;

    if month == 0 || month > 12 || day == 0 {
        return None;
    }

    let max_day = days_in_month(year, month);
    if day > max_day {
        return None;
    }

    Some(DateValue { year, month, day })
}

fn try_parse_compact_date(text: &str) -> Option<DateValue> {
    if !text.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let year: i32;
    let month: u32;
    let day: u32;

    if text.len() == 8 {
        // YYYYMMDD
        year = text[0..4].parse().ok()?;
        month = text[4..6].parse().ok()?;
        day = text[6..8].parse().ok()?;
    } else if text.len() == 6 {
        // YYMMDD - assume 19xx or 20xx based on value
        let yy: i32 = text[0..2].parse().ok()?;
        year = if yy >= 70 { 1900 + yy } else { 2000 + yy };
        month = text[2..4].parse().ok()?;
        day = text[4..6].parse().ok()?;
    } else {
        return None;
    }

    if month == 0 || month > 12 || day == 0 {
        return None;
    }

    let max_day = days_in_month(year, month);
    if day > max_day {
        return None;
    }

    Some(DateValue { year, month, day })
}

fn try_parse_julian_date(text: &str) -> Option<DateValue> {
    if !text.starts_with('J') && !text.starts_with('j') {
        return None;
    }

    let julian_day: i64 = text[1..].parse().ok()?;
    Some(civil_from_days(julian_day - 2440588)) // PostgreSQL Julian day epoch
}

fn try_parse_year_day_format(text: &str) -> Option<DateValue> {
    if !text.contains('.') {
        return None;
    }

    let parts: Vec<&str> = text.split('.').collect();
    if parts.len() != 2 {
        return None;
    }

    let year: i32 = parts[0].parse().ok()?;
    let day_of_year: u32 = parts[1].parse().ok()?;

    if day_of_year == 0 {
        return None;
    }

    let days_in_year = if is_leap_year(year) { 366 } else { 365 };
    if day_of_year > days_in_year {
        return None;
    }

    // Convert day of year to month and day
    let mut remaining_days = day_of_year;
    let mut month = 1u32;

    while month <= 12 {
        let days_this_month = days_in_month(year, month);
        if remaining_days <= days_this_month {
            return Some(DateValue {
                year,
                month,
                day: remaining_days,
            });
        }
        remaining_days -= days_this_month;
        month += 1;
    }

    None
}

fn try_parse_with_month_name(text: &str) -> Option<DateValue> {
    // Split by various delimiters
    let parts: Vec<&str> = if text.contains(',') {
        // Handle comma-separated formats like "January 8, 1999"
        text.split(&[',', ' '][..])
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    } else if text.contains('-') {
        text.split('-').map(|s| s.trim()).collect()
    } else if text.contains(' ') {
        text.split_whitespace().collect()
    } else {
        return None;
    };

    if parts.len() < 2 {
        return None;
    }

    // Try to find month name
    let mut month_idx = None;
    let mut month_val = None;

    for (idx, part) in parts.iter().enumerate() {
        if let Some(m) = parse_month_name(part) {
            month_idx = Some(idx);
            month_val = Some(m);
            break;
        }
    }

    let month = month_val?;
    let month_pos = month_idx?;

    // Collect numeric parts
    let numeric_parts: Vec<i32> = parts
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != month_pos)
        .filter_map(|(_, s)| s.parse::<i32>().ok())
        .collect();

    if numeric_parts.len() < 2 {
        return None;
    }

    // Determine year and day
    // Values > 99 are definitely years (e.g., 1999)
    // Values in 32-99 range could be years (YY format) or impossible as days, so treat as years
    // Values <= 31 could be days or 2-digit years, use position to disambiguate
    let (year, day) = if numeric_parts[0] > 31 {
        // First number is year (either > 99 or 32-99 which is invalid as day)
        (normalize_year(numeric_parts[0]), numeric_parts[1] as u32)
    } else if numeric_parts[1] > 31 {
        // Second number is year
        (normalize_year(numeric_parts[1]), numeric_parts[0] as u32)
    } else if month_pos == 0 {
        // Month first, so likely: Month DD YY
        (normalize_year(numeric_parts[1]), numeric_parts[0] as u32)
    } else if month_pos == parts.len() - 1 {
        // Month last, so likely: DD YY Month
        (normalize_year(numeric_parts[1]), numeric_parts[0] as u32)
    } else {
        // Month in middle, so likely: DD Month YY
        (normalize_year(numeric_parts[1]), numeric_parts[0] as u32)
    };

    if day == 0 || day > days_in_month(year, month) {
        return None;
    }

    Some(DateValue { year, month, day })
}

fn try_parse_numeric_date(text: &str) -> Option<DateValue> {
    // Try different separators
    let parts: Vec<&str> = if text.contains('/') {
        text.split('/').collect()
    } else if text.contains('-') {
        text.split('-').collect()
    } else if text.contains(' ') {
        text.split_whitespace().collect()
    } else {
        return None;
    };

    if parts.len() != 3 {
        return None;
    }

    let n1: i32 = parts[0].parse().ok()?;
    let n2: u32 = parts[1].parse().ok()?;
    let n3: i32 = parts[2].parse().ok()?;

    // Determine format based on values
    // If first or last > 31, it's likely year
    let (year, month, day) = if n1 > 31 || (n1 > 12 && n3 <= 12) {
        // YYYY-MM-DD or YYYY-DD-MM
        let y = normalize_year(n1);
        if n2 > 12 {
            (y, n3 as u32, n2)
        } else {
            (y, n2, n3 as u32)
        }
    } else if n3 > 31 || n3 > 99 {
        // MM-DD-YYYY or DD-MM-YYYY (ambiguous, use YMD order by default)
        let y = normalize_year(n3);
        // Default to YMD interpretation: n1=month, n2=day
        (y, n1 as u32, n2)
    } else {
        // Two-digit year: YY-MM-DD format (default)
        let y = normalize_year(n1);
        (y, n2, n3 as u32)
    };

    if month == 0 || month > 12 || day == 0 {
        return None;
    }

    let max_day = days_in_month(year, month);
    if day > max_day {
        return None;
    }

    Some(DateValue { year, month, day })
}

fn normalize_year(year: i32) -> i32 {
    if (0..70).contains(&year) {
        2000 + year
    } else if (70..100).contains(&year) {
        1900 + year
    } else {
        year
    }
}

fn parse_month_name(text: &str) -> Option<u32> {
    let lower = text.to_ascii_lowercase();
    match lower.as_str() {
        "jan" | "january" => Some(1),
        "feb" | "february" => Some(2),
        "mar" | "march" => Some(3),
        "apr" | "april" => Some(4),
        "may" => Some(5),
        "jun" | "june" => Some(6),
        "jul" | "july" => Some(7),
        "aug" | "august" => Some(8),
        "sep" | "september" | "sept" => Some(9),
        "oct" | "october" => Some(10),
        "nov" | "november" => Some(11),
        "dec" | "december" => Some(12),
        _ => None,
    }
}

pub(crate) fn parse_time_text(text: &str) -> Result<(u32, u32, u32, u32), EngineError> {
    let mut cleaned = text.trim().to_string();

    // Check for AM/PM
    let is_pm = cleaned.to_ascii_uppercase().ends_with(" PM");
    let is_am = cleaned.to_ascii_uppercase().ends_with(" AM");

    if is_pm || is_am {
        cleaned = cleaned[..cleaned.len() - 3].trim().to_string();
    }

    // Remove trailing 'Z' (UTC indicator)
    if cleaned.ends_with('Z') {
        cleaned = cleaned[..cleaned.len() - 1].to_string();
    }

    // Look for timezone offset (+/-HH:MM) or timezone name
    if let Some(sign_pos) = cleaned
        .char_indices()
        .find_map(|(idx, ch)| ((ch == '+' || ch == '-') && idx > 1).then_some(idx))
    {
        cleaned = cleaned[..sign_pos].trim().to_string();
    } else {
        // Check for timezone names (PST, EDT, etc.) - remove them
        let parts: Vec<&str> = cleaned.split_whitespace().collect();
        if parts.len() >= 2 {
            // Check if last part looks like a timezone (3 chars, all letters)
            let last = parts[parts.len() - 1];
            if last.len() == 3 && last.chars().all(|c| c.is_ascii_alphabetic()) {
                // Might be a timezone, check if it's a known one
                let tz_upper = last.to_ascii_uppercase();
                if matches!(
                    tz_upper.as_str(),
                    "PST" | "PDT" | "MST" | "MDT" | "CST" | "CDT" | "EST" | "EDT" | "UTC" | "GMT"
                ) {
                    // Join all parts except the last one
                    cleaned = parts[..parts.len() - 1].join(" ");
                }
            }
        }
    }

    let time_parts = cleaned.split(':').collect::<Vec<_>>();
    if time_parts.len() < 2 || time_parts.len() > 3 {
        return Err(EngineError {
            message: format!("invalid time format: \"{text}\""),
        });
    }

    let mut hour = time_parts[0].parse::<u32>().map_err(|_| EngineError {
        message: "invalid time hour".to_string(),
    })?;

    let minute = time_parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid time minute".to_string(),
    })?;

    let (second, microsecond) = if time_parts.len() == 3 {
        parse_seconds_with_fraction(time_parts[2])?
    } else {
        (0, 0)
    };

    // Apply AM/PM adjustment
    if is_pm && hour < 12 {
        hour += 12;
    } else if is_am && hour == 12 {
        hour = 0;
    }

    // Handle special cases for rounding
    let (second, microsecond) = if microsecond >= 1_000_000 {
        (second + 1, 0)
    } else {
        (second, microsecond)
    };

    // If seconds = 60 (leap second), round to next minute
    let (minute, second) = if second >= 60 {
        (minute + 1, 0)
    } else {
        (minute, second)
    };

    // If minutes overflow
    let (hour, minute) = if minute >= 60 {
        (hour + 1, 0)
    } else {
        (hour, minute)
    };

    // Validate bounds
    // Special case: 24:00:00 is allowed (represents midnight)
    if hour == 24 && minute == 0 && second == 0 && microsecond == 0 {
        return Ok((24, 0, 0, 0));
    }

    if hour > 23 {
        return Err(EngineError {
            message: format!("date/time field value out of range: \"{text}\""),
        });
    }

    if minute > 59 {
        return Err(EngineError {
            message: format!("date/time field value out of range: \"{text}\""),
        });
    }

    // PostgreSQL allows second values slightly over 60 that round down
    if second > 59 {
        return Err(EngineError {
            message: format!("date/time field value out of range: \"{text}\""),
        });
    }

    Ok((hour, minute, second, microsecond))
}

fn parse_seconds_with_fraction(text: &str) -> Result<(u32, u32), EngineError> {
    if let Some(dot_pos) = text.find('.') {
        let sec_part = &text[..dot_pos];
        let frac_part = &text[dot_pos + 1..];

        let second = sec_part.parse::<u32>().map_err(|_| EngineError {
            message: "invalid time second".to_string(),
        })?;

        // Parse fractional seconds and convert to microseconds
        // Pad or truncate to 6 digits
        let mut frac_str = frac_part.to_string();

        // Handle rounding if more than 6 decimal places
        if frac_str.len() > 6 {
            // Round to 6 decimal places
            let extra_digit = frac_str
                .chars()
                .nth(6)
                .and_then(|c| c.to_digit(10))
                .unwrap_or(0);
            frac_str.truncate(6);

            let mut microsecond = frac_str.parse::<u32>().unwrap_or(0);

            // Round up if the 7th digit is >= 5
            if extra_digit >= 5 {
                microsecond += 1;
            }

            Ok((second, microsecond))
        } else {
            // Pad with zeros to make it 6 digits
            while frac_str.len() < 6 {
                frac_str.push('0');
            }

            let microsecond = frac_str.parse::<u32>().map_err(|_| EngineError {
                message: "invalid time fractional seconds".to_string(),
            })?;

            Ok((second, microsecond))
        }
    } else {
        let second = text.parse::<u32>().map_err(|_| EngineError {
            message: "invalid time second".to_string(),
        })?;
        Ok((second, 0))
    }
}

fn parse_datetime_with_format(text: &str, format: &str) -> Result<DateTimeValue, EngineError> {
    let (date, hour, minute, second, microsecond) = parse_datetime_parts_with_format(text, format)?;
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
        microsecond,
    })
}

fn parse_date_with_format(text: &str, format: &str) -> Result<DateValue, EngineError> {
    let (date, _hour, _minute, _second, _microsecond) =
        parse_datetime_parts_with_format(text, format)?;
    Ok(date)
}

fn parse_datetime_parts_with_format(
    text: &str,
    format: &str,
) -> Result<(DateValue, u32, u32, u32, u32), EngineError> {
    let input = text.trim();
    let fmt = format.trim().to_ascii_uppercase();
    let mut in_idx = 0usize;
    let mut fmt_idx = 0usize;
    let bytes = input.as_bytes();

    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    let mut day: Option<u32> = None;
    let mut hour: u32 = 0;
    let mut minute: u32 = 0;
    let mut second: u32 = 0;
    let microsecond: u32 = 0;

    while fmt_idx < fmt.len() {
        let remaining = &fmt[fmt_idx..];
        if remaining.starts_with("YYYY") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 4, "year")? as i32;
            year = Some(value);
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MM") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "month")? as u32;
            month = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("DD") {
            let value = parse_fixed_digits(bytes, &mut in_idx, 2, "day")? as u32;
            day = Some(value);
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("HH24") {
            hour = parse_fixed_digits(bytes, &mut in_idx, 2, "hour")? as u32;
            fmt_idx += 4;
            continue;
        }
        if remaining.starts_with("MI") {
            minute = parse_fixed_digits(bytes, &mut in_idx, 2, "minute")? as u32;
            fmt_idx += 2;
            continue;
        }
        if remaining.starts_with("SS") {
            second = parse_fixed_digits(bytes, &mut in_idx, 2, "second")? as u32;
            fmt_idx += 2;
            continue;
        }

        let ch = fmt[fmt_idx..].chars().next().unwrap();
        let ch_len = ch.len_utf8();
        if in_idx >= bytes.len() || bytes[in_idx] != ch as u8 {
            return Err(EngineError {
                message: "to_timestamp/to_date format mismatch".to_string(),
            });
        }
        in_idx += 1;
        fmt_idx += ch_len;
    }

    if in_idx != bytes.len() {
        return Err(EngineError {
            message: "to_timestamp/to_date format mismatch".to_string(),
        });
    }

    let date = date_from_parts(
        year.ok_or_else(|| EngineError {
            message: "missing year".to_string(),
        })?,
        month.ok_or_else(|| EngineError {
            message: "missing month".to_string(),
        })?,
        day.ok_or_else(|| EngineError {
            message: "missing day".to_string(),
        })?,
    )?;

    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid time component".to_string(),
        });
    }

    Ok((date, hour, minute, second, microsecond))
}

fn parse_fixed_digits(
    bytes: &[u8],
    idx: &mut usize,
    count: usize,
    label: &str,
) -> Result<i64, EngineError> {
    if *idx + count > bytes.len() {
        return Err(EngineError {
            message: format!("invalid {label} in format input"),
        });
    }
    let slice = &bytes[*idx..*idx + count];
    let text = std::str::from_utf8(slice).unwrap_or("");
    let value = text.parse::<i64>().map_err(|_| EngineError {
        message: format!("invalid {label} in format input"),
    })?;
    *idx += count;
    Ok(value)
}

fn date_from_parts(year: i32, month: u32, day: u32) -> Result<DateValue, EngineError> {
    if month == 0 || month > 12 {
        return Err(EngineError {
            message: "invalid date month".to_string(),
        });
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(EngineError {
            message: "invalid date day".to_string(),
        });
    }
    Ok(DateValue { year, month, day })
}

pub(crate) fn format_date(date: DateValue) -> String {
    if date.year == 294276 && date.month == 12 && date.day == 31 {
        return "infinity".to_string();
    }
    if date.year == -4713 && date.month == 1 && date.day == 1 {
        return "-infinity".to_string();
    }
    format!("{:04}-{:02}-{:02}", date.year, date.month, date.day)
}

pub(crate) fn format_timestamp(datetime: DateTimeValue) -> String {
    if datetime.date.year == 294276
        && datetime.date.month == 12
        && datetime.date.day == 31
        && datetime.hour == 23
        && datetime.minute == 59
        && datetime.second == 59
        && datetime.microsecond == 999999
    {
        return "infinity".to_string();
    }
    if datetime.date.year == -4713
        && datetime.date.month == 1
        && datetime.date.day == 1
        && datetime.hour == 0
        && datetime.minute == 0
        && datetime.second == 0
        && datetime.microsecond == 0
    {
        return "-infinity".to_string();
    }
    if datetime.microsecond == 0 {
        format!(
            "{} {:02}:{:02}:{:02}",
            format_date(datetime.date),
            datetime.hour,
            datetime.minute,
            datetime.second
        )
    } else {
        // Remove trailing zeros from microseconds
        let mut frac = datetime.microsecond;
        let mut precision = 6;
        while precision > 0 && frac.is_multiple_of(10) {
            frac /= 10;
            precision -= 1;
        }
        format!(
            "{} {:02}:{:02}:{:02}.{:0width$}",
            format_date(datetime.date),
            datetime.hour,
            datetime.minute,
            datetime.second,
            frac,
            width = precision
        )
    }
}

#[allow(dead_code)]
pub(crate) fn format_time(hour: u32, minute: u32, second: u32, microsecond: u32) -> String {
    if microsecond == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        // Remove trailing zeros from microseconds
        let mut frac = microsecond;
        let mut precision = 6;
        while precision > 0 && frac.is_multiple_of(10) {
            frac /= 10;
            precision -= 1;
        }
        format!("{hour:02}:{minute:02}:{second:02}.{frac:0precision$}")
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn day_of_year(date: DateValue) -> u32 {
    let mut total = 0u32;
    for month in 1..date.month {
        total += days_in_month(date.year, month);
    }
    total + date.day
}

fn day_of_week(date: DateValue) -> u32 {
    let days = days_from_civil(date.year, date.month, date.day);
    (days + 4).rem_euclid(7) as u32
}

fn add_days(date: DateValue, days: i64) -> DateValue {
    let day_number = days_from_civil(date.year, date.month, date.day);
    civil_from_days(day_number + days)
}

pub(crate) fn datetime_to_epoch_seconds(datetime: DateTimeValue) -> i64 {
    let days = days_from_civil(datetime.date.year, datetime.date.month, datetime.date.day);
    days * 86_400
        + datetime.hour as i64 * 3_600
        + datetime.minute as i64 * 60
        + datetime.second as i64
}

pub(crate) fn datetime_from_epoch_seconds(seconds: i64) -> DateTimeValue {
    let day = seconds.div_euclid(86_400);
    let sec_of_day = seconds.rem_euclid(86_400);
    let date = civil_from_days(day);
    DateTimeValue {
        date,
        hour: (sec_of_day / 3_600) as u32,
        minute: ((sec_of_day % 3_600) / 60) as u32,
        second: (sec_of_day % 60) as u32,
        microsecond: 0,
    }
}

fn interval_from_seconds(seconds: i64) -> IntervalValue {
    let days = seconds.div_euclid(86_400);
    let rem = seconds.rem_euclid(86_400);
    IntervalValue {
        months: 0,
        days,
        seconds: rem,
    }
}

/// Parse an interval from its canonical display format (`{months} mons {days} days HH:MM:SS`)
/// or from a human-readable literal (`'1 year 2 months 3 days 4 hours'`).
///
/// Modelled after PostgreSQL's `DecodeInterval()` in `datetime.c`.
pub(crate) fn parse_interval_text(text: &str) -> Result<IntervalValue, EngineError> {
    let mut trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix('@') {
        trimmed = rest.trim_start();
    }
    if trimmed.is_empty() {
        return Err(EngineError {
            message: "invalid interval value".to_string(),
        });
    }

    // Try canonical format first: `{months} mons {days} days [-]HH:MM:SS`
    if let Ok(iv) = try_parse_canonical_interval(trimmed) {
        return Ok(iv);
    }

    // Try PostgreSQL literal format: value unit pairs like '1 year 2 months 3 days'
    parse_interval_literal(trimmed)
}

/// Try parsing the canonical `{months} mons {days} days HH:MM:SS` format.
fn try_parse_canonical_interval(text: &str) -> Result<IntervalValue, EngineError> {
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts.len() < 5 {
        return Err(EngineError {
            message: "not canonical format".to_string(),
        });
    }
    // Expect: "{months} mons {days} days {time}"
    if parts.get(1).map(|s| s.trim_end_matches(',')) != Some("mons")
        && parts.get(1).map(|s| s.trim_end_matches(',')) != Some("mon")
    {
        return Err(EngineError {
            message: "not canonical format".to_string(),
        });
    }
    let months = parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval months".to_string(),
    })?;
    let days = parts[2].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval days".to_string(),
    })?;
    let time = parts[4];
    let (negative, time_str) = if let Some(stripped) = time.strip_prefix('-') {
        (true, stripped)
    } else {
        (false, time)
    };
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() != 3 {
        return Err(EngineError {
            message: "invalid interval time".to_string(),
        });
    }
    let hour = time_parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval hour".to_string(),
    })?;
    let minute = time_parts[1].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval minute".to_string(),
    })?;
    let second = time_parts[2].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval second".to_string(),
    })?;
    let mut total_seconds = hour
        .saturating_mul(3_600)
        .saturating_add(minute.saturating_mul(60))
        .saturating_add(second);
    if negative {
        total_seconds = total_seconds.saturating_neg();
    }
    Ok(IntervalValue {
        months,
        days,
        seconds: total_seconds,
    })
}

/// Parse a PostgreSQL interval literal like `'1 year 2 months 3 days 4 hours 5 minutes 6 seconds'`.
///
/// Also handles ISO 8601 duration format (`P1Y2M3DT4H5M6S`) and PostgreSQL's
/// shorthand for plain numbers (`'90'` treated as seconds per PG default).
fn parse_interval_literal(text: &str) -> Result<IntervalValue, EngineError> {
    let mut trimmed = text.trim();
    if let Some(rest) = trimmed.strip_prefix('@') {
        trimmed = rest.trim_start();
    }

    // Handle ISO 8601 duration: P1Y2M3DT4H5M6S
    if trimmed.starts_with('P') || trimmed.starts_with('p') {
        return parse_iso_8601_interval(trimmed);
    }

    // Handle PostgreSQL SQL-standard format: 'Y-M' or 'H:M:S'
    if !trimmed.contains(' ') {
        // Pure number → seconds
        if let Ok(secs) = trimmed.parse::<i64>() {
            return Ok(IntervalValue {
                months: 0,
                days: 0,
                seconds: secs,
            });
        }
        // HH:MM:SS time format
        if trimmed.contains(':') {
            return parse_interval_time_only(trimmed);
        }
    }

    // value-unit pair parsing: '1 year 2 months 3 days ...'
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut negate = false;

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i];
        let normalized_token = token.to_ascii_lowercase();
        let normalized_token = normalized_token.trim_end_matches(',');

        if normalized_token == "ago" {
            negate = true;
            i += 1;
            continue;
        }

        // Check if this is a time component (HH:MM:SS)
        if token.contains(':') {
            let iv = parse_interval_time_only(token)?;
            seconds += iv.seconds;
            i += 1;
            continue;
        }

        // Try to parse as a number
        let value = token.parse::<f64>().map_err(|_| EngineError {
            message: format!("invalid interval component: \"{token}\""),
        })?;

        // Look ahead for unit
        let unit = if i + 1 < tokens.len() {
            let next = tokens[i + 1].to_ascii_lowercase();
            let next = next.trim_end_matches(',');
            match next {
                "year" | "years" | "yr" | "yrs" | "y" => {
                    i += 2;
                    "year"
                }
                "decade" | "decades" => {
                    i += 2;
                    "decade"
                }
                "century" | "centuries" => {
                    i += 2;
                    "century"
                }
                "millennium" | "millennia" | "millenniums" => {
                    i += 2;
                    "millennium"
                }
                "month" | "months" | "mon" | "mons" => {
                    i += 2;
                    "month"
                }
                "week" | "weeks" | "w" => {
                    i += 2;
                    "week"
                }
                "day" | "days" | "d" => {
                    i += 2;
                    "day"
                }
                "hour" | "hours" | "hr" | "hrs" | "h" => {
                    i += 2;
                    "hour"
                }
                "minute" | "minutes" | "min" | "mins" | "m" => {
                    i += 2;
                    "minute"
                }
                "second" | "seconds" | "sec" | "secs" | "s" => {
                    i += 2;
                    "second"
                }
                "millisecond" | "milliseconds" | "ms" | "msec" => {
                    i += 2;
                    "millisecond"
                }
                "microsecond" | "microseconds" | "us" | "usec" => {
                    i += 2;
                    "microsecond"
                }
                _ => {
                    // No recognized unit — treat as seconds (PostgreSQL default)
                    i += 1;
                    "second"
                }
            }
        } else {
            // Last token with no unit — treat as seconds
            i += 1;
            "second"
        };

        match unit {
            "year" => months = months.saturating_add((value * 12.0) as i64),
            "decade" => months = months.saturating_add((value * 120.0) as i64),
            "century" => months = months.saturating_add((value * 1_200.0) as i64),
            "millennium" => months = months.saturating_add((value * 12_000.0) as i64),
            "month" => months = months.saturating_add(value as i64),
            "week" => days = days.saturating_add((value * 7.0) as i64),
            "day" => days = days.saturating_add(value as i64),
            "hour" => seconds = seconds.saturating_add((value * 3_600.0) as i64),
            "minute" => seconds = seconds.saturating_add((value * 60.0) as i64),
            "second" => seconds = seconds.saturating_add(value as i64),
            "millisecond" => seconds = seconds.saturating_add((value / 1000.0) as i64),
            "microsecond" => seconds = seconds.saturating_add((value / 1_000_000.0) as i64),
            _ => {}
        }
    }

    if negate {
        months = months.saturating_neg();
        days = days.saturating_neg();
        seconds = seconds.saturating_neg();
    }

    Ok(IntervalValue {
        months,
        days,
        seconds,
    })
}

fn parse_interval_time_only(text: &str) -> Result<IntervalValue, EngineError> {
    let (negative, time_str) = if let Some(stripped) = text.strip_prefix('-') {
        (true, stripped)
    } else {
        (false, text)
    };
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(EngineError {
            message: format!("invalid interval time: \"{text}\""),
        });
    }
    let hour = parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval hour".to_string(),
    })?;
    let minute = parts[1].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval minute".to_string(),
    })?;
    let second = if parts.len() == 3 {
        parts[2].parse::<i64>().map_err(|_| EngineError {
            message: "invalid interval second".to_string(),
        })?
    } else {
        0
    };
    let mut total = hour
        .saturating_mul(3_600)
        .saturating_add(minute.saturating_mul(60))
        .saturating_add(second);
    if negative {
        total = total.saturating_neg();
    }
    Ok(IntervalValue {
        months: 0,
        days: 0,
        seconds: total,
    })
}

fn parse_iso_8601_interval(text: &str) -> Result<IntervalValue, EngineError> {
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut in_time = false;
    let mut num_start = None;
    let chars: Vec<char> = text.chars().collect();

    for i in 1..chars.len() {
        let c = chars[i];
        if c == 'T' || c == 't' {
            in_time = true;
            continue;
        }
        if c.is_ascii_digit() || c == '.' || c == '-' {
            if num_start.is_none() {
                num_start = Some(i);
            }
            continue;
        }
        if let Some(start) = num_start {
            let num_str: String = chars[start..i].iter().collect();
            let value = num_str.parse::<f64>().map_err(|_| EngineError {
                message: format!("invalid ISO 8601 interval number: \"{num_str}\""),
            })?;
            match (in_time, c) {
                (false, 'Y' | 'y') => months = months.saturating_add((value * 12.0) as i64),
                (false, 'M' | 'm') => months = months.saturating_add(value as i64),
                (false, 'W' | 'w') => days = days.saturating_add((value * 7.0) as i64),
                (false, 'D' | 'd') => days = days.saturating_add(value as i64),
                (true, 'H' | 'h') => {
                    seconds = seconds.saturating_add((value * 3_600.0) as i64);
                }
                (true, 'M' | 'm') => seconds = seconds.saturating_add((value * 60.0) as i64),
                (true, 'S' | 's') => seconds = seconds.saturating_add(value as i64),
                _ => {
                    return Err(EngineError {
                        message: format!("invalid ISO 8601 interval designator: '{c}'"),
                    });
                }
            }
            num_start = None;
        }
    }

    Ok(IntervalValue {
        months,
        days,
        seconds,
    })
}

/// Try to parse a ScalarValue as an interval. Returns `Some(IntervalValue)` if the
/// value looks like an interval string (canonical or literal format).
pub(crate) fn parse_interval_operand(value: &ScalarValue) -> Option<IntervalValue> {
    let ScalarValue::Text(text) = value else {
        return None;
    };
    parse_interval_text(text).ok()
}

/// Check whether a text value looks like it could be an interval (as opposed to a date/timestamp).
/// This is a heuristic used to disambiguate `Text` values in arithmetic operators.
pub(crate) fn is_interval_text(text: &str) -> bool {
    let mut trimmed = text.trim().to_ascii_lowercase();
    if let Some(rest) = trimmed.strip_prefix('@') {
        trimmed = rest.trim_start().to_string();
    }
    // Canonical format: starts with a number followed by "mons" or "mon"
    if trimmed.contains(" mons ") || trimmed.contains(" mon ") {
        return true;
    }
    // Keyword-style: contains interval unit keywords
    let units = [
        "year",
        "years",
        "decade",
        "decades",
        "century",
        "centuries",
        "millennium",
        "millennia",
        "millenniums",
        "month",
        "months",
        "day",
        "days",
        "hour",
        "hours",
        "minute",
        "minutes",
        "second",
        "seconds",
        "millisecond",
        "milliseconds",
        "msec",
        "ms",
        "microsecond",
        "microseconds",
        "usec",
        "us",
        "week",
        "weeks",
        "ago",
    ];
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    for part in &parts {
        let p = part.trim_end_matches(',');
        if units.contains(&p) {
            return true;
        }
    }
    // ISO 8601 duration
    if trimmed.starts_with('p') && trimmed.len() > 1 {
        return true;
    }
    false
}

/// Add an interval to a date/timestamp, following PostgreSQL semantics.
///
/// PostgreSQL's `timestamp_pl_interval` applies months first (adjusting for
/// end-of-month), then days, then seconds. This function mirrors that logic.
pub(crate) fn temporal_add_interval(
    temporal: TemporalOperand,
    interval: IntervalValue,
) -> ScalarValue {
    let mut dt = temporal.datetime;

    // Step 1: Add months (following Postgres's timestamp_pl_interval logic)
    if interval.months != 0 {
        let total_months = dt.date.year as i64 * 12 + dt.date.month as i64 - 1 + interval.months;
        let new_year = total_months.div_euclid(12) as i32;
        let new_month = (total_months.rem_euclid(12) + 1) as u32;
        // Clamp day to end of month (Postgres semantics)
        let max_day = days_in_month(new_year, new_month);
        dt.date.year = new_year;
        dt.date.month = new_month;
        if dt.date.day > max_day {
            dt.date.day = max_day;
        }
    }

    // Step 2: Add days
    if interval.days != 0 {
        dt.date = add_days(dt.date, interval.days);
    }

    // Step 3: Add seconds (including time component)
    if interval.seconds != 0 {
        let epoch = datetime_to_epoch_seconds(dt).saturating_add(interval.seconds);
        dt = datetime_from_epoch_seconds(epoch);
    }

    if temporal.date_only && interval.seconds == 0 {
        ScalarValue::Text(format_date(dt.date))
    } else {
        ScalarValue::Text(format_timestamp(dt))
    }
}

/// Negate an interval value (used for subtraction).
pub(crate) fn interval_negate(iv: IntervalValue) -> IntervalValue {
    IntervalValue {
        months: iv.months.saturating_neg(),
        days: iv.days.saturating_neg(),
        seconds: iv.seconds.saturating_neg(),
    }
}

/// Add two intervals together.
pub(crate) fn interval_add(a: IntervalValue, b: IntervalValue) -> IntervalValue {
    IntervalValue {
        months: a.months.saturating_add(b.months),
        days: a.days.saturating_add(b.days),
        seconds: a.seconds.saturating_add(b.seconds),
    }
}

/// Multiply an interval by a scalar.
pub(crate) fn interval_mul(iv: IntervalValue, factor: f64) -> IntervalValue {
    IntervalValue {
        months: (iv.months as f64 * factor) as i64,
        days: (iv.days as f64 * factor) as i64,
        seconds: (iv.seconds as f64 * factor) as i64,
    }
}

/// Format an interval as its canonical text representation for CAST/display.
/// Format an interval value to its canonical display string.
pub(crate) fn format_interval_value(iv: IntervalValue) -> String {
    format_interval(iv)
}

/// Format an interval as its canonical text representation for CAST/display.
pub(crate) fn eval_interval_cast(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let interval = match value {
        ScalarValue::Text(text) => parse_interval_text(text)?,
        ScalarValue::Int(secs) => IntervalValue {
            months: 0,
            days: 0,
            seconds: *secs,
        },
        ScalarValue::Float(secs) => IntervalValue {
            months: 0,
            days: 0,
            seconds: *secs as i64,
        },
        _ => {
            return Err(EngineError {
                message: "cannot cast value to interval".to_string(),
            });
        }
    };
    Ok(ScalarValue::Text(format_interval(interval)))
}

fn format_interval(interval: IntervalValue) -> String {
    let sign = if interval.seconds < 0 { "-" } else { "" };
    let seconds = interval.seconds.unsigned_abs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!(
        "{} mons {} days {}{:02}:{:02}:{:02}",
        interval.months, interval.days, sign, hours, minutes, secs
    )
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> DateValue {
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
    DateValue {
        year: year as i32,
        month: month as u32,
        day: day as u32,
    }
}

/// Make a time value from hour, minute, and second.
/// Matches PostgreSQL's make_time(hour, min, sec) function.
pub(crate) fn eval_make_time(
    hour: i64,
    minute: i64,
    second: f64,
) -> Result<ScalarValue, EngineError> {
    // Validate inputs like PostgreSQL does
    if !(0..=23).contains(&hour) {
        return Err(EngineError {
            message: format!("hour {hour} is out of range 0..23"),
        });
    }
    if !(0..=59).contains(&minute) {
        return Err(EngineError {
            message: format!("minute {minute} is out of range 0..59"),
        });
    }
    if !(0.0..60.0).contains(&second) {
        return Err(EngineError {
            message: format!("second {second} is out of range 0..<60"),
        });
    }

    let sec = second.trunc() as u32;
    let frac = ((second - second.trunc()) * 1_000_000.0).round() as u32;

    if frac == 0 {
        Ok(ScalarValue::Text(format!("{hour:02}:{minute:02}:{sec:02}")))
    } else {
        Ok(ScalarValue::Text(format!(
            "{hour:02}:{minute:02}:{sec:02}.{frac:06}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso_date() {
        let date = parse_date_text("1999-01-08").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);
    }

    #[test]
    fn test_parse_month_name_dates() {
        let date = parse_date_text("January 8, 1999").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);

        let date = parse_date_text("99-Jan-08").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);

        let date = parse_date_text("Jan-08-1999").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);
    }

    #[test]
    fn test_parse_compact_dates() {
        let date = parse_date_text("19990108").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);

        let date = parse_date_text("990108").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);
    }

    #[test]
    fn test_parse_year_day_format() {
        let date = parse_date_text("1999.008").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);
    }

    #[test]
    fn test_parse_julian_date() {
        let date = parse_date_text("J2451187").unwrap();
        assert_eq!(date.year, 1999);
        assert_eq!(date.month, 1);
        assert_eq!(date.day, 8);
    }

    #[test]
    fn test_parse_bc_date() {
        let date = parse_date_text("2040-04-10 BC").unwrap();
        assert_eq!(date.year, -2039);
        assert_eq!(date.month, 4);
        assert_eq!(date.day, 10);
    }

    #[test]
    fn test_parse_time_with_microseconds() {
        let (h, m, s, us) = parse_time_text("23:59:59.999999").unwrap();
        assert_eq!(h, 23);
        assert_eq!(m, 59);
        assert_eq!(s, 59);
        assert_eq!(us, 999999);
    }

    #[test]
    fn test_parse_time_rounding() {
        // 7 decimal places should round up
        let (h, m, s, us) = parse_time_text("23:59:59.9999999").unwrap();
        assert_eq!(h, 24);
        assert_eq!(m, 0);
        assert_eq!(s, 0);
        assert_eq!(us, 0);
    }

    #[test]
    fn test_parse_leap_second() {
        let (h, m, s, us) = parse_time_text("23:59:60").unwrap();
        assert_eq!(h, 24);
        assert_eq!(m, 0);
        assert_eq!(s, 0);
        assert_eq!(us, 0);
    }

    #[test]
    fn test_parse_time_24_00_00() {
        let (h, m, s, us) = parse_time_text("24:00:00").unwrap();
        assert_eq!(h, 24);
        assert_eq!(m, 0);
        assert_eq!(s, 0);
        assert_eq!(us, 0);
    }

    #[test]
    fn test_parse_time_am_pm() {
        let (h, m, s, _us) = parse_time_text("11:59:59.99 PM").unwrap();
        assert_eq!(h, 23);
        assert_eq!(m, 59);
        assert_eq!(s, 59);
    }

    #[test]
    fn test_parse_time_with_timezone() {
        let (h, m, s, _us) = parse_time_text("02:03 PST").unwrap();
        assert_eq!(h, 2);
        assert_eq!(m, 3);
        assert_eq!(s, 0);

        let (h, m, s, _us) = parse_time_text("11:59 EDT").unwrap();
        assert_eq!(h, 11);
        assert_eq!(m, 59);
        assert_eq!(s, 0);
    }

    #[test]
    fn test_parse_time_invalid_24_00_01() {
        assert!(parse_time_text("24:00:00.01").is_err());
        assert!(parse_time_text("24:01:00").is_err());
        assert!(parse_time_text("25:00:00").is_err());
    }

    #[test]
    fn test_format_time_with_microseconds() {
        let time = format_time(23, 59, 59, 990000);
        assert_eq!(time, "23:59:59.99");

        let time = format_time(23, 59, 59, 999999);
        assert_eq!(time, "23:59:59.999999");

        let time = format_time(0, 0, 0, 0);
        assert_eq!(time, "00:00:00");
    }

    #[test]
    fn test_extract_microsecond() {
        let dt = DateTimeValue {
            date: DateValue {
                year: 2020,
                month: 5,
                day: 26,
            },
            hour: 13,
            minute: 30,
            second: 25,
            microsecond: 575401,
        };

        // Verify the timestamp formats correctly with microseconds
        let ts = format_timestamp(dt);
        assert!(ts.contains(".575401"));
    }

    #[test]
    fn test_special_datetime_values() {
        let dt = parse_datetime_text("epoch").unwrap();
        assert_eq!(dt.date.year, 1970);
        assert_eq!(dt.date.month, 1);
        assert_eq!(dt.date.day, 1);
        assert_eq!(dt.hour, 0);
        assert_eq!(dt.minute, 0);
        assert_eq!(dt.second, 0);
    }

    #[test]
    fn test_parse_positive_infinity_variants() {
        let infinity = parse_datetime_text("infinity").unwrap();
        let plus_infinity = parse_datetime_text("+infinity").unwrap();
        let plus_inf = parse_datetime_text("+inf").unwrap();
        assert_eq!(infinity, plus_infinity);
        assert_eq!(infinity, plus_inf);
    }

    #[test]
    fn test_parse_negative_infinity_variants() {
        let neg_infinity = parse_datetime_text("-infinity").unwrap();
        let neg_inf = parse_datetime_text("-inf").unwrap();
        assert_eq!(neg_infinity, neg_inf);
    }

    #[test]
    fn test_format_infinity_datetime_values() {
        let pos = parse_datetime_text("+infinity").unwrap();
        let neg = parse_datetime_text("-infinity").unwrap();
        assert_eq!(format_timestamp(pos), "infinity");
        assert_eq!(format_timestamp(neg), "-infinity");
        assert_eq!(format_date(pos.date), "infinity");
        assert_eq!(format_date(neg.date), "-infinity");
    }

    #[test]
    fn test_format_date_iso() {
        let date = DateValue {
            year: 1957,
            month: 4,
            day: 9,
        };
        assert_eq!(format_date(date), "1957-04-09");
    }
}
