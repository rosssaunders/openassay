use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::{parse_f64_scalar, parse_i64_scalar};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalOperand {
    pub(crate) datetime: DateTimeValue,
    pub(crate) date_only: bool,
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
        "second" => ScalarValue::Int(datetime.second as i64),
        "dow" => ScalarValue::Int(day_of_week(datetime.date) as i64),
        "doy" => ScalarValue::Int(day_of_year(datetime.date) as i64),
        "epoch" => ScalarValue::Float(datetime_to_epoch_seconds(datetime) as f64),
        _ => {
            return Err(EngineError {
                message: format!("unsupported date/time field {}", field_name),
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
        }
        "month" => {
            datetime.date.day = 1;
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "day" => {
            datetime.hour = 0;
            datetime.minute = 0;
            datetime.second = 0;
        }
        "hour" => {
            datetime.minute = 0;
            datetime.second = 0;
        }
        "minute" => {
            datetime.second = 0;
        }
        "second" => {}
        _ => {
            return Err(EngineError {
                message: format!("unsupported date_trunc field {}", field_name),
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
struct IntervalValue {
    months: i64,
    days: i64,
    seconds: i64,
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
            !matches!(normalized.as_str(), "infinity" | "-infinity" | "nan")
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

    let (date_part, time_part) = if let Some(pos) = raw.find('T') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else if let Some(pos) = raw.find(' ') {
        (&raw[..pos], Some(&raw[pos + 1..]))
    } else {
        (raw, None)
    };

    let date = parse_date_text(date_part)?;
    let (hour, minute, second) = match time_part {
        None => (0, 0, 0),
        Some(time_raw) => parse_time_text(time_raw)?,
    };
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_text(text: &str) -> Result<DateValue, EngineError> {
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(EngineError {
            message: "invalid date value".to_string(),
        });
    }
    let year = parts[0].parse::<i32>().map_err(|_| EngineError {
        message: "invalid date year".to_string(),
    })?;
    let month = parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date month".to_string(),
    })?;
    let day = parts[2].parse::<u32>().map_err(|_| EngineError {
        message: "invalid date day".to_string(),
    })?;
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

fn parse_time_text(text: &str) -> Result<(u32, u32, u32), EngineError> {
    let mut cleaned = text.trim();
    if cleaned.ends_with('Z') {
        cleaned = &cleaned[..cleaned.len() - 1];
    }
    if let Some(sign_pos) = cleaned
        .char_indices()
        .find_map(|(idx, ch)| ((ch == '+' || ch == '-') && idx > 1).then_some(idx))
    {
        cleaned = &cleaned[..sign_pos];
    }
    let time_parts = cleaned.split(':').collect::<Vec<_>>();
    if time_parts.len() < 2 || time_parts.len() > 3 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    let hour = time_parts[0].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp hour".to_string(),
    })?;
    let minute = time_parts[1].parse::<u32>().map_err(|_| EngineError {
        message: "invalid timestamp minute".to_string(),
    })?;
    let second = if time_parts.len() == 3 {
        time_parts[2]
            .split('.')
            .next()
            .unwrap_or("")
            .parse::<u32>()
            .map_err(|_| EngineError {
                message: "invalid timestamp second".to_string(),
            })?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(EngineError {
            message: "invalid timestamp time component".to_string(),
        });
    }
    Ok((hour, minute, second))
}

fn parse_datetime_with_format(text: &str, format: &str) -> Result<DateTimeValue, EngineError> {
    let (date, hour, minute, second) = parse_datetime_parts_with_format(text, format)?;
    Ok(DateTimeValue {
        date,
        hour,
        minute,
        second,
    })
}

fn parse_date_with_format(text: &str, format: &str) -> Result<DateValue, EngineError> {
    let (date, _hour, _minute, _second) = parse_datetime_parts_with_format(text, format)?;
    Ok(date)
}

fn parse_datetime_parts_with_format(
    text: &str,
    format: &str,
) -> Result<(DateValue, u32, u32, u32), EngineError> {
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

    Ok((date, hour, minute, second))
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
    format!("{:04}-{:02}-{:02}", date.year, date.month, date.day)
}

pub(crate) fn format_timestamp(datetime: DateTimeValue) -> String {
    format!(
        "{} {:02}:{:02}:{:02}",
        format_date(datetime.date),
        datetime.hour,
        datetime.minute,
        datetime.second
    )
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

fn parse_interval_text(text: &str) -> Result<IntervalValue, EngineError> {
    let parts = text.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 5 {
        return Err(EngineError {
            message: "invalid interval value".to_string(),
        });
    }
    let months = parts[0].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval months".to_string(),
    })?;
    let days = parts[2].parse::<i64>().map_err(|_| EngineError {
        message: "invalid interval days".to_string(),
    })?;
    let time = parts[4];
    let time_parts = time.split(':').collect::<Vec<_>>();
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
    let total_seconds = hour * 3_600 + minute * 60 + second;
    Ok(IntervalValue {
        months,
        days,
        seconds: total_seconds,
    })
}

fn format_interval(interval: IntervalValue) -> String {
    let sign = if interval.seconds < 0 { "-" } else { "" };
    let seconds = interval.seconds.abs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!(
        "{} mons {} days {}{:02}:{:02}:{:02}",
        interval.months, interval.days, sign, hours, minutes, secs
    )
}

pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - if month <= 2 { 1 } else { 0 };
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
    let year = year + if month <= 2 { 1 } else { 0 };
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
            message: format!("hour {} is out of range 0..23", hour),
        });
    }
    if !(0..=59).contains(&minute) {
        return Err(EngineError {
            message: format!("minute {} is out of range 0..59", minute),
        });
    }
    if !(0.0..60.0).contains(&second) {
        return Err(EngineError {
            message: format!("second {} is out of range 0..<60", second),
        });
    }

    let sec = second.trunc() as u32;
    let frac = ((second - second.trunc()) * 1_000_000.0).round() as u32;

    if frac == 0 {
        Ok(ScalarValue::Text(format!(
            "{:02}:{:02}:{:02}",
            hour, minute, sec
        )))
    } else {
        Ok(ScalarValue::Text(format!(
            "{:02}:{:02}:{:02}.{:06}",
            hour, minute, sec, frac
        )))
    }
}
