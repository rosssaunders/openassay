use std::cmp::Ordering;

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::datetime::{
    datetime_to_epoch_seconds, days_from_civil, parse_temporal_operand,
};
use crate::utils::adt::math_functions::{parse_numeric_operand, NumericOperand};

pub(crate) fn array_value_matches(target: &ScalarValue, candidate: &ScalarValue) -> Result<bool, EngineError> {
    match (target, candidate) {
        (ScalarValue::Null, ScalarValue::Null) => Ok(true),
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => Ok(false),
        _ => Ok(compare_values_for_predicate(target, candidate)? == Ordering::Equal),
    }
}


pub(crate) fn build_regex(pattern: &str, flags: &str, fn_name: &str) -> Result<regex::Regex, EngineError> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'g' => {}
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() unsupported regex flag {}", flag),
                });
            }
        }
    }
    builder.build().map_err(|err| EngineError {
        message: format!("{fn_name}() invalid regex: {err}"),
    })
}

pub(crate) fn text_array_from_options(items: &[Option<String>]) -> String {
    let rendered = items
        .iter()
        .map(|item| item.clone().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>();
    format!("{{{}}}", rendered.join(","))
}

pub(crate) fn eval_regexp_match(text: &str, pattern: &str, flags: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, flags, "regexp_match")?;
    let Some(caps) = regex.captures(text) else {
        return Ok(ScalarValue::Null);
    };
    Ok(ScalarValue::Text(regex_captures_to_array(&caps)))
}

pub(crate) fn regex_captures_to_array(caps: &regex::Captures<'_>) -> String {
    let mut items = Vec::new();
    if caps.len() > 1 {
        for idx in 1..caps.len() {
            items.push(caps.get(idx).map(|m| m.as_str().to_string()));
        }
    } else {
        items.push(caps.get(0).map(|m| m.as_str().to_string()));
    }
    text_array_from_options(&items)
}

pub(crate) fn eval_regexp_matches_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 && args.len() != 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().take(2).any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() == 3 { args[2].render() } else { String::new() };
    let global = flags.contains('g');
    let regex = build_regex(&pattern, &flags, fn_name)?;
    let mut rows = Vec::new();
    if global {
        for caps in regex.captures_iter(&text) {
            rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
        }
    } else if let Some(caps) = regex.captures(&text) {
        rows.push(vec![ScalarValue::Text(regex_captures_to_array(&caps))]);
    }
    Ok((vec![fn_name.to_string()], rows))
}

pub(crate) fn eval_regexp_split_to_array(text: &str, pattern: &str) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, "", "regexp_split_to_array")?;
    let parts = regex
        .split(text)
        .map(|part| Some(part.to_string()))
        .collect::<Vec<_>>();
    Ok(ScalarValue::Text(text_array_from_options(&parts)))
}

pub(crate) fn eval_regexp_split_to_table_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 arguments"),
        });
    }
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let regex = build_regex(&pattern, "", fn_name)?;
    let rows = regex
        .split(&text)
        .map(|part| vec![ScalarValue::Text(part.to_string())])
        .collect();
    Ok((vec![fn_name.to_string()], rows))
}

pub(crate) fn rand_f64() -> f64 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (seed as f64) / (u32::MAX as f64)
}

pub(crate) fn eval_regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
    let global = flags.contains('g');
    let regex = build_regex(pattern, flags, "regexp_replace")?;
    let out = if global {
        regex.replace_all(source, replacement).to_string()
    } else {
        regex.replace(source, replacement).to_string()
    };
    Ok(ScalarValue::Text(out))
}

pub(crate) fn eval_extremum(args: &[ScalarValue], greatest: bool) -> Result<ScalarValue, EngineError> {
    let mut best: Option<ScalarValue> = None;
    for arg in args {
        if matches!(arg, ScalarValue::Null) {
            continue;
        }
        match &best {
            None => best = Some(arg.clone()),
            Some(current) => {
                let cmp = compare_values_for_predicate(arg, current)?;
                let should_take = if greatest {
                    cmp == Ordering::Greater
                } else {
                    cmp == Ordering::Less
                };
                if should_take {
                    best = Some(arg.clone());
                }
            }
        }
    }
    Ok(best.unwrap_or(ScalarValue::Null))
}


pub(crate) fn compare_values_for_predicate(
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Ordering, EngineError> {
    if let (Ok(left_num), Ok(right_num)) =
        (parse_numeric_operand(left), parse_numeric_operand(right))
    {
        let ord = match (left_num, right_num) {
            (NumericOperand::Int(a), NumericOperand::Int(b)) => a.cmp(&b),
            (NumericOperand::Int(a), NumericOperand::Float(b)) => {
                (a as f64).partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Int(b)) => {
                a.partial_cmp(&(b as f64)).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Float(a), NumericOperand::Float(b)) => {
                a.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
        };
        return Ok(ord);
    }

    if let (Some(left_bool), Some(right_bool)) = (try_parse_bool(left), try_parse_bool(right)) {
        return Ok(left_bool.cmp(&right_bool));
    }

    if let (Some(left_time), Some(right_time)) =
        (parse_temporal_operand(left), parse_temporal_operand(right))
    {
        if left_time.date_only && right_time.date_only {
            let left_days = days_from_civil(
                left_time.datetime.date.year,
                left_time.datetime.date.month,
                left_time.datetime.date.day,
            );
            let right_days = days_from_civil(
                right_time.datetime.date.year,
                right_time.datetime.date.month,
                right_time.datetime.date.day,
            );
            return Ok(left_days.cmp(&right_days));
        }
        let left_epoch = datetime_to_epoch_seconds(left_time.datetime);
        let right_epoch = datetime_to_epoch_seconds(right_time.datetime);
        return Ok(left_epoch.cmp(&right_epoch));
    }

    match (left, right) {
        (ScalarValue::Text(a), ScalarValue::Text(b)) => Ok(a.cmp(b)),
        _ => Ok(left.render().cmp(&right.render())),
    }
}

pub(crate) fn try_parse_bool(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Bool(v) => Some(*v),
        ScalarValue::Int(v) => Some(*v != 0),
        ScalarValue::Text(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "t" | "1" => Some(true),
                "false" | "f" | "0" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(crate) fn parse_nullable_bool(value: &ScalarValue, message: &str) -> Result<Option<bool>, EngineError> {
    match value {
        ScalarValue::Bool(v) => Ok(Some(*v)),
        ScalarValue::Null => Ok(None),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(crate) fn quote_ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

pub(crate) fn quote_nullable(value: &ScalarValue) -> String {
    if matches!(value, ScalarValue::Null) {
        "NULL".to_string()
    } else {
        quote_literal(&value.render())
    }
}

pub(crate) fn count_nulls(args: &[ScalarValue]) -> usize {
    args.iter().filter(|arg| matches!(arg, ScalarValue::Null)).count()
}

pub(crate) fn count_nonnulls(args: &[ScalarValue]) -> usize {
    args.len() - count_nulls(args)
}

pub(crate) fn parse_i64_scalar(value: &ScalarValue, message: &str) -> Result<i64, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(*v),
        ScalarValue::Float(v) if v.fract() == 0.0 => Ok(*v as i64),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(parsed);
            }
            if let Ok(parsed) = v.parse::<f64>()
                && parsed.fract() == 0.0
            {
                return Ok(parsed as i64);
            }
            Err(EngineError {
                message: message.to_string(),
            })
        }
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_f64_scalar(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Text(v) => v.parse::<f64>().map_err(|_| EngineError {
            message: message.to_string(),
        }),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_f64_numeric_scalar(
    value: &ScalarValue,
    message: &str,
) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_bool_scalar(
    value: &ScalarValue,
    message: &str,
) -> Result<bool, EngineError> {
    try_parse_bool(value).ok_or_else(|| EngineError {
        message: message.to_string(),
    })
}

pub(crate) fn truthy(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Bool(v) => *v,
        ScalarValue::Null => false,
        ScalarValue::Int(v) => *v != 0,
        ScalarValue::Float(v) => *v != 0.0,
        ScalarValue::Text(v) => !v.is_empty(),
        ScalarValue::Array(values) => !values.is_empty(),
    }
}

// ── Extension & Function execution ──────────────────────────────────────────

