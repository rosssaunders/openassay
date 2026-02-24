use std::cmp::Ordering;

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::executor::exec_main::json_value_to_scalar;
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::{compare_values_for_predicate, parse_bool_scalar};

pub(crate) fn scalar_to_json_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Null => Ok(JsonValue::Null),
        ScalarValue::Bool(v) => Ok(JsonValue::Bool(*v)),
        ScalarValue::Int(v) => Ok(JsonValue::Number(JsonNumber::from(*v))),
        ScalarValue::Float(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .ok_or_else(|| EngineError {
                message: "cannot convert non-finite float to JSON value".to_string(),
            }),
        ScalarValue::Numeric(v) => {
            let float_val = v.to_string().parse::<f64>().map_err(|_| EngineError {
                message: "cannot convert numeric to JSON number".to_string(),
            })?;
            JsonNumber::from_f64(float_val)
                .map(JsonValue::Number)
                .ok_or_else(|| EngineError {
                    message: "cannot convert non-finite numeric to JSON value".to_string(),
                })
        }
        ScalarValue::Text(v) => Ok(JsonValue::String(v.clone())),
        ScalarValue::Array(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(scalar_to_json_value(value)?);
            }
            Ok(JsonValue::Array(items))
        }
        ScalarValue::Record(fields) => {
            let mut items = Vec::with_capacity(fields.len());
            for field in fields {
                items.push(scalar_to_json_value(field)?);
            }
            Ok(JsonValue::Array(items))
        }
        ScalarValue::Vector(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                let Some(num) = JsonNumber::from_f64(*value as f64) else {
                    return Err(EngineError {
                        message: "cannot convert non-finite numeric to JSON value".to_string(),
                    });
                };
                items.push(JsonValue::Number(num));
            }
            Ok(JsonValue::Array(items))
        }
    }
}

pub(crate) fn json_build_object_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut object = JsonMap::new();
    for idx in (0..args.len()).step_by(2) {
        let key = match &args[idx] {
            ScalarValue::Null => {
                return Err(EngineError {
                    message: "null value not allowed for object key".to_string(),
                });
            }
            other => other.render(),
        };
        let value = scalar_to_json_value(&args[idx + 1])?;
        object.insert(key, value);
    }
    Ok(JsonValue::Object(object))
}

pub(crate) fn json_build_array_value(args: &[ScalarValue]) -> Result<JsonValue, EngineError> {
    let mut items = Vec::with_capacity(args.len());
    for arg in args {
        items.push(scalar_to_json_value(arg)?);
    }
    Ok(JsonValue::Array(items))
}

fn parse_json_or_scalar_value(value: &ScalarValue) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => serde_json::from_str::<JsonValue>(text)
            .or_else(|_| scalar_to_json_value(value))
            .map_err(|err| EngineError {
                message: format!("cannot convert value to JSON: {err}"),
            }),
        _ => scalar_to_json_value(value),
    }
}

fn parse_json_constructor_pretty_arg(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<bool>, EngineError> {
    if args.len() < 2 {
        return Ok(Some(false));
    }
    if matches!(args[1], ScalarValue::Null) {
        return Ok(None);
    }
    parse_bool_scalar(
        &args[1],
        &format!("{fn_name}() expects boolean pretty argument"),
    )
    .map(Some)
}

fn maybe_pretty_json(value: &JsonValue, pretty: bool) -> Result<String, EngineError> {
    if pretty {
        serde_json::to_string_pretty(value).map_err(|err| EngineError {
            message: format!("failed to pretty-print JSON: {err}"),
        })
    } else {
        Ok(value.to_string())
    }
}

pub(crate) fn eval_row_to_json(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };

    let input = parse_json_or_scalar_value(&args[0])?;
    let object = match input {
        JsonValue::Object(map) => JsonValue::Object(map),
        JsonValue::Array(items) => {
            let mut map = JsonMap::new();
            for (idx, value) in items.into_iter().enumerate() {
                map.insert(format!("f{}", idx + 1), value);
            }
            JsonValue::Object(map)
        }
        scalar => {
            let mut map = JsonMap::new();
            map.insert("f1".to_string(), scalar);
            JsonValue::Object(map)
        }
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&object, pretty)?))
}

pub(crate) fn eval_array_to_json(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let Some(pretty) = parse_json_constructor_pretty_arg(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let parsed = parse_json_or_scalar_value(&args[0])?;
    let JsonValue::Array(_) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Text(maybe_pretty_json(&parsed, pretty)?))
}

fn json_value_text_token(
    value: &JsonValue,
    fn_name: &str,
    key_mode: bool,
) -> Result<String, EngineError> {
    match value {
        JsonValue::Null if key_mode => Err(EngineError {
            message: format!("{fn_name}() key cannot be null"),
        }),
        JsonValue::Null => Ok("null".to_string()),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(v) => Ok(v.to_string()),
        JsonValue::Number(v) => Ok(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_object_pairs_from_array(
    value: &JsonValue,
    fn_name: &str,
) -> Result<Vec<(String, String)>, EngineError> {
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument must be a JSON array"),
        });
    };

    if items
        .iter()
        .all(|item| matches!(item, JsonValue::Array(inner) if inner.len() == 2))
    {
        let mut pairs = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Array(inner) = item else {
                continue;
            };
            let key = json_value_text_token(&inner[0], fn_name, true)?;
            let value = json_value_text_token(&inner[1], fn_name, false)?;
            pairs.push((key, value));
        }
        return Ok(pairs);
    }

    if items.len() % 2 != 0 {
        return Err(EngineError {
            message: format!("{fn_name}() requires an even-length text array"),
        });
    }

    let mut pairs = Vec::with_capacity(items.len() / 2);
    for idx in (0..items.len()).step_by(2) {
        let key = json_value_text_token(&items[idx], fn_name, true)?;
        let value = json_value_text_token(&items[idx + 1], fn_name, false)?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

pub(crate) fn eval_json_object(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let pairs = match args.len() {
        1 => {
            let source = parse_json_document_arg(&args[0], fn_name, 1)?;
            parse_json_object_pairs_from_array(&source, fn_name)?
        }
        2 => {
            let keys = parse_json_document_arg(&args[0], fn_name, 1)?;
            let values = parse_json_document_arg(&args[1], fn_name, 2)?;
            let JsonValue::Array(key_items) = keys else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 1 must be a JSON array"),
                });
            };
            let JsonValue::Array(value_items) = values else {
                return Err(EngineError {
                    message: format!("{fn_name}() argument 2 must be a JSON array"),
                });
            };
            if key_items.len() != value_items.len() {
                return Err(EngineError {
                    message: format!("{fn_name}() key/value array lengths must match"),
                });
            }
            key_items
                .iter()
                .zip(value_items.iter())
                .map(|(key, value)| {
                    Ok((
                        json_value_text_token(key, fn_name, true)?,
                        json_value_text_token(value, fn_name, false)?,
                    ))
                })
                .collect::<Result<Vec<_>, EngineError>>()?
        }
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}() expects one or two arguments"),
            });
        }
    };

    let mut object = JsonMap::new();
    for (key, value) in pairs {
        object.insert(key, JsonValue::String(value));
    }
    Ok(ScalarValue::Text(JsonValue::Object(object).to_string()))
}

pub(crate) fn parse_json_document_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSON text"),
        });
    };
    serde_json::from_str::<JsonValue>(text).map_err(|err| EngineError {
        message: format!("{fn_name}() argument {arg_index} is not valid JSON: {err}"),
    })
}

fn parse_json_path_segments(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Option<Vec<String>>, EngineError> {
    let mut out = Vec::with_capacity(args.len().saturating_sub(1));
    for path_arg in &args[1..] {
        match path_arg {
            ScalarValue::Null => return Ok(None),
            ScalarValue::Text(text) => out.push(text.clone()),
            ScalarValue::Int(v) => out.push(v.to_string()),
            ScalarValue::Float(v) => out.push(v.to_string()),
            ScalarValue::Numeric(v) => out.push(v.to_string()),
            ScalarValue::Bool(v) => out.push(v.to_string()),
            ScalarValue::Record(_) | ScalarValue::Array(_) | ScalarValue::Vector(_) => {
                return Err(EngineError {
                    message: format!("{fn_name}() path arguments must be scalar values"),
                });
            }
        }
    }
    if out.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires at least one path argument"),
        });
    }
    Ok(Some(out))
}

fn extract_json_path_value<'a>(root: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = root;
    for segment in path {
        current = match current {
            JsonValue::Object(map) => map.get(segment)?,
            JsonValue::Array(array) => {
                let idx = segment.parse::<usize>().ok()?;
                array.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current)
}

pub(crate) fn json_value_text_output(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::String(text) => ScalarValue::Text(text.clone()),
        JsonValue::Bool(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Number(v) => ScalarValue::Text(v.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

pub(crate) fn eval_json_extract_path(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&args[0], fn_name, 1)?;
    let Some(path) = parse_json_path_segments(args, fn_name)? else {
        return Ok(ScalarValue::Null);
    };
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

pub(crate) fn eval_json_array_length(
    value: &ScalarValue,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let JsonValue::Array(items) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };
    Ok(ScalarValue::Int(items.len() as i64))
}

pub(crate) fn eval_json_typeof(
    value: &ScalarValue,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let ty = match parsed {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    };
    Ok(ScalarValue::Text(ty.to_string()))
}

fn strip_null_object_fields(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if let Some(inner) = map.get_mut(&key) {
                    strip_null_object_fields(inner);
                }
                if map.get(&key).is_some_and(|candidate| candidate.is_null()) {
                    map.remove(&key);
                }
            }
        }
        JsonValue::Array(array) => {
            for item in array {
                strip_null_object_fields(item);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

pub(crate) fn eval_json_strip_nulls(
    value: &ScalarValue,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut parsed = parse_json_document_arg(value, fn_name, 1)?;
    strip_null_object_fields(&mut parsed);
    Ok(ScalarValue::Text(parsed.to_string()))
}

pub(crate) fn eval_json_pretty(
    value: &ScalarValue,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(value, fn_name, 1)?;
    let pretty = serde_json::to_string_pretty(&parsed).map_err(|err| EngineError {
        message: format!("{fn_name}() failed to pretty-print JSON: {err}"),
    })?;
    Ok(ScalarValue::Text(pretty))
}

pub(crate) fn eval_jsonb_exists(
    target: &ScalarValue,
    key: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(key, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, "jsonb_exists", 1)?;
    let key = scalar_to_json_path_segment(key, "jsonb_exists")?;
    Ok(ScalarValue::Bool(json_has_key(&parsed, &key)))
}

pub(crate) fn eval_jsonb_exists_any_all(
    target: &ScalarValue,
    keys: &ScalarValue,
    any_mode: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if matches!(target, ScalarValue::Null) || matches!(keys, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let parsed = parse_json_document_arg(target, fn_name, 1)?;
    let keys = parse_json_path_operand(keys, fn_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() key array cannot be empty"),
        });
    }
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&parsed, key))
    } else {
        keys.iter().all(|key| json_has_key(&parsed, key))
    };
    Ok(ScalarValue::Bool(matched))
}

fn parse_json_path_text_array(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);
    if inner.trim().is_empty() {
        return Vec::new();
    }
    inner
        .split(',')
        .map(str::trim)
        .map(|segment| {
            segment
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .unwrap_or(segment)
                .to_string()
        })
        .collect()
}

fn parse_json_new_value_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonValue, EngineError> {
    match value {
        ScalarValue::Text(text) => match serde_json::from_str::<JsonValue>(text) {
            Ok(parsed) => Ok(parsed),
            Err(_) => Ok(JsonValue::String(text.clone())),
        },
        _ => scalar_to_json_value(value).map_err(|err| EngineError {
            message: format!(
                "{fn_name}() argument {arg_index} is invalid: {}",
                err.message
            ),
        }),
    }
}

fn json_container_for_next_segment(next: Option<&str>) -> JsonValue {
    if next.is_some_and(|segment| segment.parse::<usize>().is_ok()) {
        JsonValue::Array(Vec::new())
    } else {
        JsonValue::Object(JsonMap::new())
    }
}

fn json_set_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    create_missing: bool,
) -> bool {
    if path.is_empty() {
        *root = new_value;
        return true;
    }

    let mut current = root;
    for idx in 0..path.len() {
        let segment = &path[idx];
        let is_last = idx + 1 == path.len();
        let next_segment = path.get(idx + 1).map(String::as_str);

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(segment) || create_missing {
                        map.insert(segment.clone(), new_value);
                        return true;
                    }
                    return false;
                }
                if !map.contains_key(segment) {
                    if !create_missing {
                        return false;
                    }
                    map.insert(
                        segment.clone(),
                        json_container_for_next_segment(next_segment),
                    );
                }
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                current = next;
            }
            JsonValue::Array(array) => {
                let Ok(index) = segment.parse::<usize>() else {
                    return false;
                };
                if is_last {
                    if index < array.len() {
                        array[index] = new_value;
                        return true;
                    }
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                    array[index] = new_value;
                    return true;
                }
                if index >= array.len() {
                    if !create_missing {
                        return false;
                    }
                    while array.len() <= index {
                        array.push(JsonValue::Null);
                    }
                }
                if array[index].is_null() {
                    array[index] = json_container_for_next_segment(next_segment);
                }
                current = &mut array[index];
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        }
    }
    false
}

fn json_insert_array_index(len: usize, segment: &str, insert_after: bool) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    let mut pos = if index >= 0 {
        index as isize
    } else {
        len as isize + index as isize
    };
    if insert_after {
        pos += 1;
    }
    if pos < 0 {
        pos = 0;
    }
    if pos > len as isize {
        pos = len as isize;
    }
    Some(pos as usize)
}

fn json_insert_path(
    root: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    insert_after: bool,
) -> bool {
    if path.is_empty() {
        return false;
    }

    let mut current = root;
    for segment in &path[..path.len() - 1] {
        current = match current {
            JsonValue::Object(map) => {
                let Some(next) = map.get_mut(segment) else {
                    return false;
                };
                next
            }
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), segment) else {
                    return false;
                };
                &mut array[idx]
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                return false;
            }
        };
    }

    let last = &path[path.len() - 1];
    match current {
        JsonValue::Object(map) => {
            if map.contains_key(last) {
                false
            } else {
                map.insert(last.clone(), new_value);
                true
            }
        }
        JsonValue::Array(array) => {
            let Some(pos) = json_insert_array_index(array.len(), last, insert_after) else {
                return false;
            };
            array.insert(pos, new_value);
            true
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_jsonb_set(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set", 3)?;
    let create_missing = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_set() expects boolean create_missing argument",
        )?
    } else {
        true
    };
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_jsonb_insert(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args[..3].iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_insert", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_insert() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let new_value = parse_json_new_value_arg(&args[2], "jsonb_insert", 3)?;
    let insert_after = if args.len() == 4 {
        parse_bool_scalar(
            &args[3],
            "jsonb_insert() expects boolean insert_after argument",
        )?
    } else {
        false
    };
    let _ = json_insert_path(&mut target, &path, new_value, insert_after);
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_jsonb_set_lax(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let mut target = parse_json_document_arg(&args[0], "jsonb_set_lax", 1)?;
    let path_text = match &args[1] {
        ScalarValue::Text(text) => text,
        _ => {
            return Err(EngineError {
                message: "jsonb_set_lax() argument 2 must be text path (for example '{a,b}')"
                    .to_string(),
            });
        }
    };
    let path = parse_json_path_text_array(path_text);
    let create_missing = if args.len() >= 4 {
        if matches!(args[3], ScalarValue::Null) {
            return Ok(ScalarValue::Null);
        }
        parse_bool_scalar(
            &args[3],
            "jsonb_set_lax() expects boolean create_if_missing argument",
        )?
    } else {
        true
    };

    if matches!(args[2], ScalarValue::Null) {
        let treatment = if args.len() >= 5 {
            if matches!(args[4], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            args[4].render().trim().to_ascii_lowercase()
        } else {
            "use_json_null".to_string()
        };
        match treatment.as_str() {
            "raise_exception" => {
                return Err(EngineError {
                    message: "jsonb_set_lax() null_value_treatment requested exception".to_string(),
                });
            }
            "use_json_null" => {
                let _ = json_set_path(&mut target, &path, JsonValue::Null, create_missing);
            }
            "delete_key" => {
                if !path.is_empty() {
                    let _ = json_remove_path(&mut target, &path);
                }
            }
            "return_target" => {}
            _ => {
                return Err(EngineError {
                    message: format!("jsonb_set_lax() unknown null_value_treatment {treatment}"),
                });
            }
        }
        return Ok(ScalarValue::Text(target.to_string()));
    }

    let new_value = parse_json_new_value_arg(&args[2], "jsonb_set_lax", 3)?;
    let _ = json_set_path(&mut target, &path, new_value, create_missing);
    Ok(ScalarValue::Text(target.to_string()))
}

fn extract_json_get_value<'a>(target: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    match target {
        JsonValue::Object(map) => map.get(path),
        JsonValue::Array(array) => {
            let idx = path.parse::<i64>().ok()?;
            if idx >= 0 {
                array.get(idx as usize)
            } else {
                let back = idx.unsigned_abs() as usize;
                if back > array.len() {
                    None
                } else {
                    array.get(array.len().saturating_sub(back))
                }
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => None,
    }
}

fn scalar_to_json_path_segment(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<String, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator does not accept NULL path/key operand"),
        }),
        ScalarValue::Text(text) => Ok(text.clone()),
        ScalarValue::Int(v) => Ok(v.to_string()),
        ScalarValue::Float(v) => Ok(v.to_string()),
        ScalarValue::Numeric(v) => Ok(v.to_string()),
        ScalarValue::Bool(v) => Ok(v.to_string()),
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
            Err(EngineError {
                message: format!("{operator_name} operator path operand must be scalar"),
            })
        }
    }
}

fn parse_json_path_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    match value {
        ScalarValue::Null => Err(EngineError {
            message: format!("{operator_name} operator path operand cannot be NULL"),
        }),
        ScalarValue::Text(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('[') {
                let parsed =
                    serde_json::from_str::<JsonValue>(trimmed).map_err(|err| EngineError {
                        message: format!(
                            "{operator_name} operator path operand must be text[]/json array: {err}"
                        ),
                    })?;
                let JsonValue::Array(items) = parsed else {
                    return Err(EngineError {
                        message: format!("{operator_name} operator path operand must be array"),
                    });
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::String(s) => out.push(s),
                        JsonValue::Number(n) => out.push(n.to_string()),
                        JsonValue::Bool(v) => out.push(v.to_string()),
                        JsonValue::Null => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array cannot contain null"
                                ),
                            });
                        }
                        JsonValue::Array(_) | JsonValue::Object(_) => {
                            return Err(EngineError {
                                message: format!(
                                    "{operator_name} operator path array entries must be scalar"
                                ),
                            });
                        }
                    }
                }
                return Ok(out);
            }
            Ok(parse_json_path_text_array(trimmed))
        }
        ScalarValue::Int(v) => Ok(vec![v.to_string()]),
        ScalarValue::Float(v) => Ok(vec![v.to_string()]),
        ScalarValue::Numeric(v) => Ok(vec![v.to_string()]),
        ScalarValue::Bool(v) => Ok(vec![v.to_string()]),
        ScalarValue::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                if matches!(value, ScalarValue::Null) {
                    return Err(EngineError {
                        message: format!("{operator_name} operator path array cannot contain null"),
                    });
                }
                out.push(value.render());
            }
            Ok(out)
        }
        ScalarValue::Record(_) | ScalarValue::Vector(_) => Err(EngineError {
            message: format!("{operator_name} operator path operand must be text[] or scalar"),
        }),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathStep {
    Key(String),
    Index(i64),
    Wildcard,
    Filter(JsonPathFilterExpr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonPathFilterOp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterOperand {
    Current,
    CurrentPath(Vec<JsonPathStep>),
    RootPath(Vec<JsonPathStep>),
    Variable(String),
    Literal(JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
enum JsonPathFilterExpr {
    Exists(JsonPathFilterOperand),
    Compare {
        left: JsonPathFilterOperand,
        op: JsonPathFilterOp,
        right: JsonPathFilterOperand,
    },
    Truthy(JsonPathFilterOperand),
}

fn parse_jsonpath_text_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<String, EngineError> {
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be JSONPath text"),
        });
    };
    Ok(text.clone())
}

fn parse_jsonpath_vars_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<JsonMap<String, JsonValue>, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(JsonMap::new());
    }
    let parsed = parse_json_document_arg(value, fn_name, arg_index)?;
    let JsonValue::Object(vars) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be a JSON object"),
        });
    };
    Ok(vars)
}

fn parse_jsonpath_silent_arg(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<bool, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(false);
    }
    parse_bool_scalar(
        value,
        &format!("{fn_name}() argument {arg_index} must be boolean"),
    )
}

fn strip_outer_parentheses(text: &str) -> &str {
    let mut trimmed = text.trim();
    loop {
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let mut depth = 0isize;
        let mut in_quote: Option<char> = None;
        let mut encloses = true;
        for (idx, ch) in trimmed.char_indices() {
            if let Some(quote) = in_quote {
                if ch == quote {
                    in_quote = None;
                } else if ch == '\\' {
                    continue;
                }
                continue;
            }
            if ch == '\'' || ch == '"' {
                in_quote = Some(ch);
                continue;
            }
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 && idx + ch.len_utf8() < trimmed.len() {
                    encloses = false;
                    break;
                }
            }
        }
        if encloses {
            trimmed = trimmed[1..trimmed.len() - 1].trim();
        } else {
            return trimmed;
        }
    }
}

fn find_jsonpath_compare_operator(expr: &str) -> Option<(usize, &'static str, JsonPathFilterOp)> {
    let bytes = expr.as_bytes();
    let mut idx = 0usize;
    let mut in_quote: Option<u8> = None;
    let mut paren_depth = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if let Some(quote) = in_quote {
            if b == quote {
                in_quote = None;
            } else if b == b'\\' {
                idx += 1;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' | b'"' => {
                in_quote = Some(b);
                idx += 1;
                continue;
            }
            b'(' => {
                paren_depth += 1;
                idx += 1;
                continue;
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                idx += 1;
                continue;
            }
            _ => {}
        }
        if paren_depth == 0 {
            for (token, op) in [
                ("==", JsonPathFilterOp::Eq),
                ("!=", JsonPathFilterOp::NotEq),
                (">=", JsonPathFilterOp::Gte),
                ("<=", JsonPathFilterOp::Lte),
                (">", JsonPathFilterOp::Gt),
                ("<", JsonPathFilterOp::Lt),
            ] {
                if expr[idx..].starts_with(token) {
                    return Some((idx, token, op));
                }
            }
        }
        idx += 1;
    }
    None
}

fn parse_jsonpath_filter_operand(
    token: &str,
    context: &str,
) -> Result<JsonPathFilterOperand, EngineError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(EngineError {
            message: format!("{context} JSONPath filter operand is empty"),
        });
    }

    if token == "@" {
        return Ok(JsonPathFilterOperand::Current);
    }
    if token.starts_with("@.") || token.starts_with("@[") {
        let rooted = format!("${}", &token[1..]);
        let steps = parse_jsonpath_steps(&rooted, context)?;
        return Ok(JsonPathFilterOperand::CurrentPath(steps));
    }
    if token == "$" {
        return Ok(JsonPathFilterOperand::RootPath(Vec::new()));
    }
    if token.starts_with("$.") || token.starts_with("$[") {
        let steps = parse_jsonpath_steps(token, context)?;
        return Ok(JsonPathFilterOperand::RootPath(steps));
    }
    if let Some(name) = token.strip_prefix('$')
        && !name.is_empty()
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Ok(JsonPathFilterOperand::Variable(name.to_string()));
    }

    if (token.starts_with('"') && token.ends_with('"'))
        || (token.starts_with('\'') && token.ends_with('\''))
    {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::String(
            token[1..token.len() - 1].to_string(),
        )));
    }
    if token.eq_ignore_ascii_case("true") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(true)));
    }
    if token.eq_ignore_ascii_case("false") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Bool(false)));
    }
    if token.eq_ignore_ascii_case("null") {
        return Ok(JsonPathFilterOperand::Literal(JsonValue::Null));
    }
    if let Ok(number) = serde_json::from_str::<JsonValue>(token)
        && matches!(number, JsonValue::Number(_))
    {
        return Ok(JsonPathFilterOperand::Literal(number));
    }

    Err(EngineError {
        message: format!("{context} unsupported JSONPath filter operand {token}"),
    })
}

fn parse_jsonpath_filter_expr(
    text: &str,
    context: &str,
) -> Result<JsonPathFilterExpr, EngineError> {
    let trimmed = strip_outer_parentheses(text).trim();
    if trimmed.is_empty() {
        return Err(EngineError {
            message: format!("{context} empty JSONPath filter expression"),
        });
    }

    if trimmed.len() > 7
        && trimmed[..6].eq_ignore_ascii_case("exists")
        && trimmed[6..].trim_start().starts_with('(')
    {
        let rest = trimmed[6..].trim_start();
        if let Some(inner) = rest
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        {
            let operand = parse_jsonpath_filter_operand(inner, context)?;
            return Ok(JsonPathFilterExpr::Exists(operand));
        }
    }

    if let Some((idx, token, op)) = find_jsonpath_compare_operator(trimmed) {
        let left = parse_jsonpath_filter_operand(&trimmed[..idx], context)?;
        let right = parse_jsonpath_filter_operand(&trimmed[idx + token.len()..], context)?;
        return Ok(JsonPathFilterExpr::Compare { left, op, right });
    }

    Ok(JsonPathFilterExpr::Truthy(parse_jsonpath_filter_operand(
        trimmed, context,
    )?))
}

fn parse_jsonpath_steps(path: &str, context: &str) -> Result<Vec<JsonPathStep>, EngineError> {
    let bytes = path.trim().as_bytes();
    if bytes.is_empty() || bytes[0] != b'$' {
        return Err(EngineError {
            message: format!("{context} JSONPath must start with '$'"),
        });
    }

    let mut steps = Vec::new();
    let mut idx = 1usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b' ' | b'\t' | b'\r' | b'\n' => idx += 1,
            b'.' => {
                idx += 1;
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath"),
                    });
                }
                if bytes[idx] == b'*' {
                    steps.push(JsonPathStep::Wildcard);
                    idx += 1;
                    continue;
                }
                if bytes[idx] == b'"' {
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != b'"' {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                if start == idx {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath key"),
                    });
                }
                let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                steps.push(JsonPathStep::Key(key.to_string()));
            }
            b'[' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                if bytes[idx] == b'*' {
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath wildcard index"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Wildcard);
                    continue;
                }
                if bytes[idx] == b'\'' || bytes[idx] == b'"' {
                    let quote = bytes[idx];
                    idx += 1;
                    let start = idx;
                    while idx < bytes.len() && bytes[idx] != quote {
                        idx += 1;
                    }
                    if idx >= bytes.len() {
                        return Err(EngineError {
                            message: format!("{context} unterminated quoted JSONPath key"),
                        });
                    }
                    let key = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                        idx += 1;
                    }
                    if idx >= bytes.len() || bytes[idx] != b']' {
                        return Err(EngineError {
                            message: format!("{context} invalid JSONPath bracket expression"),
                        });
                    }
                    idx += 1;
                    steps.push(JsonPathStep::Key(key.to_string()));
                    continue;
                }
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b']' {
                    idx += 1;
                }
                if idx >= bytes.len() {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath index"),
                    });
                }
                let token = std::str::from_utf8(&bytes[start..idx])
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                idx += 1;
                if token.is_empty() {
                    return Err(EngineError {
                        message: format!("{context} invalid JSONPath index"),
                    });
                }
                if let Ok(index) = token.parse::<i64>() {
                    steps.push(JsonPathStep::Index(index));
                } else {
                    steps.push(JsonPathStep::Key(token));
                }
            }
            b'?' => {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx >= bytes.len() || bytes[idx] != b'(' {
                    return Err(EngineError {
                        message: format!("{context} expected '(' after JSONPath filter marker"),
                    });
                }
                idx += 1;
                let start = idx;
                let mut depth = 1usize;
                let mut in_quote: Option<u8> = None;
                while idx < bytes.len() {
                    let b = bytes[idx];
                    if let Some(quote) = in_quote {
                        if b == quote {
                            in_quote = None;
                        } else if b == b'\\' {
                            idx += 1;
                        }
                        idx += 1;
                        continue;
                    }
                    if b == b'\'' || b == b'"' {
                        in_quote = Some(b);
                        idx += 1;
                        continue;
                    }
                    if b == b'(' {
                        depth += 1;
                        idx += 1;
                        continue;
                    }
                    if b == b')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        idx += 1;
                        continue;
                    }
                    idx += 1;
                }
                if idx >= bytes.len() || depth != 0 {
                    return Err(EngineError {
                        message: format!("{context} unterminated JSONPath filter expression"),
                    });
                }
                let expr_text = std::str::from_utf8(&bytes[start..idx]).unwrap_or_default();
                idx += 1;
                steps.push(JsonPathStep::Filter(parse_jsonpath_filter_expr(
                    expr_text, context,
                )?));
            }
            _ => {
                return Err(EngineError {
                    message: format!("{context} invalid JSONPath near byte {idx}"),
                });
            }
        }
    }
    Ok(steps)
}

fn jsonpath_query_values(
    root: &JsonValue,
    steps: &[JsonPathStep],
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    let mut current = vec![root.clone()];
    for step in steps {
        let mut next = Vec::new();
        for value in current {
            match step {
                JsonPathStep::Key(key) => {
                    if let JsonValue::Object(map) = value
                        && let Some(found) = map.get(key)
                    {
                        next.push(found.clone());
                    }
                }
                JsonPathStep::Index(index) => {
                    if let JsonValue::Array(items) = value
                        && let Some(idx) = json_array_index_from_i64(items.len(), *index)
                    {
                        next.push(items[idx].clone());
                    }
                }
                JsonPathStep::Wildcard => match value {
                    JsonValue::Array(items) => next.extend(items),
                    JsonValue::Object(map) => next.extend(map.values().cloned()),
                    JsonValue::Null
                    | JsonValue::Bool(_)
                    | JsonValue::Number(_)
                    | JsonValue::String(_) => {}
                },
                JsonPathStep::Filter(expr) => {
                    if eval_jsonpath_filter_expr(
                        expr,
                        &value,
                        absolute_root,
                        vars,
                        context,
                        silent,
                    )? {
                        next.push(value);
                    }
                }
            }
        }
        current = next;
        if current.is_empty() {
            break;
        }
    }
    Ok(current)
}

fn jsonpath_value_truthy(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => false,
        JsonValue::Bool(v) => *v,
        JsonValue::Number(v) => v.as_f64().is_some_and(|n| n != 0.0),
        JsonValue::String(v) => !v.is_empty(),
        JsonValue::Array(v) => !v.is_empty(),
        JsonValue::Object(v) => !v.is_empty(),
    }
}

fn json_values_compare(left: &JsonValue, right: &JsonValue) -> Result<Ordering, EngineError> {
    match (left, right) {
        (JsonValue::Array(_), JsonValue::Array(_))
        | (JsonValue::Object(_), JsonValue::Object(_)) => {
            if left == right {
                Ok(Ordering::Equal)
            } else {
                Ok(left.to_string().cmp(&right.to_string()))
            }
        }
        _ => {
            compare_values_for_predicate(&json_value_to_scalar(left), &json_value_to_scalar(right))
        }
    }
}

fn eval_jsonpath_filter_operand(
    operand: &JsonPathFilterOperand,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<Vec<JsonValue>, EngineError> {
    match operand {
        JsonPathFilterOperand::Current => Ok(vec![current.clone()]),
        JsonPathFilterOperand::CurrentPath(steps) => {
            jsonpath_query_values(current, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::RootPath(steps) => {
            jsonpath_query_values(absolute_root, steps, absolute_root, vars, context, silent)
        }
        JsonPathFilterOperand::Variable(name) => {
            if let Some(value) = vars.get(name) {
                Ok(vec![value.clone()])
            } else if silent {
                Ok(Vec::new())
            } else {
                Err(EngineError {
                    message: format!("{context} JSONPath variable ${name} is not provided"),
                })
            }
        }
        JsonPathFilterOperand::Literal(value) => Ok(vec![value.clone()]),
    }
}

fn eval_jsonpath_filter_expr(
    expr: &JsonPathFilterExpr,
    current: &JsonValue,
    absolute_root: &JsonValue,
    vars: &JsonMap<String, JsonValue>,
    context: &str,
    silent: bool,
) -> Result<bool, EngineError> {
    match expr {
        JsonPathFilterExpr::Exists(operand) => Ok(!eval_jsonpath_filter_operand(
            operand,
            current,
            absolute_root,
            vars,
            context,
            silent,
        )?
        .is_empty()),
        JsonPathFilterExpr::Truthy(operand) => {
            let values = eval_jsonpath_filter_operand(
                operand,
                current,
                absolute_root,
                vars,
                context,
                silent,
            )?;
            Ok(values.iter().any(jsonpath_value_truthy))
        }
        JsonPathFilterExpr::Compare { left, op, right } => {
            let left_values =
                eval_jsonpath_filter_operand(left, current, absolute_root, vars, context, silent)?;
            let right_values =
                eval_jsonpath_filter_operand(right, current, absolute_root, vars, context, silent)?;
            for left in &left_values {
                for right in &right_values {
                    let ordering = json_values_compare(left, right)?;
                    let matched = match op {
                        JsonPathFilterOp::Eq => ordering == Ordering::Equal,
                        JsonPathFilterOp::NotEq => ordering != Ordering::Equal,
                        JsonPathFilterOp::Gt => ordering == Ordering::Greater,
                        JsonPathFilterOp::Gte => {
                            matches!(ordering, Ordering::Greater | Ordering::Equal)
                        }
                        JsonPathFilterOp::Lt => ordering == Ordering::Less,
                        JsonPathFilterOp::Lte => {
                            matches!(ordering, Ordering::Less | Ordering::Equal)
                        }
                    };
                    if matched {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }
}

pub(crate) fn eval_json_path_predicate_operator(
    left: ScalarValue,
    right: ScalarValue,
    match_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(
        &left,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        1,
    )?;
    let path_text = parse_jsonpath_text_arg(
        &right,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        2,
    )?;
    let steps = parse_jsonpath_steps(
        &path_text,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
    )?;
    let vars = JsonMap::new();
    let values = jsonpath_query_values(
        &target,
        &steps,
        &target,
        &vars,
        if match_mode {
            "json operator @@"
        } else {
            "json operator @?"
        },
        false,
    )?;
    if match_mode {
        if let Some(first) = values.first() {
            Ok(ScalarValue::Bool(jsonpath_value_truthy(first)))
        } else {
            Ok(ScalarValue::Bool(false))
        }
    } else {
        Ok(ScalarValue::Bool(!values.is_empty()))
    }
}

pub(crate) fn jsonb_path_query_values(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<Vec<JsonValue>, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(Vec::new());
    }
    let silent = if args.len() >= 4 {
        parse_jsonpath_silent_arg(&args[3], fn_name, 4)?
    } else {
        false
    };
    let evaluate = || -> Result<Vec<JsonValue>, EngineError> {
        let target = parse_json_document_arg(&args[0], fn_name, 1)?;
        let path_text = parse_jsonpath_text_arg(&args[1], fn_name, 2)?;
        let vars = if args.len() >= 3 {
            parse_jsonpath_vars_arg(&args[2], fn_name, 3)?
        } else {
            JsonMap::new()
        };
        let steps = parse_jsonpath_steps(&path_text, fn_name)?;
        jsonpath_query_values(&target, &steps, &target, &vars, fn_name, silent)
    };
    if silent {
        evaluate().or(Ok(Vec::new()))
    } else {
        evaluate()
    }
}

pub(crate) fn eval_jsonb_path_exists(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Bool(!values.is_empty()))
}

pub(crate) fn eval_jsonb_path_match(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        match first {
            JsonValue::Null => Ok(ScalarValue::Null),
            JsonValue::Bool(v) => Ok(ScalarValue::Bool(*v)),
            _ => Ok(ScalarValue::Bool(jsonpath_value_truthy(first))),
        }
    } else {
        Ok(ScalarValue::Bool(false))
    }
}

pub(crate) fn eval_jsonb_path_query_array(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    Ok(ScalarValue::Text(JsonValue::Array(values).to_string()))
}

pub(crate) fn eval_jsonb_path_query_first(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    if args.len() < 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects at least two arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let values = jsonb_path_query_values(args, fn_name)?;
    if let Some(first) = values.first() {
        Ok(ScalarValue::Text(first.to_string()))
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(crate) fn eval_json_get_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ->/->>", 1)?;
    let path_segment = scalar_to_json_path_segment(&right, "->")?;
    let Some(found) = extract_json_get_value(&target, &path_segment) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

pub(crate) fn eval_json_path_operator(
    left: ScalarValue,
    right: ScalarValue,
    text_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator #>/#>>", 1)?;
    let path = parse_json_path_operand(&right, "#>")?;
    let Some(found) = extract_json_path_value(&target, &path) else {
        return Ok(ScalarValue::Null);
    };
    if text_mode {
        Ok(json_value_text_output(found))
    } else {
        Ok(ScalarValue::Text(found.to_string()))
    }
}

fn json_concat(lhs: JsonValue, rhs: JsonValue) -> JsonValue {
    match (lhs, rhs) {
        (JsonValue::Object(mut left), JsonValue::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            JsonValue::Object(left)
        }
        (JsonValue::Array(mut left), JsonValue::Array(right)) => {
            left.extend(right);
            JsonValue::Array(left)
        }
        (JsonValue::Array(mut left), right) => {
            left.push(right);
            JsonValue::Array(left)
        }
        (left, JsonValue::Array(right)) => {
            let mut out = Vec::with_capacity(right.len() + 1);
            out.push(left);
            out.extend(right);
            JsonValue::Array(out)
        }
        (left, right) => JsonValue::Array(vec![left, right]),
    }
}

pub(crate) fn eval_json_concat_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    match (left.clone(), right.clone()) {
        (ScalarValue::Array(mut left_items), ScalarValue::Array(right_items)) => {
            left_items.extend(right_items);
            Ok(ScalarValue::Array(left_items))
        }
        (ScalarValue::Array(mut left_items), other) => {
            left_items.push(other);
            Ok(ScalarValue::Array(left_items))
        }
        (other, ScalarValue::Array(mut right_items)) => {
            right_items.insert(0, other);
            Ok(ScalarValue::Array(right_items))
        }
        (left, right) => {
            if let (Ok(lhs), Ok(rhs)) = (
                parse_json_document_arg(&left, "json operator ||", 1),
                parse_json_document_arg(&right, "json operator ||", 2),
            ) {
                Ok(ScalarValue::Text(json_concat(lhs, rhs).to_string()))
            } else {
                Ok(ScalarValue::Text(format!("{}{}", left.render(), right.render())))
            }
        }
    }
}

fn json_contains(lhs: &JsonValue, rhs: &JsonValue) -> bool {
    match (lhs, rhs) {
        (JsonValue::Object(lmap), JsonValue::Object(rmap)) => rmap.iter().all(|(key, rvalue)| {
            lmap.get(key)
                .is_some_and(|lvalue| json_contains(lvalue, rvalue))
        }),
        (JsonValue::Array(larr), JsonValue::Array(rarr)) => rarr
            .iter()
            .all(|rvalue| larr.iter().any(|lvalue| json_contains(lvalue, rvalue))),
        _ => lhs == rhs,
    }
}

pub(crate) fn eval_json_contains_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator @>", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator @>", 2)?;
    Ok(ScalarValue::Bool(json_contains(&lhs, &rhs)))
}

pub(crate) fn eval_json_contained_by_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let lhs = parse_json_document_arg(&left, "json operator <@", 1)?;
    let rhs = parse_json_document_arg(&right, "json operator <@", 2)?;
    Ok(ScalarValue::Bool(json_contains(&rhs, &lhs)))
}

fn json_array_index_from_i64(len: usize, index: i64) -> Option<usize> {
    if index >= 0 {
        let idx = index as usize;
        if idx < len { Some(idx) } else { None }
    } else {
        let back = index.unsigned_abs() as usize;
        if back == 0 || back > len {
            None
        } else {
            Some(len - back)
        }
    }
}

fn json_array_index_from_segment(len: usize, segment: &str) -> Option<usize> {
    let index = segment.parse::<i64>().ok()?;
    json_array_index_from_i64(len, index)
}

fn json_remove_path(target: &mut JsonValue, path: &[String]) -> bool {
    if path.is_empty() {
        return false;
    }
    if path.len() == 1 {
        return match target {
            JsonValue::Object(map) => map.remove(&path[0]).is_some(),
            JsonValue::Array(array) => {
                let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                    return false;
                };
                array.remove(idx);
                true
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
                false
            }
        };
    }

    match target {
        JsonValue::Object(map) => map
            .get_mut(&path[0])
            .is_some_and(|next| json_remove_path(next, &path[1..])),
        JsonValue::Array(array) => {
            let Some(idx) = json_array_index_from_segment(array.len(), &path[0]) else {
                return false;
            };
            json_remove_path(&mut array[idx], &path[1..])
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_json_delete_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator -", 1)?;
    match &mut target {
        JsonValue::Object(map) => {
            let key = scalar_to_json_path_segment(&right, "-")?;
            map.remove(&key);
        }
        JsonValue::Array(array) => match right {
            ScalarValue::Int(index) => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index) {
                    array.remove(idx);
                }
            }
            ScalarValue::Float(index) if index.fract() == 0.0 => {
                if let Some(idx) = json_array_index_from_i64(array.len(), index as i64) {
                    array.remove(idx);
                }
            }
            ScalarValue::Text(text) => {
                array.retain(|item| !matches!(item, JsonValue::String(value) if value == &text));
            }
            _ => {
                return Err(EngineError {
                    message: "json operator - expects text key or integer array index".to_string(),
                });
            }
        },
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            return Err(EngineError {
                message: "json operator - expects object or array left operand".to_string(),
            });
        }
    }
    Ok(ScalarValue::Text(target.to_string()))
}

pub(crate) fn eval_json_delete_path_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let mut target = parse_json_document_arg(&left, "json operator #-", 1)?;
    let path = parse_json_path_operand(&right, "#-")?;
    if !path.is_empty() {
        let _ = json_remove_path(&mut target, &path);
    }
    Ok(ScalarValue::Text(target.to_string()))
}

fn json_has_key(target: &JsonValue, key: &str) -> bool {
    match target {
        JsonValue::Object(map) => map.contains_key(key),
        JsonValue::Array(array) => array
            .iter()
            .any(|item| matches!(item, JsonValue::String(text) if text == key)),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => false,
    }
}

pub(crate) fn eval_json_has_key_operator(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?", 1)?;
    let key = scalar_to_json_path_segment(&right, "?")?;
    Ok(ScalarValue::Bool(json_has_key(&target, &key)))
}

fn parse_json_key_list_operand(
    value: &ScalarValue,
    operator_name: &str,
) -> Result<Vec<String>, EngineError> {
    let keys = parse_json_path_operand(value, operator_name)?;
    if keys.is_empty() {
        return Err(EngineError {
            message: format!("{operator_name} operator key array cannot be empty"),
        });
    }
    Ok(keys)
}

pub(crate) fn eval_json_has_any_all_operator(
    left: ScalarValue,
    right: ScalarValue,
    any_mode: bool,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let target = parse_json_document_arg(&left, "json operator ?|/?&", 1)?;
    let keys = parse_json_key_list_operand(&right, if any_mode { "?|" } else { "?&" })?;
    let matched = if any_mode {
        keys.iter().any(|key| json_has_key(&target, key))
    } else {
        keys.iter().all(|key| json_has_key(&target, key))
    };
    Ok(ScalarValue::Bool(matched))
}

fn url_encode_component(input: &str) -> String {
    let mut out = String::new();
    for byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", *byte)),
        }
    }
    out
}

fn url_encode_pairs(pairs: &[(String, String)]) -> String {
    let mut encoded = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        encoded.push(format!(
            "{}={}",
            url_encode_component(key),
            url_encode_component(value)
        ));
    }
    encoded.join("&")
}

fn parse_json_object_pairs(
    value: &ScalarValue,
    fn_name: &str,
    arg_index: usize,
) -> Result<Vec<(String, String)>, EngineError> {
    let parsed = parse_json_document_arg(value, fn_name, arg_index)?;
    let JsonValue::Object(map) = parsed else {
        return Err(EngineError {
            message: format!("{fn_name}() argument {arg_index} must be a JSON object"),
        });
    };
    let mut pairs = Vec::with_capacity(map.len());
    for (key, value) in map {
        let value_text = json_value_text_token(&value, fn_name, false)?;
        pairs.push((key, value_text));
    }
    Ok(pairs)
}

fn append_query_params(url: &str, query: &str) -> String {
    if query.is_empty() {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&{query}")
    } else {
        format!("{url}?{query}")
    }
}

fn build_http_response(
    status: u16,
    content_type: Option<String>,
    headers: Vec<(String, String)>,
    content: String,
) -> ScalarValue {
    let headers = JsonValue::Array(
        headers
            .into_iter()
            .map(|(field, value)| {
                let mut header = JsonMap::new();
                header.insert("field".to_string(), JsonValue::String(field));
                header.insert("value".to_string(), JsonValue::String(value));
                JsonValue::Object(header)
            })
            .collect(),
    );
    let mut response = JsonMap::new();
    response.insert(
        "status".to_string(),
        JsonValue::Number(JsonNumber::from(status as i64)),
    );
    response.insert(
        "content_type".to_string(),
        content_type
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    response.insert("headers".to_string(), headers);
    response.insert("content".to_string(), JsonValue::String(content));
    ScalarValue::Text(JsonValue::Object(response).to_string())
}

async fn execute_http_request(
    method: &str,
    url: &str,
    content: Option<String>,
    content_type: Option<String>,
    read_body: bool,
    fn_name: &str,
) -> Result<ScalarValue, EngineError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use reqwest::header::CONTENT_TYPE;

        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|err| EngineError {
            message: format!("{fn_name}() invalid HTTP method: {err}"),
        })?;
        let client = reqwest::Client::new();
        let mut request = client.request(method, url);
        if let Some(body) = content {
            request = request.body(body);
        }
        if let Some(header) = content_type {
            request = request.header(CONTENT_TYPE, header);
        }
        let response = request.send().await.map_err(|err| EngineError {
            message: format!("{fn_name}() request failed: {err}"),
        })?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string());
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                let value = value.to_str().map_err(|err| EngineError {
                    message: format!("{fn_name}() response header invalid: {err}"),
                })?;
                Ok((name.to_string(), value.to_string()))
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        let content = if read_body {
            response.text().await.map_err(|err| EngineError {
                message: format!("{fn_name}() body read failed: {err}"),
            })?
        } else {
            String::new()
        };
        Ok(build_http_response(status, content_type, headers, content))
    }

    #[cfg(target_arch = "wasm32")]
    {
        use wasm_bindgen::JsCast;
        use wasm_bindgen_futures::JsFuture;

        let opts = web_sys::RequestInit::new();
        opts.set_method(method);
        if let Some(body) = content.as_ref() {
            opts.set_body(&wasm_bindgen::JsValue::from_str(body));
        }
        if let Some(header) = content_type.as_ref() {
            let headers = web_sys::Headers::new().map_err(|_| EngineError {
                message: format!("{fn_name}(): failed to create headers"),
            })?;
            headers
                .append("Content-Type", header)
                .map_err(|_| EngineError {
                    message: format!("{fn_name}(): failed to set content type"),
                })?;
            opts.set_headers(headers.as_ref());
        }
        // Use js_sys::global().fetch() so this works in both Window and Web Worker contexts.
        let global = js_sys::global();
        let fetch_fn = js_sys::Reflect::get(&global, &wasm_bindgen::JsValue::from_str("fetch"))
            .map_err(|_| EngineError {
                message: format!("{fn_name}(): fetch is not available"),
            })?;
        let fetch_fn: js_sys::Function = fetch_fn.dyn_into().map_err(|_| EngineError {
            message: format!("{fn_name}(): fetch is not a function"),
        })?;
        let resp_promise = fetch_fn
            .call2(
                &wasm_bindgen::JsValue::undefined(),
                &wasm_bindgen::JsValue::from_str(url),
                &opts,
            )
            .map_err(|_| EngineError {
                message: format!("{fn_name}(): fetch call failed"),
            })?;
        let resp_value = JsFuture::from(js_sys::Promise::from(resp_promise))
            .await
            .map_err(|e| EngineError {
                message: format!(
                    "{fn_name}() request failed: {}",
                    e.as_string().unwrap_or_else(|| "unknown error".to_string())
                ),
            })?;
        let resp: web_sys::Response = resp_value.dyn_into().map_err(|_| EngineError {
            message: format!("{fn_name}(): response was not a Response"),
        })?;
        let status = resp.status();
        let headers = resp.headers();
        let content_type = headers.get("content-type").map_err(|_| EngineError {
            message: format!("{fn_name}(): failed to read response headers"),
        })?;
        let mut header_pairs = Vec::new();
        let entries = js_sys::try_iter(&headers.entries()).map_err(|_| EngineError {
            message: format!("{fn_name}(): failed to read response headers"),
        })?;
        if let Some(entries) = entries {
            for entry in entries {
                let entry = entry.map_err(|_| EngineError {
                    message: format!("{fn_name}(): failed to read response headers"),
                })?;
                let pair = js_sys::Array::from(&entry);
                let field = pair.get(0).as_string().unwrap_or_default();
                let value = pair.get(1).as_string().unwrap_or_default();
                header_pairs.push((field, value));
            }
        }
        let content = if read_body {
            let text_promise = resp.text().map_err(|_| EngineError {
                message: format!("{fn_name}(): failed to read response body"),
            })?;
            JsFuture::from(text_promise)
                .await
                .map_err(|_| EngineError {
                    message: format!("{fn_name}(): failed to read response body"),
                })?
                .as_string()
                .unwrap_or_default()
        } else {
            String::new()
        };
        Ok(build_http_response(
            status,
            content_type,
            header_pairs,
            content,
        ))
    }
}

pub(crate) fn eval_urlencode(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(text) = value else {
        return Err(EngineError {
            message: "urlencode() expects a text argument".to_string(),
        });
    };
    Ok(ScalarValue::Text(url_encode_component(text)))
}

pub(crate) async fn eval_http_get(url_value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_get() expects a text URL argument".to_string(),
        });
    };
    execute_http_request("GET", url, None, None, true, "http_get").await
}

pub(crate) async fn eval_http_get_with_params(
    url_value: &ScalarValue,
    params_value: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) || matches!(params_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_get() expects a text URL argument".to_string(),
        });
    };
    let pairs = parse_json_object_pairs(params_value, "http_get", 2)?;
    let query = url_encode_pairs(&pairs);
    let target = append_query_params(url, &query);
    execute_http_request("GET", &target, None, None, true, "http_get").await
}

pub(crate) async fn eval_http_post_content(
    url_value: &ScalarValue,
    content_value: &ScalarValue,
    content_type_value: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null)
        || matches!(content_value, ScalarValue::Null)
        || matches!(content_type_value, ScalarValue::Null)
    {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_post() expects a text URL argument".to_string(),
        });
    };
    let ScalarValue::Text(content) = content_value else {
        return Err(EngineError {
            message: "http_post() expects a text content argument".to_string(),
        });
    };
    let ScalarValue::Text(content_type) = content_type_value else {
        return Err(EngineError {
            message: "http_post() expects a text content_type argument".to_string(),
        });
    };
    execute_http_request(
        "POST",
        url,
        Some(content.clone()),
        Some(content_type.clone()),
        true,
        "http_post",
    )
    .await
}

pub(crate) async fn eval_http_post_form(
    url_value: &ScalarValue,
    params_value: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) || matches!(params_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_post() expects a text URL argument".to_string(),
        });
    };
    let pairs = parse_json_object_pairs(params_value, "http_post", 2)?;
    let body = url_encode_pairs(&pairs);
    execute_http_request(
        "POST",
        url,
        Some(body),
        Some("application/x-www-form-urlencoded".to_string()),
        true,
        "http_post",
    )
    .await
}

pub(crate) async fn eval_http_put(
    url_value: &ScalarValue,
    content_value: &ScalarValue,
    content_type_value: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null)
        || matches!(content_value, ScalarValue::Null)
        || matches!(content_type_value, ScalarValue::Null)
    {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_put() expects a text URL argument".to_string(),
        });
    };
    let ScalarValue::Text(content) = content_value else {
        return Err(EngineError {
            message: "http_put() expects a text content argument".to_string(),
        });
    };
    let ScalarValue::Text(content_type) = content_type_value else {
        return Err(EngineError {
            message: "http_put() expects a text content_type argument".to_string(),
        });
    };
    execute_http_request(
        "PUT",
        url,
        Some(content.clone()),
        Some(content_type.clone()),
        true,
        "http_put",
    )
    .await
}

pub(crate) async fn eval_http_patch(
    url_value: &ScalarValue,
    content_value: &ScalarValue,
    content_type_value: &ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null)
        || matches!(content_value, ScalarValue::Null)
        || matches!(content_type_value, ScalarValue::Null)
    {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_patch() expects a text URL argument".to_string(),
        });
    };
    let ScalarValue::Text(content) = content_value else {
        return Err(EngineError {
            message: "http_patch() expects a text content argument".to_string(),
        });
    };
    let ScalarValue::Text(content_type) = content_type_value else {
        return Err(EngineError {
            message: "http_patch() expects a text content_type argument".to_string(),
        });
    };
    execute_http_request(
        "PATCH",
        url,
        Some(content.clone()),
        Some(content_type.clone()),
        true,
        "http_patch",
    )
    .await
}

pub(crate) async fn eval_http_delete(url_value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_delete() expects a text URL argument".to_string(),
        });
    };
    execute_http_request("DELETE", url, None, None, true, "http_delete").await
}

pub(crate) async fn eval_http_head(url_value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(url_value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let ScalarValue::Text(url) = url_value else {
        return Err(EngineError {
            message: "http_head() expects a text URL argument".to_string(),
        });
    };
    execute_http_request("HEAD", url, None, None, false, "http_head").await
}
