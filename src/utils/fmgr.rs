use std::cmp::Ordering;
use std::sync::{OnceLock, RwLock};

use serde_json::Value as JsonValue;

use crate::commands::sequence::{
    normalize_sequence_name_from_text, sequence_next_value, set_sequence_value,
    with_sequences_read, with_sequences_write,
};
use crate::security;
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::{EngineError, with_ext_read};
use crate::utils::adt::datetime::{
    JustifyMode, current_date_string, current_timestamp_string, eval_age, eval_date_add_sub,
    eval_date_function, eval_date_trunc, eval_extract_or_date_part, eval_isfinite,
    eval_justify_interval, eval_make_interval, eval_make_time, eval_timestamp_function,
    eval_to_date_with_format, eval_to_timestamp, eval_to_timestamp_with_format,
};
use crate::utils::adt::json::{
    eval_array_to_json, eval_http_delete, eval_http_get, eval_http_get_with_params, eval_http_head,
    eval_http_patch, eval_http_post_content, eval_http_post_form, eval_http_put,
    eval_json_array_length, eval_json_extract_path, eval_json_object, eval_json_pretty,
    eval_json_strip_nulls, eval_json_typeof, eval_jsonb_exists, eval_jsonb_exists_any_all,
    eval_jsonb_concat, eval_jsonb_contained, eval_jsonb_contains, eval_jsonb_delete,
    eval_jsonb_delete_path, eval_jsonb_insert, eval_jsonb_path_exists, eval_jsonb_path_match,
    eval_jsonb_path_query_array, eval_jsonb_path_query_first, eval_jsonb_set, eval_jsonb_set_lax,
    eval_row_to_json, eval_urlencode, json_build_array_value, json_build_object_value,
    scalar_to_json_value,
};
use crate::utils::adt::math_functions::{
    NumericOperand, coerce_to_f64, eval_factorial, eval_scale, eval_width_bucket, gcd_i64,
    numeric_mod, parse_numeric_operand,
};
use crate::utils::adt::misc::{
    array_value_matches, compare_values_for_predicate, count_nonnulls, count_nulls, eval_extremum,
    eval_regexp_count, eval_regexp_instr, eval_regexp_like, eval_regexp_match, eval_regexp_replace,
    eval_regexp_split_to_array, eval_regexp_substr, eval_unistr, gen_random_uuid,
    parse_bool_scalar, parse_f64_numeric_scalar, parse_i64_scalar, parse_pg_array_literal,
    pg_get_viewdef, pg_input_is_valid, quote_ident, quote_literal, quote_nullable, rand_f64,
};
use crate::utils::adt::string_functions::{
    TrimMode, ascii_code, chr_from_code, decode_bytes, encode_bytes, eval_format,
    find_substring_position, initcap_string, left_chars, md5_hex, overlay_text, pad_string,
    right_chars, sha256_hex, substring_chars, substring_regex, substring_similar, trim_text,
};

static LAST_SEQUENCE_VALUE: OnceLock<RwLock<Option<i64>>> = OnceLock::new();

fn with_last_sequence_value_read<T>(f: impl FnOnce(&Option<i64>) -> T) -> T {
    let guard = LAST_SEQUENCE_VALUE
        .get_or_init(|| RwLock::new(None))
        .read()
        .expect("last sequence value lock poisoned for read");
    f(&guard)
}

fn with_last_sequence_value_write<T>(f: impl FnOnce(&mut Option<i64>) -> T) -> T {
    let mut guard = LAST_SEQUENCE_VALUE
        .get_or_init(|| RwLock::new(None))
        .write()
        .expect("last sequence value lock poisoned for write");
    f(&mut guard)
}

fn require_http_extension() -> Result<(), EngineError> {
    if with_ext_read(|ext| ext.extensions.iter().any(|ext| ext.name == "http")) {
        Ok(())
    } else {
        Err(EngineError {
            message: "extension \"http\" is not loaded".to_string(),
        })
    }
}

fn array_values_arg(value: &ScalarValue, message: &str) -> Result<Vec<ScalarValue>, EngineError> {
    match value {
        ScalarValue::Array(values) => Ok(values.clone()),
        ScalarValue::Text(text) => match parse_pg_array_literal(text)? {
            ScalarValue::Array(values) => Ok(values),
            _ => Err(EngineError {
                message: message.to_string(),
            }),
        },
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn normalize_make_interval_args(args: &[ScalarValue]) -> Vec<ScalarValue> {
    let mut normalized = vec![
        ScalarValue::Int(0),
        ScalarValue::Int(0),
        ScalarValue::Int(0),
        ScalarValue::Int(0),
        ScalarValue::Int(0),
        ScalarValue::Int(0),
        ScalarValue::Float(0.0),
    ];
    for (idx, arg) in args.iter().enumerate() {
        normalized[idx] = arg.clone();
    }
    normalized
}

fn build_filled_array(fill: &ScalarValue, lengths: &[usize]) -> ScalarValue {
    if lengths.is_empty() {
        return fill.clone();
    }
    let mut out = Vec::with_capacity(lengths[0]);
    for _ in 0..lengths[0] {
        out.push(build_filled_array(fill, &lengths[1..]));
    }
    ScalarValue::Array(out)
}

fn scalar_cmp_fallback(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    compare_values_for_predicate(left, right).unwrap_or_else(|_| left.render().cmp(&right.render()))
}

fn json_parse_arg(value: &ScalarValue, fn_name: &str) -> Result<JsonValue, EngineError> {
    let text = value.render();
    serde_json::from_str::<JsonValue>(&text).map_err(|err| EngineError {
        message: format!("{fn_name}() argument 1 is not valid JSON: {err}"),
    })
}

fn stable_hash_i64(input: &str) -> i64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as i64
}

pub(crate) async fn eval_scalar_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "http_get" if args.len() == 1 => {
            require_http_extension()?;
            eval_http_get(&args[0]).await
        }
        "http_get" if args.len() == 2 => {
            require_http_extension()?;
            eval_http_get_with_params(&args[0], &args[1]).await
        }
        "http_post" if args.len() == 3 => {
            require_http_extension()?;
            eval_http_post_content(&args[0], &args[1], &args[2]).await
        }
        "http_post" if args.len() == 2 => {
            require_http_extension()?;
            eval_http_post_form(&args[0], &args[1]).await
        }
        "http_put" if args.len() == 3 => {
            require_http_extension()?;
            eval_http_put(&args[0], &args[1], &args[2]).await
        }
        "http_patch" if args.len() == 3 => {
            require_http_extension()?;
            eval_http_patch(&args[0], &args[1], &args[2]).await
        }
        "http_delete" if args.len() == 1 => {
            require_http_extension()?;
            eval_http_delete(&args[0]).await
        }
        "http_head" if args.len() == 1 => {
            require_http_extension()?;
            eval_http_head(&args[0]).await
        }
        "urlencode" if args.len() == 1 => {
            require_http_extension()?;
            eval_urlencode(&args[0])
        }
        "row" => Ok(ScalarValue::Text(
            JsonValue::Array(
                args.iter()
                    .map(scalar_to_json_value)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .to_string(),
        )),
        "to_json" | "to_jsonb" if args.len() == 1 => Ok(ScalarValue::Text(
            scalar_to_json_value(&args[0])?.to_string(),
        )),
        "row_to_json" if args.len() == 1 || args.len() == 2 => eval_row_to_json(args, fn_name),
        "array_to_json" if args.len() == 1 || args.len() == 2 => eval_array_to_json(args, fn_name),
        "json_object" | "jsonb_object" if args.len() == 1 || args.len() == 2 => {
            eval_json_object(args, fn_name)
        }
        "json_build_object" | "jsonb_build_object" if !args.len().is_multiple_of(2) => {
            Err(EngineError {
                message: "argument list must have even number of elements".to_string(),
            })
        }
        "json_build_object" | "jsonb_build_object" => Ok(ScalarValue::Text(
            json_build_object_value(args)?.to_string(),
        )),
        "json_build_array" | "jsonb_build_array" => {
            Ok(ScalarValue::Text(json_build_array_value(args)?.to_string()))
        }
        "json_extract_path" | "jsonb_extract_path" if args.len() >= 2 => {
            eval_json_extract_path(args, false, fn_name)
        }
        "json_extract_path_text" | "jsonb_extract_path_text" if args.len() >= 2 => {
            eval_json_extract_path(args, true, fn_name)
        }
        "json_array_length" | "jsonb_array_length" if args.len() == 1 => {
            eval_json_array_length(&args[0], fn_name)
        }
        "json_typeof" | "jsonb_typeof" if args.len() == 1 => eval_json_typeof(&args[0], fn_name),
        "json_strip_nulls" | "jsonb_strip_nulls" if args.len() == 1 || args.len() == 2 => {
            let strip_in_arrays = if args.len() == 2 {
                parse_bool_scalar(&args[1], "jsonb_strip_nulls() expects boolean second argument")?
            } else {
                false
            };
            eval_json_strip_nulls(&args[0], fn_name, strip_in_arrays)
        }
        "json_pretty" | "jsonb_pretty" if args.len() == 1 => eval_json_pretty(&args[0], fn_name),
        "jsonb_exists" if args.len() == 2 => eval_jsonb_exists(&args[0], &args[1]),
        "jsonb_exists_any" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], true, fn_name)
        }
        "jsonb_exists_all" if args.len() == 2 => {
            eval_jsonb_exists_any_all(&args[0], &args[1], false, fn_name)
        }
        "jsonb_path_exists" if args.len() >= 2 => eval_jsonb_path_exists(args, fn_name),
        "jsonb_path_match" if args.len() >= 2 => eval_jsonb_path_match(args, fn_name),
        "jsonb_path_query" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_path_query_array" if args.len() >= 2 => eval_jsonb_path_query_array(args, fn_name),
        "jsonb_path_query_first" if args.len() >= 2 => eval_jsonb_path_query_first(args, fn_name),
        "jsonb_set" if args.len() == 3 || args.len() == 4 => eval_jsonb_set(args),
        "jsonb_concat" if args.len() == 2 => eval_jsonb_concat(args),
        "jsonb_contains" if args.len() == 2 => eval_jsonb_contains(args),
        "jsonb_contained" if args.len() == 2 => eval_jsonb_contained(args),
        "jsonb_delete" if args.len() == 2 => eval_jsonb_delete(args),
        "jsonb_delete_path" if args.len() == 2 => eval_jsonb_delete_path(args),
        "jsonb_insert" if args.len() == 3 || args.len() == 4 => eval_jsonb_insert(args),
        "jsonb_set_lax" if args.len() >= 3 && args.len() <= 5 => eval_jsonb_set_lax(args),
        "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let value = json_parse_arg(&args[0], fn_name)?;
            let JsonValue::Object(map) = value else {
                return Ok(ScalarValue::Null);
            };
            if let Some((key, val)) = map.iter().next() {
                if fn_name.ends_with("_text") {
                    Ok(ScalarValue::Text(format!("{key}:{val}")))
                } else {
                    Ok(ScalarValue::Text(format!("{key}:{val}")))
                }
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "json_object_keys" | "jsonb_object_keys" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let value = json_parse_arg(&args[0], fn_name)?;
            let JsonValue::Object(map) = value else {
                return Ok(ScalarValue::Null);
            };
            Ok(map
                .keys()
                .next()
                .map_or(ScalarValue::Null, |key| ScalarValue::Text(key.clone())))
        }
        "json_array_elements"
        | "jsonb_array_elements"
        | "json_array_elements_text"
        | "jsonb_array_elements_text"
            if args.len() == 1 =>
        {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let value = json_parse_arg(&args[0], fn_name)?;
            let JsonValue::Array(items) = value else {
                return Ok(ScalarValue::Null);
            };
            if let Some(first) = items.first() {
                if fn_name.ends_with("_text") {
                    Ok(ScalarValue::Text(first.to_string().trim_matches('"').to_string()))
                } else {
                    Ok(ScalarValue::Text(first.to_string()))
                }
            } else {
                Ok(ScalarValue::Null)
            }
        }
        "jsonb_populate_record_valid" if args.len() == 2 => Ok(ScalarValue::Bool(true)),
        "json_populate_record"
        | "json_populate_recordset"
        | "jsonb_populate_record"
        | "jsonb_populate_recordset"
            if args.len() == 2 =>
        {
            Ok(args[1].clone())
        }
        "to_tsvector" | "json_to_tsvector" | "jsonb_to_tsvector" if !args.is_empty() => {
            Ok(ScalarValue::Text(
            args.last().map_or_else(String::new, ScalarValue::render),
            ))
        }
        "tsquery" if args.len() == 1 => Ok(args[0].clone()),
        "ts_headline" if args.len() >= 2 => Ok(args[1].clone()),
        "nextval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "nextval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{sequence_name}\" does not exist"),
                    });
                };
                let value = sequence_next_value(state, &sequence_name)?;
                with_last_sequence_value_write(|last| *last = Some(value));
                Ok(ScalarValue::Int(value))
            })
        }
        "currval" if args.len() == 1 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "currval() expects text sequence name".to_string(),
                    });
                }
            };
            with_sequences_read(|sequences| {
                let Some(state) = sequences.get(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{sequence_name}\" does not exist"),
                    });
                };
                if !state.called {
                    return Err(EngineError {
                        message: format!(
                            "currval of sequence \"{sequence_name}\" is not yet defined"
                        ),
                    });
                }
                Ok(ScalarValue::Int(state.current))
            })
        }
        "lastval" if args.is_empty() => with_last_sequence_value_read(|last| {
            let Some(value) = last else {
                return Err(EngineError {
                    message: "lastval is not yet defined in this session".to_string(),
                });
            };
            Ok(ScalarValue::Int(*value))
        }),
        "setval" if args.len() == 2 || args.len() == 3 => {
            let sequence_name = match &args[0] {
                ScalarValue::Text(v) => normalize_sequence_name_from_text(v)?,
                _ => {
                    return Err(EngineError {
                        message: "setval() expects text sequence name".to_string(),
                    });
                }
            };
            let value = parse_i64_scalar(&args[1], "setval() expects integer value")?;
            let is_called = if args.len() == 3 {
                parse_bool_scalar(&args[2], "setval() expects boolean third argument")?
            } else {
                true
            };
            with_sequences_write(|sequences| {
                let Some(state) = sequences.get_mut(&sequence_name) else {
                    return Err(EngineError {
                        message: format!("sequence \"{sequence_name}\" does not exist"),
                    });
                };
                set_sequence_value(state, &sequence_name, value, is_called)?;
                Ok(ScalarValue::Int(value))
            })
        }
        "lower" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_lowercase()))
        }
        "upper" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().to_ascii_uppercase()))
        }
        "length" | "char_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().chars().count() as i64))
        }
        "abs" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            _ => match parse_numeric_operand(&args[0])? {
                NumericOperand::Int(i) => {
                    let abs_value = i.checked_abs().ok_or_else(|| EngineError {
                        message: "bigint out of range".to_string(),
                    })?;
                    Ok(ScalarValue::Int(abs_value))
                }
                NumericOperand::Float(f) => Ok(ScalarValue::Float(f.abs())),
                NumericOperand::Numeric(d) => Ok(ScalarValue::Numeric(d.abs())),
            },
        },
        "nullif" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            if matches!(args[1], ScalarValue::Null) {
                return Ok(args[0].clone());
            }
            if compare_values_for_predicate(&args[0], &args[1])? == Ordering::Equal {
                Ok(ScalarValue::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "greatest" if !args.is_empty() => eval_extremum(args, true),
        "least" if !args.is_empty() => eval_extremum(args, false),
        "concat" => {
            let mut out = String::new();
            for arg in args {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                out.push_str(&arg.render());
            }
            Ok(ScalarValue::Text(out))
        }
        "concat_ws" if !args.is_empty() => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let separator = args[0].render();
            let mut parts = Vec::new();
            for arg in &args[1..] {
                if matches!(arg, ScalarValue::Null) {
                    continue;
                }
                parts.push(arg.render());
            }
            Ok(ScalarValue::Text(parts.join(&separator)))
        }
        "substring" | "substr" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            if let Ok(start) = parse_i64_scalar(&args[1], "substring() expects integer start index")
            {
                let length = if args.len() == 3 {
                    Some(parse_i64_scalar(
                        &args[2],
                        "substring() expects integer length",
                    )?)
                } else {
                    None
                };
                return Ok(ScalarValue::Text(substring_chars(&input, start, length)?));
            }

            let pattern = args[1].render();
            if args.len() == 2 {
                return Ok(match substring_regex(&input, &pattern)? {
                    Some(value) => ScalarValue::Text(value),
                    None => ScalarValue::Null,
                });
            }

            let escape = args[2].render();
            Ok(match substring_similar(&input, &pattern, &escape)? {
                Some(value) => ScalarValue::Text(value),
                None => ScalarValue::Null,
            })
        }
        "position" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let needle = args[0].render();
            let haystack = args[1].render();
            Ok(ScalarValue::Int(find_substring_position(
                &haystack, &needle,
            )))
        }
        "overlay" if args.len() == 3 || args.len() == 4 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let replacement = args[1].render();
            let start = parse_i64_scalar(&args[2], "overlay() expects integer start")?;
            let count = if args.len() == 4 {
                Some(parse_i64_scalar(
                    &args[3],
                    "overlay() expects integer count",
                )?)
            } else {
                None
            };
            Ok(ScalarValue::Text(overlay_text(
                &input,
                &replacement,
                start,
                count,
            )?))
        }
        "left" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "left() expects integer length")?;
            Ok(ScalarValue::Text(left_chars(&input, count)))
        }
        "right" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let count = parse_i64_scalar(&args[1], "right() expects integer length")?;
            Ok(ScalarValue::Text(right_chars(&input, count)))
        }
        "btrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Both,
            )))
        }
        "ltrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Left,
            )))
        }
        "rtrim" if args.len() == 1 || args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let trim_chars = args.get(1).map(ScalarValue::render);
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                TrimMode::Right,
            )))
        }
        "trim" if args.len() == 1 || args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let (mode, trim_chars, input) = match args.len() {
                1 => (TrimMode::Both, None, args[0].render()),
                2 => {
                    let first = args[0].render();
                    if first.eq_ignore_ascii_case("leading")
                        || first.eq_ignore_ascii_case("trailing")
                        || first.eq_ignore_ascii_case("both")
                    {
                        let mode = if first.eq_ignore_ascii_case("leading") {
                            TrimMode::Left
                        } else if first.eq_ignore_ascii_case("trailing") {
                            TrimMode::Right
                        } else {
                            TrimMode::Both
                        };
                        (mode, None, args[1].render())
                    } else {
                        (TrimMode::Both, Some(first), args[1].render())
                    }
                }
                3 => {
                    let mode_text = args[0].render();
                    let mode = if mode_text.eq_ignore_ascii_case("leading") {
                        TrimMode::Left
                    } else if mode_text.eq_ignore_ascii_case("trailing") {
                        TrimMode::Right
                    } else {
                        TrimMode::Both
                    };
                    (mode, Some(args[1].render()), args[2].render())
                }
                _ => unreachable!("trim() arity checked in match guard"),
            };
            Ok(ScalarValue::Text(trim_text(
                &input,
                trim_chars.as_deref(),
                mode,
            )))
        }
        "replace" if args.len() == 3 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let from = args[1].render();
            let to = args[2].render();
            Ok(ScalarValue::Text(input.replace(&from, &to)))
        }
        "ascii" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(ascii_code(&args[0].render())))
        }
        "chr" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let code = parse_i64_scalar(&args[0], "chr() expects integer")?;
            Ok(ScalarValue::Text(chr_from_code(code)?))
        }
        "encode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let data = args[0].render();
            let format = args[1].render();
            Ok(ScalarValue::Text(encode_bytes(data.as_bytes(), &format)?))
        }
        "decode" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let format = args[1].render();
            let decoded = decode_bytes(&input, &format)?;
            Ok(ScalarValue::Text(
                String::from_utf8_lossy(&decoded).to_string(),
            ))
        }
        "date" if args.len() == 1 => eval_date_function(&args[0]),
        "timestamp" if args.len() == 1 => eval_timestamp_function(&args[0]),
        "now" | "current_timestamp" if args.is_empty() => {
            Ok(ScalarValue::Text(current_timestamp_string()?))
        }
        "clock_timestamp" if args.is_empty() => Ok(ScalarValue::Text(current_timestamp_string()?)),
        "pg_sleep" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let seconds = parse_f64_numeric_scalar(&args[0], "pg_sleep() expects numeric seconds")?;
            if !seconds.is_finite() {
                return Err(EngineError {
                    message: "pg_sleep() expects finite numeric seconds".to_string(),
                });
            }
            if seconds > 0.0 {
                let sleep_for = std::time::Duration::from_secs_f64(seconds.min(60.0));
                tokio::time::sleep(sleep_for).await;
            }
            Ok(ScalarValue::Null)
        }
        "current_date" if args.is_empty() => Ok(ScalarValue::Text(current_date_string()?)),
        "age" if args.len() == 1 || args.len() == 2 => eval_age(args),
        "extract" | "date_part" if args.len() == 2 => eval_extract_or_date_part(&args[0], &args[1]),
        "date_trunc" if args.len() == 2 => eval_date_trunc(&args[0], &args[1]),
        "date_add" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], true),
        "date_sub" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], false),
        "to_timestamp" if args.len() == 1 => eval_to_timestamp(&args[0]),
        "to_timestamp" if args.len() == 2 => eval_to_timestamp_with_format(&args[0], &args[1]),
        "to_date" if args.len() == 2 => eval_to_date_with_format(&args[0], &args[1]),
        "make_interval" if args.len() <= 7 => {
            let normalized = normalize_make_interval_args(args);
            eval_make_interval(&normalized)
        }
        "justify_hours" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Hours),
        "justify_days" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Days),
        "justify_interval" if args.len() == 1 => eval_justify_interval(&args[0], JustifyMode::Full),
        "isfinite" if args.len() == 1 => eval_isfinite(&args[0]),
        "timezone" if args.len() == 2 => {
            // timezone(zone, timestamp) — simplified: just return the timestamp as-is
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            Ok(args[1].clone())
        }
        "coalesce" if !args.is_empty() => {
            for value in args {
                if !matches!(value, ScalarValue::Null) {
                    return Ok(value.clone());
                }
            }
            Ok(ScalarValue::Null)
        }
        // --- Math functions ---
        "ceil" | "ceiling" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.ceil())),
            ScalarValue::Numeric(d) => Ok(ScalarValue::Numeric(d.ceil())),
            _ => Err(EngineError {
                message: "ceil() expects numeric argument".to_string(),
            }),
        },
        "floor" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.floor())),
            ScalarValue::Numeric(d) => Ok(ScalarValue::Numeric(d.floor())),
            _ => Err(EngineError {
                message: "floor() expects numeric argument".to_string(),
            }),
        },
        "round" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "round() expects integer scale")?
            } else {
                0
            };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).round() / factor))
                }
                ScalarValue::Numeric(d) => Ok(ScalarValue::Numeric(d.round_dp(scale as u32))),
                _ => Err(EngineError {
                    message: "round() expects numeric argument".to_string(),
                }),
            }
        }
        "trunc" | "truncate" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let scale = if args.len() == 2 {
                parse_i64_scalar(&args[1], "trunc() expects integer scale")?
            } else {
                0
            };
            match &args[0] {
                ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
                ScalarValue::Float(f) => {
                    let factor = 10f64.powi(scale as i32);
                    Ok(ScalarValue::Float((f * factor).trunc() / factor))
                }
                ScalarValue::Numeric(d) => {
                    Ok(ScalarValue::Numeric(d.trunc_with_scale(scale as u32)))
                }
                _ => Err(EngineError {
                    message: "trunc() expects numeric argument".to_string(),
                }),
            }
        }
        "power" | "pow" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let base = coerce_to_f64(&args[0], "power()")?;
            let exp = coerce_to_f64(&args[1], "power()")?;
            Ok(ScalarValue::Float(base.powf(exp)))
        }
        "sqrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "sqrt()")?;
            Ok(ScalarValue::Float(v.sqrt()))
        }
        "cbrt" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "cbrt()")?;
            Ok(ScalarValue::Float(v.cbrt()))
        }
        "exp" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "exp()")?;
            Ok(ScalarValue::Float(v.exp()))
        }
        "ln" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "ln()")?;
            Ok(ScalarValue::Float(v.ln()))
        }
        "log" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "log()")?;
            Ok(ScalarValue::Float(v.log10()))
        }
        "log10" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "log10()")?;
            Ok(ScalarValue::Float(v.log10()))
        }
        "log" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let base = coerce_to_f64(&args[0], "log()")?;
            let v = coerce_to_f64(&args[1], "log()")?;
            Ok(ScalarValue::Float(v.log(base)))
        }
        "sin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "sin()")?.sin()))
        }
        "cos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "cos()")?.cos()))
        }
        "tan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "tan()")?.tan()))
        }
        "asin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "asin()")?.asin(),
            ))
        }
        "acos" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "acos()")?.acos(),
            ))
        }
        "atan" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "atan()")?.atan(),
            ))
        }
        "atan2" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let y = coerce_to_f64(&args[0], "atan2()")?;
            let x = coerce_to_f64(&args[1], "atan2()")?;
            Ok(ScalarValue::Float(y.atan2(x)))
        }
        "degrees" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "degrees()")?.to_degrees(),
            ))
        }
        "radians" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "radians()")?.to_radians(),
            ))
        }
        "sign" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.signum())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(if *f > 0.0 {
                1.0
            } else if *f < 0.0 {
                -1.0
            } else {
                0.0
            })),
            _ => Err(EngineError {
                message: "sign() expects numeric argument".to_string(),
            }),
        },
        "numeric_inc" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            match parse_numeric_operand(&args[0])? {
                NumericOperand::Int(v) => Ok(ScalarValue::Int(v.checked_add(1).ok_or_else(
                    || EngineError {
                        message: "bigint out of range".to_string(),
                    },
                )?)),
                NumericOperand::Float(v) => Ok(ScalarValue::Float(v + 1.0)),
                NumericOperand::Numeric(v) => Ok(ScalarValue::Numeric(v + rust_decimal::Decimal::ONE)),
            }
        }
        "numeric_dec" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            match parse_numeric_operand(&args[0])? {
                NumericOperand::Int(v) => Ok(ScalarValue::Int(v.checked_sub(1).ok_or_else(
                    || EngineError {
                        message: "bigint out of range".to_string(),
                    },
                )?)),
                NumericOperand::Float(v) => Ok(ScalarValue::Float(v - 1.0)),
                NumericOperand::Numeric(v) => Ok(ScalarValue::Numeric(v - rust_decimal::Decimal::ONE)),
            }
        }
        "width_bucket" if args.len() == 4 => eval_width_bucket(args),
        "width_bucket" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let thresholds = array_values_arg(
                &args[1],
                "width_bucket() expects array thresholds as second argument",
            )?;
            if thresholds.is_empty() {
                return Err(EngineError {
                    message: "width_bucket() requires non-empty thresholds array".to_string(),
                });
            }
            if thresholds.iter().any(|v| matches!(v, ScalarValue::Null)) {
                return Err(EngineError {
                    message: "width_bucket() thresholds cannot contain NULL".to_string(),
                });
            }
            let operand = &args[0];
            let first_cmp = compare_values_for_predicate(operand, &thresholds[0])?;
            if first_cmp == Ordering::Less {
                return Ok(ScalarValue::Int(0));
            }
            for (idx, threshold) in thresholds.iter().enumerate().skip(1) {
                let cmp = compare_values_for_predicate(operand, threshold)?;
                if cmp == Ordering::Less {
                    return Ok(ScalarValue::Int(idx as i64));
                }
            }
            Ok(ScalarValue::Int(thresholds.len() as i64))
        }
        "scale" if args.len() == 1 => eval_scale(&args[0]),
        "factorial" if args.len() == 1 => eval_factorial(&args[0]),
        // Hyperbolic functions
        "sinh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "sinh()")?.sinh(),
            ))
        }
        "cosh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "cosh()")?.cosh(),
            ))
        }
        "tanh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "tanh()")?.tanh(),
            ))
        }
        "asinh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Float(
                coerce_to_f64(&args[0], "asinh()")?.asinh(),
            ))
        }
        "acosh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "acosh()")?;
            if v < 1.0 && !v.is_nan() {
                return Err(EngineError {
                    message: "input is out of range".to_string(),
                });
            }
            Ok(ScalarValue::Float(v.acosh()))
        }
        "atanh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "atanh()")?;
            if !(-1.0..=1.0).contains(&v) && !v.is_nan() {
                return Err(EngineError {
                    message: "input is out of range".to_string(),
                });
            }
            Ok(ScalarValue::Float(v.atanh()))
        }
        // Degree-based trig functions
        "sind" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "sind()")?;
            Ok(ScalarValue::Float(sind(v)))
        }
        "cosd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "cosd()")?;
            Ok(ScalarValue::Float(cosd(v)))
        }
        "tand" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "tand()")?;
            Ok(ScalarValue::Float(tand(v)))
        }
        "cotd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "cotd()")?;
            let t = tand(v);
            if t == 0.0 {
                return Err(EngineError {
                    message: "division by zero".to_string(),
                });
            }
            Ok(ScalarValue::Float(1.0 / t))
        }
        "asind" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "asind()")?;
            if !(-1.0..=1.0).contains(&v) {
                return Err(EngineError {
                    message: "input is out of range".to_string(),
                });
            }
            Ok(ScalarValue::Float(v.asin().to_degrees()))
        }
        "acosd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "acosd()")?;
            if !(-1.0..=1.0).contains(&v) {
                return Err(EngineError {
                    message: "input is out of range".to_string(),
                });
            }
            Ok(ScalarValue::Float(v.acos().to_degrees()))
        }
        "atand" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = coerce_to_f64(&args[0], "atand()")?;
            Ok(ScalarValue::Float(v.atan().to_degrees()))
        }
        "atan2d" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let y = coerce_to_f64(&args[0], "atan2d()")?;
            let x = coerce_to_f64(&args[1], "atan2d()")?;
            Ok(ScalarValue::Float(y.atan2(x).to_degrees()))
        }
        "pi" if args.is_empty() => Ok(ScalarValue::Float(std::f64::consts::PI)),
        "random" if args.is_empty() => Ok(ScalarValue::Float(rand_f64())),
        "gen_random_uuid" if args.is_empty() => Ok(ScalarValue::Text(gen_random_uuid())),
        "mod" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            numeric_mod(args[0].clone(), args[1].clone())
        }
        "div" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let a = coerce_to_f64(&args[0], "div()")?;
            let b = coerce_to_f64(&args[1], "div()")?;
            if b == 0.0 {
                return Err(EngineError {
                    message: "division by zero".to_string(),
                });
            }
            Ok(ScalarValue::Int((a / b).trunc() as i64))
        }
        "gcd" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let a = parse_i64_scalar(&args[0], "gcd() expects integer")?;
            let b = parse_i64_scalar(&args[1], "gcd() expects integer")?;
            Ok(ScalarValue::Int(gcd_i64(a, b)?))
        }
        "lcm" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let a = parse_i64_scalar(&args[0], "lcm() expects integer")?;
            let b = parse_i64_scalar(&args[1], "lcm() expects integer")?;
            let g = gcd_i64(a, b)?;
            if g == 0 {
                return Ok(ScalarValue::Int(0));
            }
            let product = i128::from(a / g) * i128::from(b);
            let abs_product = product.abs();
            let value = i64::try_from(abs_product).map_err(|_| EngineError {
                message: "bigint out of range".to_string(),
            })?;
            Ok(ScalarValue::Int(value))
        }
        // --- Additional string functions ---
        "initcap" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(initcap_string(&args[0].render())))
        }
        "repeat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let s = args[0].render();
            let n = parse_i64_scalar(&args[1], "repeat() expects integer count")?;
            if n < 0 {
                return Ok(ScalarValue::Text(String::new()));
            }
            let n_usize = usize::try_from(n).map_err(|_| EngineError {
                message: "repeat() count is too large".to_string(),
            })?;
            let _ = s.len().checked_mul(n_usize).ok_or_else(|| EngineError {
                message: "repeat() result is too large".to_string(),
            })?;
            Ok(ScalarValue::Text(s.repeat(n_usize)))
        }
        "reverse" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(args[0].render().chars().rev().collect()))
        }
        "translate" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let from: Vec<char> = args[1].render().chars().collect();
            let to: Vec<char> = args[2].render().chars().collect();
            let result: String = input
                .chars()
                .filter_map(|c| {
                    if let Some(pos) = from.iter().position(|f| *f == c) {
                        to.get(pos).copied()
                    } else {
                        Some(c)
                    }
                })
                .collect();
            Ok(ScalarValue::Text(result))
        }
        "split_part" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let delimiter = args[1].render();
            let field = parse_i64_scalar(&args[2], "split_part() expects integer field")?;
            if field == 0 {
                return Err(EngineError {
                    message: "field position must be greater than zero".to_string(),
                });
            }
            if delimiter.is_empty() {
                return Ok(ScalarValue::Text(if field == 1 || field == -1 {
                    input
                } else {
                    String::new()
                }));
            }
            let parts: Vec<&str> = input.split(&delimiter).collect();
            let idx = if field > 0 {
                Some((field - 1) as usize)
            } else {
                let back = field.unsigned_abs() as usize;
                if back == 0 || back > parts.len() {
                    None
                } else {
                    Some(parts.len() - back)
                }
            };
            Ok(ScalarValue::Text(
                idx.and_then(|i| parts.get(i).copied()).unwrap_or("").to_string(),
            ))
        }
        "strpos" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let haystack = args[0].render();
            let needle = args[1].render();
            Ok(ScalarValue::Int(
                haystack.find(&needle).map(|i| i as i64 + 1).unwrap_or(0),
            ))
        }
        "lpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "lpad() expects integer length")?;
            if len <= 0 {
                return Ok(ScalarValue::Text(String::new()));
            }
            let fill = if args.len() == 3 {
                args[2].render()
            } else {
                " ".to_string()
            };
            let len = usize::try_from(len).map_err(|_| EngineError {
                message: "lpad() length is too large".to_string(),
            })?;
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, true)))
        }
        "rpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "rpad() expects integer length")?;
            if len <= 0 {
                return Ok(ScalarValue::Text(String::new()));
            }
            let fill = if args.len() == 3 {
                args[2].render()
            } else {
                " ".to_string()
            };
            let len = usize::try_from(len).map_err(|_| EngineError {
                message: "rpad() length is too large".to_string(),
            })?;
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, false)))
        }
        "quote_literal" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(quote_literal(&args[0].render())))
        }
        "quote_ident" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(quote_ident(&args[0].render())))
        }
        "format" if !args.is_empty() => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let format_str = args[0].render();
            let format_args = &args[1..];
            Ok(ScalarValue::Text(eval_format(&format_str, format_args)?))
        }
        "quote_nullable" if args.len() == 1 => Ok(ScalarValue::Text(quote_nullable(&args[0]))),
        "md5" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(md5_hex(&args[0].render())))
        }
        "sha256" | "digest" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(sha256_hex(&args[0].render())))
        }
        "regexp_match" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            eval_regexp_match(&args[0].render(), &args[1].render(), "")
        }
        "regexp_match" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            eval_regexp_match(&args[0].render(), &args[1].render(), &args[2].render())
        }
        "regexp_replace" if args.len() >= 3 && args.len() <= 6 => {
            if args.iter().take(3).any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let source = args[0].render();
            let pattern = args[1].render();
            let replacement = args[2].render();
            let mut start: i64 = 1;
            let mut occurrence: i64 = 1;
            let mut flags = String::new();
            match args.len() {
                3 => {}
                4 => {
                    if let Ok(parsed_start) =
                        parse_i64_scalar(&args[3], "regexp_replace() start argument")
                    {
                        start = parsed_start;
                    } else {
                        flags = args[3].render();
                    }
                }
                5 => {
                    start = parse_i64_scalar(&args[3], "regexp_replace() start argument")?;
                    occurrence = parse_i64_scalar(&args[4], "regexp_replace() N argument")?;
                }
                6 => {
                    start = parse_i64_scalar(&args[3], "regexp_replace() start argument")?;
                    occurrence = parse_i64_scalar(&args[4], "regexp_replace() N argument")?;
                    flags = args[5].render();
                }
                _ => {}
            }
            eval_regexp_replace(&source, &pattern, &replacement, start, occurrence, &flags)
        }
        "regexp_split_to_array" if args.len() == 2 || args.len() == 3 => {
            if args.iter().take(2).any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let flags = if args.len() == 3 {
                args[2].render()
            } else {
                String::new()
            };
            eval_regexp_split_to_array(&args[0].render(), &args[1].render(), &flags)
        }
        "regexp_count" if args.len() >= 2 && args.len() <= 4 => eval_regexp_count(args),
        "regexp_instr" if args.len() >= 2 && args.len() <= 7 => eval_regexp_instr(args),
        "regexp_substr" if args.len() >= 2 && args.len() <= 6 => eval_regexp_substr(args),
        "regexp_like" if args.len() >= 2 && args.len() <= 3 => eval_regexp_like(args),
        "to_hex" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_hex() expects integer")?;
            Ok(ScalarValue::Text(format!("{v:x}")))
        }
        "to_oct" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_oct() expects integer")?;
            Ok(ScalarValue::Text(format!("{v:o}")))
        }
        "to_bin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_bin() expects integer")?;
            Ok(ScalarValue::Text(format!("{v:b}")))
        }
        "unistr" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            eval_unistr(&args[0].render())
        }
        "starts_with" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let text = args[0].render();
            let prefix = args[1].render();
            Ok(ScalarValue::Bool(text.starts_with(&prefix)))
        }
        "octet_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().len() as i64))
        }
        "character_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().chars().count() as i64))
        }
        "bit_length" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(args[0].render().len() as i64 * 8))
        }
        "set_byte" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let mut bytes = args[0].render().into_bytes();
            let offset = parse_i64_scalar(&args[1], "set_byte() offset")? as usize;
            let new_val = parse_i64_scalar(&args[2], "set_byte() value")? as u8;
            if offset >= bytes.len() {
                return Err(EngineError {
                    message: "index out of range".to_string(),
                });
            }
            bytes[offset] = new_val;
            Ok(ScalarValue::Text(
                String::from_utf8_lossy(&bytes).to_string(),
            ))
        }
        "get_byte" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let bytes = args[0].render().into_bytes();
            let offset = parse_i64_scalar(&args[1], "get_byte() offset")? as usize;
            if offset >= bytes.len() {
                return Err(EngineError {
                    message: "index out of range".to_string(),
                });
            }
            Ok(ScalarValue::Int(bytes[offset] as i64))
        }
        "num_nulls" => Ok(ScalarValue::Int(count_nulls(args) as i64)),
        "num_nonnulls" => Ok(ScalarValue::Int(count_nonnulls(args) as i64)),
        // Boolean comparison functions (PostgreSQL compatibility)
        "booleq" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "booleq() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "booleq() expects boolean arguments")?;
            Ok(ScalarValue::Bool(a == b))
        }
        "boolne" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "boolne() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "boolne() expects boolean arguments")?;
            Ok(ScalarValue::Bool(a != b))
        }
        "boollt" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "boollt() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "boollt() expects boolean arguments")?;
            Ok(ScalarValue::Bool(!a && b))
        }
        "boolgt" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "boolgt() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "boolgt() expects boolean arguments")?;
            Ok(ScalarValue::Bool(a && !b))
        }
        "boolle" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "boolle() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "boolle() expects boolean arguments")?;
            Ok(ScalarValue::Bool(a <= b))
        }
        "boolge" if args.len() == 2 => {
            let a = parse_bool_scalar(&args[0], "boolge() expects boolean arguments")?;
            let b = parse_bool_scalar(&args[1], "boolge() expects boolean arguments")?;
            Ok(ScalarValue::Bool(a >= b))
        }
        // --- System info functions ---
        "version" if args.is_empty() => {
            Ok(ScalarValue::Text("OpenAssay 0.1.0 on Rust".to_string()))
        }
        "current_database" if args.is_empty() => Ok(ScalarValue::Text("openassay".to_string())),
        "current_schema" if args.is_empty() => Ok(ScalarValue::Text("public".to_string())),
        "current_user" | "session_user" | "user" if args.is_empty() => {
            let role = security::current_role();
            Ok(ScalarValue::Text(role))
        }
        "pg_backend_pid" if args.is_empty() => Ok(ScalarValue::Int(std::process::id() as i64)),
        "pg_typeof" if args.len() == 1 => {
            let type_name = match &args[0] {
                ScalarValue::Null => "unknown",
                ScalarValue::Bool(_) => "boolean",
                ScalarValue::Int(_) => "bigint",
                ScalarValue::Float(_) => "double precision",
                ScalarValue::Numeric(_) => "numeric",
                ScalarValue::Text(_) => "text",
                ScalarValue::Array(_) => "text[]",
                ScalarValue::Record(_) => "record",
                ScalarValue::Vector(_) => "vector",
            };
            Ok(ScalarValue::Text(type_name.to_string()))
        }
        "pg_input_is_valid" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let type_name = args[1].render();
            let is_valid = pg_input_is_valid(&input, &type_name)?;
            Ok(ScalarValue::Bool(is_valid))
        }
        "pg_column_size" if args.len() == 1 => {
            let size = match &args[0] {
                ScalarValue::Null => 0i64,
                ScalarValue::Bool(_) => 1,
                ScalarValue::Int(_) => 8,
                ScalarValue::Float(_) => 8,
                ScalarValue::Numeric(_) => 16, // Variable size, but 16 is a reasonable estimate
                ScalarValue::Text(s) => s.len() as i64 + 4, // 4-byte length prefix
                ScalarValue::Array(a) => {
                    let mut total = 20i64; // array header
                    for v in a {
                        total += match v {
                            ScalarValue::Null => 0,
                            ScalarValue::Bool(_) => 1,
                            ScalarValue::Int(_) => 8,
                            ScalarValue::Float(_) => 8,
                            ScalarValue::Numeric(_) => 16,
                            ScalarValue::Text(s) => s.len() as i64 + 4,
                            ScalarValue::Array(_) => 8, // rough estimate
                            ScalarValue::Record(r) => (r.len() * 8) as i64,
                            ScalarValue::Vector(v) => (v.len() as i64) * 4 + 4,
                        };
                    }
                    total
                }
                ScalarValue::Record(r) => (r.len() as i64) * 8 + 4,
                ScalarValue::Vector(v) => (v.len() as i64) * 4 + 4,
            };
            Ok(ScalarValue::Int(size))
        }
        "pg_relation_size" if args.len() == 1 || args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(0))
        }
        "pg_total_relation_size" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(0))
        }
        "pg_size_pretty" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let bytes = parse_i64_scalar(&args[0], "pg_size_pretty() expects integer argument")?;
            Ok(ScalarValue::Text(format!("{bytes} bytes")))
        }
        "pg_get_userbyid" if args.len() == 1 => Ok(ScalarValue::Text("openassay".to_string())),
        "pg_get_viewdef" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let view_name = args[0].render();
            Ok(ScalarValue::Text(pg_get_viewdef(&view_name, false)?))
        }
        "pg_get_viewdef" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let view_name = args[0].render();
            let pretty = parse_bool_scalar(&args[1], "pg_get_viewdef() pretty")?;
            Ok(ScalarValue::Text(pg_get_viewdef(&view_name, pretty)?))
        }
        "has_table_privilege" if args.len() == 2 || args.len() == 3 => Ok(ScalarValue::Bool(true)),
        "has_column_privilege" if args.len() == 3 || args.len() == 4 => Ok(ScalarValue::Bool(true)),
        "has_schema_privilege" if args.len() == 2 || args.len() == 3 => Ok(ScalarValue::Bool(true)),
        "pg_get_expr" if args.len() == 2 || args.len() == 3 => Ok(ScalarValue::Null),
        "pg_table_is_visible" if args.len() == 1 => Ok(ScalarValue::Bool(true)),
        "pg_type_is_visible" if args.len() == 1 => Ok(ScalarValue::Bool(true)),
        "obj_description" | "col_description" | "shobj_description" if !args.is_empty() => {
            Ok(ScalarValue::Null)
        }
        "format_type" if args.len() == 2 => Ok(ScalarValue::Text("unknown".to_string())),
        "pg_catalog.format_type" if args.len() == 2 => Ok(ScalarValue::Text("unknown".to_string())),
        // --- Make date/time ---
        "make_date" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let y = parse_i64_scalar(&args[0], "make_date() year")? as i32;
            let m = parse_i64_scalar(&args[1], "make_date() month")? as u32;
            let d = parse_i64_scalar(&args[2], "make_date() day")? as u32;
            Ok(ScalarValue::Text(format!("{y:04}-{m:02}-{d:02}")))
        }
        "make_time" if args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let hour = parse_i64_scalar(&args[0], "make_time() hour")?;
            let minute = parse_i64_scalar(&args[1], "make_time() minute")?;
            let second = coerce_to_f64(&args[2], "make_time() second")?;
            eval_make_time(hour, minute, second)
        }
        "make_timestamp" if args.len() == 6 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let y = parse_i64_scalar(&args[0], "year")? as i32;
            let mo = parse_i64_scalar(&args[1], "month")? as u32;
            let d = parse_i64_scalar(&args[2], "day")? as u32;
            let h = parse_i64_scalar(&args[3], "hour")? as u32;
            let mi = parse_i64_scalar(&args[4], "min")? as u32;
            let s = coerce_to_f64(&args[5], "sec")?;
            let sec = s.trunc() as u32;
            let frac = ((s - s.trunc()) * 1_000_000.0).round() as u32;
            if frac == 0 {
                Ok(ScalarValue::Text(format!(
                    "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec:02}"
                )))
            } else {
                Ok(ScalarValue::Text(format!(
                    "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{sec:02}.{frac:06}"
                )))
            }
        }
        "to_char" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            // Simplified: just return the first arg rendered
            Ok(ScalarValue::Text(args[0].render()))
        }
        "to_number" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let s = args[0].render();
            let cleaned: String = s
                .chars()
                .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            match cleaned.parse::<f64>() {
                Ok(v) => Ok(ScalarValue::Float(v)),
                Err(_) => Err(EngineError {
                    message: format!("invalid input for to_number: {s}"),
                }),
            }
        }
        "int4range" | "float8range" | "textrange" if args.len() == 2 || args.len() == 3 => {
            eval_range_constructor(args)
        }
        "array_append" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values =
                array_values_arg(&args[0], "array_append() expects array as first argument")?;
            values.push(args[1].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_prepend" if args.len() == 2 => {
            if matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values =
                array_values_arg(&args[1], "array_prepend() expects array as second argument")?;
            values.insert(0, args[0].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_cat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let mut left = array_values_arg(&args[0], "array_cat() expects array arguments")?;
            let right = array_values_arg(&args[1], "array_cat() expects array arguments")?;
            left.extend(right);
            Ok(ScalarValue::Array(left))
        }
        "array_remove" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_remove() expects array as first argument")?;
            let mut out = Vec::with_capacity(values.len());
            for value in &values {
                if !array_value_matches(&args[1], value)? {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_replace" if args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_replace() expects array as first argument")?;
            let mut out = Vec::with_capacity(values.len());
            for value in &values {
                if array_value_matches(&args[1], value)? {
                    out.push(args[2].clone());
                } else {
                    out.push(value.clone());
                }
            }
            Ok(ScalarValue::Array(out))
        }
        "array_position" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_position() expects array as first argument")?;
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    return Ok(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Null)
        }
        "array_positions" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values = array_values_arg(
                &args[0],
                "array_positions() expects array as first argument",
            )?;
            let mut positions = Vec::new();
            for (idx, value) in values.iter().enumerate() {
                if array_value_matches(&args[1], value)? {
                    positions.push(ScalarValue::Int((idx + 1) as i64));
                }
            }
            Ok(ScalarValue::Array(positions))
        }
        "array_length" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_length() expects array as first argument")?;
            let dim = parse_i64_scalar(&args[1], "array_length() expects integer dimension")?;
            if dim != 1 {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_dims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values = array_values_arg(&args[0], "array_dims() expects array argument")?;
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(format!("[1:{}]", values.len())))
        }
        "array_ndims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let _ = array_values_arg(&args[0], "array_ndims() expects array argument")?;
            Ok(ScalarValue::Int(1))
        }
        "array_fill" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let lengths = array_values_arg(&args[1], "array_fill() expects array of lengths")?;
            if lengths.is_empty() {
                return Ok(ScalarValue::Array(Vec::new()));
            }
            let mut parsed_lengths = Vec::with_capacity(lengths.len());
            for length in &lengths {
                let parsed = parse_i64_scalar(length, "array_fill() expects integer length")?;
                if parsed < 0 {
                    return Err(EngineError {
                        message: "array_fill() length must be non-negative".to_string(),
                    });
                }
                parsed_lengths.push(usize::try_from(parsed).map_err(|_| EngineError {
                    message: "array_fill() length is too large".to_string(),
                })?);
            }
            let total = parsed_lengths
                .iter()
                .try_fold(1usize, |acc, len| acc.checked_mul(*len))
                .ok_or_else(|| EngineError {
                    message: "array_fill() result is too large".to_string(),
                })?;
            let _ = total
                .checked_mul(std::mem::size_of::<ScalarValue>())
                .ok_or_else(|| EngineError {
                    message: "array_fill() result is too large".to_string(),
                })?;
            if args.len() == 3 && !matches!(args[2], ScalarValue::Null) {
                let _ = array_values_arg(&args[2], "array_fill() expects array of lower bounds")?;
            }
            Ok(build_filled_array(&args[0], &parsed_lengths))
        }
        "array_reverse" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values =
                array_values_arg(&args[0], "array_reverse() expects array argument")?;
            values.reverse();
            Ok(ScalarValue::Array(values))
        }
        "trim_array" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let mut values = array_values_arg(&args[0], "trim_array() expects array argument")?;
            let trim_count = parse_i64_scalar(&args[1], "trim_array() expects integer trim count")?;
            if trim_count < 0 {
                return Err(EngineError {
                    message: "trim_array() trim count must be non-negative".to_string(),
                });
            }
            let trim_count = trim_count as usize;
            if trim_count >= values.len() {
                values.clear();
            } else {
                values.truncate(values.len() - trim_count);
            }
            Ok(ScalarValue::Array(values))
        }
        "array_shuffle" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values =
                array_values_arg(&args[0], "array_shuffle() expects array argument")?;
            let mut i = values.len();
            while i > 1 {
                i -= 1;
                let j = (rand_f64() * (i + 1) as f64).floor() as usize;
                values.swap(i, j.min(i));
            }
            Ok(ScalarValue::Array(values))
        }
        "array_sample" if args.len() == 2 => {
            if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let mut values = array_values_arg(&args[0], "array_sample() expects array argument")?;
            let sample_size = parse_i64_scalar(&args[1], "array_sample() expects integer size")?;
            if sample_size < 0 {
                return Err(EngineError {
                    message: "array_sample() sample size must be non-negative".to_string(),
                });
            }
            let sample_size = sample_size as usize;
            if sample_size >= values.len() {
                return Ok(ScalarValue::Array(values));
            }
            let mut i = values.len();
            while i > 1 {
                i -= 1;
                let j = (rand_f64() * (i + 1) as f64).floor() as usize;
                values.swap(i, j.min(i));
            }
            values.truncate(sample_size);
            Ok(ScalarValue::Array(values))
        }
        "array_sort" if (1..=3).contains(&args.len()) => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values = array_values_arg(&args[0], "array_sort() expects array argument")?;
            let descending = if args.len() >= 2 && !matches!(args[1], ScalarValue::Null) {
                parse_bool_scalar(&args[1], "array_sort() second argument must be boolean")?
            } else {
                false
            };
            let nulls_first = if args.len() == 3 && !matches!(args[2], ScalarValue::Null) {
                parse_bool_scalar(&args[2], "array_sort() third argument must be boolean")?
            } else {
                descending
            };
            values.sort_by(|left, right| {
                let left_null = matches!(left, ScalarValue::Null);
                let right_null = matches!(right, ScalarValue::Null);
                if left_null || right_null {
                    return match (left_null, right_null) {
                        (true, true) => Ordering::Equal,
                        (true, false) => {
                            if nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (false, true) => {
                            if nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (false, false) => Ordering::Equal,
                    };
                }
                let ord = scalar_cmp_fallback(left, right);
                if descending { ord.reverse() } else { ord }
            });
            Ok(ScalarValue::Array(values))
        }
        "array_upper" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_upper() expects array as first argument")?;
            let dim = parse_i64_scalar(&args[1], "array_upper() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "array_lower" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let values =
                array_values_arg(&args[0], "array_lower() expects array as first argument")?;
            let dim = parse_i64_scalar(&args[1], "array_lower() expects integer dimension")?;
            if dim != 1 || values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Int(1))
        }
        "cardinality" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values = array_values_arg(&args[0], "cardinality() expects array argument")?;
            Ok(ScalarValue::Int(values.len() as i64))
        }
        "string_to_array" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let delimiter = if matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Array(vec![ScalarValue::Text(input)]));
            } else {
                args[1].render()
            };
            let null_str = args.get(2).and_then(|a| {
                if matches!(a, ScalarValue::Null) {
                    None
                } else {
                    Some(a.render())
                }
            });
            let parts = if delimiter.is_empty() {
                input.chars().map(|c| c.to_string()).collect::<Vec<_>>()
            } else {
                input
                    .split(&delimiter)
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
            };
            let values = parts
                .into_iter()
                .map(|part| {
                    if null_str.as_deref() == Some(part.as_str()) {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Text(part)
                    }
                })
                .collect();
            Ok(ScalarValue::Array(values))
        }
        "array_to_string" if args.len() == 2 || args.len() == 3 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let delimiter = args[1].render();
            let null_replacement = args.get(2).map(|a| a.render());
            let values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                ScalarValue::Text(text) => {
                    let inner = text.trim_start_matches('{').trim_end_matches('}');
                    if inner.is_empty() {
                        return Ok(ScalarValue::Text(String::new()));
                    }
                    inner
                        .split(',')
                        .map(|part| {
                            let trimmed = part.trim();
                            if trimmed == "NULL" {
                                ScalarValue::Null
                            } else {
                                ScalarValue::Text(trimmed.to_string())
                            }
                        })
                        .collect::<Vec<_>>()
                }
                _ => {
                    return Err(EngineError {
                        message: "array_to_string() expects array argument".to_string(),
                    });
                }
            };
            let result: Vec<String> = values
                .iter()
                .filter_map(|value| match value {
                    ScalarValue::Null => null_replacement.clone(),
                    _ => Some(value.render()),
                })
                .collect();
            Ok(ScalarValue::Text(result.join(&delimiter)))
        }
        "unnest" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values = array_values_arg(&args[0], "unnest() expects array argument")?;
            Ok(values.into_iter().next().unwrap_or(ScalarValue::Null))
        }
        "regexp_matches" if args.len() == 2 || args.len() == 3 => {
            if args.iter().take(2).any(|arg| matches!(arg, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let text = args[0].render();
            let pattern = args[1].render();
            let flags = if args.len() == 3 {
                args[2].render()
            } else {
                String::new()
            };
            eval_regexp_match(&text, &pattern, &flags)
        }
        "min_scale" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let scale = match parse_numeric_operand(&args[0])? {
                NumericOperand::Int(_) => 0,
                NumericOperand::Numeric(value) => i64::from(value.normalize().scale()),
                NumericOperand::Float(value) => {
                    if !value.is_finite() {
                        return Ok(ScalarValue::Null);
                    }
                    let rendered = format!("{value}");
                    if let Some((_, frac)) = rendered.split_once('.') {
                        frac.trim_end_matches('0').len() as i64
                    } else {
                        0
                    }
                }
            };
            Ok(ScalarValue::Int(scale))
        }
        "trim_scale" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            match parse_numeric_operand(&args[0])? {
                NumericOperand::Int(value) => Ok(ScalarValue::Int(value)),
                NumericOperand::Numeric(value) => Ok(ScalarValue::Numeric(value.normalize())),
                NumericOperand::Float(value) => {
                    if !value.is_finite() {
                        return Ok(ScalarValue::Float(value));
                    }
                    let rendered = format!("{value}");
                    if let Ok(parsed) = rendered.parse::<f64>() {
                        Ok(ScalarValue::Float(parsed))
                    } else {
                        Ok(ScalarValue::Float(value))
                    }
                }
            }
        }
        "pg_lsn" if args.len() == 1 => Ok(ScalarValue::Text(args[0].render())),
        "crc32" | "crc32c" if args.len() == 1 => {
            let hash = stable_hash_i64(&args[0].render());
            Ok(ScalarValue::Int(hash & 0x7fff_ffff))
        }
        "sha224" | "sha384" | "sha512" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(sha256_hex(&args[0].render())))
        }
        "bit_count" if args.len() == 1 => {
            let value = args[0].render().parse::<u128>().unwrap_or(0);
            Ok(ScalarValue::Int(value.count_ones() as i64))
        }
        "get_bit" if args.len() == 2 => {
            let value = args[0].render().parse::<u128>().unwrap_or(0);
            let offset = args[1].render().parse::<usize>().unwrap_or(0);
            Ok(ScalarValue::Int(((value >> offset) & 1) as i64))
        }
        "set_bit" if args.len() == 3 => {
            let mut value = args[0].render().parse::<u128>().unwrap_or(0);
            let offset = args[1].render().parse::<usize>().unwrap_or(0);
            let bit = args[2].render().parse::<u8>().unwrap_or(0);
            if bit == 0 {
                value &= !(1u128 << offset);
            } else {
                value |= 1u128 << offset;
            }
            Ok(ScalarValue::Text(value.to_string()))
        }
        "float8_combine" | "float8_regr_combine" | "pg_column_compression" if !args.is_empty() => {
            Ok(ScalarValue::Null)
        }
        _ => Err(EngineError {
            message: format!("unsupported function call {fn_name}"),
        }),
    }
}

fn eval_range_constructor(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let bounds = if args.len() == 3 {
        args[2].render()
    } else {
        "[)".to_string()
    };
    let mut chars = bounds.chars();
    let Some(lower_bound) = chars.next() else {
        return Err(EngineError {
            message: "invalid range bound flags".to_string(),
        });
    };
    let Some(upper_bound) = chars.next() else {
        return Err(EngineError {
            message: "invalid range bound flags".to_string(),
        });
    };
    if chars.next().is_some()
        || !matches!(lower_bound, '[' | '(')
        || !matches!(upper_bound, ']' | ')')
    {
        return Err(EngineError {
            message: "invalid range bound flags".to_string(),
        });
    }

    let lower = args[0].render();
    let upper = args[1].render();
    Ok(ScalarValue::Text(format!(
        "{lower_bound}{lower},{upper}{upper_bound}"
    )))
}

/// PostgreSQL-compatible sind() — exact results for multiples of 30 degrees.
fn sind(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    // Normalize to [0, 360)
    let mut arg = x % 360.0;
    if arg < 0.0 {
        arg += 360.0;
    }
    match arg as i64 {
        0 | 180 => 0.0,
        30 | 150 => 0.5,
        90 => 1.0,
        210 | 330 => -0.5,
        270 => -1.0,
        _ => (x.to_radians()).sin(),
    }
}

/// PostgreSQL-compatible cosd() — exact results for multiples of 30 degrees.
fn cosd(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    let mut arg = x % 360.0;
    if arg < 0.0 {
        arg += 360.0;
    }
    match arg as i64 {
        0 | 360 => 1.0,
        60 | 300 => 0.5,
        90 | 270 => 0.0,
        120 | 240 => -0.5,
        180 => -1.0,
        _ => (x.to_radians()).cos(),
    }
}

/// PostgreSQL-compatible tand() — exact results for multiples of 45 degrees.
fn tand(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    let mut arg = x % 360.0;
    if arg < 0.0 {
        arg += 360.0;
    }
    match arg as i64 {
        0 | 180 | 360 => 0.0,
        45 | 225 => 1.0,
        135 | 315 => -1.0,
        90 => f64::INFINITY,
        270 => f64::NEG_INFINITY,
        _ => (x.to_radians()).tan(),
    }
}
