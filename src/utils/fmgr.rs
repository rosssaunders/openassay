use std::cmp::Ordering;

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
    eval_http_patch, eval_http_post_content, eval_http_post_form, eval_http_put, eval_json_array_length,
    eval_json_extract_path, eval_json_object, eval_json_pretty, eval_json_strip_nulls,
    eval_json_typeof, eval_jsonb_exists, eval_jsonb_exists_any_all, eval_jsonb_insert,
    eval_jsonb_path_exists, eval_jsonb_path_match, eval_jsonb_path_query_array,
    eval_jsonb_path_query_first, eval_jsonb_set, eval_jsonb_set_lax, eval_row_to_json,
    eval_urlencode, json_build_array_value, json_build_object_value, scalar_to_json_value,
};
use crate::utils::adt::math_functions::{
    coerce_to_f64, eval_factorial, eval_scale, eval_width_bucket, gcd_i64, numeric_mod,
};
use crate::utils::adt::misc::{
    array_value_matches, compare_values_for_predicate, count_nonnulls, count_nulls, eval_extremum,
    eval_regexp_count, eval_regexp_instr, eval_regexp_like, eval_regexp_match,
    eval_regexp_replace, eval_regexp_split_to_array, eval_regexp_substr, eval_unistr,
    gen_random_uuid, parse_bool_scalar, parse_i64_scalar, pg_get_viewdef, pg_input_is_valid,
    quote_ident, quote_literal, quote_nullable, rand_f64,
};
use crate::utils::adt::string_functions::{
    TrimMode, ascii_code, chr_from_code, decode_bytes, encode_bytes, eval_format,
    find_substring_position, initcap_string, left_chars, md5_hex, overlay_text, pad_string,
    right_chars, sha256_hex, substring_chars, trim_text,
};

fn require_http_extension() -> Result<(), EngineError> {
    if with_ext_read(|ext| ext.extensions.iter().any(|ext| ext.name == "http")) {
        Ok(())
    } else {
        Err(EngineError {
            message: "extension \"http\" is not loaded".to_string(),
        })
    }
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
        "json_object" | "jsonb_object" if args.len() == 1 || args.len() == 2 => eval_json_object(args, fn_name),
        "json_build_object" | "jsonb_build_object" if !args.len().is_multiple_of(2) => {
            Err(EngineError {
                message: "argument list must have even number of elements".to_string(),
            })
        }
        "json_build_object" | "jsonb_build_object" => Ok(
            ScalarValue::Text(json_build_object_value(args)?.to_string()),
        ),
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
        "json_strip_nulls" | "jsonb_strip_nulls" if args.len() == 1 => {
            eval_json_strip_nulls(&args[0], fn_name)
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
        "jsonb_insert" if args.len() == 3 || args.len() == 4 => eval_jsonb_insert(args),
        "jsonb_set_lax" if args.len() >= 3 && args.len() <= 5 => eval_jsonb_set_lax(args),
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
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                let value = sequence_next_value(state, &sequence_name)?;
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
                        message: format!("sequence \"{}\" does not exist", sequence_name),
                    });
                };
                if !state.called {
                    return Err(EngineError {
                        message: format!(
                            "currval of sequence \"{}\" is not yet defined",
                            sequence_name
                        ),
                    });
                }
                Ok(ScalarValue::Int(state.current))
            })
        }
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
                        message: format!("sequence \"{}\" does not exist", sequence_name),
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
            ScalarValue::Int(i) => Ok(ScalarValue::Int(i.abs())),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.abs())),
            _ => Err(EngineError {
                message: "abs() expects numeric argument".to_string(),
            }),
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
            let start = parse_i64_scalar(&args[1], "substring() expects integer start index")?;
            let length = if args.len() == 3 {
                Some(parse_i64_scalar(
                    &args[2],
                    "substring() expects integer length",
                )?)
            } else {
                None
            };
            Ok(ScalarValue::Text(substring_chars(&input, start, length)?))
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
        "current_date" if args.is_empty() => Ok(ScalarValue::Text(current_date_string()?)),
        "age" if args.len() == 1 || args.len() == 2 => eval_age(args),
        "extract" | "date_part" if args.len() == 2 => eval_extract_or_date_part(&args[0], &args[1]),
        "date_trunc" if args.len() == 2 => eval_date_trunc(&args[0], &args[1]),
        "date_add" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], true),
        "date_sub" if args.len() == 2 => eval_date_add_sub(&args[0], &args[1], false),
        "to_timestamp" if args.len() == 1 => eval_to_timestamp(&args[0]),
        "to_timestamp" if args.len() == 2 => eval_to_timestamp_with_format(&args[0], &args[1]),
        "to_date" if args.len() == 2 => eval_to_date_with_format(&args[0], &args[1]),
        "make_interval" if args.len() == 7 => eval_make_interval(args),
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
            _ => Err(EngineError {
                message: "ceil() expects numeric argument".to_string(),
            }),
        },
        "floor" if args.len() == 1 => match &args[0] {
            ScalarValue::Null => Ok(ScalarValue::Null),
            ScalarValue::Int(i) => Ok(ScalarValue::Int(*i)),
            ScalarValue::Float(f) => Ok(ScalarValue::Float(f.floor())),
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
        "width_bucket" if args.len() == 4 => eval_width_bucket(args),
        "scale" if args.len() == 1 => eval_scale(&args[0]),
        "factorial" if args.len() == 1 => eval_factorial(&args[0]),
        // Hyperbolic functions
        "sinh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "sinh()")?.sinh()))
        }
        "cosh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "cosh()")?.cosh()))
        }
        "tanh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "tanh()")?.tanh()))
        }
        "asinh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            Ok(ScalarValue::Float(coerce_to_f64(&args[0], "asinh()")?.asinh()))
        }
        "acosh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "acosh()")?;
            if v < 1.0 && !v.is_nan() {
                return Err(EngineError { message: "input is out of range".to_string() });
            }
            Ok(ScalarValue::Float(v.acosh()))
        }
        "atanh" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "atanh()")?;
            if !(-1.0..=1.0).contains(&v) && !v.is_nan() {
                return Err(EngineError { message: "input is out of range".to_string() });
            }
            Ok(ScalarValue::Float(v.atanh()))
        }
        // Degree-based trig functions
        "sind" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "sind()")?;
            Ok(ScalarValue::Float(sind(v)))
        }
        "cosd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "cosd()")?;
            Ok(ScalarValue::Float(cosd(v)))
        }
        "tand" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "tand()")?;
            Ok(ScalarValue::Float(tand(v)))
        }
        "cotd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "cotd()")?;
            let t = tand(v);
            if t == 0.0 {
                return Err(EngineError { message: "division by zero".to_string() });
            }
            Ok(ScalarValue::Float(1.0 / t))
        }
        "asind" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "asind()")?;
            if !(-1.0..=1.0).contains(&v) {
                return Err(EngineError { message: "input is out of range".to_string() });
            }
            Ok(ScalarValue::Float(v.asin().to_degrees()))
        }
        "acosd" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "acosd()")?;
            if !(-1.0..=1.0).contains(&v) {
                return Err(EngineError { message: "input is out of range".to_string() });
            }
            Ok(ScalarValue::Float(v.acos().to_degrees()))
        }
        "atand" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) { return Ok(ScalarValue::Null); }
            let v = coerce_to_f64(&args[0], "atand()")?;
            Ok(ScalarValue::Float(v.atan().to_degrees()))
        }
        "atan2d" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) { return Ok(ScalarValue::Null); }
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
            Ok(ScalarValue::Int(gcd_i64(a, b)))
        }
        "lcm" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let a = parse_i64_scalar(&args[0], "lcm() expects integer")?;
            let b = parse_i64_scalar(&args[1], "lcm() expects integer")?;
            let g = gcd_i64(a, b);
            Ok(ScalarValue::Int(if g == 0 { 0 } else { (a / g * b).abs() }))
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
            Ok(ScalarValue::Text(s.repeat(n as usize)))
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
            if field <= 0 {
                return Err(EngineError {
                    message: "field position must be greater than zero".to_string(),
                });
            }
            let parts: Vec<&str> = input.split(&delimiter).collect();
            Ok(ScalarValue::Text(
                parts.get((field - 1) as usize).unwrap_or(&"").to_string(),
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
            let len = parse_i64_scalar(&args[1], "lpad() expects integer length")? as usize;
            let fill = if args.len() == 3 {
                args[2].render()
            } else {
                " ".to_string()
            };
            Ok(ScalarValue::Text(pad_string(&input, len, &fill, true)))
        }
        "rpad" if args.len() == 2 || args.len() == 3 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let input = args[0].render();
            let len = parse_i64_scalar(&args[1], "rpad() expects integer length")? as usize;
            let fill = if args.len() == 3 {
                args[2].render()
            } else {
                " ".to_string()
            };
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
        "regexp_replace" if args.len() == 3 || args.len() == 4 => {
            if args.iter().take(3).any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let source = args[0].render();
            let pattern = args[1].render();
            let replacement = args[2].render();
            let flags = if args.len() == 4 {
                args[3].render()
            } else {
                String::new()
            };
            eval_regexp_replace(&source, &pattern, &replacement, &flags)
        }
        "regexp_split_to_array" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            eval_regexp_split_to_array(&args[0].render(), &args[1].render())
        }
        "regexp_count" if args.len() >= 2 && args.len() <= 4 => {
            eval_regexp_count(args)
        }
        "regexp_instr" if args.len() >= 2 && args.len() <= 7 => {
            eval_regexp_instr(args)
        }
        "regexp_substr" if args.len() >= 2 && args.len() <= 6 => {
            eval_regexp_substr(args)
        }
        "regexp_like" if args.len() >= 2 && args.len() <= 3 => {
            eval_regexp_like(args)
        }
        "to_hex" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_hex() expects integer")?;
            Ok(ScalarValue::Text(format!("{:x}", v)))
        }
        "to_oct" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_oct() expects integer")?;
            Ok(ScalarValue::Text(format!("{:o}", v)))
        }
        "to_bin" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let v = parse_i64_scalar(&args[0], "to_bin() expects integer")?;
            Ok(ScalarValue::Text(format!("{:b}", v)))
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
            Ok(ScalarValue::Text(String::from_utf8_lossy(&bytes).to_string()))
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
        "version" if args.is_empty() => Ok(ScalarValue::Text("Postrust 0.1.0 on Rust".to_string())),
        "current_database" if args.is_empty() => Ok(ScalarValue::Text("postrust".to_string())),
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
                ScalarValue::Text(_) => "text",
                ScalarValue::Array(_) => "text[]",
                ScalarValue::Record(_) => "record",
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
                ScalarValue::Text(s) => s.len() as i64 + 4, // 4-byte length prefix
                ScalarValue::Array(a) => {
                    let mut total = 20i64; // array header
                    for v in a {
                        total += match v {
                            ScalarValue::Null => 0,
                            ScalarValue::Bool(_) => 1,
                            ScalarValue::Int(_) => 8,
                            ScalarValue::Float(_) => 8,
                            ScalarValue::Text(s) => s.len() as i64 + 4,
                            ScalarValue::Array(_) => 8, // rough estimate
                            ScalarValue::Record(r) => (r.len() * 8) as i64,
                        };
                    }
                    total
                }
                ScalarValue::Record(r) => (r.len() as i64) * 8 + 4,
            };
            Ok(ScalarValue::Int(size))
        }
        "pg_get_userbyid" if args.len() == 1 => Ok(ScalarValue::Text("postrust".to_string())),
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
            Ok(ScalarValue::Text(format!("{:04}-{:02}-{:02}", y, m, d)))
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
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    y, mo, d, h, mi, sec
                )))
            } else {
                Ok(ScalarValue::Text(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    y, mo, d, h, mi, sec, frac
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
                    message: format!("invalid input for to_number: {}", s),
                }),
            }
        }
        "array_append" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_append() expects array as first argument".to_string(),
                    });
                }
            };
            values.push(args[1].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_prepend" if args.len() == 2 => {
            if matches!(args[1], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let mut values = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_prepend() expects array as second argument".to_string(),
                    });
                }
            };
            values.insert(0, args[0].clone());
            Ok(ScalarValue::Array(values))
        }
        "array_cat" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let mut left = match &args[0] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    });
                }
            };
            let right = match &args[1] {
                ScalarValue::Array(values) => values.clone(),
                _ => {
                    return Err(EngineError {
                        message: "array_cat() expects array arguments".to_string(),
                    });
                }
            };
            left.extend(right);
            Ok(ScalarValue::Array(left))
        }
        "array_remove" if args.len() == 2 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_remove() expects array as first argument".to_string(),
                    });
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_replace() expects array as first argument".to_string(),
                    });
                }
            };
            let mut out = Vec::with_capacity(values.len());
            for value in values {
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_position() expects array as first argument".to_string(),
                    });
                }
            };
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_positions() expects array as first argument".to_string(),
                    });
                }
            };
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_length() expects array as first argument".to_string(),
                    });
                }
            };
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_dims() expects array argument".to_string(),
                    });
                }
            };
            if values.is_empty() {
                return Ok(ScalarValue::Null);
            }
            Ok(ScalarValue::Text(format!("[1:{}]", values.len())))
        }
        "array_ndims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            match &args[0] {
                ScalarValue::Array(_) => Ok(ScalarValue::Int(1)),
                _ => Err(EngineError {
                    message: "array_ndims() expects array argument".to_string(),
                }),
            }
        }
        "array_fill" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let lengths = match &args[1] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_fill() expects array of lengths".to_string(),
                    });
                }
            };
            if lengths.len() != 1 {
                return Err(EngineError {
                    message: "array_fill() currently supports one-dimensional arrays".to_string(),
                });
            }
            let length = parse_i64_scalar(&lengths[0], "array_fill() expects integer length")?;
            if length < 0 {
                return Err(EngineError {
                    message: "array_fill() length must be non-negative".to_string(),
                });
            }
            let mut out = Vec::with_capacity(length as usize);
            for _ in 0..length {
                out.push(args[0].clone());
            }
            Ok(ScalarValue::Array(out))
        }
        "array_upper" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_upper() expects array as first argument".to_string(),
                    });
                }
            };
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
            let values = match &args[0] {
                ScalarValue::Array(values) => values,
                _ => {
                    return Err(EngineError {
                        message: "array_lower() expects array as first argument".to_string(),
                    });
                }
            };
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
            match &args[0] {
                ScalarValue::Array(values) => Ok(ScalarValue::Int(values.len() as i64)),
                _ => Err(EngineError {
                    message: "cardinality() expects array argument".to_string(),
                }),
            }
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
        _ => Err(EngineError {
            message: format!("unsupported function call {}", fn_name),
        }),
    }
}

/// PostgreSQL-compatible sind() — exact results for multiples of 30 degrees.
fn sind(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return f64::NAN;
    }
    // Normalize to [0, 360)
    let mut arg = x % 360.0;
    if arg < 0.0 { arg += 360.0; }
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
    if arg < 0.0 { arg += 360.0; }
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
    if arg < 0.0 { arg += 360.0; }
    match arg as i64 {
        0 | 180 | 360 => 0.0,
        45 | 225 => 1.0,
        135 | 315 => -1.0,
        90 => f64::INFINITY,
        270 => f64::NEG_INFINITY,
        _ => (x.to_radians()).tan(),
    }
}
