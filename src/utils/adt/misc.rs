use std::cmp::Ordering;

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::datetime::{
    datetime_to_epoch_seconds, days_from_civil, parse_temporal_operand,
};
use crate::utils::adt::math_functions::{NumericOperand, parse_numeric_operand};

pub(crate) fn array_value_matches(
    target: &ScalarValue,
    candidate: &ScalarValue,
) -> Result<bool, EngineError> {
    match (target, candidate) {
        (ScalarValue::Null, ScalarValue::Null) => Ok(true),
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => Ok(false),
        _ => Ok(compare_values_for_predicate(target, candidate)? == Ordering::Equal),
    }
}

pub(crate) fn build_regex(
    pattern: &str,
    flags: &str,
    fn_name: &str,
) -> Result<regex::Regex, EngineError> {
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
                    message: format!("{fn_name}() unsupported regex flag {flag}"),
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

pub(crate) fn parse_pg_array_literal(input: &str) -> Result<ScalarValue, EngineError> {
    let mut text = input.trim();
    if text.starts_with('[') {
        let Some(eq_pos) = text.find('=') else {
            return Err(EngineError {
                message: format!("invalid array literal \"{input}\""),
            });
        };
        text = &text[eq_pos + 1..];
    }

    let mut parser = ArrayLiteralParser::new(text);
    let value = parser.parse_array()?;
    parser.skip_whitespace();
    if !parser.is_eof() {
        return Err(EngineError {
            message: format!("invalid array literal \"{input}\""),
        });
    }
    Ok(value)
}

struct ArrayLiteralParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ArrayLiteralParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    fn skip_whitespace(&mut self) {
        while let Some(byte) = self.peek() {
            if (byte as char).is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), EngineError> {
        match self.bump() {
            Some(found) if found == expected => Ok(()),
            _ => Err(EngineError {
                message: "invalid array literal".to_string(),
            }),
        }
    }

    fn parse_array(&mut self) -> Result<ScalarValue, EngineError> {
        self.skip_whitespace();
        self.expect_byte(b'{')?;
        self.skip_whitespace();
        let mut elements = Vec::new();

        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(ScalarValue::Array(elements));
        }

        loop {
            elements.push(self.parse_element()?);
            self.skip_whitespace();
            match self.bump() {
                Some(b',') => {
                    self.skip_whitespace();
                }
                Some(b'}') => break,
                _ => {
                    return Err(EngineError {
                        message: "invalid array literal".to_string(),
                    });
                }
            }
        }

        Ok(ScalarValue::Array(elements))
    }

    fn parse_element(&mut self) -> Result<ScalarValue, EngineError> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.parse_array(),
            Some(b'"') => Ok(ScalarValue::Text(self.parse_quoted_text()?)),
            Some(_) => self.parse_unquoted_element(),
            None => Err(EngineError {
                message: "invalid array literal".to_string(),
            }),
        }
    }

    fn parse_quoted_text(&mut self) -> Result<String, EngineError> {
        self.expect_byte(b'"')?;
        let mut out = String::new();
        while let Some(byte) = self.bump() {
            match byte {
                b'"' => {
                    if self.peek() == Some(b'"') {
                        self.bump();
                        out.push('"');
                    } else {
                        return Ok(out);
                    }
                }
                b'\\' => {
                    if let Some(next) = self.bump() {
                        out.push(next as char);
                    } else {
                        return Err(EngineError {
                            message: "invalid array literal".to_string(),
                        });
                    }
                }
                _ => out.push(byte as char),
            }
        }
        Err(EngineError {
            message: "invalid array literal".to_string(),
        })
    }

    fn parse_unquoted_element(&mut self) -> Result<ScalarValue, EngineError> {
        let start = self.pos;
        while let Some(byte) = self.peek() {
            if byte == b',' || byte == b'}' {
                break;
            }
            self.pos += 1;
        }
        let token = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| EngineError {
                message: "invalid array literal".to_string(),
            })?
            .trim();
        if token.eq_ignore_ascii_case("null") {
            Ok(ScalarValue::Null)
        } else {
            Ok(ScalarValue::Text(token.to_string()))
        }
    }
}

pub(crate) fn eval_regexp_match(
    text: &str,
    pattern: &str,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
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
    if args
        .iter()
        .take(2)
        .any(|arg| matches!(arg, ScalarValue::Null))
    {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() == 3 {
        args[2].render()
    } else {
        String::new()
    };
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

pub(crate) fn eval_regexp_split_to_array(
    text: &str,
    pattern: &str,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
    let regex = build_regex(pattern, flags, "regexp_split_to_array")?;
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
    if args.len() != 2 && args.len() != 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() == 3 {
        args[2].render()
    } else {
        String::new()
    };
    let regex = build_regex(&pattern, &flags, fn_name)?;
    let rows = regex
        .split(&text)
        .map(|part| vec![ScalarValue::Text(part.to_string())])
        .collect();
    Ok((vec![fn_name.to_string()], rows))
}

pub(crate) fn eval_string_to_table_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 && args.len() != 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if matches!(args[0], ScalarValue::Null) || matches!(args[1], ScalarValue::Null) {
        return Ok((vec![fn_name.to_string()], Vec::new()));
    }

    let input = args[0].render();
    let delimiter = args[1].render();
    let null_str = args.get(2).and_then(|arg| {
        if matches!(arg, ScalarValue::Null) {
            None
        } else {
            Some(arg.render())
        }
    });

    let parts = if delimiter.is_empty() {
        input.chars().map(|c| c.to_string()).collect::<Vec<_>>()
    } else {
        input
            .split(&delimiter)
            .map(|part| part.to_string())
            .collect::<Vec<_>>()
    };

    let rows = parts
        .into_iter()
        .map(|part| {
            if null_str.as_deref() == Some(part.as_str()) {
                vec![ScalarValue::Null]
            } else {
                vec![ScalarValue::Text(part)]
            }
        })
        .collect::<Vec<_>>();
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

/// Generate a random UUID (version 4).
/// Matches PostgreSQL's gen_random_uuid() function.
pub(crate) fn gen_random_uuid() -> String {
    use std::time::SystemTime;

    // Generate random bytes using SystemTime as seed
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Simple pseudo-random number generator
    let mut state = seed as u64;
    let mut next_random = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((state >> 32) as u32) as u64
    };

    // Generate UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    // where y is 8, 9, A, or B
    let r1 = next_random();
    let r2 = next_random();
    let r3 = next_random();
    let r4 = next_random();

    format!(
        "{:08x}-{:04x}-4{:03x}-{:x}{:03x}-{:08x}{:04x}",
        (r1 & 0xFFFFFFFF),
        ((r2 >> 16) & 0xFFFF),
        (r2 & 0x0FFF),
        (8 + ((r3 >> 60) & 0x3)), // y must be 8, 9, A, or B
        ((r3 >> 48) & 0x0FFF),
        (r3 & 0xFFFFFFFF),
        ((r4 >> 16) & 0xFFFF)
    )
}

pub(crate) fn eval_regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    start: i64,
    occurrence: i64,
    flags: &str,
) -> Result<ScalarValue, EngineError> {
    if start < 1 {
        return Err(EngineError {
            message: format!("invalid value for parameter \"start\": {start}"),
        });
    }
    if occurrence < 0 {
        return Err(EngineError {
            message: format!("invalid value for parameter \"N\": {occurrence}"),
        });
    }

    let start_idx = usize::try_from(start - 1).map_err(|_| EngineError {
        message: "invalid value for parameter \"start\"".to_string(),
    })?;
    let search_start_byte = source
        .char_indices()
        .nth(start_idx)
        .map_or(source.len(), |(idx, _)| idx);
    let prefix = &source[..search_start_byte];
    let search = &source[search_start_byte..];

    let global = occurrence == 0 || flags.contains('g');
    let regex = build_regex(pattern, flags, "regexp_replace")?;
    let out = if global {
        format!("{prefix}{}", regex.replace_all(search, replacement))
    } else {
        let occurrence_idx = usize::try_from(occurrence - 1).map_err(|_| EngineError {
            message: "invalid value for parameter \"N\"".to_string(),
        })?;
        let Some(matched) = regex.find_iter(search).nth(occurrence_idx) else {
            return Ok(ScalarValue::Text(source.to_string()));
        };
        let before = &search[..matched.start()];
        let tail = &search[matched.start()..];
        format!("{prefix}{before}{}", regex.replacen(tail, 1, replacement))
    };
    Ok(ScalarValue::Text(out))
}

/// regexp_count(string, pattern [, start [, flags]]) - count pattern matches
pub(crate) fn eval_regexp_count(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().take(2).any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let start = if args.len() >= 3 {
        parse_i64_scalar(&args[2], "regexp_count() expects integer start")?
    } else {
        1
    };
    let flags = if args.len() >= 4 {
        args[3].render()
    } else {
        String::new()
    };
    if start < 1 {
        return Err(EngineError {
            message: "invalid value for parameter \"start\": 0".to_string(),
        });
    }
    let start_idx = usize::try_from(start - 1).map_err(|_| EngineError {
        message: "invalid value for parameter \"start\"".to_string(),
    })?;
    let search_text: String = text.chars().skip(start_idx).collect();
    let regex = build_regex(&pattern, &flags, "regexp_count")?;
    let count = regex.find_iter(&search_text).count();
    Ok(ScalarValue::Int(count as i64))
}

/// regexp_instr(string, pattern [, start [, N [, endoption [, flags [, subexpr]]]]])
pub(crate) fn eval_regexp_instr(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().take(2).any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let start = if args.len() >= 3 {
        parse_i64_scalar(&args[2], "regexp_instr() start")?
    } else {
        1
    };
    let n = if args.len() >= 4 {
        parse_i64_scalar(&args[3], "regexp_instr() N")?
    } else {
        1
    };
    let endoption = if args.len() >= 5 {
        parse_i64_scalar(&args[4], "regexp_instr() endoption")?
    } else {
        0
    };
    let flags = if args.len() >= 6 {
        args[5].render()
    } else {
        String::new()
    };
    let subexpr = if args.len() >= 7 {
        parse_i64_scalar(&args[6], "regexp_instr() subexpr")?
    } else {
        0
    };

    if start < 1 || n < 1 {
        return Ok(ScalarValue::Int(0));
    }
    let start_idx = usize::try_from(start - 1).map_err(|_| EngineError {
        message: "invalid value for parameter \"start\"".to_string(),
    })?;
    let n = usize::try_from(n).map_err(|_| EngineError {
        message: "invalid value for parameter \"N\"".to_string(),
    })?;
    let endoption = usize::try_from(endoption).map_err(|_| EngineError {
        message: "invalid value for parameter \"endoption\"".to_string(),
    })?;
    let subexpr = usize::try_from(subexpr).map_err(|_| EngineError {
        message: "invalid value for parameter \"subexpr\"".to_string(),
    })?;

    let char_vec: Vec<char> = text.chars().collect();
    let search_text: String = char_vec.iter().skip(start_idx).collect();
    let regex = build_regex(&pattern, &flags, "regexp_instr")?;

    let mut matches_iter = regex.captures_iter(&search_text);
    let Some(caps) = matches_iter.nth(n - 1) else {
        return Ok(ScalarValue::Int(0));
    };

    let m = if subexpr > 0 {
        caps.get(subexpr)
    } else {
        caps.get(0)
    };

    let Some(m) = m else {
        return Ok(ScalarValue::Int(0));
    };

    // Convert byte offset to char position in the search substring
    let byte_start = m.start();
    let byte_end = m.end();
    let char_start = search_text[..byte_start].chars().count();
    let char_end = search_text[..byte_end].chars().count();

    let result = if endoption == 0 {
        start + char_start as i64
    } else {
        start + char_end as i64
    };
    Ok(ScalarValue::Int(result))
}

/// regexp_substr(string, pattern [, start [, N [, flags [, subexpr]]]])
pub(crate) fn eval_regexp_substr(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().take(2).any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let start = if args.len() >= 3 {
        parse_i64_scalar(&args[2], "regexp_substr() start")?
    } else {
        1
    };
    let n = if args.len() >= 4 {
        parse_i64_scalar(&args[3], "regexp_substr() N")?
    } else {
        1
    };
    let flags = if args.len() >= 5 {
        args[4].render()
    } else {
        String::new()
    };
    let subexpr = if args.len() >= 6 {
        parse_i64_scalar(&args[5], "regexp_substr() subexpr")?
    } else {
        0
    };

    if start < 1 || n < 1 {
        return Ok(ScalarValue::Null);
    }
    let start_idx = usize::try_from(start - 1).map_err(|_| EngineError {
        message: "invalid value for parameter \"start\"".to_string(),
    })?;
    let n = usize::try_from(n).map_err(|_| EngineError {
        message: "invalid value for parameter \"N\"".to_string(),
    })?;
    let subexpr = usize::try_from(subexpr).map_err(|_| EngineError {
        message: "invalid value for parameter \"subexpr\"".to_string(),
    })?;

    let search_text: String = text.chars().skip(start_idx).collect();
    let regex = build_regex(&pattern, &flags, "regexp_substr")?;

    let mut matches_iter = regex.captures_iter(&search_text);
    let Some(caps) = matches_iter.nth(n - 1) else {
        return Ok(ScalarValue::Null);
    };

    let m = if subexpr > 0 {
        caps.get(subexpr)
    } else {
        caps.get(0)
    };

    match m {
        Some(m) => Ok(ScalarValue::Text(m.as_str().to_string())),
        None => Ok(ScalarValue::Null),
    }
}

/// regexp_like(string, pattern [, flags]) - boolean match test
pub(crate) fn eval_regexp_like(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().take(2).any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let text = args[0].render();
    let pattern = args[1].render();
    let flags = if args.len() >= 3 {
        args[2].render()
    } else {
        String::new()
    };
    let regex = build_regex(&pattern, &flags, "regexp_like")?;
    Ok(ScalarValue::Bool(regex.is_match(&text)))
}

/// unistr(text) - interpret Unicode escape sequences (\XXXX or \+XXXXXX)
pub(crate) fn eval_unistr(text: &str) -> Result<ScalarValue, EngineError> {
    let mut result = String::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' {
            if i + 1 < chars.len() && chars[i + 1] == '\\' {
                result.push('\\');
                i += 2;
            } else if i + 1 < chars.len() && chars[i + 1] == '+' {
                // \+XXXXXX - 6 hex digits
                if i + 8 > chars.len() {
                    return Err(EngineError {
                        message: "invalid Unicode escape sequence".to_string(),
                    });
                }
                let hex: String = chars[i + 2..i + 8].iter().collect();
                let code = u32::from_str_radix(&hex, 16).map_err(|_| EngineError {
                    message: format!("invalid Unicode escape value: \\+{hex}"),
                })?;
                let c = char::from_u32(code).ok_or_else(|| EngineError {
                    message: format!("invalid Unicode code point: {code}"),
                })?;
                result.push(c);
                i += 8;
            } else {
                // \XXXX - 4 hex digits
                if i + 5 > chars.len() {
                    return Err(EngineError {
                        message: "invalid Unicode escape sequence".to_string(),
                    });
                }
                let hex: String = chars[i + 1..i + 5].iter().collect();
                let code = u32::from_str_radix(&hex, 16).map_err(|_| EngineError {
                    message: format!("invalid Unicode escape value: \\{hex}"),
                })?;
                let c = char::from_u32(code).ok_or_else(|| EngineError {
                    message: format!("invalid Unicode code point: {code}"),
                })?;
                result.push(c);
                i += 5;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    Ok(ScalarValue::Text(result))
}

pub(crate) fn eval_extremum(
    args: &[ScalarValue],
    greatest: bool,
) -> Result<ScalarValue, EngineError> {
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
            (NumericOperand::Int(a), NumericOperand::Numeric(b)) => {
                let a_decimal = rust_decimal::Decimal::from(a);
                a_decimal.cmp(&b)
            }
            (NumericOperand::Numeric(a), NumericOperand::Int(b)) => {
                let b_decimal = rust_decimal::Decimal::from(b);
                a.cmp(&b_decimal)
            }
            (NumericOperand::Float(a), NumericOperand::Numeric(b)) => {
                let b_float = b.to_string().parse::<f64>().unwrap_or(f64::NAN);
                a.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Numeric(a), NumericOperand::Float(b)) => {
                let a_float = a.to_string().parse::<f64>().unwrap_or(f64::NAN);
                a_float.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
            (NumericOperand::Numeric(a), NumericOperand::Numeric(b)) => a.cmp(&b),
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
        ScalarValue::Text(v) => parse_bool_from_str(v),
        _ => None,
    }
}

/// Parse a boolean value from a string, following PostgreSQL's bool.c boolin() logic.
/// Accepts: true/yes/on/1/t/y (true), false/no/off/0/f/n (false).
/// Leading/trailing whitespace is ignored. Case-insensitive.
pub(crate) fn parse_bool_from_str(s: &str) -> Option<bool> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Match PostgreSQL's parse_bool_with_len from bool.c
    match trimmed.as_bytes()[0] | 0x20 {
        b't' => {
            if trimmed.len() == 1 || trimmed.eq_ignore_ascii_case("true") {
                Some(true)
            } else {
                None
            }
        }
        b'f' => {
            if trimmed.len() == 1 || trimmed.eq_ignore_ascii_case("false") {
                Some(false)
            } else {
                None
            }
        }
        b'y' => {
            if trimmed.len() == 1 || trimmed.eq_ignore_ascii_case("yes") {
                Some(true)
            } else {
                None
            }
        }
        b'n' => {
            if trimmed.len() == 1 || trimmed.eq_ignore_ascii_case("no") {
                Some(false)
            } else {
                None
            }
        }
        b'1' => {
            if trimmed.len() == 1 {
                Some(true)
            } else {
                None
            }
        }
        b'0' => {
            if trimmed.len() == 1 {
                Some(false)
            } else {
                None
            }
        }
        b'o' => {
            if trimmed.eq_ignore_ascii_case("on") {
                Some(true)
            } else if trimmed.eq_ignore_ascii_case("off") || trimmed.eq_ignore_ascii_case("of") {
                Some(false)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn parse_nullable_bool(
    value: &ScalarValue,
    message: &str,
) -> Result<Option<bool>, EngineError> {
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
    args.iter()
        .filter(|arg| matches!(arg, ScalarValue::Null))
        .count()
}

pub(crate) fn count_nonnulls(args: &[ScalarValue]) -> usize {
    args.len() - count_nulls(args)
}

pub(crate) fn parse_i64_scalar(value: &ScalarValue, message: &str) -> Result<i64, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(*v),
        ScalarValue::Float(v) if v.fract() == 0.0 => Ok(*v as i64),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = parse_pg_int_literal(v) {
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

pub(crate) fn parse_pg_int_literal(input: &str) -> Result<i64, EngineError> {
    let text = input.trim();
    if text.is_empty() {
        return Err(EngineError {
            message: "invalid integer literal".to_string(),
        });
    }

    let (negative, rest) = if let Some(tail) = text.strip_prefix('-') {
        (true, tail)
    } else if let Some(tail) = text.strip_prefix('+') {
        (false, tail)
    } else {
        (false, text)
    };

    let (radix, digits) =
        if let Some(tail) = rest.strip_prefix("0x").or_else(|| rest.strip_prefix("0X")) {
            (16u32, tail)
        } else if let Some(tail) = rest.strip_prefix("0o").or_else(|| rest.strip_prefix("0O")) {
            (8u32, tail)
        } else if let Some(tail) = rest.strip_prefix("0b").or_else(|| rest.strip_prefix("0B")) {
            (2u32, tail)
        } else {
            (10u32, rest)
        };

    let normalized = digits.replace('_', "");
    if normalized.is_empty() || !normalized.chars().all(|c| c.is_digit(radix)) {
        return Err(EngineError {
            message: "invalid integer literal".to_string(),
        });
    }

    let unsigned = i128::from_str_radix(&normalized, radix).map_err(|_| EngineError {
        message: "invalid integer literal".to_string(),
    })?;
    let signed = if negative { -unsigned } else { unsigned };
    i64::try_from(signed).map_err(|_| EngineError {
        message: "integer out of range".to_string(),
    })
}

pub(crate) fn parse_f64_scalar(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Numeric(d) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64().ok_or_else(|| EngineError {
                message: message.to_string(),
            })
        }
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
        ScalarValue::Numeric(d) => {
            use rust_decimal::prelude::ToPrimitive;
            d.to_f64().ok_or_else(|| EngineError {
                message: message.to_string(),
            })
        }
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

pub(crate) fn parse_bool_scalar(value: &ScalarValue, message: &str) -> Result<bool, EngineError> {
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
        ScalarValue::Numeric(v) => !v.is_zero(),
        ScalarValue::Text(v) => !v.is_empty(),
        ScalarValue::Array(values) => !values.is_empty(),
        ScalarValue::Record(fields) => !fields.is_empty(),
        ScalarValue::Vector(values) => !values.is_empty(),
    }
}

/// Test whether a string is valid input for a type.
/// Implements PostgreSQL's pg_input_is_valid() function (added in PG16).
pub(crate) fn pg_input_is_valid(
    input: &str,
    type_name: &str,
) -> Result<bool, crate::tcop::engine::EngineError> {
    let input = input.trim();

    // Normalize type name
    let normalized_type = type_name.trim().to_lowercase();
    let normalized_type = normalized_type.as_str();

    let is_valid = match normalized_type {
        "integer" | "int" | "int4" => parse_pg_int_literal(input)
            .map(|v| i32::try_from(v).is_ok())
            .unwrap_or(false),
        "bigint" | "int8" => parse_pg_int_literal(input).is_ok(),
        "smallint" | "int2" => parse_pg_int_literal(input)
            .map(|v| i16::try_from(v).is_ok())
            .unwrap_or(false),
        "numeric" | "decimal" => {
            // Try parsing as float or integer
            input.parse::<f64>().is_ok() || input.parse::<i64>().is_ok()
        }
        "real" | "float4" => crate::utils::adt::float::float4in(input).is_ok(),
        "double precision" | "float8" | "float" => {
            crate::utils::adt::float::float8in(input).is_ok()
        }
        "boolean" | "bool" => {
            matches!(
                input.to_lowercase().as_str(),
                "true" | "false" | "t" | "f" | "yes" | "no" | "y" | "n" | "1" | "0"
            )
        }
        "text" | "varchar" | "char" | "character varying" => true, // any string is valid text
        "date" => {
            // Simple date validation (YYYY-MM-DD format)
            parse_date_simple(input).is_ok()
        }
        "timestamp" | "timestamp without time zone" => {
            // Simple timestamp validation
            parse_timestamp_simple(input).is_ok()
        }
        "time" | "time without time zone" => {
            // Simple time validation (HH:MM:SS format)
            parse_time_simple(input).is_ok()
        }
        "json" | "jsonb" => {
            // Try parsing as JSON
            serde_json::from_str::<serde_json::Value>(input).is_ok()
        }
        "uuid" => {
            // UUID format: 8-4-4-4-12 hex digits
            parse_uuid_simple(input).is_ok()
        }
        "vector" => serde_json::from_str::<Vec<f32>>(input).is_ok(),
        _ => {
            // Unknown type - return true by default to be permissive
            true
        }
    };

    Ok(is_valid)
}

fn parse_date_simple(s: &str) -> Result<(), ()> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(());
    }
    let _year = parts[0].parse::<i32>().map_err(|_| ())?;
    let month = parts[1].parse::<u32>().map_err(|_| ())?;
    let day = parts[2].parse::<u32>().map_err(|_| ())?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(());
    }
    Ok(())
}

fn parse_timestamp_simple(s: &str) -> Result<(), ()> {
    // Accept "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
    let parts: Vec<&str> = if s.contains('T') {
        s.split('T').collect()
    } else {
        s.split(' ').collect()
    };
    if parts.len() != 2 {
        return Err(());
    }
    parse_date_simple(parts[0])?;
    parse_time_simple(parts[1])?;
    Ok(())
}

fn parse_time_simple(s: &str) -> Result<(), ()> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 {
        return Err(());
    }
    let hour = parts[0].parse::<u32>().map_err(|_| ())?;
    let minute = parts[1].parse::<u32>().map_err(|_| ())?;
    if hour > 23 || minute > 59 {
        return Err(());
    }
    if parts.len() >= 3 {
        let sec_parts: Vec<&str> = parts[2].split('.').collect();
        let second = sec_parts[0].parse::<u32>().map_err(|_| ())?;
        if second > 59 {
            return Err(());
        }
    }
    Ok(())
}

fn parse_uuid_simple(s: &str) -> Result<(), ()> {
    let s = s.trim();
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    if s.len() != 36 {
        return Err(());
    }
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return Err(());
    }
    if parts[0].len() != 8
        || parts[1].len() != 4
        || parts[2].len() != 4
        || parts[3].len() != 4
        || parts[4].len() != 12
    {
        return Err(());
    }
    // Check all parts are hex
    for part in parts {
        if !part.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(());
        }
    }
    Ok(())
}

/// Implements PostgreSQL's pg_get_viewdef() function.
/// Returns the SQL definition of a view. Accepts view name as text or regclass OID.
pub(crate) fn pg_get_viewdef(
    view_name: &str,
    pretty: bool,
) -> Result<String, crate::tcop::engine::EngineError> {
    use crate::catalog::table::TableKind;
    use crate::catalog::{SearchPath, with_catalog_read};

    // Try to look up the view in the catalog
    let parts: Vec<String> = view_name.split('.').map(|s| s.trim().to_string()).collect();
    let result = with_catalog_read(|catalog| {
        let search_path = SearchPath::default();
        catalog
            .resolve_table(&parts, &search_path)
            .ok()
            .and_then(|table| {
                if matches!(table.kind(), TableKind::View | TableKind::MaterializedView) {
                    table.view_definition().cloned()
                } else {
                    None
                }
            })
    });

    match result {
        Some(query) => Ok(render_query_to_sql(&query, pretty)),
        None => Ok(format!("-- View definition for: {view_name}")),
    }
}

fn render_query_to_sql(query: &crate::parser::ast::Query, pretty: bool) -> String {
    use crate::parser::ast::QueryExpr;

    let mut sql = String::new();

    // Handle WITH clause if present
    if let Some(with_clause) = &query.with {
        sql.push_str("WITH ");
        if with_clause.recursive {
            sql.push_str("RECURSIVE ");
        }
        for (i, cte) in with_clause.ctes.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&cte.name);
            if !cte.column_names.is_empty() {
                sql.push('(');
                for (j, col) in cte.column_names.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(col);
                }
                sql.push(')');
            }
            sql.push_str(" AS (");
            sql.push_str(&render_query_to_sql(&cte.query, false));
            sql.push(')');
        }
        sql.push(' ');
    }

    // Handle query body
    match &query.body {
        QueryExpr::Select(select) => {
            sql.push_str(&render_select_to_sql(select));
        }
        QueryExpr::Nested(nested) => {
            sql.push('(');
            sql.push_str(&render_query_to_sql(nested, false));
            sql.push(')');
        }
        QueryExpr::Values(rows) => {
            sql.push_str("VALUES ");
            for (i, row) in rows.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (j, expr) in row.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&render_expr_to_sql(expr));
                }
                sql.push(')');
            }
        }
        QueryExpr::SetOperation {
            left,
            op,
            quantifier,
            right,
        } => {
            // Create temporary Query for left side
            let left_query = crate::parser::ast::Query {
                with: None,
                body: (**left).clone(),
                order_by: vec![],
                limit: None,
                offset: None,
            };
            sql.push_str(&render_query_to_sql(&left_query, false));
            sql.push(' ');
            sql.push_str(match op {
                crate::parser::ast::SetOperator::Union => "UNION",
                crate::parser::ast::SetOperator::Intersect => "INTERSECT",
                crate::parser::ast::SetOperator::Except => "EXCEPT",
            });
            sql.push(' ');
            sql.push_str(match quantifier {
                crate::parser::ast::SetQuantifier::All => "ALL",
                crate::parser::ast::SetQuantifier::Distinct => "DISTINCT",
            });
            sql.push(' ');
            // Create temporary Query for right side
            let right_query = crate::parser::ast::Query {
                with: None,
                body: (**right).clone(),
                order_by: vec![],
                limit: None,
                offset: None,
            };
            sql.push_str(&render_query_to_sql(&right_query, false));
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
            // DML statements in CTEs not yet fully supported
            sql.push_str("<data-modifying CTE>");
        }
    }

    // Handle ORDER BY
    if !query.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        for (i, order) in query.order_by.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&render_expr_to_sql(&order.expr));
            if let Some(asc) = order.ascending
                && !asc
            {
                sql.push_str(" DESC");
            }
            if let Some(op) = &order.using_operator {
                sql.push_str(" USING ");
                sql.push_str(op);
            }
        }
    }

    // Handle LIMIT
    if let Some(ref limit) = query.limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&render_expr_to_sql(limit));
    }

    // Handle OFFSET
    if let Some(ref offset) = query.offset {
        sql.push_str(" OFFSET ");
        sql.push_str(&render_expr_to_sql(offset));
    }

    if pretty {
        // Add basic pretty-printing (newlines and indentation)
        sql = sql.replace(" FROM ", "\n FROM ");
        sql = sql.replace(" WHERE ", "\n WHERE ");
        sql = sql.replace(" ORDER BY ", "\n ORDER BY ");
    }

    sql
}

fn render_select_to_sql(select: &crate::parser::ast::SelectStatement) -> String {
    let mut sql = String::from("SELECT");

    if let Some(ref quantifier) = select.quantifier {
        match quantifier {
            crate::parser::ast::SelectQuantifier::Distinct => sql.push_str(" DISTINCT"),
            crate::parser::ast::SelectQuantifier::All => sql.push_str(" ALL"),
        }
    }

    if !select.distinct_on.is_empty() {
        sql.push_str(" DISTINCT ON (");
        for (i, expr) in select.distinct_on.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&render_expr_to_sql(expr));
        }
        sql.push(')');
    }

    // Target list
    sql.push(' ');
    for (i, target) in select.targets.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&render_expr_to_sql(&target.expr));
        if let Some(alias) = &target.alias {
            sql.push_str(" AS ");
            sql.push_str(alias);
        }
    }

    // FROM clause
    if !select.from.is_empty() {
        sql.push_str(" FROM ");
        for (i, from) in select.from.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&render_table_expr_to_sql(from));
        }
    }

    // WHERE clause
    if let Some(ref where_expr) = select.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&render_expr_to_sql(where_expr));
    }

    // GROUP BY
    if !select.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        for (i, group) in select.group_by.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            match group {
                crate::parser::ast::GroupByExpr::Expr(expr) => {
                    sql.push_str(&render_expr_to_sql(expr));
                }
                crate::parser::ast::GroupByExpr::Rollup(exprs) => {
                    sql.push_str("ROLLUP(");
                    for (j, expr) in exprs.iter().enumerate() {
                        if j > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(&render_expr_to_sql(expr));
                    }
                    sql.push(')');
                }
                crate::parser::ast::GroupByExpr::Cube(exprs) => {
                    sql.push_str("CUBE(");
                    for (j, expr) in exprs.iter().enumerate() {
                        if j > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(&render_expr_to_sql(expr));
                    }
                    sql.push(')');
                }
                crate::parser::ast::GroupByExpr::GroupingSets(sets) => {
                    sql.push_str("GROUPING SETS(");
                    for (j, set) in sets.iter().enumerate() {
                        if j > 0 {
                            sql.push_str(", ");
                        }
                        sql.push('(');
                        for (k, expr) in set.iter().enumerate() {
                            if k > 0 {
                                sql.push_str(", ");
                            }
                            sql.push_str(&render_expr_to_sql(expr));
                        }
                        sql.push(')');
                    }
                    sql.push(')');
                }
            }
        }
    }

    // HAVING
    if let Some(ref having) = select.having {
        sql.push_str(" HAVING ");
        sql.push_str(&render_expr_to_sql(having));
    }

    sql
}

fn render_table_expr_to_sql(table: &crate::parser::ast::TableExpression) -> String {
    use crate::parser::ast::TableExpression;

    match table {
        TableExpression::Relation(table_ref) => {
            let mut sql = table_ref.name.join(".");
            if let Some(a) = &table_ref.alias {
                sql.push_str(" AS ");
                sql.push_str(a);
            }
            sql
        }
        TableExpression::Subquery(subquery_ref) => {
            let mut sql = String::from("(");
            sql.push_str(&render_query_to_sql(&subquery_ref.query, false));
            sql.push(')');
            if let Some(a) = &subquery_ref.alias {
                sql.push_str(" AS ");
                sql.push_str(a);
            }
            sql
        }
        TableExpression::Function(func_ref) => {
            let mut func_sql = func_ref.name.join(".");
            func_sql.push('(');
            for (i, arg) in func_ref.args.iter().enumerate() {
                if i > 0 {
                    func_sql.push_str(", ");
                }
                func_sql.push_str(&render_expr_to_sql(arg));
            }
            func_sql.push(')');
            if let Some(a) = &func_ref.alias {
                func_sql.push_str(" AS ");
                func_sql.push_str(a);
            }
            func_sql
        }
        TableExpression::Join(join_expr) => {
            let mut sql = render_table_expr_to_sql(&join_expr.left);
            sql.push(' ');
            sql.push_str(match join_expr.kind {
                crate::parser::ast::JoinType::Inner => "JOIN",
                crate::parser::ast::JoinType::Left => "LEFT JOIN",
                crate::parser::ast::JoinType::Right => "RIGHT JOIN",
                crate::parser::ast::JoinType::Full => "FULL JOIN",
                crate::parser::ast::JoinType::Cross => "CROSS JOIN",
            });
            sql.push(' ');
            sql.push_str(&render_table_expr_to_sql(&join_expr.right));
            if let Some(cond) = &join_expr.condition {
                match cond {
                    crate::parser::ast::JoinCondition::On(expr) => {
                        sql.push_str(" ON ");
                        sql.push_str(&render_expr_to_sql(expr));
                    }
                    crate::parser::ast::JoinCondition::Using(cols) => {
                        sql.push_str(" USING (");
                        for (i, col) in cols.iter().enumerate() {
                            if i > 0 {
                                sql.push_str(", ");
                            }
                            sql.push_str(col);
                        }
                        sql.push(')');
                    }
                }
            }
            sql
        }
    }
}

fn unary_op_to_sql(op: &crate::parser::ast::UnaryOp) -> &'static str {
    use crate::parser::ast::UnaryOp;
    match op {
        UnaryOp::Plus => "+",
        UnaryOp::Minus => "-",
        UnaryOp::Not => "NOT",
    }
}

fn binary_op_to_sql(op: &crate::parser::ast::BinaryOp) -> &'static str {
    use crate::parser::ast::BinaryOp;
    match op {
        BinaryOp::Or => "OR",
        BinaryOp::And => "AND",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonPath => "#>",
        BinaryOp::JsonPathText => "#>>",
        BinaryOp::JsonConcat => "||",
        BinaryOp::JsonContains => "@>",
        BinaryOp::JsonContainedBy => "<@",
        BinaryOp::JsonPathExists => "@?",
        BinaryOp::JsonPathMatch => "@@",
        BinaryOp::JsonHasKey => "?",
        BinaryOp::JsonHasAny => "?|",
        BinaryOp::JsonHasAll => "?&",
        BinaryOp::JsonDelete => "#-",
        BinaryOp::JsonDeletePath => "#-",
        BinaryOp::ArrayContains => "@>",
        BinaryOp::ArrayContainedBy => "<@",
        BinaryOp::ArrayOverlap => "&&",
        BinaryOp::ArrayConcat => "||",
        BinaryOp::VectorL2Distance => "<->",
        BinaryOp::VectorInnerProduct => "<#>",
        BinaryOp::VectorCosineDistance => "<=>",
    }
}

pub(crate) fn render_expr_to_sql(expr: &crate::parser::ast::Expr) -> String {
    use crate::parser::ast::Expr;

    match expr {
        // Simple literals
        Expr::String(s) => format!("'{}'", s.replace('\'', "''")),
        Expr::Integer(n) => n.to_string(),
        Expr::Float(f) => f.clone(),
        Expr::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Expr::Null => "NULL".to_string(),
        Expr::Default => "DEFAULT".to_string(),
        Expr::MultiColumnSubqueryRef { .. } => "(multi-column subquery ref)".to_string(),

        // Identifiers
        Expr::Identifier(parts) => parts.join("."),

        // Binary operations
        Expr::Binary { left, op, right } => {
            format!(
                "{} {} {}",
                render_expr_to_sql(left),
                binary_op_to_sql(op),
                render_expr_to_sql(right)
            )
        }

        // Unary operations
        Expr::Unary { op, expr } => {
            format!("{} {}", unary_op_to_sql(op), render_expr_to_sql(expr))
        }

        // Function calls
        Expr::FunctionCall { name, args, .. } => {
            let mut func_sql = name.join(".");
            func_sql.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    func_sql.push_str(", ");
                }
                func_sql.push_str(&render_expr_to_sql(arg));
            }
            func_sql.push(')');
            func_sql
        }

        // Cast
        Expr::Cast { expr, type_name } => {
            format!("CAST({} AS {})", render_expr_to_sql(expr), type_name)
        }

        // Case expressions
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            let mut sql = String::from("CASE ");
            sql.push_str(&render_expr_to_sql(operand));
            for (cond, result) in when_then {
                sql.push_str(" WHEN ");
                sql.push_str(&render_expr_to_sql(cond));
                sql.push_str(" THEN ");
                sql.push_str(&render_expr_to_sql(result));
            }
            if let Some(else_result) = else_expr {
                sql.push_str(" ELSE ");
                sql.push_str(&render_expr_to_sql(else_result));
            }
            sql.push_str(" END");
            sql
        }

        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            let mut sql = String::from("CASE");
            for (cond, result) in when_then {
                sql.push_str(" WHEN ");
                sql.push_str(&render_expr_to_sql(cond));
                sql.push_str(" THEN ");
                sql.push_str(&render_expr_to_sql(result));
            }
            if let Some(else_result) = else_expr {
                sql.push_str(" ELSE ");
                sql.push_str(&render_expr_to_sql(else_result));
            }
            sql.push_str(" END");
            sql
        }

        // Subqueries
        Expr::ScalarSubquery(query) | Expr::Exists(query) | Expr::ArraySubquery(query) => {
            format!("({})", render_query_to_sql(query, false))
        }

        // IN expression
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut sql = render_expr_to_sql(expr);
            if *negated {
                sql.push_str(" NOT");
            }
            sql.push_str(" IN (");
            for (i, item) in list.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&render_expr_to_sql(item));
            }
            sql.push(')');
            sql
        }

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let mut sql = render_expr_to_sql(expr);
            if *negated {
                sql.push_str(" NOT");
            }
            sql.push_str(" IN (");
            sql.push_str(&render_query_to_sql(subquery, false));
            sql.push(')');
            sql
        }

        // BETWEEN
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let mut sql = render_expr_to_sql(expr);
            if *negated {
                sql.push_str(" NOT");
            }
            sql.push_str(" BETWEEN ");
            sql.push_str(&render_expr_to_sql(low));
            sql.push_str(" AND ");
            sql.push_str(&render_expr_to_sql(high));
            sql
        }

        // IS NULL
        Expr::IsNull { expr, negated } => {
            let mut sql = render_expr_to_sql(expr);
            sql.push_str(" IS");
            if *negated {
                sql.push_str(" NOT");
            }
            sql.push_str(" NULL");
            sql
        }

        // Wildcard
        Expr::Wildcard => "*".to_string(),
        Expr::QualifiedWildcard(parts) => format!("{}.*", parts.join(".")),

        // Array constructor
        Expr::ArrayConstructor(elements) => {
            let mut sql = String::from("ARRAY[");
            for (i, elem) in elements.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&render_expr_to_sql(elem));
            }
            sql.push(']');
            sql
        }

        // For any other expression types, return a placeholder
        _ => "<expr>".to_string(),
    }
}

// ── Extension & Function execution ──────────────────────────────────────────
