//! PostgreSQL-compatible float4/float8 input/output functions.
//!
//! Translated from PostgreSQL's src/backend/utils/adt/float.c.
//! Same logic, same edge cases, same error messages.

use crate::tcop::engine::EngineError;

/// Parse a string to f64, matching PostgreSQL's float8in() semantics.
/// Handles NaN, Infinity, -Infinity (case insensitive), whitespace trimming,
/// overflow/underflow detection, and bad input validation.
pub fn float8in(input: &str) -> Result<f64, EngineError> {
    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Err(EngineError {
            message: format!(
                "invalid input syntax for type double precision: \"{}\"",
                input
            ),
        });
    }

    // Check for special values (case-insensitive)
    let lower = trimmed.to_lowercase();
    if lower == "nan" {
        return Ok(f64::NAN);
    }
    if lower == "infinity" || lower == "inf" {
        return Ok(f64::INFINITY);
    }
    if lower == "-infinity" || lower == "-inf" {
        return Ok(f64::NEG_INFINITY);
    }

    // Try to parse as f64
    match trimmed.parse::<f64>() {
        Ok(val) => {
            // Check for overflow (infinity from parsing means input was out of range)
            if val.is_infinite() {
                return Err(EngineError {
                    message: format!(
                        "\"{}\" is out of range for type double precision",
                        trimmed
                    ),
                });
            }
            // Check for underflow (value parsed to 0 but input wasn't zero)
            if val == 0.0 && !is_zero_input(trimmed) {
                return Err(EngineError {
                    message: format!(
                        "\"{}\" is out of range for type double precision",
                        trimmed
                    ),
                });
            }
            Ok(val)
        }
        Err(_) => Err(EngineError {
            message: format!(
                "invalid input syntax for type double precision: \"{}\"",
                input
            ),
        }),
    }
}

/// Parse a string to f32 (via f64), matching PostgreSQL's float4in() semantics.
pub fn float4in(input: &str) -> Result<f64, EngineError> {
    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Err(EngineError {
            message: format!("invalid input syntax for type real: \"{}\"", input),
        });
    }

    // Check for special values (case-insensitive)
    let lower = trimmed.to_lowercase();
    if lower == "nan" {
        return Ok(f64::NAN);
    }
    if lower == "infinity" || lower == "inf" {
        return Ok(f64::INFINITY);
    }
    if lower == "-infinity" || lower == "-inf" {
        return Ok(f64::NEG_INFINITY);
    }

    // Try to parse as f64 first (PG parses to double then checks f32 range)
    match trimmed.parse::<f64>() {
        Ok(val) => {
            if val.is_infinite() {
                return Err(EngineError {
                    message: format!("\"{}\" is out of range for type real", trimmed),
                });
            }
            // Check f32 range
            let val32 = val as f32;
            if val32.is_infinite() && !val.is_infinite() {
                return Err(EngineError {
                    message: format!("\"{}\" is out of range for type real", trimmed),
                });
            }
            if val32 == 0.0 && val != 0.0 && !is_zero_input(trimmed) {
                return Err(EngineError {
                    message: format!("\"{}\" is out of range for type real", trimmed),
                });
            }
            // Store as f32-precision value in f64
            Ok(val32 as f64)
        }
        Err(_) => Err(EngineError {
            message: format!("invalid input syntax for type real: \"{}\"", input),
        }),
    }
}

/// Check if a string input represents zero (to distinguish underflow from actual zero).
fn is_zero_input(s: &str) -> bool {
    let trimmed = s.trim().trim_start_matches(['+', '-']);
    // Check if all digits are zero (possibly with decimal point and exponent)
    let mut saw_nonzero = false;
    for ch in trimmed.chars() {
        match ch {
            '0' | '.' => {}
            'e' | 'E' => break, // exponent doesn't matter
            '1'..='9' => {
                saw_nonzero = true;
                break;
            }
            _ => break,
        }
    }
    !saw_nonzero
}

/// Cast f64 (float8) to f32 (float4) with overflow/underflow checking.
/// Matches PostgreSQL's float8_float4() behavior.
pub fn float8_to_float4(val: f64) -> Result<f64, EngineError> {
    if val.is_nan() {
        return Ok(f64::NAN);
    }
    if val.is_infinite() {
        return Ok(val); // Infinity stays Infinity
    }
    let result = val as f32;
    if result.is_infinite() {
        return Err(EngineError {
            message: "value out of range: overflow".to_string(),
        });
    }
    if result == 0.0 && val != 0.0 {
        return Err(EngineError {
            message: "value out of range: underflow".to_string(),
        });
    }
    Ok(result as f64)
}

/// Cast float8 to int2 with PostgreSQL-compatible overflow checking.
/// PostgreSQL rounds towards zero and checks range after rounding.
pub fn float8_to_int2(val: f64) -> Result<i64, EngineError> {
    if val.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to smallint".to_string(),
        });
    }
    // PostgreSQL uses rint() (round to nearest even) for float->int casts
    let rounded = val.round_ties_even();
    if !(-32768.0..=32767.0).contains(&rounded) {
        return Err(EngineError {
            message: "smallint out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// Cast float8 to int4 with PostgreSQL-compatible overflow checking.
pub fn float8_to_int4(val: f64) -> Result<i64, EngineError> {
    if val.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to integer".to_string(),
        });
    }
    let rounded = val.round_ties_even();
    if !(-2147483648.0..=2147483647.0).contains(&rounded) {
        return Err(EngineError {
            message: "integer out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// Cast float8 to int8 with PostgreSQL-compatible overflow checking.
pub fn float8_to_int8(val: f64) -> Result<i64, EngineError> {
    if val.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to bigint".to_string(),
        });
    }
    let rounded = val.round_ties_even();
    // i64::MIN = -9223372036854775808, i64::MAX = 9223372036854775807
    // As float8, the max exact i64 is approximate
    if !(-9223372036854775808.0..9223372036854775808.0).contains(&rounded) {
        return Err(EngineError {
            message: "bigint out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// Cast float4 to int2 with PostgreSQL-compatible overflow checking.
pub fn float4_to_int2(val: f64) -> Result<i64, EngineError> {
    let val32 = val as f32;
    if val32.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to smallint".to_string(),
        });
    }
    let rounded = (val32 as f64).round_ties_even();
    if !(-32768.0..=32767.0).contains(&rounded) {
        return Err(EngineError {
            message: "smallint out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// Cast float4 to int4 with PostgreSQL-compatible overflow checking.
pub fn float4_to_int4(val: f64) -> Result<i64, EngineError> {
    let val32 = val as f32;
    if val32.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to integer".to_string(),
        });
    }
    let rounded = (val32 as f64).round_ties_even();
    if !(-2147483648.0..=2147483647.0).contains(&rounded) {
        return Err(EngineError {
            message: "integer out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// Cast float4 to int8 with PostgreSQL-compatible overflow checking.
pub fn float4_to_int8(val: f64) -> Result<i64, EngineError> {
    let val32 = val as f32;
    if val32.is_nan() {
        return Err(EngineError {
            message: "cannot cast NaN to bigint".to_string(),
        });
    }
    let rounded = (val32 as f64).round_ties_even();
    if !(-9223372036854775808.0..9223372036854775808.0).contains(&rounded) {
        return Err(EngineError {
            message: "bigint out of range".to_string(),
        });
    }
    Ok(rounded as i64)
}

/// PostgreSQL float comparison: NaN = NaN is true, NaN > everything else.
/// This matches PostgreSQL's float8_cmp_internal().
pub fn float8_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    if a.is_nan() {
        if b.is_nan() {
            std::cmp::Ordering::Equal
        } else {
            std::cmp::Ordering::Greater // NaN > everything
        }
    } else if b.is_nan() || a < b {
        std::cmp::Ordering::Less
    } else if a > b {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

/// PostgreSQL-compatible float equality: NaN = NaN is true
pub fn float8_eq(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        true
    } else {
        a == b
    }
}

/// PostgreSQL-compatible float less-than: NaN > everything
pub fn float8_lt(a: f64, b: f64) -> bool {
    float8_cmp(a, b) == std::cmp::Ordering::Less
}

/// PostgreSQL-compatible float greater-than: NaN > everything
pub fn float8_gt(a: f64, b: f64) -> bool {
    float8_cmp(a, b) == std::cmp::Ordering::Greater
}

/// PostgreSQL-compatible float less-or-equal
pub fn float8_le(a: f64, b: f64) -> bool {
    float8_cmp(a, b) != std::cmp::Ordering::Greater
}

/// PostgreSQL-compatible float greater-or-equal
pub fn float8_ge(a: f64, b: f64) -> bool {
    float8_cmp(a, b) != std::cmp::Ordering::Less
}

/// PostgreSQL-compatible float not-equal
pub fn float8_ne(a: f64, b: f64) -> bool {
    !float8_eq(a, b)
}

/// Check if the result of a float8 operation overflowed.
/// PostgreSQL checks this after arithmetic operations.
pub fn check_float8_overflow(val: f64, operation: &str) -> Result<f64, EngineError> {
    if val.is_infinite() {
        return Err(EngineError {
            message: format!("value out of range: overflow in {}", operation),
        });
    }
    Ok(val)
}
