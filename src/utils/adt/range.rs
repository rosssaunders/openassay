//! Range type helpers.
//!
//! Range values are stored as `ScalarValue::Text` in the PG bounds-literal
//! form: `[lower,upper]`, `[lower,upper)`, `(lower,upper]`, `(lower,upper)`.
//! This module parses that text and evaluates the element-containment
//! operator `@>`.
//!
//! Partial scope for Phase 5.4: only `range @> element` lands here. `<@`,
//! `&&`, `=`, `lower()`, `upper()`, `isempty()`, `lower_inc`/`upper_inc`,
//! range-vs-range set operations, and multiranges are deferred — each is a
//! mechanical extension of the same parse-then-compare shape. `lower()`
//! specifically needs typed dispatch (the existing `lower(text)` overload
//! would collide) which is a separate refactor.

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

#[derive(Debug, Clone)]
pub(crate) struct ParsedRange<'a> {
    pub(crate) lower_inclusive: bool,
    pub(crate) upper_inclusive: bool,
    pub(crate) lower: &'a str,
    pub(crate) upper: &'a str,
}

/// Parse a range literal of the form `[a,b]`, `[a,b)`, `(a,b]`, `(a,b)` —
/// returns None if the string doesn't match that exact shape. Does not parse
/// the bound values; callers compare them as-needed against element types.
///
/// Only allows exactly one comma between the bounds — JSON arrays with
/// multiple elements (`[1,2,3]`) return None. Bounds strings may be empty
/// (unbounded), e.g. `(,5)` or `[1,)`.
pub(crate) fn try_parse_range_text(text: &str) -> Option<ParsedRange<'_>> {
    let bytes = text.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    let lower_inclusive = match bytes[0] {
        b'[' => true,
        b'(' => false,
        _ => return None,
    };
    let upper_inclusive = match bytes[bytes.len() - 1] {
        b']' => true,
        b')' => false,
        _ => return None,
    };
    let inside = &text[1..text.len() - 1];
    // Exactly one comma — excludes JSON arrays like `[1,2,3]`.
    let mut comma_count = 0;
    for byte in inside.bytes() {
        if byte == b',' {
            comma_count += 1;
            if comma_count > 1 {
                return None;
            }
        }
    }
    if comma_count != 1 {
        return None;
    }
    let comma_idx = inside.find(',')?;
    let lower = inside[..comma_idx].trim();
    let upper = inside[comma_idx + 1..].trim();
    Some(ParsedRange {
        lower_inclusive,
        upper_inclusive,
        lower,
        upper,
    })
}

/// Attempt to evaluate `range_text @> element`. Returns:
/// - `Ok(Some(bool))` on success
/// - `Ok(None)` if `range_text` doesn't parse as a range (caller falls through
///   to the JSON containment path)
/// - `Err(_)` only if the range parses but the element type can't be compared
///   against the bounds (stricter element typing is a follow-up).
pub(crate) fn try_range_contains_element(
    range_text: &str,
    element: &ScalarValue,
) -> Result<Option<bool>, EngineError> {
    let Some(parsed) = try_parse_range_text(range_text) else {
        return Ok(None);
    };
    if matches!(element, ScalarValue::Null) {
        return Ok(Some(false));
    }
    match element {
        ScalarValue::Int(n) => {
            let value = *n as f64;
            Ok(Some(range_contains_f64(&parsed, value)?))
        }
        ScalarValue::Float(f) => Ok(Some(range_contains_f64(&parsed, *f)?)),
        ScalarValue::Numeric(d) => {
            use rust_decimal::prelude::ToPrimitive;
            let value = d.to_f64().ok_or_else(|| EngineError {
                message: "range @> element: numeric out of range for range comparison".to_string(),
            })?;
            Ok(Some(range_contains_f64(&parsed, value)?))
        }
        _ => Err(EngineError {
            message: format!(
                "range @> element: unsupported element type {element:?} (only numeric elements are \
                 implemented in the Phase 5.4 partial)"
            ),
        }),
    }
}

fn range_contains_f64(parsed: &ParsedRange<'_>, value: f64) -> Result<bool, EngineError> {
    if !parsed.lower.is_empty() {
        let lower: f64 = parsed.lower.parse().map_err(|_| EngineError {
            message: format!("range lower bound not numeric: '{}'", parsed.lower),
        })?;
        let ok = if parsed.lower_inclusive {
            value >= lower
        } else {
            value > lower
        };
        if !ok {
            return Ok(false);
        }
    }
    if !parsed.upper.is_empty() {
        let upper: f64 = parsed.upper.parse().map_err(|_| EngineError {
            message: format!("range upper bound not numeric: '{}'", parsed.upper),
        })?;
        let ok = if parsed.upper_inclusive {
            value <= upper
        } else {
            value < upper
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_range_literals() {
        let r = try_parse_range_text("[1,10)").unwrap();
        assert!(r.lower_inclusive);
        assert!(!r.upper_inclusive);
        assert_eq!(r.lower, "1");
        assert_eq!(r.upper, "10");
    }

    #[test]
    fn rejects_json_arrays_with_multiple_elements() {
        assert!(try_parse_range_text("[1,2,3]").is_none());
    }

    #[test]
    fn accepts_unbounded_bounds() {
        let r = try_parse_range_text("(,5)").unwrap();
        assert_eq!(r.lower, "");
        assert_eq!(r.upper, "5");
    }

    #[test]
    fn half_open_range_excludes_upper() {
        let r = try_parse_range_text("[1,10)").unwrap();
        assert!(range_contains_f64(&r, 1.0).unwrap());
        assert!(range_contains_f64(&r, 9.0).unwrap());
        assert!(!range_contains_f64(&r, 10.0).unwrap());
        assert!(!range_contains_f64(&r, 0.0).unwrap());
    }

    #[test]
    fn closed_range_includes_both() {
        let r = try_parse_range_text("[1,10]").unwrap();
        assert!(range_contains_f64(&r, 1.0).unwrap());
        assert!(range_contains_f64(&r, 10.0).unwrap());
    }
}
