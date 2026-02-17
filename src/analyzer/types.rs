//! Type inference and coercion rules.
//!
//! Provides basic type checking for expressions in contexts that require
//! specific types (e.g., WHERE clauses must be boolean, LIMIT must be numeric).
//! Also defines PostgreSQL's implicit cast compatibility matrix.

use crate::catalog::TypeSignature;
use crate::parser::ast::Expr;
use crate::tcop::engine::EngineError;

/// PostgreSQL implicit cast compatibility.
/// Returns true if a value of type `from` can be implicitly coerced to `to`.
pub fn can_coerce_implicit(from: &TypeSignature, to: &TypeSignature) -> bool {
    if from == to {
        return true;
    }
    matches!(
        (from, to),
        // Numeric promotions
        (
            TypeSignature::Int8,
            TypeSignature::Float8 | TypeSignature::Numeric
        ) | (TypeSignature::Numeric, TypeSignature::Float8)
            | (
                TypeSignature::Text,
                TypeSignature::Date | TypeSignature::Timestamp
            )
            | (TypeSignature::Date, TypeSignature::Timestamp)
    )
}

/// Check that two types are comparable (for = < > operators etc).
pub fn types_are_comparable(a: &TypeSignature, b: &TypeSignature) -> bool {
    if a == b {
        return true;
    }
    // Numeric types can be compared with each other
    if is_numeric(a) && is_numeric(b) {
        return true;
    }
    // Text types can be compared with text
    if a == &TypeSignature::Text || b == &TypeSignature::Text {
        return true; // PostgreSQL is lenient here with implicit casts
    }
    // Date and Timestamp are comparable
    if is_temporal(a) && is_temporal(b) {
        return true;
    }
    false
}

/// Returns true if the type is numeric.
pub fn is_numeric(t: &TypeSignature) -> bool {
    matches!(
        t,
        TypeSignature::Int8 | TypeSignature::Float8 | TypeSignature::Numeric
    )
}

/// Returns true if the type is temporal (date/timestamp).
pub fn is_temporal(t: &TypeSignature) -> bool {
    matches!(t, TypeSignature::Date | TypeSignature::Timestamp)
}

/// Light validation: check that an expression used in a boolean context
/// (WHERE, HAVING, JOIN ON) is not obviously non-boolean.
///
/// We only flag clear violations like literal strings/numbers. The executor
/// handles the full evaluation, so this is a best-effort early check.
pub fn check_expr_for_boolean_context(expr: &Expr) -> Result<(), EngineError> {
    match expr {
        // These are clearly not boolean when used bare
        Expr::String(_) => Err(EngineError {
            message: "argument of WHERE must be type boolean, not type text".to_string(),
        }),
        // Everything else we let through — the executor handles it
        _ => Ok(()),
    }
}

/// Check that an expression used as LIMIT/OFFSET is plausibly numeric.
pub fn check_expr_is_numeric(expr: &Expr) -> Result<(), EngineError> {
    match expr {
        Expr::String(s) => {
            // PostgreSQL allows string literals if they parse as numbers
            if s.parse::<i64>().is_err() && s.parse::<f64>().is_err() {
                return Err(EngineError {
                    message: format!("invalid input syntax for type bigint: \"{s}\""),
                });
            }
            Ok(())
        }
        Expr::Boolean(_) => Err(EngineError {
            message: "argument of LIMIT must be type bigint, not type boolean".to_string(),
        }),
        _ => Ok(()),
    }
}

/// Find the common supertype for a set of types (e.g., for UNION columns or CASE branches).
pub fn find_common_type(types: &[TypeSignature]) -> Option<TypeSignature> {
    if types.is_empty() {
        return None;
    }
    let mut result = types[0];
    for t in &types[1..] {
        result = common_supertype(&result, t)?;
    }
    Some(result)
}

/// Find the common supertype of two types.
fn common_supertype(a: &TypeSignature, b: &TypeSignature) -> Option<TypeSignature> {
    if a == b {
        return Some(*a);
    }
    match (a, b) {
        // Numeric: prefer Float8 over Int8
        (TypeSignature::Int8, TypeSignature::Float8)
        | (TypeSignature::Float8, TypeSignature::Int8) => Some(TypeSignature::Float8),
        // Temporal: prefer Timestamp over Date
        (TypeSignature::Date, TypeSignature::Timestamp)
        | (TypeSignature::Timestamp, TypeSignature::Date) => Some(TypeSignature::Timestamp),
        // Text is the universal fallback
        (TypeSignature::Text, _) | (_, TypeSignature::Text) => Some(TypeSignature::Text),
        _ => None,
    }
}

#[cfg(test)]
mod type_tests {
    use super::*;

    #[test]
    fn test_implicit_coercion_same_type() {
        assert!(can_coerce_implicit(
            &TypeSignature::Int8,
            &TypeSignature::Int8
        ));
        assert!(can_coerce_implicit(
            &TypeSignature::Text,
            &TypeSignature::Text
        ));
    }

    #[test]
    fn test_implicit_coercion_int_to_float() {
        assert!(can_coerce_implicit(
            &TypeSignature::Int8,
            &TypeSignature::Float8
        ));
    }

    #[test]
    fn test_implicit_coercion_not_float_to_int() {
        assert!(!can_coerce_implicit(
            &TypeSignature::Float8,
            &TypeSignature::Int8
        ));
    }

    #[test]
    fn test_implicit_coercion_date_to_timestamp() {
        assert!(can_coerce_implicit(
            &TypeSignature::Date,
            &TypeSignature::Timestamp
        ));
    }

    #[test]
    fn test_implicit_coercion_text_to_date() {
        assert!(can_coerce_implicit(
            &TypeSignature::Text,
            &TypeSignature::Date
        ));
    }

    #[test]
    fn test_common_type_int_float() {
        assert_eq!(
            find_common_type(&[TypeSignature::Int8, TypeSignature::Float8]),
            Some(TypeSignature::Float8)
        );
    }

    #[test]
    fn test_common_type_date_timestamp() {
        assert_eq!(
            find_common_type(&[TypeSignature::Date, TypeSignature::Timestamp]),
            Some(TypeSignature::Timestamp)
        );
    }

    #[test]
    fn test_common_type_with_text() {
        assert_eq!(
            find_common_type(&[TypeSignature::Int8, TypeSignature::Text]),
            Some(TypeSignature::Text)
        );
    }

    #[test]
    fn test_check_boolean_context_string_literal() {
        let expr = Expr::String("hello".to_string());
        assert!(check_expr_for_boolean_context(&expr).is_err());
    }

    #[test]
    fn test_check_boolean_context_comparison() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Integer(1)),
            op: crate::parser::ast::BinaryOp::Eq,
            right: Box::new(Expr::Integer(1)),
        };
        assert!(check_expr_for_boolean_context(&expr).is_ok());
    }

    #[test]
    fn test_check_numeric_string_valid() {
        assert!(check_expr_is_numeric(&Expr::String("42".to_string())).is_ok());
    }

    #[test]
    fn test_check_numeric_string_invalid() {
        assert!(check_expr_is_numeric(&Expr::String("abc".to_string())).is_err());
    }

    #[test]
    fn test_check_numeric_boolean_rejected() {
        assert!(check_expr_is_numeric(&Expr::Boolean(true)).is_err());
    }

    #[test]
    fn test_types_comparable_same() {
        assert!(types_are_comparable(
            &TypeSignature::Int8,
            &TypeSignature::Int8
        ));
    }

    #[test]
    fn test_types_comparable_numeric_mix() {
        assert!(types_are_comparable(
            &TypeSignature::Int8,
            &TypeSignature::Float8
        ));
    }

    #[test]
    fn test_types_comparable_temporal() {
        assert!(types_are_comparable(
            &TypeSignature::Date,
            &TypeSignature::Timestamp
        ));
    }

    #[test]
    fn test_types_not_comparable_bool_date() {
        assert!(!types_are_comparable(
            &TypeSignature::Bool,
            &TypeSignature::Date
        ));
    }
}
