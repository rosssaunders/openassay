// Integer arithmetic with PostgreSQL-compatible overflow detection
//
// Translates directly from PostgreSQL C source code:
// - src/backend/utils/adt/int.c (int2pl, int2mi, int2mul, int2div for int2)
// - src/backend/utils/adt/int.c (int4pl, int4mi, int4mul, int4div for int4)
// - src/backend/utils/adt/int8.c (int8 operations)
//
// PostgreSQL uses pg_add_s16_overflow(), pg_mul_s32_overflow() from common/int.h
// and throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE errors on overflow.

use crate::tcop::engine::EngineError;

/// int2 (smallint) range: -32768 to 32767
pub const INT2_MIN: i64 = i16::MIN as i64;
pub const INT2_MAX: i64 = i16::MAX as i64;

/// int4 (integer) range: -2147483648 to 2147483647
pub const INT4_MIN: i64 = i32::MIN as i64;
pub const INT4_MAX: i64 = i32::MAX as i64;

/// int8 (bigint) range: -9223372036854775808 to 9223372036854775807
pub const INT8_MIN: i64 = i64::MIN;
pub const INT8_MAX: i64 = i64::MAX;

/// Validate that a value is in int2 range
pub fn validate_int2(value: i64) -> Result<i16, EngineError> {
    if !(INT2_MIN..=INT2_MAX).contains(&value) {
        return Err(EngineError {
            message: "smallint out of range".to_string(),
        });
    }
    Ok(value as i16)
}

/// Validate that a value is in int4 range
pub fn validate_int4(value: i64) -> Result<i32, EngineError> {
    if !(INT4_MIN..=INT4_MAX).contains(&value) {
        return Err(EngineError {
            message: "integer out of range".to_string(),
        });
    }
    Ok(value as i32)
}

/// int2 addition with overflow check (matches PostgreSQL int2pl)
pub fn int2_add(a: i64, b: i64) -> Result<i64, EngineError> {
    // Validate inputs are in int2 range
    let a_i16 = validate_int2(a)?;
    let b_i16 = validate_int2(b)?;

    // Use checked arithmetic to detect overflow
    match a_i16.checked_add(b_i16) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "smallint out of range".to_string(),
        }),
    }
}

/// int2 subtraction with overflow check (matches PostgreSQL int2mi)
pub fn int2_sub(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i16 = validate_int2(a)?;
    let b_i16 = validate_int2(b)?;

    match a_i16.checked_sub(b_i16) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "smallint out of range".to_string(),
        }),
    }
}

/// int2 multiplication with overflow check (matches PostgreSQL int2mul)
pub fn int2_mul(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i16 = validate_int2(a)?;
    let b_i16 = validate_int2(b)?;

    match a_i16.checked_mul(b_i16) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "smallint out of range".to_string(),
        }),
    }
}

/// int2 division with overflow and division-by-zero check (matches PostgreSQL int2div)
pub fn int2_div(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i16 = validate_int2(a)?;
    let b_i16 = validate_int2(b)?;

    if b_i16 == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    // Special case: -32768 / -1 overflows in two's complement
    if a_i16 == i16::MIN && b_i16 == -1 {
        return Err(EngineError {
            message: "smallint out of range".to_string(),
        });
    }

    Ok((a_i16 / b_i16) as i64)
}

/// int2 modulo (matches PostgreSQL int2mod)
pub fn int2_mod(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i16 = validate_int2(a)?;
    let b_i16 = validate_int2(b)?;

    if b_i16 == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    // Modulo doesn't overflow, even for -32768 % -1 (result is 0)
    Ok((a_i16 % b_i16) as i64)
}

/// int4 addition with overflow check (matches PostgreSQL int4pl)
pub fn int4_add(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i32 = validate_int4(a)?;
    let b_i32 = validate_int4(b)?;

    match a_i32.checked_add(b_i32) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "integer out of range".to_string(),
        }),
    }
}

/// int4 subtraction with overflow check (matches PostgreSQL int4mi)
pub fn int4_sub(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i32 = validate_int4(a)?;
    let b_i32 = validate_int4(b)?;

    match a_i32.checked_sub(b_i32) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "integer out of range".to_string(),
        }),
    }
}

/// int4 multiplication with overflow check (matches PostgreSQL int4mul)
pub fn int4_mul(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i32 = validate_int4(a)?;
    let b_i32 = validate_int4(b)?;

    match a_i32.checked_mul(b_i32) {
        Some(result) => Ok(result as i64),
        None => Err(EngineError {
            message: "integer out of range".to_string(),
        }),
    }
}

/// int4 division with overflow and division-by-zero check (matches PostgreSQL int4div)
pub fn int4_div(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i32 = validate_int4(a)?;
    let b_i32 = validate_int4(b)?;

    if b_i32 == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    // Special case: -2147483648 / -1 overflows
    if a_i32 == i32::MIN && b_i32 == -1 {
        return Err(EngineError {
            message: "integer out of range".to_string(),
        });
    }

    Ok((a_i32 / b_i32) as i64)
}

/// int4 modulo (matches PostgreSQL int4mod)
pub fn int4_mod(a: i64, b: i64) -> Result<i64, EngineError> {
    let a_i32 = validate_int4(a)?;
    let b_i32 = validate_int4(b)?;

    if b_i32 == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    Ok((a_i32 % b_i32) as i64)
}

/// int8 addition with overflow check (matches PostgreSQL int8pl)
pub fn int8_add(a: i64, b: i64) -> Result<i64, EngineError> {
    match a.checked_add(b) {
        Some(result) => Ok(result),
        None => Err(EngineError {
            message: "bigint out of range".to_string(),
        }),
    }
}

/// int8 subtraction with overflow check (matches PostgreSQL int8mi)
pub fn int8_sub(a: i64, b: i64) -> Result<i64, EngineError> {
    match a.checked_sub(b) {
        Some(result) => Ok(result),
        None => Err(EngineError {
            message: "bigint out of range".to_string(),
        }),
    }
}

/// int8 multiplication with overflow check (matches PostgreSQL int8mul)
pub fn int8_mul(a: i64, b: i64) -> Result<i64, EngineError> {
    match a.checked_mul(b) {
        Some(result) => Ok(result),
        None => Err(EngineError {
            message: "bigint out of range".to_string(),
        }),
    }
}

/// int8 division with overflow and division-by-zero check (matches PostgreSQL int8div)
pub fn int8_div(a: i64, b: i64) -> Result<i64, EngineError> {
    if b == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    // Special case: i64::MIN / -1 overflows
    if a == i64::MIN && b == -1 {
        return Err(EngineError {
            message: "bigint out of range".to_string(),
        });
    }

    Ok(a / b)
}

/// int8 modulo (matches PostgreSQL int8mod)
pub fn int8_mod(a: i64, b: i64) -> Result<i64, EngineError> {
    if b == 0 {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    Ok(a % b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int2_overflow_add() {
        // 32767 + 1 should overflow
        assert!(int2_add(32767, 1).is_err());
        assert_eq!(
            int2_add(32767, 1).unwrap_err().message,
            "smallint out of range"
        );

        // -32768 + (-1) should overflow
        assert!(int2_add(-32768, -1).is_err());

        // Valid addition
        assert_eq!(int2_add(100, 200).unwrap(), 300);
    }

    #[test]
    fn test_int2_overflow_mul() {
        // 200 * 200 = 40000 exceeds int2 max (32767)
        assert!(int2_mul(200, 200).is_err());

        // Valid multiplication
        assert_eq!(int2_mul(100, 100).unwrap(), 10000);
    }

    #[test]
    fn test_int2_division_by_zero() {
        assert!(int2_div(100, 0).is_err());
        assert_eq!(int2_div(100, 0).unwrap_err().message, "division by zero");
    }

    #[test]
    fn test_int2_min_div_minus_one() {
        // -32768 / -1 should overflow
        assert!(int2_div(-32768, -1).is_err());
        assert_eq!(
            int2_div(-32768, -1).unwrap_err().message,
            "smallint out of range"
        );
    }

    #[test]
    fn test_int4_overflow() {
        // 2147483647 + 1 should overflow
        assert!(int4_add(2147483647, 1).is_err());
        assert_eq!(
            int4_add(2147483647, 1).unwrap_err().message,
            "integer out of range"
        );
    }

    #[test]
    fn test_int8_overflow() {
        // i64::MAX + 1 should overflow
        assert!(int8_add(i64::MAX, 1).is_err());
        assert_eq!(
            int8_add(i64::MAX, 1).unwrap_err().message,
            "bigint out of range"
        );
    }
}
