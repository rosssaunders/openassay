use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::misc::{parse_f64_scalar, parse_i64_scalar};

pub(crate) fn numeric_mod(
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    // Handle different numeric types
    let left_num = parse_numeric_operand(&left)?;
    let right_num = parse_numeric_operand(&right)?;

    // Check for zero divisor
    let right_is_zero = matches!(right_num, NumericOperand::Int(0))
        || matches!(right_num, NumericOperand::Float(v) if v == 0.0)
        || matches!(right_num, NumericOperand::Numeric(v) if v.is_zero());
    if right_is_zero {
        return Err(EngineError {
            message: "division by zero".to_string(),
        });
    }

    match (left_num, right_num) {
        (NumericOperand::Int(a), NumericOperand::Int(b)) => Ok(ScalarValue::Int(
            crate::utils::adt::int_arithmetic::int4_mod(a, b)?,
        )),
        (NumericOperand::Int(a), NumericOperand::Numeric(b)) => {
            let a_decimal = rust_decimal::Decimal::from(a);
            Ok(ScalarValue::Numeric(a_decimal % b))
        }
        (NumericOperand::Numeric(a), NumericOperand::Int(b)) => {
            let b_decimal = rust_decimal::Decimal::from(b);
            Ok(ScalarValue::Numeric(a % b_decimal))
        }
        (NumericOperand::Numeric(a), NumericOperand::Numeric(b)) => Ok(ScalarValue::Numeric(a % b)),
        // For float operations, convert to integer-based mod (following PostgreSQL behavior)
        _ => {
            let left_int = parse_i64_scalar(&left, "operator % expects integer values")?;
            let right_int = parse_i64_scalar(&right, "operator % expects integer values")?;
            Ok(ScalarValue::Int(
                crate::utils::adt::int_arithmetic::int4_mod(left_int, right_int)?,
            ))
        }
    }
}

pub(crate) fn coerce_to_f64(v: &ScalarValue, context: &str) -> Result<f64, EngineError> {
    match v {
        ScalarValue::Int(i) => Ok(*i as f64),
        ScalarValue::Float(f) => Ok(*f),
        ScalarValue::Numeric(d) => Ok(d.to_string().parse::<f64>().map_err(|_| EngineError {
            message: format!("{context} cannot convert decimal to float"),
        })?),
        _ => Err(EngineError {
            message: format!("{context} expects numeric argument"),
        }),
    }
}

pub(crate) fn gcd_i64(a: i64, b: i64) -> Result<i64, EngineError> {
    let mut left = i128::from(a).abs();
    let mut right = i128::from(b).abs();
    while right != 0 {
        let next = left % right;
        left = right;
        right = next;
    }
    i64::try_from(left).map_err(|_| EngineError {
        message: "bigint out of range".to_string(),
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NumericOperand {
    Int(i64),
    Float(f64),
    Numeric(rust_decimal::Decimal),
}

pub(crate) fn parse_numeric_operand(value: &ScalarValue) -> Result<NumericOperand, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(NumericOperand::Int(*v)),
        ScalarValue::Float(v) => Ok(NumericOperand::Float(*v)),
        ScalarValue::Numeric(v) => Ok(NumericOperand::Numeric(*v)),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(NumericOperand::Int(parsed));
            }
            if let Ok(parsed) = v.parse::<rust_decimal::Decimal>() {
                return Ok(NumericOperand::Numeric(parsed));
            }
            if let Ok(parsed) = v.parse::<f64>() {
                return Ok(NumericOperand::Float(parsed));
            }
            Err(EngineError {
                message: "numeric operation expects numeric values".to_string(),
            })
        }
        ScalarValue::Array(_) => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
        _ => Err(EngineError {
            message: "numeric operation expects numeric values".to_string(),
        }),
    }
}

pub(crate) fn eval_width_bucket(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let value = parse_f64_scalar(&args[0], "width_bucket() expects numeric value")?;
    let min = parse_f64_scalar(&args[1], "width_bucket() expects numeric min")?;
    let max = parse_f64_scalar(&args[2], "width_bucket() expects numeric max")?;
    let count = parse_i64_scalar(&args[3], "width_bucket() expects integer count")?;
    if count <= 0 {
        return Ok(ScalarValue::Int(0));
    }
    if min == max {
        return Ok(ScalarValue::Int(1));
    }
    let buckets = count as f64;
    let compute_interior_bucket = |numerator: f64, denominator: f64| -> i64 {
        let raw = ((numerator * buckets) / denominator).floor();
        let clamped = if !raw.is_finite() {
            count - 1
        } else {
            raw.max(0.0).min((count - 1) as f64) as i64
        };
        clamped + 1
    };
    let bucket = if min < max {
        if value < min {
            0
        } else if value >= max {
            count.saturating_add(1)
        } else {
            compute_interior_bucket(value - min, max - min)
        }
    } else if value > min {
        0
    } else if value <= max {
        count.saturating_add(1)
    } else {
        compute_interior_bucket(min - value, min - max)
    };
    Ok(ScalarValue::Int(bucket))
}

pub(crate) fn eval_scale(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let rendered = value.render();
    let trimmed = rendered.trim();
    let main = if let Some(idx) = trimmed.find('e').or_else(|| trimmed.find('E')) {
        &trimmed[..idx]
    } else {
        trimmed
    };
    let scale = main
        .split_once('.')
        .map(|(_, frac)| frac.len() as i64)
        .unwrap_or(0);
    Ok(ScalarValue::Int(scale))
}

pub(crate) fn eval_factorial(value: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let n = parse_i64_scalar(value, "factorial() expects integer")?;
    if n < 0 {
        return Err(EngineError {
            message: "factorial() expects non-negative integer".to_string(),
        });
    }
    let mut acc: i64 = 1;
    for i in 1..=n {
        acc = acc.checked_mul(i).ok_or_else(|| EngineError {
            message: "factorial() overflowed".to_string(),
        })?;
    }
    Ok(ScalarValue::Int(acc))
}
