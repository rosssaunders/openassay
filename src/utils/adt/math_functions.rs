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
    let left_int = parse_i64_scalar(&left, "operator % expects integer values")?;
    let right_int = parse_i64_scalar(&right, "operator % expects integer values")?;
    match (left_int, right_int) {
        (_, 0) => Err(EngineError {
            message: "division by zero".to_string(),
        }),
        (a, b) => Ok(ScalarValue::Int(a % b)),
    }
}

pub(crate) fn coerce_to_f64(v: &ScalarValue, context: &str) -> Result<f64, EngineError> {
    match v {
        ScalarValue::Int(i) => Ok(*i as f64),
        ScalarValue::Float(f) => Ok(*f),
        _ => Err(EngineError {
            message: format!("{} expects numeric argument", context),
        }),
    }
}

pub(crate) fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericOperand {
    Int(i64),
    Float(f64),
}

pub(crate) fn parse_numeric_operand(value: &ScalarValue) -> Result<NumericOperand, EngineError> {
    match value {
        ScalarValue::Int(v) => Ok(NumericOperand::Int(*v)),
        ScalarValue::Float(v) => Ok(NumericOperand::Float(*v)),
        ScalarValue::Text(v) => {
            if let Ok(parsed) = v.parse::<i64>() {
                return Ok(NumericOperand::Int(parsed));
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
        return Err(EngineError {
            message: "width_bucket() expects positive count".to_string(),
        });
    }
    if min == max {
        return Err(EngineError {
            message: "width_bucket() requires min and max to differ".to_string(),
        });
    }
    let buckets = count as f64;
    let bucket = if min < max {
        if value < min {
            0
        } else if value >= max {
            count + 1
        } else {
            (((value - min) * buckets) / (max - min)).floor() as i64 + 1
        }
    } else if value > min {
        0
    } else if value <= max {
        count + 1
    } else {
        (((min - value) * buckets) / (min - max)).floor() as i64 + 1
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
