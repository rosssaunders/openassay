use rust_decimal::prelude::ToPrimitive;

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

fn scalar_to_f32(value: &ScalarValue, context: &str) -> Result<f32, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v as f32),
        ScalarValue::Int(v) => Ok(*v as f32),
        ScalarValue::Numeric(d) => Ok(d.to_f32().unwrap_or(f32::NAN)),
        ScalarValue::Text(t) => t.trim().parse::<f32>().map_err(|_| EngineError {
            message: format!("{context} expects numeric vector elements"),
        }),
        ScalarValue::Vector(v) if v.len() == 1 => Ok(v[0]),
        _ => Err(EngineError {
            message: format!("{context} expects numeric vector elements"),
        }),
    }
}

fn parse_vector_text(text: &str, context: &str) -> Result<Vec<f32>, EngineError> {
    let trimmed = text.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(EngineError {
            message: format!("{context} expects vector literal like [1,2,3]"),
        });
    }
    serde_json::from_str::<Vec<f32>>(trimmed).map_err(|_| EngineError {
        message: format!("{context} expects numeric vector literal"),
    })
}

fn array_to_vector(values: &[ScalarValue], context: &str) -> Result<Vec<f32>, EngineError> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(scalar_to_f32(value, context)?);
    }
    Ok(out)
}

pub(crate) fn coerce_scalar_to_vector(
    value: &ScalarValue,
    expected_dim: Option<usize>,
    context: &str,
) -> Result<Vec<f32>, EngineError> {
    let vector = match value {
        ScalarValue::Vector(v) => v.clone(),
        ScalarValue::Array(values) => array_to_vector(values, context)?,
        ScalarValue::Text(text) => parse_vector_text(text, context)?,
        ScalarValue::Float(_) | ScalarValue::Int(_) | ScalarValue::Numeric(_) => {
            vec![scalar_to_f32(value, context)?]
        }
        _ => {
            return Err(EngineError {
                message: format!("{context} expects a vector input"),
            });
        }
    };

    if let Some(dim) = expected_dim
        && vector.len() != dim
    {
        return Err(EngineError {
            message: format!(
                "{context} expects vector dimension {dim}, got {}",
                vector.len()
            ),
        });
    }
    Ok(vector)
}

fn ensure_same_dimensions(a: &[f32], b: &[f32], context: &str) -> Result<(), EngineError> {
    if a.len() != b.len() {
        return Err(EngineError {
            message: format!(
                "{context} expects vectors of the same dimension ({} != {})",
                a.len(),
                b.len()
            ),
        });
    }
    Ok(())
}

pub(crate) fn l2_distance(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "l2_distance()")?;
    let sum: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = (*x as f64) - (*y as f64);
            d * d
        })
        .sum();
    Ok(sum.sqrt())
}

pub(crate) fn l1_distance(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "l1_distance()")?;
    let sum: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| ((*x as f64) - (*y as f64)).abs())
        .sum();
    Ok(sum)
}

pub(crate) fn inner_product(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "inner_product()")?;
    Ok(a.iter()
        .zip(b.iter())
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum())
}

pub(crate) fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "cosine_distance()")?;
    let dot: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum();
    let norm_a = vector_norm(a);
    let norm_b = vector_norm(b);
    if norm_a == 0.0 || norm_b == 0.0 {
        return Err(EngineError {
            message: "cosine_distance() is undefined for zero-length vectors".to_string(),
        });
    }
    Ok(1.0 - dot / (norm_a * norm_b))
}

pub(crate) fn vector_norm(v: &[f32]) -> f64 {
    v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt()
}

pub(crate) fn vector_dims(v: &[f32]) -> usize {
    v.len()
}
