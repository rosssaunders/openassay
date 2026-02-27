use rust_decimal::prelude::ToPrimitive;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __m256, _mm256_add_ps, _mm256_castsi256_ps, _mm256_loadu_ps, _mm256_mul_ps,
    _mm256_set1_epi32, _mm256_setzero_ps, _mm256_sub_ps,
};

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

// ---------------------------------------------------------------------------
// SIMD-accelerated f32 vector primitives (AVX – 8-wide)
// ---------------------------------------------------------------------------

/// Horizontal sum of 8 f32 lanes in an AVX register.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn hsum_avx(v: __m256) -> f32 {
    use std::arch::x86_64::{
        _mm256_extractf128_ps, _mm_add_ps, _mm_cvtss_f32, _mm_hadd_ps,
    };
    let hi = _mm256_extractf128_ps(v, 1);
    let lo = _mm256_extractf128_ps(v, 0);
    let sum128 = _mm_add_ps(lo, hi);
    let sum64 = _mm_hadd_ps(sum128, sum128);
    let sum32 = _mm_hadd_ps(sum64, sum64);
    _mm_cvtss_f32(sum32)
}

/// AVX absolute value for f32x8: clear the sign bit.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn abs_ps_avx(v: __m256) -> __m256 {
    use std::arch::x86_64::_mm256_and_ps;
    // Mask with all bits except the sign bit set
    let sign_mask = _mm256_castsi256_ps(_mm256_set1_epi32(0x7FFF_FFFF_u32 as i32));
    _mm256_and_ps(v, sign_mask)
}

/// Squared-difference sum using AVX (8 f32s per iteration).
/// Used by l2_distance.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn squared_diff_sum_avx(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i * 8)) };
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i * 8)) };
        let diff = _mm256_sub_ps(va, vb);
        acc = _mm256_add_ps(acc, _mm256_mul_ps(diff, diff));
    }
    let mut result = unsafe { hsum_avx(acc) };
    for i in (chunks * 8)..n {
        let d = unsafe { *a.get_unchecked(i) - *b.get_unchecked(i) };
        result += d * d;
    }
    result
}

/// Dot product using AVX (8 f32s per iteration).
/// Used by inner_product and cosine_distance.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_product_avx(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i * 8)) };
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i * 8)) };
        acc = _mm256_add_ps(acc, _mm256_mul_ps(va, vb));
    }
    let mut result = unsafe { hsum_avx(acc) };
    for i in (chunks * 8)..n {
        result += unsafe { *a.get_unchecked(i) * *b.get_unchecked(i) };
    }
    result
}

/// Sum of squared elements using AVX. Used by vector_norm.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn sum_of_squares_avx(v: &[f32]) -> f32 {
    let n = v.len();
    let chunks = n / 8;
    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = unsafe { _mm256_loadu_ps(v.as_ptr().add(i * 8)) };
        acc = _mm256_add_ps(acc, _mm256_mul_ps(va, va));
    }
    let mut result = unsafe { hsum_avx(acc) };
    for i in (chunks * 8)..n {
        let x = unsafe { *v.get_unchecked(i) };
        result += x * x;
    }
    result
}

/// Absolute-difference sum using AVX. Used by l1_distance.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn abs_diff_sum_avx(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let chunks = n / 8;
    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i * 8)) };
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i * 8)) };
        let diff = _mm256_sub_ps(va, vb);
        acc = _mm256_add_ps(acc, unsafe { abs_ps_avx(diff) });
    }
    let mut result = unsafe { hsum_avx(acc) };
    for i in (chunks * 8)..n {
        result += unsafe { (*a.get_unchecked(i) - *b.get_unchecked(i)).abs() };
    }
    result
}

/// Compute dot(a,b), ||a||², ||b||² in a single pass using AVX.
/// Returns (dot, norm_a_sq, norm_b_sq).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_and_norms_avx(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let n = a.len();
    let chunks = n / 8;
    let mut dot_acc = _mm256_setzero_ps();
    let mut norm_a_acc = _mm256_setzero_ps();
    let mut norm_b_acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = unsafe { _mm256_loadu_ps(a.as_ptr().add(i * 8)) };
        let vb = unsafe { _mm256_loadu_ps(b.as_ptr().add(i * 8)) };
        dot_acc = _mm256_add_ps(dot_acc, _mm256_mul_ps(va, vb));
        norm_a_acc = _mm256_add_ps(norm_a_acc, _mm256_mul_ps(va, va));
        norm_b_acc = _mm256_add_ps(norm_b_acc, _mm256_mul_ps(vb, vb));
    }
    let mut dot = unsafe { hsum_avx(dot_acc) };
    let mut na = unsafe { hsum_avx(norm_a_acc) };
    let mut nb = unsafe { hsum_avx(norm_b_acc) };
    for i in (chunks * 8)..n {
        let x = unsafe { *a.get_unchecked(i) };
        let y = unsafe { *b.get_unchecked(i) };
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    (dot, na, nb)
}

// ---------------------------------------------------------------------------
// Public distance functions with SIMD fast paths
// ---------------------------------------------------------------------------

pub(crate) fn l2_distance(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "l2_distance()")?;
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") && a.len() >= 8 {
            // SAFETY: AVX feature check passed above.
            let sum = unsafe { squared_diff_sum_avx(a, b) };
            return Ok((sum as f64).sqrt());
        }
    }
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
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") && a.len() >= 8 {
            // SAFETY: AVX feature check passed above.
            return Ok(unsafe { abs_diff_sum_avx(a, b) } as f64);
        }
    }
    let sum: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| ((*x as f64) - (*y as f64)).abs())
        .sum();
    Ok(sum)
}

pub(crate) fn inner_product(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "inner_product()")?;
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") && a.len() >= 8 {
            // SAFETY: AVX feature check passed above.
            return Ok(unsafe { dot_product_avx(a, b) } as f64);
        }
    }
    Ok(a.iter()
        .zip(b.iter())
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum())
}

pub(crate) fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f64, EngineError> {
    ensure_same_dimensions(a, b, "cosine_distance()")?;
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") && a.len() >= 8 {
            // SAFETY: AVX feature check passed above.
            // Compute dot, norm_a², norm_b² in a single SIMD pass.
            let (dot, na_sq, nb_sq) = unsafe { dot_and_norms_avx(a, b) };
            let norm_a = (na_sq as f64).sqrt();
            let norm_b = (nb_sq as f64).sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                return Err(EngineError {
                    message: "cosine_distance() is undefined for zero-length vectors".to_string(),
                });
            }
            return Ok(1.0 - (dot as f64) / (norm_a * norm_b));
        }
    }
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
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx") && v.len() >= 8 {
            // SAFETY: AVX feature check passed above.
            return (unsafe { sum_of_squares_avx(v) } as f64).sqrt();
        }
    }
    v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt()
}

pub(crate) fn vector_dims(v: &[f32]) -> usize {
    v.len()
}
