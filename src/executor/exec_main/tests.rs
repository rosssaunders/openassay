use super::aggregation::{
    max_f64_fast, max_i64_fast, min_f64_fast, min_i64_fast, sum_f64_fast, sum_i64_fast,
};

#[test]
fn fast_i64_sum_matches_scalar() {
    let values = (1..=2048).collect::<Vec<i64>>();
    assert_eq!(sum_i64_fast(&values), values.iter().copied().sum::<i64>());
}

#[test]
fn fast_f64_sum_matches_scalar() {
    let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
    let fast = sum_f64_fast(&values);
    let scalar = values.iter().copied().sum::<f64>();
    assert!((fast - scalar).abs() < f64::EPSILON * 100.0);
}

#[test]
fn fast_i64_min_matches_scalar() {
    let values = (1..=2048).collect::<Vec<i64>>();
    assert_eq!(min_i64_fast(&values), values.iter().copied().min());
}

#[test]
fn fast_i64_max_matches_scalar() {
    let values = (1..=2048).collect::<Vec<i64>>();
    assert_eq!(max_i64_fast(&values), values.iter().copied().max());
}

#[test]
fn fast_f64_min_matches_scalar() {
    let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
    let fast = min_f64_fast(&values).unwrap();
    let scalar = values.iter().copied().fold(f64::INFINITY, f64::min);
    assert!((fast - scalar).abs() < f64::EPSILON);
}

#[test]
fn fast_f64_max_matches_scalar() {
    let values = (1..=2048).map(|v| v as f64 * 0.5).collect::<Vec<f64>>();
    let fast = max_f64_fast(&values).unwrap();
    let scalar = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    assert!((fast - scalar).abs() < f64::EPSILON);
}

#[test]
fn fast_i64_min_max_with_negatives() {
    let values: Vec<i64> = (-1024..=1024).collect();
    assert_eq!(min_i64_fast(&values), Some(-1024));
    assert_eq!(max_i64_fast(&values), Some(1024));
}

#[test]
fn fast_f64_min_max_with_negatives() {
    let values: Vec<f64> = (-1024..=1024).map(|v| v as f64 * 0.1).collect();
    let min = min_f64_fast(&values).unwrap();
    let max = max_f64_fast(&values).unwrap();
    assert!((min - (-102.4)).abs() < f64::EPSILON);
    assert!((max - 102.4).abs() < f64::EPSILON);
}
