use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::vector::{
    coerce_scalar_to_vector, cosine_distance, inner_product, l1_distance, l2_distance, vector_dims,
    vector_norm,
};

fn vector_pair(
    left: &ScalarValue,
    right: &ScalarValue,
    context: &str,
) -> Result<(Vec<f32>, Vec<f32>), EngineError> {
    let a = coerce_scalar_to_vector(left, None, context)?;
    let b = coerce_scalar_to_vector(right, None, context)?;
    Ok((a, b))
}

pub(crate) fn eval_pgvector_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "l2_distance" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let (a, b) = vector_pair(&args[0], &args[1], "l2_distance()")?;
            Ok(ScalarValue::Float(l2_distance(&a, &b)?))
        }
        "cosine_distance" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let (a, b) = vector_pair(&args[0], &args[1], "cosine_distance()")?;
            Ok(ScalarValue::Float(cosine_distance(&a, &b)?))
        }
        "inner_product" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let (a, b) = vector_pair(&args[0], &args[1], "inner_product()")?;
            Ok(ScalarValue::Float(inner_product(&a, &b)?))
        }
        "l1_distance" if args.len() == 2 => {
            if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
                return Ok(ScalarValue::Null);
            }
            let (a, b) = vector_pair(&args[0], &args[1], "l1_distance()")?;
            Ok(ScalarValue::Float(l1_distance(&a, &b)?))
        }
        "vector_dims" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let vec = coerce_scalar_to_vector(&args[0], None, "vector_dims()")?;
            Ok(ScalarValue::Int(vector_dims(&vec) as i64))
        }
        "vector_norm" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let vec = coerce_scalar_to_vector(&args[0], None, "vector_norm()")?;
            Ok(ScalarValue::Float(vector_norm(&vec)))
        }
        _ => Err(EngineError {
            message: format!("function pgvector.{fn_name}() does not exist"),
        }),
    }
}

pub(crate) fn eval_vector_distance_operator(
    op: &str,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, EngineError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let (a, b) = vector_pair(&left, &right, op)?;
    match op {
        "<->" => Ok(ScalarValue::Float(l2_distance(&a, &b)?)),
        "<=>" => Ok(ScalarValue::Float(cosine_distance(&a, &b)?)),
        "<#>" => Ok(ScalarValue::Float(-inner_product(&a, &b)?)),
        _ => Err(EngineError {
            message: format!("unsupported vector operator {op}"),
        }),
    }
}
