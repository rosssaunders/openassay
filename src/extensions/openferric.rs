use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use openferric::core::{BarrierDirection, BarrierStyle, ExerciseStyle, OptionType, PricingEngine};
use openferric::engines::analytic::HestonEngine;
use openferric::instruments::vanilla::VanillaOption;
use openferric::market::Market;
use openferric::pricing::american::crr_binomial_american;
use openferric::pricing::barrier::barrier_price_closed_form;
use openferric::pricing::european::{black_scholes_greeks, black_scholes_price};

pub(crate) fn eval_openferric_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "european_call" if args.len() == 5 => {
            eval_european_price(args, OptionType::Call, "openferric.european_call()")
        }
        "european_put" if args.len() == 5 => {
            eval_european_price(args, OptionType::Put, "openferric.european_put()")
        }
        "european_greeks" if args.len() == 6 => eval_european_greeks(args),
        "american_call" if args.len() == 5 || args.len() == 6 => {
            eval_american_price(args, OptionType::Call, "openferric.american_call()")
        }
        "american_put" if args.len() == 5 || args.len() == 6 => {
            eval_american_price(args, OptionType::Put, "openferric.american_put()")
        }
        "barrier" if args.len() == 9 => eval_barrier(args),
        "heston" if args.len() == 10 => eval_heston(args),
        _ => Err(EngineError {
            message: format!("function openferric.{fn_name}() does not exist"),
        }),
    }
}

fn eval_european_price(
    args: &[ScalarValue],
    option_type: OptionType,
    context: &str,
) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], &format!("{context} expects numeric spot"))?;
    let strike = parse_f64(&args[1], &format!("{context} expects numeric strike"))?;
    let expiry = parse_f64(&args[2], &format!("{context} expects numeric expiry"))?;
    let vol = parse_f64(&args[3], &format!("{context} expects numeric vol"))?;
    let rate = parse_f64(&args[4], &format!("{context} expects numeric rate"))?;

    Ok(ScalarValue::Float(black_scholes_price(
        option_type,
        spot,
        strike,
        rate,
        vol,
        expiry,
    )))
}

fn eval_european_greeks(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(
        &args[0],
        "openferric.european_greeks() expects numeric spot",
    )?;
    let strike = parse_f64(
        &args[1],
        "openferric.european_greeks() expects numeric strike",
    )?;
    let expiry = parse_f64(
        &args[2],
        "openferric.european_greeks() expects numeric expiry",
    )?;
    let vol = parse_f64(&args[3], "openferric.european_greeks() expects numeric vol")?;
    let rate = parse_f64(
        &args[4],
        "openferric.european_greeks() expects numeric rate",
    )?;
    let option_type = parse_option_type(&args[5], "openferric.european_greeks()")?;

    let greeks = black_scholes_greeks(option_type, spot, strike, rate, vol, expiry);

    let mut payload = JsonMap::new();
    payload.insert(
        "delta".to_string(),
        finite_json_number(greeks.delta, "openferric.european_greeks() delta")?,
    );
    payload.insert(
        "gamma".to_string(),
        finite_json_number(greeks.gamma, "openferric.european_greeks() gamma")?,
    );
    payload.insert(
        "vega".to_string(),
        finite_json_number(greeks.vega, "openferric.european_greeks() vega")?,
    );
    payload.insert(
        "theta".to_string(),
        finite_json_number(greeks.theta, "openferric.european_greeks() theta")?,
    );
    payload.insert(
        "rho".to_string(),
        finite_json_number(greeks.rho, "openferric.european_greeks() rho")?,
    );

    Ok(ScalarValue::Text(JsonValue::Object(payload).to_string()))
}

fn eval_barrier(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "openferric.barrier() expects numeric spot")?;
    let strike = parse_f64(&args[1], "openferric.barrier() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "openferric.barrier() expects numeric expiry")?;
    let vol = parse_f64(&args[3], "openferric.barrier() expects numeric vol")?;
    let rate = parse_f64(&args[4], "openferric.barrier() expects numeric rate")?;
    let barrier = parse_f64(&args[5], "openferric.barrier() expects numeric barrier")?;
    let option_type = parse_option_type(&args[6], "openferric.barrier()")?;
    let barrier_style = parse_barrier_style(&args[7], "openferric.barrier()")?;
    let barrier_direction = parse_barrier_direction(&args[8], "openferric.barrier()")?;

    Ok(ScalarValue::Float(barrier_price_closed_form(
        option_type,
        barrier_style,
        barrier_direction,
        spot,
        strike,
        barrier,
        rate,
        vol,
        expiry,
    )))
}

fn eval_american_price(
    args: &[ScalarValue],
    option_type: OptionType,
    context: &str,
) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], &format!("{context} expects numeric spot"))?;
    let strike = parse_f64(&args[1], &format!("{context} expects numeric strike"))?;
    let expiry = parse_f64(&args[2], &format!("{context} expects numeric expiry"))?;
    let vol = parse_f64(&args[3], &format!("{context} expects numeric vol"))?;
    let rate = parse_f64(&args[4], &format!("{context} expects numeric rate"))?;
    let steps = if args.len() == 6 {
        parse_positive_steps(&args[5], context)?
    } else {
        500
    };

    Ok(ScalarValue::Float(crr_binomial_american(
        option_type,
        spot,
        strike,
        rate,
        vol,
        expiry,
        steps,
    )))
}

fn eval_heston(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|arg| matches!(arg, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }

    let spot = parse_f64(&args[0], "openferric.heston() expects numeric spot")?;
    let strike = parse_f64(&args[1], "openferric.heston() expects numeric strike")?;
    let expiry = parse_f64(&args[2], "openferric.heston() expects numeric expiry")?;
    let rate = parse_f64(&args[3], "openferric.heston() expects numeric rate")?;
    let v0 = parse_f64(&args[4], "openferric.heston() expects numeric v0")?;
    let kappa = parse_f64(&args[5], "openferric.heston() expects numeric kappa")?;
    let theta = parse_f64(&args[6], "openferric.heston() expects numeric theta")?;
    let sigma_v = parse_f64(&args[7], "openferric.heston() expects numeric sigma_v")?;
    let rho = parse_f64(&args[8], "openferric.heston() expects numeric rho")?;
    let option_type = parse_option_type(&args[9], "openferric.heston()")?;

    let instrument = VanillaOption {
        option_type,
        strike,
        expiry,
        exercise: ExerciseStyle::European,
    };
    let market = Market::builder()
        .spot(spot)
        .rate(rate)
        .dividend_yield(0.0)
        .flat_vol(1.0)
        .build()
        .map_err(map_pricing_error)?;
    let engine = HestonEngine::new(v0, kappa, theta, sigma_v, rho);
    let result = engine
        .price(&instrument, &market)
        .map_err(map_pricing_error)?;

    Ok(ScalarValue::Float(result.price))
}

fn parse_f64(value: &ScalarValue, message: &str) -> Result<f64, EngineError> {
    match value {
        ScalarValue::Float(v) => Ok(*v),
        ScalarValue::Int(v) => Ok(*v as f64),
        ScalarValue::Numeric(d) => d.to_f64().ok_or_else(|| EngineError {
            message: message.to_string(),
        }),
        ScalarValue::Text(v) => v.parse::<f64>().map_err(|_| EngineError {
            message: message.to_string(),
        }),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn parse_positive_steps(value: &ScalarValue, context: &str) -> Result<usize, EngineError> {
    let steps = match value {
        ScalarValue::Int(v) => *v,
        ScalarValue::Float(v) if v.fract() == 0.0 => *v as i64,
        ScalarValue::Text(v) => v.parse::<i64>().map_err(|_| EngineError {
            message: format!("{context} steps must be a positive integer"),
        })?,
        _ => {
            return Err(EngineError {
                message: format!("{context} steps must be a positive integer"),
            });
        }
    };
    if steps <= 0 {
        return Err(EngineError {
            message: format!("{context} steps must be a positive integer"),
        });
    }
    Ok(steps as usize)
}

fn parse_option_type(value: &ScalarValue, context: &str) -> Result<OptionType, EngineError> {
    match normalized_text(value, "option_type must be text")?.as_str() {
        "call" => Ok(OptionType::Call),
        "put" => Ok(OptionType::Put),
        _ => Err(EngineError {
            message: format!("{context} option_type must be 'call' or 'put'"),
        }),
    }
}

fn parse_barrier_style(value: &ScalarValue, context: &str) -> Result<BarrierStyle, EngineError> {
    match normalized_text(value, "barrier_type must be text")?.as_str() {
        "in" | "knock_in" | "ki" | "knockin" => Ok(BarrierStyle::In),
        "out" | "knock_out" | "ko" | "knockout" => Ok(BarrierStyle::Out),
        _ => Err(EngineError {
            message: format!("{context} barrier_type must be 'in' or 'out'"),
        }),
    }
}

fn parse_barrier_direction(
    value: &ScalarValue,
    context: &str,
) -> Result<BarrierDirection, EngineError> {
    match normalized_text(value, "barrier_dir must be text")?.as_str() {
        "up" => Ok(BarrierDirection::Up),
        "down" => Ok(BarrierDirection::Down),
        _ => Err(EngineError {
            message: format!("{context} barrier_dir must be 'up' or 'down'"),
        }),
    }
}

fn normalized_text(value: &ScalarValue, message: &str) -> Result<String, EngineError> {
    match value {
        ScalarValue::Text(text) => Ok(text
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_")),
        _ => Err(EngineError {
            message: message.to_string(),
        }),
    }
}

fn finite_json_number(value: f64, message: &str) -> Result<JsonValue, EngineError> {
    JsonNumber::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| EngineError {
            message: message.to_string(),
        })
}

fn map_pricing_error(error: openferric::core::PricingError) -> EngineError {
    EngineError {
        message: error.to_string(),
    }
}
