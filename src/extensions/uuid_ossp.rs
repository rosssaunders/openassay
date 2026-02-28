use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;
use uuid::v1::{Context, Timestamp};

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

static V1_CONTEXT: OnceLock<Context> = OnceLock::new();
static NODE_ID: OnceLock<[u8; 6]> = OnceLock::new();

fn node_id() -> [u8; 6] {
    *NODE_ID.get_or_init(|| {
        let mut bytes = [0u8; 6];
        crate::utils::random::fill_random_bytes(&mut bytes);
        if bytes.iter().all(|b| *b == 0) {
            bytes[0] = 1;
        }
        bytes
    })
}

fn v1_context() -> &'static Context {
    V1_CONTEXT.get_or_init(|| Context::new(0))
}

fn uuid_generate_v1() -> Result<ScalarValue, EngineError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let ts = Timestamp::from_unix(v1_context(), now.as_secs(), now.subsec_nanos());
    let uuid = Uuid::new_v1(ts, &node_id());
    Ok(ScalarValue::Text(uuid.to_string()))
}

fn parse_namespace(value: &ScalarValue) -> Result<Uuid, EngineError> {
    let raw = value.render();
    let norm = raw.trim().to_ascii_lowercase();
    match norm.as_str() {
        "dns" => Ok(Uuid::NAMESPACE_DNS),
        "url" => Ok(Uuid::NAMESPACE_URL),
        "oid" => Ok(Uuid::NAMESPACE_OID),
        "x500" | "x.500" | "x-500" => Ok(Uuid::NAMESPACE_X500),
        _ => Uuid::parse_str(raw.trim()).map_err(|_| EngineError {
            message: "uuid_generate_v5() expects a namespace UUID or one of DNS/URL/OID/X.500"
                .to_string(),
        }),
    }
}

fn uuid_generate_v5(ns: &ScalarValue, name: &ScalarValue) -> Result<ScalarValue, EngineError> {
    if matches!(ns, ScalarValue::Null) || matches!(name, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }
    let namespace = parse_namespace(ns)?;
    let input = name.render();
    Ok(ScalarValue::Text(
        Uuid::new_v5(&namespace, input.as_bytes()).to_string(),
    ))
}

pub(crate) fn eval_uuid_ossp_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "uuid_generate_v4" if args.is_empty() => Ok(ScalarValue::Text(Uuid::new_v4().to_string())),
        "uuid_generate_v1" if args.is_empty() => uuid_generate_v1(),
        "uuid_generate_v5" if args.len() == 2 => uuid_generate_v5(&args[0], &args[1]),
        "uuid_nil" if args.is_empty() => Ok(ScalarValue::Text(Uuid::nil().to_string())),
        _ => Err(EngineError {
            message: format!("function uuid-ossp.{fn_name}() does not exist"),
        }),
    }
}
