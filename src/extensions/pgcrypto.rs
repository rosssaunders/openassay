use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;
use crate::utils::adt::string_functions::{md5_digest, md5_hex};
use crate::utils::adt::misc::gen_random_uuid;

type HmacSha1 = Hmac<Sha1>;
type HmacSha224 = Hmac<Sha224>;
type HmacSha256 = Hmac<Sha256>;
type HmacSha384 = Hmac<Sha384>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Clone, Copy)]
enum Algorithm {
    Md5,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

fn parse_algorithm(value: &ScalarValue, context: &str) -> Result<Algorithm, EngineError> {
    let name = value.render().to_ascii_lowercase().replace('-', "");
    match name.as_str() {
        "md5" => Ok(Algorithm::Md5),
        "sha1" => Ok(Algorithm::Sha1),
        "sha224" => Ok(Algorithm::Sha224),
        "sha256" => Ok(Algorithm::Sha256),
        "sha384" => Ok(Algorithm::Sha384),
        "sha512" => Ok(Algorithm::Sha512),
        _ => Err(EngineError {
            message: format!("{context} unsupported algorithm {name}"),
        }),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn digest_bytes(input: &[u8], algorithm: Algorithm) -> Vec<u8> {
    match algorithm {
        Algorithm::Md5 => md5_digest(input).to_vec(),
        Algorithm::Sha1 => Sha1::digest(input).to_vec(),
        Algorithm::Sha224 => Sha224::digest(input).to_vec(),
        Algorithm::Sha256 => Sha256::digest(input).to_vec(),
        Algorithm::Sha384 => Sha384::digest(input).to_vec(),
        Algorithm::Sha512 => Sha512::digest(input).to_vec(),
    }
}

fn hmac_bytes(data: &[u8], key: &[u8], algorithm: Algorithm) -> Result<Vec<u8>, EngineError> {
    let mac_result = match algorithm {
        Algorithm::Md5 => {
            let mut mac = Hmac::<md5::Md5>::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha1 => {
            let mut mac = HmacSha1::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha224 => {
            let mut mac = HmacSha224::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha256 => {
            let mut mac = HmacSha256::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha384 => {
            let mut mac = HmacSha384::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha512 => {
            let mut mac = HmacSha512::new_from_slice(key).map_err(|e| EngineError {
                message: format!("hmac() error: {e}"),
            })?;
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    };
    Ok(mac_result)
}

fn eval_digest(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let data = args[0].render();
    let algo = parse_algorithm(&args[1], "digest()")?;
    let out = digest_bytes(data.as_bytes(), algo);
    Ok(ScalarValue::Text(hex_encode(&out)))
}

fn eval_hmac(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let data = args[0].render();
    let key = args[1].render();
    let algo = parse_algorithm(&args[2], "hmac()")?;
    let out = hmac_bytes(data.as_bytes(), key.as_bytes(), algo)?;
    Ok(ScalarValue::Text(hex_encode(&out)))
}

fn eval_gen_random_bytes(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let len = match &args[0] {
        ScalarValue::Int(v) if *v >= 0 => *v as usize,
        ScalarValue::Float(v) if *v >= 0.0 && v.fract() == 0.0 => *v as usize,
        ScalarValue::Text(v) => {
            let parsed = v.parse::<i64>().map_err(|_| EngineError {
                message: "gen_random_bytes() expects integer length".to_string(),
            })?;
            if parsed < 0 {
                return Err(EngineError {
                    message: "gen_random_bytes() expects non-negative length".to_string(),
                });
            }
            parsed as usize
        }
        _ => {
            return Err(EngineError {
                message: "gen_random_bytes() expects integer length".to_string(),
            });
        }
    };
    let mut buf = vec![0u8; len];
    getrandom::getrandom(&mut buf).map_err(|e| EngineError {
        message: format!("gen_random_bytes() failed: {e}"),
    })?;
    Ok(ScalarValue::Text(hex_encode(&buf)))
}

fn eval_crypt(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let password = args[0].render();
    let salt = args[1].render();
    if salt.starts_with("$2") {
        // Extract cost from bcrypt salt/hash: "$2b$XX$..."
        let cost = salt
            .split('$')
            .nth(2)
            .and_then(|c| c.parse::<u32>().ok())
            .unwrap_or(4);
        if salt.len() >= 60 {
            // Salt is a full bcrypt hash — verify by re-hashing with same params.
            // bcrypt::verify handles extracting the salt internally.
            if matches!(bcrypt::verify(&password, &salt), Ok(true)) {
                return Ok(ScalarValue::Text(salt));
            }
            // Password doesn't match — hash with extracted cost (new salt).
            let hashed = bcrypt::hash(&password, cost).map_err(|e| EngineError {
                message: format!("crypt() bcrypt error: {e}"),
            })?;
            return Ok(ScalarValue::Text(hashed));
        }
        // Salt is a raw bcrypt salt prefix — hash with extracted cost.
        let hashed = bcrypt::hash(&password, cost).map_err(|e| EngineError {
            message: format!("crypt() bcrypt error: {e}"),
        })?;
        return Ok(ScalarValue::Text(hashed));
    }
    if salt.to_ascii_lowercase().starts_with("md5") {
        let salt_body = salt.trim_start_matches("md5");
        let candidate = format!("md5{}", md5_hex(&(password + salt_body)));
        return Ok(ScalarValue::Text(candidate));
    }
    let candidate = format!("md5{}", md5_hex(&(password + &salt)));
    Ok(ScalarValue::Text(candidate))
}

fn eval_gen_salt(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok(ScalarValue::Null);
    }
    let kind = args[0].render().to_ascii_lowercase();
    match kind.as_str() {
        "bf" => {
            let mut salt_bytes = [0u8; 16];
            let _ = getrandom::getrandom(&mut salt_bytes);
            let encoded = STANDARD_NO_PAD.encode(salt_bytes);
            // bcrypt salts are 22 characters
            let salt = format!("$2b$04${}", &encoded[..22.min(encoded.len())]);
            Ok(ScalarValue::Text(salt))
        }
        "md5" => {
            let mut salt_bytes = [0u8; 8];
            let _ = getrandom::getrandom(&mut salt_bytes);
            Ok(ScalarValue::Text(format!("md5{}", hex_encode(&salt_bytes))))
        }
        "des" => {
            let mut salt_bytes = [0u8; 2];
            let _ = getrandom::getrandom(&mut salt_bytes);
            Ok(ScalarValue::Text(hex_encode(&salt_bytes)))
        }
        other => Err(EngineError {
            message: format!("gen_salt() unsupported type {other}"),
        }),
    }
}

pub(crate) fn eval_pgcrypto_function(
    fn_name: &str,
    args: &[ScalarValue],
) -> Result<ScalarValue, EngineError> {
    match fn_name {
        "digest" if args.len() == 1 => {
            if matches!(args[0], ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let out = digest_bytes(args[0].render().as_bytes(), Algorithm::Sha256);
            Ok(ScalarValue::Text(hex_encode(&out)))
        }
        "digest" if args.len() == 2 => eval_digest(args),
        "hmac" if args.len() == 3 => eval_hmac(args),
        "gen_random_bytes" if args.len() == 1 => eval_gen_random_bytes(args),
        "gen_random_uuid" if args.is_empty() => Ok(ScalarValue::Text(gen_random_uuid())),
        "crypt" if args.len() == 2 => eval_crypt(args),
        "gen_salt" if args.len() == 1 => eval_gen_salt(args),
        _ => Err(EngineError {
            message: format!("function pgcrypto.{fn_name}() does not exist"),
        }),
    }
}
