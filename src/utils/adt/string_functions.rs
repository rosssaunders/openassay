use std::collections::HashSet;

use crate::tcop::engine::EngineError;

pub(crate) fn initcap_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = true;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if capitalize_next {
                result.extend(c.to_uppercase());
                capitalize_next = false;
            } else {
                result.extend(c.to_lowercase());
            }
        } else {
            result.push(c);
            capitalize_next = true;
        }
    }
    result
}

pub(crate) fn pad_string(input: &str, len: usize, fill: &str, left: bool) -> String {
    let input_len = input.chars().count();
    if input_len >= len {
        return input.chars().take(len).collect();
    }
    let pad_len = len - input_len;
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return input.to_string();
    }
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    if left {
        format!("{padding}{input}")
    } else {
        format!("{input}{padding}")
    }
}

pub(crate) fn md5_hex(input: &str) -> String {
    let digest = md5_digest(input.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn md5_digest(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    let mut msg = input.to_vec();
    let bit_len = (msg.len() as u64) * 8;
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            let start = i * 4;
            *word = u32::from_le_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }

        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            let rotate = a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(m[g]);
            b = b.wrapping_add(rotate.rotate_left(S[i]));
            a = temp;
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

pub(crate) fn encode_bytes(input: &[u8], format: &str) -> Result<String, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => Ok(input.iter().map(|b| format!("{b:02x}")).collect()),
        "base64" => {
            use base64::Engine;
            Ok(base64::engine::general_purpose::STANDARD.encode(input))
        }
        "escape" => Ok(escape_bytes(input)),
        _ => Err(EngineError {
            message: format!("encode() unsupported format {format}"),
        }),
    }
}

pub(crate) fn decode_bytes(input: &str, format: &str) -> Result<Vec<u8>, EngineError> {
    let format = format.trim().to_ascii_lowercase();
    match format.as_str() {
        "hex" => decode_hex_bytes(input),
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(input.trim())
                .map_err(|_| EngineError {
                    message: "decode() invalid base64 input".to_string(),
                })
        }
        "escape" => decode_escape_bytes(input),
        _ => Err(EngineError {
            message: format!("decode() unsupported format {format}"),
        }),
    }
}

fn escape_bytes(input: &[u8]) -> String {
    let mut out = String::new();
    for &byte in input {
        if byte == b'\\' {
            out.push_str("\\\\");
        } else if (0x20..=0x7e).contains(&byte) {
            out.push(byte as char);
        } else {
            out.push('\\');
            out.push_str(&format!("{byte:03o}"));
        }
    }
    out
}

fn decode_hex_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let trimmed = input.trim();
    let hex = trimmed
        .strip_prefix("\\x")
        .or_else(|| trimmed.strip_prefix("\\X"))
        .unwrap_or(trimmed);
    if !hex.len().is_multiple_of(2) {
        return Err(EngineError {
            message: "decode() invalid hex input length".to_string(),
        });
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for idx in (0..bytes.len()).step_by(2) {
        let part = std::str::from_utf8(&bytes[idx..idx + 2]).unwrap_or("");
        let value = u8::from_str_radix(part, 16).map_err(|_| EngineError {
            message: "decode() invalid hex input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

fn decode_escape_bytes(input: &str) -> Result<Vec<u8>, EngineError> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch as u8);
            continue;
        }
        let Some(next) = chars.peek().copied() else {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        };
        if next == '\\' {
            chars.next();
            out.push(b'\\');
            continue;
        }
        let mut octal = String::new();
        for _ in 0..3 {
            if let Some(digit) = chars.peek().copied()
                && digit.is_ascii_digit()
                && digit <= '7'
            {
                octal.push(digit);
                chars.next();
            }
        }
        if octal.is_empty() {
            return Err(EngineError {
                message: "decode() invalid escape input".to_string(),
            });
        }
        let value = u8::from_str_radix(&octal, 8).map_err(|_| EngineError {
            message: "decode() invalid escape input".to_string(),
        })?;
        out.push(value);
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrimMode {
    Left,
    Right,
    Both,
}

pub(crate) fn substring_chars(
    input: &str,
    start: i64,
    length: Option<i64>,
) -> Result<String, EngineError> {
    if let Some(length) = length
        && length < 0
    {
        return Err(EngineError {
            message: "negative substring length not allowed".to_string(),
        });
    }
    let chars = input.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return Ok(String::new());
    }

    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx >= chars.len() {
        return Ok(String::new());
    }
    let end_idx = match length {
        Some(len) => start_idx.saturating_add(len as usize).min(chars.len()),
        None => chars.len(),
    };
    Ok(chars[start_idx..end_idx].iter().collect())
}

pub(crate) fn left_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        return chars[..(count as usize).min(chars.len())].iter().collect();
    }
    let keep = chars.len().saturating_sub(count.unsigned_abs() as usize);
    chars[..keep].iter().collect()
}

pub(crate) fn right_chars(input: &str, count: i64) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    if count >= 0 {
        let keep = (count as usize).min(chars.len());
        return chars[chars.len() - keep..].iter().collect();
    }
    let drop = count.unsigned_abs() as usize;
    let start = drop.min(chars.len());
    chars[start..].iter().collect()
}

pub(crate) fn find_substring_position(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    let Some(byte_idx) = haystack.find(needle) else {
        return 0;
    };
    haystack[..byte_idx].chars().count() as i64 + 1
}

pub(crate) fn overlay_text(
    input: &str,
    replacement: &str,
    start: i64,
    count: Option<i64>,
) -> Result<String, EngineError> {
    let mut chars = input.chars().collect::<Vec<_>>();
    let replace_chars = replacement.chars().collect::<Vec<_>>();
    let start_idx = if start <= 1 { 0 } else { (start - 1) as usize };
    if start_idx > chars.len() {
        return Ok(input.to_string());
    }
    let count = count.unwrap_or(replace_chars.len() as i64);
    if count < 0 {
        return Err(EngineError {
            message: "overlay() expects non-negative count".to_string(),
        });
    }
    let end_idx = start_idx.saturating_add(count as usize).min(chars.len());
    chars.splice(start_idx..end_idx, replace_chars);
    Ok(chars.iter().collect())
}

pub(crate) fn ascii_code(input: &str) -> i64 {
    input.chars().next().map(|c| c as i64).unwrap_or(0)
}

pub(crate) fn chr_from_code(code: i64) -> Result<String, EngineError> {
    if code < 0 {
        return Err(EngineError {
            message: "character code out of range".to_string(),
        });
    }
    let ch = char::from_u32(code as u32).ok_or_else(|| EngineError {
        message: "character code out of range".to_string(),
    })?;
    Ok(ch.to_string())
}

pub(crate) fn trim_text(input: &str, trim_chars: Option<&str>, mode: TrimMode) -> String {
    match trim_chars {
        None => match mode {
            TrimMode::Left => input.trim_start().to_string(),
            TrimMode::Right => input.trim_end().to_string(),
            TrimMode::Both => input.trim().to_string(),
        },
        Some(chars) => trim_chars_from_text(input, chars, mode),
    }
}

fn trim_chars_from_text(input: &str, trim_chars: &str, mode: TrimMode) -> String {
    if trim_chars.is_empty() {
        return input.to_string();
    }
    let set: HashSet<char> = trim_chars.chars().collect();
    let chars = input.chars().collect::<Vec<_>>();

    let mut start = 0usize;
    let mut end = chars.len();
    if matches!(mode, TrimMode::Left | TrimMode::Both) {
        while start < end && set.contains(&chars[start]) {
            start += 1;
        }
    }
    if matches!(mode, TrimMode::Right | TrimMode::Both) {
        while end > start && set.contains(&chars[end - 1]) {
            end -= 1;
        }
    }
    chars[start..end].iter().collect()
}

pub(crate) fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    // Return as \\x prefixed hex string (bytea output format)
    let hex: String = result.iter().map(|b| format!("{b:02x}")).collect();
    format!("\\x{hex}")
}

/// Implements PostgreSQL's format() function.
/// Formats a string similar to sprintf, supporting:
/// - %s - string (any type coerced to text)
/// - %I - SQL identifier (quoted with double quotes if needed)
/// - %L - SQL literal (quoted with single quotes if needed)
/// - %% - literal %
/// - Positional specifiers like %1$s, %2$I
pub(crate) fn eval_format(
    format_str: &str,
    args: &[crate::storage::tuple::ScalarValue],
) -> Result<String, crate::tcop::engine::EngineError> {
    use crate::storage::tuple::ScalarValue;
    use crate::tcop::engine::EngineError;

    let mut result = String::new();
    let mut chars = format_str.chars().peekable();
    let mut arg_index = 0;

    while let Some(ch) = chars.next() {
        if ch != '%' {
            result.push(ch);
            continue;
        }

        // Check for %%
        if chars.peek() == Some(&'%') {
            chars.next();
            result.push('%');
            continue;
        }

        // Parse optional positional argument like %1$
        let mut position: Option<usize> = None;
        let mut digits = String::new();
        while let Some(&next_ch) = chars.peek() {
            if next_ch.is_ascii_digit() {
                digits.push(next_ch);
                chars.next();
            } else {
                break;
            }
        }

        if !digits.is_empty() {
            if chars.peek() == Some(&'$') {
                chars.next();
                position = Some(
                    digits
                        .parse::<usize>()
                        .map_err(|_| EngineError {
                            message: "invalid format specifier: position too large".to_string(),
                        })?
                        .checked_sub(1)
                        .ok_or_else(|| EngineError {
                            message: "format specifier position must be >= 1".to_string(),
                        })?,
                );
            } else {
                return Err(EngineError {
                    message: "format specifier must be followed by a conversion character"
                        .to_string(),
                });
            }
        }

        // Get the format type character
        let format_type = chars.next().ok_or_else(|| EngineError {
            message: "unterminated format specifier".to_string(),
        })?;

        // Determine which argument to use
        let idx = position.unwrap_or_else(|| {
            let i = arg_index;
            arg_index += 1;
            i
        });

        if idx >= args.len() {
            return Err(EngineError {
                message: "not enough arguments for format string".to_string(),
            });
        }

        let arg = &args[idx];

        // Handle NULL
        if matches!(arg, ScalarValue::Null) {
            continue; // NULL arguments produce empty string
        }

        match format_type {
            's' => {
                // %s - string
                result.push_str(&arg.render());
            }
            'I' => {
                // %I - quoted identifier
                let ident = arg.render();
                result.push_str(&quote_identifier(&ident));
            }
            'L' => {
                // %L - quoted literal
                let literal = arg.render();
                result.push_str(&quote_literal_str(&literal));
            }
            _ => {
                return Err(EngineError {
                    message: format!("unrecognized format type: {format_type}"),
                });
            }
        }
    }

    Ok(result)
}

/// Quote an identifier for use in SQL (double quote if necessary)
fn quote_identifier(ident: &str) -> String {
    // Check if identifier needs quoting
    let needs_quoting = ident.is_empty()
        || !ident
            .chars()
            .next()
            .map(|c| c.is_ascii_lowercase() || c == '_')
            .unwrap_or(false)
        || ident
            .chars()
            .any(|c| !(c.is_ascii_alphanumeric() || c == '_'))
        || is_keyword(ident);

    if needs_quoting {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        ident.to_string()
    }
}

/// Quote a literal string for use in SQL (single quote with escaping)
fn quote_literal_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Simple keyword check (basic set of SQL keywords)
fn is_keyword(s: &str) -> bool {
    matches!(
        s.to_uppercase().as_str(),
        "SELECT"
            | "FROM"
            | "WHERE"
            | "AND"
            | "OR"
            | "NOT"
            | "NULL"
            | "TRUE"
            | "FALSE"
            | "TABLE"
            | "CREATE"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "DROP"
            | "ALTER"
            | "INDEX"
            | "VIEW"
            | "JOIN"
            | "LEFT"
            | "RIGHT"
            | "INNER"
            | "OUTER"
            | "ON"
            | "AS"
            | "ORDER"
            | "BY"
            | "GROUP"
            | "HAVING"
            | "LIMIT"
            | "OFFSET"
    )
}
