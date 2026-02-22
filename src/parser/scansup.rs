/// PostgreSQL `NAMEDATALEN` default value.
///
/// Identifiers are limited to `NAMEDATALEN - 1` bytes.
pub const NAMEDATALEN: usize = 64;

/// Result of identifier normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentTransform {
    pub value: String,
    pub truncated: bool,
}

/// Port of PostgreSQL's `downcase_truncate_identifier()` from `scansup.c`.
pub fn downcase_truncate_identifier(ident: &str, warn: bool) -> IdentTransform {
    downcase_identifier(ident, warn, true)
}

/// Workhorse for `downcase_truncate_identifier()`.
///
/// This mirrors PostgreSQL behavior by:
/// 1) Lowercasing ASCII identifier bytes.
/// 2) Optionally truncating to `NAMEDATALEN - 1` bytes.
pub fn downcase_identifier(ident: &str, warn: bool, truncate: bool) -> IdentTransform {
    let mut out = ident
        .chars()
        .map(|c| {
            if c.is_ascii() {
                c.to_ascii_lowercase()
            } else {
                c
            }
        })
        .collect::<String>();

    let mut truncated = false;
    if truncate && out.len() >= NAMEDATALEN {
        let len = out.len();
        truncated = truncate_identifier(&mut out, len, warn);
    }

    IdentTransform {
        value: out,
        truncated,
    }
}

/// Port of PostgreSQL's `truncate_identifier()` from `scansup.c`.
///
/// Returns true if truncation occurred.
pub fn truncate_identifier(ident: &mut String, len: usize, warn: bool) -> bool {
    if len < NAMEDATALEN {
        return false;
    }

    let mut cut = NAMEDATALEN - 1;
    while cut > 0 && !ident.is_char_boundary(cut) {
        cut -= 1;
    }

    if warn {
        eprintln!(
            "notice: identifier \"{}\" will be truncated to \"{}\"",
            ident,
            &ident[..cut]
        );
    }

    ident.truncate(cut);
    true
}

/// Port of PostgreSQL's `scanner_isspace()`.
pub fn scanner_isspace(ch: char) -> bool {
    matches!(ch, ' ' | '\t' | '\n' | '\r' | '\u{000b}' | '\u{000c}')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downcases_ascii_identifier() {
        let got = downcase_truncate_identifier("FoO_BaR123", false);
        assert_eq!(got.value, "foo_bar123");
        assert!(!got.truncated);
    }

    #[test]
    fn truncates_long_identifier_to_namedatalen_minus_one() {
        let ident = "A".repeat(80);
        let got = downcase_truncate_identifier(&ident, false);
        assert_eq!(got.value.len(), NAMEDATALEN - 1);
        assert!(got.truncated);
        assert_eq!(got.value, "a".repeat(NAMEDATALEN - 1));
    }

    #[test]
    fn truncation_respects_utf8_boundaries() {
        let ident = format!("{}_{}", "a".repeat(62), "é");
        let got = downcase_truncate_identifier(&ident, false);
        assert_eq!(got.value.len(), NAMEDATALEN - 1);
        assert!(got.truncated);
    }

    #[test]
    fn scanner_isspace_matches_postgres_set() {
        for c in [' ', '\t', '\n', '\r', '\u{000b}', '\u{000c}'] {
            assert!(scanner_isspace(c));
        }
        for c in ['a', '_', '0', '-'] {
            assert!(!scanner_isspace(c));
        }
    }
}
