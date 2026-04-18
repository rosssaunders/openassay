//! The `SqlType` enum. Canonical declared type of a column or expression.

use crate::catalog::oid::Oid;

/// Declared SQL type. Carries enough information to emit a spec-correct
/// `RowDescription` and to binary-encode values on the wire without guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    /// boolean (oid 16)
    Bool,
    /// smallint / int2 (oid 21)
    Int2,
    /// integer / int4 (oid 23)
    Int4,
    /// bigint / int8 (oid 20)
    Int8,
    /// real / float4 (oid 700)
    Float4,
    /// double precision / float8 (oid 701)
    Float8,
    /// numeric(precision, scale). Either may be absent for unconstrained.
    Numeric {
        precision: Option<u8>,
        scale: Option<i16>,
    },
    /// text (oid 25) — unconstrained string
    Text,
    /// varchar(n). `None` length means unconstrained varchar.
    Varchar(Option<u32>),
    /// char(n) / bpchar(n). `None` length means 1.
    BpChar(Option<u32>),
    /// "char" type (oid 18) — internal single byte, not the SQL CHAR(1)
    InternalChar,
    /// name (oid 19) — internal identifier type
    Name,
    /// bytea (oid 17)
    Bytea,
    /// uuid (oid 2950)
    Uuid,
    /// date (oid 1082)
    Date,
    /// time without time zone (oid 1083)
    Time,
    /// time with time zone / timetz (oid 1266)
    Timetz,
    /// timestamp without time zone (oid 1114)
    Timestamp,
    /// timestamp with time zone (oid 1184)
    Timestamptz,
    /// interval (oid 1186)
    Interval,
    /// json (oid 114)
    Json,
    /// jsonb (oid 3802)
    Jsonb,
    /// oid (oid 26)
    Oid,
    /// regproc (oid 24)
    Regproc,
    /// Fixed-length array of a known element type.
    Array(Box<Self>),
    /// pgvector: Vec<f32> of fixed dimension.
    Vector(usize),
    /// User-declared enum type. `oid` is the `pg_type.oid` for this enum.
    Enum(Oid),
    /// User-declared composite type.
    Composite(Oid),
    /// User-declared domain over a base type.
    Domain {
        /// OID of the pg_type row for this domain.
        oid: Oid,
        base: Box<Self>,
    },
    /// Range type (int4range, tsrange, …).
    Range(Oid),
    /// Pseudo-type: record (oid 2249)
    Record,
    /// Pseudo-type: void (oid 2278)
    Void,
    /// Unknown — used for parameters where inference couldn't determine a
    /// concrete type. Surfaces as OID 0 in ParameterDescription.
    Unknown,
}

/// Parse a SQL type-name string into a `SqlType`.
///
/// Accepts the forms PG accepts in a CAST expression or CREATE TABLE column
/// declaration: bare names (`int4`, `text`, `uuid`), length-qualified types
/// (`varchar(10)`, `char(5)`), precision/scale (`numeric(10, 2)`), and array
/// suffixes (`int4[]`, `text[]`). Case-insensitive. Whitespace around
/// modifiers is permitted.
///
/// Returns `None` if the name isn't recognised. Callers typically fall back
/// to `SqlType::Text` in that case to preserve backwards-compatible
/// permissive behaviour — unknown-type handling is Phase 5 territory.
pub fn parse_sql_type_name(name: &str) -> Option<SqlType> {
    let trimmed = name.trim();
    // Strip trailing array suffix(es) and recurse on the element.
    if let Some(elem) = trimmed.strip_suffix("[]") {
        let inner = parse_sql_type_name(elem)?;
        return Some(SqlType::Array(Box::new(inner)));
    }
    // Split off the parenthesised modifier, if any.
    let (base, modifier) = match trimmed.find('(') {
        Some(open) => {
            let close = trimmed.rfind(')')?;
            if close <= open {
                return None;
            }
            (
                trimmed[..open].trim(),
                Some(trimmed[open + 1..close].trim()),
            )
        }
        None => (trimmed, None),
    };

    let base_lc = base.to_ascii_lowercase();
    // PG accepts `character varying` / `character` as multi-word aliases for
    // varchar / bpchar. Normalise those before matching.
    let normalised: std::borrow::Cow<'_, str> = {
        let collapsed: String = base_lc.split_whitespace().collect::<Vec<_>>().join(" ");
        match collapsed.as_str() {
            "character varying" => std::borrow::Cow::Borrowed("varchar"),
            "character" => std::borrow::Cow::Borrowed("bpchar"),
            "double precision" => std::borrow::Cow::Borrowed("float8"),
            "timestamp with time zone" => std::borrow::Cow::Borrowed("timestamptz"),
            "timestamp without time zone" => std::borrow::Cow::Borrowed("timestamp"),
            "time with time zone" => std::borrow::Cow::Borrowed("timetz"),
            "time without time zone" => std::borrow::Cow::Borrowed("time"),
            _ => std::borrow::Cow::Owned(collapsed),
        }
    };

    match normalised.as_ref() {
        "bool" | "boolean" => Some(SqlType::Bool),
        "bytea" => Some(SqlType::Bytea),
        "char" => Some(SqlType::BpChar(parse_length(modifier).or(Some(1)))),
        "bpchar" => Some(SqlType::BpChar(parse_length(modifier))),
        "name" => Some(SqlType::Name),
        "text" => Some(SqlType::Text),
        "varchar" => Some(SqlType::Varchar(parse_length(modifier))),
        "int2" | "smallint" => Some(SqlType::Int2),
        "int4" | "int" | "integer" | "serial" => Some(SqlType::Int4),
        "int8" | "bigint" | "bigserial" => Some(SqlType::Int8),
        "float4" | "real" => Some(SqlType::Float4),
        "float8" => Some(SqlType::Float8),
        "numeric" | "decimal" => Some(parse_numeric(modifier)),
        "oid" => Some(SqlType::Oid),
        "regproc" => Some(SqlType::Regproc),
        "uuid" => Some(SqlType::Uuid),
        "date" => Some(SqlType::Date),
        "time" => Some(SqlType::Time),
        "timetz" => Some(SqlType::Timetz),
        "timestamp" => Some(SqlType::Timestamp),
        "timestamptz" => Some(SqlType::Timestamptz),
        "interval" => Some(SqlType::Interval),
        "json" => Some(SqlType::Json),
        "jsonb" => Some(SqlType::Jsonb),
        "vector" => {
            let dim = modifier
                .and_then(|m| m.trim().parse::<usize>().ok())
                .unwrap_or(0);
            Some(SqlType::Vector(dim))
        }
        "record" => Some(SqlType::Record),
        "void" => Some(SqlType::Void),
        _ => None,
    }
}

fn parse_length(modifier: Option<&str>) -> Option<u32> {
    modifier.and_then(|m| m.trim().parse::<u32>().ok())
}

fn parse_numeric(modifier: Option<&str>) -> SqlType {
    let Some(body) = modifier else {
        return SqlType::Numeric {
            precision: None,
            scale: None,
        };
    };
    let mut parts = body.split(',').map(str::trim);
    let precision = parts.next().and_then(|p| p.parse::<u8>().ok());
    let scale = parts.next().and_then(|s| s.parse::<i16>().ok());
    SqlType::Numeric { precision, scale }
}

impl SqlType {
    /// Canonical `pg_type.oid` for this SQL type.
    ///
    /// Values match `src/catalog/builtin_types.rs::BUILTIN_TYPES`. For
    /// user-declared types (enum, composite, domain, range) the OID comes
    /// from catalog registration.
    pub fn oid(&self) -> Oid {
        match self {
            Self::Bool => 16,
            Self::Bytea => 17,
            Self::InternalChar => 18,
            Self::Name => 19,
            Self::Int8 => 20,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Regproc => 24,
            Self::Text => 25,
            Self::Oid => 26,
            Self::Json => 114,
            Self::Float4 => 700,
            Self::Float8 => 701,
            Self::BpChar(_) => 1042,
            Self::Varchar(_) => 1043,
            Self::Date => 1082,
            Self::Time => 1083,
            Self::Timestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Interval => 1186,
            Self::Timetz => 1266,
            Self::Numeric { .. } => 1700,
            Self::Uuid => 2950,
            Self::Record => 2249,
            Self::Void => 2278,
            Self::Jsonb => 3802,
            Self::Vector(_) => 6000,
            Self::Array(inner) => array_oid_for_element(inner.as_ref()),
            Self::Enum(oid) | Self::Composite(oid) | Self::Range(oid) => *oid,
            Self::Domain { oid, .. } => *oid,
            Self::Unknown => 0,
        }
    }

    /// `typlen` per pg_type.dat: -1 for variable-length, else the fixed
    /// byte count. Used as `RowDescriptionField::type_size`.
    pub fn typlen(&self) -> i16 {
        match self {
            Self::Bool | Self::InternalChar => 1,
            Self::Int2 => 2,
            Self::Int4 | Self::Float4 | Self::Date | Self::Oid | Self::Regproc => 4,
            Self::Int8 | Self::Float8 | Self::Time | Self::Timestamp | Self::Timestamptz => 8,
            Self::Timetz => 12,
            Self::Interval | Self::Uuid => 16,
            Self::Name => 64,
            Self::Bytea
            | Self::Text
            | Self::Varchar(_)
            | Self::BpChar(_)
            | Self::Numeric { .. }
            | Self::Json
            | Self::Jsonb
            | Self::Array(_)
            | Self::Vector(_)
            | Self::Range(_) => -1,
            // Enums are fixed-length 4 bytes (the enum-value OID); composites
            // and domains inherit from their underlying representation.
            Self::Enum(_) => 4,
            Self::Composite(_) => -1,
            Self::Domain { base, .. } => base.typlen(),
            Self::Record => -1,
            Self::Void => 4,
            Self::Unknown => -1,
        }
    }

    /// Wire-format `typmod` (RowDescriptionField::type_modifier). Defaults
    /// to -1 meaning "no modifier." Postgres encodes typmod for varchar /
    /// bpchar / numeric specially:
    ///
    /// - `varchar(n)` / `char(n)` → `n + 4`
    /// - `numeric(p, s)` → `((p << 16) | (s & 0xFFFF)) + 4`
    pub fn typmod(&self) -> i32 {
        match self {
            Self::Varchar(Some(n)) | Self::BpChar(Some(n)) => (*n as i32) + 4,
            Self::Numeric {
                precision: Some(p),
                scale,
            } => {
                let p = i32::from(*p);
                let s = i32::from(scale.unwrap_or(0)) & 0xFFFF;
                ((p << 16) | s) + 4
            }
            Self::Domain { base, .. } => base.typmod(),
            _ => -1,
        }
    }

    /// True if values of this type are binary-compatible with `i64` storage.
    /// Used by the binary encoder to decide whether to narrow from `Int(i64)`
    /// before shipping on the wire.
    pub const fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Int2 | Self::Int4 | Self::Int8 | Self::Oid | Self::Regproc
        )
    }

    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Float4 | Self::Float8)
    }

    pub const fn is_text_like(&self) -> bool {
        matches!(
            self,
            Self::Text | Self::Varchar(_) | Self::BpChar(_) | Self::Name | Self::InternalChar
        )
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    /// Element type if this is an array; `None` otherwise.
    pub fn array_element(&self) -> Option<&Self> {
        match self {
            Self::Array(inner) => Some(inner.as_ref()),
            _ => None,
        }
    }
}

/// Map an element type's OID to its corresponding array-of-that-element OID.
/// Canonical PG values from `BUILTIN_TYPES` — keep this table in sync when
/// adding new array types.
fn array_oid_for_element(element: &SqlType) -> Oid {
    match element {
        SqlType::Bool => 1000,
        SqlType::Bytea => 1001,
        SqlType::InternalChar => 1002,
        SqlType::Name => 1003,
        SqlType::Int8 => 1016,
        SqlType::Int2 => 1005,
        SqlType::Int4 => 1007,
        SqlType::Regproc => 1008,
        SqlType::Text => 1009,
        SqlType::Oid => 1028,
        SqlType::Json => 199,
        SqlType::Float4 => 1021,
        SqlType::Float8 => 1022,
        SqlType::BpChar(_) => 1014,
        SqlType::Varchar(_) => 1015,
        SqlType::Date => 1182,
        SqlType::Time => 1183,
        SqlType::Timestamp => 1115,
        SqlType::Timestamptz => 1185,
        SqlType::Interval => 1187,
        SqlType::Timetz => 1270,
        SqlType::Numeric { .. } => 1231,
        SqlType::Uuid => 2951,
        SqlType::Jsonb => 3807,
        // Ranges and pseudo-types have no standard array form we ship.
        SqlType::Range(_)
        | SqlType::Vector(_)
        | SqlType::Enum(_)
        | SqlType::Composite(_)
        | SqlType::Domain { .. }
        | SqlType::Record
        | SqlType::Void
        | SqlType::Array(_)
        | SqlType::Unknown => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oids_match_builtin_table() {
        // Spot-check that every SqlType variant's oid() agrees with what
        // we ship in pg_type. Regressions here mean drivers see one OID in
        // RowDescription and another in pg_type lookup — silently breaking
        // type decoding.
        assert_eq!(SqlType::Bool.oid(), 16);
        assert_eq!(SqlType::Int2.oid(), 21);
        assert_eq!(SqlType::Int4.oid(), 23);
        assert_eq!(SqlType::Int8.oid(), 20);
        assert_eq!(SqlType::Float4.oid(), 700);
        assert_eq!(SqlType::Float8.oid(), 701);
        assert_eq!(SqlType::Text.oid(), 25);
        assert_eq!(SqlType::Varchar(None).oid(), 1043);
        assert_eq!(SqlType::Varchar(Some(10)).oid(), 1043);
        assert_eq!(SqlType::BpChar(Some(5)).oid(), 1042);
        assert_eq!(SqlType::Bytea.oid(), 17);
        assert_eq!(SqlType::Uuid.oid(), 2950);
        assert_eq!(SqlType::Date.oid(), 1082);
        assert_eq!(SqlType::Timestamp.oid(), 1114);
        assert_eq!(SqlType::Timestamptz.oid(), 1184);
        assert_eq!(
            SqlType::Numeric {
                precision: Some(10),
                scale: Some(2)
            }
            .oid(),
            1700
        );
        assert_eq!(SqlType::Json.oid(), 114);
        assert_eq!(SqlType::Jsonb.oid(), 3802);
        assert_eq!(SqlType::Array(Box::new(SqlType::Int4)).oid(), 1007);
        assert_eq!(SqlType::Array(Box::new(SqlType::Text)).oid(), 1009);
        assert_eq!(SqlType::Unknown.oid(), 0);
    }

    #[test]
    fn typlen_matches_pg() {
        // Fixed-length types ship specific byte counts; variable-length
        // types ship -1 per RowDescription convention.
        assert_eq!(SqlType::Bool.typlen(), 1);
        assert_eq!(SqlType::Int2.typlen(), 2);
        assert_eq!(SqlType::Int4.typlen(), 4);
        assert_eq!(SqlType::Int8.typlen(), 8);
        assert_eq!(SqlType::Float4.typlen(), 4);
        assert_eq!(SqlType::Float8.typlen(), 8);
        assert_eq!(SqlType::Date.typlen(), 4);
        assert_eq!(SqlType::Timestamp.typlen(), 8);
        assert_eq!(SqlType::Timestamptz.typlen(), 8);
        assert_eq!(SqlType::Interval.typlen(), 16);
        assert_eq!(SqlType::Uuid.typlen(), 16);
        assert_eq!(SqlType::Text.typlen(), -1);
        assert_eq!(SqlType::Varchar(Some(10)).typlen(), -1);
        assert_eq!(
            SqlType::Numeric {
                precision: None,
                scale: None
            }
            .typlen(),
            -1
        );
    }

    #[test]
    fn typmod_encodes_varchar_length() {
        // PG wire encodes varchar(n) as typmod = n + 4.
        assert_eq!(SqlType::Varchar(None).typmod(), -1);
        assert_eq!(SqlType::Varchar(Some(0)).typmod(), 4);
        assert_eq!(SqlType::Varchar(Some(10)).typmod(), 14);
        assert_eq!(SqlType::Varchar(Some(255)).typmod(), 259);
        assert_eq!(SqlType::BpChar(Some(5)).typmod(), 9);
    }

    #[test]
    fn typmod_encodes_numeric_precision_scale() {
        // numeric(p, s) packs as ((p << 16) | s) + 4. Drivers that decode
        // typmod rely on this exact bit layout.
        assert_eq!(
            SqlType::Numeric {
                precision: None,
                scale: None
            }
            .typmod(),
            -1
        );
        // numeric(10, 2) -> (10 << 16 | 2) + 4 = 655362 + 4 = 655366
        assert_eq!(
            SqlType::Numeric {
                precision: Some(10),
                scale: Some(2)
            }
            .typmod(),
            (10 << 16 | 2) + 4
        );
        // numeric(5) with no scale -> scale defaults to 0
        assert_eq!(
            SqlType::Numeric {
                precision: Some(5),
                scale: None
            }
            .typmod(),
            (5 << 16) + 4
        );
    }

    #[test]
    fn array_oids_match_builtin_table() {
        // Every array-of-T OID must match BUILTIN_TYPES' `_<t>` row.
        assert_eq!(SqlType::Array(Box::new(SqlType::Int4)).oid(), 1007);
        assert_eq!(SqlType::Array(Box::new(SqlType::Int8)).oid(), 1016);
        assert_eq!(SqlType::Array(Box::new(SqlType::Text)).oid(), 1009);
        assert_eq!(SqlType::Array(Box::new(SqlType::Bool)).oid(), 1000);
        assert_eq!(SqlType::Array(Box::new(SqlType::Uuid)).oid(), 2951);
        assert_eq!(SqlType::Array(Box::new(SqlType::Timestamptz)).oid(), 1185);
        assert_eq!(SqlType::Array(Box::new(SqlType::Jsonb)).oid(), 3807);
    }

    #[test]
    fn domain_delegates_to_base_for_typlen_and_typmod() {
        let d = SqlType::Domain {
            oid: 99_999,
            base: Box::new(SqlType::Int4),
        };
        assert_eq!(d.oid(), 99_999);
        assert_eq!(d.typlen(), 4);
        let d_varchar = SqlType::Domain {
            oid: 99_998,
            base: Box::new(SqlType::Varchar(Some(20))),
        };
        assert_eq!(d_varchar.typmod(), 24);
    }

    #[test]
    fn parse_bare_names() {
        assert_eq!(parse_sql_type_name("int4"), Some(SqlType::Int4));
        assert_eq!(parse_sql_type_name("INT4"), Some(SqlType::Int4));
        assert_eq!(parse_sql_type_name("integer"), Some(SqlType::Int4));
        assert_eq!(parse_sql_type_name("smallint"), Some(SqlType::Int2));
        assert_eq!(parse_sql_type_name("bigint"), Some(SqlType::Int8));
        assert_eq!(parse_sql_type_name("real"), Some(SqlType::Float4));
        assert_eq!(
            parse_sql_type_name("double precision"),
            Some(SqlType::Float8)
        );
        assert_eq!(parse_sql_type_name("uuid"), Some(SqlType::Uuid));
        assert_eq!(parse_sql_type_name("bytea"), Some(SqlType::Bytea));
        assert_eq!(
            parse_sql_type_name("timestamp with time zone"),
            Some(SqlType::Timestamptz)
        );
    }

    #[test]
    fn parse_length_qualified() {
        assert_eq!(
            parse_sql_type_name("varchar(10)"),
            Some(SqlType::Varchar(Some(10)))
        );
        assert_eq!(
            parse_sql_type_name("  varchar ( 255 ) "),
            Some(SqlType::Varchar(Some(255)))
        );
        assert_eq!(
            parse_sql_type_name("character varying(20)"),
            Some(SqlType::Varchar(Some(20)))
        );
        assert_eq!(
            parse_sql_type_name("char(5)"),
            Some(SqlType::BpChar(Some(5)))
        );
        assert_eq!(parse_sql_type_name("char"), Some(SqlType::BpChar(Some(1))));
        assert_eq!(parse_sql_type_name("varchar"), Some(SqlType::Varchar(None)));
    }

    #[test]
    fn parse_numeric_variants() {
        assert_eq!(
            parse_sql_type_name("numeric"),
            Some(SqlType::Numeric {
                precision: None,
                scale: None
            })
        );
        assert_eq!(
            parse_sql_type_name("numeric(10)"),
            Some(SqlType::Numeric {
                precision: Some(10),
                scale: None
            })
        );
        assert_eq!(
            parse_sql_type_name("numeric(10, 2)"),
            Some(SqlType::Numeric {
                precision: Some(10),
                scale: Some(2)
            })
        );
        assert_eq!(
            parse_sql_type_name("decimal(5,0)"),
            Some(SqlType::Numeric {
                precision: Some(5),
                scale: Some(0)
            })
        );
    }

    #[test]
    fn parse_arrays() {
        assert_eq!(
            parse_sql_type_name("int4[]"),
            Some(SqlType::Array(Box::new(SqlType::Int4)))
        );
        assert_eq!(
            parse_sql_type_name("text[]"),
            Some(SqlType::Array(Box::new(SqlType::Text)))
        );
        assert_eq!(
            parse_sql_type_name("int4[][]"),
            Some(SqlType::Array(Box::new(SqlType::Array(Box::new(
                SqlType::Int4
            )))))
        );
    }

    #[test]
    fn parse_rejects_unknown() {
        assert_eq!(parse_sql_type_name("nosuch"), None);
        assert_eq!(
            parse_sql_type_name("varchar(abc)"),
            Some(SqlType::Varchar(None))
        );
    }

    #[test]
    fn category_predicates() {
        assert!(SqlType::Int2.is_integer());
        assert!(SqlType::Int4.is_integer());
        assert!(SqlType::Int8.is_integer());
        assert!(SqlType::Oid.is_integer());
        assert!(!SqlType::Float4.is_integer());
        assert!(SqlType::Float4.is_float());
        assert!(SqlType::Text.is_text_like());
        assert!(SqlType::Varchar(Some(10)).is_text_like());
        assert!(!SqlType::Bytea.is_text_like());
    }
}
