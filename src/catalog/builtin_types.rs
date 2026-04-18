//! Canonical Postgres built-in types surfaced through `pg_catalog.pg_type`.
//!
//! Clients (tokio-postgres, asyncpg, psycopg, node-postgres, …) resolve
//! unknown type OIDs by querying `pg_catalog.pg_type`. If the JOIN onto
//! `pg_namespace` or the expected column set doesn't line up, those clients
//! fail — often catastrophically, by infinite-retry. This module is the
//! source of truth for every PG type OID the engine hands out on the wire.
//!
//! OID, typlen, typbyval, typtype, typcategory, typelem, and typarray values
//! come from `src/include/catalog/pg_type.dat` in the Postgres source tree.
//! We ship the subset the planner/executor can actually produce plus the
//! pseudo-types every driver expects to find (`void`, `record`, `unknown`).

use super::oid::Oid;

#[derive(Debug, Clone, Copy)]
pub struct BuiltinType {
    pub oid: Oid,
    pub name: &'static str,
    /// Fixed byte length, or -1 for variable-length.
    pub typlen: i16,
    /// True if values are passed by value (fixed-size, register-width).
    pub typbyval: bool,
    /// 'b' = base, 'c' = composite, 'd' = domain, 'e' = enum, 'p' = pseudo,
    /// 'r' = range, 'm' = multirange.
    pub typtype: char,
    /// Type category (pg_type.dat's typcategory). Drivers use this to group
    /// related types ('N' numeric, 'S' string, 'D' date/time, 'B' boolean,
    /// 'A' array, 'E' enum, 'P' pseudo, 'U' user-defined, 'V' bitstring,
    /// 'X' unknown, 'Z' internal-use).
    pub typcategory: char,
    /// OID of the element type for array types, else 0.
    pub typelem: Oid,
    /// OID of the corresponding array type, else 0.
    pub typarray: Oid,
    /// Delimiter used by array_in / array_out. Almost always ','.
    pub typdelim: char,
    /// Alignment: 'c' char, 's' short (2), 'i' int (4), 'd' double (8).
    pub typalign: char,
    /// Storage strategy: 'p' plain, 'e' external, 'm' main, 'x' extended.
    pub typstorage: char,
}

const fn base(
    oid: Oid,
    name: &'static str,
    typlen: i16,
    typbyval: bool,
    typcategory: char,
    typarray: Oid,
    typalign: char,
    typstorage: char,
) -> BuiltinType {
    BuiltinType {
        oid,
        name,
        typlen,
        typbyval,
        typtype: 'b',
        typcategory,
        typelem: 0,
        typarray,
        typdelim: ',',
        typalign,
        typstorage,
    }
}

const fn array(oid: Oid, name: &'static str, element_oid: Oid, typalign: char) -> BuiltinType {
    BuiltinType {
        oid,
        name,
        typlen: -1,
        typbyval: false,
        typtype: 'b',
        typcategory: 'A',
        typelem: element_oid,
        typarray: 0,
        typdelim: ',',
        typalign,
        typstorage: 'x',
    }
}

const fn pseudo(oid: Oid, name: &'static str, typcategory: char) -> BuiltinType {
    BuiltinType {
        oid,
        name,
        typlen: 4,
        typbyval: true,
        typtype: 'p',
        typcategory,
        typelem: 0,
        typarray: 0,
        typdelim: ',',
        typalign: 'i',
        typstorage: 'p',
    }
}

/// Built-in types shipped in `pg_catalog.pg_type`. OIDs and ancillary fields
/// match the stock Postgres bootstrap catalog. Kept sorted by OID so pg_type
/// scans return a stable, PG-like ordering.
pub const BUILTIN_TYPES: &[BuiltinType] = &[
    // Boolean
    base(16, "bool", 1, true, 'B', 1000, 'c', 'p'),
    // Binary
    base(17, "bytea", -1, false, 'U', 1001, 'i', 'x'),
    // Character types
    base(18, "char", 1, true, 'S', 1002, 'c', 'p'),
    base(19, "name", 64, false, 'S', 1003, 'c', 'p'),
    // Integers
    base(20, "int8", 8, true, 'N', 1016, 'd', 'p'),
    base(21, "int2", 2, true, 'N', 1005, 's', 'p'),
    base(22, "int2vector", -1, false, 'A', 1006, 'i', 'p'),
    base(23, "int4", 4, true, 'N', 1007, 'i', 'p'),
    // OID family
    base(24, "regproc", 4, true, 'N', 1008, 'i', 'p'),
    base(25, "text", -1, false, 'S', 1009, 'i', 'x'),
    base(26, "oid", 4, true, 'N', 1028, 'i', 'p'),
    base(27, "tid", 6, false, 'U', 1010, 's', 'p'),
    base(28, "xid", 4, true, 'U', 1011, 'i', 'p'),
    base(29, "cid", 4, true, 'U', 1012, 'i', 'p'),
    base(30, "oidvector", -1, false, 'A', 1013, 'i', 'p'),
    // JSON
    base(114, "json", -1, false, 'U', 199, 'i', 'x'),
    base(142, "xml", -1, false, 'U', 143, 'i', 'x'),
    // Arrays of low-OID types (sorted near low OIDs to keep consistency)
    array(143, "_xml", 142, 'i'),
    array(199, "_json", 114, 'i'),
    // Geometric (complete the low-OID block even though the executor doesn't
    // produce them — drivers still look these up when mapping OIDs).
    base(600, "point", 16, false, 'G', 1017, 'd', 'p'),
    base(601, "lseg", 32, false, 'G', 1018, 'd', 'p'),
    base(602, "path", -1, false, 'G', 1019, 'd', 'x'),
    base(603, "box", 32, false, 'G', 1020, 'd', 'p'),
    base(604, "polygon", -1, false, 'G', 1027, 'd', 'x'),
    base(628, "line", 24, false, 'G', 629, 'd', 'p'),
    array(629, "_line", 628, 'd'),
    // Network
    base(650, "cidr", -1, false, 'I', 651, 'i', 'm'),
    array(651, "_cidr", 650, 'i'),
    // Floats
    base(700, "float4", 4, true, 'N', 1021, 'i', 'p'),
    base(701, "float8", 8, true, 'N', 1022, 'd', 'p'),
    base(718, "circle", 24, false, 'G', 719, 'd', 'p'),
    array(719, "_circle", 718, 'd'),
    // Money
    base(790, "money", 8, true, 'N', 791, 'd', 'p'),
    array(791, "_money", 790, 'd'),
    // MAC address
    base(829, "macaddr", 6, false, 'U', 1040, 'i', 'p'),
    base(869, "inet", -1, false, 'I', 1041, 'i', 'm'),
    // Low-OID arrays
    array(1000, "_bool", 16, 'c'),
    array(1001, "_bytea", 17, 'i'),
    array(1002, "_char", 18, 'c'),
    array(1003, "_name", 19, 'c'),
    array(1005, "_int2", 21, 's'),
    array(1006, "_int2vector", 22, 'i'),
    array(1007, "_int4", 23, 'i'),
    array(1008, "_regproc", 24, 'i'),
    array(1009, "_text", 25, 'i'),
    array(1010, "_tid", 27, 's'),
    array(1011, "_xid", 28, 'i'),
    array(1012, "_cid", 29, 'i'),
    array(1013, "_oidvector", 30, 'i'),
    array(1014, "_bpchar", 1042, 'i'),
    array(1015, "_varchar", 1043, 'i'),
    array(1016, "_int8", 20, 'd'),
    array(1017, "_point", 600, 'd'),
    array(1018, "_lseg", 601, 'd'),
    array(1019, "_path", 602, 'd'),
    array(1020, "_box", 603, 'd'),
    array(1021, "_float4", 700, 'i'),
    array(1022, "_float8", 701, 'd'),
    array(1027, "_polygon", 604, 'd'),
    array(1028, "_oid", 26, 'i'),
    // Character types (bpchar / varchar)
    base(1042, "bpchar", -1, false, 'S', 1014, 'i', 'x'),
    base(1043, "varchar", -1, false, 'S', 1015, 'i', 'x'),
    // Date/time
    base(1082, "date", 4, true, 'D', 1182, 'i', 'p'),
    base(1083, "time", 8, true, 'D', 1183, 'd', 'p'),
    base(1114, "timestamp", 8, true, 'D', 1115, 'd', 'p'),
    array(1115, "_timestamp", 1114, 'd'),
    array(1182, "_date", 1082, 'i'),
    array(1183, "_time", 1083, 'd'),
    base(1184, "timestamptz", 8, true, 'D', 1185, 'd', 'p'),
    array(1185, "_timestamptz", 1184, 'd'),
    base(1186, "interval", 16, false, 'T', 1187, 'd', 'p'),
    array(1187, "_interval", 1186, 'd'),
    array(1040, "_macaddr", 829, 'i'),
    array(1041, "_inet", 869, 'i'),
    base(1266, "timetz", 12, false, 'D', 1270, 'd', 'p'),
    array(1270, "_timetz", 1266, 'd'),
    // Bit string
    base(1560, "bit", -1, false, 'V', 1561, 'i', 'x'),
    array(1561, "_bit", 1560, 'i'),
    base(1562, "varbit", -1, false, 'V', 1563, 'i', 'x'),
    array(1563, "_varbit", 1562, 'i'),
    // Numeric
    base(1700, "numeric", -1, false, 'N', 1231, 'i', 'm'),
    array(1231, "_numeric", 1700, 'i'),
    // Refcursor
    base(1790, "refcursor", -1, false, 'U', 2201, 'i', 'x'),
    array(2201, "_refcursor", 1790, 'i'),
    // Pseudo-types (needed by many drivers' type-resolution flow)
    pseudo(2249, "record", 'P'),
    pseudo(2275, "cstring", 'P'),
    pseudo(2276, "any", 'P'),
    pseudo(2277, "anyarray", 'P'),
    pseudo(2278, "void", 'P'),
    pseudo(2279, "trigger", 'P'),
    pseudo(2280, "language_handler", 'P'),
    pseudo(2281, "internal", 'P'),
    pseudo(2283, "anyelement", 'P'),
    pseudo(2776, "anynonarray", 'P'),
    pseudo(3500, "anyenum", 'P'),
    pseudo(3831, "anyrange", 'P'),
    pseudo(4537, "anymultirange", 'P'),
    // UUID
    base(2950, "uuid", 16, false, 'U', 2951, 'c', 'p'),
    array(2951, "_uuid", 2950, 'c'),
    // Text search
    base(3614, "tsvector", -1, false, 'U', 3643, 'i', 'x'),
    array(3643, "_tsvector", 3614, 'i'),
    base(3615, "tsquery", -1, false, 'U', 3645, 'i', 'x'),
    array(3645, "_tsquery", 3615, 'i'),
    // JSONB
    base(3802, "jsonb", -1, false, 'U', 3807, 'i', 'x'),
    array(3807, "_jsonb", 3802, 'i'),
    // Range types
    BuiltinType {
        oid: 3904,
        name: "int4range",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3905,
        typdelim: ',',
        typalign: 'i',
        typstorage: 'x',
    },
    array(3905, "_int4range", 3904, 'i'),
    BuiltinType {
        oid: 3906,
        name: "numrange",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3907,
        typdelim: ',',
        typalign: 'i',
        typstorage: 'x',
    },
    array(3907, "_numrange", 3906, 'i'),
    BuiltinType {
        oid: 3908,
        name: "tsrange",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3909,
        typdelim: ',',
        typalign: 'd',
        typstorage: 'x',
    },
    array(3909, "_tsrange", 3908, 'd'),
    BuiltinType {
        oid: 3910,
        name: "tstzrange",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3911,
        typdelim: ',',
        typalign: 'd',
        typstorage: 'x',
    },
    array(3911, "_tstzrange", 3910, 'd'),
    BuiltinType {
        oid: 3912,
        name: "daterange",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3913,
        typdelim: ',',
        typalign: 'i',
        typstorage: 'x',
    },
    array(3913, "_daterange", 3912, 'i'),
    BuiltinType {
        oid: 3926,
        name: "int8range",
        typlen: -1,
        typbyval: false,
        typtype: 'r',
        typcategory: 'R',
        typelem: 0,
        typarray: 3927,
        typdelim: ',',
        typalign: 'd',
        typstorage: 'x',
    },
    array(3927, "_int8range", 3926, 'd'),
];

/// Range-type metadata surfaced through `pg_catalog.pg_range`. Each entry's
/// `rngtypid` matches the corresponding BUILTIN_TYPES row.
pub struct BuiltinRange {
    pub rngtypid: Oid,
    pub rngsubtype: Oid,
    pub rngmultitypid: Oid,
}

pub const BUILTIN_RANGES: &[BuiltinRange] = &[
    BuiltinRange {
        rngtypid: 3904,
        rngsubtype: 23,
        rngmultitypid: 4451,
    },
    BuiltinRange {
        rngtypid: 3906,
        rngsubtype: 1700,
        rngmultitypid: 4532,
    },
    BuiltinRange {
        rngtypid: 3908,
        rngsubtype: 1114,
        rngmultitypid: 4533,
    },
    BuiltinRange {
        rngtypid: 3910,
        rngsubtype: 1184,
        rngmultitypid: 4534,
    },
    BuiltinRange {
        rngtypid: 3912,
        rngsubtype: 1082,
        rngmultitypid: 4535,
    },
    BuiltinRange {
        rngtypid: 3926,
        rngsubtype: 20,
        rngmultitypid: 4536,
    },
];

/// Built-in procedural languages surfaced through `pg_catalog.pg_language`.
pub struct BuiltinLanguage {
    pub oid: Oid,
    pub name: &'static str,
    pub lanispl: bool,
    pub lanpltrusted: bool,
}

pub const BUILTIN_LANGUAGES: &[BuiltinLanguage] = &[
    BuiltinLanguage {
        oid: 12,
        name: "internal",
        lanispl: false,
        lanpltrusted: false,
    },
    BuiltinLanguage {
        oid: 13,
        name: "c",
        lanispl: false,
        lanpltrusted: false,
    },
    BuiltinLanguage {
        oid: 14,
        name: "sql",
        lanispl: false,
        lanpltrusted: true,
    },
    BuiltinLanguage {
        oid: 13_288,
        name: "plpgsql",
        lanispl: true,
        lanpltrusted: true,
    },
];

/// Built-in collations surfaced through `pg_catalog.pg_collation`.
pub struct BuiltinCollation {
    pub oid: Oid,
    pub name: &'static str,
    pub encoding: i32,
    pub collcollate: &'static str,
    pub collctype: &'static str,
}

pub const BUILTIN_COLLATIONS: &[BuiltinCollation] = &[
    BuiltinCollation {
        oid: 100,
        name: "default",
        encoding: -1,
        collcollate: "",
        collctype: "",
    },
    BuiltinCollation {
        oid: 950,
        name: "C",
        encoding: -1,
        collcollate: "C",
        collctype: "C",
    },
    BuiltinCollation {
        oid: 951,
        name: "POSIX",
        encoding: -1,
        collcollate: "POSIX",
        collctype: "POSIX",
    },
];
