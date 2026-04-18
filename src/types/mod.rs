//! SQL type system for OpenAssay.
//!
//! `SqlType` is the declared SQL-level type of a column or expression. It is
//! distinct from `ScalarValue` (runtime storage representation) and from
//! `catalog::TypeSignature` (coarse catalog storage — 8 variants). `SqlType`
//! is rich enough to carry everything drivers need to see on the wire:
//! int2 vs int4 vs int8, varchar(N)'s length, numeric(p, s)'s precision and
//! scale, array element types, etc.
//!
//! ## Why three type representations exist
//!
//! - **`ScalarValue`** (`storage::tuple`): runtime storage. Collapses int2/
//!   int4/int8 to `Int(i64)`, float4/float8 to `Float(f64)`, any string type
//!   to `Text(String)`. This is fine for execution — the extra precision
//!   doesn't affect arithmetic.
//! - **`TypeSignature`** (`catalog::table`): what a table column persists
//!   about its declared type. Shipped in Phase 0 with 8 variants. Kept
//!   in place during Phase 2 to avoid a flag-day change to every catalog
//!   consumer.
//! - **`SqlType`** (this module): the wire-level declared type, used by
//!   `RowDescription`, binary encoding/decoding, and typed parameter
//!   handling. Introduced in Phase 2.
//!
//! When a column is read off disk, the catalog's `TypeSignature` is combined
//! with any typmod metadata to produce a `SqlType` that the planner threads
//! through to the executor and then to the wire.
//!
//! ## Canonical OIDs
//!
//! Every variant maps to a specific built-in Postgres type OID via `oid()`.
//! Those OIDs match `pg_type.oid` rows served by
//! `catalog::builtin_types::BUILTIN_TYPES`. Do not invent OIDs — pick from
//! what's in that table. If an OID isn't yet in `BUILTIN_TYPES`, add the
//! row there first.

pub mod sql_type;

pub use sql_type::{SqlType, parse_sql_type_name};
