//! Postgres object-identifier primitives.
//!
//! Crate-root leaf module so both `catalog` and `types` can depend on it
//! without either reaching into the other. The split was introduced during
//! Phase 2 when `types::SqlType` grew to carry wire-level OIDs; before that
//! the `Oid` alias lived in `catalog::oid`, which forced `types` to import
//! from `catalog` (a higher-level module in terms of semantics).
//!
//! Only the primitive alias and canonical PG-bootstrap constants live here.
//! The `OidGenerator` stays in `catalog::oid` because it models catalog state
//! (error-on-exhaustion behaviour and `CatalogError`) rather than the pure
//! identifier concept.

pub type Oid = u32;

/// The lowest OID catalog generators hand out for non-bootstrap objects.
/// Matches Postgres's `FirstNormalObjectId` from `src/include/access/transam.h`.
pub const FIRST_NORMAL_OID: Oid = 16_384;

// PG-canonical namespace OIDs. These match Postgres's bootstrap constants so
// that catalog JOINs like `pg_type.typnamespace = pg_namespace.oid` hit the
// rows real drivers expect.
pub const PG_CATALOG_NAMESPACE_OID: Oid = 11;
pub const PUBLIC_NAMESPACE_OID: Oid = 2_200;
pub const INFORMATION_SCHEMA_NAMESPACE_OID: Oid = 13_000;

/// Superuser role OID used wherever pg_catalog rows need an owner.
pub const BOOTSTRAP_SUPERUSER_OID: Oid = 10;
