use super::CatalogError;

pub type Oid = u32;

pub const FIRST_NORMAL_OID: Oid = 16_384;

// PG-canonical namespace OIDs. These match Postgres's bootstrap constants so
// that catalog JOINs like `pg_type.typnamespace = pg_namespace.oid` hit the
// rows real drivers expect.
pub const PG_CATALOG_NAMESPACE_OID: Oid = 11;
pub const PUBLIC_NAMESPACE_OID: Oid = 2_200;
pub const INFORMATION_SCHEMA_NAMESPACE_OID: Oid = 13_000;

// Superuser-role OID used wherever pg_catalog rows need an owner.
pub const BOOTSTRAP_SUPERUSER_OID: Oid = 10;

#[derive(Debug, Clone)]
pub struct OidGenerator {
    next: Oid,
}

impl Default for OidGenerator {
    fn default() -> Self {
        Self::new(FIRST_NORMAL_OID)
    }
}

impl OidGenerator {
    pub const fn new(start: Oid) -> Self {
        Self { next: start }
    }

    pub fn next_oid(&mut self) -> Result<Oid, CatalogError> {
        let oid = self.next;
        self.next = self.next.checked_add(1).ok_or_else(|| CatalogError {
            message: "catalog OID space exhausted".to_string(),
        })?;
        Ok(oid)
    }
}
