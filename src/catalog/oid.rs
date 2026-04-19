use super::CatalogError;

// Re-exports from the crate-root `oid` module. Kept here so existing
// callers that import `crate::catalog::oid::Oid` continue to work after
// the leaf module was introduced (Phase 2 layering cleanup).
pub use crate::oid::{
    BOOTSTRAP_SUPERUSER_OID, FIRST_NORMAL_OID, INFORMATION_SCHEMA_NAMESPACE_OID, Oid,
    PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID,
};

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
