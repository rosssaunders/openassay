use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::catalog::oid::Oid;
use crate::storage::tuple::ScalarValue;

#[derive(Debug, Default)]
pub(crate) struct InMemoryStorage {
    pub(crate) rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
}

static GLOBAL_STORAGE: OnceLock<RwLock<InMemoryStorage>> = OnceLock::new();

fn global_storage() -> &'static RwLock<InMemoryStorage> {
    GLOBAL_STORAGE.get_or_init(|| RwLock::new(InMemoryStorage::default()))
}

pub(crate) fn with_storage_read<T>(f: impl FnOnce(&InMemoryStorage) -> T) -> T {
    let storage = global_storage()
        .read()
        .expect("global storage lock poisoned for read");
    f(&storage)
}

pub(crate) fn with_storage_write<T>(f: impl FnOnce(&mut InMemoryStorage) -> T) -> T {
    let mut storage = global_storage()
        .write()
        .expect("global storage lock poisoned for write");
    f(&mut storage)
}
