use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::catalog::oid::Oid;
use crate::storage::btree::{BTreeIndex, CompositeKey};
use crate::storage::tuple::ScalarValue;

pub(crate) const DEFAULT_BTREE_ORDER: usize = 48;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredIndex {
    pub(crate) column_names: Vec<String>,
    pub(crate) column_indexes: Vec<usize>,
    pub(crate) unique: bool,
    pub(crate) btree: BTreeIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredIndexDescriptor {
    pub(crate) name: String,
    pub(crate) column_names: Vec<String>,
    pub(crate) column_indexes: Vec<usize>,
    pub(crate) unique: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryStorage {
    pub(crate) rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
    pub(crate) indexes_by_table: HashMap<(Oid, String), StoredIndex>,
}

impl InMemoryStorage {
    pub(crate) fn register_index(
        &mut self,
        table_oid: Oid,
        index_name: String,
        column_names: Vec<String>,
        column_indexes: Vec<usize>,
        unique: bool,
    ) -> Result<(), String> {
        if column_names.len() != column_indexes.len() {
            return Err("index column metadata is inconsistent".to_string());
        }
        if column_names.is_empty() {
            return Err("index must include at least one column".to_string());
        }
        let normalized_name = index_name.to_ascii_lowercase();
        let key = (table_oid, normalized_name.clone());
        if self.indexes_by_table.contains_key(&key) {
            return Err(format!(
                "index \"{normalized_name}\" already exists for relation OID {table_oid}"
            ));
        }

        self.indexes_by_table.insert(
            key,
            StoredIndex {
                column_names,
                column_indexes,
                unique,
                btree: BTreeIndex::new(DEFAULT_BTREE_ORDER),
            },
        );
        Ok(())
    }

    pub(crate) fn rebuild_index(&mut self, table_oid: Oid, index_name: &str) -> Result<(), String> {
        let normalized_name = index_name.to_ascii_lowercase();
        let key = (table_oid, normalized_name.clone());
        let rows = self
            .rows_by_table
            .get(&table_oid)
            .cloned()
            .unwrap_or_default();
        let (column_indexes, unique, order) = {
            let index = self.indexes_by_table.get(&key).ok_or_else(|| {
                format!("index \"{normalized_name}\" does not exist for relation OID {table_oid}")
            })?;
            (
                index.column_indexes.clone(),
                index.unique,
                index.btree.max_entries(),
            )
        };
        let mut rebuilt = BTreeIndex::new(order);
        for (offset, row) in rows.iter().enumerate() {
            let composite_key = composite_key_from_row(row, &column_indexes)?;
            if unique
                && !composite_key_contains_nulls(&composite_key)
                && !rebuilt.search(&composite_key).is_empty()
            {
                return Err(format!(
                    "duplicate value violates unique index \"{normalized_name}\""
                ));
            }
            rebuilt.insert(composite_key, offset);
        }
        let index = self.indexes_by_table.get_mut(&key).ok_or_else(|| {
            format!("index \"{normalized_name}\" does not exist for relation OID {table_oid}")
        })?;
        index.btree = rebuilt;
        Ok(())
    }

    pub(crate) fn replace_rows_for_table(
        &mut self,
        table_oid: Oid,
        rows: Vec<Vec<ScalarValue>>,
    ) -> Result<(), String> {
        self.rows_by_table.insert(table_oid, rows);
        self.rebuild_indexes_for_table(table_oid)
    }

    pub(crate) fn append_row(
        &mut self,
        table_oid: Oid,
        row: Vec<ScalarValue>,
    ) -> Result<usize, String> {
        let offset = self
            .rows_by_table
            .get(&table_oid)
            .map_or(0, std::vec::Vec::len);
        let index_names = self.index_names_for_table(table_oid);
        let mut inserted_keys: Vec<(String, CompositeKey)> = Vec::new();

        for index_name in &index_names {
            let composite_key = {
                let index = self.index_for_table(table_oid, index_name).ok_or_else(|| {
                    format!("index \"{index_name}\" does not exist for relation OID {table_oid}")
                })?;
                composite_key_from_row(&row, &index.column_indexes)?
            };
            let index = self
                .index_mut_for_table(table_oid, index_name)
                .ok_or_else(|| {
                    format!("index \"{index_name}\" does not exist for relation OID {table_oid}")
                })?;
            index.btree.insert(composite_key.clone(), offset);
            inserted_keys.push((index_name.clone(), composite_key));
        }

        if let Some(rows) = self.rows_by_table.get_mut(&table_oid) {
            rows.push(row);
        } else {
            self.rows_by_table.insert(table_oid, vec![row]);
        }
        let row_len = self
            .rows_by_table
            .get(&table_oid)
            .map_or(0, std::vec::Vec::len);
        if row_len != offset + 1 {
            for (index_name, composite_key) in inserted_keys {
                if let Some(index) = self.index_mut_for_table(table_oid, &index_name) {
                    index.btree.delete(&composite_key, offset)?;
                }
            }
            return Err("failed to append row to storage".to_string());
        }
        Ok(offset)
    }

    pub(crate) fn update_row(
        &mut self,
        table_oid: Oid,
        offset: usize,
        row: Vec<ScalarValue>,
    ) -> Result<(), String> {
        let Some(current_rows) = self.rows_by_table.get(&table_oid) else {
            return Err(format!("relation OID {table_oid} has no row storage"));
        };
        let Some(existing_row) = current_rows.get(offset).cloned() else {
            return Err(format!(
                "row offset {offset} does not exist in relation OID {table_oid}"
            ));
        };

        let index_names = self.index_names_for_table(table_oid);
        for index_name in &index_names {
            let (old_key, new_key) = {
                let index = self.index_for_table(table_oid, index_name).ok_or_else(|| {
                    format!("index \"{index_name}\" does not exist for relation OID {table_oid}")
                })?;
                (
                    composite_key_from_row(&existing_row, &index.column_indexes)?,
                    composite_key_from_row(&row, &index.column_indexes)?,
                )
            };
            if old_key == new_key {
                continue;
            }
            let index = self
                .index_mut_for_table(table_oid, index_name)
                .ok_or_else(|| {
                    format!("index \"{index_name}\" does not exist for relation OID {table_oid}")
                })?;
            index.btree.delete(&old_key, offset)?;
            index.btree.insert(new_key, offset);
        }

        if let Some(rows) = self.rows_by_table.get_mut(&table_oid)
            && offset < rows.len()
        {
            rows[offset] = row;
            return Ok(());
        }
        Err(format!(
            "row offset {offset} does not exist in relation OID {table_oid}"
        ))
    }

    pub(crate) fn delete_rows_by_offsets(
        &mut self,
        table_oid: Oid,
        offsets: &[usize],
    ) -> Result<(), String> {
        if offsets.is_empty() {
            return Ok(());
        }
        let Some(rows) = self.rows_by_table.get(&table_oid) else {
            return Err(format!("relation OID {table_oid} has no row storage"));
        };
        let mut sorted = offsets.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.iter().any(|offset| *offset >= rows.len()) {
            return Err(format!(
                "row offset out of bounds for relation OID {table_oid}"
            ));
        }
        let removed_rows = sorted
            .iter()
            .map(|offset| rows[*offset].clone())
            .collect::<Vec<_>>();

        let index_names = self.index_names_for_table(table_oid);
        for (offset, removed_row) in sorted.iter().zip(removed_rows.iter()) {
            for index_name in &index_names {
                let composite_key = {
                    let index = self.index_for_table(table_oid, index_name).ok_or_else(|| {
                        format!(
                            "index \"{index_name}\" does not exist for relation OID {table_oid}"
                        )
                    })?;
                    composite_key_from_row(removed_row, &index.column_indexes)?
                };
                let index = self
                    .index_mut_for_table(table_oid, index_name)
                    .ok_or_else(|| {
                        format!(
                            "index \"{index_name}\" does not exist for relation OID {table_oid}"
                        )
                    })?;
                index.btree.delete(&composite_key, *offset)?;
            }
        }

        if let Some(rows) = self.rows_by_table.get_mut(&table_oid) {
            for offset in sorted.iter().rev() {
                rows.remove(*offset);
            }
        }
        for index_name in &index_names {
            if let Some(index) = self.index_mut_for_table(table_oid, index_name) {
                index.btree.remap_offsets_after_deletions(&sorted);
            }
        }
        Ok(())
    }

    pub(crate) fn index_offsets_for_key(
        &self,
        table_oid: Oid,
        index_name: &str,
        key: &[ScalarValue],
    ) -> Vec<usize> {
        self.index_for_table(table_oid, index_name)
            .map_or_else(Vec::new, |index| index.btree.search(key))
    }

    pub(crate) fn index_for_table(&self, table_oid: Oid, index_name: &str) -> Option<&StoredIndex> {
        self.indexes_by_table
            .get(&(table_oid, index_name.to_ascii_lowercase()))
    }

    pub(crate) fn index_mut_for_table(
        &mut self,
        table_oid: Oid,
        index_name: &str,
    ) -> Option<&mut StoredIndex> {
        self.indexes_by_table
            .get_mut(&(table_oid, index_name.to_ascii_lowercase()))
    }

    pub(crate) fn index_descriptors_for_table(&self, table_oid: Oid) -> Vec<StoredIndexDescriptor> {
        let mut descriptors = self
            .indexes_by_table
            .iter()
            .filter_map(|((oid, name), index)| {
                if *oid != table_oid {
                    return None;
                }
                Some(StoredIndexDescriptor {
                    name: name.clone(),
                    column_names: index.column_names.clone(),
                    column_indexes: index.column_indexes.clone(),
                    unique: index.unique,
                })
            })
            .collect::<Vec<_>>();
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        descriptors
    }

    pub(crate) fn drop_index(&mut self, table_oid: Oid, index_name: &str) {
        self.indexes_by_table
            .remove(&(table_oid, index_name.to_ascii_lowercase()));
    }

    pub(crate) fn drop_all_indexes_for_table(&mut self, table_oid: Oid) {
        self.indexes_by_table
            .retain(|(oid, _), _| *oid != table_oid);
    }

    pub(crate) fn rebuild_indexes_for_table(&mut self, table_oid: Oid) -> Result<(), String> {
        let index_names = self.index_names_for_table(table_oid);
        for index_name in &index_names {
            self.rebuild_index(table_oid, index_name)?;
        }
        Ok(())
    }

    fn index_names_for_table(&self, table_oid: Oid) -> Vec<String> {
        let mut names = self
            .indexes_by_table
            .keys()
            .filter_map(|(oid, name)| {
                if *oid == table_oid {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
    }
}

fn composite_key_from_row(row: &[ScalarValue], indexes: &[usize]) -> Result<CompositeKey, String> {
    let mut out = Vec::with_capacity(indexes.len());
    for idx in indexes {
        let value = row
            .get(*idx)
            .cloned()
            .ok_or_else(|| format!("row does not have index column offset {idx}"))?;
        out.push(value);
    }
    Ok(out)
}

fn composite_key_contains_nulls(key: &[ScalarValue]) -> bool {
    key.iter().any(|value| matches!(value, ScalarValue::Null))
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
