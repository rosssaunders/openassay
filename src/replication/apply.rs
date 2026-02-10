use std::collections::HashMap;

use crate::access::transam::xact::TransactionContext;
use crate::catalog::{SearchPath, oid::Oid, with_catalog_read};
use crate::replication::pgoutput::{
    CommitMessage, DeleteMessage, InsertMessage, PgOutputMessage, RelationColumn, RelationMessage,
    TruncateMessage, UpdateMessage,
};
use crate::replication::schema_sync::{ensure_relation_from_message, TableSchema};
use crate::replication::tuple_decoder::{decode_tuple, DecodedColumn};
use crate::replication::ReplicationError;
use crate::storage::heap::{with_storage_read, with_storage_write};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::{restore_state, snapshot_state};

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
    pub local_oid: Oid,
}

impl RelationInfo {
    fn key_indexes(&self) -> Vec<usize> {
        let mut indexes: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                if column.flags & 0x01 != 0 {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        if indexes.is_empty() {
            indexes = primary_key_indexes(&self.namespace, &self.name, &self.columns);
        }
        if indexes.is_empty() {
            indexes = (0..self.columns.len()).collect();
        }
        indexes
    }
}

pub struct ApplyWorker {
    relations: HashMap<u32, RelationInfo>,
    tx_state: TransactionContext,
}

impl ApplyWorker {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            tx_state: TransactionContext::default(),
        }
    }

    pub fn register_table_schema(&mut self, table: &TableSchema, local_oid: Oid) {
        let columns = table
            .columns
            .iter()
            .map(|column| RelationColumn {
                name: column.name.clone(),
                type_oid: column.type_oid,
                flags: if column.primary_key { 0x01 } else { 0x00 },
                type_modifier: -1,
            })
            .collect();
        self.relations.insert(
            table.relation_id,
            RelationInfo {
                relation_id: table.relation_id,
                namespace: table.namespace.clone(),
                name: table.name.clone(),
                replica_identity: 0,
                columns,
                local_oid,
            },
        );
    }

    pub fn apply_message(&mut self, message: &PgOutputMessage) -> Result<(), ReplicationError> {
        match message {
            PgOutputMessage::Begin(_) => {
                self.tx_state.begin();
                Ok(())
            }
            PgOutputMessage::Commit(commit) => self.apply_commit(commit),
            PgOutputMessage::Relation(relation) => self.handle_relation(relation),
            PgOutputMessage::Insert(insert) => self.apply_in_scope(|this| this.apply_insert(insert)),
            PgOutputMessage::Update(update) => self.apply_in_scope(|this| this.apply_update(update)),
            PgOutputMessage::Delete(delete) => self.apply_in_scope(|this| this.apply_delete(delete)),
            PgOutputMessage::Truncate(truncate) => {
                self.apply_in_scope(|this| this.apply_truncate(truncate))
            }
            PgOutputMessage::Origin(_) | PgOutputMessage::Type(_) | PgOutputMessage::Message(_) => Ok(()),
        }
    }

    fn apply_commit(&mut self, _commit: &CommitMessage) -> Result<(), ReplicationError> {
        if let Some(snapshot) = self.tx_state.commit() {
            restore_state(snapshot);
        }
        Ok(())
    }

    fn handle_relation(&mut self, relation: &RelationMessage) -> Result<(), ReplicationError> {
        let local_oid = ensure_relation_from_message(relation)?;
        let mut columns: Vec<RelationColumn> = relation
            .columns
            .iter()
            .map(|column| RelationColumn {
                name: column.name.clone(),
                type_oid: column.type_oid,
                flags: column.flags,
                type_modifier: column.type_modifier,
            })
            .collect();
        if let Some(existing) = self.relations.get(&relation.relation_id) {
            for (idx, column) in columns.iter_mut().enumerate() {
                if column.flags & 0x01 == 0
                    && let Some(existing_col) = existing.columns.get(idx)
                    && existing_col.flags & 0x01 != 0
                {
                    column.flags |= 0x01;
                }
            }
        }
        self.relations.insert(
            relation.relation_id,
            RelationInfo {
                relation_id: relation.relation_id,
                namespace: relation.namespace.clone(),
                name: relation.name.clone(),
                replica_identity: relation.replica_identity,
                columns,
                local_oid,
            },
        );
        Ok(())
    }

    fn apply_insert(&mut self, insert: &InsertMessage) -> Result<(), ReplicationError> {
        let relation = self.get_relation(insert.relation_id)?;
        let decoded = decode_tuple(&relation.columns, &insert.new_tuple)?;
        let mut row = Vec::with_capacity(decoded.len());
        for value in decoded {
            match value {
                DecodedColumn::Value(value) => row.push(value),
                DecodedColumn::Unchanged => row.push(ScalarValue::Null),
            }
        }
        with_storage_write(|storage| {
            storage
                .rows_by_table
                .entry(relation.local_oid)
                .or_default()
                .push(row);
        });
        Ok(())
    }

    fn apply_update(&mut self, update: &UpdateMessage) -> Result<(), ReplicationError> {
        let relation = self.get_relation(update.relation_id)?;
        let key_indexes = relation.key_indexes();
        let decoded_new = decode_tuple(&relation.columns, &update.new_tuple)?;
        let key_values = if let Some((_, old_tuple)) = update.old_tuple.as_ref() {
            let decoded_old = decode_tuple(&relation.columns, old_tuple)?;
            extract_key_values(&decoded_old, &key_indexes)?
        } else {
            extract_key_values(&decoded_new, &key_indexes)?
        };
        if key_values.is_empty() {
            return Ok(());
        }
        let mut rows = with_storage_read(|storage| {
            storage
                .rows_by_table
                .get(&relation.local_oid)
                .cloned()
                .unwrap_or_default()
        });
        let Some(row_idx) = find_row_index(&rows, &key_indexes, &key_values) else {
            eprintln!(
                "replication update skipped: no matching row for {}.{}",
                relation.namespace, relation.name
            );
            return Ok(());
        };
        let mut updated = rows[row_idx].clone();
        for (idx, value) in decoded_new.into_iter().enumerate() {
            if let DecodedColumn::Value(value) = value && idx < updated.len() {
                updated[idx] = value;
            }
        }
        rows[row_idx] = updated;
        with_storage_write(|storage| {
            storage.rows_by_table.insert(relation.local_oid, rows);
        });
        Ok(())
    }

    fn apply_delete(&mut self, delete: &DeleteMessage) -> Result<(), ReplicationError> {
        let relation = self.get_relation(delete.relation_id)?;
        let decoded_old = decode_tuple(&relation.columns, &delete.old_tuple.1)?;
        let key_indexes = relation.key_indexes();
        let key_values = extract_key_values(&decoded_old, &key_indexes)?;
        if key_values.is_empty() {
            return Ok(());
        }
        let mut rows = with_storage_read(|storage| {
            storage
                .rows_by_table
                .get(&relation.local_oid)
                .cloned()
                .unwrap_or_default()
        });
        let Some(row_idx) = find_row_index(&rows, &key_indexes, &key_values) else {
            eprintln!(
                "replication delete skipped: no matching row for {}.{}",
                relation.namespace, relation.name
            );
            return Ok(());
        };
        rows.remove(row_idx);
        with_storage_write(|storage| {
            storage.rows_by_table.insert(relation.local_oid, rows);
        });
        Ok(())
    }

    fn apply_truncate(&mut self, truncate: &TruncateMessage) -> Result<(), ReplicationError> {
        with_storage_write(|storage| {
            for relation_id in &truncate.relation_ids {
                if let Some(info) = self.relations.get(relation_id) {
                    storage.rows_by_table.insert(info.local_oid, Vec::new());
                }
            }
        });
        Ok(())
    }

    fn apply_in_scope<F>(&mut self, action: F) -> Result<(), ReplicationError>
    where
        F: FnOnce(&mut ApplyWorker) -> Result<(), ReplicationError>,
    {
        if !self.tx_state.in_explicit_block() {
            return action(self);
        }
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| ReplicationError {
                message: "replication transaction missing working snapshot".to_string(),
            })?;
        restore_state(working);
        let result = action(self);
        let next_working = result.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);
        if let Some(snapshot) = next_working {
            self.tx_state.set_working_snapshot(snapshot);
        }
        result
    }

    fn get_relation(&self, relation_id: u32) -> Result<&RelationInfo, ReplicationError> {
        self.relations.get(&relation_id).ok_or_else(|| ReplicationError {
            message: format!("unknown relation id {}", relation_id),
        })
    }
}

impl Default for ApplyWorker {
    fn default() -> Self {
        Self::new()
    }
}

fn extract_key_values(
    decoded: &[DecodedColumn],
    key_indexes: &[usize],
) -> Result<Vec<ScalarValue>, ReplicationError> {
    let mut values = Vec::with_capacity(key_indexes.len());
    for idx in key_indexes {
        let Some(value) = decoded.get(*idx) else {
            return Err(ReplicationError {
                message: "key index out of bounds".to_string(),
            });
        };
        match value {
            DecodedColumn::Value(value) => values.push(value.clone()),
            DecodedColumn::Unchanged => return Ok(Vec::new()),
        }
    }
    Ok(values)
}

fn find_row_index(
    rows: &[Vec<ScalarValue>],
    key_indexes: &[usize],
    key_values: &[ScalarValue],
) -> Option<usize> {
    rows.iter().position(|row| {
        key_indexes.iter().enumerate().all(|(i, idx)| {
            row.get(*idx)
                .map(|value| value == &key_values[i])
                .unwrap_or(false)
        })
    })
}

fn primary_key_indexes(
    namespace: &str,
    table: &str,
    columns: &[RelationColumn],
) -> Vec<usize> {
    let key_columns = with_catalog_read(|catalog| {
        catalog
            .resolve_table(
                &[namespace.to_string(), table.to_string()],
                &SearchPath::default(),
            )
            .ok()
            .and_then(|table| {
                table
                    .key_constraints()
                    .iter()
                    .find(|constraint| constraint.primary)
                    .map(|constraint| constraint.columns.clone())
            })
    })
    .unwrap_or_default();
    key_columns
        .into_iter()
        .filter_map(|key| columns.iter().position(|col| col.name == key))
        .collect()
}
