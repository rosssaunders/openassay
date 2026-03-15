use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::kernels::cmp;
use arrow::compute::{and_kleene, filter_record_batch, ilike, like, nilike, nlike};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::catalog::oid::Oid;
use crate::catalog::{TypeSignature, with_catalog_read};
use crate::executor::column_batch::ColumnBatch;
use crate::executor::profiling;
use crate::storage::btree::{BTreeIndex, CompositeKey};
use crate::storage::tuple::{
    ScalarValue, append_scalar_value_to_builder, arrow_value_to_scalar_value, scalar_values_schema,
};
use crate::utils::adt::misc::{compare_values_for_predicate, like_matches};

pub(crate) const DEFAULT_BTREE_ORDER: usize = 48;
pub(crate) const COLUMNAR_BATCH_SIZE: usize = 1_024;

type SelectedOffsetsBatchCallback<'a> =
    dyn FnMut(&RecordBatch, Vec<usize>, Vec<usize>) -> Result<bool, String> + 'a;
type RecordBatchScanCallback<'a> =
    dyn FnMut(&RecordBatch, &[bool], usize) -> Result<bool, String> + 'a;

#[derive(Debug, Clone)]
pub(crate) struct ColumnarBatch {
    pub(crate) record_batch: RecordBatch,
    pub(crate) deleted_rows: Vec<bool>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ColumnarTable {
    pub(crate) column_names: Vec<String>,
    pub(crate) schema: Option<Arc<Schema>>,
    pub(crate) batches: Vec<ColumnarBatch>,
    pub(crate) pending_rows: Vec<Vec<ScalarValue>>,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanPredicateOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    NotLike,
    ILike,
    NotILike,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScanPredicate {
    pub(crate) column_index: usize,
    pub(crate) op: ScanPredicateOp,
    pub(crate) value: ScalarValue,
    pub(crate) escape: Option<char>,
}

enum LikePredicateMatcher {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Generic {
        pattern: String,
        escape: Option<char>,
        case_insensitive: bool,
    },
}

impl LikePredicateMatcher {
    fn new(pattern: String, escape: Option<char>, case_insensitive: bool) -> Self {
        classify_like_pattern(&pattern, escape, case_insensitive).unwrap_or(Self::Generic {
            pattern,
            escape,
            case_insensitive,
        })
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Exact(literal) => value == literal,
            Self::Prefix(literal) => value.starts_with(literal),
            Self::Suffix(literal) => value.ends_with(literal),
            Self::Contains(literal) => value.contains(literal),
            Self::Generic {
                pattern,
                escape,
                case_insensitive,
            } => {
                if *case_insensitive {
                    like_matches(
                        &value.to_ascii_lowercase(),
                        &pattern.to_ascii_lowercase(),
                        *escape,
                    )
                } else {
                    like_matches(value, pattern, *escape)
                }
            }
        }
    }
}

enum CompiledScanPredicate<'a> {
    Int64 {
        values: &'a Int64Array,
        op: ScanPredicateOp,
        literal: i64,
    },
    Float64 {
        values: &'a Float64Array,
        op: ScanPredicateOp,
        literal: f64,
    },
    Text {
        values: &'a StringArray,
        op: ScanPredicateOp,
        literal: String,
        like_matcher: Option<LikePredicateMatcher>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColumnAggregateOp {
    CountAll,
    Count,
    Sum,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnAggregateRequest {
    pub(crate) op: ColumnAggregateOp,
    pub(crate) column_index: Option<usize>,
    pub(crate) distinct: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryStorage {
    pub(crate) rows_by_table: HashMap<Oid, Vec<Vec<ScalarValue>>>,
    pub(crate) columnar_by_table: HashMap<Oid, ColumnarTable>,
    pub(crate) indexes_by_table: HashMap<(Oid, String), StoredIndex>,
}

impl InMemoryStorage {
    pub(crate) fn ensure_table(&mut self, table_oid: Oid) -> Result<(), String> {
        self.rows_by_table.entry(table_oid).or_default();
        let columnar = self.columnar_by_table.entry(table_oid).or_default();
        if columnar.schema.is_none() {
            let rows = self
                .rows_by_table
                .get(&table_oid)
                .cloned()
                .unwrap_or_default();
            let row_hint = rows.first().map(std::vec::Vec::as_slice);
            self.ensure_columnar_schema(table_oid, row_hint)?;
            if !rows.is_empty() {
                self.rebuild_columnar_table(table_oid)?;
            }
        }
        Ok(())
    }

    pub(crate) fn remove_table(&mut self, table_oid: Oid) {
        self.rows_by_table.remove(&table_oid);
        self.columnar_by_table.remove(&table_oid);
        self.drop_all_indexes_for_table(table_oid);
    }

    #[allow(dead_code)]
    pub(crate) fn scan_rows(
        &mut self,
        table_oid: Oid,
        offsets: Option<&[usize]>,
        projected_columns: Option<&[usize]>,
    ) -> Vec<Vec<ScalarValue>> {
        self.scan_rows_for_table(table_oid, offsets, &[], projected_columns)
            .unwrap_or_default()
    }

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
        self.rebuild_columnar_table(table_oid)?;
        self.rebuild_indexes_for_table(table_oid)
    }

    pub(crate) fn append_row(
        &mut self,
        table_oid: Oid,
        row: Vec<ScalarValue>,
    ) -> Result<usize, String> {
        self.ensure_table(table_oid)?;
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
            rows.push(row.clone());
        } else {
            self.rows_by_table.insert(table_oid, vec![row.clone()]);
        }

        if let Err(err) = self.append_row_to_columnar(table_oid, row) {
            if let Some(rows) = self.rows_by_table.get_mut(&table_oid) {
                let _ = rows.pop();
            }
            for (index_name, composite_key) in inserted_keys {
                if let Some(index) = self.index_mut_for_table(table_oid, &index_name) {
                    index.btree.delete(&composite_key, offset)?;
                }
            }
            return Err(err);
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
            return self.rebuild_columnar_table(table_oid);
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

        self.mark_deleted_in_columnar(table_oid, &sorted);

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
        self.rebuild_columnar_table(table_oid)
    }

    pub(crate) fn flush_pending(&mut self, table_oid: Oid) -> Result<(), String> {
        let pending_rows = {
            let Some(table) = self.columnar_by_table.get_mut(&table_oid) else {
                return Ok(());
            };
            if table.pending_rows.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut table.pending_rows)
        };

        self.ensure_columnar_schema(table_oid, pending_rows.first().map(std::vec::Vec::as_slice))?;
        let schema = self
            .columnar_by_table
            .get(&table_oid)
            .and_then(|table| table.schema.clone())
            .ok_or_else(|| format!("relation OID {table_oid} is missing columnar schema"))?;
        let batch = build_record_batch(&schema, &pending_rows)?;
        let deleted_rows = vec![false; batch.num_rows()];
        self.columnar_by_table
            .entry(table_oid)
            .or_default()
            .batches
            .push(ColumnarBatch {
                record_batch: batch,
                deleted_rows,
            });
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

    pub(crate) fn scan_rows_for_table(
        &mut self,
        table_oid: Oid,
        offsets: Option<&[usize]>,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
    ) -> Result<Vec<Vec<ScalarValue>>, String> {
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(Vec::new());
        };
        if table.batches.is_empty() {
            return Ok(Vec::new());
        }

        match offsets {
            Some(offsets) => {
                self.scan_rows_by_offsets(table, offsets, predicates, projected_columns)
            }
            None => self.scan_all_rows(table, predicates, projected_columns),
        }
    }

    pub(crate) fn count_rows_for_table(
        &mut self,
        table_oid: Oid,
        offsets: Option<&[usize]>,
        predicates: &[ScanPredicate],
    ) -> Result<usize, String> {
        let _span = profiling::span("storage_count_rows_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(0);
        };
        if table.batches.is_empty() {
            return Ok(0);
        }

        match offsets {
            Some(offsets) => self.count_rows_by_offsets(table, offsets, predicates),
            None if predicates.is_empty() => Ok(table
                .batches
                .iter()
                .map(|batch| {
                    batch.record_batch.num_rows().saturating_sub(
                        batch
                            .deleted_rows
                            .iter()
                            .filter(|deleted| **deleted)
                            .count(),
                    )
                })
                .sum()),
            None => self.count_all_rows(table, predicates),
        }
    }

    pub(crate) fn aggregate_columns_for_table(
        &mut self,
        table_oid: Oid,
        requests: &[ColumnAggregateRequest],
    ) -> Result<Option<Vec<ScalarValue>>, String> {
        let _span = profiling::span("storage_aggregate_columns_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(Some(
                requests.iter().map(column_aggregate_empty_result).collect(),
            ));
        };

        let mut states = requests
            .iter()
            .map(ColumnAggregateState::from_request)
            .collect::<Vec<_>>();
        for batch in &table.batches {
            for (request, state) in requests.iter().zip(states.iter_mut()) {
                if !apply_column_aggregate_request(batch, request, state)? {
                    return Ok(None);
                }
            }
        }

        Ok(Some(
            states
                .into_iter()
                .map(ColumnAggregateState::finalize)
                .collect(),
        ))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn group_aggregate_columns_for_table(
        &mut self,
        table_oid: Oid,
        group_column_indexes: &[usize],
        requests: &[ColumnAggregateRequest],
    ) -> Result<Option<Vec<(Vec<ScalarValue>, Vec<ScalarValue>)>>, String> {
        let _span = profiling::span("storage_group_aggregate_columns_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(Some(Vec::new()));
        };

        let mut group_indexes = HashMap::new();
        let mut groups = Vec::new();
        for batch in &table.batches {
            for row_idx in 0..batch.record_batch.num_rows() {
                if batch.deleted_rows.get(row_idx).copied().unwrap_or(false) {
                    continue;
                }

                let mut group_values = Vec::with_capacity(group_column_indexes.len());
                for &column_index in group_column_indexes {
                    let Some(column) = batch.record_batch.columns().get(column_index) else {
                        return Err(format!("column index {column_index} is out of bounds"));
                    };
                    group_values.push(arrow_value_to_scalar_value(column.as_ref(), row_idx));
                }
                let group_key = format!("{group_values:?}");
                let group_idx = if let Some(existing) = group_indexes.get(&group_key) {
                    *existing
                } else {
                    let idx = groups.len();
                    groups.push(GroupAggregateEntry::new(group_values.clone(), requests));
                    group_indexes.insert(group_key, idx);
                    idx
                };

                let entry = groups
                    .get_mut(group_idx)
                    .ok_or_else(|| "group accumulator is missing".to_string())?;
                for (request, state) in requests.iter().zip(entry.aggregate_states.iter_mut()) {
                    if !apply_group_aggregate_request(&batch.record_batch, row_idx, request, state)?
                    {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(Some(
            groups
                .into_iter()
                .map(|entry| {
                    (
                        entry.group_values,
                        entry
                            .aggregate_states
                            .into_iter()
                            .map(GroupAggregateState::finalize)
                            .collect(),
                    )
                })
                .collect(),
        ))
    }

    pub(crate) fn scan_columnar_for_table(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
    ) -> Result<ColumnBatch, String> {
        let _span = profiling::span("storage_scan_columnar_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(ColumnBatch::empty(Vec::new()));
        };
        let output_column_names = projected_column_names(&table.column_names, projected_columns);
        if table.batches.is_empty() {
            return Ok(ColumnBatch::empty(output_column_names));
        }

        let mut combined = ColumnBatch::empty(output_column_names);
        for batch in &table.batches {
            let batch_columns =
                if predicates.is_empty() && batch.deleted_rows.iter().all(|deleted| !*deleted) {
                    ColumnBatch::from_record_batch_projected(
                        &batch.record_batch,
                        &table.column_names,
                        projected_columns,
                    )
                } else {
                    let (filtered_batch, _) = filter_batch(batch, predicates, None)?;
                    ColumnBatch::from_record_batch_projected(
                        &filtered_batch,
                        &table.column_names,
                        projected_columns,
                    )
                };
            combined.append_batch(&batch_columns)?;
        }
        Ok(combined)
    }

    pub(crate) fn scan_batches_for_table(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
        callback: &mut dyn FnMut(ColumnBatch) -> Result<bool, String>,
    ) -> Result<(), String> {
        let _span = profiling::span("storage_scan_batches_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        for batch in &table.batches {
            let batch_columns =
                if predicates.is_empty() && batch.deleted_rows.iter().all(|deleted| !*deleted) {
                    ColumnBatch::from_record_batch_projected(
                        &batch.record_batch,
                        &table.column_names,
                        projected_columns,
                    )
                } else {
                    let (filtered_batch, _) = filter_batch(batch, predicates, None)?;
                    ColumnBatch::from_record_batch_projected(
                        &filtered_batch,
                        &table.column_names,
                        projected_columns,
                    )
                };
            if !callback(batch_columns)? {
                break;
            }
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn scan_batches_for_table_with_offsets(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
        callback: &mut dyn FnMut(ColumnBatch, Vec<usize>) -> Result<bool, String>,
    ) -> Result<(), String> {
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        let mut batch_start = 0usize;
        for batch in &table.batches {
            let batch_len = batch.record_batch.num_rows();
            let (batch_columns, global_offsets) = if predicates.is_empty()
                && batch.deleted_rows.iter().all(|deleted| !*deleted)
            {
                (
                    ColumnBatch::from_record_batch_projected(
                        &batch.record_batch,
                        &table.column_names,
                        projected_columns,
                    ),
                    (batch_start..batch_start + batch_len).collect::<Vec<_>>(),
                )
            } else {
                let (filtered_batch, surviving_offsets) = filter_batch(batch, predicates, None)?;
                (
                    ColumnBatch::from_record_batch_projected(
                        &filtered_batch,
                        &table.column_names,
                        projected_columns,
                    ),
                    surviving_offsets
                        .into_iter()
                        .map(|local_offset| batch_start + local_offset)
                        .collect::<Vec<_>>(),
                )
            };
            if !callback(batch_columns, global_offsets)? {
                break;
            }
            batch_start += batch_len;
        }
        Ok(())
    }

    pub(crate) fn scan_selected_batches_for_table(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
        callback: &mut dyn FnMut(ColumnBatch, Vec<usize>) -> Result<bool, String>,
    ) -> Result<(), String> {
        let _span = profiling::span("storage_scan_selected_batches_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        for batch in &table.batches {
            let selected_rows = matching_row_offsets_in_batch(batch, predicates, None)?;
            if selected_rows.is_empty() {
                continue;
            }
            let batch_columns = ColumnBatch::from_record_batch_projected_selected(
                &batch.record_batch,
                &table.column_names,
                projected_columns,
                &selected_rows,
            );
            let dense_rows = (0..selected_rows.len()).collect::<Vec<_>>();
            if !callback(batch_columns, dense_rows)? {
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn scan_selected_batches_for_table_with_offsets(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
        callback: &mut dyn FnMut(ColumnBatch, Vec<usize>, Vec<usize>) -> Result<bool, String>,
    ) -> Result<(), String> {
        let _span = profiling::span("storage_scan_selected_batches_for_table_with_offsets");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        let mut batch_start = 0usize;
        for batch in &table.batches {
            let batch_len = batch.record_batch.num_rows();
            let selected_rows = matching_row_offsets_in_batch(batch, predicates, None)?;
            if !selected_rows.is_empty() {
                let global_offsets = selected_rows
                    .iter()
                    .map(|local_offset| batch_start + local_offset)
                    .collect::<Vec<_>>();
                let batch_columns = ColumnBatch::from_record_batch_projected_selected(
                    &batch.record_batch,
                    &table.column_names,
                    projected_columns,
                    &selected_rows,
                );
                let dense_rows = (0..selected_rows.len()).collect::<Vec<_>>();
                if !callback(batch_columns, dense_rows, global_offsets)? {
                    break;
                }
            }
            batch_start += batch_len;
        }
        Ok(())
    }

    pub(crate) fn scan_selected_offsets_for_table(
        &mut self,
        table_oid: Oid,
        predicates: &[ScanPredicate],
        callback: &mut SelectedOffsetsBatchCallback<'_>,
    ) -> Result<(), String> {
        let _span = profiling::span("storage_scan_selected_offsets_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        let mut batch_start = 0usize;
        for batch in &table.batches {
            let selected_rows = matching_row_offsets_in_batch(batch, predicates, None)?;
            if !selected_rows.is_empty() {
                let global_offsets = selected_rows
                    .iter()
                    .map(|local_offset| batch_start + local_offset)
                    .collect::<Vec<_>>();
                if !callback(&batch.record_batch, selected_rows, global_offsets)? {
                    break;
                }
            }
            batch_start += batch.record_batch.num_rows();
        }
        Ok(())
    }

    pub(crate) fn scan_record_batches_for_table(
        &mut self,
        table_oid: Oid,
        callback: &mut RecordBatchScanCallback<'_>,
    ) -> Result<(), String> {
        let _span = profiling::span("storage_scan_record_batches_for_table");
        self.ensure_table(table_oid)?;
        self.flush_pending(table_oid)?;
        let Some(table) = self.columnar_by_table.get(&table_oid) else {
            return Ok(());
        };
        if table.batches.is_empty() {
            return Ok(());
        }

        let mut batch_start = 0usize;
        for batch in &table.batches {
            if !callback(&batch.record_batch, &batch.deleted_rows, batch_start)? {
                break;
            }
            batch_start += batch.record_batch.num_rows();
        }
        Ok(())
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

    fn append_row_to_columnar(
        &mut self,
        table_oid: Oid,
        row: Vec<ScalarValue>,
    ) -> Result<(), String> {
        self.ensure_columnar_schema(table_oid, Some(&row))?;
        let table = self.columnar_by_table.entry(table_oid).or_default();
        table.pending_rows.push(row);
        if table.pending_rows.len() >= COLUMNAR_BATCH_SIZE {
            self.flush_pending(table_oid)?;
        }
        Ok(())
    }

    fn rebuild_columnar_table(&mut self, table_oid: Oid) -> Result<(), String> {
        let rows = self
            .rows_by_table
            .get(&table_oid)
            .cloned()
            .unwrap_or_default();
        self.ensure_columnar_schema(table_oid, rows.first().map(std::vec::Vec::as_slice))?;
        let table = self.columnar_by_table.entry(table_oid).or_default();
        table.batches.clear();
        table.pending_rows.clear();
        for chunk in rows.chunks(COLUMNAR_BATCH_SIZE) {
            let schema = table
                .schema
                .clone()
                .ok_or_else(|| format!("relation OID {table_oid} is missing columnar schema"))?;
            let batch = build_record_batch(&schema, chunk)?;
            table.batches.push(ColumnarBatch {
                record_batch: batch,
                deleted_rows: vec![false; chunk.len()],
            });
        }
        Ok(())
    }

    fn ensure_columnar_schema(
        &mut self,
        table_oid: Oid,
        first_row: Option<&[ScalarValue]>,
    ) -> Result<(), String> {
        let table = self.columnar_by_table.entry(table_oid).or_default();
        if let Some((column_names, schema)) = lookup_catalog_schema(table_oid, first_row)? {
            table.column_names = column_names;
            table.schema = Some(Arc::new(schema));
            return Ok(());
        }

        if table.schema.is_some() {
            return Ok(());
        }

        if let Some(row) = first_row {
            let columns = row
                .iter()
                .enumerate()
                .map(|(idx, value)| (format!("column_{idx}"), value.clone()))
                .collect::<Vec<_>>();
            let schema = scalar_values_schema(&columns);
            table.column_names = columns.into_iter().map(|(name, _)| name).collect();
            table.schema = Some(Arc::new(schema));
        }
        Ok(())
    }

    fn mark_deleted_in_columnar(&mut self, table_oid: Oid, offsets: &[usize]) {
        let Some(table) = self.columnar_by_table.get_mut(&table_oid) else {
            return;
        };
        let deleted: HashSet<usize> = offsets.iter().copied().collect();
        let mut current_offset = 0usize;
        for batch in &mut table.batches {
            for (row_idx, deleted_flag) in batch.deleted_rows.iter_mut().enumerate() {
                let global_offset = current_offset + row_idx;
                if deleted.contains(&global_offset) {
                    *deleted_flag = true;
                }
            }
            current_offset += batch.record_batch.num_rows();
        }
    }

    fn scan_all_rows(
        &self,
        table: &ColumnarTable,
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
    ) -> Result<Vec<Vec<ScalarValue>>, String> {
        let mut rows = Vec::new();
        for batch in &table.batches {
            let (filtered_batch, _) = filter_batch(batch, predicates, None)?;
            rows.extend(record_batch_to_rows(&filtered_batch, projected_columns));
        }
        Ok(rows)
    }

    fn scan_rows_by_offsets(
        &self,
        table: &ColumnarTable,
        offsets: &[usize],
        predicates: &[ScanPredicate],
        projected_columns: Option<&[usize]>,
    ) -> Result<Vec<Vec<ScalarValue>>, String> {
        let projected = projected_columns
            .map(<[usize]>::to_vec)
            .unwrap_or_else(|| (0..table.column_names.len()).collect());
        let mut rows_by_request = vec![None; offsets.len()];
        let mut batch_start = 0usize;

        for batch in &table.batches {
            let batch_len = batch.record_batch.num_rows();
            let batch_end = batch_start + batch_len;
            let relevant = offsets
                .iter()
                .enumerate()
                .filter_map(|(request_idx, offset)| {
                    if *offset >= batch_start && *offset < batch_end {
                        Some((request_idx, *offset - batch_start))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if relevant.is_empty() {
                batch_start = batch_end;
                continue;
            }

            for (request_idx, local_offset) in relevant {
                if batch
                    .deleted_rows
                    .get(local_offset)
                    .copied()
                    .unwrap_or(false)
                {
                    continue;
                }
                if !predicates.is_empty()
                    && !record_batch_row_matches_predicates(
                        &batch.record_batch,
                        local_offset,
                        predicates,
                    )?
                {
                    continue;
                }

                rows_by_request[request_idx] = Some(record_batch_row_to_values(
                    &batch.record_batch,
                    local_offset,
                    &projected,
                ));
            }
            batch_start = batch_end;
        }

        Ok(rows_by_request.into_iter().flatten().collect())
    }

    fn count_all_rows(
        &self,
        table: &ColumnarTable,
        predicates: &[ScanPredicate],
    ) -> Result<usize, String> {
        let mut count = 0usize;
        for batch in &table.batches {
            count += count_matching_rows_in_batch(batch, predicates, None)?;
        }
        Ok(count)
    }

    fn count_rows_by_offsets(
        &self,
        table: &ColumnarTable,
        offsets: &[usize],
        predicates: &[ScanPredicate],
    ) -> Result<usize, String> {
        let requested_offsets = offsets.iter().copied().collect::<HashSet<_>>();
        let mut count = 0usize;
        let mut batch_start = 0usize;

        for batch in &table.batches {
            let batch_len = batch.record_batch.num_rows();
            let batch_end = batch_start + batch_len;
            let relevant = requested_offsets
                .iter()
                .copied()
                .filter(|offset| *offset >= batch_start && *offset < batch_end)
                .collect::<Vec<_>>();
            if relevant.is_empty() {
                batch_start = batch_end;
                continue;
            }

            let mut selected_rows = vec![false; batch_len];
            for offset in &relevant {
                selected_rows[*offset - batch_start] = true;
            }
            count += count_matching_rows_in_batch(batch, predicates, Some(&selected_rows))?;
            batch_start = batch_end;
        }

        Ok(count)
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

#[derive(Debug, Clone, Copy, Default)]
struct NumericAggregateState {
    int_sum: i64,
    float_sum: f64,
    non_null_count: i64,
    saw_float: bool,
}

impl NumericAggregateState {
    fn add_int(&mut self, value: i64) {
        self.int_sum = self.int_sum.wrapping_add(value);
        self.non_null_count += 1;
    }

    fn add_float(&mut self, value: f64) {
        self.float_sum += value;
        self.non_null_count += 1;
        self.saw_float = true;
    }

    fn has_values(&self) -> bool {
        self.non_null_count > 0
    }
}

enum ColumnAggregateState {
    CountAll(i64),
    Count(i64),
    Sum(NumericAggregateState),
    Avg(NumericAggregateState),
}

impl ColumnAggregateState {
    fn from_request(request: &ColumnAggregateRequest) -> Self {
        match request.op {
            ColumnAggregateOp::CountAll => Self::CountAll(0),
            ColumnAggregateOp::Count => Self::Count(0),
            ColumnAggregateOp::Sum => Self::Sum(NumericAggregateState::default()),
            ColumnAggregateOp::Avg => Self::Avg(NumericAggregateState::default()),
        }
    }

    fn finalize(self) -> ScalarValue {
        match self {
            Self::CountAll(count) | Self::Count(count) => ScalarValue::Int(count),
            Self::Sum(state) => {
                if !state.has_values() {
                    ScalarValue::Null
                } else if state.saw_float {
                    ScalarValue::Float(state.float_sum + state.int_sum as f64)
                } else {
                    ScalarValue::Int(state.int_sum)
                }
            }
            Self::Avg(state) => {
                if !state.has_values() {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(
                        (state.float_sum + state.int_sum as f64) / state.non_null_count as f64,
                    )
                }
            }
        }
    }
}

fn column_aggregate_empty_result(request: &ColumnAggregateRequest) -> ScalarValue {
    match request.op {
        ColumnAggregateOp::CountAll | ColumnAggregateOp::Count => ScalarValue::Int(0),
        ColumnAggregateOp::Sum | ColumnAggregateOp::Avg => ScalarValue::Null,
    }
}

fn apply_column_aggregate_request(
    batch: &ColumnarBatch,
    request: &ColumnAggregateRequest,
    state: &mut ColumnAggregateState,
) -> Result<bool, String> {
    match (request.op, state) {
        (ColumnAggregateOp::CountAll, ColumnAggregateState::CountAll(count)) => {
            *count += visible_row_count(batch);
            Ok(true)
        }
        (ColumnAggregateOp::Count, ColumnAggregateState::Count(count)) => {
            if request.distinct {
                return Ok(false);
            }
            let Some(column_index) = request.column_index else {
                return Ok(false);
            };
            let Some(column) = batch.record_batch.columns().get(column_index) else {
                return Err(format!("column index {column_index} is out of bounds"));
            };
            *count += count_non_null_values(column.as_ref(), &batch.deleted_rows);
            Ok(true)
        }
        (ColumnAggregateOp::Sum, ColumnAggregateState::Sum(numeric_state))
        | (ColumnAggregateOp::Avg, ColumnAggregateState::Avg(numeric_state)) => {
            if request.distinct {
                return Ok(false);
            }
            let Some(column_index) = request.column_index else {
                return Ok(false);
            };
            let Some(column) = batch.record_batch.columns().get(column_index) else {
                return Err(format!("column index {column_index} is out of bounds"));
            };
            accumulate_numeric_values(numeric_state, column.as_ref(), &batch.deleted_rows)
        }
        _ => Ok(false),
    }
}

fn visible_row_count(batch: &ColumnarBatch) -> i64 {
    (batch.record_batch.num_rows()
        - batch
            .deleted_rows
            .iter()
            .filter(|deleted| **deleted)
            .count()) as i64
}

fn count_non_null_values(array: &dyn Array, deleted_rows: &[bool]) -> i64 {
    if array.null_count() == 0 && deleted_rows.iter().all(|deleted| !deleted) {
        return array.len() as i64;
    }

    let mut count = 0i64;
    for row_idx in 0..array.len() {
        if deleted_rows.get(row_idx).copied().unwrap_or(false) || array.is_null(row_idx) {
            continue;
        }
        count += 1;
    }
    count
}

fn accumulate_numeric_values(
    state: &mut NumericAggregateState,
    array: &dyn Array,
    deleted_rows: &[bool],
) -> Result<bool, String> {
    if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
        for row_idx in 0..int_array.len() {
            if deleted_rows.get(row_idx).copied().unwrap_or(false) || int_array.is_null(row_idx) {
                continue;
            }
            state.add_int(int_array.value(row_idx));
        }
        return Ok(true);
    }
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        for row_idx in 0..float_array.len() {
            if deleted_rows.get(row_idx).copied().unwrap_or(false) || float_array.is_null(row_idx) {
                continue;
            }
            state.add_float(float_array.value(row_idx));
        }
        return Ok(true);
    }
    Ok(false)
}

struct GroupAggregateEntry {
    group_values: Vec<ScalarValue>,
    aggregate_states: Vec<GroupAggregateState>,
}

impl GroupAggregateEntry {
    fn new(group_values: Vec<ScalarValue>, requests: &[ColumnAggregateRequest]) -> Self {
        Self {
            group_values,
            aggregate_states: requests
                .iter()
                .map(GroupAggregateState::from_request)
                .collect(),
        }
    }
}

enum GroupAggregateState {
    CountAll(i64),
    Count(i64),
    CountDistinct(HashSet<String>),
    Sum(NumericAggregateState),
    Avg(NumericAggregateState),
}

impl GroupAggregateState {
    fn from_request(request: &ColumnAggregateRequest) -> Self {
        match (request.op, request.distinct) {
            (ColumnAggregateOp::CountAll, _) => Self::CountAll(0),
            (ColumnAggregateOp::Count, false) => Self::Count(0),
            (ColumnAggregateOp::Count, true) => Self::CountDistinct(HashSet::new()),
            (ColumnAggregateOp::Sum, false) => Self::Sum(NumericAggregateState::default()),
            (ColumnAggregateOp::Avg, false) => Self::Avg(NumericAggregateState::default()),
            (ColumnAggregateOp::Sum | ColumnAggregateOp::Avg, true) => {
                Self::CountDistinct(HashSet::new())
            }
        }
    }

    fn finalize(self) -> ScalarValue {
        match self {
            Self::CountAll(count) | Self::Count(count) => ScalarValue::Int(count),
            Self::CountDistinct(values) => ScalarValue::Int(values.len() as i64),
            Self::Sum(state) => ColumnAggregateState::Sum(state).finalize(),
            Self::Avg(state) => ColumnAggregateState::Avg(state).finalize(),
        }
    }
}

fn apply_group_aggregate_request(
    batch: &RecordBatch,
    row_idx: usize,
    request: &ColumnAggregateRequest,
    state: &mut GroupAggregateState,
) -> Result<bool, String> {
    match (request.op, request.distinct, state) {
        (ColumnAggregateOp::CountAll, false, GroupAggregateState::CountAll(count)) => {
            *count += 1;
            Ok(true)
        }
        (ColumnAggregateOp::Count, false, GroupAggregateState::Count(count)) => {
            let Some(column_index) = request.column_index else {
                return Ok(false);
            };
            let Some(column) = batch.columns().get(column_index) else {
                return Err(format!("column index {column_index} is out of bounds"));
            };
            if !column.is_null(row_idx) {
                *count += 1;
            }
            Ok(true)
        }
        (ColumnAggregateOp::Count, true, GroupAggregateState::CountDistinct(values)) => {
            let Some(column_index) = request.column_index else {
                return Ok(false);
            };
            let Some(column) = batch.columns().get(column_index) else {
                return Err(format!("column index {column_index} is out of bounds"));
            };
            if !column.is_null(row_idx) {
                let value = arrow_value_to_scalar_value(column.as_ref(), row_idx);
                values.insert(format!("{value:?}"));
            }
            Ok(true)
        }
        (ColumnAggregateOp::Sum, false, GroupAggregateState::Sum(numeric_state))
        | (ColumnAggregateOp::Avg, false, GroupAggregateState::Avg(numeric_state)) => {
            let Some(column_index) = request.column_index else {
                return Ok(false);
            };
            let Some(column) = batch.columns().get(column_index) else {
                return Err(format!("column index {column_index} is out of bounds"));
            };
            accumulate_numeric_value_at_row(numeric_state, column.as_ref(), row_idx)
        }
        _ => Ok(false),
    }
}

fn accumulate_numeric_value_at_row(
    state: &mut NumericAggregateState,
    array: &dyn Array,
    row_idx: usize,
) -> Result<bool, String> {
    if array.is_null(row_idx) {
        return Ok(true);
    }
    if let Some(int_array) = array.as_any().downcast_ref::<Int64Array>() {
        state.add_int(int_array.value(row_idx));
        return Ok(true);
    }
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        state.add_float(float_array.value(row_idx));
        return Ok(true);
    }
    Ok(false)
}

fn lookup_catalog_schema(
    table_oid: Oid,
    first_row: Option<&[ScalarValue]>,
) -> Result<Option<(Vec<String>, Schema)>, String> {
    let maybe_table = with_catalog_read(|catalog| {
        catalog
            .schemas()
            .flat_map(crate::catalog::schema::Schema::tables)
            .find(|table| table.oid() == table_oid)
            .cloned()
    });
    let Some(table) = maybe_table else {
        return Ok(None);
    };

    let fields = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let data_type = data_type_for_column(
                column.type_signature(),
                first_row.and_then(|row| row.get(idx)),
            );
            Field::new(column.name(), data_type, true)
        })
        .collect::<Vec<_>>();
    let column_names = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    Ok(Some((column_names, Schema::new(fields))))
}

fn data_type_for_column(type_signature: TypeSignature, _sample: Option<&ScalarValue>) -> DataType {
    match type_signature {
        TypeSignature::Bool => DataType::Boolean,
        TypeSignature::Int8 => DataType::Int64,
        TypeSignature::Float8 => DataType::Float64,
        TypeSignature::Numeric => DataType::Utf8,
        TypeSignature::Text => DataType::Utf8,
        TypeSignature::Date => DataType::Date32,
        TypeSignature::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        TypeSignature::Vector(Some(len)) => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            len as i32,
        ),
        TypeSignature::Vector(None) => DataType::Utf8,
    }
}

fn build_record_batch(
    schema: &Arc<Schema>,
    rows: &[Vec<ScalarValue>],
) -> Result<RecordBatch, String> {
    if schema.fields().is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (column_idx, field) in schema.fields().iter().enumerate() {
        let mut builder = arrow::array::builder::make_builder(field.data_type(), rows.len());
        for row in rows {
            let value = row.get(column_idx).unwrap_or(&ScalarValue::Null);
            append_scalar_value_to_builder(value, builder.as_mut())?;
        }
        columns.push(builder.finish());
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|err| err.to_string())
}

fn filter_batch(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<(RecordBatch, Vec<usize>), String> {
    let _span = profiling::span("heap_filter_batch");
    let mask = build_filter_mask(batch, predicates, selected_rows)?;
    let surviving_offsets = mask
        .iter()
        .enumerate()
        .filter_map(|(row_idx, selected)| matches!(selected, Some(true)).then_some(row_idx))
        .collect::<Vec<_>>();
    let filtered =
        filter_record_batch(&batch.record_batch, &mask).map_err(|err| err.to_string())?;
    Ok((filtered, surviving_offsets))
}

fn matching_row_offsets_in_batch(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<Vec<usize>, String> {
    let _span = profiling::span("heap_matching_row_offsets_in_batch");
    if let Some(offsets) = try_matching_row_offsets_fast(batch, predicates, selected_rows)? {
        return Ok(offsets);
    }
    Ok(build_filter_mask(batch, predicates, selected_rows)?
        .iter()
        .enumerate()
        .filter_map(|(row_idx, selected)| matches!(selected, Some(true)).then_some(row_idx))
        .collect())
}

fn count_matching_rows_in_batch(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<usize, String> {
    if let Some(offsets) = try_matching_row_offsets_fast(batch, predicates, selected_rows)? {
        return Ok(offsets.len());
    }
    Ok(build_filter_mask(batch, predicates, selected_rows)?
        .iter()
        .filter(|selected| matches!(selected, Some(true)))
        .count())
}

fn try_matching_row_offsets_fast(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<Option<Vec<usize>>, String> {
    let _span = profiling::span("heap_matching_row_offsets_fast");
    let Some(compiled_predicates) = compile_scan_predicates(batch, predicates)? else {
        return Ok(None);
    };

    let mut offsets = Vec::new();
    for row_idx in 0..batch.record_batch.num_rows() {
        let selected = selected_rows.is_none_or(|rows| rows.get(row_idx).copied().unwrap_or(false));
        let deleted = batch.deleted_rows.get(row_idx).copied().unwrap_or(false);
        if !selected || deleted {
            continue;
        }
        if compiled_predicates
            .iter()
            .all(|predicate| predicate.matches(row_idx))
        {
            offsets.push(row_idx);
        }
    }
    Ok(Some(offsets))
}

fn compile_scan_predicates<'a>(
    batch: &'a ColumnarBatch,
    predicates: &[ScanPredicate],
) -> Result<Option<Vec<CompiledScanPredicate<'a>>>, String> {
    let mut compiled = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let Some(column) = batch.record_batch.columns().get(predicate.column_index) else {
            return Err(format!(
                "row does not have predicate column offset {}",
                predicate.column_index
            ));
        };
        let compiled_predicate = if let (Some(values), ScalarValue::Int(literal)) = (
            column.as_any().downcast_ref::<Int64Array>(),
            &predicate.value,
        ) {
            CompiledScanPredicate::Int64 {
                values,
                op: predicate.op,
                literal: *literal,
            }
        } else if let (Some(values), ScalarValue::Float(literal)) = (
            column.as_any().downcast_ref::<Float64Array>(),
            &predicate.value,
        ) {
            CompiledScanPredicate::Float64 {
                values,
                op: predicate.op,
                literal: *literal,
            }
        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            let literal = match &predicate.value {
                ScalarValue::Text(text) => text.clone(),
                other => other.render(),
            };
            let like_matcher = match predicate.op {
                ScanPredicateOp::Like | ScanPredicateOp::NotLike => Some(
                    LikePredicateMatcher::new(literal.clone(), predicate.escape, false),
                ),
                ScanPredicateOp::ILike | ScanPredicateOp::NotILike => Some(
                    LikePredicateMatcher::new(literal.clone(), predicate.escape, true),
                ),
                _ => None,
            };
            CompiledScanPredicate::Text {
                values,
                op: predicate.op,
                literal,
                like_matcher,
            }
        } else {
            return Ok(None);
        };
        compiled.push(compiled_predicate);
    }
    Ok(Some(compiled))
}

impl CompiledScanPredicate<'_> {
    fn matches(&self, row_idx: usize) -> bool {
        match self {
            Self::Int64 {
                values,
                op,
                literal,
            } => {
                if values.is_null(row_idx) {
                    return false;
                }
                compare_ordering(values.value(row_idx).cmp(literal), *op)
            }
            Self::Float64 {
                values,
                op,
                literal,
            } => {
                if values.is_null(row_idx) {
                    return false;
                }
                let value = values.value(row_idx);
                let ordering = value.partial_cmp(literal).unwrap_or(Ordering::Equal);
                compare_ordering(ordering, *op)
            }
            Self::Text {
                values,
                op,
                literal,
                like_matcher,
            } => {
                if values.is_null(row_idx) {
                    return false;
                }
                let value = values.value(row_idx);
                match op {
                    ScanPredicateOp::Eq => value == literal,
                    ScanPredicateOp::NotEq => value != literal,
                    ScanPredicateOp::Lt => value < literal.as_str(),
                    ScanPredicateOp::Lte => value <= literal.as_str(),
                    ScanPredicateOp::Gt => value > literal.as_str(),
                    ScanPredicateOp::Gte => value >= literal.as_str(),
                    ScanPredicateOp::Like | ScanPredicateOp::ILike => like_matcher
                        .as_ref()
                        .is_some_and(|matcher| matcher.matches(value)),
                    ScanPredicateOp::NotLike | ScanPredicateOp::NotILike => like_matcher
                        .as_ref()
                        .is_some_and(|matcher| !matcher.matches(value)),
                }
            }
        }
    }
}

fn compare_ordering(ordering: Ordering, op: ScanPredicateOp) -> bool {
    match op {
        ScanPredicateOp::Eq => ordering == Ordering::Equal,
        ScanPredicateOp::NotEq => ordering != Ordering::Equal,
        ScanPredicateOp::Lt => ordering == Ordering::Less,
        ScanPredicateOp::Lte => matches!(ordering, Ordering::Less | Ordering::Equal),
        ScanPredicateOp::Gt => ordering == Ordering::Greater,
        ScanPredicateOp::Gte => matches!(ordering, Ordering::Greater | Ordering::Equal),
        ScanPredicateOp::Like
        | ScanPredicateOp::NotLike
        | ScanPredicateOp::ILike
        | ScanPredicateOp::NotILike => false,
    }
}

fn classify_like_pattern(
    pattern: &str,
    escape: Option<char>,
    case_insensitive: bool,
) -> Option<LikePredicateMatcher> {
    if pattern.contains('_') || escape.is_some() {
        return None;
    }

    let normalized = if case_insensitive {
        pattern.to_ascii_lowercase()
    } else {
        pattern.to_string()
    };

    if let Some(literal) = normalized
        .strip_prefix('%')
        .and_then(|value| value.strip_suffix('%'))
        && !literal.contains('%')
    {
        return Some(LikePredicateMatcher::Contains(literal.to_string()));
    }
    if let Some(literal) = normalized.strip_prefix('%')
        && !literal.contains('%')
    {
        return Some(LikePredicateMatcher::Suffix(literal.to_string()));
    }
    if let Some(literal) = normalized.strip_suffix('%')
        && !literal.contains('%')
    {
        return Some(LikePredicateMatcher::Prefix(literal.to_string()));
    }
    if !normalized.contains('%') {
        return Some(LikePredicateMatcher::Exact(normalized));
    }
    None
}

fn build_filter_mask(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<BooleanArray, String> {
    let _span = profiling::span("heap_build_filter_mask");
    if let Some(mask) = try_build_arrow_filter_mask(batch, predicates, selected_rows)? {
        return Ok(mask);
    }
    build_filter_mask_row(batch, predicates, selected_rows)
}

fn try_build_arrow_filter_mask(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<Option<BooleanArray>, String> {
    let _span = profiling::span("heap_try_build_arrow_filter_mask");
    let mut mask = visibility_mask(batch, selected_rows);
    for predicate in predicates {
        let Some(column) = batch.record_batch.columns().get(predicate.column_index) else {
            return Err(format!(
                "row does not have predicate column offset {}",
                predicate.column_index
            ));
        };
        let Some(predicate_mask) = try_eval_arrow_scan_predicate(column.as_ref(), predicate)?
        else {
            return Ok(None);
        };
        mask = and_kleene(&mask, &predicate_mask).map_err(|err| err.to_string())?;
    }
    Ok(Some(mask))
}

fn build_filter_mask_row(
    batch: &ColumnarBatch,
    predicates: &[ScanPredicate],
    selected_rows: Option<&[bool]>,
) -> Result<BooleanArray, String> {
    let _span = profiling::span("heap_build_filter_mask_row");
    let row_count = batch.record_batch.num_rows();
    let values = (0..row_count)
        .map(|row_idx| {
            let selected =
                selected_rows.is_none_or(|rows| rows.get(row_idx).copied().unwrap_or(false));
            let deleted = batch.deleted_rows.get(row_idx).copied().unwrap_or(false);
            if !selected || deleted {
                return Ok(Some(false));
            }
            Ok(Some(record_batch_row_matches_predicates(
                &batch.record_batch,
                row_idx,
                predicates,
            )?))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(BooleanArray::from(values))
}

fn visibility_mask(batch: &ColumnarBatch, selected_rows: Option<&[bool]>) -> BooleanArray {
    (0..batch.record_batch.num_rows())
        .map(|row_idx| {
            let selected =
                selected_rows.is_none_or(|rows| rows.get(row_idx).copied().unwrap_or(false));
            let deleted = batch.deleted_rows.get(row_idx).copied().unwrap_or(false);
            Some(selected && !deleted)
        })
        .collect::<BooleanArray>()
}

fn try_eval_arrow_scan_predicate(
    column: &dyn Array,
    predicate: &ScanPredicate,
) -> Result<Option<BooleanArray>, String> {
    if let (Some(values), ScalarValue::Int(literal)) = (
        column.as_any().downcast_ref::<Int64Array>(),
        &predicate.value,
    ) {
        let scalar = Int64Array::new_scalar(*literal);
        return Ok(eval_arrow_scan_comparison(values, &scalar, predicate));
    }
    if let (Some(values), ScalarValue::Float(literal)) = (
        column.as_any().downcast_ref::<Float64Array>(),
        &predicate.value,
    ) {
        let scalar = Float64Array::new_scalar(*literal);
        return Ok(eval_arrow_scan_comparison(values, &scalar, predicate));
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        if predicate.escape.is_some()
            && matches!(
                predicate.op,
                ScanPredicateOp::Like
                    | ScanPredicateOp::NotLike
                    | ScanPredicateOp::ILike
                    | ScanPredicateOp::NotILike
            )
        {
            return Ok(None);
        }
        let literal = match &predicate.value {
            ScalarValue::Text(text) => text.clone(),
            other => other.render(),
        };
        let scalar = StringArray::new_scalar(literal.as_str());
        let mask = match predicate.op {
            ScanPredicateOp::Eq => cmp::eq(values, &scalar).ok(),
            ScanPredicateOp::NotEq => cmp::neq(values, &scalar).ok(),
            ScanPredicateOp::Lt => cmp::lt(values, &scalar).ok(),
            ScanPredicateOp::Lte => cmp::lt_eq(values, &scalar).ok(),
            ScanPredicateOp::Gt => cmp::gt(values, &scalar).ok(),
            ScanPredicateOp::Gte => cmp::gt_eq(values, &scalar).ok(),
            ScanPredicateOp::Like => like(values, &scalar).ok(),
            ScanPredicateOp::NotLike => nlike(values, &scalar).ok(),
            ScanPredicateOp::ILike => ilike(values, &scalar).ok(),
            ScanPredicateOp::NotILike => nilike(values, &scalar).ok(),
        };
        return Ok(mask);
    }
    Ok(None)
}

fn eval_arrow_scan_comparison(
    values: &dyn arrow::array::Datum,
    scalar: &dyn arrow::array::Datum,
    predicate: &ScanPredicate,
) -> Option<BooleanArray> {
    match predicate.op {
        ScanPredicateOp::Eq => cmp::eq(values, scalar).ok(),
        ScanPredicateOp::NotEq => cmp::neq(values, scalar).ok(),
        ScanPredicateOp::Lt => cmp::lt(values, scalar).ok(),
        ScanPredicateOp::Lte => cmp::lt_eq(values, scalar).ok(),
        ScanPredicateOp::Gt => cmp::gt(values, scalar).ok(),
        ScanPredicateOp::Gte => cmp::gt_eq(values, scalar).ok(),
        ScanPredicateOp::Like
        | ScanPredicateOp::NotLike
        | ScanPredicateOp::ILike
        | ScanPredicateOp::NotILike => None,
    }
}

fn record_batch_to_rows(
    batch: &RecordBatch,
    projected_columns: Option<&[usize]>,
) -> Vec<Vec<ScalarValue>> {
    let _span = profiling::span("heap_record_batch_to_rows");
    let projected = projected_columns
        .map(<[usize]>::to_vec)
        .unwrap_or_else(|| (0..batch.num_columns()).collect());
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::with_capacity(projected.len());
        for column_idx in &projected {
            if let Some(column) = batch.columns().get(*column_idx) {
                row.push(arrow_value_to_scalar_value(column.as_ref(), row_idx));
            }
        }
        rows.push(row);
    }
    rows
}

fn record_batch_row_to_values(
    batch: &RecordBatch,
    row_idx: usize,
    projected_columns: &[usize],
) -> Vec<ScalarValue> {
    let _span = profiling::span("heap_record_batch_row_to_values");
    let mut row = Vec::with_capacity(projected_columns.len());
    for column_idx in projected_columns {
        if let Some(column) = batch.columns().get(*column_idx) {
            row.push(arrow_value_to_scalar_value(column.as_ref(), row_idx));
        }
    }
    row
}

fn projected_column_names(columns: &[String], projected_columns: Option<&[usize]>) -> Vec<String> {
    match projected_columns {
        Some(projected_columns) => projected_columns
            .iter()
            .filter_map(|idx| columns.get(*idx).cloned())
            .collect(),
        None => columns.to_vec(),
    }
}

fn record_batch_row_matches_predicates(
    batch: &RecordBatch,
    row_idx: usize,
    predicates: &[ScanPredicate],
) -> Result<bool, String> {
    for predicate in predicates {
        let Some(column) = batch.columns().get(predicate.column_index) else {
            return Err(format!(
                "row does not have predicate column offset {}",
                predicate.column_index
            ));
        };
        let value = arrow_value_to_scalar_value(column.as_ref(), row_idx);
        if !scan_predicate_matches(&value, predicate)? {
            return Ok(false);
        }
    }
    Ok(true)
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

fn scan_predicate_matches(left: &ScalarValue, predicate: &ScanPredicate) -> Result<bool, String> {
    if matches!(left, ScalarValue::Null) || matches!(predicate.value, ScalarValue::Null) {
        return Ok(false);
    }
    let ord = compare_values_for_predicate(left, &predicate.value).map_err(|err| err.message)?;
    Ok(match predicate.op {
        ScanPredicateOp::Eq => ord == Ordering::Equal,
        ScanPredicateOp::NotEq => ord != Ordering::Equal,
        ScanPredicateOp::Lt => ord == Ordering::Less,
        ScanPredicateOp::Lte => matches!(ord, Ordering::Less | Ordering::Equal),
        ScanPredicateOp::Gt => ord == Ordering::Greater,
        ScanPredicateOp::Gte => matches!(ord, Ordering::Greater | Ordering::Equal),
        ScanPredicateOp::Like
        | ScanPredicateOp::NotLike
        | ScanPredicateOp::ILike
        | ScanPredicateOp::NotILike => {
            let text_storage;
            let text = match left {
                ScalarValue::Text(value) => value.as_str(),
                other => {
                    text_storage = other.render();
                    text_storage.as_str()
                }
            };
            let pattern_storage;
            let pattern = match &predicate.value {
                ScalarValue::Text(value) => value.as_str(),
                other => {
                    pattern_storage = other.render();
                    pattern_storage.as_str()
                }
            };
            let case_insensitive = matches!(
                predicate.op,
                ScanPredicateOp::ILike | ScanPredicateOp::NotILike
            );
            let matched = if case_insensitive {
                like_matches(
                    &text.to_ascii_lowercase(),
                    &pattern.to_ascii_lowercase(),
                    predicate.escape,
                )
            } else {
                like_matches(text, pattern, predicate.escape)
            };
            match predicate.op {
                ScanPredicateOp::Like | ScanPredicateOp::ILike => matched,
                ScanPredicateOp::NotLike | ScanPredicateOp::NotILike => !matched,
                _ => unreachable!(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{COLUMNAR_BATCH_SIZE, InMemoryStorage, ScanPredicate, ScanPredicateOp};
    use crate::storage::tuple::ScalarValue;

    #[test]
    fn scan_rows_for_table_filters_without_cloning_non_matches() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                42,
                vec![
                    vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                    vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
                    vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
                ],
            )
            .expect("replace rows");

        let rows = storage
            .scan_rows_for_table(
                42,
                None,
                &[ScanPredicate {
                    column_index: 0,
                    op: ScanPredicateOp::Gt,
                    value: ScalarValue::Int(1),
                    escape: None,
                }],
                None,
            )
            .expect("scan should succeed");

        assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
                vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
            ]
        );
    }

    #[test]
    fn scan_rows_for_table_honors_offsets() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                42,
                vec![
                    vec![ScalarValue::Int(1)],
                    vec![ScalarValue::Int(2)],
                    vec![ScalarValue::Int(3)],
                ],
            )
            .expect("replace rows");

        let rows = storage
            .scan_rows_for_table(
                42,
                Some(&[2, 0, 2]),
                &[ScanPredicate {
                    column_index: 0,
                    op: ScanPredicateOp::NotEq,
                    value: ScalarValue::Int(1),
                    escape: None,
                }],
                None,
            )
            .expect("scan should succeed");

        assert_eq!(
            rows,
            vec![vec![ScalarValue::Int(3)], vec![ScalarValue::Int(3)]]
        );
    }

    #[test]
    fn scan_rows_for_table_honors_offsets_with_projection() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                42,
                vec![
                    vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                    vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
                    vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
                ],
            )
            .expect("replace rows");

        let rows = storage
            .scan_rows_for_table(42, Some(&[2, 1]), &[], Some(&[1]))
            .expect("scan should succeed");

        assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Text("c".to_string())],
                vec![ScalarValue::Text("b".to_string())],
            ]
        );
    }

    #[test]
    fn append_flushes_pending_rows_into_record_batches() {
        let mut storage = InMemoryStorage::default();
        storage.rows_by_table.insert(7, Vec::new());
        for idx in 0..=COLUMNAR_BATCH_SIZE {
            storage
                .append_row(7, vec![ScalarValue::Int(idx as i64)])
                .expect("append row");
        }

        let table = storage
            .columnar_by_table
            .get(&7)
            .expect("columnar table should exist");
        assert_eq!(table.batches.len(), 1);
        assert_eq!(table.pending_rows.len(), 1);
    }

    #[test]
    fn scan_batches_for_table_streams_and_can_stop_early() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                77,
                (0..=COLUMNAR_BATCH_SIZE as i64)
                    .map(|value| vec![ScalarValue::Int(value)])
                    .collect(),
            )
            .expect("replace rows");

        let mut seen_batches = 0usize;
        let mut seen_rows = 0usize;
        storage
            .scan_batches_for_table(77, &[], None, &mut |batch| {
                seen_batches += 1;
                seen_rows += batch.row_count;
                Ok(false)
            })
            .expect("streaming scan should succeed");

        assert_eq!(seen_batches, 1);
        assert_eq!(seen_rows, COLUMNAR_BATCH_SIZE);
    }

    #[test]
    fn scan_batches_for_table_with_offsets_reports_global_offsets() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                78,
                (0..=COLUMNAR_BATCH_SIZE as i64)
                    .map(|value| vec![ScalarValue::Int(value)])
                    .collect(),
            )
            .expect("replace rows");

        let mut seen = Vec::new();
        storage
            .scan_batches_for_table_with_offsets(78, &[], None, &mut |batch, offsets| {
                seen.push((batch.row_count, offsets));
                Ok(false)
            })
            .expect("streaming scan with offsets should succeed");

        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, COLUMNAR_BATCH_SIZE);
        assert_eq!(seen[0].1.first().copied(), Some(0));
        assert_eq!(seen[0].1.last().copied(), Some(COLUMNAR_BATCH_SIZE - 1));
    }

    #[test]
    fn scan_batches_for_table_applies_like_predicate_columnarly() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                79,
                vec![
                    vec![ScalarValue::Text("alpha".to_string())],
                    vec![ScalarValue::Text("beta".to_string())],
                    vec![ScalarValue::Text("alphabet".to_string())],
                ],
            )
            .expect("replace rows");

        let mut rows = Vec::new();
        storage
            .scan_batches_for_table(
                79,
                &[ScanPredicate {
                    column_index: 0,
                    op: ScanPredicateOp::Like,
                    value: ScalarValue::Text("%alpha%".to_string()),
                    escape: None,
                }],
                None,
                &mut |batch| {
                    rows.extend(batch.to_rows());
                    Ok(true)
                },
            )
            .expect("streaming scan should succeed");

        assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Text("alpha".to_string())],
                vec![ScalarValue::Text("alphabet".to_string())],
            ]
        );
    }

    #[test]
    fn scan_selected_batches_with_offsets_reports_original_positions() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                88,
                vec![
                    vec![ScalarValue::Int(1), ScalarValue::Text("a".to_string())],
                    vec![ScalarValue::Int(2), ScalarValue::Text("b".to_string())],
                    vec![ScalarValue::Int(3), ScalarValue::Text("c".to_string())],
                ],
            )
            .expect("replace rows");

        let mut seen = Vec::new();
        storage
            .scan_selected_batches_for_table_with_offsets(
                88,
                &[ScanPredicate {
                    column_index: 0,
                    op: ScanPredicateOp::Gt,
                    value: ScalarValue::Int(1),
                    escape: None,
                }],
                Some(&[1]),
                &mut |batch, selected_rows, offsets| {
                    let values = selected_rows
                        .iter()
                        .map(|row_idx| batch.columns[0].value_at(*row_idx))
                        .collect::<Vec<_>>();
                    seen.push((offsets, values));
                    Ok(true)
                },
            )
            .expect("selected scan should succeed");

        assert_eq!(
            seen,
            vec![(
                vec![1, 2],
                vec![
                    ScalarValue::Text("b".to_string()),
                    ScalarValue::Text("c".to_string()),
                ],
            )]
        );
    }

    #[test]
    fn scan_selected_batches_handles_like_and_not_like_fast_path() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                89,
                vec![
                    vec![
                        ScalarValue::Text("https://example.com/widget/a".to_string()),
                        ScalarValue::Text("keep".to_string()),
                    ],
                    vec![
                        ScalarValue::Text("https://example.com/widget.internal".to_string()),
                        ScalarValue::Text("drop".to_string()),
                    ],
                    vec![
                        ScalarValue::Text("https://example.com/widget/b".to_string()),
                        ScalarValue::Text("keep".to_string()),
                    ],
                ],
            )
            .expect("replace rows");

        let mut rows = Vec::new();
        storage
            .scan_selected_batches_for_table(
                89,
                &[
                    ScanPredicate {
                        column_index: 0,
                        op: ScanPredicateOp::Like,
                        value: ScalarValue::Text("%widget%".to_string()),
                        escape: None,
                    },
                    ScanPredicate {
                        column_index: 0,
                        op: ScanPredicateOp::NotLike,
                        value: ScalarValue::Text("%.internal%".to_string()),
                        escape: None,
                    },
                ],
                Some(&[1]),
                &mut |batch, selected_rows| {
                    rows.extend(
                        selected_rows
                            .into_iter()
                            .map(|row_idx| batch.columns[0].value_at(row_idx)),
                    );
                    Ok(true)
                },
            )
            .expect("selected batch scan should succeed");

        assert_eq!(
            rows,
            vec![
                ScalarValue::Text("keep".to_string()),
                ScalarValue::Text("keep".to_string()),
            ]
        );
    }

    #[test]
    fn scan_record_batches_for_table_reports_batch_start_offsets() {
        let mut storage = InMemoryStorage::default();
        let rows = (0..=COLUMNAR_BATCH_SIZE)
            .map(|idx| vec![ScalarValue::Int(idx as i64)])
            .collect::<Vec<_>>();
        storage
            .replace_rows_for_table(90, rows)
            .expect("replace rows");

        let mut starts = Vec::new();
        let mut lengths = Vec::new();
        storage
            .scan_record_batches_for_table(90, &mut |batch, deleted_rows, batch_start| {
                starts.push(batch_start);
                lengths.push((batch.num_rows(), deleted_rows.len()));
                Ok(true)
            })
            .expect("raw batch scan should succeed");

        assert_eq!(starts, vec![0, COLUMNAR_BATCH_SIZE]);
        assert_eq!(
            lengths,
            vec![(COLUMNAR_BATCH_SIZE, COLUMNAR_BATCH_SIZE), (1, 1)]
        );
    }

    #[test]
    fn delete_marks_rows_and_rebuilds_columnar_storage() {
        let mut storage = InMemoryStorage::default();
        storage
            .replace_rows_for_table(
                9,
                vec![
                    vec![ScalarValue::Int(1)],
                    vec![ScalarValue::Int(2)],
                    vec![ScalarValue::Int(3)],
                ],
            )
            .expect("replace rows");

        storage
            .delete_rows_by_offsets(9, &[1])
            .expect("delete should succeed");

        let rows = storage
            .scan_rows_for_table(9, None, &[], None)
            .expect("scan should succeed");
        assert_eq!(
            rows,
            vec![vec![ScalarValue::Int(1)], vec![ScalarValue::Int(3)]]
        );
    }
}

static GLOBAL_STORAGE: OnceLock<RwLock<InMemoryStorage>> = OnceLock::new();

fn global_storage() -> &'static RwLock<InMemoryStorage> {
    GLOBAL_STORAGE.get_or_init(|| RwLock::new(InMemoryStorage::default()))
}

pub(crate) fn with_storage_read<T>(f: impl FnOnce(&InMemoryStorage) -> T) -> T {
    let storage = match global_storage().read() {
        Ok(storage) => storage,
        Err(poisoned) => {
            debug_assert!(false, "global storage lock poisoned for read");
            poisoned.into_inner()
        }
    };
    f(&storage)
}

pub(crate) fn with_storage_write<T>(f: impl FnOnce(&mut InMemoryStorage) -> T) -> T {
    let mut storage = match global_storage().write() {
        Ok(storage) => storage,
        Err(poisoned) => {
            debug_assert!(false, "global storage lock poisoned for write");
            poisoned.into_inner()
        }
    };
    f(&mut storage)
}
