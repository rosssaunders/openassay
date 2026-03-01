use std::collections::HashSet;
use std::sync::{OnceLock, RwLock};

use crate::catalog::{SearchPath, TableKind, with_catalog_read};
use crate::commands::sequence::with_sequences_write;
use crate::parser::ast::{
    AlterViewStatement, CreateViewStatement, DropViewStatement, RefreshMaterializedViewStatement,
};
use crate::tcop::engine::{EngineError, QueryResult, ScalarValue, with_storage_write};

#[derive(Debug, Default)]
struct RefreshScheduler {
    active_relation_oids: HashSet<crate::catalog::oid::Oid>,
}

static GLOBAL_REFRESH_SCHEDULER: OnceLock<RwLock<RefreshScheduler>> = OnceLock::new();

fn global_refresh_scheduler() -> &'static RwLock<RefreshScheduler> {
    GLOBAL_REFRESH_SCHEDULER.get_or_init(|| RwLock::new(RefreshScheduler::default()))
}

fn with_refresh_scheduler_write<T>(f: impl FnOnce(&mut RefreshScheduler) -> T) -> T {
    let mut scheduler = match global_refresh_scheduler().write() {
        Ok(scheduler) => scheduler,
        Err(poisoned) => {
            debug_assert!(false, "global refresh scheduler lock poisoned for write");
            poisoned.into_inner()
        }
    };
    f(&mut scheduler)
}

#[cfg(test)]
pub(crate) fn reset_refresh_scheduler_for_tests() {
    with_refresh_scheduler_write(|scheduler| {
        scheduler.active_relation_oids.clear();
    });
}

#[derive(Debug)]
struct RefreshExecutionGuard {
    relation_oid: crate::catalog::oid::Oid,
}

impl Drop for RefreshExecutionGuard {
    fn drop(&mut self) {
        with_refresh_scheduler_write(|scheduler| {
            scheduler.active_relation_oids.remove(&self.relation_oid);
        });
    }
}

fn acquire_refresh_execution_guard(
    relation_oid: crate::catalog::oid::Oid,
    qualified_name: &str,
) -> Result<RefreshExecutionGuard, EngineError> {
    let inserted = with_refresh_scheduler_write(|scheduler| {
        scheduler.active_relation_oids.insert(relation_oid)
    });
    if !inserted {
        return Err(EngineError {
            message: format!(
                "cannot refresh materialized view \"{qualified_name}\" because it is already being refreshed"
            ),
        });
    }
    Ok(RefreshExecutionGuard { relation_oid })
}

pub async fn execute_create_materialized_view(
    create: &CreateViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    crate::commands::view::execute_create_view_internal(create, params).await
}

pub async fn execute_alter_materialized_view(
    alter: &AlterViewStatement,
) -> Result<QueryResult, EngineError> {
    crate::commands::view::execute_alter_view_internal(alter).await
}

pub async fn execute_drop_materialized_view(
    drop_view: &DropViewStatement,
) -> Result<QueryResult, EngineError> {
    crate::commands::view::execute_drop_view_internal(drop_view).await
}

pub async fn execute_refresh_materialized_view(
    refresh: &RefreshMaterializedViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let relation = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&refresh.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    if relation.kind() != TableKind::MaterializedView {
        return Err(EngineError {
            message: format!(
                "\"{}\" is not a materialized view",
                relation.qualified_name()
            ),
        });
    }
    crate::tcop::engine::require_relation_owner(&relation)?;
    if refresh.concurrently && !refresh.with_data {
        return Err(EngineError {
            message: "REFRESH MATERIALIZED VIEW CONCURRENTLY does not support WITH NO DATA"
                .to_string(),
        });
    }
    if refresh.concurrently && !relation.indexes().iter().any(|index| index.unique) {
        return Err(EngineError {
            message: format!(
                "cannot refresh materialized view \"{}\" concurrently because it does not have a unique index",
                relation.qualified_name()
            ),
        });
    }
    let _refresh_guard =
        acquire_refresh_execution_guard(relation.oid(), &relation.qualified_name())?;
    let rows = if refresh.concurrently {
        evaluate_materialized_view_rows_concurrently(&relation, refresh.with_data, params).await?
    } else {
        evaluate_materialized_view_rows_live(&relation, refresh.with_data, params).await?
    };
    if refresh.concurrently {
        let still_valid = with_catalog_read(|catalog| {
            catalog
                .table(relation.schema_name(), relation.name())
                .is_some_and(|current| {
                    current.oid() == relation.oid()
                        && current.kind() == TableKind::MaterializedView
                        && current.indexes().iter().any(|index| index.unique)
                })
        });
        if !still_valid {
            return Err(EngineError {
                message: format!(
                    "cannot refresh materialized view \"{}\" concurrently because it changed during refresh",
                    relation.qualified_name()
                ),
            });
        }
    }
    with_storage_write(|storage| {
        let _ = storage.replace_rows_for_table(relation.oid(), rows);
    });
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "REFRESH MATERIALIZED VIEW".to_string(),
        rows_affected: 0,
    })
}

async fn evaluate_materialized_view_rows_live(
    relation: &crate::catalog::Table,
    with_data: bool,
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    if !with_data {
        return Ok(Vec::new());
    }
    let definition = relation.view_definition().ok_or_else(|| EngineError {
        message: format!(
            "view definition for relation \"{}\" is missing",
            relation.qualified_name()
        ),
    })?;
    Ok(crate::tcop::engine::execute_query(definition, params)
        .await?
        .rows)
}

async fn evaluate_materialized_view_rows_concurrently(
    relation: &crate::catalog::Table,
    with_data: bool,
    params: &[Option<String>],
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    let baseline = crate::tcop::engine::snapshot_state();
    let evaluated = evaluate_materialized_view_rows_live(relation, with_data, params).await;
    let post_eval = crate::tcop::engine::snapshot_state();
    crate::tcop::engine::restore_state(baseline);
    with_sequences_write(|sequences| {
        *sequences = post_eval.sequences;
    });
    evaluated
}
