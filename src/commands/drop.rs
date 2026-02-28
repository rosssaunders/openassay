use crate::catalog::dependency::{
    describe_table as describe_table_from_catalog,
    expand_and_order_relation_drop as expand_and_order_relation_drop_in_catalog,
    expand_table_dependency_set as expand_table_dependencies_in_catalog,
};
use crate::catalog::{SearchPath, with_catalog_read, with_catalog_write};
use crate::parser::ast::{DropBehavior, DropTableStatement, TruncateStatement};
use crate::tcop::engine::{EngineError, QueryResult, with_storage_write};

pub fn drop_relations_by_oid_order(
    drop_order: &[crate::catalog::oid::Oid],
) -> Result<(), EngineError> {
    for table_oid in drop_order {
        let ident = with_catalog_read(|catalog| describe_table_from_catalog(catalog, *table_oid))
            .map_err(|err| EngineError {
            message: err.message,
        })?;
        with_catalog_write(|catalog| catalog.drop_table(&ident.schema_name, &ident.table_name))
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        with_storage_write(|storage| {
            storage.rows_by_table.remove(table_oid);
            storage.drop_all_indexes_for_table(*table_oid);
        });
        crate::security::clear_relation_security(*table_oid);
    }
    Ok(())
}

pub async fn execute_drop_table(
    drop_table: &DropTableStatement,
) -> Result<QueryResult, EngineError> {
    let mut base_table_oids = Vec::new();
    for name in &drop_table.names {
        let resolved = with_catalog_read(|catalog| {
            catalog.resolve_table(name, &SearchPath::default()).cloned()
        });
        let table = match resolved {
            Ok(table) => table,
            Err(_err) if drop_table.if_exists => continue,
            Err(err) => {
                return Err(EngineError {
                    message: err.message,
                });
            }
        };
        if !base_table_oids.contains(&table.oid()) {
            base_table_oids.push(table.oid());
        }
    }
    if base_table_oids.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "DROP TABLE".to_string(),
            rows_affected: 0,
        });
    }

    let drop_order = with_catalog_read(|catalog| {
        expand_and_order_relation_drop_in_catalog(
            catalog,
            &base_table_oids,
            matches!(drop_table.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        crate::tcop::engine::require_relation_owner(&relation_table)?;
    }
    drop_relations_by_oid_order(&drop_order)?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP TABLE".to_string(),
        rows_affected: drop_order.len() as u64,
    })
}

pub async fn execute_truncate(truncate: &TruncateStatement) -> Result<QueryResult, EngineError> {
    let mut base_table_oids = Vec::with_capacity(truncate.table_names.len());
    for table_name in &truncate.table_names {
        let table = with_catalog_read(|catalog| {
            catalog
                .resolve_table(table_name, &SearchPath::default())
                .cloned()
        })
        .map_err(|err| EngineError {
            message: err.message,
        })?;
        if table.kind() != crate::catalog::TableKind::Heap {
            return Err(EngineError {
                message: format!(
                    "cannot truncate system relation \"{}\"",
                    table.qualified_name()
                ),
            });
        }
        crate::tcop::engine::require_relation_privilege(
            &table,
            crate::security::TablePrivilege::Truncate,
        )?;
        if !base_table_oids.iter().any(|oid| oid == &table.oid()) {
            base_table_oids.push(table.oid());
        }
    }

    let targets = with_catalog_read(|catalog| {
        expand_table_dependencies_in_catalog(
            catalog,
            &base_table_oids,
            matches!(truncate.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &targets {
        let relation =
            with_catalog_read(|catalog| describe_table_from_catalog(catalog, *relation_oid))
                .map_err(|err| EngineError {
                    message: err.message,
                })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        crate::tcop::engine::require_relation_privilege(
            &relation_table,
            crate::security::TablePrivilege::Truncate,
        )?;
    }
    with_storage_write(|storage| {
        for table_oid in &targets {
            let _ = storage.replace_rows_for_table(*table_oid, Vec::new());
        }
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "TRUNCATE".to_string(),
        rows_affected: targets.len() as u64,
    })
}
