use crate::catalog::{SearchPath, TableKind, with_catalog_read, with_catalog_write};
use crate::commands::create_table::relation_name_for_create;
use crate::parser::ast::{
    AlterViewAction, AlterViewStatement, CreateViewStatement, DropBehavior, DropViewStatement,
};
use crate::tcop::engine::{EngineError, QueryResult, with_storage_write};

pub async fn execute_create_view(
    create: &CreateViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    execute_create_view_internal(create, params).await
}

pub async fn execute_alter_view(alter: &AlterViewStatement) -> Result<QueryResult, EngineError> {
    execute_alter_view_internal(alter).await
}

pub async fn execute_drop_view(drop_view: &DropViewStatement) -> Result<QueryResult, EngineError> {
    execute_drop_view_internal(drop_view).await
}

pub(crate) async fn execute_create_view_internal(
    create: &CreateViewStatement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    let (schema_name, view_name) = relation_name_for_create(&create.name)?;
    
    // Handle IF NOT EXISTS
    if create.if_not_exists {
        let existing =
            with_catalog_read(|catalog| catalog.table(&schema_name, &view_name).cloned());
        if existing.is_some() {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: if create.materialized {
                    "CREATE MATERIALIZED VIEW".to_string()
                } else {
                    "CREATE VIEW".to_string()
                },
                rows_affected: 0,
            });
        }
    }
    
    if create.or_replace {
        let existing =
            with_catalog_read(|catalog| catalog.table(&schema_name, &view_name).cloned());
        if let Some(existing) = existing {
            crate::tcop::engine::require_relation_owner(&existing)?;
        }
    }
    let mut columns = crate::tcop::engine::derive_query_columns(&create.query)?;
    // Apply column aliases from CREATE VIEW v(a, b, c) AS ...
    if !create.column_aliases.is_empty() {
        if create.column_aliases.len() > columns.len() {
            return Err(EngineError {
                message: format!(
                    "CREATE VIEW specifies {} column names, but query produces {} columns",
                    create.column_aliases.len(),
                    columns.len()
                ),
            });
        }
        for (i, alias) in create.column_aliases.iter().enumerate() {
            columns[i] = alias.clone();
        }
    }
    let rows = if create.materialized && create.with_data {
        crate::tcop::engine::execute_query(&create.query, params)
            .await?
            .rows
    } else {
        Vec::new()
    };
    let oid = with_catalog_write(|catalog| {
        if create.or_replace {
            if catalog.table(&schema_name, &view_name).is_some() {
                catalog.replace_view(
                    &schema_name,
                    &view_name,
                    create.materialized,
                    columns,
                    create.query.clone(),
                )
            } else {
                catalog.create_view(
                    &schema_name,
                    &view_name,
                    create.materialized,
                    columns,
                    create.query.clone(),
                )
            }
        } else {
            catalog.create_view(
                &schema_name,
                &view_name,
                create.materialized,
                columns,
                create.query.clone(),
            )
        }
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if create.or_replace {
        with_storage_write(|storage| {
            storage.rows_by_table.remove(&oid);
        });
    }
    if create.materialized {
        with_storage_write(|storage| {
            storage.rows_by_table.insert(oid, rows);
        });
    }
    crate::security::set_relation_owner(oid, &crate::security::current_role());

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if create.materialized {
            "CREATE MATERIALIZED VIEW".to_string()
        } else {
            "CREATE VIEW".to_string()
        },
        rows_affected: 0,
    })
}

pub(crate) async fn execute_alter_view_internal(
    alter: &AlterViewStatement,
) -> Result<QueryResult, EngineError> {
    let relation = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&alter.name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    let expected_kind = if alter.materialized {
        TableKind::MaterializedView
    } else {
        TableKind::View
    };
    if relation.kind() != expected_kind {
        return Err(EngineError {
            message: if alter.materialized {
                format!(
                    "\"{}\" is not a materialized view",
                    relation.qualified_name()
                )
            } else {
                format!("\"{}\" is not a view", relation.qualified_name())
            },
        });
    }
    crate::tcop::engine::require_relation_owner(&relation)?;

    match &alter.action {
        AlterViewAction::RenameTo { new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_relation(relation.schema_name(), relation.name(), new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterViewAction::RenameColumn { old_name, new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_column(relation.schema_name(), relation.name(), old_name, new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterViewAction::SetSchema { schema_name } => {
            with_catalog_write(|catalog| {
                catalog.move_relation_to_schema(
                    relation.schema_name(),
                    relation.name(),
                    schema_name,
                )
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if alter.materialized {
            "ALTER MATERIALIZED VIEW".to_string()
        } else {
            "ALTER VIEW".to_string()
        },
        rows_affected: 0,
    })
}

pub(crate) async fn execute_drop_view_internal(
    drop_view: &DropViewStatement,
) -> Result<QueryResult, EngineError> {
    let expected_kind = if drop_view.materialized {
        TableKind::MaterializedView
    } else {
        TableKind::View
    };

    let mut base_oids = Vec::new();
    for name in &drop_view.names {
        let resolved = with_catalog_read(|catalog| {
            catalog.resolve_table(name, &SearchPath::default()).cloned()
        });
        let relation = match resolved {
            Ok(table) => table,
            Err(_err) if drop_view.if_exists => continue,
            Err(err) => {
                return Err(EngineError {
                    message: err.message,
                });
            }
        };
        if relation.kind() != expected_kind {
            return Err(EngineError {
                message: if drop_view.materialized {
                    format!(
                        "\"{}\" is not a materialized view",
                        relation.qualified_name()
                    )
                } else {
                    format!("\"{}\" is not a view", relation.qualified_name())
                },
            });
        }
        crate::tcop::engine::require_relation_owner(&relation)?;
        if !base_oids.iter().any(|oid| oid == &relation.oid()) {
            base_oids.push(relation.oid());
        }
    }
    if base_oids.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: if drop_view.materialized {
                "DROP MATERIALIZED VIEW".to_string()
            } else {
                "DROP VIEW".to_string()
            },
            rows_affected: 0,
        });
    }

    let drop_order = with_catalog_read(|catalog| {
        crate::catalog::dependency::expand_and_order_relation_drop(
            catalog,
            &base_oids,
            matches!(drop_view.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation = with_catalog_read(|catalog| {
            crate::catalog::dependency::describe_table(catalog, *relation_oid)
        })
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
    crate::commands::drop::drop_relations_by_oid_order(&drop_order)?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: if drop_view.materialized {
            "DROP MATERIALIZED VIEW".to_string()
        } else {
            "DROP VIEW".to_string()
        },
        rows_affected: drop_order.len() as u64,
    })
}
